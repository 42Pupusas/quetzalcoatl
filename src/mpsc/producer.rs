use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::RingBuffer;

pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Cached snapshot of `head` to avoid cross-cache-line reads on every push.
    /// Since `head` only ever increases, a stale value is safe — it just makes
    /// the buffer appear fuller than it is. We re-fetch only when needed.
    pub(super) cached_head: std::cell::Cell<usize>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            cached_head: std::cell::Cell::new(0),
        }
    }
}

impl<T> Producer<T> {
    /// Exponential backoff for CAS contention. Marked `#[inline(never)]` to
    /// keep the hot push loop's instruction footprint small — this code only
    /// matters under real multi-producer contention.
    #[inline(never)]
    fn cas_backoff(failures: &mut u32) {
        let f = *failures;
        if f > 1 {
            for _ in 0..1u32 << f {
                std::hint::spin_loop();
            }
        }
        *failures = f.saturating_add(1).min(6);
    }

    /// Atomically claims the next available slot via CAS loop.
    ///
    /// Returns raw pointers to the slot's data and ready flag, or `None`
    /// if the buffer is full. Used by both `push` and `reserve`.
    fn claim_slot(
        &self,
    ) -> Option<(*mut MaybeUninit<T>, *const AtomicBool)> {
        let mut backoff = 0u32;
        let mut tail = self.queue.tail.load(Ordering::Relaxed);
        loop {
            // Fast path: check against cached head (avoids cross-cache-line read)
            if tail - self.cached_head.get() >= self.queue.cap {
                // Cached head says full — refresh from the real atomic
                let head = self.queue.head.load(Ordering::Acquire);
                self.cached_head.set(head);

                if tail - head >= self.queue.cap {
                    return None;
                }
            }

            // Atomically reserve this slot
            match self.queue.tail.compare_exchange_weak(
                tail,
                tail + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // SAFETY: `tail & mask` is always < cap by construction
                    let slot =
                        unsafe { self.queue.buf.get_unchecked(tail & self.queue.mask) };
                    return Some((slot.data.get(), &raw const slot.ready));
                }
                Err(actual) => {
                    // Use the actual tail returned by CAS instead of reloading
                    tail = actual;
                    Self::cas_backoff(&mut backoff);
                }
            }
        }
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Multiple producers can push concurrently. Uses CAS loop to
    /// atomically reserve slots.
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, ready_ptr)) => {
                // SAFETY: We atomically claimed this slot via CAS
                unsafe { (*data_ptr).write(val) };
                // SAFETY: ready_ptr points into the RingBuffer kept alive by Arc
                unsafe {
                    (*ready_ptr).store(true, Ordering::Release);
                }
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns a
    /// [`SlotWriter`] that provides direct mutable access to the slot.
    ///
    /// # Contract
    ///
    /// You **must** call [`SlotWriter::commit`] after writing data.
    /// Dropping a `SlotWriter` without committing aborts the process.
    #[must_use]
    pub fn reserve(&self) -> Option<SlotWriter<T>> {
        self.claim_slot().map(|(data_ptr, ready_ptr)| SlotWriter {
            slot_data: data_ptr,
            slot_ready: ready_ptr,
            _ring: Arc::clone(&self.queue),
            committed: false,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }
}

/// A write-reservation into a ring buffer slot.
///
/// Obtained via [`Producer::reserve`]. Provides direct mutable access
/// to the slot's memory, enabling zero-copy writes for large types.
///
/// # Contract
///
/// You **must** call [`commit`](SlotWriter::commit) after writing data.
/// Dropping a `SlotWriter` without committing will **abort the process**
/// because the slot cannot be reclaimed (the tail has already advanced).
pub struct SlotWriter<T> {
    slot_data: *mut MaybeUninit<T>,
    slot_ready: *const AtomicBool,
    _ring: Arc<RingBuffer<T>>,
    committed: bool,
}

// SAFETY: SlotWriter holds exclusive access to the slot (claimed via CAS).
// The raw pointers point into the Arc<RingBuffer<T>> which is kept alive.
unsafe impl<T: Send> Send for SlotWriter<T> {}

impl<T> SlotWriter<T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use this for fine-grained control over initialization.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: We have exclusive access via CAS claim. The pointer
        // is valid because _ring keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the reserved slot and returns a mutable
    /// reference to the now-initialized data.
    pub fn write(&mut self, val: T) -> &mut T {
        // SAFETY: Same as slot_mut — exclusive access, valid pointer.
        unsafe { (*self.slot_data).write(val) }
    }

    /// Commits the write, making the slot visible to the consumer.
    ///
    /// Sets the slot's ready flag with `Release` ordering and consumes
    /// the `SlotWriter`.
    ///
    /// # Safety contract
    ///
    /// The caller must have initialized the slot data (via [`write`] or
    /// [`slot_mut`] + `MaybeUninit::write`) before calling `commit`.
    /// Committing without initializing causes the consumer to read
    /// uninitialized memory (undefined behavior).
    pub fn commit(mut self) {
        // SAFETY: slot_ready points into the RingBuffer kept alive by _ring.
        unsafe {
            (*self.slot_ready).store(true, Ordering::Release);
        }
        self.committed = true;
    }
}

impl<T> Drop for SlotWriter<T> {
    fn drop(&mut self) {
        if !self.committed {
            eprintln!(
                "FATAL: SlotWriter<{}> dropped without commit. \
                 The ring buffer slot is permanently stuck. Aborting.",
                std::any::type_name::<T>()
            );
            std::process::abort();
        }
    }
}
