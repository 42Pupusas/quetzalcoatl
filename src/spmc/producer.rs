use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The producer side of an SPMC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one producer exists per buffer.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local write cursor. Because there is only one producer, no atomic
    /// operations are needed — we simply increment after each claim.
    pub(super) write_pos: std::cell::Cell<usize>,
    /// Cached snapshot of `head` to avoid cross-cache-line reads on every push.
    /// Since `head` only ever increases, a stale value is safe — it just makes
    /// the buffer appear fuller than it is. We re-fetch only when needed.
    pub(super) cached_head: std::cell::Cell<usize>,
}

impl<T> Producer<T> {
    /// Checks whether the next slot is available for writing.
    ///
    /// Two conditions must hold before the producer may write to a slot:
    ///
    /// 1. **Distance check** (`write_pos - head < cap`): prevents the
    ///    producer from wrapping past its own outstanding reservations.
    ///
    /// 2. **Ready flag check** (`ready == false`): the authoritative signal
    ///    that a consumer has finished reading and released the slot.
    ///    Because consumers CAS `head` *before* reading data, `head`
    ///    advancing does NOT mean the slot is free — only `ready = false`
    ///    does.
    fn try_claim(&self) -> Option<(*mut MaybeUninit<T>, *const AtomicBool)> {
        let pos = self.write_pos.get();

        // Fast path: check against cached head (avoids cross-cache-line read)
        if pos - self.cached_head.get() >= self.queue.cap {
            // Cached head says full — refresh from the real atomic
            let head = self.queue.head.load(Ordering::Acquire);
            self.cached_head.set(head);

            if pos - head >= self.queue.cap {
                return None;
            }
        }

        // SAFETY: `pos & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(pos & self.queue.mask) };

        // The ready flag is the authoritative "slot is free" signal.
        // A consumer CAS-es head before reading, so head advancing does
        // NOT mean the data has been read. Only ready=false (set by the
        // consumer after reading) guarantees the slot is safe to overwrite.
        if slot.ready.load(Ordering::Acquire) {
            return None;
        }

        // Advance write_pos — this is our local claim.
        self.write_pos.set(pos + 1);

        Some((slot.data.get(), &raw const slot.ready))
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full. Only one producer may
    /// exist, so no CAS is needed.
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.try_claim() {
            Some((data_ptr, ready_ptr)) => {
                // SAFETY: We are the sole producer and the slot is free
                // (ready=false verified in try_claim).
                unsafe { (*data_ptr).write(val) };

                // Publish the data: set ready flag so consumers know data
                // is valid.
                // SAFETY: ready_ptr points into the RingBuffer kept alive
                // by Arc.
                unsafe {
                    (*ready_ptr).store(true, Ordering::Release);
                }

                // Update tail for len()/is_empty()/is_full() queries.
                self.queue
                    .tail
                    .store(self.write_pos.get(), Ordering::Release);

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
        self.try_claim().map(|(data_ptr, ready_ptr)| SlotWriter {
            slot_data: data_ptr,
            slot_ready: ready_ptr,
            tail: &raw const *self.queue.tail,
            write_pos: self.write_pos.get(),
            _ring: Arc::clone(&self.queue),
            committed: false,
        })
    }

    /// Returns the number of items currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns `true` if the buffer is at capacity.
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
/// because the slot cannot be reclaimed (the write position has already
/// advanced).
pub struct SlotWriter<T> {
    slot_data: *mut MaybeUninit<T>,
    slot_ready: *const AtomicBool,
    tail: *const std::sync::atomic::AtomicUsize,
    write_pos: usize,
    _ring: Arc<RingBuffer<T>>,
    committed: bool,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The raw pointers point into the Arc<RingBuffer<T>> which is kept alive.
unsafe impl<T: Send> Send for SlotWriter<T> {}

impl<T> SlotWriter<T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use this for fine-grained control over initialization.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: We have exclusive access as the sole producer. The pointer
        // is valid because _ring keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the reserved slot and returns a mutable
    /// reference to the now-initialized data.
    pub fn write(&mut self, val: T) -> &mut T {
        // SAFETY: Same as slot_mut — exclusive access, valid pointer.
        unsafe { (*self.slot_data).write(val) }
    }

    /// Commits the write, making the slot visible to consumers.
    ///
    /// Sets the slot's ready flag with `Release` ordering and consumes
    /// the `SlotWriter`.
    ///
    /// # Safety contract
    ///
    /// The caller must have initialized the slot data (via [`write`] or
    /// [`slot_mut`](Self::slot_mut) + `MaybeUninit::write`) before
    /// calling `commit`. Committing without initializing causes a
    /// consumer to read uninitialized memory (undefined behavior).
    pub fn commit(mut self) {
        // SAFETY: slot_ready points into the RingBuffer kept alive by _ring.
        unsafe {
            (*self.slot_ready).store(true, Ordering::Release);
        }
        // Update tail for len()/is_empty()/is_full() queries.
        // SAFETY: tail points into the RingBuffer kept alive by _ring.
        unsafe {
            (*self.tail).store(self.write_pos, Ordering::Release);
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
