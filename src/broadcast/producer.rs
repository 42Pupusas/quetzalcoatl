use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The producer side of a broadcast ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for slots
/// via atomic CAS.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Cached snapshot of `min_head` to avoid scanning all consumer slots on
    /// every push. Since heads only increase, a stale value is safe — it just
    /// makes the buffer appear fuller than it is. Refreshed only when needed.
    pub(super) cached_min_head: std::cell::Cell<usize>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            cached_min_head: std::cell::Cell::new(0),
        }
    }
}

impl<T> Producer<T> {
    /// Exponential backoff for CAS contention. Marked `#[inline(never)]` to
    /// keep the hot push loop's instruction footprint small.
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
    /// Returns raw pointers to the slot's data, sequence atomic, and the
    /// claimed position. Returns `None` if the buffer is full.
    ///
    /// On success, drops any old value in the slot (from a previous lap)
    /// and clears the sequence to 0.
    fn claim_slot(
        &self,
    ) -> Option<(*mut MaybeUninit<T>, *const AtomicUsize, usize)> {
        let mut backoff = 0u32;
        let mut tail = self.queue.tail.load(Ordering::Relaxed);
        loop {
            // Fast path: check against cached min_head
            if tail.wrapping_sub(self.cached_min_head.get()) >= self.queue.cap {
                let min_head = self.queue.min_head();
                self.cached_min_head.set(min_head);

                if tail.wrapping_sub(min_head) >= self.queue.cap {
                    return None;
                }
            }

            // Atomically reserve this slot
            match self.queue.tail.compare_exchange_weak(
                tail,
                tail.wrapping_add(1),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // SAFETY: `tail & mask` is always < cap by construction
                    let slot =
                        unsafe { self.queue.buf.get_unchecked(tail & self.queue.mask) };

                    // Drop old value if this slot was previously written.
                    if std::mem::needs_drop::<T>() {
                        // swap(0, Acquire) atomically reads old sequence and clears it,
                        // synchronizing with the previous producer's Release store.
                        let old_seq = slot.sequence.swap(0, Ordering::Acquire);
                        if old_seq > 0 {
                            // SAFETY: old_seq > 0 means data was initialized by a prior write.
                            // All consumers have advanced past this slot (min_head check passed).
                            unsafe {
                                slot.data.get().cast::<T>().drop_in_place();
                            }
                        }
                    } else {
                        // No destructor needed — just clear the sequence.
                        // A simple store is cheaper than an atomic swap.
                        slot.sequence.store(0, Ordering::Relaxed);
                    }

                    return Some((slot.data.get(), &raw const slot.sequence, tail));
                }
                Err(actual) => {
                    tail = actual;
                    Self::cas_backoff(&mut backoff);
                }
            }
        }
    }

    /// Pushes a value into the broadcast ring buffer.
    ///
    /// Multiple producers can push concurrently. Returns `Err(val)` if the
    /// buffer is full (all slots occupied by data that the slowest consumer
    /// hasn't read yet).
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, seq_ptr, pos)) => {
                // SAFETY: We atomically claimed this slot via CAS and dropped
                // any old value. The slot is now uninitialized and exclusively ours.
                unsafe { (*data_ptr).write(val) };
                // Publish: set sequence = pos + 1 so consumers see this data.
                unsafe {
                    (*seq_ptr).store(pos + 1, Ordering::Release);
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
        self.claim_slot().map(|(data_ptr, seq_ptr, pos)| SlotWriter {
            slot_data: data_ptr,
            slot_sequence: seq_ptr,
            pos,
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

/// A write-reservation into a broadcast ring buffer slot.
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
    slot_sequence: *const AtomicUsize,
    pos: usize,
    _ring: Arc<RingBuffer<T>>,
    committed: bool,
}

// SAFETY: SlotWriter holds exclusive access to the slot (claimed via CAS).
// The raw pointers point into the Arc<RingBuffer<T>> which is kept alive.
unsafe impl<T: Send + Sync> Send for SlotWriter<T> {}

impl<T> SlotWriter<T> {
    /// Returns a mutable reference to the uninitialized slot memory.
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

    /// Commits the write, making the slot visible to all consumers.
    ///
    /// Sets the slot's sequence to `pos + 1` with `Release` ordering.
    ///
    /// # Safety contract
    ///
    /// The caller must have initialized the slot data (via [`write`] or
    /// [`slot_mut`](Self::slot_mut) + `MaybeUninit::write`) before calling `commit`.
    /// Committing without initializing causes consumers to read
    /// uninitialized memory (undefined behavior).
    pub fn commit(mut self) {
        // SAFETY: slot_sequence points into the RingBuffer kept alive by _ring.
        unsafe {
            (*self.slot_sequence).store(self.pos + 1, Ordering::Release);
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
