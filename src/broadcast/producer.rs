use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::TOMBSTONE;

/// The producer side of a broadcast ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for slots
/// via atomic fetch-and-add (FAA).
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
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
    #[inline]
    fn claim_slot(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        // Pre-check with L1/L2/L3 min_head cache: avoid a wasted FAA
        // when the buffer is clearly full.
        let current_tail = self.queue.tail.load(Ordering::Relaxed);
        if current_tail.wrapping_sub(self.cached_min_head.get()) >= self.queue.cap {
            let shared = self.queue.min_head_cache.load(Ordering::Acquire);
            self.cached_min_head.set(shared);

            if current_tail.wrapping_sub(shared) >= self.queue.cap {
                let min_head = self.queue.min_head();
                self.queue
                    .min_head_cache
                    .fetch_max(min_head, Ordering::Release);
                self.cached_min_head.set(min_head);

                if current_tail.wrapping_sub(min_head) >= self.queue.cap {
                    return None;
                }
            }
        }

        // Claim a unique position via FAA — always succeeds on the first
        // try, eliminating inter-producer cache-line contention entirely.
        let pos = self.queue.tail.fetch_add(1, Ordering::Relaxed);

        let slot = self.queue.slot(pos);

        // Wait for all consumers to advance past the slot's previous
        // occupant (pos - cap). Each producer spins on min_head — no
        // inter-producer contention on the tail cache line.
        if pos.wrapping_sub(self.cached_min_head.get()) >= self.queue.cap {
            let mut backoff = 0u32;
            loop {
                let min_head = self.queue.min_head();
                self.queue
                    .min_head_cache
                    .fetch_max(min_head, Ordering::Release);
                self.cached_min_head.set(min_head);
                if pos.wrapping_sub(min_head) < self.queue.cap {
                    break;
                }
                crate::common::cas_backoff(&mut backoff);
            }
        }

        // Drop old value if this slot was previously written.
        if std::mem::needs_drop::<T>() {
            let old_seq = slot.sequence.swap(0, Ordering::Acquire);
            if old_seq > 0 && old_seq != TOMBSTONE {
                // SAFETY: old_seq > 0 means data was initialized
                // by a prior push. We own the slot via FAA claim.
                unsafe {
                    slot.data.get().cast::<T>().drop_in_place();
                }
            }
        } else {
            slot.sequence.store(0, Ordering::Relaxed);
        }

        Some((slot.data.get(), &slot.sequence, pos))
    }

    /// Pushes a value into the broadcast ring buffer.
    ///
    /// Multiple producers can push concurrently. Returns `Err(val)` if
    /// the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, slot_seq, pos)) => {
                // SAFETY: We exclusively own this slot via FAA claim.
                unsafe { (*data_ptr).write(val) };
                slot_seq.store(pos * 2 + 1, Ordering::Release);
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Takes `&mut self` to guarantee at most one outstanding reservation
    /// per producer handle. If dropped without writing, the slot is
    /// tombstoned and consumers silently skip it.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        self.claim_slot()
            .map(|(data_ptr, slot_sequence, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_sequence,
                pos,
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
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → slot is tombstoned (consumers skip it).
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_sequence: &'a AtomicUsize,
    pos: usize,
}

// SAFETY: SlotWriter holds exclusive access to the slot (FAA claim).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send + Sync> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Requires [`commit_unchecked`](Self::commit_unchecked) (unsafe) to publish.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Exclusive access via FAA claim. The pointer is valid
        // because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access via FAA claim, valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_sequence: this.slot_sequence,
            pos: this.pos,
            committed: false,
        }
    }

    /// Commits without verifying initialization.
    ///
    /// # Safety
    ///
    /// The caller must have initialized the slot data via
    /// [`slot_mut`](Self::slot_mut).
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        self.slot_sequence
            .store(self.pos * 2 + 1, Ordering::Release);
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        self.slot_sequence.store(TOMBSTONE, Ordering::Release);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and slot is tombstoned.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_sequence: &'a AtomicUsize,
    pos: usize,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send + Sync> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to all consumers.
    #[inline]
    pub fn commit(mut self) {
        self.slot_sequence
            .store(self.pos * 2 + 1, Ordering::Release);
        self.committed = true;
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot data.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            self.slot_sequence.store(TOMBSTONE, Ordering::Release);
        }
    }
}
