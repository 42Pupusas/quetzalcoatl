use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::TOMBSTONE;

/// The producer side of an MPSC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and claims slots via
/// atomic fetch-and-add (FAA), eliminating inter-producer cache-line
/// contention on the tail pointer.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
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
    #[inline]
    fn claim_slot(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        // Pre-check: is there room? Uses cached head as a fast-path to avoid
        // an atomic load. Since head only increases, a stale cache just makes
        // the buffer look fuller than it is — safe to refresh on demand.
        let current_tail = self.queue.tail.load(Ordering::Relaxed);
        if current_tail - self.cached_head.get() >= self.queue.cap {
            let head = self.queue.head.load(Ordering::Acquire);
            self.cached_head.set(head);

            if current_tail - head >= self.queue.cap {
                return None;
            }
        }

        // Claim a unique position via fetch-and-add. Unlike CAS, this always
        // succeeds on the first try — no retry loop, no inter-producer
        // cache-line contention on the tail pointer.
        let pos = self.queue.tail.fetch_add(1, Ordering::Relaxed);

        let slot = self.queue.slot(pos);

        // Wait for the slot to be free. This handles the race where multiple
        // producers passed the pre-check and claimed positions via FAA — some
        // may land on slots the consumer hasn't released yet. Each producer
        // spins on its own slot's sequence number (different cache lines),
        // so there is no cross-producer contention during this wait.
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != pos * 2 {
            let mut backoff = 0u32;
            while slot.sequence.load(Ordering::Acquire) != pos * 2 {
                crate::common::cas_backoff(&mut backoff);
            }
        }

        Some((slot.data.get(), &slot.sequence, pos))
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Multiple producers can push concurrently.
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
    /// tombstoned and the consumer silently skips it.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        self.claim_slot()
            .map(|(data_ptr, slot_seq, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_seq,
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

/// A write-reservation into an MPSC ring buffer slot.
///
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → slot is tombstoned (consumer skips it).
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    pos: usize,
}

// SAFETY: SlotWriter holds exclusive access to the slot (FAA claim).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

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
            slot_seq: this.slot_seq,
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
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        self.slot_seq.store(TOMBSTONE, Ordering::Release);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and slot is tombstoned.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    pos: usize,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to the consumer.
    #[inline]
    pub fn commit(mut self) {
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
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
            self.slot_seq.store(TOMBSTONE, Ordering::Release);
        }
    }
}
