use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::TOMBSTONE;

/// The producer side of an MPSC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for slots
/// via atomic CAS.
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
    /// Atomically claims the next available slot via CAS loop.
    ///
    /// Uses a cached head for the distance check (thread-local, zero
    /// contention), then CAS on tail to claim the position. The distance
    /// check guarantees the slot is free — no per-slot atomic load needed
    /// on the producer side.
    #[inline]
    fn claim_slot(&self) -> Option<(*mut MaybeUninit<T>, *const AtomicUsize, usize)> {
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
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // SAFETY: `tail & mask` is always < cap by construction.
                    // The distance check guarantees this slot has been fully
                    // consumed — no sequence check needed on the producer side.
                    let slot = unsafe { self.queue.buf.get_unchecked(tail & self.queue.mask) };
                    return Some((slot.data.get(), &raw const slot.sequence, tail));
                }
                Err(actual) => {
                    // Use the actual tail returned by CAS instead of reloading
                    tail = actual;
                    crate::common::cas_backoff(&mut backoff);
                }
            }
        }
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Multiple producers can push concurrently. Uses CAS loop to
    /// atomically reserve slots.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, seq_ptr, pos)) => {
                // SAFETY: We atomically claimed this slot via CAS
                unsafe { (*data_ptr).write(val) };
                // Publish: set sequence = pos * 2 + 1 so consumer sees data.
                // SAFETY: seq_ptr points into the RingBuffer kept alive by Arc
                unsafe {
                    (*seq_ptr).store(pos * 2 + 1, Ordering::Release);
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
    /// Takes `&mut self` to guarantee at most one outstanding reservation
    /// per producer handle. If the `SlotWriter` is dropped without
    /// committing, the slot is tombstoned and the consumer silently
    /// skips it — no panic, no abort.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        self.claim_slot()
            .map(|(data_ptr, seq_ptr, pos)| SlotWriter {
                slot_data: data_ptr,
                // SAFETY: seq_ptr points into the RingBuffer kept alive by our Arc.
                slot_seq: unsafe { &*seq_ptr },
                pos,
                state: SlotState::Reserved,
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

/// Tracks whether data was written into a reserved slot.
enum SlotState {
    Reserved,
    Written,
    Committed,
}

/// A write-reservation into a ring buffer slot.
///
/// Obtained via [`Producer::reserve`]. Provides direct mutable access
/// to the slot's memory, enabling zero-copy writes for large types.
///
/// If dropped without committing, the slot is marked as a tombstone
/// and the consumer silently skips it. Any data written via
/// [`write`](SlotWriter::write) is properly dropped. Data written
/// directly through [`slot_mut`](SlotWriter::slot_mut) will be leaked
/// on rollback — use [`write`](SlotWriter::write) for automatic cleanup.
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    pos: usize,
    state: SlotState,
}

// SAFETY: SlotWriter holds exclusive access to the slot (claimed via CAS).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<T> SlotWriter<'_, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use this for fine-grained control over initialization. Note: if
    /// you initialize data through this reference and then drop the
    /// `SlotWriter` without committing, the value will be leaked. Use
    /// [`write`](Self::write) instead for automatic cleanup on drop.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: We have exclusive access via CAS claim. The pointer
        // is valid because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the reserved slot and returns a mutable
    /// reference to the now-initialized data.
    ///
    /// If the `SlotWriter` is dropped without committing, the value
    /// is properly dropped during tombstoning.
    pub fn write(&mut self, val: T) -> &mut T {
        // SAFETY: Same as slot_mut — exclusive access, valid pointer.
        let r = unsafe { (*self.slot_data).write(val) };
        self.state = SlotState::Written;
        r
    }

    /// Commits the write, making the slot visible to the consumer.
    ///
    /// Sets the slot's sequence to `pos * 2 + 1` with `Release` ordering
    /// and consumes the `SlotWriter`.
    ///
    /// # Safety contract
    ///
    /// The caller must have initialized the slot data (via [`write`] or
    /// [`slot_mut`](Self::slot_mut) + `MaybeUninit::write`) before calling `commit`.
    /// Committing without initializing causes the consumer to read
    /// uninitialized memory (undefined behavior).
    #[inline]
    pub fn commit(mut self) {
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
        self.state = SlotState::Committed;
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        match self.state {
            SlotState::Committed => {}
            SlotState::Written => {
                // Drop the written value, then tombstone the slot.
                // SAFETY: write() initialized this slot.
                unsafe {
                    self.slot_data.cast::<T>().drop_in_place();
                }
                self.slot_seq.store(TOMBSTONE, Ordering::Release);
            }
            SlotState::Reserved => {
                // Nothing written — just tombstone the slot so the
                // consumer can skip past it.
                self.slot_seq.store(TOMBSTONE, Ordering::Release);
            }
        }
    }
}
