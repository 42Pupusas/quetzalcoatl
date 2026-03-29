use std::cell::Cell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The producer side of an SPSC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one producer exists per buffer.
///
/// `Producer` is [`Send`] but not [`Sync`] (due to internal [`Cell`]s).
/// Wrapping it in a `Mutex` is valid but pointless — the buffer is SPSC
/// by design, so there is no benefit to sharing the producer across threads.
/// If you need multi-producer semantics, use [`mpsc::RingBuffer`](crate::mpsc::RingBuffer).
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local write cursor — always >= the atomic tail. Incremented on every
    /// push or reserve.
    write_pos: Cell<usize>,
    /// Cached snapshot of `head` to avoid cross-cache-line reads on every push.
    /// Since `head` only ever increases, a stale value is safe — it just makes
    /// the buffer appear fuller than it is. We re-fetch only when needed.
    cached_head: Cell<usize>,
}

// No Clone impl — SPSC enforces a single producer.

impl<T> Producer<T> {
    pub(super) const fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            write_pos: Cell::new(0),
            cached_head: Cell::new(0),
        }
    }

    /// Claims the next slot using the local write cursor.
    /// Returns the write position on success, or `None` if the buffer is full.
    #[inline]
    fn try_claim(&self) -> Option<usize> {
        let pos = self.write_pos.get();

        if pos - self.cached_head.get() >= self.queue.cap {
            let head = self.queue.head.load(Ordering::Acquire);
            self.cached_head.set(head);

            if pos - head >= self.queue.cap {
                return None;
            }
        }

        self.write_pos.set(pos + 1);
        Some(pos)
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let Some(pos) = self.try_claim() else {
            return Err(val);
        };

        // SAFETY: Single producer owns this slot. `pos & mask` < cap.
        unsafe {
            (*self.queue.buf.get_unchecked(pos & self.queue.mask))
                .get()
                .write(MaybeUninit::new(val));
        }

        // Release: ensures the data write above is visible before tail advances.
        self.queue.tail.store(pos + 1, Ordering::Release);

        Ok(())
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns a
    /// [`SlotWriter`] that provides direct mutable access to the slot.
    /// The slot is not visible to the consumer until committed.
    ///
    /// Takes `&mut self` to guarantee at most one outstanding reservation.
    /// If dropped without writing, the reservation is silently rolled back.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        let pos = self.try_claim()?;

        // SAFETY: Single producer owns this slot. `pos & mask` < cap.
        let slot_data = unsafe {
            (*self.queue.buf.get_unchecked(pos & self.queue.mask))
                .get()
                .cast::<MaybeUninit<T>>()
        };

        Some(SlotWriter {
            slot_data,
            tail: &self.queue.tail,
            write_pos: &self.write_pos,
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

/// A write-reservation into a ring buffer slot.
///
/// Obtained via [`Producer::reserve`]. Call [`write`](Self::write) to
/// initialize the slot and get a [`WrittenSlot`] that can be committed.
///
/// For fine-grained control, use [`slot_mut`](Self::slot_mut) to access
/// the raw `MaybeUninit<T>`, then [`commit_unchecked`](Self::commit_unchecked)
/// (unsafe) to publish.
///
/// If dropped, the reservation is silently rolled back.
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    tail: &'a AtomicUsize,
    write_pos: &'a Cell<usize>,
    pos: usize,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use [`commit_unchecked`](Self::commit_unchecked) (unsafe) after
    /// initializing through this reference. If dropped without
    /// committing, any data written through this reference is leaked.
    ///
    /// Prefer [`write`](Self::write) for the safe path.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Single producer has exclusive access. The pointer
        // is valid because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the slot, consuming this `SlotWriter` and
    /// returning a [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        // Prevent SlotWriter::drop from rolling back write_pos.
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access, valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            tail: this.tail,
            write_pos: this.write_pos,
            pos: this.pos,
            committed: false,
        }
    }

    /// Commits without verifying initialization.
    ///
    /// # Safety
    ///
    /// The caller must have initialized the slot data (via
    /// [`slot_mut`](Self::slot_mut) + [`MaybeUninit::write`]).
    /// Committing without initializing causes the consumer to read
    /// uninitialized memory (undefined behavior).
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        self.tail.store(self.pos + 1, Ordering::Release);
        // Skip the SlotWriter drop (which would roll back write_pos).
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        // No data was written (write() consumes self) — just roll back.
        self.write_pos.set(self.pos);
    }
}

/// A slot that has been initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to make the data visible to the consumer.
/// If dropped without committing, the value is dropped and the
/// reservation is rolled back.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    tail: &'a AtomicUsize,
    write_pos: &'a Cell<usize>,
    pos: usize,
    committed: bool,
}

// SAFETY: Same as SlotWriter — exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to the consumer.
    #[inline]
    pub fn commit(mut self) {
        self.tail.store(self.pos + 1, Ordering::Release);
        self.committed = true;
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            self.write_pos.set(self.pos);
        }
    }
}
