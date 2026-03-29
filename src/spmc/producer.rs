use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
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
}

impl<T> Producer<T> {
    fn try_claim(&self) -> Option<(*mut MaybeUninit<T>, *const AtomicUsize, usize)> {
        let pos = self.write_pos.get();

        // SAFETY: `pos & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(pos & self.queue.mask) };

        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != pos * 2 {
            return None;
        }

        self.write_pos.set(pos + 1);

        Some((slot.data.get(), &raw const slot.sequence, pos))
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.try_claim() {
            Some((data_ptr, seq_ptr, pos)) => {
                // SAFETY: We are the sole producer and the slot is free.
                unsafe { (*data_ptr).write(val) };
                unsafe {
                    (*seq_ptr).store(pos * 2 + 1, Ordering::Release);
                }
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
    /// Returns `None` if the buffer is full. Takes `&mut self` to
    /// guarantee at most one outstanding reservation. If dropped
    /// without writing, the reservation is silently rolled back.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        self.try_claim().map(|(data_ptr, seq_ptr, pos)| SlotWriter {
            slot_data: data_ptr,
            // SAFETY: seq_ptr points into the RingBuffer kept alive by our Arc.
            slot_seq: unsafe { &*seq_ptr },
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

/// A write-reservation into an SPMC ring buffer slot.
///
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → silently rolled back.
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
}

unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Requires [`commit_unchecked`](Self::commit_unchecked) (unsafe) to publish.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_seq: this.slot_seq,
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
    /// The caller must have initialized the slot data via
    /// [`slot_mut`](Self::slot_mut).
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
        self.tail.store(self.write_pos.get(), Ordering::Release);
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        // Sequence is still pos * 2 (free) — just roll back.
        self.write_pos.set(self.pos);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and reservation rolled back.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
    committed: bool,
}

unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to consumers.
    #[inline]
    pub fn commit(mut self) {
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
        self.tail.store(self.write_pos.get(), Ordering::Release);
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
            self.slot_seq.store(self.pos * 2, Ordering::Release);
            self.write_pos.set(self.pos);
        }
    }
}
