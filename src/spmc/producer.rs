use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The producer side of an SPMC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one producer exists per buffer.
///
/// On drop, sets the ring buffer's `closed` flag so any consumer
/// spinning on overshoot can return `None` instead of hanging.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local write cursor. Single producer means no atomics needed for
    /// our own state — we publish via per-slot `ready` markers and
    /// (Release-)update the shared `tail` so consumers can pre-FAA peek.
    pub(super) write_pos: std::cell::Cell<usize>,
}

impl<T> Producer<T> {
    /// Tries to claim slot at logical position `tail`.
    ///
    /// The slot is free iff `ready[s] == tail` — i.e. the previous
    /// consumer at logical position `tail - cap` released it for us
    /// (or, for the first lap, this is the slot's initial state).
    fn try_claim(&self) -> Option<(*mut MaybeUninit<T>, *const AtomicUsize, usize)> {
        let pos = self.write_pos.get();
        let s = pos & self.queue.mask;

        // SAFETY: `s` is always < cap by construction.
        let ready = unsafe { self.queue.ready.get_unchecked(s) };
        if ready.0.load(Ordering::Acquire) != pos {
            return None;
        }

        // SAFETY: `s` is always < cap by construction.
        let data_ptr = unsafe { self.queue.data.get_unchecked(s) }.get();

        self.write_pos.set(pos + 1);
        Some((data_ptr, &raw const ready.0, pos))
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.try_claim() {
            Some((data_ptr, ready_ptr, pos)) => {
                // SAFETY: We are the sole producer and the slot is free.
                unsafe { (*data_ptr).write(val) };
                // Publish: ready[s] = pos + 1 tells the consumer at
                // logical position `pos` that data is available.
                // SAFETY: ready_ptr points into the RingBuffer kept alive by Arc.
                unsafe {
                    (*ready_ptr).store(pos + 1, Ordering::Release);
                }
                // Consumers Acquire-load `tail` in their pre-FAA peek;
                // Release ensures they see published slot data.
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
        self.try_claim()
            .map(|(data_ptr, ready_ptr, pos)| SlotWriter {
                slot_data: data_ptr,
                // SAFETY: ready_ptr points into the RingBuffer kept alive by our Arc.
                slot_ready: unsafe { &*ready_ptr },
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

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Wake any consumer spinning on overshoot. Release synchronizes
        // with consumer's Acquire-load of the closed flag.
        self.queue.closed.0.store(true, Ordering::Release);
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
    slot_ready: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Requires [`commit_unchecked`](Self::commit_unchecked) (unsafe) to publish.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Single producer has exclusive access. The pointer is valid
        // because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access (single producer), valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_ready: this.slot_ready,
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
        self.slot_ready.store(self.pos + 1, Ordering::Release);
        self.tail.store(self.write_pos.get(), Ordering::Release);
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        // ready is still `pos` (free) — just roll back the local cursor.
        self.write_pos.set(self.pos);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and reservation rolled back.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_ready: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to consumers.
    #[inline]
    pub fn commit(mut self) {
        self.slot_ready.store(self.pos + 1, Ordering::Release);
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
            // Restore ready to "free for producer at this position".
            self.slot_ready.store(self.pos, Ordering::Release);
            self.write_pos.set(self.pos);
        }
    }
}
