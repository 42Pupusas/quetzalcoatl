use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::RingBuffer;

pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
}

impl<T> Consumer<T> {
    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    ///
    /// Returns None if the queue is empty or if a slot has been claimed
    /// by a producer but not yet written (non-blocking behavior).
    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        let head = self.queue.head.load(Ordering::Relaxed);
        let tail = self.queue.tail.load(Ordering::Relaxed);

        if tail == head {
            return None;
        }

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        // Check if slot is ready (producer may have claimed but not written yet)
        if !slot.ready.load(Ordering::Acquire) {
            return None; // Slot claimed but not written yet
        }

        // SAFETY: We checked that the slot is ready
        let val = unsafe { (*slot.data.get()).assume_init_read() };

        // Clear ready flag for next lap around the ring
        slot.ready.store(false, Ordering::Relaxed);

        // Advance head
        self.queue
            .head
            .store(head + 1, Ordering::Release);

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    ///
    /// Unlike [`pop`], this does not copy the data out. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The slot is released
    /// when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the queue is empty or the next slot is not yet
    /// committed (same semantics as `pop`).
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let head = self.queue.head.load(Ordering::Relaxed);
        let tail = self.queue.tail.load(Ordering::Relaxed);

        if tail == head {
            return None;
        }

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        if !slot.ready.load(Ordering::Acquire) {
            return None;
        }

        // SAFETY: ready=true guarantees the slot has been initialized.
        // We store raw pointers to avoid the borrow conflict between
        // borrowing slot data and holding &mut self.
        let data_ptr = slot.data.get().cast_const();
        let ready_ptr = &raw const slot.ready;

        Some(SlotReader {
            data_ptr,
            ready_ptr,
            consumer: self,
            head,
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

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without copying.
///
/// When dropped, drops the `T` value, clears the slot's ready flag,
/// and advances the head pointer.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    ready_ptr: *const AtomicBool,
    consumer: &'a mut Consumer<T>,
    head: usize,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was checked ready=true (Acquire) in pop_ref.
        // The data is initialized and we have exclusive access via &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (ready=true was checked in pop_ref).
        // Exclusive access guaranteed by &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // SAFETY: ready_ptr points into the RingBuffer kept alive by
        // consumer's Arc.
        unsafe {
            (*self.ready_ptr).store(false, Ordering::Relaxed);
        }

        self.consumer
            .queue
            .head
            .store(self.head + 1, Ordering::Release);
    }
}
