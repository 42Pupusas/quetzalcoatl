use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The consumer side of an MPSC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one consumer exists per buffer.
///
/// Drains remaining items when dropped.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
}

impl<T> Consumer<T> {
    /// Returns None if the queue is empty or if a slot has been claimed
    /// by a producer but not yet written (non-blocking behavior).
    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        let head = self.queue.head.load(Ordering::Relaxed);

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        // The sequence number is the sole synchronization point. It subsumes
        // the tail check: seq == head * 2 + 1 means a producer has both claimed
        // AND written the slot. Any other value means either empty or
        // claimed-but-not-written — both cases require returning None.
        // This avoids loading the contended tail cache line, which producers
        // are hammering with CAS.
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != head * 2 + 1 {
            return None;
        }

        // SAFETY: We checked that the slot is ready
        let val = unsafe { (*slot.data.get()).assume_init_read() };

        // Release the slot: set sequence to (head + cap) * 2 so producers
        // know this slot is free for reuse.
        slot.sequence
            .store((head + self.queue.cap) * 2, Ordering::Release);

        // Advance head
        self.queue
            .head
            .store(head + 1, Ordering::Release);

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    ///
    /// Unlike [`pop`](Self::pop), this does not copy the data out. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The slot is released
    /// when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the queue is empty or the next slot is not yet
    /// committed (same semantics as `pop`).
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let head = self.queue.head.load(Ordering::Relaxed);

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        // Same as pop(): sequence number is the sole synchronization point.
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != head * 2 + 1 {
            return None;
        }

        // SAFETY: seq == head * 2 + 1 guarantees the slot has been initialized.
        let data_ptr = slot.data.get().cast_const();
        let seq_ptr = &raw const slot.sequence;

        Some(SlotReader {
            data_ptr,
            seq_ptr,
            consumer: self,
            head,
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
/// When dropped, drops the `T` value, releases the slot by updating
/// the sequence number, and advances the head pointer.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    seq_ptr: *const AtomicUsize,
    consumer: &'a mut Consumer<T>,
    head: usize,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was checked seq == head * 2 + 1 (Acquire) in pop_ref.
        // The data is initialized and we have exclusive access via &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (seq check verified in pop_ref).
        // Exclusive access guaranteed by &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // Release the slot so producers can reuse it.
        // SAFETY: seq_ptr points into the RingBuffer kept alive by
        // consumer's Arc.
        let cap = self.consumer.queue.cap;
        unsafe {
            (*self.seq_ptr).store((self.head + cap) * 2, Ordering::Release);
        }

        self.consumer
            .queue
            .head
            .store(self.head + 1, Ordering::Release);
    }
}
