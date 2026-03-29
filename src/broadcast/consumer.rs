use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

/// The consumer side of a broadcast ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split) or by
/// cloning an existing consumer. Each clone starts reading from the
/// current tail (only future items).
///
/// Deactivates its consumer slot on drop but does **not** drain items —
/// data is shared with other consumers.
///
/// # Panics
///
/// Cloning panics if the maximum consumer count is exceeded.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Index into `consumer_slots` — identifies this consumer's head.
    pub(super) slot_index: usize,
}

impl<T> Clone for Consumer<T> {
    /// Creates a new consumer that sees only items published after this point.
    ///
    /// # Panics
    ///
    /// Panics if the maximum consumer count is exceeded.
    fn clone(&self) -> Self {
        let tail = self.queue.tail.load(Ordering::Acquire);
        let idx = self.queue.claim_consumer_slot();
        self.queue.consumer_slots[idx]
            .head
            .store(tail, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            slot_index: idx,
        }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Deactivate this consumer's slot. Does NOT drain items —
        // data is shared with other consumers and cleaned up by
        // the producer (on overwrite) or RingBuffer (on drop).
        self.queue.consumer_slots[self.slot_index]
            .active
            .store(false, Ordering::Release);
    }
}

impl<T> Consumer<T> {
    /// Pops the next item, returning a clone.
    ///
    /// Returns `None` if the buffer is empty or the next slot hasn't
    /// been committed yet.
    ///
    /// Uses the per-slot sequence number as the sole synchronization point,
    /// avoiding a load of the contended `tail` cache line entirely.
    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T>
    where
        T: Clone,
    {
        let head = self.queue.consumer_slots[self.slot_index]
            .head
            .load(Ordering::Relaxed);

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        // The sequence number is the sole synchronization point.
        // seq == head * 2 + 1 means the producer has written data at this position.
        // The Acquire ordering synchronizes with the producer's Release store
        // on the sequence, ensuring the data write is visible.
        // Any other value means either empty or not-yet-committed.
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != head * 2 + 1 {
            return None;
        }

        // SAFETY: sequence == head * 2 + 1 synchronizes with producer's Release,
        // ensuring the data write is visible. The data won't be overwritten
        // because this consumer's head hasn't advanced (min_head blocks producer).
        let val = unsafe { (*slot.data.get()).assume_init_ref().clone() };

        // Advance this consumer's head
        self.queue.consumer_slots[self.slot_index]
            .head
            .store(head + 1, Ordering::Release);

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item.
    ///
    /// Unlike [`pop`](Self::pop), this does not clone the data. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The consumer's head
    /// is advanced when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the buffer is empty or the next slot hasn't
    /// been committed yet.
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let head = self.queue.consumer_slots[self.slot_index]
            .head
            .load(Ordering::Relaxed);

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != head * 2 + 1 {
            return None;
        }

        let data_ptr = slot.data.get().cast_const();

        Some(SlotReader {
            data_ptr,
            consumer: self,
            head,
        })
    }

    /// Returns the number of items this consumer has yet to read.
    #[must_use]
    pub fn len(&self) -> usize {
        let tail = self.queue.tail.load(Ordering::Relaxed);
        let head = self.queue.consumer_slots[self.slot_index]
            .head
            .load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// Returns `true` if this consumer has no items to read.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if this consumer's backlog has reached capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() >= self.queue.cap
    }
}

/// A zero-copy read reference to an item in the broadcast ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without cloning.
///
/// When dropped, advances the consumer's head pointer. Does **not** drop
/// the `T` value — other consumers may still need it. The producer drops
/// old values when overwriting slots.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    consumer: &'a mut Consumer<T>,
    head: usize,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was verified via sequence check (Acquire) in pop_ref.
        // The data is initialized and won't be overwritten while we hold a reference
        // (this consumer's head hasn't advanced, blocking the producer via min_head).
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // Do NOT drop the T value — other consumers may still need it.
        // Just advance this consumer's head.
        self.consumer.queue.consumer_slots[self.consumer.slot_index]
            .head
            .store(self.head + 1, Ordering::Release);
    }
}
