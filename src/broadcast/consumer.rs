use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::RingBuffer;
use crate::common::TOMBSTONE;

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
        // Flush blocking producers parked in push_block/reserve_block.
        // Going inactive raises `min_head` (this consumer no longer
        // pins a slot) and may have been the last consumer — either way
        // every parked producer must re-check `any_consumer_active`
        // / `min_head` so it can make progress or return Err.
        self.queue.producer_park.flush();
        // Flush producers parked on push_async so they can re-check
        // any_consumer_active() and either retry or return Err.
        #[cfg(feature = "async")]
        self.queue.producer_waker.flush();
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
    /// Automatically skips tombstoned slots (abandoned reservations).
    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T>
    where
        T: Clone,
    {
        loop {
            let head = self.queue.consumer_slots[self.slot_index]
                .head
                .load(Ordering::Relaxed);

            let slot = self.queue.slot(head);

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                // Abandoned slot — skip it by advancing this consumer's head.
                self.queue.consumer_slots[self.slot_index]
                    .head
                    .store(head + 1, Ordering::Release);
                continue;
            }

            // The sequence number is the sole synchronization point.
            // seq == head * 2 + 1 means the producer has written data at this position.
            // Any other value means either empty or not-yet-committed.
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

            self.queue.wake_producer();
            #[cfg(feature = "async")]
            self.queue.wake_producer_async();

            return Some(val);
        }
    }

    /// Pops a value asynchronously, yielding to the executor when this
    /// consumer's backlog is empty until a producer publishes one.
    /// Returns `None` once all producers have dropped and this
    /// consumer's backlog has drained.
    ///
    /// The future is cancel-safe: dropping it before completion does
    /// not consume any item.
    #[cfg(feature = "async")]
    #[allow(clippy::future_not_send)]
    pub fn pop_async(&mut self) -> impl std::future::Future<Output = Option<T>> + '_
    where
        T: Clone,
    {
        let slot = self.slot_index;
        std::future::poll_fn(move |cx| {
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            self.queue.consumer_waker.register(slot, cx);
            // SeqCst fence after register: pairs with the producer drop's
            // SeqCst store of `closed`, ensuring our re-check observes any
            // close that happened after our pre-register load. Without
            // this, an x86 store buffer can leave a window where the
            // close is invisible across our register, the flush has
            // already passed our slot, and we park forever.
            std::sync::atomic::fence(Ordering::SeqCst);
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            Poll::Pending
        })
    }

    /// Returns a zero-copy read reference to the next item.
    ///
    /// Unlike [`pop`](Self::pop), this does not clone the data. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The consumer's head
    /// is advanced when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the buffer is empty or the next slot hasn't
    /// been committed yet.
    ///
    /// Automatically skips tombstoned slots (abandoned reservations).
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        loop {
            let head = self.queue.consumer_slots[self.slot_index]
                .head
                .load(Ordering::Relaxed);

            let slot = self.queue.slot(head);

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                // Abandoned slot — skip it.
                self.queue.consumer_slots[self.slot_index]
                    .head
                    .store(head + 1, Ordering::Release);
                continue;
            }

            if seq != head * 2 + 1 {
                return None;
            }

            let data_ptr = slot.data.get().cast_const();

            return Some(SlotReader {
                data_ptr,
                consumer: self,
                head,
            });
        }
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
        self.consumer.queue.wake_producer();
        #[cfg(feature = "async")]
        self.consumer.queue.wake_producer_async();
    }
}
