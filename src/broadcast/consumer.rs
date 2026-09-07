use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::head_advance::HeadAdvance;
use super::RingBuffer;
#[cfg(feature = "async")]
use crate::common::park_registration::{ParkSite, ParkedFuture};
#[cfg(feature = "async")]
use crate::common::park_registry::ParkSlot;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::SlotSnapshot;

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
        Self {
            queue: Arc::clone(&self.queue),
            slot_index: self.queue.consumers.subscribe_at(tail),
        }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Deactivate this consumer's slot. Does NOT drain items —
        // data is shared with other consumers and cleaned up by
        // the producer (on overwrite) or RingBuffer (on drop).
        self.queue.consumers.unsubscribe(self.slot_index);
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
    /// Publishes this consumer's head past `head` and notifies the
    /// producers.
    ///
    /// Every path that moves the head runs through here — delivering a
    /// value, skipping an abandoned position, or dropping a reader.
    /// Each raises this consumer's contribution to the floor, so each is
    /// a capacity-releasing event a blocked producer must learn about.
    #[inline]
    fn advance_head(&self, head: usize) {
        self.queue.consumers.publish_head_past(self.slot_index, head);
        self.queue.wake_producer();
        self.queue.notify_producers();
    }

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
            let head = self.queue.consumers.head_of(self.slot_index);

            let slot = self.queue.slot(head);

            let data_ptr = match slot.classify(head) {
                SlotSnapshot::Tombstoned => {
                    // Abandoned slot — skip it by advancing this
                    // consumer's head. That frees the position for the
                    // producers, so it is published and notified like
                    // any other head advance.
                    self.advance_head(head);
                    continue;
                }
                SlotSnapshot::NotReady => return None,
                SlotSnapshot::Ready(data_ptr) => data_ptr,
            };

            // Armed before the clone: `T::clone` is user code, and
            // unwinding past the advance would leave this consumer
            // rereading the position forever.
            let _advance = HeadAdvance::new(&self.queue, self.slot_index, head);

            // SAFETY: classify() returned Ready, which synchronizes with the
            // producer's Release, ensuring the data write is visible. The
            // data won't be overwritten because this consumer's head hasn't
            // advanced (min_head blocks the producer).
            let val = unsafe { (*data_ptr).assume_init_ref().clone() };

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
        let slot = ParkSlot::from_exclusive_index(self.slot_index);
        ParkedFuture::new(self, slot, |this, parker, cx| {
            if let Some(v) = this.pop() {
                return Poll::Ready(Some(v));
            }
            if this.queue.closed.is_closed() {
                return Poll::Ready(this.pop());
            }
            parker.arm(&this.queue.consumer_waker, cx);
            // SeqCst fence after register: pairs with the producer drop's
            // SeqCst store of `closed`, ensuring our re-check observes any
            // close that happened after our pre-register load. Without
            // this, an x86 store buffer can leave a window where the
            // close is invisible across our register, the flush has
            // already passed our slot, and we park forever.
            std::sync::atomic::fence(Ordering::SeqCst);
            if let Some(v) = this.pop() {
                return Poll::Ready(Some(v));
            }
            if this.queue.closed.is_closed_for_parking() {
                return Poll::Ready(this.pop());
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
            let head = self.queue.consumers.head_of(self.slot_index);

            let slot = self.queue.slot(head);

            let data_ptr = match slot.classify(head) {
                SlotSnapshot::Tombstoned => {
                    // Abandoned slot — skip it, releasing the position.
                    self.advance_head(head);
                    continue;
                }
                SlotSnapshot::NotReady => return None,
                SlotSnapshot::Ready(data_ptr) => data_ptr,
            };

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
        tail.wrapping_sub(self.queue.consumers.head_of(self.slot_index))
    }

    /// Returns `true` if this consumer has no items to read.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if this consumer's backlog has reached capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() >= self.queue.capacity.get()
    }
}

/// A zero-copy read reference to an item in the broadcast ring buffer.
///
#[cfg(feature = "async")]
impl<T> ParkSite for Consumer<T> {
    fn park_set(&mut self) -> &WakerSet {
        &self.queue.consumer_waker
    }
}

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
        self.consumer.advance_head(self.head);
    }
}
