use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::consumer_close::ConsumerClose;
use super::done_word::DoneWord;
use super::drain_wake::DrainWake;
use super::slot_release::SlotRelease;
use super::RingBuffer;
use crate::capacity::Capacity;
use crate::common::backoff::Backoff;
#[cfg(feature = "async")]
use crate::common::park_registration::ParkRegistration;
use crate::common::park_registry::ParkSlot;

/// The consumer side of an SPMC ring buffer.
///
/// Generic over `R`: the ring reference. Defaults to `Arc<RingBuffer<T>>`
/// (owned split via [`SplitRing::split`](super::SplitRing::split)). When
/// `R = &'a RingBuffer<T>` (borrowed split via
/// [`SplitRing::split_borrowed`](super::SplitRing::split_borrowed)), `T` may carry
/// lifetimes shorter than `'static`.
///
/// Cloneable when `R = Arc<RingBuffer<T>>`. For borrowed consumers, create
/// additional handles via [`Consumer::new_consumer`].
pub struct Consumer<T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    pub(super) queue: R,
    cached_tail: Cell<usize>,
    batch_next: Cell<usize>,
    batch_end: Cell<usize>,
    park_slot: ParkSlot,
}

const BATCH_SIZE: usize = 4;

// Clone only for the Arc variant.
impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        self.queue.consumer_count_live.register();
        Self::new_with(Arc::clone(&self.queue), self.queue.consumer_slots.lease())
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send (each clone is single-
// threaded by contract). R is Send, RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Consumer<T, R> {}

impl<T> Consumer<T> {
    pub(super) fn new(queue: Arc<RingBuffer<T>>) -> Self {
        let park_slot = queue.consumer_slots.lease();
        Self::new_with(queue, park_slot)
    }
}

impl<'a, T> Consumer<T, &'a RingBuffer<T>> {
    /// Creates an additional consumer handle for the same ring.
    ///
    /// The borrowed counterpart to `Clone`, which is only available when
    /// `R = Arc`. The new handle gets its own park slot and batch
    /// cursors.
    ///
    /// This hangs off the consumer rather than the ring because
    /// [`SplitRing::split_borrowed`](super::SplitRing::split_borrowed)
    /// takes `&mut self`: while these handles are alive the ring is
    /// mutably borrowed and cannot be borrowed again. An existing handle
    /// already holds the shared reference, so it can hand out siblings
    /// with the same lifetime.
    #[must_use]
    pub fn new_consumer(&self) -> Self {
        let queue: &'a RingBuffer<T> = self.queue;
        queue.consumer_count_live.register();
        Self::new_with(queue, queue.consumer_slots.lease())
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Consumer<T, R> {
    /// The tail cache starts at the current head, which shows no
    /// backlog and so sends the first claim to the real tail. Seeding
    /// it with zero showed a consumer created in the last lap before
    /// the position rollover a backlog that had not been published.
    pub(super) fn new_with(queue: R, park_slot: ParkSlot) -> Self {
        let head = queue.cursors.head().load(Ordering::Relaxed);
        Self {
            queue,
            cached_tail: Cell::new(head),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
            park_slot,
        }
    }

    #[inline]
    fn ring(&self) -> &RingBuffer<T> {
        &self.queue
    }

    #[inline]
    fn claim_slot(&self) -> Option<(*const MaybeUninit<T>, &DoneWord, usize)> {
        let next = self.batch_next.get();
        if next != self.batch_end.get() {
            self.batch_next.set(next.wrapping_add(1));
            return Some(self.bind_pos(next));
        }

        self.claim_batch()
    }

    /// How many positions the cached tail shows ahead of `head`.
    ///
    /// Peers advance `head` past a tail this consumer cached earlier,
    /// and the gap then wraps to more than the ring holds. That reads
    /// as no backlog, so the caller goes to the real tail.
    #[inline]
    fn cached_backlog(&self, head: usize) -> usize {
        let backlog = self.cached_tail.get().wrapping_sub(head);
        if backlog > self.ring().capacity.get() {
            0
        } else {
            backlog
        }
    }

    /// Reloads the tail and returns how far it is ahead of `head`.
    ///
    /// The gap is compared as a signed distance, so it stays correct
    /// when the tail has rolled over `usize` and `head` has not.
    #[inline]
    fn refreshed_backlog(&self, head: usize) -> usize {
        let tail = self.ring().cursors.tail().load(Ordering::Acquire);
        self.cached_tail.set(tail);
        let backlog = tail.wrapping_sub(head);
        if backlog.cast_signed() < 0 {
            0
        } else {
            backlog
        }
    }

    #[cold]
    #[inline(never)]
    fn claim_batch(&self) -> Option<(*const MaybeUninit<T>, &DoneWord, usize)> {
        let q = self.ring();

        let mut backoff = Backoff::new();
        loop {
            let head = q.cursors.head().load(Ordering::Relaxed);
            let mut avail = self.cached_backlog(head);
            if avail == 0 {
                avail = self.refreshed_backlog(head);
                if avail == 0 {
                    if q.closed.is_closed() {
                        // The tail is final once the ring is closed,
                        // so an empty reading against it is conclusive.
                        let head = q.cursors.head().load(Ordering::Relaxed);
                        if self.refreshed_backlog(head) == 0 {
                            return None;
                        }
                        continue;
                    }
                    return None;
                }
            }

            let take = avail.min(BATCH_SIZE);
            let new_head = head.wrapping_add(take);
            if q.cursors
                .head()
                .compare_exchange_weak(head, new_head, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.batch_next.set(head.wrapping_add(1));
                self.batch_end.set(new_head);
                return Some(self.bind_pos(head));
            }
            backoff.spin();
        }
    }

    #[inline]
    fn has_item(&self) -> bool {
        if self.batch_next.get() != self.batch_end.get() {
            return true;
        }
        let head = self.ring().cursors.head().load(Ordering::Relaxed);
        self.cached_backlog(head) != 0 || self.refreshed_backlog(head) != 0
    }

    #[inline]
    fn bind_pos(&self, h: usize) -> (*const MaybeUninit<T>, &DoneWord, usize) {
        let q = self.ring();
        let data_ptr = q.data_slot(h).get().cast_const();
        let slot_done = q.done_slot(h);
        (data_ptr, slot_done, h)
    }

    /// Drains all currently available items, calling `f` for each.
    ///
    /// If `f` panics, the slots already freed are still announced to the
    /// producer while unwinding, so a producer blocked on a full ring
    /// does not sleep through the space this drain made.
    pub fn drain(&self, mut f: impl FnMut(T)) -> usize {
        let mut wake = DrainWake::new(self.ring());
        let mut count = 0usize;
        while let Some((data_ptr, slot_done, head)) = self.claim_slot() {
            let val = unsafe { data_ptr.cast::<T>().read() };
            slot_done.release(head, self.ring().capacity);
            wake.released();
            count += 1;
            f(val);
        }
        count
    }

    /// Drains up to `limit` available items, calling `f` for each.
    ///
    /// Panic behaviour matches [`drain`](Self::drain).
    pub fn drain_up_to(&self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let mut wake = DrainWake::new(self.ring());
        let mut count = 0usize;
        while count < limit {
            let Some((data_ptr, slot_done, head)) = self.claim_slot() else {
                break;
            };
            let val = unsafe { data_ptr.cast::<T>().read() };
            slot_done.release(head, self.ring().capacity);
            wake.released();
            count += 1;
            f(val);
        }
        count
    }

    /// Drains items, blocking when empty, until the producer drops.
    pub fn drain_block(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut total = 0usize;
        loop {
            total += self.drain(&mut f);
            match self.pop_block() {
                Some(v) => {
                    total += 1;
                    f(v);
                }
                None => return total,
            }
        }
    }

    /// Pops an item from the ring buffer.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let (data_ptr, slot_done, head) = self.claim_slot()?;

        let val = unsafe { data_ptr.cast::<T>().read() };

        slot_done.release(head, self.ring().capacity);

        self.ring().wake_producer();
        #[cfg(feature = "async")]
        self.ring().wake_producer_async();

        Some(val)
    }

    /// Pops the next item, blocking when empty.
    #[must_use]
    pub fn pop_block(&self) -> Option<T> {
        let q = self.ring();
        let slot = self.park_slot;
        let mut backoff = Backoff::new();
        loop {
            if let Some(v) = self.pop() {
                return Some(v);
            }
            if q.closed.is_closed() {
                if let Some(v) = self.pop() {
                    return Some(v);
                }
                return None;
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }

            q.consumer_park.arm(slot);
            // Pairs with WakeSet::wake_one's fence: the pop() re-check
            // below reads the slot sequence with Acquire, which the
            // SeqCst RMW above does not place in the total order.
            std::sync::atomic::fence(Ordering::SeqCst);

            if let Some(v) = self.pop() {
                q.consumer_park.stand_down(slot);
                return Some(v);
            }
            // SeqCst: post-arm half of the close handshake.
            if q.closed.is_closed_for_parking() {
                q.consumer_park.stand_down(slot);
                return self.pop();
            }

            q.consumer_park.park();
            q.consumer_park.disarm(slot);
        }
    }

    /// Pops a value asynchronously.
    #[cfg(feature = "async")]
    #[allow(clippy::future_not_send)]
    pub fn pop_async(&self) -> impl std::future::Future<Output = Option<T>> + '_ {
        let mut parked = ParkRegistration::new(&self.ring().consumer_waker, self.park_slot);
        std::future::poll_fn(move |cx| {
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().closed.is_closed() {
                return Poll::Ready(self.pop());
            }
            parked.arm(cx);
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().closed.is_closed() {
                return Poll::Ready(self.pop());
            }
            Poll::Pending
        })
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let claimed = self.claim_detached()?;
        Some(self.reader_for(claimed))
    }

    /// Claims a position, describing it without borrowing `self`.
    ///
    /// The blocking path has to tell a lost claim from an empty ring,
    /// and cannot do that through a returned reader: the reader borrows
    /// `self` for the rest of the function, so a `None` cannot be
    /// retried. Holding the claim as raw pointers keeps the loan out of
    /// the way until [`reader_for`](Self::reader_for) binds it.
    #[inline]
    fn claim_detached(&self) -> Option<ClaimedSlot<T>> {
        let (data_ptr, slot_done, head) = self.claim_slot()?;
        Some(ClaimedSlot {
            data_ptr,
            done_ptr: std::ptr::from_ref(slot_done),
            head,
        })
    }

    /// Wraps an already-claimed position in its reader.
    #[inline]
    fn reader_for(&self, claimed: ClaimedSlot<T>) -> SlotReader<'_, T> {
        claimed.into_reader(self.ring())
    }

    /// Returns a zero-copy read reference, blocking when empty.
    ///
    /// A claim is what proves ownership; `has_item` only reports that
    /// the shared head was behind the tail. With several consumers
    /// competing, a peer can take that position between the two, and
    /// returning the resulting `None` would end a blocking read on an
    /// open, non-empty ring. A lost claim is contention, not emptiness:
    /// the loop restarts.
    #[must_use]
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T>> {
        let slot = self.park_slot;
        let mut backoff = Backoff::new();
        loop {
            if self.has_item() {
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                backoff.reset();
                continue;
            }
            if self.ring().closed.is_closed() {
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                // Closed and nothing claimable: a peer took whatever
                // `has_item` saw and no producer will publish again.
                return None;
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }

            self.ring().consumer_park.arm(slot);
            // See pop_block: pairs with WakeSet::wake_one's fence.
            std::sync::atomic::fence(Ordering::SeqCst);

            if self.has_item() {
                self.ring().consumer_park.stand_down(slot);
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                backoff.reset();
                continue;
            }
            // SeqCst: post-arm half of the close handshake, paired
            // with the SeqCst fetch_or on the park bitmask above.
            if self.ring().closed.is_closed_for_parking() {
                self.ring().consumer_park.stand_down(slot);
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                return None;
            }

            self.ring().consumer_park.park();
            self.ring().consumer_park.disarm(slot);
        }
    }

    /// Returns the number of items currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ring().len()
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ring().is_empty()
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.ring().is_full()
    }

    /// Returns `true` if the producer has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.ring().closed.is_closed()
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Consumer<T, R> {
    fn drop(&mut self) {
        let q = self.ring();
        // Armed before the batch drops: `T::drop` is user code, and
        // unwinding past the retire would leave the live count high
        // forever, so the ring never reports itself closed.
        // SAFETY: the consumer's handle keeps the ring alive for the
        // whole of this destructor, and `park_slot` is ours, unreleased.
        let _close = unsafe { ConsumerClose::new(std::ptr::from_ref(q), self.park_slot) };
        let end = self.batch_end.get();
        let mut pos = self.batch_next.get();
        while pos != end {
            unsafe {
                q.data_slot(pos).get().cast::<T>().drop_in_place();
            }
            q.done_slot(pos).release(pos, q.capacity);
            pos = pos.wrapping_add(1);
        }
    }
}

/// A claimed position, held without borrowing the consumer.
///
/// The pointers name storage inside the ring, which the consumer's
/// handle keeps alive; the claim itself is proven by `claim_slot`'s
/// batch reservation.
struct ClaimedSlot<T> {
    data_ptr: *const MaybeUninit<T>,
    done_ptr: *const DoneWord,
    head: usize,
}

impl<T> ClaimedSlot<T> {
    /// Hands the claim to the ring it was made on, as the reader that
    /// will release it on drop.
    #[inline]
    const fn into_reader(self, ring: &RingBuffer<T>) -> SlotReader<'_, T> {
        SlotReader {
            data_ptr: self.data_ptr,
            done_ptr: self.done_ptr,
            head: self.head,
            capacity: ring.capacity,
            ring,
        }
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
/// The reader holds the ring, not the consumer. Its lifetime is the
/// `&mut Consumer` borrow `pop_ref` took, so the consumer cannot be
/// used while a reader is alive.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    done_ptr: *const DoneWord,
    head: usize,
    capacity: Capacity,
    ring: &'a RingBuffer<T>,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The consumer claimed this published position.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // Armed before the destructor runs: a panicking `T::drop` would
        // otherwise unwind past the release and lose the position for
        // the life of the ring.
        // SAFETY: `done_ptr` names the slot at `head`, in the ring this
        // reader borrows.
        let _release =
            unsafe { SlotRelease::new(self.ring, self.done_ptr, self.head, self.capacity) };
        // SAFETY: the consumer claimed this published position, so the
        // value is initialized and owned by this reader.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Arc;
    use crate::capacity::Capacity;
    use crate::common::staged_handoff::StagedHandoff;
    use crate::spmc::SplitRing;

    /// An empty ring of four, one producer and two consumers. The
    /// first consumer runs the real `pop_block`, held open between its
    /// pop and its stand-down by the park gate; see [`StagedHandoff`].
    ///
    /// An item is published before the first consumer arms, and its
    /// re-check takes it. The second consumer then finds the ring empty
    /// and parks. The producer publishes again, and that publish has
    /// one wake.
    struct StagedWake;

    impl StagedWake {
        /// Runs the schedule and returns whether the parked consumer
        /// was woken. `first_stands_down_first` orders the first
        /// consumer's stand-down before the second publish; without
        /// it the publish falls in the window.
        fn parked_consumer_is_woken(first_stands_down_first: bool) -> bool {
            let (producer, first) = SplitRing::<u32>::new(Capacity::exact(4)).split();
            let second = first.clone();
            let ring = Arc::clone(&first.queue);
            let handoff =
                StagedHandoff::new(&ring.consumer_park, first.park_slot, second.park_slot);

            handoff.hold_first();
            let first_done = std::thread::spawn(move || first.pop_block());
            handoff.wait_first_before_arm();
            producer.push(0).unwrap();
            handoff.let_first_arm();

            let parked = std::thread::spawn(move || second.pop_block());
            handoff.wait_second_parked();

            if first_stands_down_first {
                handoff.let_first_stand_down();
                assert_eq!(first_done.join().unwrap(), Some(0));
                producer.push(1).unwrap();
            } else {
                producer.push(1).unwrap();
                handoff.let_first_stand_down();
                assert_eq!(first_done.join().unwrap(), Some(0));
            }

            let woken = StagedHandoff::second_woke(&parked);
            if !woken {
                // Closing the ring releases the thread so the test
                // process can end.
                drop(producer);
            }
            assert_eq!(parked.join().unwrap(), Some(1));
            woken
        }
    }

    /// Control for the test below: the first consumer stands down
    /// before the publish, so the harness has to deliver the wake.
    #[test]
    fn a_publish_after_the_first_consumer_stands_down_wakes_the_parked_one() {
        assert!(StagedWake::parked_consumer_is_woken(true));
    }

    /// A consumer's wake bit stays set from its arm until it stands
    /// down, so a publish can pick a consumer whose post-arm re-check
    /// has already taken an item. That wake must still reach a
    /// consumer that is parked beside the new item.
    #[test]
    fn a_wake_spent_on_a_consumer_that_already_popped_reaches_a_parked_one() {
        assert!(StagedWake::parked_consumer_is_woken(false));
    }
}
