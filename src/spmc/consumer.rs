use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;
use crate::common::park_registry::ParkSlot;

/// The consumer side of an SPMC ring buffer.
///
/// Generic over `R`: the ring reference. Defaults to `Arc<RingBuffer<T>>`
/// (owned split via [`RingBuffer::split`]). When `R = &'a RingBuffer<T>`
/// (borrowed split via [`RingBuffer::split_borrowed`]), `T` may carry
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
        self.queue.consumer_count.fetch_add(1, Ordering::Relaxed);
        self.queue
            .consumer_count_live
            .fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            cached_tail: Cell::new(0),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
            park_slot: self.queue.consumer_slots.lease(),
        }
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send (each clone is single-
// threaded by contract). R is Send, RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Consumer<T, R> {}

impl<T> Consumer<T> {
    pub(super) fn new(queue: Arc<RingBuffer<T>>) -> Self {
        let park_slot = queue.consumer_slots.lease();
        Self {
            queue,
            cached_tail: Cell::new(0),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
            park_slot,
        }
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
    /// [`RingBuffer::split_borrowed`](super::RingBuffer::split_borrowed)
    /// takes `&mut self`: while these handles are alive the ring is
    /// mutably borrowed and cannot be borrowed again. An existing handle
    /// already holds the shared reference, so it can hand out siblings
    /// with the same lifetime.
    #[must_use]
    pub fn new_consumer(&self) -> Self {
        let queue: &'a RingBuffer<T> = self.queue;
        queue.consumer_count.fetch_add(1, Ordering::Relaxed);
        queue.consumer_count_live.fetch_add(1, Ordering::Relaxed);
        Self::new_with(queue, queue.consumer_slots.lease())
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Consumer<T, R> {
    pub(super) const fn new_with(queue: R, park_slot: ParkSlot) -> Self {
        Self {
            queue,
            cached_tail: Cell::new(0),
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
    fn claim_slot(&self) -> Option<(*const MaybeUninit<T>, &AtomicUsize, usize)> {
        let q = self.ring();
        let mask = q.mask;

        let next = self.batch_next.get();
        let end = self.batch_end.get();
        if next < end {
            self.batch_next.set(next + 1);
            return Some(self.bind_pos(next, mask));
        }

        self.claim_batch(mask)
    }

    #[cold]
    #[inline(never)]
    fn claim_batch(&self, mask: usize) -> Option<(*const MaybeUninit<T>, &AtomicUsize, usize)> {
        let q = self.ring();

        let mut backoff = 0u32;
        loop {
            let head = q.head.load(Ordering::Relaxed);
            let mut tail = self.cached_tail.get();
            if head >= tail {
                tail = q.tail.load(Ordering::Acquire);
                self.cached_tail.set(tail);
                if head >= tail {
                    if q.closed.0.load(Ordering::Acquire) {
                        let tail2 = q.tail.load(Ordering::Acquire);
                        self.cached_tail.set(tail2);
                        let head2 = q.head.load(Ordering::Relaxed);
                        if head2 >= tail2 {
                            return None;
                        }
                        continue;
                    }
                    return None;
                }
            }

            let avail = tail - head;
            let take = avail.min(BATCH_SIZE);
            let new_head = head + take;
            if q.head
                .compare_exchange_weak(head, new_head, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.batch_next.set(head + 1);
                self.batch_end.set(new_head);
                return Some(self.bind_pos(head, mask));
            }
            crate::common::cas_backoff(&mut backoff);
        }
    }

    #[inline]
    fn has_item(&self) -> bool {
        if self.batch_next.get() < self.batch_end.get() {
            return true;
        }
        let q = self.ring();
        let head = q.head.load(Ordering::Relaxed);
        if head < self.cached_tail.get() {
            return true;
        }
        let tail = q.tail.load(Ordering::Acquire);
        self.cached_tail.set(tail);
        head < tail
    }

    #[inline]
    fn bind_pos(&self, h: usize, _mask: usize) -> (*const MaybeUninit<T>, &AtomicUsize, usize) {
        let q = self.ring();
        let data_ptr = q.data_slot(h).get().cast_const();
        let slot_done = q.done_slot(h);
        (data_ptr, slot_done, h)
    }

    /// Drains all currently available items, calling `f` for each.
    pub fn drain(&self, mut f: impl FnMut(T)) -> usize {
        let mut count = 0usize;
        while let Some((data_ptr, slot_done, head)) = self.claim_slot() {
            let val = unsafe { data_ptr.cast::<T>().read() };
            slot_done.store(head + self.ring().cap, Ordering::Release);
            count += 1;
            f(val);
        }
        if count > 0 {
            self.ring().wake_producer();
            #[cfg(feature = "async")]
            self.ring().wake_producer_async();
        }
        count
    }

    /// Drains up to `limit` available items, calling `f` for each.
    pub fn drain_up_to(&self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let mut count = 0usize;
        while count < limit {
            let Some((data_ptr, slot_done, head)) = self.claim_slot() else {
                break;
            };
            let val = unsafe { data_ptr.cast::<T>().read() };
            slot_done.store(head + self.ring().cap, Ordering::Release);
            count += 1;
            f(val);
        }
        if count > 0 {
            self.ring().wake_producer();
            #[cfg(feature = "async")]
            self.ring().wake_producer_async();
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

        slot_done.store(head + self.ring().cap, Ordering::Release);

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
        let mut backoff = 0u32;
        loop {
            if let Some(v) = self.pop() {
                return Some(v);
            }
            if q.closed.0.load(Ordering::Acquire) {
                if let Some(v) = self.pop() {
                    return Some(v);
                }
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            q.consumer_park.arm(slot);
            // Pairs with WakeSet::wake_one's fence: the pop() re-check
            // below reads the slot sequence with Acquire, which the
            // SeqCst RMW above does not place in the total order.
            std::sync::atomic::fence(Ordering::SeqCst);

            if let Some(v) = self.pop() {
                q.consumer_park.disarm(slot);
                return Some(v);
            }
            // SeqCst: post-arm half of the close handshake.
            if q.closed.0.load(Ordering::SeqCst) {
                q.consumer_park.disarm(slot);
                return self.pop();
            }

            slot.park();
            q.consumer_park.disarm(slot);
        }
    }

    /// Pops a value asynchronously.
    #[cfg(feature = "async")]
    #[allow(clippy::future_not_send)]
    pub fn pop_async(&self) -> impl std::future::Future<Output = Option<T>> + '_ {
        let slot = self.park_slot;
        std::future::poll_fn(move |cx| {
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            self.ring().consumer_waker.register(slot, cx);
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            Poll::Pending
        })
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T, R>> {
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
    fn reader_for(&mut self, claimed: ClaimedSlot<T>) -> SlotReader<'_, T, R> {
        let cap = self.ring().cap;
        claimed.into_reader(self, cap)
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
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T, R>> {
        let slot = self.park_slot;
        let mut backoff = 0u32;
        loop {
            if self.has_item() {
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                backoff = 0;
                continue;
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                // Closed and nothing claimable: a peer took whatever
                // `has_item` saw and no producer will publish again.
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            self.ring().consumer_park.arm(slot);
            // See pop_block: pairs with WakeSet::wake_one's fence.
            std::sync::atomic::fence(Ordering::SeqCst);

            if self.has_item() {
                self.ring().consumer_park.disarm(slot);
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                backoff = 0;
                continue;
            }
            // SeqCst: post-arm half of the close handshake, paired
            // with the SeqCst fetch_or on the park bitmask above.
            if self.ring().closed.0.load(Ordering::SeqCst) {
                self.ring().consumer_park.disarm(slot);
                if let Some(claimed) = self.claim_detached() {
                    return Some(self.reader_for(claimed));
                }
                return None;
            }

            slot.park();
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
        self.ring().closed.0.load(Ordering::Acquire)
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Consumer<T, R> {
    fn drop(&mut self) {
        let q = self.ring();
        let cap = q.cap;
        let next = self.batch_next.get();
        let end = self.batch_end.get();
        for pos in next..end {
            unsafe {
                q.data_slot(pos).get().cast::<T>().drop_in_place();
            }
            q.done_slot(pos).store(pos + cap, Ordering::Release);
        }
        q.consumer_slots.release(self.park_slot);
        if q.consumer_count_live.fetch_sub(1, Ordering::AcqRel) == 1 {
            // SeqCst store + wake_producer's SeqCst load of
            // producer_parked are the two halves of the close
            // handshake. Reading the parker handle directly skips the
            // load and lets the producer park after we decide not to
            // wake it.
            q.consumer_closed.0.store(true, Ordering::SeqCst);
            q.wake_producer();
            #[cfg(feature = "async")]
            q.producer_waker.flush();
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
    done_ptr: *const AtomicUsize,
    head: usize,
}

impl<T> ClaimedSlot<T> {
    /// Hands the claim to the consumer that made it, as the reader that
    /// will release it on drop.
    #[inline]
    const fn into_reader<R: Deref<Target = RingBuffer<T>>>(
        self,
        consumer: &mut Consumer<T, R>,
        cap: usize,
    ) -> SlotReader<'_, T, R> {
        SlotReader {
            data_ptr: self.data_ptr,
            done_ptr: self.done_ptr,
            head: self.head,
            cap,
            consumer,
        }
    }
}

/// A zero-copy read reference to an item in the ring buffer.
pub struct SlotReader<'a, T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    data_ptr: *const MaybeUninit<T>,
    done_ptr: *const AtomicUsize,
    head: usize,
    cap: usize,
    consumer: &'a mut Consumer<T, R>,
}

impl<T, R: Deref<Target = RingBuffer<T>>> std::ops::Deref for SlotReader<'_, T, R> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The consumer claimed this published position.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for SlotReader<'_, T, R> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }
        unsafe {
            (*self.done_ptr).store(self.head + self.cap, Ordering::Release);
        }
        self.consumer.ring().wake_producer();
        #[cfg(feature = "async")]
        self.consumer.ring().wake_producer_async();
    }
}
