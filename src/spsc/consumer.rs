use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;
#[cfg(feature = "async")]
use std::task::Poll;

/// The consumer side of an SPSC ring buffer.
///
/// Generic over `R`: the ring reference. Defaults to `Arc<RingBuffer<T>>`
/// (owned split via [`RingBuffer::split`]). When `R = &'a RingBuffer<T>`
/// (borrowed split via [`RingBuffer::split_borrowed`]), `T` may carry
/// lifetimes shorter than `'static`.
///
/// Not cloneable — only one consumer exists per buffer.
///
/// Drains remaining items when dropped.
pub struct Consumer<T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    pub(super) queue: R,
    pub(super) cached_tail: std::cell::Cell<usize>,
}

// SAFETY: Consumer holds exclusive read access. The Cell field is only
// touched by the single consumer thread. R is Send, and RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Consumer<T, R> {}

impl<T, R: Deref<Target = RingBuffer<T>>> Consumer<T, R> {
    #[inline]
    fn ring(&self) -> &RingBuffer<T> {
        &self.queue
    }

    /// Returns the current head if an item is available, or `None` if empty.
    /// Uses `cached_tail` as a fast path to avoid cross-cache-line atomic loads.
    #[inline]
    fn available(&self, head: usize) -> bool {
        if head == self.cached_tail.get() {
            // Cached tail says empty — refresh from the real atomic.
            // Acquire: synchronizes with producer's Release on tail,
            // ensuring we see the data written before tail advanced.
            let tail = self.ring().tail.load(Ordering::Acquire);
            self.cached_tail.set(tail);

            head != tail
        } else {
            true
        }
    }

    /// Pops the next item from the ring buffer.
    ///
    /// Returns `None` if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        let head = self.ring().head.load(Ordering::Relaxed);

        if !self.available(head) {
            return None;
        }

        // SAFETY: producer wrote this slot and advanced tail with Release,
        // synchronized with our Acquire in `available`.
        let val = unsafe { (*self.ring().slot(head).get()).assume_init_read() };

        // SeqCst: pairs with the producer's SeqCst store of
        // `producer_parked = true` in `push_block` to close the missed-
        // wakeup race; subsumes the Release semantics for slot reuse.
        self.ring().head.store(head + 1, Ordering::SeqCst);

        self.ring().wake_producer();
        #[cfg(feature = "async")]
        self.ring().wake_producer_async();

        Some(val)
    }

    /// Drains all currently available items, calling `f` for each
    /// one. Returns the number drained.
    ///
    /// Amortizes the `head` pointer update: individual reads happen
    /// in sequence but the shared `head` is written **once** at the
    /// end of the batch, reducing cache-line invalidation traffic
    /// between consumer and producer from O(n) to O(1). The single
    /// post-batch `wake_producer` is sufficient because spsc has
    /// only one producer.
    pub fn drain(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut head = self.ring().head.load(Ordering::Relaxed);
        let mut count = 0usize;
        loop {
            if !self.available(head) {
                break;
            }
            // SAFETY: available(head) returned true → tail > head with
            // Acquire, so the slot at `head` is initialized.
            let val = unsafe { (*self.ring().slot(head).get()).assume_init_read() };
            head += 1;
            count += 1;
            f(val);
        }
        if count > 0 {
            // SeqCst: see Consumer::pop. Pairs with the producer's SeqCst
            // `parked.store(true)` to close the missed-wakeup race.
            self.ring().head.store(head, Ordering::SeqCst);
            self.ring().wake_producer();
            #[cfg(feature = "async")]
            self.ring().wake_producer_async();
        }
        count
    }

    /// Drains up to `limit` items, calling `f` for each. Returns the
    /// number drained. Useful for fairness in multi-source consumer
    /// loops.
    pub fn drain_up_to(&mut self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let mut head = self.ring().head.load(Ordering::Relaxed);
        let mut count = 0usize;
        while count < limit {
            if !self.available(head) {
                break;
            }
            let val = unsafe { (*self.ring().slot(head).get()).assume_init_read() };
            head += 1;
            count += 1;
            f(val);
        }
        if count > 0 {
            // SeqCst: see Consumer::pop.
            self.ring().head.store(head, Ordering::SeqCst);
            self.ring().wake_producer();
            #[cfg(feature = "async")]
            self.ring().wake_producer_async();
        }
        count
    }

    /// Drains items, blocking when empty, until the producer drops.
    /// Calls `f` for each item drained. Returns the total count.
    ///
    /// Combines [`drain`](Self::drain)'s amortized head update with
    /// [`pop_block`](Self::pop_block)'s park-on-empty protocol. Use
    /// this for a long-running consumer loop that wants throughput
    /// (drain) and CPU efficiency on idle (block).
    pub fn drain_block(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut total = 0usize;
        loop {
            total += self.drain(&mut f);
            // After the drain, either wait for more items or exit on
            // producer-closed (with a final drain to catch any
            // late-published items).
            match self.pop_block() {
                Some(v) => {
                    total += 1;
                    f(v);
                }
                None => return total,
            }
        }
    }

    /// Pops the next item, blocking the calling thread when the ring
    /// is empty until a producer publishes one. Returns `None` only
    /// when the [`Producer`](super::Producer) has been dropped AND
    /// the ring has drained.
    ///
    /// Spins via the shared backoff schedule first, then registers
    /// the consumer parker handle and parks. The producer signals
    /// after every push, gated on a single `Relaxed` load.
    #[must_use]
    pub fn pop_block(&mut self) -> Option<T> {
        crate::common::SingleParkerConsumer::pop_block(self)
    }

    /// Pops a value asynchronously, yielding to the executor when the
    /// ring is empty until the producer pushes one. Returns `None` when
    /// the producer has been dropped and the ring has drained.
    ///
    /// The future is cancel-safe: dropping it before completion does not
    /// consume any item.
    #[cfg(feature = "async")]
    pub fn pop_async(&mut self) -> impl std::future::Future<Output = Option<T>> + '_ {
        std::future::poll_fn(move |cx| {
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().producer_closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            // Register waker then re-check to close the lost-wake race.
            self.ring().consumer_waker.register(0, cx);
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().producer_closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            Poll::Pending
        })
    }

    /// Returns `true` once the [`Producer`](super::Producer) has been
    /// dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.ring().producer_closed.0.load(Ordering::Acquire)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    ///
    /// Unlike [`pop`](Self::pop), this does not copy the data out. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The slot is released
    /// when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T, R>> {
        let head = self.ring().head.load(Ordering::Relaxed);

        if !self.available(head) {
            return None;
        }

        // Data is initialized — tail advanced past this slot, synchronized
        // via Acquire in `available`.
        let data_ptr = self.ring().slot(head).get().cast_const();

        Some(SlotReader {
            data_ptr,
            consumer: self,
            head,
        })
    }

    /// Non-mutating peek: returns true if a `pop_ref` (or `pop`)
    /// would currently succeed. Used by `pop_ref_block` as the
    /// pre-park gate — calling `pop_ref` itself for the gate would
    /// construct a `SlotReader` that, when dropped, advances head
    /// and consumes the item we wanted to return.
    #[inline]
    fn has_item(&self) -> bool {
        let head = self.ring().head.load(Ordering::Relaxed);
        self.available(head)
    }

    /// Returns a zero-copy read reference to the next item, blocking
    /// the calling thread when the ring is empty until a producer
    /// publishes one. Returns `None` only when the
    /// [`Producer`](super::Producer) has been dropped AND the ring
    /// has drained.
    ///
    /// Same wait protocol as [`pop_block`](Self::pop_block). Useful
    /// when you want zero-copy reads plus blocking; otherwise prefer
    /// [`pop_ref`](Self::pop_ref) for non-blocking or
    /// [`pop_block`](Self::pop_block) for value-copy reads.
    #[must_use]
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T, R>> {
        crate::common::SingleParkerConsumerRef::pop_ref_block(self)
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
}

impl<T, R: Deref<Target = RingBuffer<T>>> crate::common::SingleParkerConsumer<T>
    for Consumer<T, R>
{
    fn try_pop(&mut self) -> Option<T> {
        self.pop()
    }
    fn producer_gone(&self) -> bool {
        // SeqCst, not Acquire: this load is the second half of the
        // close handshake and must sit in the same total order as the
        // closer's producer_closed store and its consumer_parked load.
        self.ring().producer_closed.0.load(Ordering::SeqCst)
    }
    fn arm_park(&self) {
        // SeqCst pairs with the producer's `consumer_parked.load` after
        // `tail.store(Release)`: either our re-check sees the published
        // slot, or the producer sees our flag and unparks us.
        let _ = self.ring().consumer_parker.set(std::thread::current());
        self.ring().consumer_parked.0.store(true, Ordering::SeqCst);
    }
    fn disarm_park(&self) {
        self.ring()
            .consumer_parked
            .0
            .store(false, Ordering::Relaxed);
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> crate::common::SingleParkerConsumerRef
    for Consumer<T, R>
{
    type Reader<'a>
        = SlotReader<'a, T, R>
    where
        Self: 'a;
    fn has_item(&self) -> bool {
        self.has_item()
    }
    fn try_pop_ref(&mut self) -> Option<SlotReader<'_, T, R>> {
        self.pop_ref()
    }
    fn producer_gone(&self) -> bool {
        // SeqCst: see the SingleParkerConsumer impl above.
        self.ring().producer_closed.0.load(Ordering::SeqCst)
    }
    fn arm_park(&self) {
        let _ = self.ring().consumer_parker.set(std::thread::current());
        self.ring().consumer_parked.0.store(true, Ordering::SeqCst);
    }
    fn disarm_park(&self) {
        self.ring()
            .consumer_parked
            .0
            .store(false, Ordering::Relaxed);
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Consumer<T, R> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
        // Symmetric to Producer::drop.
        self.ring().consumer_closed.0.store(true, Ordering::SeqCst);
        self.ring().wake_producer();
        #[cfg(feature = "async")]
        self.ring().producer_waker.flush();
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without copying.
///
/// When dropped, drops the `T` value and advances the head pointer.
pub struct SlotReader<'a, T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    data_ptr: *const MaybeUninit<T>,
    consumer: &'a mut Consumer<T, R>,
    head: usize,
}

impl<T, R: Deref<Target = RingBuffer<T>>> std::ops::Deref for SlotReader<'_, T, R> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was verified available (tail > head with Acquire)
        // in pop_ref. The data is initialized and we have exclusive access
        // via &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for SlotReader<'_, T, R> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }
        // SeqCst: see Consumer::pop.
        self.consumer
            .ring()
            .head
            .store(self.head + 1, Ordering::SeqCst);
        self.consumer.ring().wake_producer();
        #[cfg(feature = "async")]
        self.consumer.ring().wake_producer_async();
    }
}
