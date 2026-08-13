use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::RingBuffer;
use crate::common::SlotSnapshot;

/// The consumer side of an MPSC ring buffer.
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
}

// SAFETY: Consumer holds exclusive read access. R is Send, RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Consumer<T, R> {}

impl<T, R: Deref<Target = RingBuffer<T>>> Consumer<T, R> {
    #[inline]
    fn ring(&self) -> &RingBuffer<T> {
        &self.queue
    }

    #[inline]
    fn has_item(&self) -> bool {
        let head = self.ring().head.load(Ordering::Relaxed);
        !matches!(
            self.ring().slot(head).classify(head),
            SlotSnapshot::NotReady
        )
    }

    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        loop {
            let head = self.ring().head.load(Ordering::Relaxed);

            let slot = self.ring().slot(head);

            let data_ptr = match slot.classify(head) {
                SlotSnapshot::Tombstoned => {
                    slot.sequence
                        .store((head + self.ring().cap) * 2, Ordering::Release);
                    self.ring().head.store(head + 1, Ordering::Release);
                    continue;
                }
                SlotSnapshot::NotReady => return None,
                SlotSnapshot::Ready(data_ptr) => data_ptr,
            };

            // SAFETY: classify() returned Ready, meaning this slot is
            // initialized and synchronized via the Acquire load inside it.
            let val = unsafe { (*data_ptr).assume_init_read() };

            slot.sequence
                .store((head + self.ring().cap) * 2, Ordering::Release);

            self.ring().head.store(head + 1, Ordering::Release);

            self.ring().producer_park.wake_one();
            #[cfg(feature = "async")]
            self.ring().wake_producer_async();

            return Some(val);
        }
    }

    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T, R>> {
        loop {
            let head = self.ring().head.load(Ordering::Relaxed);

            let slot = self.ring().slot(head);

            let data_ptr = match slot.classify(head) {
                SlotSnapshot::Tombstoned => {
                    slot.sequence
                        .store((head + self.ring().cap) * 2, Ordering::Release);
                    self.ring().head.store(head + 1, Ordering::Release);
                    continue;
                }
                SlotSnapshot::NotReady => return None,
                SlotSnapshot::Ready(data_ptr) => data_ptr,
            };

            let seq_ptr = &raw const slot.sequence;

            return Some(SlotReader {
                data_ptr,
                seq_ptr,
                consumer: self,
                head,
            });
        }
    }

    pub fn drain(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut head = self.ring().head.load(Ordering::Relaxed);
        let mut count = 0usize;

        loop {
            let slot = self.ring().slot(head);

            let data_ptr = match slot.classify(head) {
                SlotSnapshot::Tombstoned => {
                    slot.sequence
                        .store((head + self.ring().cap) * 2, Ordering::Release);
                    head += 1;
                    continue;
                }
                SlotSnapshot::NotReady => break,
                SlotSnapshot::Ready(data_ptr) => data_ptr,
            };

            // SAFETY: classify() returned Ready.
            let val = unsafe { (*data_ptr).assume_init_read() };

            slot.sequence
                .store((head + self.ring().cap) * 2, Ordering::Release);

            head += 1;
            count += 1;
            f(val);
        }

        if count > 0 {
            self.ring().head.store(head, Ordering::Release);
            self.ring().producer_park.wake_n(count);
            #[cfg(feature = "async")]
            self.ring().wake_producer_async_n(count);
        }

        count
    }

    pub fn drain_up_to(&mut self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let mut head = self.ring().head.load(Ordering::Relaxed);
        let mut count = 0usize;

        while count < limit {
            let slot = self.ring().slot(head);

            let data_ptr = match slot.classify(head) {
                SlotSnapshot::Tombstoned => {
                    slot.sequence
                        .store((head + self.ring().cap) * 2, Ordering::Release);
                    head += 1;
                    continue;
                }
                SlotSnapshot::NotReady => break,
                SlotSnapshot::Ready(data_ptr) => data_ptr,
            };

            // SAFETY: classify() returned Ready.
            let val = unsafe { (*data_ptr).assume_init_read() };

            slot.sequence
                .store((head + self.ring().cap) * 2, Ordering::Release);

            head += 1;
            count += 1;
            f(val);
        }

        if count > 0 {
            self.ring().head.store(head, Ordering::Release);
            self.ring().producer_park.wake_n(count);
            #[cfg(feature = "async")]
            self.ring().wake_producer_async_n(count);
        }

        count
    }

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

    #[must_use]
    pub fn pop_block(&mut self) -> Option<T> {
        crate::common::SingleParkerConsumer::pop_block(self)
    }

    #[cfg(feature = "async")]
    pub fn pop_async(&mut self) -> impl std::future::Future<Output = Option<T>> + '_ {
        std::future::poll_fn(move |cx| {
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            self.ring().consumer_waker.register(0, cx);
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            Poll::Pending
        })
    }

    #[must_use]
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T, R>> {
        crate::common::SingleParkerConsumerRef::pop_ref_block(self)
    }

    /// Returns `true` once the last [`Producer`](super::Producer)
    /// has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.ring().closed.0.load(Ordering::Acquire)
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
        self.ring().closed.0.load(Ordering::Acquire)
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
        self.ring().closed.0.load(Ordering::Acquire)
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
        self.ring().consumer_closed.0.store(true, Ordering::Release);
        self.ring().producer_park.flush();
        #[cfg(feature = "async")]
        self.ring().producer_waker.flush();
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
pub struct SlotReader<'a, T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    data_ptr: *const MaybeUninit<T>,
    seq_ptr: *const AtomicUsize,
    consumer: &'a mut Consumer<T, R>,
    head: usize,
}

impl<T, R: Deref<Target = RingBuffer<T>>> std::ops::Deref for SlotReader<'_, T, R> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was checked seq == head * 2 + 1 (Acquire) in pop_ref.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for SlotReader<'_, T, R> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (seq check verified in pop_ref).
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // SAFETY: seq_ptr points into the RingBuffer kept alive by
        // consumer's reference.
        let cap = self.consumer.ring().cap;
        unsafe {
            (*self.seq_ptr).store((self.head + cap) * 2, Ordering::Release);
        }

        self.consumer
            .ring()
            .head
            .store(self.head + 1, Ordering::Release);

        self.consumer.ring().producer_park.wake_one();
        #[cfg(feature = "async")]
        self.consumer.ring().wake_producer_async();
    }
}
