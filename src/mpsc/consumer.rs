use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;
use crate::common::TOMBSTONE;

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
        let seq = self.ring().slot(head).sequence.load(Ordering::Acquire);
        seq == head * 2 + 1 || seq == TOMBSTONE
    }

    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        loop {
            let head = self.ring().head.load(Ordering::Relaxed);

            let slot = self.ring().slot(head);

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                slot.sequence
                    .store((head + self.ring().cap) * 2, Ordering::Release);
                self.ring().head.store(head + 1, Ordering::Release);
                continue;
            }

            if seq != head * 2 + 1 {
                return None;
            }

            // SAFETY: We checked that the slot is ready
            let val = unsafe { (*slot.data.get()).assume_init_read() };

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

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                slot.sequence
                    .store((head + self.ring().cap) * 2, Ordering::Release);
                self.ring().head.store(head + 1, Ordering::Release);
                continue;
            }

            if seq != head * 2 + 1 {
                return None;
            }

            // SAFETY: seq == head * 2 + 1 guarantees the slot has been initialized.
            let data_ptr = slot.data.get().cast_const();
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

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                slot.sequence
                    .store((head + self.ring().cap) * 2, Ordering::Release);
                head += 1;
                continue;
            }

            if seq != head * 2 + 1 {
                break;
            }

            // SAFETY: We checked that the slot is ready
            let val = unsafe { (*slot.data.get()).assume_init_read() };

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

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                slot.sequence
                    .store((head + self.ring().cap) * 2, Ordering::Release);
                head += 1;
                continue;
            }

            if seq != head * 2 + 1 {
                break;
            }

            let val = unsafe { (*slot.data.get()).assume_init_read() };

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
        let mut backoff = 0u32;
        loop {
            if let Some(v) = self.pop() {
                return Some(v);
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                if let Some(v) = self.pop() {
                    return Some(v);
                }
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            let _ = self.ring().consumer_parker.set(std::thread::current());
            self.ring().consumer_parked.0.store(true, Ordering::SeqCst);

            if let Some(v) = self.pop() {
                self.ring()
                    .consumer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                return Some(v);
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                self.ring()
                    .consumer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                return self.pop();
            }

            std::thread::park();
            self.ring()
                .consumer_parked
                .0
                .store(false, Ordering::Relaxed);
        }
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
        let mut backoff = 0u32;
        loop {
            if self.has_item() {
                return self.pop_ref();
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                if self.has_item() {
                    return self.pop_ref();
                }
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            let _ = self.ring().consumer_parker.set(std::thread::current());
            self.ring().consumer_parked.0.store(true, Ordering::SeqCst);

            if self.has_item() {
                self.ring()
                    .consumer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                return self.pop_ref();
            }
            if self.ring().closed.0.load(Ordering::Acquire) {
                self.ring()
                    .consumer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                if self.has_item() {
                    return self.pop_ref();
                }
                return None;
            }

            std::thread::park();
            self.ring()
                .consumer_parked
                .0
                .store(false, Ordering::Relaxed);
        }
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
