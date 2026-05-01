use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;

/// The consumer side of an SPSC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one consumer exists per buffer.
///
/// Drains remaining items when dropped.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Cached snapshot of `tail` to avoid cross-cache-line reads on every pop.
    /// Since `tail` only ever increases, a stale value is safe — it just makes
    /// the buffer appear emptier than it is. We re-fetch only when needed.
    pub(super) cached_tail: std::cell::Cell<usize>,
}

impl<T> Consumer<T> {
    /// Returns the current head if an item is available, or `None` if empty.
    /// Uses `cached_tail` as a fast path to avoid cross-cache-line atomic loads.
    #[inline]
    fn available(&self, head: usize) -> bool {
        if head == self.cached_tail.get() {
            // Cached tail says empty — refresh from the real atomic.
            // Acquire: synchronizes with producer's Release on tail,
            // ensuring we see the data written before tail advanced.
            let tail = self.queue.tail.load(Ordering::Acquire);
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
        let head = self.queue.head.load(Ordering::Relaxed);

        if !self.available(head) {
            return None;
        }

        // SAFETY: producer wrote this slot and advanced tail with Release,
        // synchronized with our Acquire in `available`.
        let val = unsafe { (*self.queue.slot(head).get()).assume_init_read() };

        // Release: ensures the read above completes before head advances,
        // signaling to the producer that the slot is free.
        self.queue.head.store(head + 1, Ordering::Release);

        // Wake the producer if it's parked in push_block. Relaxed gate
        // keeps the no-park hot path free.
        self.queue.wake_producer();

        Some(val)
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
        let mut backoff = 0u32;
        loop {
            if let Some(v) = self.pop() {
                return Some(v);
            }
            if self.queue.producer_closed.0.load(Ordering::Acquire) {
                // Re-check after observing closed: producer may have
                // published just before its drop, and we must drain
                // that before returning None.
                if let Some(v) = self.pop() {
                    return Some(v);
                }
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            let _ = self.queue.consumer_parker.set(std::thread::current());
            // SeqCst pairs with the producer's `consumer_parked.load`
            // after `tail.store(Release)`: either we see the published
            // slot in the re-check below, or the producer sees our flag
            // and unparks us.
            self.queue.consumer_parked.0.store(true, Ordering::SeqCst);

            if let Some(v) = self.pop() {
                self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
                return Some(v);
            }
            if self.queue.producer_closed.0.load(Ordering::Acquire) {
                self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
                return self.pop();
            }

            std::thread::park();
            self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
        }
    }

    /// Returns `true` once the [`Producer`](super::Producer) has been
    /// dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.producer_closed.0.load(Ordering::Acquire)
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
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let head = self.queue.head.load(Ordering::Relaxed);

        if !self.available(head) {
            return None;
        }

        // Data is initialized — tail advanced past this slot, synchronized
        // via Acquire in `available`.
        let data_ptr = self.queue.slot(head).get().cast_const();

        Some(SlotReader {
            data_ptr,
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
        // Mark consumer-closed so a parked producer in push_block can
        // observe it and return Err(val).
        self.queue.consumer_closed.0.store(true, Ordering::Release);
        // Wake the producer if it's parked.
        if let Some(handle) = self.queue.producer_parker.get() {
            handle.unpark();
        }
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without copying.
///
/// When dropped, drops the `T` value and advances the head pointer.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    consumer: &'a mut Consumer<T>,
    head: usize,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was verified available (tail > head with Acquire)
        // in pop_ref. The data is initialized and we have exclusive access
        // via &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (verified in pop_ref).
        // Exclusive access guaranteed by &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // Release: ensures the read completes before head advances.
        self.consumer
            .queue
            .head
            .store(self.head + 1, Ordering::Release);
        self.consumer.queue.wake_producer();
    }
}
