use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

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

        // SAFETY: `head & mask` is always < cap by construction.
        // The producer has written this slot and advanced tail with Release,
        // which we synchronized with via Acquire in `available`.
        let val = unsafe {
            (*(*self.queue.buf.get_unchecked(head & self.queue.mask)).get())
                .assume_init_read()
        };

        // Release: ensures the read above completes before head advances,
        // signaling to the producer that the slot is free.
        self.queue.head.store(head + 1, Ordering::Release);

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    ///
    /// Unlike [`pop`], this does not copy the data out. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The slot is released
    /// when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the buffer is empty.
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let head = self.queue.head.load(Ordering::Relaxed);

        if !self.available(head) {
            return None;
        }

        // SAFETY: `head & mask` is always < cap. Data is initialized
        // because tail has advanced past this slot (synchronized via Acquire).
        let data_ptr = unsafe {
            (*self.queue.buf.get_unchecked(head & self.queue.mask)).get()
                .cast_const()
        };

        Some(SlotReader {
            data_ptr,
            consumer: self,
            head,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

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
    }
}
