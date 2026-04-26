use std::cell::Cell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The consumer side of an SPMC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for items
/// via **fetch-and-add** on the head pointer.
///
/// # Synchronization (split-plane)
///
/// The consumer reads `ready[s]` to wait for the producer's publish and
/// writes `done[s]` to signal slot reuse. These are on **different cache
/// lines**, so the consumer's release-store does not invalidate the line
/// the producer needs to write next. The producer reads `done[s]` (a line
/// last touched by some consumer) and writes `ready[s]` (a line last
/// touched by itself, so its writes hit M-state without a coherence stall).
///
/// # Overshoot
///
/// In the rare race where multiple consumers peek "non-empty" and FAA
/// in parallel, a consumer may obtain a head ticket `h >= tail`. Since
/// the queue is lossless, the consumer must wait for the producer to
/// publish at `h` rather than abandon the ticket. This is bounded by
/// producer push latency.
///
/// If the producer is dropped (sets the `closed` flag), consumers
/// spinning on overshoot return `None` rather than hanging.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local snapshot of the producer's `tail`. Avoids an Acquire-load
    /// of the producer-write line on every pop; we only re-load when
    /// our cached value says the queue is empty. Monotonic-safe: tail
    /// only grows, so a stale cached value is always a safe under-
    /// estimate (we may briefly return `None` when data is actually
    /// available; the caller's next pop refreshes).
    ///
    /// `Cell` makes Consumer `!Sync` — share via `clone()` instead.
    cached_tail: Cell<usize>,
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            cached_tail: Cell::new(0),
        }
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send (each clone is single-
// threaded by contract). The queue Arc keeps the RingBuffer alive.
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Consumer<T> {
    pub(super) fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            cached_tail: Cell::new(0),
        }
    }

    /// Claims the next slot via FAA on `head`, then waits for the
    /// producer to publish data at the claimed position.
    ///
    /// Returns `(data_ptr, done_ptr, h)` on success. Returns `None`
    /// if the queue was observed empty before the FAA, or if the
    /// consumer overshot and the producer is closed.
    #[inline]
    fn claim_slot(&self) -> Option<(*const MaybeUninit<T>, *const AtomicUsize, usize)> {
        let q = &*self.queue;
        let data = &q.data;
        let ready = &q.ready;
        let done = &q.done;
        let mask = q.mask;

        // Pre-FAA peek using the cached tail.
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
                } else {
                    return None;
                }
            }
        }

        // Commit: FAA gives us a unique seqid. Always succeeds.
        let h = q.head.fetch_add(1, Ordering::Relaxed);
        let s = h & mask;
        // SAFETY: `s` is always < cap by construction.
        let slot_ready = unsafe { ready.get_unchecked(s) };
        let slot_done = unsafe { done.get_unchecked(s) };

        // Wait for the producer to publish at logical position h.
        // ready[s] == h + 1 means data is there. Common case (we peeked
        // non-empty and h < tail): immediate. Overshoot case: spin until
        // producer reaches h, OR until producer is closed and won't.
        let mut backoff = 0u32;
        loop {
            if slot_ready.0.load(Ordering::Acquire) == h + 1 {
                let data_ptr = unsafe { data.get_unchecked(s) }.get().cast_const();
                return Some((data_ptr, &raw const slot_done.0, h));
            }

            if q.closed.0.load(Ordering::Acquire) {
                let tail_now = q.tail.load(Ordering::Acquire);
                if h >= tail_now {
                    // Producer is gone and never reached h. Mark slot
                    // consumed via `done` so RingBuffer::Drop sees the
                    // slot as released (no double-drop, no leak).
                    slot_done.0.store(h + q.cap, Ordering::Release);
                    return None;
                }
            }

            crate::common::cas_backoff(&mut backoff);
        }
    }

    /// Pops an item from the ring buffer.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let (data_ptr, done_ptr, head) = self.claim_slot()?;

        // SAFETY: ready[s] == head+1 was verified, so data is initialized
        // and exclusively ours (FAA gave us a unique head ticket).
        let val = unsafe { data_ptr.cast::<T>().read() };

        // Release the slot via the consumer-write `done` line. The
        // producer at logical position head + cap will Acquire-load
        // done[s] == head + cap and proceed to overwrite the slot.
        // SAFETY: done_ptr points into the RingBuffer kept alive by Arc.
        unsafe {
            (*done_ptr).store(head + self.queue.cap, Ordering::Release);
        }

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let (data_ptr, done_ptr, head) = self.claim_slot()?;

        Some(SlotReader {
            data_ptr,
            done_ptr,
            head,
            cap: self.queue.cap,
            _consumer: self,
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

/// A zero-copy read reference to an item in the ring buffer.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    done_ptr: *const AtomicUsize,
    head: usize,
    cap: usize,
    _consumer: &'a mut Consumer<T>,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: ready[s] == head+1 was verified before construction.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: Value is initialized; we have exclusive access via FAA
        // claim + &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }
        // Release via the `done` line. SAFETY: done_ptr points into the
        // RingBuffer kept alive by consumer's Arc.
        unsafe {
            (*self.done_ptr).store(self.head + self.cap, Ordering::Release);
        }
    }
}
