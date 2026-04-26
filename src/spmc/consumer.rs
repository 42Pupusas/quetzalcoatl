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
/// # Synchronization
///
/// Consumers use FAA on `head` to claim slots, after a peek at `tail`
/// confirms data is available. FAA always succeeds, so there is no
/// consumer-vs-consumer CAS retry storm. Each consumer then waits on
/// its own (cache-padded) `ready[h]` line for the producer's publish.
///
/// # Overshoot
///
/// In the rare race where multiple consumers peek "non-empty" and FAA
/// in parallel, a consumer may obtain a head ticket `h >= tail`. Since
/// the queue is lossless, the consumer must wait for the producer to
/// publish at `h` rather than abandon the ticket. This is bounded by
/// producer push latency and only occurs under high consumer contention
/// on a near-empty queue.
///
/// If the producer is dropped (sets the `closed` flag), consumers
/// spinning on overshoot return `None` rather than hanging.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local snapshot of the producer's `tail`. Avoids an Acquire-load
    /// of the producer-write line on every pop; we only re-load when
    /// our cached value says the queue is empty. The producer's stores
    /// to `tail` are monotonic, so a stale cached value is always a
    /// safe under-estimate (we may return `None` when data is actually
    /// available; the caller's next pop will refresh).
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

// SAFETY: Cell is !Sync, but Consumer is Send (single-threaded use per
// clone). The raw pointers in returned slot pointers point into the
// RingBuffer kept alive by the Arc.
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
    /// Returns `None` only if (a) the queue is observed empty before
    /// the FAA, or (b) the consumer overshot and the producer is
    /// closed (gone). Once FAA has been issued and the producer is
    /// active, the consumer waits for the publish.
    ///
    /// # Overshoot recovery
    ///
    /// If the consumer FAAs a position `h` past the current `tail`
    /// (because of racing peeks), it spin-waits on `ready[h] == h+1`.
    /// In each iteration it also re-checks `closed`; if the producer
    /// is gone *and* we're still beyond `tail`, the slot will never
    /// be filled, so we publish a "skip" marker and return `None`.
    ///
    /// The skip marker (`ready[s] = h + 1 + cap`) tells any future
    /// producer-resurrection (impossible here; producer is dropped)
    /// that the slot was abandoned, but more importantly it marks the
    /// slot as "consumed" so `RingBuffer::Drop` does not try to drop
    /// uninitialized data.
    #[inline]
    fn claim_slot(&self) -> Option<(*const MaybeUninit<T>, *const AtomicUsize, usize)> {
        let q = &*self.queue;
        let data = &q.data;
        let ready = &q.ready;
        let mask = q.mask;

        // Pre-FAA peek using the cached tail. The cached value is
        // monotonic-safe: tail only grows, so if cached_tail says
        // there's data, there really is. If cached_tail says empty,
        // we refresh from the shared atomic (one Acquire-load) before
        // returning None.
        let head = q.head.load(Ordering::Relaxed);
        let mut tail = self.cached_tail.get();
        if head >= tail {
            // Cache says empty — refresh from shared `tail`.
            tail = q.tail.load(Ordering::Acquire);
            self.cached_tail.set(tail);
            if head >= tail {
                if q.closed.0.load(Ordering::Acquire) {
                    // Producer is gone; one final tail re-check.
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

        // Wait for producer to publish at logical position h.
        // Common case (we peeked non-empty and h < tail): immediate.
        // Overshoot case: spin until producer reaches h, OR until producer
        // is closed and won't reach h.
        let mut backoff = 0u32;
        loop {
            let r = slot_ready.0.load(Ordering::Acquire);
            if r == h + 1 {
                // Data published.
                let data_ptr = unsafe { data.get_unchecked(s) }.get().cast_const();
                return Some((data_ptr, &raw const slot_ready.0, h));
            }

            // Not yet ready. Check if producer is gone and won't ever
            // publish at h.
            if q.closed.0.load(Ordering::Acquire) {
                let tail_now = q.tail.load(Ordering::Acquire);
                if h >= tail_now {
                    // Producer is gone and never reached h. Mark the
                    // slot as "consumed" for this lap by storing the
                    // post-consume value, so RingBuffer::Drop and any
                    // (impossible) future producer see a coherent state.
                    slot_ready.0.store(h + q.cap, Ordering::Release);
                    return None;
                }
                // Otherwise: producer published past h before drop;
                // ready[s] just hasn't propagated yet — keep spinning,
                // it's about to land.
            }

            crate::common::cas_backoff(&mut backoff);
        }
    }

    /// Pops an item from the ring buffer.
    ///
    /// Multiple consumers can pop concurrently. Uses FAA on `head` —
    /// no consumer-vs-consumer CAS contention.
    ///
    /// Returns `None` if the queue is empty *and* either the producer
    /// is currently inactive or has been dropped. May briefly wait
    /// (microseconds) if a racing peek causes consumer overshoot.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let (data_ptr, ready_ptr, head) = self.claim_slot()?;

        // SAFETY: ready[s] == head+1 was verified, so data is initialized
        // and exclusively ours (FAA gave us a unique head ticket).
        let val = unsafe { data_ptr.cast::<T>().read() };

        // Release: ready[s] = head + cap means "free for producer at
        // logical position head + cap" (next lap of this slot).
        // SAFETY: ready_ptr points into the RingBuffer kept alive by Arc.
        unsafe {
            (*ready_ptr).store(head + self.queue.cap, Ordering::Release);
        }

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let (data_ptr, ready_ptr, head) = self.claim_slot()?;

        Some(SlotReader {
            data_ptr,
            ready_ptr,
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
    ready_ptr: *const AtomicUsize,
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
        // SAFETY: ready_ptr points into the RingBuffer kept alive by
        // consumer's Arc.
        unsafe {
            (*self.ready_ptr).store(self.head + self.cap, Ordering::Release);
        }
    }
}
