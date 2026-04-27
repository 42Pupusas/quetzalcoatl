use std::cell::Cell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The consumer side of an SPMC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for items
/// via batched bounded-CAS claim on the head pointer.
///
/// # Claim protocol
///
/// Each consumer maintains a private cursor over a locally-claimed batch
/// of positions. `pop()` first tries to advance the private cursor; only
/// when the batch is exhausted does it claim a fresh batch via a single
/// CAS on the shared `head`. The CAS is bounded by the producer's
/// published `tail`, so a consumer never claims a position the producer
/// has not yet reached — eliminating the post-claim wait that earlier
/// FAA-based designs spent in the spin loop.
///
/// Concretely: when our private batch is exhausted, we load `head`,
/// observe (or refresh) `tail`, take `K = min(BATCH_SIZE, tail - head)`,
/// and CAS `head` from `h` to `h + K`. On success, positions `[h, h+K)`
/// are ours to drain; on conflict we retry with backoff. Other consumers
/// can be claiming non-overlapping batches concurrently.
///
/// This amortizes the cost of head-line invalidation across `K` items
/// (typically 32) and is the principal driver of SPMC scalability under
/// many-consumer contention.
///
/// # Synchronization (split-plane)
///
/// The consumer reads `ready[s]` only when the producer is genuinely
/// behind (not used in the common batched path) and writes `done[s]` to
/// signal slot reuse. These are on **different cache lines**, so the
/// consumer's release-store does not invalidate the line the producer
/// needs to write next. The producer reads `done[s]` (a line last touched
/// by some consumer) and writes `ready[s]` (a line last touched by
/// itself, so its writes hit M-state without a coherence stall).
///
/// # Closure
///
/// If the producer is dropped, the `closed` flag is set. Consumers can
/// distinguish "transiently empty" from "permanently drained" via
/// [`is_closed`](Self::is_closed); this lets a worker exit cleanly
/// instead of spinning forever on an empty queue.
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
    /// Next position in our locally-claimed batch `[batch_next, batch_end)`.
    /// When `batch_next == batch_end`, claim a new batch via bounded FAA
    /// on `head`. Batched claim amortizes head-line invalidation across
    /// up to `BATCH_SIZE` items.
    batch_next: Cell<usize>,
    batch_end: Cell<usize>,
}

/// Maximum positions claimed in one head FAA. Larger = fewer atomics on
/// the contended head line, but greater work-imbalance between consumers
/// and longer worst-case post-claim wait if the producer has not yet
/// published all `K` positions.
const BATCH_SIZE: usize = 32;

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            cached_tail: Cell::new(0),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
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
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
        }
    }

    /// Claims the next position from our local batch, or claims a new
    /// batch from `head` via bounded CAS.
    ///
    /// Batched claim: we advance `head` by up to `BATCH_SIZE` positions
    /// in one CAS, then pop from the local cursor without touching `head`
    /// for the next K-1 calls. This amortizes head-line invalidation.
    ///
    /// Returns `(data_ptr, done_ptr, h)` on success. Returns `None` if
    /// the queue is empty (and producer not closed-and-drained).
    #[inline]
    fn claim_slot(&self) -> Option<(*const MaybeUninit<T>, *const AtomicUsize, usize)> {
        let q = &*self.queue;
        let mask = q.mask;

        // Fast path: we have positions left in our local batch.
        let next = self.batch_next.get();
        let end = self.batch_end.get();
        if next < end {
            self.batch_next.set(next + 1);
            return Some(self.bind_pos(next, mask));
        }

        // Slow path: claim a new batch from `head`.
        self.claim_batch(mask)
    }

    /// Slow path: claim a new batch. Bounded CAS on `head` clamped by
    /// `tail`, so we never claim a position the producer hasn't reached.
    /// Returns the first position of the new batch (and stashes the rest
    /// in our local cursor for subsequent `claim_slot` calls).
    #[cold]
    #[inline(never)]
    fn claim_batch(
        &self,
        mask: usize,
    ) -> Option<(*const MaybeUninit<T>, *const AtomicUsize, usize)> {
        let q = &*self.queue;

        let mut backoff = 0u32;
        loop {
            let head = q.head.load(Ordering::Relaxed);
            let mut tail = self.cached_tail.get();
            if head >= tail {
                tail = q.tail.load(Ordering::Acquire);
                self.cached_tail.set(tail);
                if head >= tail {
                    // Empty under our cached view.
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

            // Bounded batch: claim up to BATCH_SIZE positions, but never
            // past the producer's current tail.
            let avail = tail - head;
            let take = avail.min(BATCH_SIZE);
            let new_head = head + take;
            if q.head
                .compare_exchange_weak(head, new_head, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                // Batch claimed: positions [head, new_head). Use first,
                // stash rest.
                self.batch_next.set(head + 1);
                self.batch_end.set(new_head);
                return Some(self.bind_pos(head, mask));
            }
            crate::common::cas_backoff(&mut backoff);
        }
    }

    /// Resolve `(data_ptr, done_ptr, h)` for a position we have claimed.
    ///
    /// The position is guaranteed `< tail` (bounded CAS), and the
    /// producer Release-stores `ready[s] = h+1` *before* Release-storing
    /// `tail >= h+1`. Our Acquire on tail therefore happens-after the
    /// ready store, so the slot's data is already initialized and
    /// observable; no spin needed.
    #[inline]
    fn bind_pos(
        &self,
        h: usize,
        mask: usize,
    ) -> (*const MaybeUninit<T>, *const AtomicUsize, usize) {
        let q = &*self.queue;
        let s = h & mask;
        // SAFETY: `s` is always < cap by construction.
        let data_ptr = unsafe { q.data.get_unchecked(s) }.get().cast_const();
        let slot_done = unsafe { q.done.get_unchecked(s) };
        (data_ptr, &raw const slot_done.0, h)
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

    /// Returns `true` if the producer has been dropped.
    ///
    /// Once true, no more items will ever be pushed; a subsequent `pop`
    /// returning `None` is therefore terminal and signals the queue is
    /// permanently drained.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.closed.0.load(Ordering::Acquire)
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Release any positions we claimed but never popped. The producer
        // gates slot reuse on `done[s] == pos + cap`, so without this
        // release the producer would permanently stall on those slots.
        //
        // Each unconsumed slot has live data (producer published before
        // we claimed via bounded CAS), so we must drop the value too.
        let q = &*self.queue;
        let mask = q.mask;
        let cap = q.cap;
        let next = self.batch_next.get();
        let end = self.batch_end.get();
        for pos in next..end {
            let s = pos & mask;
            // SAFETY: bounded CAS guaranteed pos < tail at claim time, so
            // `ready[s] == pos+1` is observable and data is initialized.
            // We are the unique owner of this position.
            unsafe {
                q.data.get_unchecked(s).get().cast::<T>().drop_in_place();
            }
            // SAFETY: `s` < cap.
            unsafe {
                q.done
                    .get_unchecked(s)
                    .0
                    .store(pos + cap, Ordering::Release);
            }
        }
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
