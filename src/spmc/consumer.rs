use std::cell::Cell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::park::{BACKOFF_PARK_THRESHOLD, PARK_MASK};

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
    /// Stable park slot for this consumer (mod `PARK_SLOTS`). Used by
    /// `pop_block` to set/clear its bit on `consumer_park.wake`.
    park_slot: usize,
}

/// Maximum positions claimed in one head FAA. Larger = fewer atomics on
/// the contended head line, but greater work-imbalance between consumers
/// and longer worst-case post-claim wait if the producer has not yet
/// published all `K` positions.
const BATCH_SIZE: usize = 32;

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        // Distinct park slot per clone, mod PARK_SLOTS.
        let park_idx = self
            .queue
            .consumer_count
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        // Bump the live-consumer count so the producer can detect
        // "all consumers gone" via consumer_closed.
        self.queue
            .consumer_count_live
            .fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            cached_tail: Cell::new(0),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
            park_slot: park_idx & PARK_MASK,
        }
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send (each clone is single-
// threaded by contract). The queue Arc keeps the RingBuffer alive.
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Consumer<T> {
    pub(super) const fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            cached_tail: Cell::new(0),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
            park_slot: 0,
        }
    }

    /// Claims the next position from our local batch, or claims a new
    /// batch from `head` via bounded CAS.
    ///
    /// Batched claim: we advance `head` by up to `BATCH_SIZE` positions
    /// in one CAS, then pop from the local cursor without touching `head`
    /// for the next K-1 calls. This amortizes head-line invalidation.
    ///
    /// Returns `(data_ptr, done_ref, h)` on success. Returns `None` if
    /// the queue is empty (and producer not closed-and-drained).
    #[inline]
    fn claim_slot(&self) -> Option<(*const MaybeUninit<T>, &AtomicUsize, usize)> {
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
    fn claim_batch(&self, mask: usize) -> Option<(*const MaybeUninit<T>, &AtomicUsize, usize)> {
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

    /// Non-mutating check: returns true if `pop`/`pop_ref` would
    /// currently succeed. Used by `pop_ref_block` as the pre-park
    /// gate so we don't construct a `SlotReader` we'd have to drop
    /// (which would release the slot via `done`, consuming the item
    /// we wanted to return).
    #[inline]
    fn has_item(&self) -> bool {
        // Local batch has positions left.
        if self.batch_next.get() < self.batch_end.get() {
            return true;
        }
        let q = &*self.queue;
        let head = q.head.load(Ordering::Relaxed);
        // Fast cached check.
        if head < self.cached_tail.get() {
            return true;
        }
        // Refresh tail; queue may have advanced.
        let tail = q.tail.load(Ordering::Acquire);
        self.cached_tail.set(tail);
        head < tail
    }

    /// Resolve `(data_ptr, done_ptr, h)` for a position we have claimed.
    ///
    /// The position is guaranteed `< tail` (bounded CAS), and the
    /// producer Release-stores `ready[s] = h+1` *before* Release-storing
    /// `tail >= h+1`. Our Acquire on tail therefore happens-after the
    /// ready store, so the slot's data is already initialized and
    /// observable; no spin needed.
    #[inline]
    fn bind_pos(&self, h: usize, _mask: usize) -> (*const MaybeUninit<T>, &AtomicUsize, usize) {
        let q = &*self.queue;
        let data_ptr = q.data_slot(h).get().cast_const();
        let slot_done = q.done_slot(h);
        (data_ptr, &slot_done.0, h)
    }

    /// Pops an item from the ring buffer.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let (data_ptr, slot_done, head) = self.claim_slot()?;

        // SAFETY: ready[s] == head+1 was verified, so data is initialized
        // and exclusively ours (FAA gave us a unique head ticket).
        let val = unsafe { data_ptr.cast::<T>().read() };

        // Release the slot via the consumer-write `done` line. The
        // producer at logical position head + cap will Acquire-load
        // done[s] == head + cap and proceed to overwrite the slot.
        slot_done.store(head + self.queue.cap, Ordering::Release);

        // Wake the producer if it's parked in push_block. Self-gated
        // on a Relaxed load of `producer_parked`.
        self.queue.wake_producer();

        Some(val)
    }

    /// Pops the next item, blocking the calling thread when the ring
    /// is empty until a producer publishes one. Returns `None` only
    /// when the [`Producer`](super::Producer) has been dropped AND
    /// the ring has drained.
    ///
    /// Spins via the shared backoff schedule first, then sets a wake
    /// bit on the consumer wake bitmap and parks. The producer signals
    /// after every push, gated on a single `Relaxed` load.
    #[must_use]
    pub fn pop_block(&self) -> Option<T> {
        let q = &*self.queue;
        let bit_mask = 1u64 << self.park_slot;
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

            q.consumer_park.ensure_handle_installed(self.park_slot);
            // SeqCst pairs with the producer's `consumer_park.wake.load`
            // after `ready.store(Release)`.
            q.consumer_park.wake.fetch_or(bit_mask, Ordering::SeqCst);

            if let Some(v) = self.pop() {
                q.consumer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return Some(v);
            }
            if q.closed.0.load(Ordering::Acquire) {
                q.consumer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return self.pop();
            }

            std::thread::park();
            q.consumer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
        }
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let (data_ptr, slot_done, head) = self.claim_slot()?;
        // Cast to raw pointer because SlotReader is self-referential
        // (holds &mut Consumer alongside this borrow into the same Arc).
        let done_ptr: *const AtomicUsize = slot_done;

        Some(SlotReader {
            data_ptr,
            done_ptr,
            head,
            cap: self.queue.cap,
            consumer: self,
        })
    }

    /// Returns a zero-copy read reference to the next item, blocking
    /// the calling thread when the ring is empty until a producer
    /// publishes one. Returns `None` only when the
    /// [`Producer`](super::Producer) has been dropped AND the ring
    /// has drained.
    ///
    /// Same wait protocol as [`pop_block`](Self::pop_block). Useful
    /// when you want zero-copy reads plus blocking.
    #[must_use]
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T>> {
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        loop {
            // Non-mutating gate: avoid constructing-then-dropping
            // a SlotReader (which would release the slot's done
            // store and consume the item we wanted to return).
            if self.has_item() {
                return self.pop_ref();
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                if self.has_item() {
                    return self.pop_ref();
                }
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            self.queue
                .consumer_park
                .ensure_handle_installed(self.park_slot);
            self.queue
                .consumer_park
                .wake
                .fetch_or(bit_mask, Ordering::SeqCst);

            if self.has_item() {
                self.queue
                    .consumer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                return self.pop_ref();
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                self.queue
                    .consumer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                if self.has_item() {
                    return self.pop_ref();
                }
                return None;
            }

            std::thread::park();
            self.queue
                .consumer_park
                .wake
                .fetch_and(!bit_mask, Ordering::Relaxed);
        }
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
        let cap = q.cap;
        let next = self.batch_next.get();
        let end = self.batch_end.get();
        for pos in next..end {
            // SAFETY: bounded CAS guaranteed pos < tail at claim time, so
            // `ready[s] == pos+1` is observable and data is initialized.
            // We are the unique owner of this position.
            unsafe {
                q.data_slot(pos).get().cast::<T>().drop_in_place();
            }
            q.done_slot(pos).0.store(pos + cap, Ordering::Release);
        }
        // Last-consumer drop: flag `consumer_closed` and wake the
        // producer if it's parked in push_block.
        if q.consumer_count_live.fetch_sub(1, Ordering::AcqRel) == 1 {
            q.consumer_closed.0.store(true, Ordering::Release);
            if let Some(handle) = q.producer_parker.get() {
                handle.unpark();
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
    consumer: &'a mut Consumer<T>,
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
        self.consumer.queue.wake_producer();
    }
}
