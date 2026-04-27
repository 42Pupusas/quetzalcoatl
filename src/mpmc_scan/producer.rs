use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

/// Maximum positions reserved per FAA on the shared `claim` cursor.
/// Larger = fewer cacheline transfers on the contended `claim` line,
/// at the cost of "imbalanced" producer progress (a slow producer
/// holding a partially-used batch keeps its positions out of reach of
/// other producers). 32 is a balance point.
const PRODUCER_BATCH: usize = 32;

/// Producer for the relaxed-FIFO scan-based MPMC ring.
///
/// Cloneable. Each producer reserves a *batch* of logical positions
/// from the shared `claim` cursor in one FAA, then publishes into
/// them from a private cursor without touching `claim` again until
/// the batch is exhausted. This amortizes the contended FAA on
/// `claim` across `PRODUCER_BATCH` items.
///
/// Batch sizing is bounded by available ring space — computed from
/// the consumers' `consumed` watermark — so a single FAA never
/// claims more positions than can be drained without the producer
/// blocking indefinitely. The watermark lags real consumer progress
/// (consumers flush their local count infrequently), so producers
/// underestimate free space — safe direction; just smaller batches
/// when the queue is near-full.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Next position in the producer's locally-reserved batch.
    next_pos: Cell<usize>,
    /// One-past-end of the producer's locally-reserved batch.
    /// `next_pos == batch_end` means the batch is exhausted; the
    /// next `push` will FAA a new batch from `claim`.
    batch_end: Cell<usize>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            next_pos: Cell::new(0),
            batch_end: Cell::new(0),
        }
    }
}

// SAFETY: Cell is !Sync, but Producer is Send (each clone is single-
// threaded by contract). The queue Arc keeps the RingBuffer alive.
unsafe impl<T: Send> Send for Producer<T> {}

impl<T> Producer<T> {
    pub(super) const fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            next_pos: Cell::new(0),
            batch_end: Cell::new(0),
        }
    }

    /// Pushes a value. Returns `Err(val)` if the ring is (approximately)
    /// full from this producer's perspective — i.e., reserving even one
    /// more position would exceed the consumer-released watermark by
    /// more than `cap`. The check is approximate because `consumed` lags
    /// real consumer progress; it errs on the side of returning Err
    /// earlier than strictly necessary, never later.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let q = &*self.queue;

        let next = self.next_pos.get();
        let end = self.batch_end.get();
        let pos = if next < end {
            // Fast path: position from local batch.
            self.next_pos.set(next + 1);
            next
        } else {
            // Slow path: reserve a new batch.
            //
            // Bound the batch by available free space:
            //   in_flight = claim - consumed   (loose upper bound)
            //   free      = cap - in_flight    (loose lower bound)
            //   batch     = min(PRODUCER_BATCH, free)
            //
            // If `free == 0`, the ring is (loosely) full from our
            // POV — return Err. With monotonic claim and consumed,
            // and consumed lagging real progress, `in_flight` is
            // never *less* than the true count of in-flight items,
            // so `free` is never *more* than the true free count.
            let claim = q.claim.load(Ordering::Relaxed);
            let consumed = q.consumed.load(Ordering::Acquire);
            let in_flight = claim.wrapping_sub(consumed);
            let free = if in_flight >= q.cap {
                // The `consumed` watermark says the ring is full, but
                // `consumed` lags real consumer progress (consumers
                // only flush every CONSUMED_FLUSH pops). Fall back to
                // the per-slot check used by the strict-FIFO MPMC:
                // peek `done[claim & mask]`. If it equals `claim`,
                // the previous round's consumer has actually
                // released the slot and we can claim 1 position
                // even though `consumed` hasn't been updated yet.
                let next_done = q.done_slot(claim).load(Ordering::Acquire);
                if next_done != claim {
                    return Err(val);
                }
                1
            } else {
                q.cap - in_flight
            };
            let batch = PRODUCER_BATCH.min(free);

            let start = q.claim.fetch_add(batch, Ordering::Relaxed);
            self.next_pos.set(start + 1);
            self.batch_end.set(start + batch);
            start
        };

        // Wait for the previous round's consumer (logical pos -
        // cap) to release this slot. Different producers spin on
        // different slots — no cross-producer contention.
        let done = q.done_slot(pos);
        let mut backoff = 0u32;
        while done.load(Ordering::Acquire) != pos {
            crate::common::cas_backoff(&mut backoff);
        }

        // SAFETY: we are the unique owner of `pos` via the batch
        // reservation, and `done[s] == pos` confirms the slot is
        // free for our round.
        let data_ptr = q.data_slot(pos).get();
        unsafe { (*data_ptr).write(val) };

        // Publish: ready[s] = pos + 1 → "published, available."
        q.ready_slot(pos).store(pos + 1, Ordering::Release);
        Ok(())
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Release any positions reserved in our local batch but never
        // pushed. Without this, the slots leak: consumers scan and
        // never see them in state==1 (we never published), the
        // next-round producer waits on `done[s] == pos` forever
        // (consumer never released because there was nothing to
        // release), and items pushed to higher positions become
        // permanently unreachable.
        //
        // For each leaked position, we mark the slot as if a
        // consumer had claimed-and-released it: ready[s] = pos + 2
        // (claimed marker, so future scanners skip it as state==2)
        // and done[s] = pos + cap (so the next-round producer can
        // proceed). Consumers reading these slots see state==2 and
        // skip — no data was written, so there's nothing to drop.
        let q = &*self.queue;
        let cap = q.cap;
        let next = self.next_pos.get();
        let end = self.batch_end.get();
        for pos in next..end {
            // Wait for previous round's consumer to release this
            // slot (so done[s] == pos and ready[s] == pos initial
            // state). Without this wait, we'd stomp on a state
            // belonging to the previous round.
            let done = q.done_slot(pos);
            let mut backoff = 0u32;
            while done.load(Ordering::Acquire) != pos {
                crate::common::cas_backoff(&mut backoff);
            }
            // Now we own the slot. Mark "claimed" then "released."
            q.ready_slot(pos).store(pos + 2, Ordering::Release);
            q.done_slot(pos).store(pos + cap, Ordering::Release);
        }

        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.queue.closed.0.store(true, Ordering::Release);
        }
    }
}
