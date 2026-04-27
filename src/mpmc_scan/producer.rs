use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

/// Maximum positions reserved per FAA on the shared `claim` cursor.
/// Larger = fewer cacheline transfers on the contended `claim` line,
/// at the cost of "imbalanced" producer progress (a slow producer
/// holding a partially-used batch keeps its positions out of reach of
/// other producers). 32 is also the bit-width of the `batch_unused`
/// bitmap below.
const PRODUCER_BATCH: usize = 32;

/// On the primary slot's `done` not being ready, how many tight-spin
/// iterations before falling back to scanning other unused bits in
/// the batch for a ready slot. Short enough to not waste cycles when
/// the primary is about to release; long enough to amortize the
/// bitmap-scan cost.
const PRIMARY_SHORT_SPIN: u32 = 4;

/// Producer for the relaxed-FIFO scan-based MPMC ring.
///
/// Cloneable. Each producer reserves a *batch* of logical positions
/// from the shared `claim` cursor in one FAA, then publishes into
/// them out-of-order: on each `push`, the producer scans its batch
/// for any position whose `done[s]` is already the value the
/// previous-round consumer released to, and uses that one. This
/// trades within-producer FIFO (which the relaxed-FIFO design
/// already breaks across producers) for converting blocking-spin
/// time into productive publishing time when one slot is held up.
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
    /// Start of current batch (logical position). Positions in this
    /// batch are `[batch_start, batch_start + popcnt(batch_unused) +
    /// used)` — but more usefully, position `batch_start + i` is
    /// available iff bit `i` of `batch_unused` is set, for `i in
    /// 0..PRODUCER_BATCH`.
    batch_start: Cell<usize>,
    /// Bitmap of unused positions in the current batch. Bit `i` set =
    /// position `batch_start + i` is reserved-but-unwritten and
    /// available for `push` to use. When zero, the batch is exhausted
    /// and the next `push` will FAA a new batch from `claim`.
    batch_unused: Cell<u32>,
    /// Number of positions in the current batch (≤ `PRODUCER_BATCH`).
    /// Tracked for diagnostics; the live state of the batch is in
    /// `batch_unused`.
    batch_size: Cell<u32>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            batch_start: Cell::new(0),
            batch_unused: Cell::new(0),
            batch_size: Cell::new(0),
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
            batch_start: Cell::new(0),
            batch_unused: Cell::new(0),
            batch_size: Cell::new(0),
        }
    }

    /// Pushes a value. Returns `Err(val)` if the ring is (approximately)
    /// full from this producer's perspective.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let q = &*self.queue;

        // Refill the batch if exhausted.
        let mut unused = self.batch_unused.get();
        let start = if unused == 0 {
            // Slow path: reserve a new batch from `claim`.
            //
            // Bound the batch by available free space:
            //   in_flight = claim - consumed   (loose upper bound)
            //   free      = cap - in_flight    (loose lower bound)
            //   batch     = min(PRODUCER_BATCH, free)
            //
            // The `consumed` watermark lags real consumer progress, so
            // we underestimate free space. Safe direction: we may
            // reserve smaller batches than possible, never larger.
            let claim = q.claim.load(Ordering::Relaxed);
            let consumed = q.consumed.load(Ordering::Acquire);
            let in_flight = claim.wrapping_sub(consumed);
            let free = if in_flight >= q.cap {
                // Watermark says full; fall back to the per-slot
                // check from the strict-FIFO MPMC.
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
            // Set bits 0..batch in batch_unused. `batch >= 1` always.
            unused = if batch >= 32 {
                u32::MAX
            } else {
                #[allow(clippy::cast_possible_truncation)]
                let b = batch as u32;
                (1u32 << b) - 1
            };
            self.batch_start.set(start);
            self.batch_unused.set(unused);
            #[allow(clippy::cast_possible_truncation)]
            self.batch_size.set(batch as u32);
            start
        } else {
            self.batch_start.get()
        };

        // Pick the lowest-index unused position. Spin briefly on its
        // `done`; if it's still held after a few iterations, try
        // other unused positions in our batch — one of them might be
        // ready while this one is stuck behind a slow consumer.
        // (Out-of-order publishing within a batch.)
        let primary_bit = unused.trailing_zeros();
        let primary_pos = start + primary_bit as usize;
        let mut bit = primary_bit;
        let mut pos = primary_pos;

        // First: a short spin on the primary. If consumer is just
        // about to release, we save the cost of the bitmap scan.
        let mut found_ready = false;
        for _ in 0..PRIMARY_SHORT_SPIN {
            if q.done_slot(primary_pos).load(Ordering::Acquire) == primary_pos {
                found_ready = true;
                break;
            }
            std::hint::spin_loop();
        }

        if !found_ready {
            // Primary still blocked. Scan other unused bits for one
            // whose slot IS ready right now.
            let mut bits = unused & !(1u32 << primary_bit);
            while bits != 0 {
                let b = bits.trailing_zeros();
                let p = start + b as usize;
                if q.done_slot(p).load(Ordering::Acquire) == p {
                    bit = b;
                    pos = p;
                    found_ready = true;
                    break;
                }
                bits &= bits - 1;
            }
        }

        if !found_ready {
            // No ready slot anywhere; keep spinning on the primary
            // with exponential backoff.
            let done = q.done_slot(primary_pos);
            let mut backoff = 0u32;
            while done.load(Ordering::Acquire) != primary_pos {
                crate::common::cas_backoff(&mut backoff);
            }
            // Use the primary.
            bit = primary_bit;
            pos = primary_pos;
        }

        // Clear the chosen bit — this position is now consumed-by-us.
        self.batch_unused.set(unused & !(1u32 << bit));

        // SAFETY: we own this position (batch reservation), and
        // `done[s] == pos` confirms the slot is free for our round.
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
        // pushed (the bits still set in `batch_unused`). Without this,
        // the slots leak: consumers scan and never see them in
        // state==1 (we never published), the next-round producer
        // waits on `done[s] == pos` forever, and items pushed to
        // higher positions become permanently unreachable.
        //
        // For each leaked position, we mark the slot as if a
        // consumer had claimed-and-released it: ready[s] = pos + 2
        // (state==2, consumer scans skip it) and done[s] = pos + cap
        // (so the next-round producer can proceed). No data was
        // written, so there's nothing to drop.
        let q = &*self.queue;
        let cap = q.cap;
        let start = self.batch_start.get();
        let mut unused = self.batch_unused.get();
        while unused != 0 {
            let bit = unused.trailing_zeros() as usize;
            let pos = start + bit;
            // Wait for previous round's consumer to release this
            // slot. Without this wait, we'd stomp on state belonging
            // to the previous round.
            let done = q.done_slot(pos);
            let mut backoff = 0u32;
            while done.load(Ordering::Acquire) != pos {
                crate::common::cas_backoff(&mut backoff);
            }
            // Mark "claimed" then "released."
            q.ready_slot(pos).store(pos + 2, Ordering::Release);
            q.done_slot(pos).store(pos + cap, Ordering::Release);
            unused &= unused - 1;
        }

        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.queue.closed.0.store(true, Ordering::Release);
        }
    }
}
