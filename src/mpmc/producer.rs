use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::{RingBuffer, PARK_MASK};

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
    /// Park slot index in `RingBuffer::wake_state` / `parkers`.
    /// Stable for this producer's lifetime — assigned at clone time
    /// from the queue's `clone_counter`. Beyond `PARK_SLOTS = 64`
    /// producers, slots alias and a wake on the shared bit rouses
    /// every producer mapped there (correct, slightly wasteful).
    park_slot: usize,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        let n = self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            batch_start: Cell::new(0),
            batch_unused: Cell::new(0),
            batch_size: Cell::new(0),
            // Each clone gets its own park slot. `producer_count`
            // is monotonically incremented at clone time and never
            // decremented, so successive clones get distinct
            // values; mod PARK_SLOTS keeps it within the bitmap.
            park_slot: (n + 1) & PARK_MASK,
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
            // The original Producer (made by split()) owns slot 0.
            park_slot: 0,
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
            // No slot in the batch is ready. Park-with-rescan on
            // the futex-style wake bitmap: on each pass, re-check
            // every unused bit in the batch (any consumer release
            // unblocks any of them); if still no luck, set our
            // wake_state bit and park.
            //
            // Why park: under p+q saturating the CPU (e.g. p=q=8
            // on 16 logical cores), pure spin loops steal SMT
            // decode bandwidth from the very consumers we're
            // waiting on. Park lets the kernel hand the core to
            // the consumer immediately.
            //
            // Why rescan-all: any slot in our batch is an
            // interchangeable publish target — whichever consumer
            // releases first wins. Sticking on the primary alone
            // wastes producer time when sibling slots free up
            // first.
            //
            // Missed-wake protection: we set the wake_state bit
            // BEFORE re-checking `done[s]` (with SeqCst pairing
            // against the consumer's done-store + wake_state load).
            // If a consumer signaled between our last check and
            // bit-set, the consumer's load sees our bit and
            // unparks us; if not, we see done released here and
            // skip the park. A 200μs timeout backstop covers any
            // edge case.
            let mut backoff = 0u32;
            // Lazily install our park handle on first park (kept
            // for the producer's lifetime, dropped in RingBuffer
            // ::drop).
            ensure_handle_installed(q, self.park_slot);
            let bit_mask = 1u64 << self.park_slot;

            'outer: loop {
                // Cheap rescan first — short-circuit before any
                // park machinery if anything is ready.
                let mut bits = unused;
                while bits != 0 {
                    let b = bits.trailing_zeros();
                    let p = start + b as usize;
                    if q.done_slot(p).load(Ordering::Acquire) == p {
                        bit = b;
                        pos = p;
                        break 'outer;
                    }
                    bits &= bits - 1;
                }
                // Spin-with-backoff before paying for the park
                // round-trip. cas_backoff saturates at f=12; we
                // park only once it has fully escalated (yields
                // included), meaning we've already waited ~tens
                // of microseconds and the kernel has had a chance
                // to reschedule. At that point a futex round-trip
                // (1-2μs) is amortized.
                if backoff < 12 {
                    crate::common::cas_backoff(&mut backoff);
                    continue;
                }

                // Set our wake bit (SeqCst gives the StoreLoad
                // barrier needed against consumers' done-store +
                // wake_state load; either we see done released in
                // the re-check below, or they see our bit).
                q.wake_state.fetch_or(bit_mask, Ordering::SeqCst);

                // Re-check `done[s]` for all unused bits. If
                // ready now, retract our wake bit and proceed.
                let mut bits = unused;
                let mut found = None;
                while bits != 0 {
                    let b = bits.trailing_zeros();
                    let p = start + b as usize;
                    if q.done_slot(p).load(Ordering::Acquire) == p {
                        found = Some((b, p));
                        break;
                    }
                    bits &= bits - 1;
                }
                if let Some((b, p)) = found {
                    q.wake_state.fetch_and(!bit_mask, Ordering::Relaxed);
                    bit = b;
                    pos = p;
                    break 'outer;
                }

                // Park with a 200μs safety-net timeout. Even if a
                // wake is somehow lost (impossible with the SeqCst
                // pairing above, but defensive against future
                // refactors), the timeout caps wait latency.
                std::thread::park_timeout(Duration::from_micros(200));

                // Clear our bit on wake. Either consumer cleared
                // it when waking us (fetch_and returns prev with
                // bit clear) or we clear it ourselves (timeout
                // path); both are safe.
                q.wake_state.fetch_and(!bit_mask, Ordering::Relaxed);
                // Loop and re-check.
            }
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
            // Wake any siblings still parked in their push slow
            // path so they can observe `closed` and exit cleanly.
            // Cheap — only runs at the last-producer drop.
            let ws = self.queue.wake_state.swap(0, Ordering::AcqRel);
            let mut bits = ws;
            while bits != 0 {
                let b = bits.trailing_zeros() as usize;
                if let Some(handle) = self.queue.parkers[b].get() {
                    handle.unpark();
                }
                bits &= bits - 1;
            }
        }
    }
}

/// Install this producer's `Thread` handle in `q.parkers[slot]` if
/// it isn't there yet. The handle is set once (idempotent
/// `OnceLock::set`) and stays for the lifetime of the queue;
/// consumers obtain `&Thread` references via `OnceLock::get`,
/// avoiding any unsafe pointer plumbing.
///
/// First-park-only: subsequent calls (by this or any other
/// producer mapped to the same slot) observe `OnceLock` already
/// initialized and no-op. With more than `PARK_SLOTS` producers,
/// slot aliasing means a wake unparks the *first installer* — a
/// possibly-wrong producer for the wakeup, which is a benign
/// false wake (the woken producer just re-checks and re-parks).
#[inline]
fn ensure_handle_installed<T>(q: &RingBuffer<T>, slot: usize) {
    // OnceLock::set is itself idempotent and atomic; calling it
    // unconditionally is fine. The Err return on already-set is
    // discarded.
    let _ = q.parkers[slot].set(std::thread::current());
}
