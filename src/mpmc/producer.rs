use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::{Config, DefaultConfig, RingBuffer, PARK_MASK};

/// Tight-spin iterations on the primary slot's `done` before
/// falling back to bitmap scan.
const PRIMARY_SHORT_SPIN: u32 = 4;

/// Cloneable producer for an MPMC ring.
///
/// Each producer reserves a batch of logical positions from the
/// shared `claim` cursor, then publishes into them out of order —
/// using whichever batch slot's `done[s]` releases first, rather
/// than waiting on a particular slot.
pub struct Producer<T, C: Config = DefaultConfig> {
    pub(super) queue: Arc<RingBuffer<T, C>>,
    /// Start position of the current batch.
    batch_start: Cell<usize>,
    /// Bitmap of reserved-but-unpublished positions in the current
    /// batch. Bit `i` set ↔ `batch_start + i` is available to use.
    batch_unused: Cell<u32>,
    /// Original batch size (≤ `C::PRODUCER_BATCH`); diagnostic.
    batch_size: Cell<u32>,
    /// Stable park slot for this producer (mod `PARK_SLOTS`).
    park_slot: usize,
}

impl<T, C: Config> Clone for Producer<T, C> {
    fn clone(&self) -> Self {
        let n = self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            batch_start: Cell::new(0),
            batch_unused: Cell::new(0),
            batch_size: Cell::new(0),
            park_slot: (n + 1) & PARK_MASK,
        }
    }
}

// SAFETY: Cell is !Sync, but Producer is Send because each handle
// is single-threaded by contract. The Arc keeps the RingBuffer alive.
unsafe impl<T: Send, C: Config> Send for Producer<T, C> {}

impl<T, C: Config> Producer<T, C> {
    pub(super) const fn new(queue: Arc<RingBuffer<T, C>>) -> Self {
        Self {
            queue,
            batch_start: Cell::new(0),
            batch_unused: Cell::new(0),
            batch_size: Cell::new(0),
            park_slot: 0,
        }
    }

    /// Pushes a value. Returns `Err(val)` if the ring is full from
    /// this producer's perspective (approximate — based on the
    /// `consumed` watermark, which lags real consumer progress).
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let q = &*self.queue;

        // Refill the batch if exhausted.
        let mut unused = self.batch_unused.get();
        let start = if unused == 0 {
            // Bound the batch by free space estimated from the
            // (lagging) `consumed` watermark — safe to underestimate.
            let claim = q.claim.load(Ordering::Relaxed);
            let consumed = q.consumed.load(Ordering::Acquire);
            let in_flight = claim.wrapping_sub(consumed);
            let free = if in_flight >= q.cap {
                // Watermark says full; per-slot check before giving up.
                let next_done = q.done_slot(claim).load(Ordering::Acquire);
                if next_done != claim {
                    return Err(val);
                }
                1
            } else {
                q.cap - in_flight
            };
            let batch = C::PRODUCER_BATCH.min(free);

            let start = q.claim.fetch_add(batch, Ordering::Relaxed);
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

        let primary_bit = unused.trailing_zeros();
        let primary_pos = start + primary_bit as usize;
        let mut bit = primary_bit;
        let mut pos = primary_pos;

        // Short spin on the primary first — saves the bitmap-scan
        // cost when the consumer is about to release.
        let mut found_ready = false;
        for _ in 0..PRIMARY_SHORT_SPIN {
            if q.done_slot(primary_pos).load(Ordering::Acquire) == primary_pos {
                found_ready = true;
                break;
            }
            std::hint::spin_loop();
        }

        // Primary blocked? Scan other unused bits for any slot
        // already released — out-of-order publishing within batch.
        if !found_ready {
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

        // Whole batch blocked: park on the futex-style wake bitmap.
        // The SeqCst on `producer_wake.fetch_or` pairs with the
        // consumer's `producer_wake.load` after `done[s].store(Release)`
        // to close the missed-wake window; the 200μs timeout is a
        // belt-and-braces backstop.
        if !found_ready {
            ensure_handle_installed(q, self.park_slot);
            let bit_mask = 1u64 << self.park_slot;
            let mut backoff = 0u32;
            'outer: loop {
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
                // Spin until cas_backoff fully escalates (~tens of
                // μs including yields) before paying for park.
                if backoff < 12 {
                    crate::common::cas_backoff(&mut backoff);
                    continue;
                }

                q.producer_wake.fetch_or(bit_mask, Ordering::SeqCst);

                // Re-check after publishing our wake bit.
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
                    q.producer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
                    bit = b;
                    pos = p;
                    break 'outer;
                }

                std::thread::park_timeout(Duration::from_micros(200));
                q.producer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
            }
        }

        self.batch_unused.set(unused & !(1u32 << bit));

        // SAFETY: we own this position via the batch reservation,
        // and `done[s] == pos` confirmed the slot is free for our
        // round.
        let data_ptr = q.data_slot(pos).get();
        unsafe { (*data_ptr).write(val) };

        // Publish: ready[s] = pos + 1 (state = published).
        q.ready_slot(pos).store(pos + 1, Ordering::Release);

        // Wake one consumer parked in pop_block, if any. Relaxed
        // gate keeps the no-park fast path free; missed wakes are
        // bounded by the consumer's park-timeout backstop.
        if q.consumer_wake.load(Ordering::Relaxed) != 0 {
            super::wake_one_consumer(q);
        }
        Ok(())
    }

    /// Pushes a value, blocking the calling thread when the ring
    /// is full until a consumer makes space. Returns `Err(val)`
    /// only when the last [`Consumer`](super::Consumer) has been
    /// dropped (no consumer left to drain).
    ///
    /// Uses the same futex-style wake bitmap as the producer slow
    /// path: spins briefly first, then sets a wake bit and parks.
    /// Consumers signal after every `done[s].store(Release)`,
    /// gated on a single `Relaxed` load.
    pub fn push_block(&self, mut val: T) -> Result<(), T> {
        let q = &*self.queue;
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        loop {
            match self.push(val) {
                Ok(()) => return Ok(()),
                Err(returned) => val = returned,
            }
            if q.consumer_closed.0.load(Ordering::Acquire) {
                return Err(val);
            }
            // Spin until cas_backoff fully escalates (~tens of μs
            // including yields) before paying for park.
            if backoff < 12 {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            ensure_handle_installed(q, self.park_slot);
            // SeqCst pairs with consumer's `producer_wake.load`
            // after `done.store(Release)`: either we succeed in
            // the re-check below, or the consumer sees our bit
            // and unparks us.
            q.producer_wake.fetch_or(bit_mask, Ordering::SeqCst);

            match self.push(val) {
                Ok(()) => {
                    q.producer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
                    return Ok(());
                }
                Err(returned) => val = returned,
            }
            if q.consumer_closed.0.load(Ordering::Acquire) {
                q.producer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return Err(val);
            }

            std::thread::park_timeout(Duration::from_micros(200));
            q.producer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
        }
    }
}

impl<T, C: Config> Drop for Producer<T, C> {
    fn drop(&mut self) {
        // Tombstone any reserved-but-unpublished batch positions:
        // mark them as already-claimed-and-released so the
        // next-round producer can proceed. Without this, dropped
        // producers strand slots and stall the ring.
        let q = &*self.queue;
        let cap = q.cap;
        let start = self.batch_start.get();
        let mut unused = self.batch_unused.get();
        while unused != 0 {
            let bit = unused.trailing_zeros() as usize;
            let pos = start + bit;
            // Wait for previous round's consumer to release before
            // tombstoning, else we'd stomp on prior-round state.
            let done = q.done_slot(pos);
            let mut backoff = 0u32;
            while done.load(Ordering::Acquire) != pos {
                crate::common::cas_backoff(&mut backoff);
            }
            q.ready_slot(pos).store(pos + 2, Ordering::Release);
            q.done_slot(pos).store(pos + cap, Ordering::Release);
            unused &= unused - 1;
        }

        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.queue.closed.0.store(true, Ordering::Release);
            // Wake every parked producer and consumer so they can
            // observe `closed` and exit. Cold path, last-drop only.
            flush_wake_bitmap(&self.queue.producer_wake, &self.queue.producer_parkers);
            flush_wake_bitmap(&self.queue.consumer_wake, &self.queue.consumer_parkers);
        }
    }
}

/// Wakes every peer parked on the bitmap, swapping it to zero in
/// the process. Cold-path helper for close-time draining.
fn flush_wake_bitmap(
    wake: &std::sync::atomic::AtomicU64,
    parkers: &[std::sync::OnceLock<std::thread::Thread>],
) {
    let mut bits = wake.swap(0, Ordering::AcqRel);
    while bits != 0 {
        let b = bits.trailing_zeros() as usize;
        if let Some(handle) = parkers[b].get() {
            handle.unpark();
        }
        bits &= bits - 1;
    }
}

/// Idempotently installs the current thread's `Thread` handle in
/// `q.producer_parkers[slot]`. Subsequent calls observe the slot already
/// set and no-op. Slot aliasing (>`PARK_SLOTS` producers) means
/// the first installer wins; later wakes on that bit may unpark
/// the wrong producer (benign — it just re-checks and re-parks).
#[inline]
fn ensure_handle_installed<T, C: Config>(q: &RingBuffer<T, C>, slot: usize) {
    let _ = q.producer_parkers[slot].set(std::thread::current());
}
