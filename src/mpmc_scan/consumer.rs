use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

/// Consumer for the relaxed-FIFO scan-based MPMC ring.
///
/// Each consumer keeps a private `next_scan` cursor (its guess at where
/// to look next). On `pop`, the consumer reads `ready[s]` at the
/// current scan position; if "published," it CASes the slot to the
/// "claimed" state, reads the value, and releases via `done[s]`.
/// Otherwise it scans forward, bounded by the producer's `claim`
/// cursor.
///
/// No shared `head` cursor exists. Consumers contend per-slot, not on
/// a global atomic, so contention is naturally distributed across the
/// ring's `cap` cachelines.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Private scan cursor. Logical position to start the next scan
    /// from. On each `pop`, the consumer walks at most `cap` slots
    /// forward from here, looking for a published slot it can claim.
    next_scan: Cell<usize>,
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            // New consumer starts scanning from 0 (logical position).
            // We previously tried `claim` (most recent producer
            // cursor) to stagger consumers, but that creates a fatal
            // hazard: if any slot's previous round was published but
            // never claimed (because all existing consumers had
            // already scanned past it), the new consumer starting at
            // `claim` *also* skips it, and the slot becomes
            // permanently stranded — the next-round producer waits on
            // `done[s] == pos` forever.
            //
            // Starting at 0 means the new consumer scans through the
            // whole ring once (cheap: at most `cap` slot reads with no
            // contention — most slots are claimed-state and skipped
            // instantly), picking up any orphans, then naturally
            // drifts forward. The cost is paid once per Clone.
            next_scan: Cell::new(0),
        }
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send (each clone is single-
// threaded). The queue Arc keeps the RingBuffer alive.
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Consumer<T> {
    pub(super) const fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            next_scan: Cell::new(0),
        }
    }

    /// Pops an item via scan. Returns the first published slot found at
    /// or after `next_scan`, bounded by the producer's claim cursor.
    ///
    /// **Order:** within a single consumer's stream, items are returned
    /// in the order their slots become published. Since multi-producer
    /// publishing is out-of-order with claim order, the per-consumer
    /// stream is *not* strict-FIFO of the original push order. Across
    /// consumers, ordering is best-effort (each consumer follows its
    /// own scan).
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let q = &*self.queue;
        let cap = q.cap;
        let mask = q.mask;

        let start_scan = self.next_scan.get();
        let mut scan = start_scan;
        // Walk at most `cap` distinct slot indices before declaring
        // empty. Any published-unclaimed item lives at exactly one
        // slot index `pos & mask`; one full lap of slots is sufficient
        // to find it. We may revisit a slot index if `start_scan`
        // happens to wrap past `claim`, but that's bounded.
        let max_iters = cap;
        let mut iters = 0usize;

        loop {
            // Read the slot. Decode (state, round_pos) from the value.
            let s = scan & mask;
            let r = q.ready_slot(scan).load(Ordering::Acquire);
            let delta = r.wrapping_sub(s);
            // `delta % cap` — `cap` is power-of-two so equivalent to
            // `& mask`. Compiler can't infer this from the runtime
            // `cap`; the explicit `& mask` saves a `div`.
            let state = delta & mask;
            let round_pos = r.wrapping_sub(state); // s + R*cap

            if state == 1 {
                // Published. Try to claim. Note: we no longer require
                // `round_pos <= scan` — we'll claim any published slot
                // we find. Producer-side ordering already ensures the
                // round in `ready[s]` corresponds to a real produced
                // item.
                let claimed_marker = round_pos + 2;
                if q.ready_slot(scan)
                    .compare_exchange(r, claimed_marker, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    // SAFETY: producer wrote slot `s` for logical
                    // position `round_pos`. Our successful CAS gives us
                    // unique ownership.
                    let val = unsafe { q.data_slot(scan).get().cast::<T>().read() };
                    // Release: done[s] = round_pos + cap → "free for
                    // the next round at logical pos round_pos + cap."
                    q.done_slot(scan).store(round_pos + cap, Ordering::Release);
                    // Advance scan to just past the consumed slot's
                    // round. We use round_pos + 1 (rather than
                    // scan + 1) so a successful claim of a "future"
                    // slot doesn't make us skip back; max() with
                    // start_scan + 1 keeps us moving forward.
                    self.next_scan.set((round_pos + 1).max(start_scan + 1));
                    return Some(val);
                }
                // CAS failed: another consumer claimed it. Move on.
            }

            scan += 1;
            iters += 1;
            if iters >= max_iters {
                // Walked through all `cap` slots; no published item
                // found. Persist scan position so we resume from here
                // next time.
                self.next_scan.set(scan);
                return None;
            }
        }
    }

    /// Best-effort length: producers' claim cursor minus a coarse
    /// estimate of consumed positions. Not precise.
    #[must_use]
    pub fn approx_len(&self) -> usize {
        let claim = self.queue.claim.load(Ordering::Relaxed);
        // We don't track total consumed; approximate by cap as upper
        // bound. Honest answer: this design doesn't have a cheap len.
        let scan = self.next_scan.get();
        claim.wrapping_sub(scan).min(self.queue.cap)
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.closed.0.load(Ordering::Acquire)
    }

    /// Diagnostic: returns `(my_next_scan, claim, ready_snapshot,
    /// done_snapshot)`.
    #[doc(hidden)]
    pub fn debug_snapshot(&self) -> (usize, usize, Vec<usize>, Vec<usize>) {
        let (claim, r, d) = self.queue.debug_snapshot();
        (self.next_scan.get(), claim, r, d)
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // No claimed-but-unread positions to release: in this design,
        // claim and read happen atomically (CAS to claimed → read →
        // store done). There's no "batch" of held positions to
        // release. So nothing to do here.
    }
}
