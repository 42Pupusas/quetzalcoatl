//! The shared "items consumed" watermark and its per-consumer tally.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::CachePadded;

/// A lower bound on the number of positions consumers have released.
///
/// Producers subtract it from the claim cursor to bound how many
/// positions they may reserve. The value lags real consumer progress,
/// because each consumer batches its pops privately before publishing
/// them (see [`ConsumedTally`]).
///
/// Lagging is safe in one direction only: an understated watermark
/// overstates how full the ring is, so a producer reserves less than it
/// could and falls back to a per-slot check. Overstating it would let a
/// producer reserve a position whose previous occupant is still live.
pub(super) struct ConsumedWatermark(CachePadded<AtomicUsize>);

impl ConsumedWatermark {
    pub(super) const fn new() -> Self {
        Self(CachePadded(AtomicUsize::new(0)))
    }

    #[inline]
    pub(super) fn get(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn add(&self, count: usize) {
        self.0.fetch_add(count, Ordering::Relaxed);
    }

    /// Positions a producer may reserve, or `None` when the watermark
    /// accounts for the whole ring.
    ///
    /// `None` is an estimate, not a verdict: the watermark is a lower
    /// bound, so consumers may have freed positions it does not yet
    /// reflect. The caller falls back to a per-slot check rather than
    /// treating it as full.
    ///
    /// The subtraction wraps, so the answer stays correct once `claim`
    /// has run past `usize::MAX`.
    #[inline]
    pub(super) fn free(&self, claim: usize, cap: usize) -> Option<usize> {
        let in_flight = claim.wrapping_sub(self.get());
        (in_flight < cap).then(|| cap - in_flight)
    }
}

/// One consumer's unpublished pops.
///
/// Every pop would otherwise be a read-modify-write on a line every
/// producer reads. Counting privately and publishing once per
/// `flush_at` pops trades a staler watermark for that traffic.
pub(super) struct ConsumedTally {
    pending: Cell<usize>,
}

impl ConsumedTally {
    pub(super) const fn new() -> Self {
        Self {
            pending: Cell::new(0),
        }
    }

    /// Counts one pop, publishing the batch once it reaches `flush_at`.
    #[inline]
    pub(super) fn record(&self, watermark: &ConsumedWatermark, flush_at: usize) {
        let pending = self.pending.get() + 1;
        if pending >= flush_at {
            watermark.add(pending);
            self.pending.set(0);
        } else {
            self.pending.set(pending);
        }
    }

    /// Publishes whatever has not reached a flush boundary.
    ///
    /// A departing consumer owes this: its pending count would otherwise
    /// hold the watermark down for the lifetime of the ring, permanently
    /// shrinking what producers believe is free.
    #[inline]
    pub(super) fn flush(&self, watermark: &ConsumedWatermark) {
        let pending = self.pending.get();
        if pending > 0 {
            watermark.add(pending);
            self.pending.set(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_watermark_has_consumed_nothing() {
        assert_eq!(ConsumedWatermark::new().get(), 0);
    }

    #[test]
    fn adding_advances_the_watermark() {
        let watermark = ConsumedWatermark::new();
        watermark.add(3);
        watermark.add(4);
        assert_eq!(watermark.get(), 7);
    }

    #[test]
    fn the_watermark_keeps_its_own_cache_line() {
        assert_eq!(
            std::mem::align_of::<ConsumedWatermark>(),
            std::mem::align_of::<CachePadded<AtomicUsize>>()
        );
    }

    #[test]
    fn an_untouched_ring_has_every_position_free() {
        assert_eq!(ConsumedWatermark::new().free(0, 8), Some(8));
    }

    #[test]
    fn free_space_is_the_capacity_less_the_positions_in_flight() {
        let watermark = ConsumedWatermark::new();
        watermark.add(2);
        assert_eq!(watermark.free(5, 8), Some(5));
    }

    #[test]
    fn a_ring_with_every_position_in_flight_reports_no_estimate() {
        assert_eq!(ConsumedWatermark::new().free(8, 8), None);
    }

    /// The watermark only ever understates progress, so a producer can
    /// hold a claim beyond what it accounts for. That must read as "no
    /// estimate" rather than wrapping into a huge free count.
    #[test]
    fn a_claim_beyond_the_watermark_reports_no_estimate() {
        assert_eq!(ConsumedWatermark::new().free(9, 8), None);
    }

    #[test]
    fn free_space_survives_the_position_rollover() {
        let watermark = ConsumedWatermark::new();
        watermark.add(usize::MAX);
        assert_eq!(watermark.free(2, 8), Some(5));
    }

    #[test]
    fn a_fresh_tally_publishes_nothing() {
        let watermark = ConsumedWatermark::new();
        ConsumedTally::new().flush(&watermark);
        assert_eq!(watermark.get(), 0);
    }

    #[test]
    fn pops_below_the_threshold_stay_private() {
        let watermark = ConsumedWatermark::new();
        let tally = ConsumedTally::new();
        for _ in 0..3 {
            tally.record(&watermark, 4);
        }
        assert_eq!(watermark.get(), 0);
    }

    #[test]
    fn reaching_the_threshold_publishes_the_whole_batch() {
        let watermark = ConsumedWatermark::new();
        let tally = ConsumedTally::new();
        for _ in 0..4 {
            tally.record(&watermark, 4);
        }
        assert_eq!(watermark.get(), 4);
    }

    #[test]
    fn each_batch_publishes_once() {
        let watermark = ConsumedWatermark::new();
        let tally = ConsumedTally::new();
        for _ in 0..12 {
            tally.record(&watermark, 4);
        }
        assert_eq!(watermark.get(), 12);
    }

    #[test]
    fn flushing_publishes_the_pops_that_never_filled_a_batch() {
        let watermark = ConsumedWatermark::new();
        let tally = ConsumedTally::new();
        for _ in 0..6 {
            tally.record(&watermark, 4);
        }
        tally.flush(&watermark);
        assert_eq!(watermark.get(), 6);
    }

    /// Flushing twice must not count the same pops again.
    #[test]
    fn a_flushed_tally_owes_nothing_further() {
        let watermark = ConsumedWatermark::new();
        let tally = ConsumedTally::new();
        tally.record(&watermark, 4);
        tally.flush(&watermark);
        tally.flush(&watermark);
        assert_eq!(watermark.get(), 1);
    }

    /// With `flush_at` of 1 every pop is published immediately, leaving
    /// the tally with nothing to flush.
    #[test]
    fn a_threshold_of_one_never_holds_a_pop_back() {
        let watermark = ConsumedWatermark::new();
        let tally = ConsumedTally::new();
        tally.record(&watermark, 1);
        tally.record(&watermark, 1);
        assert_eq!(watermark.get(), 2);
        tally.flush(&watermark);
        assert_eq!(watermark.get(), 2);
    }
}
