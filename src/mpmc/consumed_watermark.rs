//! The shared "items consumed" watermark and its per-consumer tally.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::CachePadded;

/// A lower bound on the number of positions consumers have claimed.
///
/// Producers subtract it from the claim cursor to bound how many
/// positions they may reserve. The value lags real consumer progress,
/// because each consumer batches its pops privately before publishing
/// them (see [`ConsumedTally`]).
///
/// It counts claims, not releases: a consumer records a pop when it
/// wins the slot, before it reads the value and releases `done`. So a
/// position counted here may still be live, and the watermark does not
/// protect a slot from reuse. That is the job of the `done` word, which
/// a producer checks before every write. The watermark only throttles
/// how many positions are reserved, and an understated one reserves
/// less than it could and falls back to a per-slot check.
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
mod tests;
