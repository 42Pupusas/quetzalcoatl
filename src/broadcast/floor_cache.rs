//! Cached views of the consumer floor, avoiding redundant registry scans.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::position_order::PositionOrder;
use crate::common::CachePadded;

use super::consumer_floor::ConsumerFloor;

/// The shared L2 cache of the consumer floor.
///
/// Any producer that completes a full registry scan republishes the
/// result here, so its peers can skip their own scan. Updates keep the
/// later position, which makes the value monotonic: a producer holding
/// an older floor cannot drag the cache backwards. "Later" is in
/// position order, so it holds across the `usize` rollover, where a
/// numeric maximum would pick the stale value.
///
/// The value is always `<=` the true floor. Staleness therefore only
/// costs a redundant scan; it never permits a write it should refuse,
/// because a floor that is too low constrains more, not less.
///
/// A bare position cannot express [`ConsumerFloor::NoConsumers`], so
/// this must only be consulted while a consumer is registered — see
/// [`FloorCache::permits`].
pub(super) struct SharedFloor(CachePadded<AtomicUsize>);

impl SharedFloor {
    pub(super) const fn new() -> Self {
        Self(CachePadded(AtomicUsize::new(0)))
    }

    #[inline]
    pub(super) fn get(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    /// Publishes `head`, keeping the latest floor seen by any producer.
    #[inline]
    pub(super) fn publish(&self, head: usize) {
        let _ = self
            .0
            .try_update(Ordering::Release, Ordering::Relaxed, |cached| {
                head.is_after(cached).then_some(head)
            });
    }
}

/// One producer's private view of the consumer floor, backed by the
/// shared cache.
///
/// Answering "may I write `pos`?" gets more expensive at each level: the
/// private [`Cell`], then the shared atomic, then a full O(N) scan of
/// the registry. [`permits`](Self::permits) stops at the first level
/// that says yes, and only a scan can say no.
pub(super) struct FloorCache {
    private: Cell<usize>,
}

impl FloorCache {
    pub(super) const fn new() -> Self {
        Self {
            private: Cell::new(0),
        }
    }

    /// The last floor this producer observed, as a constraint.
    ///
    /// The cache holds a plain position, so it is always treated as
    /// constraining; a stale floor only costs a redundant scan.
    #[inline]
    pub(super) const fn cached(&self) -> ConsumerFloor {
        ConsumerFloor::At(self.private.get())
    }

    /// Whether `pos` is writable, escalating through the cache levels.
    ///
    /// `any_subscribed` gates both caches: neither can represent "no
    /// consumers", so a cache seeded at 0 would otherwise permit the
    /// first `cap` positions with no consumer having ever existed.
    /// `scan` supplies the authoritative floor for the final level.
    ///
    /// The caches answer with the strict window only. A floor that
    /// leads `pos` is a rare case, and a cached one cannot be told from
    /// a floor left half the position space behind, so it is left to
    /// the scan.
    #[inline]
    pub(super) fn permits(
        &self,
        pos: usize,
        cap: usize,
        any_subscribed: bool,
        shared: &SharedFloor,
        scan: impl FnOnce() -> ConsumerFloor,
    ) -> bool {
        if any_subscribed {
            if self.cached().permits_from_stale(pos, cap) {
                return true;
            }
            let published = shared.get();
            self.private.set(published);
            if ConsumerFloor::At(published).permits_from_stale(pos, cap) {
                return true;
            }
        }
        self.refresh(shared, scan).permits(pos, cap)
    }

    /// Rescans the registry and republishes the result to both levels.
    #[inline]
    pub(super) fn refresh(
        &self,
        shared: &SharedFloor,
        scan: impl FnOnce() -> ConsumerFloor,
    ) -> ConsumerFloor {
        let floor = scan();
        if let ConsumerFloor::At(head) = floor {
            shared.publish(head);
            self.private.set(head);
        }
        floor
    }
}

#[cfg(test)]
mod tests;
