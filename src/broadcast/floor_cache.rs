//! Cached views of the consumer floor, avoiding redundant registry scans.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::CachePadded;

use super::consumer_floor::ConsumerFloor;

/// The shared L2 cache of the consumer floor.
///
/// Any producer that completes a full registry scan republishes the
/// result here, so its peers can skip their own scan. Updates go through
/// `fetch_max`, which makes the value monotonic: a producer holding an
/// older floor cannot drag the cache backwards.
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

    /// Publishes `head`, keeping the highest floor seen by any producer.
    #[inline]
    pub(super) fn publish(&self, head: usize) {
        self.0.fetch_max(head, Ordering::Release);
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
            if self.cached().permits(pos, cap) {
                return true;
            }
            let published = shared.get();
            self.private.set(published);
            if ConsumerFloor::At(published).permits(pos, cap) {
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
mod tests {
    use super::*;

    #[test]
    fn a_fresh_shared_floor_reads_as_the_origin() {
        assert_eq!(SharedFloor::new().get(), 0);
    }

    #[test]
    fn publishing_a_higher_floor_advances_the_shared_cache() {
        let shared = SharedFloor::new();
        shared.publish(7);
        assert_eq!(shared.get(), 7);
    }

    /// A producer holding an older floor must not drag the cache back:
    /// peers would then rescan, but worse, the value must stay a lower
    /// bound that only ever tightens.
    #[test]
    fn publishing_a_lower_floor_leaves_the_shared_cache_alone() {
        let shared = SharedFloor::new();
        shared.publish(9);
        shared.publish(4);
        assert_eq!(shared.get(), 9);
    }

    #[test]
    fn the_shared_floor_keeps_its_own_cache_line() {
        assert_eq!(std::mem::align_of::<SharedFloor>(), 64);
    }

    #[test]
    fn a_fresh_cache_constrains_from_the_origin() {
        assert_eq!(FloorCache::new().cached(), ConsumerFloor::At(0));
    }

    /// The private level is the whole point: a hit must not scan.
    #[test]
    fn a_private_hit_never_scans() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        assert!(cache.permits(3, 4, true, &shared, || {
            panic!("scanned despite a private hit")
        }));
    }

    #[test]
    fn a_shared_hit_answers_without_scanning() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        shared.publish(8);
        assert!(cache.permits(10, 4, true, &shared, || {
            panic!("scanned despite a shared hit")
        }));
        assert_eq!(cache.cached(), ConsumerFloor::At(8));
    }

    #[test]
    fn a_miss_at_both_levels_falls_through_to_the_scan() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        assert!(cache.permits(9, 4, true, &shared, || ConsumerFloor::At(8)));
        assert_eq!(cache.cached(), ConsumerFloor::At(8));
        assert_eq!(shared.get(), 8);
    }

    #[test]
    fn a_scan_that_still_refuses_reports_full() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        assert!(!cache.permits(9, 4, true, &shared, || ConsumerFloor::At(2)));
    }

    /// Neither cache can express "no consumers", so both are skipped
    /// and only the scan's verdict counts.
    #[test]
    fn without_subscribers_the_caches_are_bypassed_entirely() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        shared.publish(64);
        assert!(!cache.permits(0, 4, false, &shared, || ConsumerFloor::NoConsumers));
    }

    /// The bug the gate exists to prevent: a cache seeded at 0 would
    /// permit the first `cap` positions before any consumer subscribed.
    #[test]
    fn a_fresh_cache_does_not_permit_the_first_lap_without_subscribers() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        assert!(!cache.permits(0, 4, false, &shared, || ConsumerFloor::NoConsumers));
        assert!(!cache.permits(3, 4, false, &shared, || ConsumerFloor::NoConsumers));
    }

    #[test]
    fn refreshing_republishes_to_both_levels() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        assert_eq!(cache.refresh(&shared, || ConsumerFloor::At(5)), ConsumerFloor::At(5));
        assert_eq!(cache.cached(), ConsumerFloor::At(5));
        assert_eq!(shared.get(), 5);
    }

    /// `NoConsumers` carries no position, so there is nothing to cache;
    /// writing one would fabricate a floor at 0.
    #[test]
    fn refreshing_to_no_consumers_caches_nothing() {
        let cache = FloorCache::new();
        let shared = SharedFloor::new();
        shared.publish(6);
        cache.refresh(&shared, || ConsumerFloor::At(6));
        assert_eq!(
            cache.refresh(&shared, || ConsumerFloor::NoConsumers),
            ConsumerFloor::NoConsumers
        );
        assert_eq!(cache.cached(), ConsumerFloor::At(6));
        assert_eq!(shared.get(), 6);
    }
}
