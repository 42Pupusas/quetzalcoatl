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
fn a_floor_past_the_rollover_is_later_than_one_before_it() {
    let shared = SharedFloor::new();
    shared.publish(usize::MAX - 1);
    shared.publish(2);
    assert_eq!(shared.get(), 2);
    shared.publish(usize::MAX - 1);
    assert_eq!(shared.get(), 2);
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

/// A cache left more than half the position space behind reads as
/// ahead of `pos` under a signed distance, and would permit it.
#[test]
fn a_cache_left_half_the_position_space_behind_defers_to_the_scan() {
    let cache = FloorCache::new();
    let shared = SharedFloor::new();
    let pos = usize::MAX / 2 + 10;
    assert!(!cache.permits(pos, 4, true, &shared, || ConsumerFloor::At(pos - 100)));
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
    assert_eq!(
        cache.refresh(&shared, || ConsumerFloor::At(5)),
        ConsumerFloor::At(5)
    );
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
