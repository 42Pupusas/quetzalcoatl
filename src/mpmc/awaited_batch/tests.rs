use super::{AwaitedBatch, WakeInterest};

#[test]
fn a_fresh_announcement_wants_nothing_in_particular() {
    let awaited = AwaitedBatch::new();
    assert_eq!(awaited.interest(0), WakeInterest::Any);
    assert_eq!(awaited.interest(usize::MAX), WakeInterest::Any);
}

#[test]
fn an_announced_batch_wants_only_its_unused_positions() {
    let awaited = AwaitedBatch::new();
    awaited.announce(100, 0b1010);
    assert_eq!(awaited.interest(101), WakeInterest::Reserved);
    assert_eq!(awaited.interest(103), WakeInterest::Reserved);
    for pos in [99usize, 100, 102, 104] {
        assert_eq!(awaited.interest(pos), WakeInterest::Declines);
    }
}

/// The distinction routing depends on: a producer pinned to other
/// positions can do nothing with this release, while one waiting
/// to refill can take whatever frees. Both fail to name `pos`, so
/// a yes/no answer collapses them and the release goes to the one
/// that cannot use it.
#[test]
fn a_pinned_producer_declines_where_a_refilling_one_takes_anything() {
    let pinned = AwaitedBatch::new();
    pinned.announce(100, 0b1);
    let refilling = AwaitedBatch::new();
    refilling.announce(100, 0);

    assert_eq!(pinned.interest(200), WakeInterest::Declines);
    assert_eq!(refilling.interest(200), WakeInterest::Any);
}

/// A producer waiting to refill is not a taker for a specific
/// release: serving it ahead of a pinned producer would spend the
/// wake the pinned one was owed.
#[test]
fn an_exhausted_batch_wants_nothing_in_particular() {
    let awaited = AwaitedBatch::new();
    awaited.announce(100, 0);
    assert_eq!(awaited.interest(7), WakeInterest::Any);
    assert_eq!(awaited.interest(100), WakeInterest::Any);
}

#[test]
fn a_later_announcement_replaces_the_earlier_one() {
    let awaited = AwaitedBatch::new();
    awaited.announce(100, 0b1);
    awaited.announce(200, 0b1);
    assert_eq!(awaited.interest(100), WakeInterest::Declines);
    assert_eq!(awaited.interest(200), WakeInterest::Reserved);
}

#[test]
fn a_position_far_beyond_the_batch_is_not_wanted() {
    let awaited = AwaitedBatch::new();
    awaited.announce(100, u32::MAX);
    assert_eq!(awaited.interest(131), WakeInterest::Reserved);
    assert_eq!(awaited.interest(132), WakeInterest::Declines);
}

/// A producer stops parking without retracting its announcement,
/// and `ParkRegistry` hands the slot to a new producer on the next
/// lease. The word left behind still names positions, so it can
/// attract a targeted wake on behalf of a producer that is no
/// longer waiting for anything.
///
/// `wake_one_interested` only spends the wake if the slot's bit is
/// also set, so a stale announcement over a *clear* bit costs
/// nothing. The reachable case is the inherited one: a new lease
/// holder arms its bit while the previous holder's word is still
/// published, and is then routed a position it never reserved.
/// Its own re-check finds nothing, and it re-parks having spent
/// the release that the genuinely pinned producer needed.
#[test]
fn a_slot_reused_by_a_new_producer_does_not_inherit_the_old_want() {
    let awaited = AwaitedBatch::new();
    awaited.announce(100, 0b1111);
    assert_eq!(
        awaited.interest(101),
        WakeInterest::Reserved,
        "the first producer is pinned here"
    );

    awaited.retract();

    assert_ne!(
        awaited.interest(101),
        WakeInterest::Reserved,
        "a producer that stopped parking must not still attract a wake"
    );
    assert_eq!(
        awaited.announced(),
        None,
        "a retracted slot is indistinguishable from a fresh one"
    );
}

/// Retracting is what a fresh lease inherits, so the two must
/// agree: a recycled slot has to look exactly like a new one.
#[test]
fn a_retracted_slot_matches_a_fresh_one() {
    let fresh = AwaitedBatch::new();
    let reused = AwaitedBatch::new();
    reused.announce(4096, 0b1011);
    reused.retract();

    for pos in [0usize, 1, 4095, 4096, 4097, 4099, usize::MAX] {
        assert_eq!(
            reused.interest(pos),
            fresh.interest(pos),
            "a recycled slot must not answer differently at {pos}"
        );
    }
}

/// The packed word keeps only the low 32 bits of `start`, so the
/// match must hold once positions have run past that width.
#[test]
fn matching_survives_the_low_word_wrapping() {
    let awaited = AwaitedBatch::new();
    let start = (1usize << 32) - 2;
    awaited.announce(start, 0b111);
    for offset in 0..3 {
        assert_eq!(awaited.interest(start + offset), WakeInterest::Reserved);
    }
    assert_eq!(awaited.interest(start + 3), WakeInterest::Declines);
}
