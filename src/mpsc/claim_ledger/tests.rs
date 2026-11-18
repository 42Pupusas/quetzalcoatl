use super::*;

const LAST_POSITION: usize = u32::MAX as usize;

#[test]
fn a_fresh_ledger_is_empty() {
    let capacity = Capacity::exact(4);
    assert_eq!(ClaimLedger::new(capacity).len(capacity), 0);
}

#[test]
fn claims_are_consecutive_and_stop_when_the_credits_run_out() {
    let capacity = Capacity::exact(4);
    let ledger = ClaimLedger::new(capacity);
    for expected in 0..4 {
        assert_eq!(ledger.claim(), Some(expected));
    }
    assert_eq!(ledger.claim(), None);
    assert_eq!(ledger.len(capacity), 4);
}

#[test]
fn a_release_returns_exactly_the_positions_released() {
    let capacity = Capacity::exact(4);
    let ledger = ClaimLedger::new(capacity);
    while ledger.claim().is_some() {}

    ledger.publish_head(3);
    assert_eq!(ledger.head(), 3);
    assert_eq!(ledger.len(capacity), 1);
    for expected in 4..7 {
        assert_eq!(ledger.claim(), Some(expected));
    }
    assert_eq!(ledger.claim(), None);
}

#[test]
fn a_full_ring_is_claimable_again_after_each_release() {
    let capacity = Capacity::exact(2);
    let ledger = ClaimLedger::new(capacity);
    while ledger.claim().is_some() {}

    for head in 1..=8 {
        assert_eq!(ledger.claim(), None);
        ledger.publish_head(head);
        assert_eq!(ledger.claim(), Some(head + 1));
    }
}

#[test]
fn the_position_wraps_without_disturbing_the_credits() {
    let capacity = Capacity::exact(4);
    let mut ledger = ClaimLedger::new(capacity);
    ledger.start_at(LAST_POSITION - 1);

    let claimed: Vec<_> = std::iter::from_fn(|| ledger.claim()).collect();
    assert_eq!(claimed, [LAST_POSITION - 1, LAST_POSITION, 0, 1]);
    assert_eq!(ledger.len(capacity), 4);
}

#[test]
fn a_refill_carries_the_position_across_the_wrap() {
    let capacity = Capacity::exact(1);
    let mut ledger = ClaimLedger::new(capacity);
    ledger.start_at(LAST_POSITION - 1);

    assert_eq!(ledger.claim(), Some(LAST_POSITION - 1));
    ledger.publish_head(LAST_POSITION);
    assert_eq!(ledger.claim(), Some(LAST_POSITION));
    ledger.publish_head(LAST_POSITION.wrapping_add(1));
    assert_eq!(ledger.claim(), Some(0));
    assert_eq!(ledger.claim(), None);
}

#[test]
fn a_full_ring_refuses_a_claim_at_every_position() {
    let capacity = Capacity::exact(2);
    for start in [0, 1, LAST_POSITION - 1, LAST_POSITION] {
        let mut ledger = ClaimLedger::new(capacity);
        ledger.start_at(start);
        assert!(ledger.claim().is_some());
        assert!(ledger.claim().is_some());
        assert_eq!(ledger.claim(), None, "start {start}");
    }
}

#[test]
fn the_unreleased_positions_start_at_the_head() {
    let capacity = Capacity::exact(4);
    let mut ledger = ClaimLedger::new(capacity);
    while ledger.claim().is_some() {}
    ledger.publish_head(1);
    assert_eq!(ledger.occupied(capacity).collect::<Vec<_>>(), [1, 2, 3]);
}

#[test]
#[should_panic(expected = "mpsc capacity exceeds 2^30")]
#[cfg(target_pointer_width = "64")]
fn a_capacity_too_large_for_the_credit_field_is_rejected() {
    let _ = ClaimLedger::new(Capacity::exact(1 << 31));
}
