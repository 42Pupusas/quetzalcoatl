use super::WakeInterest;

#[test]
fn a_reserved_claim_outranks_every_other() {
    assert!(WakeInterest::Reserved.rank() < WakeInterest::Any.rank());
    assert!(WakeInterest::Reserved.rank() < WakeInterest::Declines.rank());
}

#[test]
fn a_waiter_wanting_anything_outranks_one_that_declines() {
    assert!(WakeInterest::Any.rank() < WakeInterest::Declines.rank());
}

#[test]
fn only_a_declining_waiter_can_use_nothing() {
    assert!(WakeInterest::Reserved.is_usable());
    assert!(WakeInterest::Any.is_usable());
    assert!(!WakeInterest::Declines.is_usable());
}

#[test]
fn the_ranks_are_distinct() {
    let ranks = [
        WakeInterest::Reserved.rank(),
        WakeInterest::Any.rank(),
        WakeInterest::Declines.rank(),
    ];
    for (i, a) in ranks.iter().enumerate() {
        for b in &ranks[i + 1..] {
            assert_ne!(a, b);
        }
    }
}
