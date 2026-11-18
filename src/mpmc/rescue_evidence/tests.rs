use super::{RescueEvidence, RescueExplanation, RescueTiming, WakeDelivery};

#[test]
fn a_waiter_alone_at_both_samples_is_a_sole_waiter() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 0, true);
    assert!(evidence.is_sole_waiter());
    assert!(!evidence.was_blind());
    assert_eq!(evidence.explanation(), RescueExplanation::Untouched);
}

/// Work visible at the moment of parking means the re-check
/// missed it; no wake was owed for a publish that preceded the
/// arm.
#[test]
fn parking_with_work_visible_marks_the_episode_blind() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, true);
    evidence.parked(WakeDelivery::Untouched, 0, true);
    assert!(evidence.was_blind());
    assert_eq!(evidence.timing(), RescueTiming::Blind);
    assert!(
        evidence.is_sole_waiter(),
        "blindness is a separate axis from who else was parked"
    );
}

/// A later park with nothing visible does not un-see the earlier
/// one: the item was there, and the re-check before that park
/// failed to find it.
#[test]
fn a_later_park_with_nothing_visible_does_not_clear_blindness() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, true);
    evidence.parked(WakeDelivery::Untouched, 0, false);
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 0, false);
    assert!(evidence.was_blind());
    assert_eq!(evidence.timing(), RescueTiming::Blind);
}

/// Nothing before the sleep, something the instant it ended: the
/// item was published while the waiter slept, and nobody woke it.
#[test]
fn work_that_appeared_during_the_sleep_was_waiting() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 0, true);
    assert_eq!(evidence.timing(), RescueTiming::Waiting);
}

/// Nothing at either end of the sleep: whatever the rescue found
/// arrived after the park returned, so the timeout rescued nothing.
#[test]
fn work_absent_when_the_park_returned_arrived_late() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 0, false);
    assert_eq!(evidence.timing(), RescueTiming::Late);
}

/// The rescue belongs to the sleep that immediately preceded it,
/// so an earlier park's return sample must not carry over.
#[test]
fn the_return_sample_is_the_last_parks_not_an_earlier_ones() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 0, true);
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 0, false);
    assert_eq!(evidence.timing(), RescueTiming::Late);
}

/// A peer that was already waiting when we went to sleep could
/// have taken our wake, so round-robin explains the rescue.
#[test]
fn a_peer_parked_before_the_sleep_disqualifies_a_sole_waiter() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(1, false);
    evidence.parked(WakeDelivery::Untouched, 0, true);
    assert!(!evidence.is_sole_waiter());
}

/// And so could one that arrived during it and is still there.
#[test]
fn a_peer_parked_after_the_sleep_disqualifies_a_sole_waiter() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 1, true);
    assert!(!evidence.is_sole_waiter());
}

/// A taken bit is recorded, but must not excuse the rescue: the
/// staged lost wake in `the_monitor_counts_a_wake_that_never
/// _arrived` produces exactly this state.
#[test]
fn a_taken_bit_is_recorded_without_excusing_the_rescue() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::InFlight, 0, true);
    assert!(evidence.is_sole_waiter());
    assert_eq!(
        evidence.explanation(),
        RescueExplanation::BitTaken,
        "a stolen wake bit looks identical to a late delivery"
    );
}

/// The observation is on record for the whole episode: a later
/// quiet timeout must not erase a bit that was taken.
#[test]
fn a_later_untouched_park_does_not_erase_a_taken_bit() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::InFlight, 0, true);
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Untouched, 0, true);
    assert_eq!(evidence.explanation(), RescueExplanation::BitTaken);
}

#[test]
fn a_slotless_waiter_outranks_every_other_observation() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::InFlight, 0, true);
    evidence.about_to_park(0, false);
    evidence.parked(WakeDelivery::Slotless, 0, true);
    assert_eq!(evidence.explanation(), RescueExplanation::Slotless);
}

#[test]
fn taking_the_evidence_resets_it_for_the_next_episode() {
    let mut evidence = RescueEvidence::new();
    evidence.about_to_park(0, true);
    evidence.parked(WakeDelivery::InFlight, 0, true);

    let taken = evidence.take();
    assert!(taken.is_sole_waiter());
    assert!(taken.was_blind());
    assert_eq!(taken.explanation(), RescueExplanation::BitTaken);

    assert!(!evidence.is_sole_waiter());
    assert!(!evidence.was_blind());
    assert_eq!(evidence.timing(), RescueTiming::Late);
    assert_eq!(evidence.explanation(), RescueExplanation::Untouched);
}

#[test]
fn a_watch_that_never_parked_reports_no_sole_waiter() {
    assert!(!RescueEvidence::new().is_sole_waiter());
}

#[test]
fn observations_are_ordered_by_how_much_they_account_for() {
    assert!(RescueExplanation::Slotless > RescueExplanation::BitTaken);
    assert!(RescueExplanation::BitTaken > RescueExplanation::Untouched);
}
