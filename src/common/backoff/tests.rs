use super::*;

#[test]
fn a_fresh_backoff_has_not_spun() {
    assert!(!Backoff::new().is_exhausted());
}

#[test]
fn the_schedule_latches_after_enough_failures() {
    let mut backoff = Backoff::new();
    for _ in 0..PARK_THRESHOLD {
        assert!(!backoff.is_exhausted());
        backoff.spin();
    }
    assert!(backoff.is_exhausted());
}

#[test]
fn the_gate_spins_up_to_the_threshold_then_defers_to_parking() {
    let mut backoff = Backoff::new();
    let spins = std::iter::from_fn(|| backoff.spin_unless_exhausted().then_some(()))
        .take(1_000)
        .count();
    assert_eq!(spins, PARK_THRESHOLD as usize);
}

#[test]
fn an_exhausted_backoff_stays_exhausted() {
    let mut backoff = Backoff::new();
    for _ in 0..1_000 {
        backoff.spin();
    }
    assert!(backoff.is_exhausted());
    assert!(!backoff.spin_unless_exhausted());
}

#[test]
fn the_counter_saturates_rather_than_wrapping() {
    let mut backoff = Backoff::new();
    for _ in 0..10_000 {
        backoff.spin();
    }
    assert_eq!(backoff.failures, COUNTER_CAP);
}

#[test]
fn a_reset_returns_an_exhausted_backoff_to_the_start_of_the_schedule() {
    let mut backoff = Backoff::new();
    for _ in 0..PARK_THRESHOLD {
        backoff.spin();
    }
    assert!(backoff.is_exhausted());

    backoff.reset();

    assert!(!backoff.is_exhausted());
    assert_eq!(backoff.failures, Backoff::new().failures);
}

#[test]
fn a_reset_backoff_spins_the_full_schedule_again() {
    let mut backoff = Backoff::new();
    while backoff.spin_unless_exhausted() {}
    backoff.reset();

    let spins = std::iter::from_fn(|| backoff.spin_unless_exhausted().then_some(()))
        .take(1_000)
        .count();

    assert_eq!(spins, PARK_THRESHOLD as usize);
}

#[test]
fn a_default_backoff_matches_a_new_one() {
    assert_eq!(Backoff::default().failures, Backoff::new().failures);
}
