use super::*;

/// Without a consumer, nothing proves a slot's previous occupant
/// has been read, so producers must not lap the ring.
#[test]
fn no_consumers_permits_nothing() {
    assert!(!ConsumerFloor::NoConsumers.permits(0, 4));
    assert!(!ConsumerFloor::NoConsumers.permits(usize::MAX, 4));
}

#[test]
fn floor_behind_by_less_than_capacity_permits() {
    assert!(ConsumerFloor::At(0).permits(3, 4));
}

#[test]
fn floor_behind_by_capacity_blocks() {
    assert!(!ConsumerFloor::At(0).permits(4, 4));
    assert!(!ConsumerFloor::At(2).permits(9, 4));
}

/// The case that used to wrap: a floor ahead of the claimed slot.
#[test]
fn floor_ahead_of_position_permits_without_wrapping() {
    assert!(ConsumerFloor::At(8).permits(4, 4));
    assert!(ConsumerFloor::At(1).permits(0, 1));
}

#[test]
fn position_or_tail_reports_zero_depth_without_consumers() {
    let tail = 42usize;
    assert_eq!(tail - ConsumerFloor::NoConsumers.position_or_tail(tail), 0);
    assert_eq!(ConsumerFloor::At(40).position_or_tail(tail), 40);
}

#[test]
fn a_stale_floor_far_behind_does_not_read_as_ahead() {
    let pos = usize::MAX / 2 + 10;
    assert!(!ConsumerFloor::At(0).permits_from_stale(pos, 4));
    assert!(ConsumerFloor::At(pos - 3).permits_from_stale(pos, 4));
    assert!(!ConsumerFloor::At(pos - 4).permits_from_stale(pos, 4));
    assert!(!ConsumerFloor::NoConsumers.permits_from_stale(pos, 4));
}

#[test]
fn the_window_is_the_same_across_the_position_rollover() {
    let head = usize::MAX - 1;
    assert!(ConsumerFloor::At(head).permits(head.wrapping_add(3), 4));
    assert!(!ConsumerFloor::At(head).permits(head.wrapping_add(4), 4));
}

#[test]
fn a_floor_ahead_across_the_rollover_permits() {
    assert!(ConsumerFloor::At(2).permits(usize::MAX, 4));
    assert!(ConsumerFloor::At(usize::MAX).permits(usize::MAX - 5, 4));
}
