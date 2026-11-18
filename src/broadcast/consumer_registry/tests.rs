use super::*;

#[test]
fn a_fresh_registry_has_nobody_subscribed() {
    assert!(!ConsumerRegistry::new(4).any_subscribed());
}

#[test]
fn an_empty_registry_reports_no_floor() {
    assert_eq!(ConsumerRegistry::new(4).floor(), ConsumerFloor::NoConsumers);
}

#[test]
fn a_registry_without_seats_is_rejected() {
    assert!(std::panic::catch_unwind(|| ConsumerRegistry::new(0)).is_err());
}

#[test]
fn the_first_subscriber_takes_seat_zero_and_starts_at_the_beginning() {
    let registry = ConsumerRegistry::new(4);
    assert_eq!(registry.subscribe_first(), 0);
    assert_eq!(registry.head_of(0), 0);
    assert_eq!(registry.floor(), ConsumerFloor::At(0));
}

#[test]
fn a_later_subscriber_starts_at_the_tail_it_was_given() {
    let registry = ConsumerRegistry::new(4);
    registry.subscribe_first();
    let index = registry.subscribe_at(7);
    assert_eq!(registry.head_of(index), 7);
}

#[test]
fn each_subscriber_gets_a_distinct_seat() {
    let registry = ConsumerRegistry::new(3);
    let seats = [
        registry.subscribe_first(),
        registry.subscribe_at(0),
        registry.subscribe_at(0),
    ];
    assert_eq!(seats, [0, 1, 2]);
}

#[test]
fn subscribing_past_the_last_seat_is_rejected() {
    let registry = ConsumerRegistry::new(1);
    registry.subscribe_first();
    assert!(std::panic::catch_unwind(|| registry.subscribe_at(0)).is_err());
}

#[test]
fn a_vacated_seat_is_handed_out_again() {
    let registry = ConsumerRegistry::new(1);
    let index = registry.subscribe_first();
    registry.unsubscribe(index);
    assert_eq!(registry.subscribe_at(3), index);
    assert_eq!(registry.head_of(index), 3);
}

#[test]
fn the_floor_follows_the_slowest_subscriber() {
    let registry = ConsumerRegistry::new(3);
    let fast = registry.subscribe_first();
    let slow = registry.subscribe_at(0);
    registry.publish_head_past(fast, 9);
    registry.publish_head_past(slow, 2);
    assert_eq!(registry.floor(), ConsumerFloor::At(3));
}

#[test]
fn the_floor_is_the_earliest_position_across_the_rollover() {
    let registry = ConsumerRegistry::new(2);
    let behind = registry.subscribe_first();
    let ahead = registry.subscribe_at(0);
    registry.publish_head_past(behind, usize::MAX - 2);
    registry.publish_head_past(ahead, 1);
    assert_eq!(registry.floor(), ConsumerFloor::At(usize::MAX - 1));
}

#[test]
fn a_departed_consumer_stops_holding_the_floor_back() {
    let registry = ConsumerRegistry::new(2);
    let fast = registry.subscribe_first();
    let slow = registry.subscribe_at(0);
    registry.publish_head_past(fast, 9);
    registry.unsubscribe(slow);
    assert_eq!(registry.floor(), ConsumerFloor::At(10));
}

#[test]
fn the_last_departure_leaves_no_floor_at_all() {
    let registry = ConsumerRegistry::new(2);
    let only = registry.subscribe_first();
    assert!(registry.any_subscribed());
    registry.unsubscribe(only);
    assert!(!registry.any_subscribed());
    assert_eq!(registry.floor(), ConsumerFloor::NoConsumers);
}

#[test]
fn publishing_moves_the_head_one_past_the_position_read() {
    let registry = ConsumerRegistry::new(2);
    let index = registry.subscribe_first();
    registry.publish_head_past(index, 4);
    assert_eq!(registry.head_of(index), 5);
}

#[test]
fn the_registry_reports_the_seat_count_it_was_built_with() {
    assert_eq!(ConsumerRegistry::new(5).seats(), 5);
}

#[test]
fn each_head_occupies_its_own_cache_line() {
    let registry = ConsumerRegistry::new(2);
    let first = std::ptr::from_ref(&registry.slots[0].head).addr();
    let second = std::ptr::from_ref(&registry.slots[1].head).addr();
    assert!(second - first >= 64);
}
