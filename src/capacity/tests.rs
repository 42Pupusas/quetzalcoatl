use super::*;

#[test]
fn capacity_exact_valid() {
    let c = Capacity::exact(16);
    assert_eq!(c.get(), 16);
}

#[test]
#[should_panic(expected = "capacity must be a non-zero power of two")]
fn capacity_exact_rejects_non_power_of_two() {
    let _ = Capacity::exact(3);
}

#[test]
#[should_panic(expected = "capacity must be a non-zero power of two")]
fn capacity_exact_rejects_zero() {
    let _ = Capacity::exact(0);
}

#[test]
fn capacity_at_least_rounds_up() {
    assert_eq!(Capacity::at_least(3).get(), 4);
    assert_eq!(Capacity::at_least(5).get(), 8);
    assert_eq!(Capacity::at_least(1000).get(), 1024);
}

#[test]
fn capacity_at_least_preserves_power_of_two() {
    assert_eq!(Capacity::at_least(4).get(), 4);
    assert_eq!(Capacity::at_least(1024).get(), 1024);
}

#[test]
#[should_panic(expected = "capacity must be non-zero")]
fn capacity_at_least_rejects_zero() {
    let _ = Capacity::at_least(0);
}

#[test]
fn the_largest_capacity_is_accepted() {
    assert_eq!(Capacity::MAX, 1 << (usize::BITS - 3));
    assert_eq!(Capacity::exact(Capacity::MAX).get(), Capacity::MAX);
    assert_eq!(Capacity::at_least(Capacity::MAX).get(), Capacity::MAX);
}

#[test]
fn twice_the_largest_capacity_is_below_half_the_position_space() {
    const { assert!(Capacity::MAX * 2 < 1 << (usize::BITS - 1)) };
}

#[test]
#[should_panic(expected = "capacity exceeds Capacity::MAX")]
fn capacity_exact_rejects_above_the_maximum() {
    let _ = Capacity::exact(Capacity::MAX * 2);
}

#[test]
#[should_panic(expected = "capacity exceeds Capacity::MAX")]
fn capacity_at_least_rejects_above_the_maximum() {
    let _ = Capacity::at_least(Capacity::MAX + 1);
}

#[test]
#[should_panic(expected = "capacity exceeds Capacity::MAX")]
fn capacity_at_least_rejects_a_value_whose_power_of_two_overflows() {
    let _ = Capacity::at_least(usize::MAX);
}

#[test]
fn an_index_never_reaches_the_capacity() {
    let c = Capacity::exact(8);
    for pos in 0..64usize {
        assert!(c.index_of(pos) < c.get());
    }
}

#[test]
fn positions_within_one_lap_map_to_distinct_slots() {
    let c = Capacity::exact(16);
    let seen: Vec<usize> = (0..16).map(|p| c.index_of(p)).collect();
    assert_eq!(seen, (0..16).collect::<Vec<_>>());
}

#[test]
fn a_position_and_its_next_lap_share_a_slot() {
    let c = Capacity::exact(8);
    assert_eq!(c.index_of(3), c.index_of(3 + 8));
    assert_eq!(c.index_of(3), c.index_of(3 + 800 * 8));
}

#[test]
fn indexing_survives_a_position_that_wrapped_around_usize() {
    let c = Capacity::exact(16);
    assert!(c.index_of(usize::MAX) < c.get());
    assert_eq!(c.index_of(usize::MAX.wrapping_add(1)), 0);
}

#[test]
fn wrapping_a_delta_agrees_with_the_remainder() {
    let c = Capacity::exact(32);
    for delta in 0..200usize {
        assert_eq!(c.wrap(delta), delta % c.get());
    }
}
