use super::PositionOrder;

#[test]
fn a_later_position_is_after_an_earlier_one() {
    assert!(5usize.is_after(3));
    assert!(!3usize.is_after(5));
}

#[test]
fn a_position_is_not_after_itself() {
    assert!(!7usize.is_after(7));
}

#[test]
fn a_position_past_the_rollover_is_after_one_before_it() {
    assert!(2usize.is_after(usize::MAX - 1));
    assert!(!(usize::MAX - 1).is_after(2));
}

#[test]
fn latest_and_earliest_follow_the_order_across_the_rollover() {
    let before = usize::MAX - 1;
    let after = 2usize;
    assert_eq!(before.latest(after), after);
    assert_eq!(after.latest(before), after);
    assert_eq!(before.earliest(after), before);
    assert_eq!(after.earliest(before), before);
}

#[test]
fn positions_half_the_space_apart_are_the_limit_of_the_order() {
    let half = usize::MAX / 2;
    assert!(half.is_after(0));
    assert!(!(half + 2).is_after(0));
}
