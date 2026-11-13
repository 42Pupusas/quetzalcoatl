//! Ordering of ring positions across the `usize` rollover.
//!
//! Positions only grow, and wrap when they pass `usize::MAX`, so a
//! plain `<` or `max` puts a position just after the rollover before
//! one just ahead of it. Two positions that are live in the same ring
//! are never more than a capacity apart, so the signed value of their
//! wrapping difference orders them correctly: positive means `self` is
//! the later one.
//!
//! The order is meaningful only for positions fewer than
//! `2^(usize::BITS - 1)` apart.

/// Compares positions by the signed distance between them.
pub trait PositionOrder: Copy {
    /// Whether `self` is a later position than `other`.
    fn is_after(self, other: Self) -> bool;

    /// The later of two positions.
    #[must_use]
    fn latest(self, other: Self) -> Self {
        if other.is_after(self) {
            other
        } else {
            self
        }
    }

    /// The earlier of two positions.
    #[must_use]
    fn earliest(self, other: Self) -> Self {
        if self.is_after(other) {
            other
        } else {
            self
        }
    }
}

impl PositionOrder for usize {
    #[inline]
    fn is_after(self, other: Self) -> bool {
        self.wrapping_sub(other).cast_signed() > 0
    }
}

#[cfg(test)]
mod tests {
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
}
