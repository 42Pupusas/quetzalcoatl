//! The lower bound consumers place on slot reuse.

/// How far behind the producers the slowest active consumer is.
///
/// A producer may overwrite the slot at `pos` once every consumer has
/// read the previous occupant, `pos - cap`. The floor answers that
/// question, and it has two genuinely different states: some consumer is
/// holding position `n`, or there is no consumer at all.
///
/// The floor can legitimately sit *ahead* of a position a producer has
/// already claimed: peer producers keep advancing `tail`, and the old
/// code reported `tail` as the floor once the registry emptied.
/// Computing `pos - floor` by wrapping subtraction then yielded a huge
/// value that was always `>= cap`, so the producer's "is my slot free
/// yet" spin never terminated. [`permits`](Self::permits) reads the
/// difference as signed, so a floor ahead is a negative distance, and
/// stays right when the positions straddle the `usize` rollover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerFloor {
    /// No consumers are registered, so no position can be validated.
    ///
    /// This refuses writes rather than allowing them. A consumer floor
    /// is what stops two producers from writing the same slot: `pos` and
    /// `pos + cap` alias, and only a consumer's progress proves the
    /// previous occupant is gone. With no consumer there is no such
    /// proof, so producers would lap the ring and race each other.
    NoConsumers,
    /// The slowest active consumer's head.
    At(usize),
}

impl ConsumerFloor {
    /// Whether the slot backing `pos` may be written now.
    pub(crate) const fn permits(self, pos: usize, cap: usize) -> bool {
        match self {
            Self::NoConsumers => false,
            // A negative distance means no consumer is behind us, so
            // the previous occupant of our slot is already read.
            Self::At(head) => pos.wrapping_sub(head).cast_signed() < cap.cast_signed(),
        }
    }

    /// Whether the slot backing `pos` is within one capacity above this
    /// floor, for a floor that may be stale.
    ///
    /// A stale floor is only ever behind the true one, so this can
    /// refuse a write the true floor would allow but never the
    /// reverse. [`permits`](Self::permits) cannot be used on it: a
    /// floor left more than half the position space behind reads as
    /// ahead of `pos` there, and would permit a write over unread
    /// slots.
    pub(crate) const fn permits_from_stale(self, pos: usize, cap: usize) -> bool {
        match self {
            Self::NoConsumers => false,
            Self::At(head) => pos.wrapping_sub(head) < cap,
        }
    }

    /// The floor as a position, using `tail` when there are no
    /// consumers.
    ///
    /// For queue-depth reporting, where "no consumers" means "nothing
    /// outstanding" and yields a length of zero.
    pub(crate) const fn position_or_tail(self, tail: usize) -> usize {
        match self {
            Self::NoConsumers => tail,
            Self::At(head) => head,
        }
    }
}

#[cfg(test)]
mod tests {
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
}
