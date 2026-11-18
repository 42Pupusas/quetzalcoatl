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
mod tests;
