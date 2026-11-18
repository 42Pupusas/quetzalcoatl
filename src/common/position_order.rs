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
mod tests;
