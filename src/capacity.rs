/// A power-of-two capacity for use with ring buffers.
///
/// Guarantees that the capacity is always a power of two, enabling
/// fast bitwise-AND indexing instead of expensive modulo operations.
///
/// The capacity is also at most [`Capacity::MAX`]. Position comparisons
/// and the broadcast slot words are only unambiguous for positions
/// fewer than a quarter of the `usize` space apart, and two occupants
/// of one slot are a capacity apart.
#[derive(Debug, Clone, Copy)]
pub struct Capacity {
    cap: usize,
    mask: usize,
}

impl Capacity {
    /// The largest capacity: `2^(usize::BITS - 3)`.
    ///
    /// Keeps twice a capacity below half the position space, which
    /// `ConsumerFloor` needs to read a wrapped distance as signed, and
    /// keeps a capacity short of the `2^(usize::BITS - 2)` period at
    /// which broadcast slot words repeat.
    pub const MAX: usize = 1 << (usize::BITS - 3);

    /// Creates a capacity of exactly `cap` elements.
    ///
    /// # Panics
    ///
    /// Panics if `cap` is not a power of two, is zero, or exceeds
    /// [`Capacity::MAX`].
    #[must_use]
    pub fn exact(cap: usize) -> Self {
        assert!(
            cap > 0 && cap.is_power_of_two(),
            "capacity must be a non-zero power of two"
        );
        assert!(cap <= Self::MAX, "capacity exceeds Capacity::MAX");
        Self { cap, mask: cap - 1 }
    }

    /// Creates a capacity of at least `min` elements by rounding up
    /// to the next power of two.
    ///
    /// # Panics
    ///
    /// Panics if `min` is zero or exceeds [`Capacity::MAX`].
    #[must_use]
    pub fn at_least(min: usize) -> Self {
        assert!(min > 0, "capacity must be non-zero");
        assert!(min <= Self::MAX, "capacity exceeds Capacity::MAX");
        let cap = min.next_power_of_two();
        Self { cap, mask: cap - 1 }
    }

    /// Returns the actual capacity (always a power of two).
    #[must_use]
    pub const fn get(self) -> usize {
        self.cap
    }

    /// Maps a logical position to its slot index.
    ///
    /// The result is always `< self.get()`, which is what makes
    /// unchecked indexing into a `cap`-length buffer sound: `mask`
    /// is `cap - 1` and `cap` is a power of two, both enforced by
    /// the constructors.
    #[inline]
    #[must_use]
    pub(crate) const fn index_of(self, pos: usize) -> usize {
        pos & self.mask
    }

    /// Reduces a position *delta* modulo the capacity.
    ///
    /// Same arithmetic as [`index_of`](Self::index_of), different
    /// meaning: the result is a distance within one lap, not a slot
    /// index, and carries no bounds contract.
    #[inline]
    #[must_use]
    pub(crate) const fn wrap(self, delta: usize) -> usize {
        delta & self.mask
    }
}

#[cfg(test)]
mod tests;
