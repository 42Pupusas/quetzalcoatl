/// A power-of-two capacity for use with ring buffers.
///
/// Guarantees that the capacity is always a power of two, enabling
/// fast bitwise-AND indexing instead of expensive modulo operations.
#[derive(Debug, Clone, Copy)]
pub struct Capacity {
    cap: usize,
    mask: usize,
}

impl Capacity {
    /// Creates a capacity of exactly `cap` elements.
    ///
    /// # Panics
    ///
    /// Panics if `cap` is not a power of two or is zero.
    #[must_use]
    pub fn exact(cap: usize) -> Self {
        assert!(
            cap > 0 && cap.is_power_of_two(),
            "capacity must be a non-zero power of two"
        );
        Self { cap, mask: cap - 1 }
    }

    /// Creates a capacity of at least `min` elements by rounding up
    /// to the next power of two.
    ///
    /// # Panics
    ///
    /// Panics if `min` is zero.
    #[must_use]
    pub fn at_least(min: usize) -> Self {
        assert!(min > 0, "capacity must be non-zero");
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
mod tests {
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
}
