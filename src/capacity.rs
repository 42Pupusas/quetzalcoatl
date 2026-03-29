/// A power-of-two capacity for use with ring buffers.
///
/// Guarantees that the capacity is always a power of two, enabling
/// fast bitwise-AND indexing instead of expensive modulo operations.
#[derive(Debug, Clone, Copy)]
pub struct Capacity {
    pub(crate) cap: usize,
    pub(crate) mask: usize,
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
}
