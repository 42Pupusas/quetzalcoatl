//! Lap accounting for a consumer's scan of the ring.
//!
//! [`Consumer::claim_slot`](super::Consumer) walks positions looking
//! for a published slot to CAS-claim, and needs to stop after one lap
//! so an empty ring returns rather than spins. Two separate quantities
//! decide that, and conflating them is a bug:
//!
//! - **How far to jump** after losing a CAS. A lost CAS means a peer
//!   consumer is working the same cache line, and moving well clear of
//!   it before the next attempt is worth real time.
//! - **How much of the lap that costs.** The lap exists to bound the
//!   search, and the search is over the ring's `cap` positions.
//!
//! A jump is only meaningful modulo the ring: on a ring of 4, jumping
//! 127 positions lands 31 laps later on the same slot. Charging 127
//! against a budget of 4 ends the lap outright, so a consumer that
//! loses a single CAS reports the ring empty while items sit in it —
//! and the caller parks on a ring with work in it.
//!
//! [`ScanBudget`] owns both quantities so the jump can be large in
//! position and small in budget, and so the policy can be tested
//! without staging a CAS race between threads.

/// Tracks one lap of a consumer's scan: where to look next, and how
/// much of the lap is left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScanBudget {
    position: usize,
    spent: usize,
    lap: usize,
    cas_skip: usize,
}

impl ScanBudget {
    /// Starts a lap of `capacity` positions from `start`, jumping
    /// `cas_skip` positions past a slot lost to a peer.
    ///
    /// The skip is clamped to the ring: a jump of a full lap or more
    /// would return to the same slot having examined nothing.
    pub(super) const fn new(start: usize, capacity: usize, cas_skip: usize) -> Self {
        let clamped = if cas_skip >= capacity {
            capacity - 1
        } else {
            cas_skip
        };
        Self {
            position: start,
            spent: 0,
            lap: capacity,
            cas_skip: clamped,
        }
    }

    /// The position to examine.
    pub(super) const fn position(&self) -> usize {
        self.position
    }

    /// Advances one position, charging one against the lap.
    pub(super) const fn step(&mut self) {
        self.position += 1;
        self.spent += 1;
    }

    /// Moves clear of a slot a peer claimed first.
    ///
    /// Charges only the positions actually skipped over, which the
    /// clamp keeps within the lap. A lost CAS is evidence the ring is
    /// *busy*, not that it is empty, so it must not be able to end the
    /// lap on its own.
    pub(super) const fn skip_contended(&mut self) {
        self.position += self.cas_skip;
        self.spent += self.cas_skip;
    }

    /// Whether the lap is over.
    pub(super) const fn exhausted(&self) -> bool {
        self.spent >= self.lap
    }
}

#[cfg(test)]
mod tests {
    use super::ScanBudget;

    /// The defect this type exists to remove: on any ring smaller than
    /// the skip, one lost CAS spent the entire lap, so a consumer gave
    /// up with published slots it had never looked at.
    #[test]
    fn one_lost_cas_does_not_end_the_lap_on_a_small_ring() {
        let mut budget = ScanBudget::new(0, 4, 128);
        budget.skip_contended();
        assert!(
            !budget.exhausted(),
            "three slots were never examined, so the lap is not over"
        );
    }

    /// The clamp must leave at least one position examinable per skip,
    /// otherwise a ring of contended slots would loop forever.
    #[test]
    fn a_lap_of_contended_slots_still_ends() {
        let mut budget = ScanBudget::new(0, 4, 128);
        for _ in 0..4 {
            budget.skip_contended();
        }
        assert!(budget.exhausted(), "a bounded lap must terminate");
    }

    #[test]
    fn a_lap_ends_after_capacity_steps() {
        let mut budget = ScanBudget::new(0, 4, 128);
        for _ in 0..3 {
            budget.step();
            assert!(!budget.exhausted());
        }
        budget.step();
        assert!(budget.exhausted());
    }

    /// On a ring large enough for the skip, the skip is used as given:
    /// the clamp must not degrade the cache-line backoff it exists for.
    #[test]
    fn a_large_ring_keeps_the_full_skip() {
        let mut budget = ScanBudget::new(0, 1024, 128);
        budget.skip_contended();
        assert_eq!(
            budget.position(),
            128,
            "the jump past a contended line is the point of the skip"
        );
        assert!(!budget.exhausted());
    }

    /// The stress ring is `cap = 16` against the default skip of 128,
    /// so the clamp is what governs there rather than an edge case.
    #[test]
    fn the_default_skip_is_clamped_on_a_typical_small_ring() {
        let mut budget = ScanBudget::new(0, 16, 128);
        budget.skip_contended();
        assert_eq!(
            budget.position(),
            15,
            "a jump of 128 on a ring of 16 lands back on the same slot"
        );
        assert!(
            !budget.exhausted(),
            "one contended slot must leave the lap alive"
        );
    }

    #[test]
    fn scanning_starts_where_it_was_told_to() {
        let mut budget = ScanBudget::new(7, 8, 4);
        assert_eq!(budget.position(), 7);
        budget.step();
        assert_eq!(budget.position(), 8);
    }
}
