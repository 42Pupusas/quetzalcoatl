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
//!   search, and it is spent in *examinations*: every position looked
//!   at costs one, and a lost CAS is one position looked at.
//!
//! Charging the jump distance against the lap conflates the two. On a
//! ring of 16 with a skip of 128, one lost CAS spent the whole lap and
//! the consumer reported the ring empty while items sat in it — and
//! the caller parked on a ring with work in it. Clamping the jump to
//! the ring was not enough: the clamp left it at 15 and the examined
//! slot's own step made 16, so the lap still ended on one lost CAS.
//!
//! [`ScanBudget`] owns both quantities so the jump can be large in
//! position and cost one examination, and so the policy can be tested
//! without staging a CAS race between threads.
//!
//! # Why a stride cannot carry the guarantee
//!
//! Clamping the jump to `capacity - 1` was justified by coprimality:
//! a scan that loses *every* CAS walks distinct slots. That argument
//! only covers a uniform lap. A real lap mixes both moves, and no
//! constant jump survives the mix on a power-of-two ring:
//!
//! - all-contended laps need the jump `k` coprime with `capacity`,
//!   so `k` must be **odd**;
//! - laps that alternate `step` and `skip_contended` advance by
//!   `1 + k` per pair, which needs `1 + k` odd, so `k` must be
//!   **even**.
//!
//! No `k` is both. At `k = capacity - 1` the clamp picks `-1` modulo
//! the capacity, so a step and a skip cancel exactly and the scan
//! oscillates over two slots until the lap is spent. A large ring is
//! not exempt either: one jump of 128 on a ring of 1024 leaves the
//! slots it flew over unexamined for the rest of the lap.
//!
//! Either way the consumer reports an empty ring holding published
//! items, and its caller parks. Since the park is untimed, that is a
//! hang rather than a stall.
//!
//! # What is guaranteed instead
//!
//! Coverage is tracked directly rather than inferred from arithmetic.
//! A `floor` records the lowest position not yet examined, and the lap
//! is over only when the floor has crossed the whole ring. The jump
//! stays exactly as large as it was, so the uncontended fast path is
//! unchanged — position and floor advance together and the lap still
//! ends after `capacity` examinations. Once the probe budget is spent
//! the scan resumes a plain sweep from the floor, which bounds the
//! whole lap to `2 * capacity` examinations and leaves no slot
//! unlooked-at.

/// Tracks one lap of a consumer's scan: where to look next, and how
/// much of the lap is left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScanBudget {
    position: usize,
    floor: usize,
    end: usize,
    spent: usize,
    probe_limit: usize,
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
            floor: start,
            end: start + capacity,
            spent: 0,
            probe_limit: capacity,
            cas_skip: clamped,
        }
    }

    /// The position to examine.
    pub(super) const fn position(&self) -> usize {
        self.position
    }

    /// Advances one position, charging one against the lap.
    pub(super) const fn step(&mut self) {
        self.record();
        self.position += 1;
        self.resume_sweep_when_probes_are_spent();
    }

    /// Charges the position now being examined against the lap, and
    /// advances the coverage floor when this is the position the floor
    /// was waiting on.
    const fn record(&mut self) {
        if self.position == self.floor {
            self.floor += 1;
        }
        self.spent += 1;
    }

    /// Whether the lap has spent the probes it may take out of order.
    const fn probes_are_spent(&self) -> bool {
        self.spent >= self.probe_limit
    }

    /// Returns the scan to the floor once the probe budget is gone, so
    /// the remainder of the lap is a plain sweep over the positions no
    /// jump has examined.
    const fn resume_sweep_when_probes_are_spent(&mut self) {
        if self.probes_are_spent() && self.position != self.floor {
            self.position = self.floor;
        }
    }

    /// Moves clear of a slot a peer claimed first.
    ///
    /// Charges one examination — the slot that was lost — however far
    /// the position moves. A lost CAS is evidence the ring is *busy*,
    /// not that it is empty, so it must not be able to end the lap on
    /// its own. This replaces [`step`](Self::step) for that slot; the
    /// caller does not step again.
    /// Once the probe budget is spent the jump is abandoned in favour
    /// of the sweep: the slots the jumps flew over are the only ones
    /// that can still be holding unexamined work.
    pub(super) const fn skip_contended(&mut self) {
        self.record();
        if self.probes_are_spent() {
            self.position += 1;
        } else {
            self.position += self.cas_skip;
        }
        self.resume_sweep_when_probes_are_spent();
    }

    /// Whether the lap is over.
    ///
    /// The lap ends when every position in it has been examined, not
    /// when a count of moves runs out. A jump that flies over slots
    /// therefore cannot end the lap on their behalf.
    pub(super) const fn exhausted(&self) -> bool {
        self.floor >= self.end
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

    /// A lost CAS is one examination, so it cannot end the lap on its
    /// own however far the position moves.
    #[test]
    fn a_lost_cas_costs_one_examination() {
        let mut budget = ScanBudget::new(0, 16, 128);
        for lost in 1..16 {
            budget.skip_contended();
            assert!(
                !budget.exhausted(),
                "lost CAS {lost} of 15 must leave the lap alive"
            );
        }
    }

    /// A ring of contended slots still terminates.
    #[test]
    fn a_lap_of_contended_slots_still_ends() {
        let mut budget = ScanBudget::new(0, 4, 128);
        for _ in 0..16 {
            if budget.exhausted() {
                return;
            }
            budget.skip_contended();
        }
        panic!("a bounded lap must terminate");
    }

    /// Losing every CAS examines every slot before the lap ends.
    #[test]
    fn a_clamped_jump_visits_every_slot_before_repeating() {
        Coverage::of(16, ScanBudget::skip_contended).assert_complete();
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

    /// Runs a lap under a caller-chosen pattern of moves and records
    /// which slots it examined, so coverage is asserted directly
    /// rather than inferred from the stride's arithmetic.
    struct Coverage {
        seen: Vec<bool>,
        examinations: usize,
    }

    impl Coverage {
        /// Drives a lap of `cap` to exhaustion, applying `advance`
        /// after each examination. Panics rather than looping forever
        /// if the lap fails to terminate.
        fn of(cap: usize, mut advance: impl FnMut(&mut ScanBudget)) -> Self {
            let mut budget = ScanBudget::new(0, cap, 128);
            let mut this = Self {
                seen: vec![false; cap],
                examinations: 0,
            };
            while !budget.exhausted() {
                this.seen[budget.position() % cap] = true;
                this.examinations += 1;
                assert!(
                    this.examinations <= 4 * cap,
                    "a bounded lap must terminate"
                );
                advance(&mut budget);
            }
            this
        }

        /// Asserts the lap looked at every slot in the ring, and cost
        /// no more than the documented `2 * capacity` bound.
        fn assert_complete(&self) {
            let missed: Vec<_> = self
                .seen
                .iter()
                .enumerate()
                .filter(|(_, &s)| !s)
                .map(|(slot, _)| slot)
                .collect();
            assert!(
                missed.is_empty(),
                "the lap ended with slots never examined: {missed:?}"
            );
            assert!(
                self.examinations <= 2 * self.seen.len(),
                "a lap cost {} examinations on a ring of {}",
                self.examinations,
                self.seen.len()
            );
        }
    }

    /// The defect: a `step` and a clamped `skip_contended` cancel
    /// exactly, because the clamp is `-1` modulo the capacity. A
    /// contended slot beside an unpublished one used to walk the scan
    /// back and forth over the same pair until the lap was spent, and
    /// the consumer reported an empty ring holding published items it
    /// had never looked at. Its caller then parked, untimed.
    #[test]
    fn a_lap_alternating_the_two_moves_still_examines_every_slot() {
        let mut contended = false;
        Coverage::of(16, |budget| {
            if contended {
                budget.skip_contended();
            } else {
                budget.step();
            }
            contended = !contended;
        })
        .assert_complete();
    }

    /// The same cancellation, driven from the opposite phase.
    #[test]
    fn a_lap_alternating_from_a_contended_slot_examines_every_slot() {
        let mut contended = true;
        Coverage::of(16, |budget| {
            if contended {
                budget.skip_contended();
            } else {
                budget.step();
            }
            contended = !contended;
        })
        .assert_complete();
    }

    /// A large ring is not exempt: a jump of 128 on a ring of 1024
    /// flies over slots that no later move returns to.
    #[test]
    fn a_large_ring_examines_the_slots_its_jumps_flew_over() {
        let mut contended = false;
        Coverage::of(1024, |budget| {
            if contended {
                budget.skip_contended();
            } else {
                budget.step();
            }
            contended = !contended;
        })
        .assert_complete();
    }

    /// Every phase of every alternating pattern, over the ring sizes
    /// the crate admits, must examine the whole ring.
    #[test]
    fn no_pattern_of_moves_leaves_a_slot_unexamined() {
        for cap in [4usize, 8, 16, 64, 256] {
            for pattern in 1u32..64 {
                let mut move_index = 0u32;
                Coverage::of(cap, |budget| {
                    if pattern >> (move_index % 6) & 1 == 1 {
                        budget.skip_contended();
                    } else {
                        budget.step();
                    }
                    move_index += 1;
                })
                .assert_complete();
            }
        }
    }

    /// An uncontended lap is the fast path and must not have grown:
    /// exactly `capacity` examinations, one per slot, in order.
    #[test]
    fn an_uncontended_lap_costs_exactly_one_pass() {
        let mut budget = ScanBudget::new(0, 16, 128);
        for expected in 0..16 {
            assert_eq!(budget.position(), expected, "the sweep must stay in order");
            assert!(!budget.exhausted(), "slot {expected} not yet examined");
            budget.step();
        }
        assert!(budget.exhausted(), "a full pass ends the lap");
    }

    #[test]
    fn scanning_starts_where_it_was_told_to() {
        let mut budget = ScanBudget::new(7, 8, 4);
        assert_eq!(budget.position(), 7);
        budget.step();
        assert_eq!(budget.position(), 8);
    }
}
