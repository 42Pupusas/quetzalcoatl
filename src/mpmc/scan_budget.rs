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

use crate::common::position_order::PositionOrder;

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
            end: start.wrapping_add(capacity),
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
        self.position = self.position.wrapping_add(1);
        self.resume_sweep_when_probes_are_spent();
    }

    /// Charges the position now being examined against the lap, and
    /// advances the coverage floor when this is the position the floor
    /// was waiting on.
    const fn record(&mut self) {
        if self.position == self.floor {
            self.floor = self.floor.wrapping_add(1);
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
        let stride = if self.probes_are_spent() {
            1
        } else {
            self.cas_skip
        };
        self.position = self.position.wrapping_add(stride);
        self.resume_sweep_when_probes_are_spent();
    }

    /// Whether the lap is over.
    ///
    /// The lap ends when every position in it has been examined, not
    /// when a count of moves runs out. A jump that flies over slots
    /// therefore cannot end the lap on their behalf.
    pub(super) fn exhausted(&self) -> bool {
        !self.end.is_after(self.floor)
    }
}

#[cfg(test)]
mod tests;
