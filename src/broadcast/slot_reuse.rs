//! Producer-side ownership of a slot's previous occupant.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Sequence value of a slot that a producer has claimed but not yet
/// published or tombstoned.
const IN_PROGRESS: usize = 0;

/// Whether the slot backing a position may be reused yet.
///
/// The consumer floor answers "have the consumers read the previous
/// occupant", which is necessary but not sufficient. A reservation is
/// outstanding storage the producers own: `reserve` claims position
/// `pos` and only `commit` or the guard's `Drop` resolves it.
///
/// Consumer progress cannot speak for that window. A consumer that
/// subscribes after a reservation is taken starts at `tail`, already
/// past the reserved position, so the floor legitimately sits ahead of
/// a slot that is still being written. Reusing that slot would hand two
/// producers the same storage.
///
/// The test is local: the slot backing `pos` holds position
/// `pos - cap`, and [`IN_PROGRESS`] there means its occupant is
/// unresolved.
pub(super) struct SlotReuse {
    cap: usize,
}

impl SlotReuse {
    pub(super) const fn new(cap: usize) -> Self {
        Self { cap }
    }

    /// Whether the previous occupant of the slot backing `pos` has
    /// resolved. Positions in the first lap have no previous occupant.
    pub(super) fn prior_occupant_resolved(&self, pos: usize, sequence: &AtomicUsize) -> bool {
        pos < self.cap || sequence.load(Ordering::Acquire) != IN_PROGRESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_lap_has_no_prior_occupant() {
        let reuse = SlotReuse::new(4);
        let seq = AtomicUsize::new(IN_PROGRESS);
        assert!(reuse.prior_occupant_resolved(0, &seq));
        assert!(reuse.prior_occupant_resolved(3, &seq));
    }

    #[test]
    fn outstanding_reservation_blocks_reuse() {
        let reuse = SlotReuse::new(4);
        let seq = AtomicUsize::new(IN_PROGRESS);
        assert!(!reuse.prior_occupant_resolved(4, &seq));
        assert!(!reuse.prior_occupant_resolved(400, &seq));
    }

    #[test]
    fn published_or_tombstoned_occupant_permits_reuse() {
        let reuse = SlotReuse::new(4);
        let published = AtomicUsize::new(1);
        assert!(reuse.prior_occupant_resolved(4, &published));
        let tombstoned = AtomicUsize::new(crate::common::TOMBSTONE);
        assert!(reuse.prior_occupant_resolved(4, &tombstoned));
    }
}
