//! Producer-side ownership of a slot's previous occupant.

use super::slot_state::SequenceWord;

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
/// `pos - cap`, and an unresolved claim there means its occupant is
/// still being written.
pub(super) struct SlotReuse {
    cap: usize,
}

impl SlotReuse {
    pub(super) const fn new(cap: usize) -> Self {
        Self { cap }
    }

    /// Whether the previous occupant of the slot backing `pos` has
    /// resolved. Positions in the first lap have no previous occupant.
    pub(super) fn prior_occupant_resolved(&self, pos: usize, sequence: &SequenceWord) -> bool {
        pos < self.cap || !sequence.is_claim_in_progress()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_lap_has_no_prior_occupant() {
        let reuse = SlotReuse::new(4);
        let seq = SequenceWord::vacant();
        assert!(reuse.prior_occupant_resolved(0, &seq));
        assert!(reuse.prior_occupant_resolved(3, &seq));
    }

    #[test]
    fn outstanding_reservation_blocks_reuse() {
        let reuse = SlotReuse::new(4);
        let seq = SequenceWord::vacant();
        assert!(!reuse.prior_occupant_resolved(4, &seq));
        assert!(!reuse.prior_occupant_resolved(400, &seq));
    }

    #[test]
    fn published_or_abandoned_occupant_permits_reuse() {
        let reuse = SlotReuse::new(4);

        let published = SequenceWord::vacant();
        published.publish(0);
        assert!(reuse.prior_occupant_resolved(4, &published));

        let abandoned = SequenceWord::vacant();
        abandoned.abandon(0);
        assert!(reuse.prior_occupant_resolved(4, &abandoned));
    }
}
