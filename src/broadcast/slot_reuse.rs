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
/// The test is local: an unresolved claim on the slot means its
/// occupant is still being written. A slot never used has no claim, so
/// the first lap needs no special case. Testing `pos < cap` for it
/// instead would hold again after `pos` rolled over `usize`, and skip
/// the check for a slot that does have a claim outstanding.
pub(super) struct SlotReuse;

impl SlotReuse {
    /// Whether the previous occupant of the slot has resolved.
    pub(super) fn prior_occupant_resolved(sequence: &SequenceWord) -> bool {
        !sequence.is_claim_in_progress()
    }
}

#[cfg(test)]
mod tests;
