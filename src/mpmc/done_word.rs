//! The encoding of an MPMC slot's `done` word.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::capacity::Capacity;

/// A slot's `done` word: the handshake that hands a slot from the
/// consumer of one round to the producer of the next.
///
/// The word holds the position the slot is *next free for*. A consumer
/// finishing at `pos` stores `pos + capacity`, one full lap ahead, and
/// the producer for that later position waits to read its own position
/// back. Both halves are the same fact stated from opposite ends, which
/// is why they belong together here.
///
/// The two-step read — move the value out, *then* release — is what
/// makes the word necessary. Releasing first would let the next round's
/// producer refill a slot whose old value is still being read.
#[repr(transparent)]
pub(super) struct DoneWord(AtomicUsize);

impl DoneWord {
    /// The initial word for `slot`: as if round −1 had already released
    /// it, so the round-zero producer's check passes immediately.
    pub(super) const fn released_for(slot: usize) -> Self {
        Self(AtomicUsize::new(slot))
    }

    /// Whether the slot is free for a producer writing at `pos`.
    pub(super) fn is_free_for(&self, pos: usize) -> bool {
        self.0.load(Ordering::Acquire) == pos
    }

    /// Hands the slot to the producer one lap ahead of `round_pos`.
    ///
    /// `SeqCst` rather than `Release` drains the store buffer, so the
    /// `SeqCst` load of `producer_park.wake` inside the following
    /// `wake_one` cannot be satisfied before this store is globally
    /// visible — otherwise the wake could miss a producer that has just
    /// parked on this very slot. A leading `SeqCst` fence in `wake_one`
    /// would be equivalent; putting it on the store keeps the pairing
    /// local to the call site.
    pub(super) fn release(&self, round_pos: usize, capacity: Capacity) {
        self.0
            .store(round_pos + capacity.get(), Ordering::SeqCst);
    }

    /// Whether the consumer that claimed `round_pos` released the slot.
    ///
    /// A slot whose `ready` word reads as claimed still holds a live
    /// value until this turns true: the claim and the release bracket
    /// the window in which the value is being read out. Teardown asks
    /// this to decide whether it still owes the value a destructor.
    pub(super) fn was_released_after(&mut self, round_pos: usize, capacity: Capacity) -> bool {
        *self.0.get_mut() == round_pos + capacity.get()
    }

    /// The raw word, for diagnostics.
    pub(super) fn word(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap() -> Capacity {
        Capacity::exact(8)
    }

    #[test]
    fn a_fresh_word_frees_the_slot_for_its_first_round() {
        for slot in 0..cap().get() {
            let word = DoneWord::released_for(slot);
            assert!(word.is_free_for(slot));
        }
    }

    #[test]
    fn a_fresh_word_frees_nothing_but_the_first_round() {
        let word = DoneWord::released_for(3);
        assert!(!word.is_free_for(3 + cap().get()));
    }

    #[test]
    fn releasing_frees_the_slot_one_lap_ahead() {
        let word = DoneWord::released_for(3);
        word.release(3, cap());
        assert!(word.is_free_for(3 + cap().get()));
    }

    #[test]
    fn a_released_slot_is_not_free_for_the_round_that_just_ended() {
        let word = DoneWord::released_for(3);
        word.release(3, cap());
        assert!(!word.is_free_for(3));
    }

    #[test]
    fn successive_rounds_each_hand_the_slot_one_lap_on() {
        let capacity = cap();
        let slot = 2;
        let word = DoneWord::released_for(slot);
        for round in 0..5 {
            let round_pos = slot + round * capacity.get();
            assert!(word.is_free_for(round_pos), "round {round}");
            word.release(round_pos, capacity);
        }
    }

    #[test]
    fn a_release_is_visible_to_the_teardown_check() {
        let capacity = cap();
        let mut word = DoneWord::released_for(4);
        assert!(!word.was_released_after(4, capacity));
        word.release(4, capacity);
        assert!(word.was_released_after(4, capacity));
    }

    #[test]
    fn a_release_for_another_round_does_not_answer_for_this_one() {
        let capacity = cap();
        let mut word = DoneWord::released_for(4);
        word.release(4, capacity);
        assert!(!word.was_released_after(4 + capacity.get(), capacity));
    }

    #[test]
    fn the_word_is_the_width_of_the_atomic_it_wraps() {
        assert_eq!(
            std::mem::size_of::<DoneWord>(),
            std::mem::size_of::<AtomicUsize>()
        );
        assert_eq!(
            std::mem::align_of::<DoneWord>(),
            std::mem::align_of::<AtomicUsize>()
        );
    }
}
