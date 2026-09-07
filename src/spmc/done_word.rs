//! The encoding of an SPMC slot's `done` word.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::capacity::Capacity;

/// A slot's `done` word: the handshake that hands a slot from the
/// consumer that read it back to the sole producer.
///
/// The word holds the position the slot is *next free for*. A consumer
/// finishing at `pos` stores `pos + capacity`, one full lap ahead, and
/// the producer waits to read its own position back. Both halves are
/// the same fact stated from opposite ends, which is why they belong
/// together here.
///
/// The two-step read — move the value out, *then* release — is what
/// makes the word necessary. Releasing first would let the producer
/// refill a slot whose old value is still being read.
///
/// # Ordering
///
/// The release is `Release`, and every caller follows it with
/// [`SoleParker::wake`](crate::common::sole_parker::SoleParker::wake),
/// whose leading `SeqCst` fence drains the store buffer before the
/// parked flag is sampled. That fence is what closes the missed-wake
/// window; the store itself does not need to.
///
/// The MPMC twin stores `SeqCst` instead and then calls the equally
/// fenced `wake_one`, which makes its stronger ordering redundant
/// rather than load-bearing. The two are kept apart here because the
/// pairing is a property of each ring's wake path, not a shared fact.
#[repr(transparent)]
pub(super) struct DoneWord(AtomicUsize);

impl DoneWord {
    /// The initial word for `slot`: as if a prior lap had already
    /// released it, so the first producer's check passes immediately.
    pub(super) const fn released_for(slot: usize) -> Self {
        Self(AtomicUsize::new(slot))
    }

    /// Whether the slot is free for the producer writing at `pos`.
    #[inline]
    pub(super) fn is_free_for(&self, pos: usize) -> bool {
        self.0.load(Ordering::Acquire) == pos
    }

    /// Hands the slot to the producer one lap ahead of `pos`.
    #[inline]
    pub(super) fn release(&self, pos: usize, capacity: Capacity) {
        self.0.store(pos + capacity.get(), Ordering::Release);
    }

    /// Whether the slot still owes its value a destructor at teardown.
    ///
    /// A position inside the occupied range holds a live value unless
    /// its consumer already released it; teardown asks this to avoid
    /// dropping a value twice.
    pub(super) fn holds_unconsumed_value_at(&mut self, pos: usize, capacity: Capacity) -> bool {
        *self.0.get_mut() != pos + capacity.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap() -> Capacity {
        Capacity::exact(8)
    }

    #[test]
    fn a_fresh_word_frees_the_slot_for_its_first_lap() {
        for slot in 0..cap().get() {
            let word = DoneWord::released_for(slot);
            assert!(word.is_free_for(slot));
        }
    }

    #[test]
    fn a_fresh_word_frees_nothing_but_the_first_lap() {
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
    fn a_released_slot_is_not_free_for_the_position_that_just_ended() {
        let word = DoneWord::released_for(3);
        word.release(3, cap());
        assert!(!word.is_free_for(3));
    }

    #[test]
    fn successive_laps_each_hand_the_slot_one_lap_on() {
        let capacity = cap();
        let slot = 2;
        let word = DoneWord::released_for(slot);
        for lap in 0..5 {
            let pos = slot + lap * capacity.get();
            assert!(word.is_free_for(pos), "lap {lap}");
            word.release(pos, capacity);
        }
    }

    #[test]
    fn an_unreleased_slot_still_owes_its_value_a_destructor() {
        let capacity = cap();
        let mut word = DoneWord::released_for(4);
        assert!(word.holds_unconsumed_value_at(4, capacity));
    }

    #[test]
    fn a_released_slot_owes_nothing() {
        let capacity = cap();
        let mut word = DoneWord::released_for(4);
        word.release(4, capacity);
        assert!(!word.holds_unconsumed_value_at(4, capacity));
    }

    #[test]
    fn a_release_for_one_lap_does_not_answer_for_the_next() {
        let capacity = cap();
        let mut word = DoneWord::released_for(4);
        word.release(4, capacity);
        assert!(word.holds_unconsumed_value_at(4 + capacity.get(), capacity));
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
