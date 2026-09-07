//! The encoding of an MPMC slot's `ready` word.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::capacity::Capacity;

/// What a slot's `ready` word says about the slot.
///
/// The word is `slot + round * capacity + state`, so it carries both the
/// round it describes and the slot's state within that round. Each
/// variant names the *round position* — the logical position, one full
/// lap wide, that the slot currently stands for.
///
/// The state occupies the low end of the encoding, which is why it must
/// stay below the capacity: at `capacity <= 2` a [`Claimed`] word is
/// indistinguishable from a [`Free`] one a round later. That is the
/// aliasing the `capacity >= 4` assert in
/// [`RingBuffer::new`](super::RingBuffer::new) exists to prevent.
///
/// [`Claimed`]: Self::Claimed
/// [`Free`]: Self::Free
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum ReadyState {
    /// No value: the producer for this round position may write here,
    /// once `done` confirms the previous round's consumer has finished.
    Free(usize),
    /// Holds the value published at this round position, unclaimed.
    Published(usize),
    /// A consumer won the claim and owns the slot, or a departing
    /// producer abandoned its reservation. The `done` word tells the two
    /// apart — see [`DoneWord`](super::done_word::DoneWord).
    Claimed(usize),
}

impl ReadyState {
    /// The logical position this state describes.
    pub(super) const fn round_pos(self) -> usize {
        match self {
            Self::Free(pos) | Self::Published(pos) | Self::Claimed(pos) => pos,
        }
    }

    /// Whether a consumer may try to claim this slot.
    pub(super) const fn is_published(self) -> bool {
        matches!(self, Self::Published(_))
    }
}

/// A slot's `ready` word: the round-tagged tri-state a producer
/// publishes and a consumer claims.
///
/// `repr(transparent)` over the atomic keeps the ring's `ready` array
/// eight words to a cache line, which is what makes a consumer's scan
/// amortize one line load across eight slots.
#[repr(transparent)]
pub(super) struct ReadyWord(AtomicUsize);

impl ReadyWord {
    const PUBLISHED: usize = 1;
    const CLAIMED: usize = 2;

    /// The initial word for `slot`: free for round zero.
    pub(super) const fn free_at(slot: usize) -> Self {
        Self(AtomicUsize::new(slot))
    }

    /// Reads and decodes the word for the slot at index `slot`.
    pub(super) fn state(&self, slot: usize, capacity: Capacity) -> ReadyState {
        Self::decode(self.0.load(Ordering::Acquire), slot, capacity)
    }

    /// Decodes the word through a unique borrow, for the teardown path.
    pub(super) fn state_mut(&mut self, slot: usize, capacity: Capacity) -> ReadyState {
        Self::decode(*self.0.get_mut(), slot, capacity)
    }

    const fn decode(word: usize, slot: usize, capacity: Capacity) -> ReadyState {
        let state = capacity.wrap(word.wrapping_sub(slot));
        let round_pos = word.wrapping_sub(state);
        match state {
            0 => ReadyState::Free(round_pos),
            Self::PUBLISHED => ReadyState::Published(round_pos),
            _ => ReadyState::Claimed(round_pos),
        }
    }

    /// Publishes the value written at `pos`, making it claimable.
    ///
    /// `SeqCst` rather than `Release` drains the store buffer, so the
    /// `SeqCst` load of `consumer_park.wake` inside the following
    /// `wake_one` cannot miss a consumer that has just parked. This is
    /// the mirror of [`DoneWord::release`](super::done_word::DoneWord::release):
    /// both directions need the drain to close the Dekker race against
    /// the peer's parking sequence.
    pub(super) fn publish(&self, pos: usize) {
        self.0.store(pos + Self::PUBLISHED, Ordering::SeqCst);
    }

    /// Takes exclusive ownership of the value published at `round_pos`.
    ///
    /// Returns whether this caller won; a losing consumer must rescan,
    /// since the winner has already moved the word on.
    pub(super) fn claim(&self, round_pos: usize) -> bool {
        self.0
            .compare_exchange(
                round_pos + Self::PUBLISHED,
                round_pos + Self::CLAIMED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Gives up a reservation at `pos` that was never written.
    ///
    /// The word lands in the same state a consumer's claim would leave;
    /// paired with a `done` release it says "this round is finished and
    /// left no value", which is exactly what teardown needs to hear.
    pub(super) fn abandon(&self, pos: usize) {
        self.0.store(pos + Self::CLAIMED, Ordering::Release);
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
    fn a_fresh_word_is_free_for_its_own_slot() {
        for slot in 0..cap().get() {
            let word = ReadyWord::free_at(slot);
            assert_eq!(word.state(slot, cap()), ReadyState::Free(slot));
        }
    }

    #[test]
    fn publishing_marks_the_slot_as_holding_that_positions_value() {
        let word = ReadyWord::free_at(3);
        word.publish(3);
        assert_eq!(word.state(3, cap()), ReadyState::Published(3));
        assert!(word.state(3, cap()).is_published());
    }

    #[test]
    fn a_claim_takes_the_published_value_exactly_once() {
        let word = ReadyWord::free_at(3);
        word.publish(3);
        assert!(word.claim(3));
        assert!(!word.claim(3));
        assert_eq!(word.state(3, cap()), ReadyState::Claimed(3));
    }

    #[test]
    fn a_claim_of_an_unpublished_slot_fails() {
        let word = ReadyWord::free_at(3);
        assert!(!word.claim(3));
        assert_eq!(word.state(3, cap()), ReadyState::Free(3));
    }

    #[test]
    fn an_abandoned_reservation_reads_as_claimed() {
        let word = ReadyWord::free_at(5);
        word.abandon(5);
        assert_eq!(word.state(5, cap()), ReadyState::Claimed(5));
    }

    #[test]
    fn the_three_states_are_distinct_within_a_round() {
        let slot = 2;
        let free = ReadyWord::free_at(slot);
        let published = ReadyWord::free_at(slot);
        published.publish(slot);
        let claimed = ReadyWord::free_at(slot);
        claimed.publish(slot);
        assert!(claimed.claim(slot));

        assert_eq!(free.state(slot, cap()), ReadyState::Free(slot));
        assert_eq!(published.state(slot, cap()), ReadyState::Published(slot));
        assert_eq!(claimed.state(slot, cap()), ReadyState::Claimed(slot));
    }

    #[test]
    fn a_later_round_decodes_to_its_own_position() {
        let slot = 1;
        let capacity = cap();
        for round in 0..4 {
            let round_pos = slot + round * capacity.get();
            let word = ReadyWord::free_at(slot);
            word.publish(round_pos);
            assert_eq!(
                word.state(slot, capacity),
                ReadyState::Published(round_pos),
                "round {round}"
            );
        }
    }

    #[test]
    fn the_state_never_reaches_the_capacity() {
        let capacity = cap();
        assert!(
            ReadyWord::CLAIMED < capacity.get(),
            "a claimed word must not alias the next round's free word"
        );
    }

    #[test]
    fn a_unique_borrow_decodes_the_same_state() {
        let mut word = ReadyWord::free_at(4);
        word.publish(4);
        assert_eq!(word.state_mut(4, cap()), ReadyState::Published(4));
    }

    #[test]
    fn the_word_is_the_width_of_the_atomic_it_wraps() {
        assert_eq!(
            std::mem::size_of::<ReadyWord>(),
            std::mem::size_of::<AtomicUsize>()
        );
        assert_eq!(
            std::mem::align_of::<ReadyWord>(),
            std::mem::align_of::<AtomicUsize>()
        );
    }
}
