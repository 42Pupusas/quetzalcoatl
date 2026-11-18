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
        self.0
            .store(pos.wrapping_add(Self::PUBLISHED), Ordering::SeqCst);
    }

    /// Takes exclusive ownership of the value published at `round_pos`.
    ///
    /// Returns whether this caller won; a losing consumer must rescan,
    /// since the winner has already moved the word on.
    pub(super) fn claim(&self, round_pos: usize) -> bool {
        self.0
            .compare_exchange(
                round_pos.wrapping_add(Self::PUBLISHED),
                round_pos.wrapping_add(Self::CLAIMED),
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
        self.0
            .store(pos.wrapping_add(Self::CLAIMED), Ordering::Release);
    }

    /// The raw word, for diagnostics.
    pub(super) fn word(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests;
