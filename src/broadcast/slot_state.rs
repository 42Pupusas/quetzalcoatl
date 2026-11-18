//! The encoding of a broadcast slot's `sequence` word.

use crate::common::SlotSnapshot;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A broadcast slot's sequence word.
///
/// The word is the slot's whole protocol: a producer claims by storing
/// [`SlotState::VACANT`], then resolves the claim by publishing or
/// abandoning it at its own position. Consumers only ever read.
///
/// See [`SlotState`] for what the stored words mean.
#[repr(transparent)]
pub(super) struct SequenceWord(AtomicUsize);

impl SequenceWord {
    /// A word for a slot that has never been written.
    pub(super) const fn unwritten() -> Self {
        Self(AtomicUsize::new(SlotState::UNWRITTEN))
    }

    /// Publishes the value written at `pos`, making it readable.
    #[inline]
    pub(super) fn publish(&self, pos: usize) {
        self.0
            .store(SlotState::published_word(pos), Ordering::Release);
    }

    /// Marks the reservation at `pos` as resolved without a value.
    ///
    /// The marker names `pos` so a consumer that walks past it does not
    /// read it again as its own on a later lap.
    #[inline]
    pub(super) fn abandon(&self, pos: usize) {
        self.0
            .store(SlotState::abandoned_word(pos), Ordering::Release);
    }

    /// Takes the slot for a new claim, reporting what it held.
    ///
    /// `Acquire` on the swap pairs with the publishing producer's
    /// `Release`, so a returned [`SlotState::Published`] guarantees the
    /// value it names is visible to this thread — which is what makes
    /// dropping it sound.
    #[inline]
    pub(super) fn claim_taking_previous(&self) -> SlotState {
        SlotState::decode(self.0.swap(SlotState::VACANT, Ordering::Acquire))
    }

    /// Takes the slot for a new claim, discarding what it held.
    ///
    /// For a `T` with no drop glue, where the previous state cannot
    /// oblige this producer to do anything.
    #[inline]
    pub(super) fn claim(&self) {
        self.0.store(SlotState::VACANT, Ordering::Relaxed);
    }

    /// Whether a producer's reservation on this slot is still unresolved.
    ///
    /// A slot never written has no reservation, so it is not one. That
    /// is what lets a producer tell a first-lap slot from a reused one
    /// by the slot alone: the position says nothing once it has rolled
    /// over `usize`.
    #[inline]
    pub(super) fn is_claim_in_progress(&self) -> bool {
        self.0.load(Ordering::Acquire) == SlotState::VACANT
    }

    /// Classifies the slot for a consumer reading `pos`.
    #[inline]
    pub(super) fn classify<T>(
        &self,
        data: &UnsafeCell<MaybeUninit<T>>,
        pos: usize,
    ) -> SlotSnapshot<*const MaybeUninit<T>> {
        SlotState::classify(&self.0, data, pos)
    }

    /// The state at teardown, when the ring is uniquely borrowed.
    pub(super) fn state_mut(&mut self) -> SlotState {
        SlotState::decode(*self.0.get_mut())
    }
}

/// What a broadcast slot's sequence word says about the slot.
///
/// Broadcast consumers cannot clear a marker after reading it — every
/// other consumer still has to see the same slot. A marker therefore has
/// to name the position it belongs to, or a consumer cannot tell "the
/// reservation at my head was abandoned" from "a reservation at some
/// earlier lap was abandoned and I have already walked past it". With a
/// positionless marker and `cap == 1` the latter reads as the former on
/// every call, so the consumer advances forever instead of reporting an
/// empty ring.
///
/// The low two bits carry the state and the rest the position:
///
/// | word            | state              |
/// |-----------------|--------------------|
/// | `0`             | [`Vacant`]         |
/// | `pos * 4 + 1`   | [`Published`]      |
/// | `pos * 4 + 2`   | [`Abandoned`]      |
/// | `3`             | [`Unwritten`]      |
///
/// `pos * 4 + 3` is never produced by a position, so `3` cannot be
/// mistaken for a resolved word.
///
/// The multiplication wraps, so the top bits of a position are lost
/// and the word cannot be decoded back to one. A consumer instead
/// compares the word it loaded with the word it expects for its own
/// position. Two positions collide only when they differ by a multiple
/// of `2^(usize::BITS - 2)`, far beyond the capacity that separates a
/// slot's occupants.
///
/// A published or abandoned word always has a nonzero state, so
/// neither can equal [`VACANT`](Self::VACANT) at any position. The
/// earlier `pos * 2 + 2` met zero at `pos == usize::MAX / 2`, which
/// is push number 2^31 on a 32-bit target.
///
/// [`Vacant`]: Self::Vacant
/// [`Published`]: Self::Published
/// [`Abandoned`]: Self::Abandoned
/// [`Unwritten`]: Self::Unwritten
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum SlotState {
    /// Claimed by a producer that has not yet published or abandoned
    /// it.
    Vacant,
    /// Never claimed. Holds no value and has no reservation.
    Unwritten,
    /// Holds a value published at some position.
    Published,
    /// A reservation at some position was dropped without a commit.
    Abandoned,
}

impl SlotState {
    /// The word stored by a claim, before the producer resolves it.
    pub(super) const VACANT: usize = 0;
    const PUBLISHED: usize = 1;
    const ABANDONED: usize = 2;
    const UNWRITTEN: usize = 3;
    const STATE_MASK: usize = 3;

    const fn encode(pos: usize, state: usize) -> usize {
        pos.wrapping_mul(4).wrapping_add(state)
    }

    /// The word published by a commit at `pos`.
    pub(super) const fn published_word(pos: usize) -> usize {
        Self::encode(pos, Self::PUBLISHED)
    }

    /// The word stored when a reservation at `pos` is abandoned.
    pub(super) const fn abandoned_word(pos: usize) -> usize {
        Self::encode(pos, Self::ABANDONED)
    }

    pub(super) const fn decode(word: usize) -> Self {
        match word & Self::STATE_MASK {
            0 => Self::Vacant,
            Self::PUBLISHED => Self::Published,
            Self::ABANDONED => Self::Abandoned,
            _ => Self::Unwritten,
        }
    }

    /// Whether this state owns a live `T` in the slot's storage.
    ///
    /// Only a published value does. `RingBuffer::drop` and the producer's
    /// overwrite path both use this to decide whether to run `T::drop`.
    pub(super) const fn holds_value(self) -> bool {
        matches!(self, Self::Published)
    }

    /// Classifies the slot at `pos` for a consumer reading that position.
    ///
    /// A marker naming any other position belongs to a different lap, so
    /// it reads as [`SlotSnapshot::NotReady`] — the consumer has either
    /// already passed it or has not reached it.
    pub(super) fn classify<T>(
        sequence: &AtomicUsize,
        data: &UnsafeCell<MaybeUninit<T>>,
        pos: usize,
    ) -> SlotSnapshot<*const MaybeUninit<T>> {
        let word = sequence.load(Ordering::Acquire);
        if word == Self::published_word(pos) {
            SlotSnapshot::Ready(data.get().cast_const())
        } else if word == Self::abandoned_word(pos) {
            SlotSnapshot::Tombstoned
        } else {
            SlotSnapshot::NotReady
        }
    }
}

#[cfg(test)]
mod tests;
