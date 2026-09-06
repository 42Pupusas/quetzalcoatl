//! The encoding of a broadcast slot's `sequence` word.

use crate::common::SlotSnapshot;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

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
/// The parity of the word carries the distinction:
///
/// | word            | state              |
/// |-----------------|--------------------|
/// | `0`             | [`Vacant`]         |
/// | `pos * 2 + 1`   | [`Published`]      |
/// | `pos * 2 + 2`   | [`Abandoned`]      |
///
/// [`Vacant`]: Self::Vacant
/// [`Published`]: Self::Published
/// [`Abandoned`]: Self::Abandoned
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum SlotState {
    /// Never written, or claimed by a producer that has not yet
    /// published or abandoned it.
    Vacant,
    /// Holds the value published at this position.
    Published(usize),
    /// A reservation at this position was dropped without a commit.
    Abandoned(usize),
}

impl SlotState {
    /// The word stored by a claim, before the producer resolves it.
    pub(super) const VACANT: usize = 0;

    /// The word published by a commit at `pos`.
    pub(super) const fn published_word(pos: usize) -> usize {
        pos * 2 + 1
    }

    /// The word stored when a reservation at `pos` is abandoned.
    pub(super) const fn abandoned_word(pos: usize) -> usize {
        pos * 2 + 2
    }

    pub(super) const fn decode(word: usize) -> Self {
        if word == Self::VACANT {
            Self::Vacant
        } else if word % 2 == 1 {
            Self::Published((word - 1) / 2)
        } else {
            Self::Abandoned((word - 2) / 2)
        }
    }

    /// Whether this state owns a live `T` in the slot's storage.
    ///
    /// Only a published value does. `RingBuffer::drop` and the producer's
    /// overwrite path both use this to decide whether to run `T::drop`.
    pub(super) const fn holds_value(self) -> bool {
        matches!(self, Self::Published(_))
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
        match Self::decode(sequence.load(Ordering::Acquire)) {
            Self::Published(at) if at == pos => SlotSnapshot::Ready(data.get().cast_const()),
            Self::Abandoned(at) if at == pos => SlotSnapshot::Tombstoned,
            _ => SlotSnapshot::NotReady,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_round_trip_through_decode() {
        for pos in [0usize, 1, 2, 7, 4096] {
            assert_eq!(
                SlotState::decode(SlotState::published_word(pos)),
                SlotState::Published(pos)
            );
            assert_eq!(
                SlotState::decode(SlotState::abandoned_word(pos)),
                SlotState::Abandoned(pos)
            );
        }
        assert_eq!(SlotState::decode(SlotState::VACANT), SlotState::Vacant);
    }

    /// The encodings must not alias: a claim, a commit and an abandoned
    /// reservation at the same position are three distinct words.
    #[test]
    fn the_three_states_are_distinct_at_every_position() {
        for pos in [0usize, 1, 2, 7, 4096] {
            let published = SlotState::published_word(pos);
            let abandoned = SlotState::abandoned_word(pos);
            assert_ne!(published, abandoned);
            assert_ne!(published, SlotState::VACANT);
            assert_ne!(abandoned, SlotState::VACANT);
        }
    }

    /// The bug this encoding exists to fix: with `cap == 1` a consumer
    /// that skips an abandoned position must not read the same marker as
    /// abandoned again at its next head.
    #[test]
    fn an_abandoned_marker_names_only_its_own_position() {
        let word = SlotState::abandoned_word(0);
        assert_eq!(SlotState::decode(word), SlotState::Abandoned(0));
        assert_ne!(SlotState::decode(word), SlotState::Abandoned(1));
    }

    #[test]
    fn only_published_slots_hold_a_value() {
        assert!(SlotState::Published(3).holds_value());
        assert!(!SlotState::Abandoned(3).holds_value());
        assert!(!SlotState::Vacant.holds_value());
    }

    #[test]
    fn classify_matches_only_the_named_position() {
        let data: UnsafeCell<MaybeUninit<u32>> = UnsafeCell::new(MaybeUninit::uninit());
        let seq = AtomicUsize::new(SlotState::abandoned_word(5));
        assert!(matches!(
            SlotState::classify(&seq, &data, 5),
            SlotSnapshot::Tombstoned
        ));
        assert!(matches!(
            SlotState::classify(&seq, &data, 6),
            SlotSnapshot::NotReady
        ));

        seq.store(SlotState::published_word(5), Ordering::Release);
        assert!(matches!(
            SlotState::classify(&seq, &data, 5),
            SlotSnapshot::Ready(_)
        ));
        assert!(matches!(
            SlotState::classify(&seq, &data, 4),
            SlotSnapshot::NotReady
        ));
    }
}
