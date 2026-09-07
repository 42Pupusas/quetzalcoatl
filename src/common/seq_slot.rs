//! The per-slot sequence encoding, and how a consumer reads it.
//!
//! A slot needs to answer "is there a value here for position `pos`?"
//! in one atomic load, for a position that keeps growing while the
//! storage wraps. A ready flag cannot: it says a value is present but
//! not which lap it belongs to, so a consumer at position `pos` cannot
//! distinguish the value it wants from one the producer published a
//! lap later into the same slot.
//!
//! The sequence number answers both at once. `pos * 2` is free for the
//! producer at `pos`, `pos * 2 + 1` holds that position's value, and
//! the doubling keeps the two apart even at capacity 1, where
//! consecutive positions share the only slot. [`TOMBSTONE`] is a third
//! value outside the scheme, marking a position reserved and then
//! abandoned, which consumers skip.
//!
//! Reading it is a three-way branch, and [`SlotSnapshot`] exists
//! because that branch was written out identically in `pop`,
//! `pop_ref`, `drain` and `drain_up_to` across two rings. It is
//! computed and matched immediately, never stored, so it costs the
//! same load and comparisons the hand-rolled chains did.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Marks a slot reserved but never committed, at any position.
///
/// `usize::MAX` cannot collide with a real `pos * 2 + 1`: reaching it
/// would take 2^63 pushes.
pub const TOMBSTONE: usize = usize::MAX;

/// A slot carrying its own publication state as a sequence number.
pub struct SeqSlot<T> {
    pub data: UnsafeCell<MaybeUninit<T>>,
    pub sequence: AtomicUsize,
}

impl<T> SeqSlot<T> {
    /// Classifies this slot for logical position `pos` from a single
    /// atomic load.
    #[inline]
    pub fn classify(&self, pos: usize) -> SlotSnapshot<*const MaybeUninit<T>> {
        SlotSnapshot::classify(&self.sequence, &self.data, pos)
    }
}

/// What a slot holds for one position, as of a single load.
pub enum SlotSnapshot<D> {
    /// A value published at this position. Carries a raw pointer
    /// rather than `&T` because callers disagree about what to do with
    /// it: `pop` and `drain` move the value out, `pop_ref` borrows it.
    Ready(D),
    /// Reserved and then abandoned — no value was ever written, and
    /// consumers must release the position and move past it.
    Tombstoned,
    /// Nothing to read at this position yet: either free, or a
    /// producer is mid-write.
    NotReady,
}

impl<T> SlotSnapshot<*const MaybeUninit<T>> {
    /// Classifies a raw sequence/data pair for position `pos`.
    ///
    /// Shared with the broadcast ring's slot, which uses the same
    /// `pos * 2 + 1` and [`TOMBSTONE`] values but a different "free"
    /// encoding. That difference does not reach here: anything neither
    /// published at `pos` nor tombstoned is [`NotReady`](Self::NotReady).
    #[inline]
    pub fn classify(seq: &AtomicUsize, data: &UnsafeCell<MaybeUninit<T>>, pos: usize) -> Self {
        let seq = seq.load(Ordering::Acquire);
        if seq == TOMBSTONE {
            Self::Tombstoned
        } else if seq == pos * 2 + 1 {
            Self::Ready(data.get().cast_const())
        } else {
            Self::NotReady
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl<D> SlotSnapshot<D> {
        fn is_ready(&self) -> bool {
            matches!(self, Self::Ready(_))
        }

        fn is_tombstoned(&self) -> bool {
            matches!(self, Self::Tombstoned)
        }

        fn is_not_ready(&self) -> bool {
            matches!(self, Self::NotReady)
        }
    }

    fn slot_at(seq: usize) -> SeqSlot<u32> {
        SeqSlot {
            data: UnsafeCell::new(MaybeUninit::new(7)),
            sequence: AtomicUsize::new(seq),
        }
    }

    #[test]
    fn a_slot_holding_its_positions_value_reads_as_ready() {
        for pos in [0usize, 1, 2, 63, 4096] {
            assert!(slot_at(pos * 2 + 1).classify(pos).is_ready());
        }
    }

    #[test]
    fn a_free_slot_reads_as_not_ready() {
        for pos in [0usize, 1, 2, 63, 4096] {
            assert!(slot_at(pos * 2).classify(pos).is_not_ready());
        }
    }

    #[test]
    fn a_tombstone_reads_as_tombstoned_at_every_position() {
        for pos in [0usize, 1, 2, 63, 4096] {
            assert!(slot_at(TOMBSTONE).classify(pos).is_tombstoned());
        }
    }

    #[test]
    fn a_value_published_for_another_position_is_not_ready_here() {
        let slot = slot_at(5 * 2 + 1);
        assert!(slot.classify(5).is_ready());
        for other in [0usize, 4, 6, 10] {
            assert!(slot.classify(other).is_not_ready());
        }
    }

    #[test]
    fn consecutive_positions_never_share_an_encoding() {
        let mut seen = std::collections::HashSet::new();
        for pos in 0..64usize {
            assert!(seen.insert(pos * 2), "free value repeated at {pos}");
            assert!(seen.insert(pos * 2 + 1), "ready value repeated at {pos}");
        }
        assert!(!seen.contains(&TOMBSTONE));
    }

    #[test]
    fn a_ready_snapshot_points_at_the_slots_own_storage() {
        let slot = slot_at(1);
        let SlotSnapshot::Ready(ptr) = slot.classify(0) else {
            panic!("expected Ready");
        };
        assert_eq!(ptr, slot.data.get().cast_const());
        // SAFETY: slot_at initialises the payload, and classify reported it ready.
        assert_eq!(unsafe { (*ptr).assume_init() }, 7);
    }
}
