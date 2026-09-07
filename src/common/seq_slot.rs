//! An MPSC slot: storage plus the sequence word that guards it.
//!
//! The encoding and its transitions belong to
//! [`SlotSequence`](super::SlotSequence); this module pairs one with
//! the storage it describes, and defines the three-way answer a
//! consumer gets from reading it.
//!
//! [`SlotSnapshot`] exists because that three-way branch was written
//! out identically in `pop`, `pop_ref`, `drain` and `drain_up_to`
//! across two rings. It is computed and matched immediately, never
//! stored, so it costs the same load and comparisons the hand-rolled
//! chains did.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

use super::SlotSequence;

/// A slot carrying its own publication state as a sequence number.
pub struct SeqSlot<T> {
    pub data: UnsafeCell<MaybeUninit<T>>,
    pub sequence: SlotSequence,
}

impl<T> SeqSlot<T> {
    /// Creates a slot that is free and awaiting position `pos`.
    #[must_use]
    pub const fn free_at(pos: usize) -> Self {
        Self {
            data: UnsafeCell::new(MaybeUninit::uninit()),
            sequence: SlotSequence::free_at(pos),
        }
    }

    /// Classifies this slot for logical position `pos` from a single
    /// atomic load.
    #[inline]
    pub fn classify(&self, pos: usize) -> SlotSnapshot<*const MaybeUninit<T>> {
        self.sequence.classify(&self.data, pos)
    }

    /// Drops the value this slot holds, if it holds the one published
    /// at `pos`.
    ///
    /// Takes `&mut self`, so the caller — `RingBuffer::drop` — has the
    /// exclusive access that makes reading the word without an atomic
    /// load sound.
    pub fn drop_value_at(&mut self, pos: usize) {
        if self.sequence.holds_value_at(pos) {
            // SAFETY: the slot is published at `pos`, so its storage
            // holds an initialized `T`, and `&mut self` rules out any
            // concurrent access.
            unsafe { self.data.get().cast::<T>().drop_in_place() };
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::slot_sequence::TOMBSTONE;
    use crate::common::BorrowedDropCounter;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn published_slot(pos: usize) -> SeqSlot<u32> {
        let slot = SeqSlot::free_at(pos);
        // SAFETY: exclusive access; nothing else refers to this slot.
        unsafe { (*slot.data.get()).write(7) };
        slot.sequence.publish(pos);
        slot
    }

    #[test]
    fn a_slot_holding_its_positions_value_reads_as_ready() {
        for pos in [0usize, 1, 2, 63, 4096] {
            assert!(published_slot(pos).classify(pos).is_ready());
        }
    }

    #[test]
    fn a_free_slot_reads_as_not_ready() {
        for pos in [0usize, 1, 2, 63, 4096] {
            assert!(SeqSlot::<u32>::free_at(pos).classify(pos).is_not_ready());
        }
    }

    #[test]
    fn a_tombstoned_slot_reads_as_tombstoned_at_every_position() {
        let slot = SeqSlot::<u32>::free_at(0);
        slot.sequence.tombstone();
        for pos in [0usize, 1, 2, 63, 4096] {
            assert!(slot.classify(pos).is_tombstoned());
        }
    }

    #[test]
    fn a_value_published_for_another_position_is_not_ready_here() {
        let slot = published_slot(5);
        assert!(slot.classify(5).is_ready());
        for other in [0usize, 4, 6, 10] {
            assert!(slot.classify(other).is_not_ready());
        }
    }

    #[test]
    fn a_ready_snapshot_points_at_the_slots_own_storage() {
        let slot = published_slot(0);
        let SlotSnapshot::Ready(ptr) = slot.classify(0) else {
            panic!("expected Ready");
        };
        assert_eq!(ptr, slot.data.get().cast_const());
        // SAFETY: published_slot initialises the payload, and classify reported it ready.
        assert_eq!(unsafe { (*ptr).assume_init() }, 7);
    }

    #[test]
    fn dropping_a_published_slot_runs_the_values_destructor() {
        let counter = AtomicUsize::new(0);
        let mut slot = SeqSlot::free_at(3);
        // SAFETY: exclusive access to a slot nothing else refers to.
        unsafe { (*slot.data.get()).write(BorrowedDropCounter::new(&counter)) };
        slot.sequence.publish(3);

        slot.drop_value_at(3);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dropping_a_slot_at_the_wrong_position_leaves_the_value_alone() {
        let counter = AtomicUsize::new(0);
        let mut slot = SeqSlot::free_at(3);
        // SAFETY: exclusive access to a slot nothing else refers to.
        unsafe { (*slot.data.get()).write(BorrowedDropCounter::new(&counter)) };
        slot.sequence.publish(3);

        slot.drop_value_at(4);
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        slot.drop_value_at(3);
    }

    #[test]
    fn dropping_a_tombstoned_slot_drops_nothing() {
        let mut slot = SeqSlot::<BorrowedDropCounter<'_>>::free_at(3);
        slot.sequence.tombstone();
        slot.drop_value_at(3);
    }

    /// The one position whose published word *is* [`TOMBSTONE`], which
    /// is why the encoding rests on reachability rather than on the
    /// arithmetic being injective.
    #[test]
    fn the_tombstone_collides_only_with_a_position_no_ring_can_reach() {
        let colliding = TOMBSTONE / 2;
        assert_eq!(colliding * 2 + 1, TOMBSTONE);

        let mut slot = SeqSlot::<u32>::free_at(0);
        slot.sequence.tombstone();
        assert!(slot.sequence.holds_value_at(colliding));
        assert!(!slot.sequence.holds_value_at(colliding - 1));
        assert_eq!(colliding, usize::MAX / 2);
    }
}
