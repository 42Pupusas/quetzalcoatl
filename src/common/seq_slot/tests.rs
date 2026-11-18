use super::*;
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
fn a_tombstoned_slot_reads_as_tombstoned_only_at_its_own_position() {
    let slot = SeqSlot::<u32>::free_at(2);
    slot.sequence.tombstone(2);
    assert!(slot.classify(2).is_tombstoned());
    for pos in [0usize, 1, 3, 63, 4096] {
        assert!(slot.classify(pos).is_not_ready());
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
    slot.sequence.tombstone(3);
    slot.drop_value_at(3);
}
