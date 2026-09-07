//! The sequence word of an MPSC slot, and every transition it admits.
//!
//! A slot must answer "is there a value here for position `pos`?" in
//! one atomic load, for a position that keeps growing while the storage
//! wraps. The word encodes both the state and the lap it belongs to:
//!
//! | word          | state                                    |
//! |---------------|------------------------------------------|
//! | `pos * 2`     | free, and `pos` is the next lap to use it |
//! | `pos * 2 + 1` | holds the value published at `pos`        |
//! | [`TOMBSTONE`] | reserved at some lap, then abandoned      |
//!
//! The doubling keeps free and published apart at capacity 1, where
//! consecutive positions share the only slot.
//!
//! Every writer reaches this word through a method here. That is the
//! point of the type: the release transition alone was written out as
//! `(head + capacity) * 2` at seven call sites across three modules,
//! and each of them had to know that consuming position `head` frees
//! the slot for position `head + capacity`, one whole lap ahead.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::SlotSnapshot;
use crate::capacity::Capacity;

/// Marks a slot reserved but never committed, at any position.
///
/// `usize::MAX` cannot collide with a real `pos * 2 + 1`: reaching it
/// would take 2^63 pushes.
pub const TOMBSTONE: usize = usize::MAX;

/// The publication state of one slot, as a single atomic word.
pub struct SlotSequence {
    word: AtomicUsize,
}

impl SlotSequence {
    /// A word for a slot that is free and awaiting position `pos`.
    #[must_use]
    pub const fn free_at(pos: usize) -> Self {
        Self {
            word: AtomicUsize::new(pos * 2),
        }
    }

    /// Publishes the value written at `pos`, handing the slot to the
    /// consumer.
    ///
    /// Release: the value must be visible to any consumer that observes
    /// this word.
    #[inline]
    pub fn publish(&self, pos: usize) {
        self.word.store(pos * 2 + 1, Ordering::Release);
    }

    /// Marks a reservation abandoned, so consumers skip the position
    /// instead of waiting for a value that will never arrive.
    #[inline]
    pub fn tombstone(&self) {
        self.word.store(TOMBSTONE, Ordering::Release);
    }

    /// Hands the slot back to the producers after position `head` has
    /// been consumed.
    ///
    /// The slot becomes free for `head + capacity`, the next position
    /// that maps to it. Naming the future position rather than a bare
    /// "free" flag is what stops a producer one lap ahead from
    /// mistaking this slot for its own.
    ///
    /// Release: the read of the value must not be reordered after the
    /// store that lets a producer overwrite it.
    #[inline]
    pub fn release(&self, head: usize, capacity: Capacity) {
        self.word.store((head + capacity.get()) * 2, Ordering::Release);
    }

    /// Classifies this word against `pos`, pairing it with the storage
    /// it guards.
    #[inline]
    pub fn classify<T>(
        &self,
        data: &UnsafeCell<MaybeUninit<T>>,
        pos: usize,
    ) -> SlotSnapshot<*const MaybeUninit<T>> {
        let word = self.word.load(Ordering::Acquire);
        if word == TOMBSTONE {
            SlotSnapshot::Tombstoned
        } else if word == pos * 2 + 1 {
            SlotSnapshot::Ready(data.get().cast_const())
        } else {
            SlotSnapshot::NotReady
        }
    }

    /// Whether this slot owns a live `T` published at `pos`.
    ///
    /// Takes `&mut self` because the only caller is `RingBuffer::drop`,
    /// which has exclusive access and needs no atomic load.
    #[must_use]
    pub fn holds_value_at(&mut self, pos: usize) -> bool {
        *self.word.get_mut() == pos * 2 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word_of(seq: &SlotSequence) -> usize {
        seq.word.load(Ordering::Relaxed)
    }

    #[test]
    fn a_fresh_word_is_free_for_its_own_position() {
        for pos in [0usize, 1, 7, 4096] {
            let mut seq = SlotSequence::free_at(pos);
            assert!(!seq.holds_value_at(pos), "free must not read as published");
            assert_eq!(word_of(&seq), pos * 2);
        }
    }

    #[test]
    fn publishing_marks_the_slot_as_holding_that_positions_value() {
        for pos in [0usize, 1, 7, 4096] {
            let mut seq = SlotSequence::free_at(pos);
            seq.publish(pos);
            assert!(seq.holds_value_at(pos));
        }
    }

    #[test]
    fn a_published_value_belongs_to_one_position_only() {
        let mut seq = SlotSequence::free_at(5);
        seq.publish(5);
        for other in [0usize, 4, 6, 10] {
            assert!(!seq.holds_value_at(other));
        }
    }

    #[test]
    fn releasing_frees_the_slot_for_the_next_lap() {
        let capacity = Capacity::exact(8);
        for head in [0usize, 1, 7, 4096] {
            let seq = SlotSequence::free_at(head);
            seq.publish(head);
            seq.release(head, capacity);
            assert_eq!(word_of(&seq), word_of(&SlotSequence::free_at(head + 8)));
        }
    }

    #[test]
    fn a_released_slot_is_not_yet_free_for_positions_before_the_next_lap() {
        let capacity = Capacity::exact(4);
        let seq = SlotSequence::free_at(0);
        seq.release(0, capacity);
        assert_eq!(word_of(&seq), 4 * 2);
        assert_ne!(word_of(&seq), word_of(&SlotSequence::free_at(0)));
    }

    #[test]
    fn a_tombstone_is_not_a_published_value_at_any_position() {
        let mut seq = SlotSequence::free_at(3);
        seq.tombstone();
        for pos in [0usize, 3, 4, 4096] {
            assert!(!seq.holds_value_at(pos));
        }
    }

    #[test]
    fn the_free_and_published_words_never_collide() {
        let mut seen = std::collections::HashSet::new();
        for pos in 0..64usize {
            assert!(seen.insert(pos * 2), "free word repeated at {pos}");
            assert!(seen.insert(pos * 2 + 1), "published word repeated at {pos}");
        }
        assert!(!seen.contains(&TOMBSTONE));
    }

    #[test]
    fn classify_reports_ready_only_for_the_published_position() {
        let data = UnsafeCell::new(MaybeUninit::new(7u32));
        let seq = SlotSequence::free_at(5);
        seq.publish(5);
        assert!(matches!(seq.classify(&data, 5), SlotSnapshot::Ready(_)));
        assert!(matches!(seq.classify(&data, 6), SlotSnapshot::NotReady));
    }

    #[test]
    fn classify_reports_a_free_slot_as_not_ready() {
        let data = UnsafeCell::new(MaybeUninit::new(7u32));
        let seq = SlotSequence::free_at(5);
        assert!(matches!(seq.classify(&data, 5), SlotSnapshot::NotReady));
    }

    #[test]
    fn classify_reports_a_tombstone_at_every_position() {
        let data = UnsafeCell::new(MaybeUninit::new(7u32));
        let seq = SlotSequence::free_at(5);
        seq.tombstone();
        for pos in [0usize, 5, 6, 4096] {
            assert!(matches!(seq.classify(&data, pos), SlotSnapshot::Tombstoned));
        }
    }

    #[test]
    fn a_ready_snapshot_points_at_the_storage_it_was_given() {
        let data = UnsafeCell::new(MaybeUninit::new(7u32));
        let seq = SlotSequence::free_at(0);
        seq.publish(0);
        let SlotSnapshot::Ready(ptr) = seq.classify(&data, 0) else {
            panic!("expected Ready");
        };
        assert_eq!(ptr, data.get().cast_const());
        // SAFETY: the storage is initialized above and classify reported it ready.
        assert_eq!(unsafe { (*ptr).assume_init() }, 7);
    }
}
