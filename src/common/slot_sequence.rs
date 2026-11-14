//! The sequence word of an MPSC slot, and every transition it admits.
//!
//! A slot must answer "is there a value here for position `pos`?" in
//! one atomic load, for a position that keeps growing while the storage
//! wraps. The word encodes both the state and the position it belongs
//! to:
//!
//! | word          | state                                    |
//! |---------------|------------------------------------------|
//! | `pos * 4`     | free, and `pos` is the next lap to use it |
//! | `pos * 4 + 1` | holds the value published at `pos`        |
//! | `pos * 4 + 2` | reserved at `pos`, then abandoned         |
//!
//! Scaling by four keeps the three states of one position below the
//! free word of the next, which matters at capacity 1, where
//! consecutive positions share the only slot.
//!
//! Every word names its position, so none is set aside as a sentinel.
//! The abandoned marker used to be a bare `usize::MAX`, which is also
//! what `pos * 2 + 1` gave for position `usize::MAX / 2`: the value
//! published there was skipped as abandoned and never delivered. On a
//! 32-bit target that is push number 2^31.
//!
//! Only the low 32 bits of a position enter the word, because that is
//! all a producer knows of the position it claimed (see
//! `mpsc::claim_ledger`). The consumer's head runs on unreduced, so
//! the reduction is made here, where the two meet. The multiplication
//! then wraps on a 32-bit target. Two words are equal only when both
//! position and state are, for positions fewer than 2^30 apart. That
//! bounds the capacity, not the number of pushes.
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

/// The publication state of one slot, as a single atomic word.
pub struct SlotSequence {
    word: AtomicUsize,
}

impl SlotSequence {
    const FREE: usize = 0;
    const PUBLISHED: usize = 1;
    const ABANDONED: usize = 2;
    const POSITION_MASK: usize = u32::MAX as usize;

    const fn encode(pos: usize, state: usize) -> usize {
        (pos & Self::POSITION_MASK)
            .wrapping_mul(4)
            .wrapping_add(state)
    }

    /// A word for a slot that is free and awaiting position `pos`.
    #[must_use]
    pub const fn free_at(pos: usize) -> Self {
        Self {
            word: AtomicUsize::new(Self::encode(pos, Self::FREE)),
        }
    }

    /// Publishes the value written at `pos`, handing the slot to the
    /// consumer.
    ///
    /// Release: the value must be visible to any consumer that observes
    /// this word.
    #[inline]
    pub fn publish(&self, pos: usize) {
        self.word
            .store(Self::encode(pos, Self::PUBLISHED), Ordering::Release);
    }

    /// Marks the reservation at `pos` abandoned, so the consumer skips
    /// the position instead of waiting for a value that will never
    /// arrive.
    #[inline]
    pub fn tombstone(&self, pos: usize) {
        self.word
            .store(Self::encode(pos, Self::ABANDONED), Ordering::Release);
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
        let next_lap = head.wrapping_add(capacity.get());
        self.word
            .store(Self::encode(next_lap, Self::FREE), Ordering::Release);
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
        if word == Self::encode(pos, Self::PUBLISHED) {
            SlotSnapshot::Ready(data.get().cast_const())
        } else if word == Self::encode(pos, Self::ABANDONED) {
            SlotSnapshot::Tombstoned
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
        *self.word.get_mut() == Self::encode(pos, Self::PUBLISHED)
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
            assert_eq!(word_of(&seq), pos * 4);
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
        assert_eq!(word_of(&seq), word_of(&SlotSequence::free_at(4)));
        assert_ne!(word_of(&seq), word_of(&SlotSequence::free_at(0)));
    }

    #[test]
    fn a_tombstone_is_not_a_published_value_at_any_position() {
        let mut seq = SlotSequence::free_at(3);
        seq.tombstone(3);
        for pos in [0usize, 3, 4, 4096] {
            assert!(!seq.holds_value_at(pos));
        }
    }

    /// Each window is checked on its own: words are distinct only for
    /// positions fewer than `2^(usize::BITS - 2)` apart. The windows
    /// cover the origin, the positions where `pos * 4` wraps, and the
    /// one whose published word the old sentinel collided with.
    #[test]
    fn no_two_words_collide_within_a_window() {
        for start in [0, usize::MAX / 4 - 32, usize::MAX / 2 - 32] {
            let mut seen = std::collections::HashSet::new();
            for pos in start..start + 64 {
                for state in [
                    SlotSequence::FREE,
                    SlotSequence::PUBLISHED,
                    SlotSequence::ABANDONED,
                ] {
                    assert!(
                        seen.insert(SlotSequence::encode(pos, state)),
                        "word repeated at {pos}, state {state}"
                    );
                }
            }
        }
    }

    /// A producer publishes the position it claimed, which is reduced
    /// to 32 bits; the consumer classifies against its unreduced head.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn a_reduced_position_reads_as_ready_at_the_unreduced_one() {
        let data = UnsafeCell::new(MaybeUninit::new(7u32));
        let head = (1usize << 32) + 5;
        let seq = SlotSequence::free_at(5);
        seq.publish(5);
        assert!(matches!(seq.classify(&data, head), SlotSnapshot::Ready(_)));
        assert!(matches!(
            seq.classify(&data, head + 1),
            SlotSnapshot::NotReady
        ));
    }

    #[test]
    fn a_value_published_where_the_sentinel_used_to_sit_reads_as_ready() {
        let pos = usize::MAX / 2;
        let data = UnsafeCell::new(MaybeUninit::new(7u32));
        let mut seq = SlotSequence::free_at(pos);
        seq.publish(pos);
        assert!(matches!(seq.classify(&data, pos), SlotSnapshot::Ready(_)));
        assert!(seq.holds_value_at(pos));
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
    fn classify_reports_a_tombstone_only_at_its_own_position() {
        let data = UnsafeCell::new(MaybeUninit::new(7u32));
        let seq = SlotSequence::free_at(5);
        seq.tombstone(5);
        assert!(matches!(seq.classify(&data, 5), SlotSnapshot::Tombstoned));
        for pos in [0usize, 4, 6, 4096] {
            assert!(matches!(seq.classify(&data, pos), SlotSnapshot::NotReady));
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
