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
mod tests;
