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
mod tests;
