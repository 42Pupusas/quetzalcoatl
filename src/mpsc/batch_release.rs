//! Batched release of consumed MPSC positions.

use std::mem::MaybeUninit;

use super::RingBuffer;
use crate::common::{SeqSlot, SlotSnapshot};

/// Owns the obligation to publish a drain's head cursor and to wake the
/// producers it freed.
///
/// A drain releases two kinds of position: one that delivered a value,
/// and one that only carried an abandoned reservation. Both hand a slot
/// back to the producers, so both must be counted — publishing only when
/// a value came out leaves a tombstone-only batch having cleared the
/// slot metadata while `head` still names the tombstone, so the space is
/// never returned and the producers see a ring that stays full.
///
/// The count of released positions is therefore kept separately from the
/// count of delivered values: the former decides what to publish and how
/// many producers to wake, the latter is what the caller asked for.
///
/// Publication happens in the destructor, which also makes it unwind-
/// safe: a panicking drain callback leaves the positions already taken
/// out of the ring published as consumed rather than stranded.
pub(super) struct BatchRelease<'a, T> {
    ring: &'a RingBuffer<T>,
    head: usize,
    released: usize,
    delivered: usize,
}

impl<'a, T> BatchRelease<'a, T> {
    /// Opens a batch at `head`.
    ///
    /// # Safety
    ///
    /// The caller must be the ring's only consumer for as long as the
    /// batch lives, and `head` its current head. [`take`](Self::take)
    /// moves values out of the slots on the strength of that.
    pub(super) const unsafe fn new(ring: &'a RingBuffer<T>, head: usize) -> Self {
        Self {
            ring,
            head,
            released: 0,
            delivered: 0,
        }
    }

    pub(super) const fn delivered(&self) -> usize {
        self.delivered
    }

    /// The position the batch has advanced to.
    pub(super) const fn head(&self) -> usize {
        self.head
    }

    /// Releases every abandoned position at the head and reports the
    /// first one that is not. `None` when that position has nothing
    /// published; otherwise its slot and the pointer to its value.
    ///
    /// The only place an abandoned reservation is recognised and
    /// stepped over. Skipping one frees a slot exactly as consuming a
    /// value does, so each is counted in `released`.
    #[inline]
    pub(super) fn settle(&mut self) -> Option<(&'a SeqSlot<T>, *const MaybeUninit<T>)> {
        loop {
            let slot = self.ring.slot(self.head);
            match slot.classify(self.head) {
                SlotSnapshot::NotReady => return None,
                SlotSnapshot::Tombstoned => self.release(slot),
                SlotSnapshot::Ready(data) => return Some((slot, data)),
            }
        }
    }

    /// Moves the next value out of the ring, releasing any abandoned
    /// positions ahead of it. `None` once the next position has nothing
    /// published.
    ///
    /// A returned value is already counted, so an unwind through the
    /// caller's callback still publishes its position.
    #[inline]
    pub(super) fn take(&mut self) -> Option<T> {
        let (slot, data) = self.settle()?;
        // SAFETY: the slot is published at `head`, so it holds an
        // initialized `T`, and `new`'s contract makes this the only
        // reader.
        let value = unsafe { (*data).assume_init_read() };
        self.release(slot);
        self.delivered += 1;
        Some(value)
    }

    /// Hands `slot`, the one at the batch's head, to its next lap and
    /// steps past it.
    #[inline]
    fn release(&mut self, slot: &SeqSlot<T>) {
        slot.sequence.release(self.head, self.ring.capacity);
        self.head = self.head.wrapping_add(1);
        self.released += 1;
    }
}

impl<T> Drop for BatchRelease<'_, T> {
    fn drop(&mut self) {
        if self.released == 0 {
            return;
        }
        // See Consumer::pop. Drains the store buffer so the sequence
        // stores above are visible before the wake path loads the park
        // bitmap.
        self.ring.ledger.publish_head(self.head);
        self.ring.producer_park.wake_n(self.released);
        self.ring.notify_producers_n(self.released);
    }
}
