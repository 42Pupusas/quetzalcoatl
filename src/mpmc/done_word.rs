//! The encoding of an MPMC slot's `done` word.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::capacity::Capacity;

/// A slot's `done` word: the handshake that hands a slot from the
/// consumer of one round to the producer of the next.
///
/// The word holds the position the slot is *next free for*. A consumer
/// finishing at `pos` stores `pos + capacity`, one full lap ahead, and
/// the producer for that later position waits to read its own position
/// back. Both halves are the same fact stated from opposite ends, which
/// is why they belong together here.
///
/// The two-step read — move the value out, *then* release — is what
/// makes the word necessary. Releasing first would let the next round's
/// producer refill a slot whose old value is still being read.
#[repr(transparent)]
pub(super) struct DoneWord(AtomicUsize);

impl DoneWord {
    /// The initial word for `slot`: as if round −1 had already released
    /// it, so the round-zero producer's check passes immediately.
    pub(super) const fn released_for(slot: usize) -> Self {
        Self(AtomicUsize::new(slot))
    }

    /// Whether the slot is free for a producer writing at `pos`.
    pub(super) fn is_free_for(&self, pos: usize) -> bool {
        self.0.load(Ordering::Acquire) == pos
    }

    /// Hands the slot to the producer one lap ahead of `round_pos`.
    ///
    /// `SeqCst` rather than `Release` drains the store buffer, so the
    /// `SeqCst` load of `producer_park.wake` inside the following
    /// `wake_one` cannot be satisfied before this store is globally
    /// visible — otherwise the wake could miss a producer that has just
    /// parked on this very slot. A leading `SeqCst` fence in `wake_one`
    /// would be equivalent; putting it on the store keeps the pairing
    /// local to the call site.
    pub(super) fn release(&self, round_pos: usize, capacity: Capacity) {
        self.0
            .store(round_pos.wrapping_add(capacity.get()), Ordering::SeqCst);
    }

    /// Whether the consumer that claimed `round_pos` released the slot.
    ///
    /// A slot whose `ready` word reads as claimed still holds a live
    /// value until this turns true: the claim and the release bracket
    /// the window in which the value is being read out. Teardown asks
    /// this to decide whether it still owes the value a destructor.
    pub(super) fn was_released_after(&mut self, round_pos: usize, capacity: Capacity) -> bool {
        *self.0.get_mut() == round_pos.wrapping_add(capacity.get())
    }

    /// The raw word, for diagnostics.
    pub(super) fn word(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests;
