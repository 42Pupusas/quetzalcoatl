//! The encoding of an SPMC slot's `done` word.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::capacity::Capacity;

/// A slot's `done` word: the handshake that hands a slot from the
/// consumer that read it back to the sole producer.
///
/// The word holds the position the slot is *next free for*. A consumer
/// finishing at `pos` stores `pos + capacity`, one full lap ahead, and
/// the producer waits to read its own position back. Both halves are
/// the same fact stated from opposite ends, which is why they belong
/// together here.
///
/// The two-step read — move the value out, *then* release — is what
/// makes the word necessary. Releasing first would let the producer
/// refill a slot whose old value is still being read.
///
/// # Ordering
///
/// The release is `Release`, and every caller follows it with
/// [`SoleParker::wake`](crate::common::sole_parker::SoleParker::wake),
/// whose leading `SeqCst` fence drains the store buffer before the
/// parked flag is sampled. That fence is what closes the missed-wake
/// window; the store itself does not need to.
///
/// The MPMC twin stores `SeqCst` instead and then calls the equally
/// fenced `wake_one`, which makes its stronger ordering redundant
/// rather than load-bearing. The two are kept apart here because the
/// pairing is a property of each ring's wake path, not a shared fact.
#[repr(transparent)]
pub(super) struct DoneWord(AtomicUsize);

impl DoneWord {
    /// The initial word for `slot`: as if a prior lap had already
    /// released it, so the first producer's check passes immediately.
    pub(super) const fn released_for(slot: usize) -> Self {
        Self(AtomicUsize::new(slot))
    }

    /// Whether the slot is free for the producer writing at `pos`.
    #[inline]
    pub(super) fn is_free_for(&self, pos: usize) -> bool {
        self.0.load(Ordering::Acquire) == pos
    }

    /// Hands the slot to the producer one lap ahead of `pos`.
    #[inline]
    pub(super) fn release(&self, pos: usize, capacity: Capacity) {
        self.0
            .store(pos.wrapping_add(capacity.get()), Ordering::Release);
    }

    /// Whether the slot still owes its value a destructor at teardown.
    ///
    /// A position inside the occupied range holds a live value unless
    /// its consumer already released it; teardown asks this to avoid
    /// dropping a value twice.
    pub(super) fn holds_unconsumed_value_at(&mut self, pos: usize, capacity: Capacity) -> bool {
        *self.0.get_mut() != pos.wrapping_add(capacity.get())
    }
}

#[cfg(test)]
mod tests;
