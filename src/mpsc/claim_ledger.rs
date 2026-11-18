//! The claim word of an MPSC ring, and the consumer's side of it.
//!
//! Room in the ring is a count of credits, one per free slot. A
//! producer spends a credit to claim a position; the consumer creates
//! one for each position it releases. Credits are moved and spent only
//! by atomic operations, so a producer never decides there is room from
//! something it read earlier.
//!
//! A compare-exchange proves only that a word has the value the
//! producer loaded, not that nothing happened in between: a position
//! counter can wrap all the way round and come back. A test of the tail
//! against a head read earlier is therefore stale by an unknown number
//! of laps when the exchange succeeds, at any word width. Here the
//! exchange succeeds on a word that itself holds a credit, so a slot
//! was free at that instant.
//!
//! The claim word holds three fields:
//!
//! | bits  | field                                   |
//! |-------|-----------------------------------------|
//! | 63–32 | the next position to claim, modulo 2^32 |
//! | 31    | set while one producer moves credits in |
//! | 30–0  | credits ready to claim                  |
//!
//! The consumer does not write this word. It adds its credits to
//! `pending`, on its own cache line. A producer that finds no credit in
//! the word sets the refill bit, moves everything pending into the word
//! and keeps one credit for itself. While the bit is set the word holds
//! no credit, so no other thread writes it and the holder may store to
//! it.
//!
//! A producer that finds the bit set waits for it to clear: until then
//! it cannot tell a full ring from credits on their way in. Only the
//! holder of the bit reports a full ring, on finding nothing pending,
//! and at that instant every slot is claimed.
//!
//! `proofs/Qtz/MpscRing.lean` models each atomic operation here as one
//! step and proves that a claimed position always has a free slot.
//! `MpscClaimWord.lean` proves the field arithmetic of the word, and
//! `MpscJoin.lean` that the tests and updates made on the word are
//! those steps. The proofs take every operation as atomic and
//! sequentially consistent; the orderings chosen below are argued in
//! the comments, not proved.

use crate::capacity::Capacity;
use crate::common::backoff::Backoff;
use crate::common::CachePadded;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// What the consumer writes, kept off the producers' cache line.
struct ConsumerSide {
    /// The next position to read. Only the consumer touches it.
    head: AtomicUsize,
    /// Credits for released positions that no producer has moved into
    /// the claim word yet.
    pending: AtomicUsize,
}

#[repr(C)]
pub(super) struct ClaimLedger {
    word: CachePadded<AtomicU64>,
    consumer: CachePadded<ConsumerSide>,
}

impl ClaimLedger {
    const POSITION_SHIFT: u32 = 32;
    const REFILLING: u64 = 1 << 31;
    const CREDIT_MASK: u64 = Self::REFILLING - 1;
    /// Added to the word by a claim: one position on, one credit fewer.
    const CLAIM: u64 = (1 << Self::POSITION_SHIFT) - 1;
    /// The largest capacity whose credit count fits its field.
    const MAX_CAPACITY: usize = 1 << 30;

    /// # Panics
    ///
    /// Panics if `capacity` exceeds 2^30.
    pub(super) fn new(capacity: Capacity) -> Self {
        assert!(
            capacity.get() <= Self::MAX_CAPACITY,
            "mpsc capacity exceeds 2^30"
        );
        Self {
            word: CachePadded(AtomicU64::new(Self::pack(0, capacity.get()))),
            consumer: CachePadded(ConsumerSide {
                head: AtomicUsize::new(0),
                pending: AtomicUsize::new(0),
            }),
        }
    }

    /// The word for `position` with `credits` ready and no refill under
    /// way. The shift drops every bit of `position` above the low 32.
    const fn pack(position: usize, credits: usize) -> u64 {
        ((position as u64) << Self::POSITION_SHIFT) | credits as u64
    }

    // Both fields are below 2^32, so neither cast loses bits.
    #[allow(clippy::cast_possible_truncation)]
    const fn credits_of(word: u64) -> usize {
        (word & Self::CREDIT_MASK) as usize
    }

    #[allow(clippy::cast_possible_truncation)]
    const fn position_of(word: u64) -> usize {
        (word >> Self::POSITION_SHIFT) as usize
    }

    /// Claims the next position, or `None` when the ring is full.
    ///
    /// `AcqRel` on success. A credit reaches this word from the
    /// consumer, which read the slot's previous value before releasing
    /// it; that read must happen before this producer's write. The
    /// acquire takes the ordering from whoever wrote the word last, and
    /// the release passes it to whoever claims next.
    #[inline]
    pub(super) fn claim(&self) -> Option<usize> {
        let mut word = self.word.load(Ordering::Relaxed);
        let mut backoff = Backoff::new();
        loop {
            let next = if Self::credits_of(word) != 0 {
                word.wrapping_add(Self::CLAIM)
            } else if word & Self::REFILLING == 0 {
                word | Self::REFILLING
            } else {
                backoff.spin();
                word = self.word.load(Ordering::Relaxed);
                continue;
            };
            match self
                .word
                .compare_exchange_weak(word, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) if next & Self::REFILLING == 0 => return Some(Self::position_of(word)),
                Ok(_) => return self.refill(Self::position_of(word)),
                Err(observed) => {
                    word = observed;
                    backoff.spin();
                }
            }
        }
    }

    /// Moves the pending credits into the word and claims `position`
    /// with one of them, or reports the ring full when none are
    /// pending.
    ///
    /// The caller has set the refill bit, so it is the only writer of
    /// the word until one of the stores here clears the bit.
    ///
    /// `SeqCst` on `pending`: this is the producer's half of the park
    /// handshake. Either this sees the consumer's credit, or the
    /// consumer's wake, which follows its `SeqCst` addition, sees this
    /// producer's park bit.
    fn refill(&self, position: usize) -> Option<usize> {
        let pending = &self.consumer.pending;
        let granted = if pending.load(Ordering::SeqCst) == 0 {
            0
        } else {
            pending.swap(0, Ordering::SeqCst)
        };
        let Some(surplus) = granted.checked_sub(1) else {
            self.word.store(Self::pack(position, 0), Ordering::Release);
            return None;
        };
        self.word.store(
            Self::pack(position.wrapping_add(1), surplus),
            Ordering::Release,
        );
        Some(position)
    }

    /// The consumer's head. Only the consumer writes it.
    #[inline]
    pub(super) fn head(&self) -> usize {
        self.consumer.head.load(Ordering::Relaxed)
    }

    /// Moves the consumer's head to `head` and credits the producers
    /// with every position released on the way.
    ///
    /// `SeqCst`: the addition is the consumer's half of the park
    /// handshake, see [`refill`](Self::refill). As a locked instruction
    /// it also drains the store buffer, so the slot words released
    /// before it are visible first.
    ///
    /// Only the ring's consumer may call this, and only with positions
    /// it has read or skipped.
    #[inline]
    pub(super) fn publish_head(&self, head: usize) {
        let released = head.wrapping_sub(self.consumer.head.load(Ordering::Relaxed));
        self.consumer.head.store(head, Ordering::Relaxed);
        self.consumer.pending.fetch_add(released, Ordering::SeqCst);
    }

    /// The number of claimed positions not yet released.
    ///
    /// The two counts are loaded separately. A credit that moves or is
    /// spent between the loads makes this an estimate, clamped to the
    /// capacity.
    #[inline]
    pub(super) fn len(&self, capacity: Capacity) -> usize {
        let credits = Self::credits_of(self.word.load(Ordering::Relaxed));
        let pending = self.consumer.pending.load(Ordering::Relaxed);
        capacity.get().saturating_sub(credits + pending)
    }

    /// The positions not yet released, for a ring being dropped.
    pub(super) fn occupied(&mut self, capacity: Capacity) -> impl Iterator<Item = usize> {
        let head = *self.consumer.0.head.get_mut();
        let free = Self::credits_of(*self.word.0.get_mut()) + *self.consumer.0.pending.get_mut();
        let len = capacity.get() - free;
        (0..len).map(move |offset| head.wrapping_add(offset))
    }

    /// Restarts an untouched ledger at `pos`.
    #[cfg(test)]
    pub(super) fn start_at(&mut self, pos: usize) {
        let credits = Self::credits_of(*self.word.0.get_mut());
        *self.consumer.0.head.get_mut() = pos;
        *self.word.0.get_mut() = Self::pack(pos, credits);
    }
}

#[cfg(test)]
mod tests;
