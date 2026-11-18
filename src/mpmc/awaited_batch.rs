//! What a parked producer is waiting for, stated so a release can be
//! routed to it.
//!
//! A producer parks holding a batch of specific reserved positions, and
//! only the release of one of *those* positions lets it continue. A
//! release that wakes some other producer is spent: the woken one finds
//! nothing and re-parks, and the pinned one is not woken again unless
//! a further release happens to pick it. With the consumers idle there
//! is no further release, and the ring is deadlocked with a free slot in
//! it. `a_release_a_pinned_producer_cannot_use_does_not_reach_it`
//! stages exactly this.
//!
//! The batch is published here as a compact pair packed into one word:
//! the batch's starting position and the bitmap of positions still
//! unused. A waker that frees `pos` asks each parked producer whether
//! `pos` falls in its batch, and wakes the one that says yes.
//!
//! A producer with an exhausted batch is waiting for a refill rather
//! than a position. A refill needs the slot at the *claim cursor* to
//! be free, which a release of some other position does nothing for;
//! such a producer announces nothing and is served only when no
//! producer wanted the position. Treating it as a taker for every
//! release is the deadlock in another form: a release for a pinned
//! producer spent on one that cannot refill, and then re-parks.

use crate::common::wake_interest::WakeInterest;
use std::sync::atomic::{AtomicU64, Ordering};

/// A parked producer's announced batch, or "anything".
///
/// Packed as `start << 32 | unused`. A batch holds at most 32
/// positions, so `unused` fits in the low half; `start` keeps its
/// low 32 bits, which suffices because a match is decided on the
/// offset `pos - start` and only offsets below 32 can be in the batch.
#[repr(transparent)]
pub(super) struct AwaitedBatch(AtomicU64);

impl AwaitedBatch {
    const ANY: u64 = u64::MAX;
    const BATCH_WIDTH: u64 = 32;

    pub(super) const fn new() -> Self {
        Self(AtomicU64::new(Self::ANY))
    }

    /// Announces the positions `start + i` for each set bit `i` of
    /// `unused`. An empty `unused` means the producer needs a refill
    /// and will take any release.
    #[inline]
    pub(super) fn announce(&self, start: usize, unused: u32) {
        let word = if unused == 0 {
            Self::ANY
        } else {
            Self::pack(start, unused)
        };
        self.0.store(word, Ordering::Relaxed);
    }

    /// Withdraws any announcement, leaving the slot as a fresh one.
    ///
    /// A producer that stops parking leaves its last announcement
    /// published, and `ParkRegistry` recycles park slots. Without
    /// this, a new lease holder arms its bit under the previous
    /// holder's word and is routed positions it never reserved,
    /// spending the release a pinned producer was waiting for.
    #[inline]
    pub(super) fn retract(&self) {
        self.0.store(Self::ANY, Ordering::Relaxed);
    }

    /// What the announcer can do with a release of `pos`.
    ///
    /// The three cases are distinct and a router needs all three. A
    /// producer that announced a batch containing `pos` is the one the
    /// release belongs to. One that announced nothing is waiting for a
    /// refill and will take whatever frees first. One that announced a
    /// batch *without* `pos` is pinned elsewhere: the release cannot
    /// advance it, so waking it spends the release and leaves the
    /// producer that could have used it parked.
    #[inline]
    pub(super) fn interest(&self, pos: usize) -> WakeInterest {
        let word = self.0.load(Ordering::Relaxed);
        if word == Self::ANY {
            return WakeInterest::Any;
        }
        if Self::matches(word, pos) {
            WakeInterest::Reserved
        } else {
            WakeInterest::Declines
        }
    }

    /// The announced `(start, unused)`, or `None` for "anything".
    #[cfg(test)]
    pub(super) fn announced(&self) -> Option<(u64, u32)> {
        let word = self.0.load(Ordering::Relaxed);
        if word == Self::ANY {
            return None;
        }
        #[allow(clippy::cast_possible_truncation)]
        Some((word >> 32, word as u32))
    }

    const fn pack(start: usize, unused: u32) -> u64 {
        ((start as u64) << 32) | (unused as u64)
    }

    const fn matches(word: u64, pos: usize) -> bool {
        if word == Self::ANY {
            return false;
        }
        let start = word >> 32;
        let unused = word & 0xffff_ffff;
        let offset = ((pos as u64) & 0xffff_ffff).wrapping_sub(start) & 0xffff_ffff;
        offset < Self::BATCH_WIDTH && (unused >> offset) & 1 == 1
    }
}

#[cfg(test)]
mod tests;
