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
mod tests {
    use super::{AwaitedBatch, WakeInterest};

    #[test]
    fn a_fresh_announcement_wants_nothing_in_particular() {
        let awaited = AwaitedBatch::new();
        assert_eq!(awaited.interest(0), WakeInterest::Any);
        assert_eq!(awaited.interest(usize::MAX), WakeInterest::Any);
    }

    #[test]
    fn an_announced_batch_wants_only_its_unused_positions() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, 0b1010);
        assert_eq!(awaited.interest(101), WakeInterest::Reserved);
        assert_eq!(awaited.interest(103), WakeInterest::Reserved);
        for pos in [99usize, 100, 102, 104] {
            assert_eq!(awaited.interest(pos), WakeInterest::Declines);
        }
    }

    /// The distinction routing depends on: a producer pinned to other
    /// positions can do nothing with this release, while one waiting
    /// to refill can take whatever frees. Both fail to name `pos`, so
    /// a yes/no answer collapses them and the release goes to the one
    /// that cannot use it.
    #[test]
    fn a_pinned_producer_declines_where_a_refilling_one_takes_anything() {
        let pinned = AwaitedBatch::new();
        pinned.announce(100, 0b1);
        let refilling = AwaitedBatch::new();
        refilling.announce(100, 0);

        assert_eq!(pinned.interest(200), WakeInterest::Declines);
        assert_eq!(refilling.interest(200), WakeInterest::Any);
    }

    /// A producer waiting to refill is not a taker for a specific
    /// release: serving it ahead of a pinned producer would spend the
    /// wake the pinned one was owed.
    #[test]
    fn an_exhausted_batch_wants_nothing_in_particular() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, 0);
        assert_eq!(awaited.interest(7), WakeInterest::Any);
        assert_eq!(awaited.interest(100), WakeInterest::Any);
    }

    #[test]
    fn a_later_announcement_replaces_the_earlier_one() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, 0b1);
        awaited.announce(200, 0b1);
        assert_eq!(awaited.interest(100), WakeInterest::Declines);
        assert_eq!(awaited.interest(200), WakeInterest::Reserved);
    }

    #[test]
    fn a_position_far_beyond_the_batch_is_not_wanted() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, u32::MAX);
        assert_eq!(awaited.interest(131), WakeInterest::Reserved);
        assert_eq!(awaited.interest(132), WakeInterest::Declines);
    }

    /// A producer stops parking without retracting its announcement,
    /// and `ParkRegistry` hands the slot to a new producer on the next
    /// lease. The word left behind still names positions, so it can
    /// attract a targeted wake on behalf of a producer that is no
    /// longer waiting for anything.
    ///
    /// `wake_one_interested` only spends the wake if the slot's bit is
    /// also set, so a stale announcement over a *clear* bit costs
    /// nothing. The reachable case is the inherited one: a new lease
    /// holder arms its bit while the previous holder's word is still
    /// published, and is then routed a position it never reserved.
    /// Its own re-check finds nothing, and it re-parks having spent
    /// the release that the genuinely pinned producer needed.
    #[test]
    fn a_slot_reused_by_a_new_producer_does_not_inherit_the_old_want() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, 0b1111);
        assert_eq!(
            awaited.interest(101),
            WakeInterest::Reserved,
            "the first producer is pinned here"
        );

        awaited.retract();

        assert_ne!(
            awaited.interest(101),
            WakeInterest::Reserved,
            "a producer that stopped parking must not still attract a wake"
        );
        assert_eq!(
            awaited.announced(),
            None,
            "a retracted slot is indistinguishable from a fresh one"
        );
    }

    /// Retracting is what a fresh lease inherits, so the two must
    /// agree: a recycled slot has to look exactly like a new one.
    #[test]
    fn a_retracted_slot_matches_a_fresh_one() {
        let fresh = AwaitedBatch::new();
        let reused = AwaitedBatch::new();
        reused.announce(4096, 0b1011);
        reused.retract();

        for pos in [0usize, 1, 4095, 4096, 4097, 4099, usize::MAX] {
            assert_eq!(
                reused.interest(pos),
                fresh.interest(pos),
                "a recycled slot must not answer differently at {pos}"
            );
        }
    }

    /// The packed word keeps only the low 32 bits of `start`, so the
    /// match must hold once positions have run past that width.
    #[test]
    fn matching_survives_the_low_word_wrapping() {
        let awaited = AwaitedBatch::new();
        let start = (1usize << 32) - 2;
        awaited.announce(start, 0b111);
        for offset in 0..3 {
            assert_eq!(awaited.interest(start + offset), WakeInterest::Reserved);
        }
        assert_eq!(awaited.interest(start + 3), WakeInterest::Declines);
    }
}
