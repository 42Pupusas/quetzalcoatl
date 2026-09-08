//! What a parked producer is waiting for, stated so a release can be
//! routed to it.
//!
//! A producer parks holding a batch of specific reserved positions, and
//! only the release of one of *those* positions lets it continue. A
//! release that wakes some other producer is spent: the woken one finds
//! nothing and re-parks, and the pinned one is not woken again unless
//! a further release happens to pick it. With the consumers idle there
//! is no further release, and the ring is deadlocked with a free slot in
//! it. `a_pop_can_wake_the_one_producer_that_cannot_use_the_freed_slot`
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

    /// Whether the announcer reserved `pos` and is waiting for it.
    ///
    /// A producer waiting for a refill has reserved nothing and does
    /// not want any specific position; it is served by the caller's
    /// fallback, after every pinned producer has been asked.
    #[inline]
    pub(super) fn wants(&self, pos: usize) -> bool {
        Self::matches(self.0.load(Ordering::Relaxed), pos)
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
    use super::AwaitedBatch;

    #[test]
    fn a_fresh_announcement_wants_nothing_in_particular() {
        let awaited = AwaitedBatch::new();
        assert!(!awaited.wants(0));
        assert!(!awaited.wants(usize::MAX));
    }

    #[test]
    fn an_announced_batch_wants_only_its_unused_positions() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, 0b1010);
        assert!(!awaited.wants(100));
        assert!(awaited.wants(101));
        assert!(!awaited.wants(102));
        assert!(awaited.wants(103));
        assert!(!awaited.wants(104));
        assert!(!awaited.wants(99));
    }

    /// A producer waiting to refill is not a taker for a specific
    /// release: serving it first would spend a pinned producer's wake
    /// on one that cannot use it.
    #[test]
    fn an_exhausted_batch_wants_nothing_in_particular() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, 0);
        assert!(!awaited.wants(7));
        assert!(!awaited.wants(100));
    }

    #[test]
    fn a_later_announcement_replaces_the_earlier_one() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, 0b1);
        awaited.announce(200, 0b1);
        assert!(!awaited.wants(100));
        assert!(awaited.wants(200));
    }

    #[test]
    fn a_position_far_beyond_the_batch_is_not_wanted() {
        let awaited = AwaitedBatch::new();
        awaited.announce(100, u32::MAX);
        assert!(awaited.wants(131));
        assert!(!awaited.wants(132));
    }

    /// The packed word keeps only the low 32 bits of `start`, so the
    /// match must hold once positions have run past that width.
    #[test]
    fn matching_survives_the_low_word_wrapping() {
        let awaited = AwaitedBatch::new();
        let start = (1usize << 32) - 2;
        awaited.announce(start, 0b111);
        assert!(awaited.wants(start));
        assert!(awaited.wants(start + 1));
        assert!(awaited.wants(start + 2));
        assert!(!awaited.wants(start + 3));
    }
}
