//! What a parked waiter can do with the release being routed.
//!
//! Routing a release asks each parked waiter one question, and a yes/no
//! answer is not enough to route it well. "No" covers two waiters that
//! must be treated differently: one pinned to other positions, which
//! this release can never help, and one holding no reservation at all,
//! which any release helps. Collapsing them sends the wake to the
//! waiter that provably cannot use it while the one that can stays
//! parked — and when nothing else releases, that is a deadlock with a
//! free slot in the ring.

/// A parked waiter's use for one particular release.
///
/// Ordered by how well the release fits: [`Reserved`](Self::Reserved)
/// names the waiter the release belongs to,
/// [`Any`](Self::Any) a waiter that can take it for want of a better
/// claimant, and [`Declines`](Self::Declines) one it cannot serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeInterest {
    /// The waiter reserved this exact position. Only it can proceed on
    /// this release, so it outranks every other claim.
    Reserved,
    /// The waiter holds no reservation and is waiting for room
    /// anywhere, so it can use whatever frees first.
    Any,
    /// The waiter is pinned to positions this release does not include.
    /// Waking it delivers nothing: it re-parks, and the release is
    /// spent.
    Declines,
}

impl WakeInterest {
    /// How strong a claim this is, lower being stronger. Used to order
    /// the passes a router makes over the parked waiters.
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Reserved => 0,
            Self::Any => 1,
            Self::Declines => 2,
        }
    }

    /// Whether waking on this interest can deliver anything at all.
    #[must_use]
    pub const fn is_usable(self) -> bool {
        !matches!(self, Self::Declines)
    }
}

#[cfg(test)]
mod tests {
    use super::WakeInterest;

    #[test]
    fn a_reserved_claim_outranks_every_other() {
        assert!(WakeInterest::Reserved.rank() < WakeInterest::Any.rank());
        assert!(WakeInterest::Reserved.rank() < WakeInterest::Declines.rank());
    }

    #[test]
    fn a_waiter_wanting_anything_outranks_one_that_declines() {
        assert!(WakeInterest::Any.rank() < WakeInterest::Declines.rank());
    }

    #[test]
    fn only_a_declining_waiter_can_use_nothing() {
        assert!(WakeInterest::Reserved.is_usable());
        assert!(WakeInterest::Any.is_usable());
        assert!(!WakeInterest::Declines.is_usable());
    }

    #[test]
    fn the_ranks_are_distinct() {
        let ranks = [
            WakeInterest::Reserved.rank(),
            WakeInterest::Any.rank(),
            WakeInterest::Declines.rank(),
        ];
        for (i, a) in ranks.iter().enumerate() {
            for b in &ranks[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
