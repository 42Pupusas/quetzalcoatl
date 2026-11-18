//! What ended a bounded park.
//!
//! A waiter whose [`park`](super::park_registry::ParkSlot::park) has
//! returned needs to know whether a peer released it or something else
//! did — a spurious unpark, or the timed park the ring used before the
//! lost-wake defect was found. The
//! obvious test is whether its park handle is still armed, since a
//! waker claims the handle out of the slot to unpark it. That test is
//! too coarse, and the gap is what this type closes.
//!
//! [`WakeSet::wake_one`](super::park::WakeSet::wake_one) clears the
//! waiter's bit *first* and claims its handle *second*. Between those
//! two steps a peer has taken ownership of the wake and not yet
//! delivered it. A waiter whose timeout expires inside that window
//! sees an armed handle and concludes nobody was coming — when in fact
//! a wake was already in flight and merely lost a race with the clock.
//!
//! Reading the bit alongside the handle narrows it, because the two
//! are written in a known order:
//!
//! | handle | bit | meaning |
//! |---|---|---|
//! | claimed | — | [`Delivered`](Self::Delivered): a peer unparked us |
//! | armed | clear | [`InFlight`](Self::InFlight): *somebody took our bit* and no unpark arrived |
//! | armed | set | [`Untouched`](Self::Untouched): nobody touched our bit |
//!
//! # What `InFlight` does not prove
//!
//! It says the bit was taken, not that a wake is coming. The two are
//! different, and the crate's own calibration test
//! `the_monitor_counts_a_wake_that_never_arrived` is the counterexample:
//! it stages a *lost* wake by clearing the waiter's bit so the
//! publisher finds nobody to unpark, which leaves precisely this
//! signature. A stolen bit and a wake still in flight are
//! indistinguishable from the waiter's side.
//!
//! So `InFlight` is a *weaker suspicion* of a lost wake than
//! [`Untouched`](Self::Untouched), not an exoneration. Do not subtract
//! it from a lost-wake count; a defect that steals bits would
//! disappear into the subtraction. It is useful for telling apart two
//! populations of rescue when comparing policies, and for narrowing
//! where to look.
//!
//! A [`ParkSlot::Shared`] waiter publishes no bit and arms no handle,
//! so neither column says anything about it. It is reached through
//! [`ParkOverflow`](super::park_overflow::ParkOverflow) instead, which
//! records no per-waiter state a sample could read. That is
//! [`Slotless`](Self::Slotless): not evidence of a lost wake, just a
//! waiter this classification cannot see.

use super::park::WakeSet;
use super::park_registry::ParkSlot;
use std::sync::atomic::Ordering;

/// How a waiter's park came to an end.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WakeDelivery {
    /// A peer claimed the handle and issued the unpark.
    Delivered,
    /// The wake bit was cleared but no unpark arrived. Either a peer
    /// is mid-`wake_one` and about to deliver, or the bit was
    /// consumed without a wake reaching us. These are not
    /// distinguishable from here.
    InFlight,
    /// The bit was still published and the handle still armed: no peer
    /// had begun to wake this waiter.
    Untouched,
    /// The waiter holds no park slot, so no peer could ever find it.
    Slotless,
}

impl WakeDelivery {
    /// Reads the waiter's park state from `set`.
    ///
    /// Both loads are `SeqCst` so this sample is ordered against the
    /// waker's `fetch_and` on the bitmap and its swap on the handle.
    #[must_use]
    #[inline]
    pub fn sample(set: &WakeSet, slot: ParkSlot) -> Self {
        if !slot.is_leased() {
            return Self::Slotless;
        }
        let bit_published = set.wake.load(Ordering::SeqCst) & slot.mask() != 0;
        Self::from_park_state(set.is_armed(slot), bit_published)
    }

    /// The classification itself, separated from reading it so the
    /// table above is testable without a live ring.
    #[must_use]
    #[inline]
    pub const fn from_park_state(armed: bool, bit_published: bool) -> Self {
        if !armed {
            Self::Delivered
        } else if bit_published {
            Self::Untouched
        } else {
            Self::InFlight
        }
    }

    /// Whether the sleep ended without a peer's unpark reaching us.
    #[must_use]
    #[inline]
    pub const fn is_unwoken(self) -> bool {
        !matches!(self, Self::Delivered)
    }

    /// Whether this waiter's wake bit was taken without an unpark
    /// reaching it. Consistent with a delivery still in progress, and
    /// equally consistent with a wake that was lost.
    #[must_use]
    #[inline]
    pub const fn is_in_flight(self) -> bool {
        matches!(self, Self::InFlight)
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
