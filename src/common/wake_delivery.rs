//! What ended a bounded park.
//!
//! A waiter that wakes from [`park_bounded`](super::park_registry::ParkSlot::park_bounded)
//! needs to know whether a peer released it or the clock did. The
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
//! so nothing can be inferred about it: its parks *always* end on the
//! timeout by construction. That is [`Slotless`](Self::Slotless),
//! which is unwoken without being evidence of anything.

use super::park::WakeSet;
use super::park_registry::ParkSlot;
use std::sync::atomic::Ordering;

/// How a waiter's bounded park came to an end.
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
mod tests {
    use super::{ParkSlot, WakeDelivery, WakeSet};

    #[test]
    fn a_claimed_handle_means_a_peer_unparked_us() {
        assert_eq!(
            WakeDelivery::from_park_state(false, false),
            WakeDelivery::Delivered
        );
    }

    /// The handle is claimed after the bit is cleared, so a claimed
    /// handle settles the question whatever the bit says.
    #[test]
    fn a_claimed_handle_outranks_a_republished_bit() {
        assert_eq!(
            WakeDelivery::from_park_state(false, true),
            WakeDelivery::Delivered
        );
    }

    #[test]
    fn an_armed_handle_with_its_bit_still_set_means_nobody_tried() {
        let state = WakeDelivery::from_park_state(true, true);
        assert_eq!(state, WakeDelivery::Untouched);
        assert!(state.is_unwoken());
        assert!(!state.is_in_flight());
    }

    /// The window this type exists to name: `wake_one` clears the bit
    /// and is descheduled before claiming the handle.
    #[test]
    fn an_armed_handle_with_its_bit_taken_means_a_wake_is_in_flight() {
        let state = WakeDelivery::from_park_state(true, false);
        assert_eq!(state, WakeDelivery::InFlight);
        assert!(
            state.is_unwoken(),
            "no unpark has reached us yet, so the park did end on the timeout"
        );
        assert_ne!(
            state,
            WakeDelivery::Untouched,
            "a peer owns our wake, so this is not evidence of a lost one"
        );
    }

    #[test]
    fn a_fresh_waiters_park_state_reads_as_untouched() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Leased(3));
        assert_eq!(
            WakeDelivery::sample(&set, ParkSlot::Leased(3)),
            WakeDelivery::Untouched
        );
    }

    #[test]
    fn a_woken_waiters_park_state_reads_as_delivered() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Leased(3));
        set.wake_one();
        assert_eq!(
            WakeDelivery::sample(&set, ParkSlot::Leased(3)),
            WakeDelivery::Delivered
        );
    }

    /// Staged exactly as `wake_one` would leave it mid-flight: the bit
    /// retired, the handle not yet claimed.
    #[test]
    fn a_bit_taken_without_the_handle_reads_as_in_flight() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Leased(3));
        set.disarm(ParkSlot::Leased(3));
        assert_eq!(
            WakeDelivery::sample(&set, ParkSlot::Leased(3)),
            WakeDelivery::InFlight
        );
    }

    /// A slotless waiter is invisible to every peer, so its timeouts
    /// are by construction rather than evidence.
    #[test]
    fn a_slotless_waiter_is_unwoken_without_being_untouched() {
        let set = WakeSet::new();
        let state = WakeDelivery::sample(&set, ParkSlot::Shared);
        assert_eq!(state, WakeDelivery::Slotless);
        assert!(state.is_unwoken());
        assert!(!state.is_in_flight());
    }
}
