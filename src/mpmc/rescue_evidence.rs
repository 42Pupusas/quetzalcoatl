//! What a park episode leaves behind to explain a rescue.
//!
//! A rescue is a waiter released by the timeout that immediately found
//! work. On its own that says little: several benign mechanisms produce
//! one, and each has to be ruled out before the remainder can be read
//! as a wake that went missing.
//!
//! [`RescueEvidence`] accumulates the facts across every park in one
//! blocking call, because a waiter can time out repeatedly before it
//! succeeds and an explanation observed at any of those parks still
//! explains the rescue at the end.
//!
//! # The categories, most benign first
//!
//! - **Slotless.** The waiter holds no park slot, publishes no wake
//!   bit, and arms no handle. No peer can reach it and every one of
//!   its parks ends on the clock. This one really is structural.
//! - **`BitTaken`.** The waiter's wake bit was cleared but no unpark
//!   arrived. Consistent with a peer mid-delivery, and equally
//!   consistent with a wake that was consumed and lost.
//! - **Untouched.** Nobody had touched the bit at all.
//!
//! Only [`RescueExplanation::Slotless`] excuses a rescue.
//! [`RescueExplanation::BitTaken`] is *not* an exoneration — see
//! [`crate::common::wake_delivery`] for the staged lost wake that
//! produces it — so it is reported alongside the sole-waiter count
//! rather than subtracted from it.
//!
//! Later parks cannot un-observe an earlier one, so the category only
//! ever strengthens: see [`RescueExplanation::strongest`].

use crate::common::wake_delivery::WakeDelivery;

/// What was observed about a waiter's park state, ordered by how much
/// it accounts for a rescue.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum RescueExplanation {
    /// Nobody had touched this waiter's bit or handle.
    Untouched,
    /// The bit was taken without an unpark arriving. Suspicious, not
    /// exculpatory: a stolen wake looks exactly like a late one.
    BitTaken,
    /// The waiter was unreachable by construction, so no wake could
    /// ever have been delivered to it.
    Slotless,
}

impl RescueExplanation {
    /// The more informative of two observations.
    ///
    /// Evidence accumulates over a park episode and never weakens: a
    /// waiter whose bit was once taken has that on record however many
    /// quiet timeouts follow it.
    #[must_use]
    #[inline]
    pub fn strongest(self, other: Self) -> Self {
        self.max(other)
    }

}

impl From<WakeDelivery> for RescueExplanation {
    /// A delivered wake never reaches this conversion — it ends the
    /// park rather than the timeout — so it maps to `Untouched`,
    /// which explains nothing and is discarded when no rescue follows.
    fn from(delivery: WakeDelivery) -> Self {
        if matches!(delivery, WakeDelivery::Slotless) {
            Self::Slotless
        } else if delivery.is_in_flight() {
            Self::BitTaken
        } else {
            Self::Untouched
        }
    }
}

/// The facts gathered across one waiter's park episode.
#[derive(Clone, Copy, Debug)]
pub struct RescueEvidence {
    sole_waiter: bool,
    explanation: RescueExplanation,
}

impl RescueEvidence {
    /// Fresh evidence for a waiter that has not yet parked.
    ///
    /// `sole_waiter` starts false so a watch that never parks cannot
    /// report one; the first pre-park sample sets it.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            sole_waiter: false,
            explanation: RescueExplanation::Untouched,
        }
    }

    /// Records the peer count sampled just before sleeping.
    pub const fn about_to_park(&mut self, peers_parked: u32) {
        self.sole_waiter = peers_parked == 0;
    }

    /// Records how one park ended.
    ///
    /// A peer parked at *either* end of the sleep means round-robin
    /// had somewhere else to send the wake, so `sole_waiter` only
    /// survives when both samples saw nobody.
    pub fn parked(&mut self, delivery: WakeDelivery, peers_parked: u32) {
        self.sole_waiter = self.sole_waiter && peers_parked == 0;
        self.explanation = self.explanation.strongest(delivery.into());
    }

    /// Whether no peer was parked to have absorbed this waiter's wake.
    #[must_use]
    pub const fn is_sole_waiter(self) -> bool {
        self.sole_waiter
    }

    /// The strongest explanation observed during the episode.
    #[must_use]
    pub const fn explanation(self) -> RescueExplanation {
        self.explanation
    }


    /// Consumes the evidence, resetting for the next episode.
    pub const fn take(&mut self) -> Self {
        let taken = *self;
        *self = Self::new();
        taken
    }
}

impl Default for RescueEvidence {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{RescueEvidence, RescueExplanation, WakeDelivery};

    #[test]
    fn a_waiter_alone_at_both_samples_is_a_sole_waiter() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::Untouched, 0);
        assert!(evidence.is_sole_waiter());
        assert_eq!(evidence.explanation(), RescueExplanation::Untouched);
    }

    /// A peer that was already waiting when we went to sleep could
    /// have taken our wake, so round-robin explains the rescue.
    #[test]
    fn a_peer_parked_before_the_sleep_disqualifies_a_sole_waiter() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(1);
        evidence.parked(WakeDelivery::Untouched, 0);
        assert!(!evidence.is_sole_waiter());
    }

    /// And so could one that arrived during it and is still there.
    #[test]
    fn a_peer_parked_after_the_sleep_disqualifies_a_sole_waiter() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::Untouched, 1);
        assert!(!evidence.is_sole_waiter());
    }

    /// A taken bit is recorded, but must not excuse the rescue: the
    /// staged lost wake in `the_monitor_counts_a_wake_that_never
    /// _arrived` produces exactly this state.
    #[test]
    fn a_taken_bit_is_recorded_without_excusing_the_rescue() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::InFlight, 0);
        assert!(evidence.is_sole_waiter());
        assert_eq!(
            evidence.explanation(),
            RescueExplanation::BitTaken,
            "a stolen wake bit looks identical to a late delivery"
        );
    }

    /// The observation is on record for the whole episode: a later
    /// quiet timeout must not erase a bit that was taken.
    #[test]
    fn a_later_untouched_park_does_not_erase_a_taken_bit() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::InFlight, 0);
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::Untouched, 0);
        assert_eq!(evidence.explanation(), RescueExplanation::BitTaken);
    }

    #[test]
    fn a_slotless_waiter_outranks_every_other_observation() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::InFlight, 0);
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::Slotless, 0);
        assert_eq!(evidence.explanation(), RescueExplanation::Slotless);
    }

    #[test]
    fn taking_the_evidence_resets_it_for_the_next_episode() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0);
        evidence.parked(WakeDelivery::InFlight, 0);

        let taken = evidence.take();
        assert!(taken.is_sole_waiter());
        assert_eq!(taken.explanation(), RescueExplanation::BitTaken);

        assert!(!evidence.is_sole_waiter());
        assert_eq!(evidence.explanation(), RescueExplanation::Untouched);
    }

    #[test]
    fn a_watch_that_never_parked_reports_no_sole_waiter() {
        assert!(!RescueEvidence::new().is_sole_waiter());
    }

    #[test]
    fn observations_are_ordered_by_how_much_they_account_for() {
        assert!(RescueExplanation::Slotless > RescueExplanation::BitTaken);
        assert!(RescueExplanation::BitTaken > RescueExplanation::Untouched);
    }
}
