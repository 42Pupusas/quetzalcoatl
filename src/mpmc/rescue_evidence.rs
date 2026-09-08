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
//! # When the work arrived
//!
//! Separately from who touched the bit: *when* did the work the rescue
//! found appear, relative to the sleep? Two samples bracket it, one
//! just before the park and one the instant it returns, and
//! [`RescueTiming`] names the three answers.
//!
//! - **Blind**: visible before the park. No wake was owed — the publish
//!   preceded the arm — and the waiter's own re-check is what failed.
//!   A scan defect, not a wake defect.
//! - **Late**: not visible when the park returned. The item arrived in
//!   the window between the sleep ending and the next attempt, so the
//!   timeout did not rescue anything; it merely preceded the work. A
//!   spurious park return followed by an ordinary publish lands here
//!   too.
//! - **Waiting**: not visible before, visible at return. The item was
//!   published *during* the sleep and the waiter was not woken for it.
//!   With an untouched bit, this is the lost-wake signature and the
//!   only timing a defect in wake delivery can produce.
//!
//! From the waiter's side all three end the same way — a timeout and
//! work found — which is why the two samples are needed to tell them
//! apart.
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

/// When, relative to the sleep, the work a rescue found appeared.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RescueTiming {
    /// Visible before the park: the pre-park re-check missed it.
    Blind,
    /// Not visible when the park returned: it arrived afterwards.
    Late,
    /// Appeared during the sleep and was there when it ended.
    Waiting,
}

/// The facts gathered across one waiter's park episode.
#[derive(Clone, Copy, Debug)]
pub struct RescueEvidence {
    sole_waiter: bool,
    blind: bool,
    work_at_return: bool,
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
            blind: false,
            work_at_return: false,
            explanation: RescueExplanation::Untouched,
        }
    }

    /// Records what was sampled just before sleeping: how many peers
    /// were parked, and whether work was visible to this waiter.
    ///
    /// Work visible at any park in the episode marks it blind for
    /// good: the re-check that preceded that park had already failed,
    /// so the item outlived the decision to sleep.
    pub const fn about_to_park(&mut self, peers_parked: u32, work_visible: bool) {
        self.sole_waiter = peers_parked == 0;
        self.blind = self.blind || work_visible;
    }

    /// Records how one park ended, and whether work was visible the
    /// instant it did.
    ///
    /// A peer parked at *either* end of the sleep means round-robin
    /// had somewhere else to send the wake, so `sole_waiter` only
    /// survives when both samples saw nobody.
    ///
    /// `work_visible` is per park, not latched: the rescue is
    /// attributed to the sleep that immediately preceded it.
    pub fn parked(&mut self, delivery: WakeDelivery, peers_parked: u32, work_visible: bool) {
        self.sole_waiter = self.sole_waiter && peers_parked == 0;
        self.work_at_return = work_visible;
        self.explanation = self.explanation.strongest(delivery.into());
    }

    /// Whether no peer was parked to have absorbed this waiter's wake.
    #[must_use]
    pub const fn is_sole_waiter(self) -> bool {
        self.sole_waiter
    }

    /// Whether the waiter parked with work already visible to it.
    ///
    /// A blind rescue is explained without any wake going missing:
    /// the publish preceded the arm, so no wake was owed, and the
    /// waiter's own re-check is what failed.
    #[must_use]
    pub const fn was_blind(self) -> bool {
        self.blind
    }

    /// When the work the rescue found appeared, relative to the sleep.
    #[must_use]
    pub const fn timing(self) -> RescueTiming {
        if self.blind {
            RescueTiming::Blind
        } else if self.work_at_return {
            RescueTiming::Waiting
        } else {
            RescueTiming::Late
        }
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
    use super::{RescueEvidence, RescueExplanation, RescueTiming, WakeDelivery};

    #[test]
    fn a_waiter_alone_at_both_samples_is_a_sole_waiter() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 0, true);
        assert!(evidence.is_sole_waiter());
        assert!(!evidence.was_blind());
        assert_eq!(evidence.explanation(), RescueExplanation::Untouched);
    }

    /// Work visible at the moment of parking means the re-check
    /// missed it; no wake was owed for a publish that preceded the
    /// arm.
    #[test]
    fn parking_with_work_visible_marks_the_episode_blind() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, true);
        evidence.parked(WakeDelivery::Untouched, 0, true);
        assert!(evidence.was_blind());
        assert_eq!(evidence.timing(), RescueTiming::Blind);
        assert!(
            evidence.is_sole_waiter(),
            "blindness is a separate axis from who else was parked"
        );
    }

    /// A later park with nothing visible does not un-see the earlier
    /// one: the item was there, and the re-check before that park
    /// failed to find it.
    #[test]
    fn a_later_park_with_nothing_visible_does_not_clear_blindness() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, true);
        evidence.parked(WakeDelivery::Untouched, 0, false);
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 0, false);
        assert!(evidence.was_blind());
        assert_eq!(evidence.timing(), RescueTiming::Blind);
    }

    /// Nothing before the sleep, something the instant it ended: the
    /// item was published while the waiter slept, and nobody woke it.
    #[test]
    fn work_that_appeared_during_the_sleep_was_waiting() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 0, true);
        assert_eq!(evidence.timing(), RescueTiming::Waiting);
    }

    /// Nothing at either end of the sleep: whatever the rescue found
    /// arrived after the park returned, so the timeout rescued nothing.
    #[test]
    fn work_absent_when_the_park_returned_arrived_late() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 0, false);
        assert_eq!(evidence.timing(), RescueTiming::Late);
    }

    /// The rescue belongs to the sleep that immediately preceded it,
    /// so an earlier park's return sample must not carry over.
    #[test]
    fn the_return_sample_is_the_last_parks_not_an_earlier_ones() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 0, true);
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 0, false);
        assert_eq!(evidence.timing(), RescueTiming::Late);
    }

    /// A peer that was already waiting when we went to sleep could
    /// have taken our wake, so round-robin explains the rescue.
    #[test]
    fn a_peer_parked_before_the_sleep_disqualifies_a_sole_waiter() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(1, false);
        evidence.parked(WakeDelivery::Untouched, 0, true);
        assert!(!evidence.is_sole_waiter());
    }

    /// And so could one that arrived during it and is still there.
    #[test]
    fn a_peer_parked_after_the_sleep_disqualifies_a_sole_waiter() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 1, true);
        assert!(!evidence.is_sole_waiter());
    }

    /// A taken bit is recorded, but must not excuse the rescue: the
    /// staged lost wake in `the_monitor_counts_a_wake_that_never
    /// _arrived` produces exactly this state.
    #[test]
    fn a_taken_bit_is_recorded_without_excusing_the_rescue() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::InFlight, 0, true);
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
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::InFlight, 0, true);
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Untouched, 0, true);
        assert_eq!(evidence.explanation(), RescueExplanation::BitTaken);
    }

    #[test]
    fn a_slotless_waiter_outranks_every_other_observation() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::InFlight, 0, true);
        evidence.about_to_park(0, false);
        evidence.parked(WakeDelivery::Slotless, 0, true);
        assert_eq!(evidence.explanation(), RescueExplanation::Slotless);
    }

    #[test]
    fn taking_the_evidence_resets_it_for_the_next_episode() {
        let mut evidence = RescueEvidence::new();
        evidence.about_to_park(0, true);
        evidence.parked(WakeDelivery::InFlight, 0, true);

        let taken = evidence.take();
        assert!(taken.is_sole_waiter());
        assert!(taken.was_blind());
        assert_eq!(taken.explanation(), RescueExplanation::BitTaken);

        assert!(!evidence.is_sole_waiter());
        assert!(!evidence.was_blind());
        assert_eq!(evidence.timing(), RescueTiming::Late);
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
