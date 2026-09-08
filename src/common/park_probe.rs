//! Test-only synchronization probe for blocking tests.
//!
//! A blocking test must not trigger a wake until the peer thread has
//! committed to parking. A wall-clock `sleep` does not establish this.
//! Miri's scheduler is deterministic and does not advance real time, so
//! the sleep returns with the peer still running and the test fails
//! spuriously.
//!
//! [`ParkProbe`] polls a *latching* observable instead, and yields
//! between reads to give the peer a preemption point.
//!
//! # Choosing the observable
//!
//! The observable must latch for long enough that a polling probe
//! cannot miss it. [`SoleParker::is_parked`] is set before the waiter's
//! final re-checks and stays set while it is parked, so a probe polling
//! it observes a waiter that has committed to parking.
//!
//! [`SoleParker::is_parked`]: super::sole_parker::SoleParker::is_parked
//!
//! An armed park handle is *not* a safe substitute: a wake claims the
//! handle out of its slot, so the observable disappears the moment the
//! peer acts on it. Probe [`SoleParker::is_parked`] or the wake bitmap
//! instead.
//!
//! How long the probe waits is [`ProbeBudget`]'s decision, because the
//! right bound differs between Miri and real hardware.

use super::probe_budget::ProbeBudget;

/// Polls a latching condition until it holds, or the budget runs out.
///
/// Each wait draws its own [`ProbeBudget`], so one wait timing out
/// does not eat the allowance of the next.
pub struct ParkProbe;

impl ParkProbe {
    /// Returns `true` as soon as `cond` holds, `false` if the budget
    /// runs out first.
    pub fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
        let mut budget = ProbeBudget::new();
        loop {
            if cond() {
                return true;
            }
            if budget.is_spent() {
                return cond();
            }
            std::thread::yield_now();
        }
    }

    /// Waits until `cond` holds. Panics with `what` if the budget runs
    /// out first.
    pub fn expect_until(what: &str, cond: impl FnMut() -> bool) {
        assert!(
            Self::wait_until(cond),
            "timed out after {} waiting for: {what}",
            ProbeBudget::describe()
        );
    }
}
