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
//! The budget counts attempts, not time, and is deliberately small: a
//! blown budget must fail the test quickly rather than burn CPU.

/// Polls a latching condition until it holds, or the attempt budget
/// runs out.
pub struct ParkProbe {
    max_attempts: u32,
}

impl ParkProbe {
    const DEFAULT_ATTEMPTS: u32 = 20_000;

    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_attempts: Self::DEFAULT_ATTEMPTS,
        }
    }

    /// Returns `true` as soon as `cond` holds, `false` if the budget
    /// runs out first.
    pub fn wait_until(&self, mut cond: impl FnMut() -> bool) -> bool {
        for _ in 0..self.max_attempts {
            if cond() {
                return true;
            }
            std::thread::yield_now();
        }
        cond()
    }

    /// Waits until `cond` holds. Panics with `what` if the budget runs
    /// out first.
    pub fn expect_until(&self, what: &str, cond: impl FnMut() -> bool) {
        assert!(
            self.wait_until(cond),
            "timed out after {} polls waiting for: {what}",
            self.max_attempts
        );
    }
}

impl Default for ParkProbe {
    fn default() -> Self {
        Self::new()
    }
}
