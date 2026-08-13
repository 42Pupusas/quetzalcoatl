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
//! The observable must be monotonic. `producer_parked` is **not**: the
//! waiter sets it, re-checks, and may clear it and return without ever
//! parking, so a probe can miss the edge and spin forever. The parker
//! `OnceLock` handles are monotonic — installed before the re-checks and
//! never cleared — so they are the correct thing to wait on.
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
