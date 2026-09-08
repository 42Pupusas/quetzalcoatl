//! How long a [`ParkProbe`] keeps polling before it gives up.
//!
//! [`ParkProbe`]: super::park_probe::ParkProbe
//!
//! The two execution environments need different answers, and a single
//! constant cannot serve both.
//!
//! Under Miri the scheduler is deterministic and real time does not
//! advance, so a wall-clock deadline either never expires or expires
//! immediately. The budget must count attempts.
//!
//! On real hardware an attempt count is not a bound on anything the
//! test cares about. The waiter being probed has to exhaust a spin
//! schedule of roughly tens of microseconds before it arms its park
//! bit, and it only makes progress when the OS schedules it. A prober
//! spinning on `yield_now` can retire 20,000 attempts in a few
//! milliseconds on a loaded or low-core machine, so the count says more
//! about how many cores are idle than about whether the peer parked.
//! The budget must be a deadline.
//!
//! The deadline is generous because it is not a performance
//! assertion: the probe is waiting for a peer to reach a state it
//! reaches in microseconds when scheduled, so anything approaching the
//! limit means it is not being scheduled at all, and the extra wait
//! costs a passing run nothing.

/// A polling allowance, spent by repeated calls to
/// [`is_spent`](Self::is_spent).
pub struct ProbeBudget {
    #[cfg(miri)]
    attempts: u32,
    #[cfg(not(miri))]
    started: Option<std::time::Instant>,
}

impl ProbeBudget {
    #[cfg(miri)]
    const MAX_ATTEMPTS: u32 = 20_000;

    #[cfg(not(miri))]
    const LIMIT: std::time::Duration = std::time::Duration::from_secs(30);

    #[must_use]
    pub const fn new() -> Self {
        Self {
            #[cfg(miri)]
            attempts: 0,
            #[cfg(not(miri))]
            started: None,
        }
    }

    /// Consumes one unit of the allowance and reports whether it is
    /// now exhausted.
    ///
    /// The clock starts on the first call rather than at construction,
    /// so a probe built ahead of the wait still gets its full deadline.
    #[cfg(not(miri))]
    pub fn is_spent(&mut self) -> bool {
        let started = self.started.get_or_insert_with(std::time::Instant::now);
        started.elapsed() >= Self::LIMIT
    }

    #[cfg(miri)]
    pub fn is_spent(&mut self) -> bool {
        self.attempts = self.attempts.saturating_add(1);
        self.attempts >= Self::MAX_ATTEMPTS
    }

    /// How the allowance is measured, for the timeout message.
    #[cfg(not(miri))]
    pub fn describe() -> String {
        format!("{:?}", Self::LIMIT)
    }

    #[cfg(miri)]
    pub fn describe() -> String {
        format!("{} polls", Self::MAX_ATTEMPTS)
    }
}

impl Default for ProbeBudget {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ProbeBudget;

    #[test]
    fn a_fresh_budget_is_not_spent() {
        assert!(!ProbeBudget::new().is_spent());
    }

    #[test]
    fn a_budget_survives_the_polling_a_scheduled_peer_needs() {
        let mut budget = ProbeBudget::new();
        for _ in 0..10_000 {
            assert!(!budget.is_spent());
        }
    }

    /// The Miri budget is a count, so it must actually run out; the
    /// wall-clock one would take 30s to demonstrate and is left alone.
    #[test]
    #[cfg(miri)]
    fn an_attempt_budget_runs_out() {
        let mut budget = ProbeBudget::new();
        let spent = (0..ProbeBudget::MAX_ATTEMPTS).any(|_| budget.is_spent());
        assert!(spent);
    }
}
