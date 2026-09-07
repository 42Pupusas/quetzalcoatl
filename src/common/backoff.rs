//! Contention backoff: the escalating spin a waiter performs before it
//! gives up and pays for a park.
//!
//! Two situations use it. A CAS retry loop backs off after every failed
//! compare-exchange and never parks, because the position it wants is
//! being taken by a peer that is making progress. A blocking loop backs
//! off while the ring is full (or empty) and then parks, because the
//! peer it waits on may not be running at all.
//!
//! Both are the same escalation over the same counter, which is why
//! they share this type rather than a threshold constant that every
//! call site compares against by hand.

/// Failure count past which spinning stops paying and a park is
/// cheaper.
///
/// The schedule saturates at 6 (64 pauses per call, plus a `yield_now`
/// from 8 on). Reaching 12 means roughly tens of microseconds have
/// already been burned, which amortizes a futex round-trip of 1–10μs.
const PARK_THRESHOLD: u32 = 12;

/// Escalation cap. Past this the spin count stops doubling; the
/// counter keeps rising only so [`Backoff::is_exhausted`] can latch.
#[cfg(not(miri))]
const SPIN_CAP: u32 = 6;

/// Counter cap, chosen so the value saturates rather than wraps.
const COUNTER_CAP: u32 = 12;

/// Call count from which each backoff also yields the timeslice.
#[cfg(not(miri))]
const YIELD_FROM: u32 = 8;

/// Exponential backoff for contended loops.
///
/// Counts consecutive failures and spins for longer on each one:
///
/// - calls 1–2: no spin, the counter only ramps up
/// - calls 3–7: 4, 8, 16, 32, 64 pauses (~17μs in total)
/// - calls 8+: 64 pauses, capped, plus a `yield_now` — by this point
///   the peer holding us up is more likely preempted than merely slow.
///   The yield is sparse because it only starts once the counter has
///   nearly saturated, so a burst of contention does not trigger it.
#[derive(Debug, Default, Clone, Copy)]
pub struct Backoff {
    failures: u32,
}

impl Backoff {
    /// A fresh counter, as taken at the top of a retry loop.
    #[must_use]
    pub const fn new() -> Self {
        Self { failures: 0 }
    }

    /// Restarts the schedule.
    ///
    /// Called after a park returns, and after a lap that observed
    /// progress but could not use it — a peer took the slot first, say.
    /// Either way the next wait is a fresh one and should spin before
    /// it parks again, rather than inheriting an exhausted counter and
    /// parking immediately.
    #[inline]
    pub const fn reset(&mut self) {
        self.failures = 0;
    }

    /// Whether spinning has gone on long enough that parking is the
    /// cheaper option.
    ///
    /// Loops that cannot park (a CAS retry racing a peer that is
    /// making progress) ignore this and just call [`spin`](Self::spin).
    #[must_use]
    #[inline]
    pub const fn is_exhausted(self) -> bool {
        self.failures >= PARK_THRESHOLD
    }

    /// Backs off once, escalating the schedule.
    ///
    /// Marked `#[inline(never)]`: this only runs under real contention,
    /// and keeping it out of line holds the hot CAS loop's instruction
    /// footprint down.
    #[inline(never)]
    pub fn spin(&mut self) {
        #[cfg(miri)]
        {
            // spin_loop is an interleaving point under Miri, and
            // exponential counts explode the state space. Yielding
            // still advances the counter, which matters: pinning it at
            // zero would make every waiter spin forever and hide the
            // park and unpark paths from Miri entirely.
            std::thread::yield_now();
        }
        #[cfg(not(miri))]
        {
            let f = self.failures;
            if f > 1 {
                for _ in 0..1u32 << f.min(SPIN_CAP) {
                    std::hint::spin_loop();
                }
            }
            if f >= YIELD_FROM {
                std::thread::yield_now();
            }
        }
        self.failures = self.failures.saturating_add(1).min(COUNTER_CAP);
    }

    /// Backs off and reports `true` when the caller should keep
    /// spinning, or reports `false` without spinning once the schedule
    /// is spent and the caller should park.
    ///
    /// This is the whole pre-park gate, so a blocking loop reads:
    ///
    /// ```
    /// # use quetzalcoatl::Backoff;
    /// # fn ring_has_space() -> bool { true }
    /// let mut backoff = Backoff::new();
    /// loop {
    ///     if ring_has_space() {
    ///         break;
    ///     }
    ///     if backoff.spin_unless_exhausted() {
    ///         continue;
    ///     }
    ///     // arm the park, re-check, park
    ///     break;
    /// }
    /// ```
    #[inline]
    pub fn spin_unless_exhausted(&mut self) -> bool {
        if self.is_exhausted() {
            return false;
        }
        self.spin();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_backoff_has_not_spun() {
        assert!(!Backoff::new().is_exhausted());
    }

    #[test]
    fn the_schedule_latches_after_enough_failures() {
        let mut backoff = Backoff::new();
        for _ in 0..PARK_THRESHOLD {
            assert!(!backoff.is_exhausted());
            backoff.spin();
        }
        assert!(backoff.is_exhausted());
    }

    #[test]
    fn the_gate_spins_up_to_the_threshold_then_defers_to_parking() {
        let mut backoff = Backoff::new();
        let spins = std::iter::from_fn(|| backoff.spin_unless_exhausted().then_some(()))
            .take(1_000)
            .count();
        assert_eq!(spins, PARK_THRESHOLD as usize);
    }

    #[test]
    fn an_exhausted_backoff_stays_exhausted() {
        let mut backoff = Backoff::new();
        for _ in 0..1_000 {
            backoff.spin();
        }
        assert!(backoff.is_exhausted());
        assert!(!backoff.spin_unless_exhausted());
    }

    #[test]
    fn the_counter_saturates_rather_than_wrapping() {
        let mut backoff = Backoff::new();
        for _ in 0..10_000 {
            backoff.spin();
        }
        assert_eq!(backoff.failures, COUNTER_CAP);
    }

    #[test]
    fn a_reset_returns_an_exhausted_backoff_to_the_start_of_the_schedule() {
        let mut backoff = Backoff::new();
        for _ in 0..PARK_THRESHOLD {
            backoff.spin();
        }
        assert!(backoff.is_exhausted());

        backoff.reset();

        assert!(!backoff.is_exhausted());
        assert_eq!(backoff.failures, Backoff::new().failures);
    }

    #[test]
    fn a_reset_backoff_spins_the_full_schedule_again() {
        let mut backoff = Backoff::new();
        while backoff.spin_unless_exhausted() {}
        backoff.reset();

        let spins = std::iter::from_fn(|| backoff.spin_unless_exhausted().then_some(()))
            .take(1_000)
            .count();

        assert_eq!(spins, PARK_THRESHOLD as usize);
    }

    #[test]
    fn a_default_backoff_matches_a_new_one() {
        assert_eq!(Backoff::default().failures, Backoff::new().failures);
    }
}
