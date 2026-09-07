//! Instrumentation for the [`PARK_BACKSTOP`](super::PARK_BACKSTOP)
//! timeout.
//!
//! The backstop exists because a residual missed wake was observed under
//! saturated stress and never explained. A timeout that never rescues
//! anyone costs nothing and is evidence the bound can go; one that does
//! rescue someone is a reproduction of the bug it guards against.
//!
//! Silently, the backstop cannot tell those apart — a lost wake becomes
//! a millisecond of latency and nothing else. These counters make the
//! difference observable.
//!
//! # What the counts mean
//!
//! A waiter's park handle is claimed by whoever wakes it, so a handle
//! still armed after the park returns proves no peer delivered a wake:
//! the sleep ended on the timeout. That is an *unwoken timeout*, and it
//! is routine — a waiter with genuinely nothing to do times out and
//! re-parks for as long as the ring stays quiet.
//!
//! The diagnostic count is a *rescue*: an unwoken timeout whose very
//! next re-check found work. The waiter was waiting for something that
//! was already there, and only the clock released it.
//!
//! Rescues are an upper bound on lost wakes, not an exact count. Work
//! can legitimately arrive during the timeout window, and
//! [`WakeSet::wake_one`](crate::common::park::WakeSet::wake_one) hands
//! each wake to one slot in round-robin order, so a waiter can be
//! passed over while another is served. A zero count across the stress
//! matrix is therefore strong evidence the bound is unnecessary; a
//! non-zero one is a starting point, not a verdict.
//!
//! `sole_waiter_rescues` narrows it. It counts the rescues where no
//! other waiter on this side was parked when the sleep ended, so there
//! was nobody for round-robin to have preferred at that moment. Those
//! are much harder to explain away as scheduling policy.
//!
//! The count is sampled both before the sleep and after it, and only a
//! rescue with no peer at *either* end is counted, which catches a peer
//! that was already waiting or is still waiting. It is still not a
//! proof: a peer that parks and leaves entirely within our sleep falls
//! between the samples. Treat a sole-waiter rescue as the strongest
//! available evidence of a lost wake, not as a closed case.
//!
//! # Cost
//!
//! Gated behind the `backstop-metrics` feature. Without it
//! [`BackstopWatch`] is a zero-sized type whose methods have empty
//! bodies, and [`RingBuffer`](super::RingBuffer) carries no counters.

#[cfg(feature = "backstop-metrics")]
use std::sync::atomic::{AtomicU64, Ordering};

/// A reading of one ring's backstop counters.
#[cfg(feature = "backstop-metrics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackstopStats {
    /// Parks that ended on the timeout rather than a peer's wake.
    pub unwoken_timeouts: u64,
    /// Unwoken timeouts whose next re-check found work waiting.
    pub rescues: u64,
    /// Rescues where no other waiter was parked on this side, so
    /// round-robin cannot explain the wake going elsewhere.
    pub sole_waiter_rescues: u64,
}

#[cfg(feature = "backstop-metrics")]
impl BackstopStats {
    /// Whether the backstop ever released a waiter that had work
    /// available — the signature of a wake that went missing.
    #[must_use]
    pub const fn saw_rescue(self) -> bool {
        self.rescues > 0
    }

    /// Whether a rescue happened with no peer parked to absorb the
    /// wake. Round-robin cannot account for these: the wake was owed
    /// to this waiter and never arrived.
    #[must_use]
    pub const fn saw_unexplained_rescue(self) -> bool {
        self.sole_waiter_rescues > 0
    }
}

/// Per-ring backstop counters.
#[cfg(feature = "backstop-metrics")]
pub struct BackstopMonitor {
    unwoken_timeouts: AtomicU64,
    rescues: AtomicU64,
    sole_waiter_rescues: AtomicU64,
}

#[cfg(feature = "backstop-metrics")]
impl BackstopMonitor {
    pub(super) const fn new() -> Self {
        Self {
            unwoken_timeouts: AtomicU64::new(0),
            rescues: AtomicU64::new(0),
            sole_waiter_rescues: AtomicU64::new(0),
        }
    }

    fn record_timeout(&self) {
        self.unwoken_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rescue(&self, sole_waiter: bool) {
        self.rescues.fetch_add(1, Ordering::Relaxed);
        if sole_waiter {
            self.sole_waiter_rescues.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Reads both counters.
    ///
    /// The two loads are independent, so a concurrent waiter can be
    /// counted in one and not the other. Read a quiesced ring for an
    /// exact pair.
    pub(super) fn stats(&self) -> BackstopStats {
        BackstopStats {
            unwoken_timeouts: self.unwoken_timeouts.load(Ordering::Relaxed),
            rescues: self.rescues.load(Ordering::Relaxed),
            sole_waiter_rescues: self.sole_waiter_rescues.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "backstop-metrics")]
impl Default for BackstopMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks one waiter's park loop, attributing a rescue to the timeout
/// that preceded it.
///
/// Held across a single blocking call. `parked` records how each sleep
/// ended; `made_progress` closes the loop, counting a rescue when the
/// waiter succeeds directly after an unwoken timeout.
///
/// The monitor is held as a raw pointer because `reserve_block` needs
/// `&mut self` on the producer for the whole loop this watch spans.
#[cfg(feature = "backstop-metrics")]
pub struct BackstopWatch {
    monitor: *const BackstopMonitor,
    timed_out: bool,
    sole_waiter: bool,
}

#[cfg(feature = "backstop-metrics")]
impl BackstopWatch {
    /// # Safety
    ///
    /// `monitor` must stay live until this watch is dropped.
    pub(super) const unsafe fn new(monitor: *const BackstopMonitor) -> Self {
        Self {
            monitor,
            timed_out: false,
            sole_waiter: false,
        }
    }

    const fn monitor(&self) -> &BackstopMonitor {
        // SAFETY: `new` requires the monitor to outlive this watch; it
        // lives in the ring, which the caller's handle keeps alive.
        unsafe { &*self.monitor }
    }

    /// Records the peer count sampled just before the sleep, so a
    /// waiter that was already parked when we went to sleep is not
    /// mistaken for an absent one.
    pub(crate) const fn about_to_park(&mut self, peers_parked: u32) {
        self.sole_waiter = peers_parked == 0;
    }

    /// Records how the park that just returned ended. `unwoken` is
    /// whether this waiter's handle was still armed, meaning no peer
    /// claimed it; `peers_parked` is how many other waiters on this
    /// side were parked, which is what round-robin could have served
    /// instead of us.
    pub(crate) fn parked(&mut self, unwoken: bool, peers_parked: u32) {
        self.timed_out = unwoken;
        self.sole_waiter = self.sole_waiter && peers_parked == 0;
        if unwoken {
            self.monitor().record_timeout();
        }
    }

    /// Records that the waiter found work. Counts a rescue when the
    /// preceding park ended on the timeout.
    pub(crate) fn made_progress(&mut self) {
        if std::mem::replace(&mut self.timed_out, false) {
            self.monitor().record_rescue(self.sole_waiter);
        }
    }
}

/// No-op twin, compiled when `backstop-metrics` is off.
#[cfg(not(feature = "backstop-metrics"))]
pub struct BackstopWatch;

#[cfg(not(feature = "backstop-metrics"))]
#[allow(
    clippy::unused_self,
    clippy::needless_pass_by_ref_mut,
    reason = "signature mirrors the instrumented twin so call sites need no cfg"
)]
impl BackstopWatch {
    #[inline]
    pub(crate) const fn about_to_park(&mut self, _peers_parked: u32) {}

    #[inline]
    pub(crate) const fn parked(&mut self, _unwoken: bool, _peers_parked: u32) {}

    #[inline]
    pub(crate) const fn made_progress(&mut self) {}
}
