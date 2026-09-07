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
//! between the samples.
//!
//! [`WakeSet::wake_one`] clears a waiter's bit *before* claiming its
//! handle, so a timeout firing between those two steps finds an armed
//! handle with the bit already gone. [`WakeDelivery`] reads both and
//! `bit_taken_rescues` counts the sole-waiter rescues that looked like
//! that.
//!
//! That split narrows where to look; it does not exonerate anything.
//! A bit cleared with no unpark arriving is equally the signature of a
//! wake that was *stolen*, which is exactly how
//! `the_monitor_counts_a_wake_that_never_arrived` stages a lost wake.
//! So the assertion under stress stays on the raw
//! [`sole_waiter_rescues`](BackstopStats::sole_waiter_rescues); the
//! two populations are reported beside each other rather than
//! subtracted.
//!
//! # Futile wakes: what "round-robin explains it" is really claiming
//!
//! Attributing a rescue to round-robin says the wake reached another
//! waiter instead. That is only harmless if the other waiter could use
//! it, and in mpmc it often cannot: a producer parks holding a batch of
//! *specific* reserved positions, and a slot is free only for the exact
//! position [`DoneWord`](super::done_word::DoneWord) names. A producer
//! woken for a position outside its batch re-parks, and the publish
//! that woke it is spent — [`WakeSet::wake_one`] delivers one unpark per
//! publish, so the producer that *was* waiting on that position does
//! not get one.
//!
//! `futile_wakes` counts exactly that: a park a peer's wake genuinely
//! ended, whose waiter then found nothing to do and parked again. It
//! separates "the wake was lost" from "the wake was delivered to the
//! wrong waiter", which the rescue counts cannot do alone. A rescue
//! accompanied by futile wakes is a routing failure; a sole-waiter
//! rescue with none is a genuinely lost wake.
//!
//! A non-zero count is not by itself a defect — a woken producer can
//! lose a slot to a peer that claimed it first, which is ordinary
//! contention. The count matters as a *rate* next to the rescues, and
//! as the thing to watch when a wake-routing change is meant to help.
//!
//! [`WakeSet::wake_one`]: crate::common::park::WakeSet::wake_one
//!
//! # Cost
//!
//! Gated behind the `backstop-metrics` feature. Without it
//! [`BackstopWatch`] is a zero-sized type whose methods have empty
//! bodies, and [`RingBuffer`](super::RingBuffer) carries no counters.

#[cfg(feature = "backstop-metrics")]
use super::rescue_evidence::{RescueEvidence, RescueExplanation};
#[cfg(feature = "backstop-metrics")]
use crate::common::park_registry::ParkSlot;
#[cfg(feature = "backstop-metrics")]
use crate::common::wake_delivery::WakeDelivery;
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
    /// Sole-waiter rescues where the waiter's wake bit had been
    /// cleared but no unpark ever arrived.
    ///
    /// **Not an exoneration.** This is consistent with a peer caught
    /// mid-`wake_one`, and equally consistent with a wake that was
    /// consumed without being delivered — the staged lost wake in
    /// `the_monitor_counts_a_wake_that_never_arrived` produces exactly
    /// this reading. Subtracting it from
    /// [`sole_waiter_rescues`](Self::sole_waiter_rescues) would hide
    /// the defect rather than explain it, so the two are reported side
    /// by side and the raw count remains the one to assert on.
    ///
    /// Its use is to split the rescues into two populations when
    /// comparing wake policies, and to say where to look.
    pub bit_taken_rescues: u64,
    /// Rescues of a waiter holding no park slot.
    ///
    /// Such a waiter publishes no wake bit and arms no handle, so no
    /// peer can reach it and *every* one of its parks ends on the
    /// timeout. Its rescues are structural and are excluded from the
    /// sole-waiter count rather than counted as lost wakes.
    pub slotless_rescues: u64,
    /// Futile wakes where work was still visible to the waiter at
    /// the moment it gave up and re-parked.
    ///
    /// This is the discriminator between the two explanations for a
    /// futile wake. If a peer claimed the item first, the ring is
    /// genuinely barren by the time we look, and the wake was an
    /// ordinary lost race — a cost, not a defect. If work is *still*
    /// there and the waiter parks anyway, the waiter failed to find
    /// what it was woken for, and that is a defect in the scan.
    ///
    /// Sampled after the pre-park re-check has already failed, so a
    /// count here means the item outlived the decision to sleep.
    ///
    /// Not a defect count on its own. The sample is one more
    /// unsynchronised look at a ring other threads are still working,
    /// so an item published between the failed re-check and the sample
    /// reads as blindness when nothing was missed. Reading it as a
    /// *rate against a changed policy* is sound; reading a single
    /// event as a bug is not.
    pub blind_futile_wakes: u64,
    /// Blind futile wakes suffered by producers specifically.
    ///
    /// Separates a consumer that failed to find a published item from
    /// a producer that failed to find a free slot. The two have
    /// different causes and only the first is a scan defect.
    pub producer_blind_futile_wakes: u64,
    /// Futile wakes suffered by producers specifically.
    ///
    /// Only producers can be pinned to a position, so only theirs can
    /// be removed by routing a wake to the position it frees. Counting
    /// both sides together hides that: a consumer scans for any
    /// published slot and its futile wakes are ordinary lost races,
    /// not misrouting.
    pub producer_futile_wakes: u64,
    /// Parks that a peer's wake genuinely ended, but whose waiter
    /// then found nothing to do and parked again.
    ///
    /// This is the cost of routing a wake by park slot when waiters
    /// are not interchangeable. An mpmc producer parks holding a
    /// batch of specific reserved positions and
    /// [`DoneWord::is_free_for`](super::done_word::DoneWord) matches
    /// an exact position, so a producer woken for a position outside
    /// its batch cannot use it. The publish that woke it is spent,
    /// and the producer that *was* waiting on that position is not
    /// woken by it.
    ///
    /// Distinguishes "the wake was lost" from "the wake was
    /// delivered to someone who could not use it", which the rescue
    /// counts alone cannot.
    pub futile_wakes: u64,
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

    /// Sole-waiter rescues where nobody had even taken the wake bit.
    ///
    /// The narrower of the two populations, and the one where a lost
    /// wake would have left no trace at all. The complement is
    /// [`bit_taken_rescues`](Self::bit_taken_rescues), which is
    /// *also* consistent with a lost wake — neither is safe to treat
    /// as benign.
    #[must_use]
    pub const fn untouched_sole_waiter_rescues(self) -> u64 {
        self.sole_waiter_rescues
            .saturating_sub(self.bit_taken_rescues)
    }

    /// Whether any delivered wake was spent on a waiter that could
    /// not use it.
    #[must_use]
    pub const fn saw_futile_wake(self) -> bool {
        self.futile_wakes > 0
    }

    /// Whether a waiter ever re-parked with work still visible to it.
    /// Unlike a plain futile wake, contention cannot explain this:
    /// the item was there and the waiter went to sleep anyway.
    #[must_use]
    pub const fn saw_blind_futile_wake(self) -> bool {
        self.blind_futile_wakes > 0
    }
}

/// Per-ring backstop counters.
#[cfg(feature = "backstop-metrics")]
pub struct BackstopMonitor {
    unwoken_timeouts: AtomicU64,
    rescues: AtomicU64,
    sole_waiter_rescues: AtomicU64,
    bit_taken_rescues: AtomicU64,
    slotless_rescues: AtomicU64,
    futile_wakes: AtomicU64,
    producer_futile_wakes: AtomicU64,
    blind_futile_wakes: AtomicU64,
    producer_blind_futile_wakes: AtomicU64,
}

/// Which side of the ring a watched waiter sits on.
///
/// Producers can be pinned to a specific position; consumers take any
/// published slot. The distinction decides whether a futile wake is
/// something wake routing could have prevented.
#[cfg(feature = "backstop-metrics")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WaiterSide {
    Producer,
    Consumer,
}

#[cfg(feature = "backstop-metrics")]
impl BackstopMonitor {
    pub(super) const fn new() -> Self {
        Self {
            unwoken_timeouts: AtomicU64::new(0),
            rescues: AtomicU64::new(0),
            sole_waiter_rescues: AtomicU64::new(0),
            bit_taken_rescues: AtomicU64::new(0),
            slotless_rescues: AtomicU64::new(0),
            futile_wakes: AtomicU64::new(0),
            producer_futile_wakes: AtomicU64::new(0),
            blind_futile_wakes: AtomicU64::new(0),
            producer_blind_futile_wakes: AtomicU64::new(0),
        }
    }

    fn record_timeout(&self) {
        self.unwoken_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rescue(&self, evidence: RescueEvidence) {
        self.rescues.fetch_add(1, Ordering::Relaxed);
        if matches!(evidence.explanation(), RescueExplanation::Slotless) {
            self.slotless_rescues.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !evidence.is_sole_waiter() {
            return;
        }
        self.sole_waiter_rescues.fetch_add(1, Ordering::Relaxed);
        if matches!(evidence.explanation(), RescueExplanation::BitTaken) {
            self.bit_taken_rescues.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_futile_wake(&self, side: WaiterSide, work_visible: bool) {
        self.futile_wakes.fetch_add(1, Ordering::Relaxed);
        if matches!(side, WaiterSide::Producer) {
            self.producer_futile_wakes.fetch_add(1, Ordering::Relaxed);
        }
        if work_visible {
            self.blind_futile_wakes.fetch_add(1, Ordering::Relaxed);
            if matches!(side, WaiterSide::Producer) {
                self.producer_blind_futile_wakes
                    .fetch_add(1, Ordering::Relaxed);
            }
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
            bit_taken_rescues: self.bit_taken_rescues.load(Ordering::Relaxed),
            slotless_rescues: self.slotless_rescues.load(Ordering::Relaxed),
            futile_wakes: self.futile_wakes.load(Ordering::Relaxed),
            producer_futile_wakes: self.producer_futile_wakes.load(Ordering::Relaxed),
            blind_futile_wakes: self.blind_futile_wakes.load(Ordering::Relaxed),
            producer_blind_futile_wakes: self
                .producer_blind_futile_wakes
                .load(Ordering::Relaxed),
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
    side: WaiterSide,
    evidence: RescueEvidence,
    timed_out: bool,
    woken: bool,
}

#[cfg(feature = "backstop-metrics")]
impl BackstopWatch {
    /// # Safety
    ///
    /// `monitor` must stay live until this watch is dropped.
    pub(super) const unsafe fn new(monitor: *const BackstopMonitor, side: WaiterSide) -> Self {
        Self {
            monitor,
            side,
            evidence: RescueEvidence::new(),
            timed_out: false,
            woken: false,
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
    /// Reaching this a second time without an intervening
    /// [`made_progress`](Self::made_progress) means the wake that
    /// ended the last park bought nothing: the waiter looped, found
    /// no work, and is parking again.
    pub(crate) fn about_to_park(&mut self, peers_parked: u32, work_visible: bool) {
        if std::mem::replace(&mut self.woken, false) {
            self.monitor().record_futile_wake(self.side, work_visible);
        }
        self.evidence.about_to_park(peers_parked);
    }

    /// Records how the park that just returned ended, reading the
    /// waiter's state out of `set` itself.
    ///
    /// Sampling here rather than at the call site keeps
    /// [`WakeDelivery`] out of the uninstrumented build, where the
    /// no-op twin takes the same arguments and does nothing.
    pub(crate) fn parked_at(&mut self, set: &crate::common::park::WakeSet, slot: ParkSlot) {
        self.parked(WakeDelivery::sample(set, slot), set.others_parked(slot));
    }

    /// Records how the park that just returned ended. `delivery` says
    /// whether a peer's unpark reached us, was in flight, or had not
    /// begun; `peers_parked` is how many other waiters on this side
    /// were parked, which is what round-robin could have served
    /// instead of us.
    fn parked(&mut self, delivery: WakeDelivery, peers_parked: u32) {
        let unwoken = delivery.is_unwoken();
        self.timed_out = unwoken;
        self.woken = !unwoken;
        self.evidence.parked(delivery, peers_parked);
        if unwoken {
            self.monitor().record_timeout();
        }
    }

    /// Records that the waiter found work. Counts a rescue when the
    /// preceding park ended on the timeout.
    pub(crate) fn made_progress(&mut self) {
        self.woken = false;
        let evidence = self.evidence.take();
        if std::mem::replace(&mut self.timed_out, false) {
            self.monitor().record_rescue(evidence);
        }
    }
}

#[cfg(all(test, feature = "backstop-metrics"))]
mod tests {
    use super::{BackstopMonitor, BackstopWatch, WaiterSide, WakeDelivery};

    /// Drives a watch through one park cycle, so the counter logic is
    /// tested without threads, rings or timing.
    struct Cycle {
        monitor: BackstopMonitor,
    }

    impl Cycle {
        const fn new() -> Self {
            Self {
                monitor: BackstopMonitor::new(),
            }
        }

        const fn watch(&self, side: WaiterSide) -> BackstopWatch {
            // SAFETY: the monitor outlives the watch, both owned here.
            unsafe { BackstopWatch::new(std::ptr::from_ref(&self.monitor), side) }
        }
    }

    /// A wake that ends a park and leaves the waiter with nothing is
    /// futile; whether it was *blind* is the question of whether work
    /// was visible when it gave up, and both answers must be
    /// recordable.
    #[test]
    fn a_futile_wake_is_blind_only_when_work_was_visible() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Delivered, 0);
        watch.about_to_park(0, true);

        let stats = cycle.monitor.stats();
        assert_eq!(stats.futile_wakes, 1);
        assert_eq!(
            stats.blind_futile_wakes, 1,
            "work was visible when the waiter re-parked: {stats:?}"
        );
    }

    #[test]
    fn a_futile_wake_on_an_empty_ring_is_not_blind() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Delivered, 0);
        watch.about_to_park(0, false);

        let stats = cycle.monitor.stats();
        assert_eq!(stats.futile_wakes, 1);
        assert_eq!(
            stats.blind_futile_wakes, 0,
            "nothing was there to be missed: {stats:?}"
        );
    }

    /// Work being visible is only interesting when a wake was wasted.
    /// A park that ends on the timeout is not a futile wake, so it
    /// must not be counted as a blind one however much work is around.
    #[test]
    fn a_timeout_with_work_visible_is_not_a_blind_futile_wake() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Untouched, 0);
        watch.about_to_park(0, true);

        let stats = cycle.monitor.stats();
        assert_eq!(stats.unwoken_timeouts, 1);
        assert_eq!(stats.futile_wakes, 0);
        assert_eq!(
            stats.blind_futile_wakes, 0,
            "no wake was spent, so nothing was wasted: {stats:?}"
        );
    }

    /// A waiter that uses its wake has not suffered a futile one, so
    /// the visibility flag on its next park must not be attributed to
    /// the wake it already spent.
    #[test]
    fn progress_clears_the_pending_wake_before_the_next_park() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Producer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Delivered, 0);
        watch.made_progress();
        watch.about_to_park(0, true);

        let stats = cycle.monitor.stats();
        assert_eq!(stats.futile_wakes, 0);
        assert_eq!(stats.blind_futile_wakes, 0);
    }

    /// A taken bit splits the rescue into the narrower population but
    /// must not remove it from the sole-waiter count, which is what
    /// the stress assertion reads.
    #[test]
    fn a_rescue_with_a_taken_bit_still_counts_as_a_sole_waiters() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::InFlight, 0);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.rescues, 1);
        assert_eq!(
            stats.sole_waiter_rescues, 1,
            "a stolen wake bit reads exactly like a late one: {stats:?}"
        );
        assert_eq!(stats.bit_taken_rescues, 1);
        assert_eq!(stats.untouched_sole_waiter_rescues(), 0);
        assert!(stats.saw_unexplained_rescue(), "{stats:?}");
    }

    /// The other population: nobody had touched this waiter's bit at
    /// all, and it still had work waiting.
    #[test]
    fn a_rescue_with_an_untouched_bit_is_the_narrower_population() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Untouched, 0);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.sole_waiter_rescues, 1);
        assert_eq!(stats.bit_taken_rescues, 0);
        assert_eq!(
            stats.untouched_sole_waiter_rescues(),
            1,
            "no peer had begun to wake this waiter: {stats:?}"
        );
    }

    /// A slotless waiter can never be found by a peer, so its timeouts
    /// are structural. Counting them as unexplained lost wakes would
    /// be a permanent false positive.
    #[test]
    fn a_slotless_waiters_rescue_is_not_evidence_of_a_lost_wake() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Slotless, 0);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.unwoken_timeouts, 1, "the sleep did end on the clock");
        assert_eq!(
            stats.slotless_rescues, 1,
            "a waiter with no bit has no wake to lose: {stats:?}"
        );
        assert_eq!(
            stats.sole_waiter_rescues, 0,
            "structural, not a defect: {stats:?}"
        );
    }

    /// The observation must survive to the rescue that follows it: a
    /// waiter can time out, loop, and only then find work.
    #[test]
    fn a_taken_bit_is_remembered_across_a_later_timeout() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::InFlight, 0);
        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Untouched, 0);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.sole_waiter_rescues, 1);
        assert_eq!(stats.bit_taken_rescues, 1, "{stats:?}");
    }

    /// The side split and the blindness split are independent axes.
    #[test]
    fn a_blind_futile_wake_is_attributed_to_its_side() {
        let cycle = Cycle::new();
        let mut producer = cycle.watch(WaiterSide::Producer);
        producer.about_to_park(0, false);
        producer.parked(WakeDelivery::Delivered, 0);
        producer.about_to_park(0, true);

        let mut consumer = cycle.watch(WaiterSide::Consumer);
        consumer.about_to_park(0, false);
        consumer.parked(WakeDelivery::Delivered, 0);
        consumer.about_to_park(0, true);

        let stats = cycle.monitor.stats();
        assert_eq!(stats.futile_wakes, 2);
        assert_eq!(stats.blind_futile_wakes, 2);
        assert_eq!(
            stats.producer_futile_wakes, 1,
            "only the producer's is the producer's: {stats:?}"
        );
        assert_eq!(
            stats.producer_blind_futile_wakes, 1,
            "the two splits are independent axes: {stats:?}"
        );
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
    pub(crate) const fn about_to_park(&mut self, _peers_parked: u32, _work_visible: bool) {}


    #[inline]
    pub(crate) const fn parked_at(
        &mut self,
        _set: &crate::common::park::WakeSet,
        _slot: crate::common::park_registry::ParkSlot,
    ) {
    }

    #[inline]
    pub(crate) const fn made_progress(&mut self) {}
}
