//! Instrumentation for the mpmc park path.
//!
//! Until the lost-wake defect in `WakeSet::wake_one` was found, every
//! mpmc park was bounded by a 1 ms `PARK_BACKSTOP`, and these counters
//! were built to decide whether it could go. A timeout that never
//! rescued anyone cost nothing and was evidence the bound was
//! unnecessary; one that did was a reproduction of the bug it guarded
//! against. Silently, the backstop could not tell those apart — a lost
//! wake became a millisecond of latency and nothing else.
//!
//! The bound is gone, and so is the slotless waiter's re-check
//! interval: every blocking park in the ring is now untimed. The
//! counters stay, because they are the only instrument that can see a
//! lost wake short of a hang, and the ignored stress harness prints
//! them. What changes without any timeout is *when* a park can return
//! unwoken: only on a spurious unpark. A rescue is therefore no longer
//! common enough to need the round-robin and timing splits to
//! interpret — but the splits still apply, and a non-zero
//! `waiting_sole_waiter_rescues` remains the one reading that only a
//! lost wake can produce.
//!
//! # What the counts mean
//!
//! A waiter's park handle is claimed by whoever wakes it, so a handle
//! still armed after the park returns proves no peer delivered a wake.
//! That is an *unwoken timeout* (the name predates the bound's
//! removal).
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
use super::rescue_evidence::{RescueEvidence, RescueExplanation, RescueTiming};
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
    /// Sole-waiter rescues suffered by producers specifically.
    ///
    /// The two sides park on different conditions and are woken by
    /// different releases, so a lost wake on one says nothing about
    /// the other. A consumer's rescue is a published item it failed
    /// to find; a producer's is a freed position it was not told
    /// about. Attributing the residual to a side is what makes a
    /// candidate mechanism testable.
    pub producer_sole_waiter_rescues: u64,
    /// Sole-waiter rescues where work was already visible to the
    /// waiter when it parked.
    ///
    /// These are not lost wakes. The publish preceded the arm, so no
    /// wake was owed; the waiter's own pre-park re-check is what
    /// failed to find the item, and the timeout is what finally did.
    /// From the waiter's side the two are identical — untouched bit,
    /// work waiting — which is why the pre-park sample is needed to
    /// separate them.
    ///
    /// The complement,
    /// [`unexplained_sole_waiter_rescues`](Self::unexplained_sole_waiter_rescues),
    /// is the residual that a lost wake would have to be found in.
    pub blind_sole_waiter_rescues: u64,
    /// Sole-waiter rescues where work was *not* visible the instant
    /// the park returned.
    ///
    /// The timeout did not rescue these; the work arrived in the gap
    /// between the sleep ending and the next attempt, and the rescue
    /// count attributes it to the timeout only because that is what
    /// preceded it. Not a lost wake: nothing was there to be woken
    /// for while the waiter slept.
    pub late_sole_waiter_rescues: u64,
    /// Sole-waiter rescues where work was absent before the sleep
    /// and present the instant it ended.
    ///
    /// The item was published while the waiter slept, and the waiter
    /// was not woken for it. With an untouched bit and no peer to
    /// have absorbed the wake, this is the signature of a wake that
    /// went missing, and the only timing a delivery defect can
    /// produce. This is the count to explain.
    pub waiting_sole_waiter_rescues: u64,
    /// Parks that returned on the clock looking like the residual —
    /// untouched bit, work waiting, nobody else parked — whose handle
    /// a waker then claimed within [`LATE_WAKE_GRACE`].
    ///
    /// The wake was coming; the clock beat it. The publisher was
    /// between publishing and waking when the timeout fired, which
    /// is a window of a few hundred instructions unless the
    /// publisher is descheduled inside it. These are re-sampled
    /// after the grace and counted as woken, so they leave the
    /// rescue counts. Reported so the size of the window is known.
    pub wakes_arriving_after_timeout: u64,
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
impl std::ops::AddAssign for BackstopStats {
    fn add_assign(&mut self, other: Self) {
        self.unwoken_timeouts += other.unwoken_timeouts;
        self.rescues += other.rescues;
        self.sole_waiter_rescues += other.sole_waiter_rescues;
        self.producer_sole_waiter_rescues += other.producer_sole_waiter_rescues;
        self.blind_sole_waiter_rescues += other.blind_sole_waiter_rescues;
        self.late_sole_waiter_rescues += other.late_sole_waiter_rescues;
        self.waiting_sole_waiter_rescues += other.waiting_sole_waiter_rescues;
        self.wakes_arriving_after_timeout += other.wakes_arriving_after_timeout;
        self.bit_taken_rescues += other.bit_taken_rescues;
        self.slotless_rescues += other.slotless_rescues;
        self.blind_futile_wakes += other.blind_futile_wakes;
        self.producer_blind_futile_wakes += other.producer_blind_futile_wakes;
        self.producer_futile_wakes += other.producer_futile_wakes;
        self.futile_wakes += other.futile_wakes;
    }
}

#[cfg(feature = "backstop-metrics")]
impl BackstopStats {
    /// The all-zero reading, for accumulating across rings.
    pub const ZERO: Self = Self {
        unwoken_timeouts: 0,
        rescues: 0,
        sole_waiter_rescues: 0,
        producer_sole_waiter_rescues: 0,
        blind_sole_waiter_rescues: 0,
        late_sole_waiter_rescues: 0,
        waiting_sole_waiter_rescues: 0,
        wakes_arriving_after_timeout: 0,
        bit_taken_rescues: 0,
        slotless_rescues: 0,
        blind_futile_wakes: 0,
        producer_blind_futile_wakes: 0,
        producer_futile_wakes: 0,
        futile_wakes: 0,
    };

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

    /// Sole-waiter rescues suffered by consumers.
    #[must_use]
    pub const fn consumer_sole_waiter_rescues(self) -> u64 {
        self.sole_waiter_rescues
            .saturating_sub(self.producer_sole_waiter_rescues)
    }

    /// The residual a lost wake would have to be found in: sole-waiter
    /// rescues that were neither blind nor late.
    ///
    /// Both subtractions are sound where the bit-taken one was not.
    /// Blindness is diagnosed from the waiter's *own* failed re-check,
    /// before any peer is involved; lateness from the work being
    /// absent when the sleep ended. Neither can be confused with a
    /// peer's wake going missing, because in neither was a wake owed
    /// during the sleep.
    #[must_use]
    pub const fn unexplained_sole_waiter_rescues(self) -> u64 {
        self.waiting_sole_waiter_rescues
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
    producer_sole_waiter_rescues: AtomicU64,
    blind_sole_waiter_rescues: AtomicU64,
    late_sole_waiter_rescues: AtomicU64,
    waiting_sole_waiter_rescues: AtomicU64,
    wakes_arriving_after_timeout: AtomicU64,
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
            producer_sole_waiter_rescues: AtomicU64::new(0),
            blind_sole_waiter_rescues: AtomicU64::new(0),
            late_sole_waiter_rescues: AtomicU64::new(0),
            waiting_sole_waiter_rescues: AtomicU64::new(0),
            wakes_arriving_after_timeout: AtomicU64::new(0),
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

    fn record_late_wake(&self) {
        self.wakes_arriving_after_timeout
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_rescue(&self, side: WaiterSide, evidence: RescueEvidence) {
        self.rescues.fetch_add(1, Ordering::Relaxed);
        if matches!(evidence.explanation(), RescueExplanation::Slotless) {
            self.slotless_rescues.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !evidence.is_sole_waiter() {
            return;
        }
        self.sole_waiter_rescues.fetch_add(1, Ordering::Relaxed);
        if matches!(side, WaiterSide::Producer) {
            self.producer_sole_waiter_rescues
                .fetch_add(1, Ordering::Relaxed);
        }
        let timing_counter = match evidence.timing() {
            RescueTiming::Blind => &self.blind_sole_waiter_rescues,
            RescueTiming::Late => &self.late_sole_waiter_rescues,
            RescueTiming::Waiting => &self.waiting_sole_waiter_rescues,
        };
        timing_counter.fetch_add(1, Ordering::Relaxed);
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
            producer_sole_waiter_rescues: self
                .producer_sole_waiter_rescues
                .load(Ordering::Relaxed),
            blind_sole_waiter_rescues: self
                .blind_sole_waiter_rescues
                .load(Ordering::Relaxed),
            late_sole_waiter_rescues: self
                .late_sole_waiter_rescues
                .load(Ordering::Relaxed),
            waiting_sole_waiter_rescues: self
                .waiting_sole_waiter_rescues
                .load(Ordering::Relaxed),
            wakes_arriving_after_timeout: self
                .wakes_arriving_after_timeout
                .load(Ordering::Relaxed),
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

/// How long a park that looks like the residual waits for a wake
/// that may be a few instructions behind whatever ended the park.
///
/// A publisher stores the item, then loads the wake bitmap and
/// unparks. A park returning between those two steps finds the item
/// present and the bit untouched — the residual's exact signature —
/// with the wake nanoseconds away. The grace is long enough to cover
/// a publisher descheduled inside that window on a loaded box; a
/// wake that has not arrived by then was not coming.
#[cfg(feature = "backstop-metrics")]
pub const LATE_WAKE_GRACE: std::time::Duration = std::time::Duration::from_millis(5);

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
        self.evidence.about_to_park(peers_parked, work_visible);
    }

    /// Records how the park that just returned ended, reading the
    /// waiter's state out of `set` itself.
    ///
    /// `work_visible` is consulted only when the park ended without a
    /// peer's wake: it is the return-time half of the timing bracket,
    /// and a delivered wake has no rescue to time. Keeping it a
    /// closure spares the woken path a scan of the ring.
    ///
    /// Sampling here rather than at the call site keeps
    /// [`WakeDelivery`] out of the uninstrumented build, where the
    /// no-op twin takes the same arguments and does nothing.
    pub(crate) fn parked_at(
        &mut self,
        set: &crate::common::park::WakeSet,
        slot: ParkSlot,
        work_visible: impl FnOnce() -> bool,
    ) {
        let mut delivery = WakeDelivery::sample(set, slot);
        let work = delivery.is_unwoken() && work_visible();
        let peers = set.others_parked(slot);
        if delivery == WakeDelivery::Untouched
            && work
            && peers == 0
            && !self.evidence.was_blind()
            && set.handle_claimed_within(slot, LATE_WAKE_GRACE)
        {
            self.monitor().record_late_wake();
            // The claim's unpark landed as a token on this thread,
            // which is not parked; drain it so the next park does
            // not return on it and read as a spurious timeout.
            std::thread::park_timeout(std::time::Duration::ZERO);
            delivery = WakeDelivery::sample(set, slot);
        }
        self.parked(delivery, peers, work);
    }

    /// Records how the park that just returned ended. `delivery` says
    /// whether a peer's unpark reached us, was in flight, or had not
    /// begun; `peers_parked` is how many other waiters on this side
    /// were parked, which is what round-robin could have served
    /// instead of us; `work_visible` is whether the ring held work
    /// the instant the park returned.
    fn parked(&mut self, delivery: WakeDelivery, peers_parked: u32, work_visible: bool) {
        let unwoken = delivery.is_unwoken();
        self.timed_out = unwoken;
        self.woken = !unwoken;
        self.evidence.parked(delivery, peers_parked, work_visible);
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
            self.monitor().record_rescue(self.side, evidence);
        }
    }
}

#[cfg(all(test, feature = "backstop-metrics"))]
mod tests {
    use super::{BackstopMonitor, BackstopStats, BackstopWatch, WaiterSide, WakeDelivery};
    use std::sync::atomic::Ordering;

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
        watch.parked(WakeDelivery::Delivered, 0, false);
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
        watch.parked(WakeDelivery::Delivered, 0, false);
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
        watch.parked(WakeDelivery::Untouched, 0, true);
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
        watch.parked(WakeDelivery::Delivered, 0, false);
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
        watch.parked(WakeDelivery::InFlight, 0, true);
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
        watch.parked(WakeDelivery::Untouched, 0, true);
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
        watch.parked(WakeDelivery::Slotless, 0, true);
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
        watch.parked(WakeDelivery::InFlight, 0, true);
        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Untouched, 0, true);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.sole_waiter_rescues, 1);
        assert_eq!(stats.bit_taken_rescues, 1, "{stats:?}");
    }

    /// A waiter that parks with work already visible was not owed a
    /// wake, so its rescue is explained without one going missing.
    /// It stays in the raw sole-waiter count and leaves the
    /// unexplained residual.
    #[test]
    fn a_sole_waiter_that_parked_blind_is_an_explained_rescue() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, true);
        watch.parked(WakeDelivery::Untouched, 0, true);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.sole_waiter_rescues, 1);
        assert_eq!(stats.blind_sole_waiter_rescues, 1, "{stats:?}");
        assert_eq!(
            stats.unexplained_sole_waiter_rescues(),
            0,
            "the item was there before the park; no wake was owed: {stats:?}"
        );
    }

    /// Nothing before the sleep, work the instant it ended, nobody
    /// else parked, bit untouched: every benign explanation is ruled
    /// out, and this is the residual.
    #[test]
    fn a_sole_waiter_whose_work_arrived_during_the_sleep_is_unexplained() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Untouched, 0, true);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.blind_sole_waiter_rescues, 0);
        assert_eq!(stats.late_sole_waiter_rescues, 0);
        assert_eq!(stats.waiting_sole_waiter_rescues, 1, "{stats:?}");
        assert_eq!(stats.unexplained_sole_waiter_rescues(), 1, "{stats:?}");
    }

    /// Work absent when the park returned: the timeout preceded the
    /// work rather than rescuing the waiter from it. Still counted as
    /// a rescue and a sole-waiter one, but explained.
    #[test]
    fn a_sole_waiter_whose_work_arrived_after_the_park_is_explained() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);

        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Untouched, 0, false);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.sole_waiter_rescues, 1);
        assert_eq!(stats.late_sole_waiter_rescues, 1, "{stats:?}");
        assert_eq!(
            stats.unexplained_sole_waiter_rescues(),
            0,
            "nothing was there while the waiter slept: {stats:?}"
        );
    }

    /// The return-time sample is only taken when the park ended on
    /// the clock: a delivered wake has no rescue to time, and the
    /// closure is the cost of a ring scan.
    #[test]
    fn a_delivered_wake_does_not_sample_the_ring() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);
        let set = crate::common::park::WakeSet::new();
        let slot = crate::common::park_registry::ParkSlot::Leased(0);
        set.arm(slot);
        set.wake_one();

        watch.about_to_park(0, false);
        watch.parked_at(&set, slot, || panic!("a woken waiter must not pay for the sample"));
    }

    /// One attempt at the late-wake bracket.
    ///
    /// The wake has to land after the park is sampled and before the
    /// grace expires, and both ends belong to the scheduler: fire too
    /// early and the park reads as woken outright, too late and it
    /// reads as the residual. An attempt therefore reports what it
    /// produced instead of asserting, and the test retries until the
    /// bracket is hit.
    struct LateWakeEpisode;

    impl LateWakeEpisode {
        /// Biases the wake past the sample at the top of `parked_at`
        /// without reaching the 5ms grace. Missing either edge costs
        /// an attempt, not the run.
        const NUDGE: std::time::Duration = std::time::Duration::from_micros(200);

        fn run() -> BackstopStats {
            let cycle = Cycle::new();
            let mut watch = cycle.watch(WaiterSide::Consumer);
            let set = std::sync::Arc::new(crate::common::park::WakeSet::new());
            let slot = crate::common::park_registry::ParkSlot::Leased(0);
            set.arm(slot);

            let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let late_waker = {
                let set = std::sync::Arc::clone(&set);
                let release = std::sync::Arc::clone(&release);
                std::thread::spawn(move || {
                    while !release.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    std::thread::sleep(Self::NUDGE);
                    set.wake_one();
                })
            };

            watch.about_to_park(0, false);
            release.store(true, Ordering::Release);
            watch.parked_at(&set, slot, || true);
            watch.made_progress();
            late_waker.join().unwrap();
            cycle.monitor.stats()
        }
    }

    /// A publisher caught between publish and wake by the timeout
    /// delivers its wake a moment later. The grace catches it, and
    /// the park is recorded as woken rather than as the residual.
    #[test]
    fn a_wake_arriving_within_the_grace_is_not_a_rescue() {
        const ATTEMPTS: u32 = 64;

        let mut last = None;
        for _ in 0..ATTEMPTS {
            let stats = LateWakeEpisode::run();
            if stats.wakes_arriving_after_timeout != 1 {
                last = Some(stats);
                continue;
            }
            assert_eq!(
                stats.waiting_sole_waiter_rescues, 0,
                "the wake was in flight; the clock merely beat it: {stats:?}"
            );
            assert_eq!(stats.rescues, 0, "{stats:?}");
            return;
        }
        panic!("the wake never landed inside the grace in {ATTEMPTS} attempts: {last:?}");
    }

    /// The grace must not invent a wake: with nobody coming, the park
    /// stays a residual rescue.
    #[test]
    fn a_wake_that_never_arrives_is_still_the_residual() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);
        let set = crate::common::park::WakeSet::new();
        let slot = crate::common::park_registry::ParkSlot::Leased(0);
        set.arm(slot);

        watch.about_to_park(0, false);
        watch.parked_at(&set, slot, || true);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.wakes_arriving_after_timeout, 0, "{stats:?}");
        assert_eq!(stats.waiting_sole_waiter_rescues, 1, "{stats:?}");
    }

    #[test]
    fn an_unwoken_timeout_samples_the_ring_at_return() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Consumer);
        let set = crate::common::park::WakeSet::new();
        let slot = crate::common::park_registry::ParkSlot::Leased(0);
        set.arm(slot);

        watch.about_to_park(0, false);
        watch.parked_at(&set, slot, || true);
        watch.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.waiting_sole_waiter_rescues, 1, "{stats:?}");
    }

    #[test]
    fn readings_sum_field_by_field() {
        let cycle = Cycle::new();
        let mut watch = cycle.watch(WaiterSide::Producer);
        watch.about_to_park(0, false);
        watch.parked(WakeDelivery::Untouched, 0, true);
        watch.made_progress();
        let one = cycle.monitor.stats();

        let mut total = super::BackstopStats::ZERO;
        total += one;
        total += one;

        assert_eq!(total.rescues, 2);
        assert_eq!(total.sole_waiter_rescues, 2);
        assert_eq!(total.producer_sole_waiter_rescues, 2);
        assert_eq!(total.unwoken_timeouts, 2);
    }

    #[test]
    fn a_sole_waiter_rescue_is_attributed_to_its_side() {
        let cycle = Cycle::new();
        let mut producer = cycle.watch(WaiterSide::Producer);
        producer.about_to_park(0, false);
        producer.parked(WakeDelivery::Untouched, 0, true);
        producer.made_progress();

        let mut consumer = cycle.watch(WaiterSide::Consumer);
        consumer.about_to_park(0, false);
        consumer.parked(WakeDelivery::Untouched, 0, true);
        consumer.made_progress();

        let stats = cycle.monitor.stats();
        assert_eq!(stats.sole_waiter_rescues, 2);
        assert_eq!(stats.producer_sole_waiter_rescues, 1, "{stats:?}");
        assert_eq!(stats.consumer_sole_waiter_rescues(), 1, "{stats:?}");
    }

    /// The side split and the blindness split are independent axes.
    #[test]
    fn a_blind_futile_wake_is_attributed_to_its_side() {
        let cycle = Cycle::new();
        let mut producer = cycle.watch(WaiterSide::Producer);
        producer.about_to_park(0, false);
        producer.parked(WakeDelivery::Delivered, 0, false);
        producer.about_to_park(0, true);

        let mut consumer = cycle.watch(WaiterSide::Consumer);
        consumer.about_to_park(0, false);
        consumer.parked(WakeDelivery::Delivered, 0, false);
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
    pub(crate) fn parked_at(
        &mut self,
        _set: &crate::common::park::WakeSet,
        _slot: crate::common::park_registry::ParkSlot,
        _work_visible: impl FnOnce() -> bool,
    ) {
    }

    #[inline]
    pub(crate) const fn made_progress(&mut self) {}
}
