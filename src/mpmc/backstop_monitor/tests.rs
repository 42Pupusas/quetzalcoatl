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
    watch.parked_at(&set, slot, || {
        panic!("a woken waiter must not pay for the sample")
    });
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
