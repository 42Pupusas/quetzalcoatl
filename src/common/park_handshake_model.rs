//! Exhaustive-schedule model of the blocking park/wake handshake.
//!
//! The mpmc rings bound every parked sleep with
//! [`PARK_BACKSTOP`](crate::mpmc::PARK_BACKSTOP) because a missed wake
//! was observed under stress and never explained. A timeout turns that
//! defect into a millisecond of latency, which is exactly what makes it
//! so hard to find by running the code: the bug repairs itself.
//!
//! Stress testing samples schedules. At roughly one occurrence per four
//! hundred saturated runs, a sampling search is a poor instrument. Loom
//! enumerates instead, and models the handshake with **no timeout at
//! all**, so a lost wake is a deadlock the model reports rather than a
//! stall it sleeps through.
//!
//! # What is modeled
//!
//! The protocol, not the ring. A waiter arms its bit and handle, fences,
//! re-checks a one-word stand-in for "work available", and parks. A peer
//! publishes work and calls [`WakeSet::wake_one`]. The ordering claim
//! under test is the one written on [`crate::common::park`]: either the
//! waiter's re-check sees the publication, or the peer sees the bit and
//! unparks. Never neither.
//!
//! `park` here is loom's mock, so a schedule that parks a waiter nobody
//! unparks ends the model run as a deadlock and names the interleaving.
//!
//! # Status: the deadlock these models report is NOT yet attributed
//!
//! Both handshake models below currently deadlock under loom. That is
//! not yet a verdict on the ring, for two reasons, and neither has been
//! ruled out:
//!
//! 1. **Loom treats `SeqCst` loads and stores as `AcqRel`** (its README
//!    lists this as unsupported; `fence(SeqCst)` *is* supported). This
//!    handshake is a store-buffering pattern whose correctness rests on
//!    exactly the `SeqCst` guarantee loom weakens, so a false alarm is
//!    the leading hypothesis. `WakeSet::wake_one`'s bitmap load is a
//!    `SeqCst` load, and the waiter's `arm` is a `SeqCst` `fetch_or`.
//! 2. **The model may be wrong.** An earlier calibration deadlocked
//!    because it parked unconditionally after arming, with no re-check
//!    — a bug in the test, not the code.
//!
//! [`loom_itself_holds_an_unpark_token_until_the_park`] rules out the
//! third possibility, that loom's mocked park drops a token; it passes.
//!
//! Next step is to distinguish (1) from a real defect: rewrite the
//! arm/re-check pair so its ordering rests on `fence(SeqCst)` rather
//! than on `SeqCst` accesses, and see whether the deadlock survives. If
//! it does, the ring has a genuine lost-wake schedule and loom has
//! named it. Until then these models are marked `ignore` so they do not
//! report a failure the crate has not earned.

use super::atomics::{thread, AtomicU64};
use super::park::WakeSet;
use super::park_registry::ParkSlot;
use loom::sync::Arc;
use std::sync::atomic::Ordering;

/// A one-word stand-in for the ring's "work is available" signal.
///
/// The rings publish through `ready`/`done` slot words; every property
/// this model checks depends only on the publication being a single
/// release-ordered write the waiter re-reads, so a counter carries the
/// protocol without the slot machinery's state space.
struct Work {
    available: AtomicU64,
}

impl Work {
    fn new() -> Self {
        Self {
            available: AtomicU64::new(0),
        }
    }

    /// Publishes one unit of work, then wakes a parked waiter exactly
    /// as a producer does after `ready.store(Release)`.
    fn publish(&self, wakers: &WakeSet) {
        self.available.store(1, Ordering::Release);
        wakers.wake_one();
    }

    /// Takes the published work, if any.
    fn take(&self) -> bool {
        self.available.swap(0, Ordering::Acquire) != 0
    }
}

/// One waiter's blocking loop, in the shape the rings use.
///
/// Arm, fence, re-check, park — and on waking, disarm and re-check.
/// The fence between the arm and the re-check is the ordering half of
/// the handshake; without it the re-check may be reordered ahead of the
/// `SeqCst` `fetch_or` and the waiter parks on stale state.
struct Waiter<'a> {
    wakers: &'a WakeSet,
    work: &'a Work,
    slot: ParkSlot,
}

impl<'a> Waiter<'a> {
    const fn new(wakers: &'a WakeSet, work: &'a Work, slot: ParkSlot) -> Self {
        Self { wakers, work, slot }
    }

    /// Blocks until work arrives. Returns once it has taken a unit.
    ///
    /// No timeout: the only way out is a peer's wake or a re-check that
    /// sees the publication. That is the point of the model.
    fn wait(&self) {
        loop {
            if self.work.take() {
                return;
            }
            self.wakers.arm(self.slot);
            super::atomics::fence(Ordering::SeqCst);
            if self.work.take() {
                self.wakers.disarm(self.slot);
                return;
            }
            thread::park();
            self.wakers.disarm(self.slot);
        }
    }
}

/// Calibration with nothing of ours in it: loom's own `park` and
/// `unpark`, no `WakeSet`, no `ThreadParker`. If this deadlocks,
/// loom's mock does not hold an unpark token across a later `park`,
/// and every park-based model in this file is measuring loom rather
/// than the ring.
///
/// It passes, so a deadlock reported below is about this crate's code.
/// (An earlier calibration attempt parked *unconditionally* after
/// arming, with no re-check of the work; that deadlocks by
/// construction, because a wake issued before the arm has no bit to
/// find. The re-check is not incidental to the protocol — it is half
/// of it.)
#[test]
fn loom_itself_holds_an_unpark_token_until_the_park() {
    loom::model(|| {
        let flag = Arc::new(AtomicU64::new(0));
        let peer = Arc::clone(&flag);

        let parked = thread::spawn(move || {
            peer.store(1, Ordering::SeqCst);
            thread::park();
        });

        // Spin until the child has committed to parking, then unpark.
        // The store may still be observed before the park executes, so
        // loom explores the token-arrives-first ordering too.
        while flag.load(Ordering::SeqCst) == 0 {
            loom::thread::yield_now();
        }
        parked.thread().unpark();
        parked.join().unwrap();
    });
}

/// The core claim, with one waiter and one publisher: a waiter can
/// never be left parked on work that has already been published.
///
/// A schedule that loses the wake deadlocks — the waiter parks and no
/// unpark is ever delivered — and loom reports it with the
/// interleaving, rather than the 1ms stall the real backstop produces.
///
/// Currently deadlocks; see the module docs on why that is not yet
/// attributable to the ring.
#[test]
#[ignore = "deadlocks under loom; not yet distinguished from loom's SeqCst-as-AcqRel false alarms"]
fn a_published_unit_of_work_always_reaches_a_parked_waiter() {
    loom::model(|| {
        let wakers = Arc::new(WakeSet::new());
        let work = Arc::new(Work::new());

        let publisher = {
            let wakers = Arc::clone(&wakers);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                work.publish(&wakers);
            })
        };

        Waiter::new(&wakers, &work, ParkSlot::Leased(0)).wait();
        publisher.join().unwrap();
    });
}

/// Two waiters on distinct slots against two publications.
///
/// This is the shape the round-robin cursor exists for, and the shape
/// the stress failure had: every waiter must be served, and a wake must
/// not be delivered twice to one slot while the other stays parked.
#[test]
#[ignore = "deadlocks under loom; not yet distinguished from loom's SeqCst-as-AcqRel false alarms"]
fn two_publications_reach_two_waiters_on_distinct_slots() {
    loom::model(|| {
        let wakers = Arc::new(WakeSet::new());
        let work = Arc::new(Work::new());

        let publisher = {
            let wakers = Arc::clone(&wakers);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                work.publish(&wakers);
            })
        };
        let second = {
            let wakers = Arc::clone(&wakers);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                work.publish(&wakers);
            })
        };
        let waiter = {
            let wakers = Arc::clone(&wakers);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                Waiter::new(&wakers, &work, ParkSlot::Leased(1)).wait();
            })
        };

        Waiter::new(&wakers, &work, ParkSlot::Leased(0)).wait();
        waiter.join().unwrap();
        publisher.join().unwrap();
        second.join().unwrap();
    });
}
