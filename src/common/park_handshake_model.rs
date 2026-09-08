//! Exhaustive-schedule model of the blocking park/wake handshake.
//!
//! The mpmc rings used to bound every parked sleep with a 1 ms
//! `PARK_BACKSTOP` because a missed wake was observed under stress and
//! never explained. A timeout turns that defect into a millisecond of
//! latency, which is exactly what makes it so hard to find by running
//! the code: the bug repairs itself.
//!
//! Stress testing samples schedules. At roughly one occurrence per four
//! hundred saturated runs, a sampling search is a poor instrument. Loom
//! enumerates instead, and models the handshake with no timeout, so a
//! lost wake is a deadlock the model reports rather than a stall it
//! sleeps through.
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
//! # Status: loom 0.7.2 cannot decide this protocol
//!
//! **The deadlocks these models report are artifacts of loom, not
//! defects in the ring.** Every handshake model here is marked `ignore`
//! for that reason. This is a negative result, and the calibrations
//! below are the evidence for it; they are kept running so the claim
//! stays checkable rather than remembered.
//!
//! The finding is
//! [`a_handshake_built_only_from_loom_primitives_cannot_miss_a_wake`]:
//! a park/unpark handshake containing **no code from this crate** —
//! loom atomics, a loom `Thread` handle, loom's `park` and `unpark` —
//! deadlocks under loom. Its doc comment traces every schedule the
//! model admits; none leaves the waiter parked. Loom reports a state
//! its own memory model forbids.
//!
//! The bisection that got there, each step passing unless noted:
//!
//! | Calibration | Result |
//! |---|---|
//! | loom's own park/unpark, token before park | passes |
//! | unpark via a handle from `thread::current()` | passes |
//! | fenced store-buffering, no park | passes |
//! | unpark from a spawned thread to main | passes |
//! | [`ThreadParker`](super::thread_parker::ThreadParker) alone | passes |
//! | wake straight to `parkers[0]`, bitmap bypassed | passes |
//! | re-check alone, publisher never wakes | passes |
//! | `WakeSet` handshake, waiter on main | **deadlocks** |
//! | `WakeSet` handshake, waiter spawned | **deadlocks** |
//! | single arm/re-check/park, no loop | **deadlocks** |
//! | hand-rolled bitmap, no `WakeSet` | **deadlocks** |
//! | loom primitives only, no crate code | **deadlocks** |
//!
//! Note the third row: `fence(SeqCst)` store-buffering *is* modeled
//! correctly on its own. So the earlier hypothesis — that loom's
//! documented weakening of `SeqCst` accesses to `AcqRel` explained the
//! failures — is wrong. The fences are honored; what breaks is the
//! combination of a fenced handshake with the mocked park.
//!
//! The likely mechanism is loom's unpark-token bookkeeping. Loom has
//! had bugs of exactly this shape before (tokio-rs/loom#246, "incorrect
//! semantics for `Thread::unpark` followed by `thread::park`", fixed;
//! and the open #422, where `Condvar` and `thread::park` corrupt each
//! other's tokens). The precise mechanism here is not yet pinned down,
//! and pinning it down means reducing this to a report against loom.
//!
//! What this does **not** show: nothing here vindicates the ring. The
//! missed wake was found by other means — `WakeSet::wake_one` consumed
//! wakes on bits whose handles were already claimed — and the bound
//! was removed once the `backstop-metrics` campaign showed no park
//! left asleep on published work. Loom simply cannot be the
//! instrument. The park path stays wired through the shim so these
//! models can be re-run against a later loom.

use super::atomics::{thread, AtomicU64};
use super::park::WakeSet;
use super::park_registry::ParkSlot;
use super::thread_parker::ThreadParker;
use loom::sync::{Arc, Mutex};
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

    /// Publishes without waking, for the model that asks whether the
    /// re-check alone closes the handshake.
    fn publish_silently(&self) {
        self.available.store(1, Ordering::Release);
    }

    /// Publishes and wakes every parked waiter, bypassing the bitmap
    /// selection and the handle-claiming in
    /// [`WakeSet::wake_one`]. Bisects the wake half.
    fn publish_and_flush(&self, wakers: &WakeSet) {
        self.available.store(1, Ordering::Release);
        super::atomics::fence(Ordering::SeqCst);
        wakers.flush();
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

/// Second calibration, and the one that matters for this crate.
///
/// The first calibration unparks through `JoinHandle::thread()`, which
/// loom hands out itself. [`ThreadParker`](super::thread_parker::ThreadParker)
/// does something different: the waiter captures its *own*
/// `thread::current()`, publishes it, and a peer unparks through that
/// carried handle. If loom's `thread::current()` does not name a
/// parkable thread when used from elsewhere, every model below reports
/// a deadlock that the real code does not have.
#[test]
fn loom_unparks_through_a_handle_captured_by_thread_current() {
    loom::model(|| {
        let handoff = Arc::new(Mutex::new(None));
        let armed = Arc::new(AtomicU64::new(0));
        let peer_handoff = Arc::clone(&handoff);
        let peer_armed = Arc::clone(&armed);

        let parked = thread::spawn(move || {
            *peer_handoff.lock().unwrap() = Some(thread::current());
            peer_armed.store(1, Ordering::SeqCst);
            thread::park();
        });

        while armed.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
        handoff.lock().unwrap().take().unwrap().unpark();
        parked.join().unwrap();
    });
}

/// Third calibration, and the decisive one: does loom model **fenced**
/// store-buffering soundly?
///
/// This is the shape the whole handshake rests on, with no crate code
/// in it. Each thread stores to its own word, executes a `SeqCst`
/// fence, then loads the other's. Under the real memory model the two
/// fences forbid both loads returning zero, and that guarantee is the
/// only reason a waiter cannot park on work already published.
///
/// Loom's README lists `SeqCst` *accesses* as modeled like `AcqRel`
/// (false alarms) while `fence(SeqCst)` is supported — so this must
/// pass. If it fails, loom cannot decide this protocol at all and every
/// deadlock below is an artifact of the tool.
#[test]
fn loom_forbids_both_sides_of_a_fenced_store_buffer_reading_stale() {
    loom::model(|| {
        let x = Arc::new(AtomicU64::new(0));
        let y = Arc::new(AtomicU64::new(0));
        let (px, py) = (Arc::clone(&x), Arc::clone(&y));

        let peer = thread::spawn(move || {
            px.store(1, Ordering::Relaxed);
            super::atomics::fence(Ordering::SeqCst);
            py.load(Ordering::Relaxed)
        });

        y.store(1, Ordering::Relaxed);
        super::atomics::fence(Ordering::SeqCst);
        let saw_x = x.load(Ordering::Relaxed);
        let saw_y = peer.join().unwrap();

        assert!(
            saw_x != 0 || saw_y != 0,
            "both sides read stale across SeqCst fences: \
             the store-buffering guarantee the park handshake rests on"
        );
    });
}

/// Fourth calibration: [`ThreadParker`](super::thread_parker::ThreadParker)
/// on its own, with no [`WakeSet`], no bitmap, and no `AlignedBuf`.
///
/// Reached by bisection: `flush` — which swaps the whole bitmap and
/// unparks every slot it finds — deadlocks just as `wake_one` does.
/// For one waiter and one publisher no interleaving can lose a flushed
/// wake, so the fault is more likely below the bitmap than in it.
#[test]
fn a_thread_parker_wake_reaches_a_waiter_that_armed_it() {
    loom::model(|| {
        let parker = Arc::new(ThreadParker::new());
        let armed = Arc::new(AtomicU64::new(0));
        let peer_parker = Arc::clone(&parker);
        let peer_armed = Arc::clone(&armed);

        let waiter = thread::spawn(move || {
            peer_parker.arm();
            peer_armed.store(1, Ordering::SeqCst);
            thread::park();
        });

        while armed.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
        parker.wake();
        waiter.join().unwrap();
    });
}

/// Fifth calibration, and the one the bisection converged on: can a
/// **spawned** thread's unpark reach the **main** thread?
///
/// Every calibration that passes parks a spawned thread and unparks it
/// from main. Every model that deadlocks parks main and unparks it from
/// a spawned thread. That direction is the last uncontrolled difference,
/// and it is a property of loom, not of this crate.
#[test]
fn loom_delivers_an_unpark_from_a_spawned_thread_to_the_main_thread() {
    loom::model(|| {
        let handoff = Arc::new(Mutex::new(None));
        let armed = Arc::new(AtomicU64::new(0));
        let peer_handoff = Arc::clone(&handoff);
        let peer_armed = Arc::clone(&armed);

        *handoff.lock().unwrap() = Some(thread::current());
        armed.store(1, Ordering::SeqCst);

        let waker = thread::spawn(move || {
            while peer_armed.load(Ordering::SeqCst) == 0 {
                thread::yield_now();
            }
            peer_handoff.lock().unwrap().take().unwrap().unpark();
        });

        thread::park();
        waker.join().unwrap();
    });
}

/// Sixth calibration: the full [`WakeSet`] handshake, but with the
/// waiter on a **spawned** thread rather than main.
///
/// Isolates the last two candidates from each other. The parker alone
/// passes and the unpark direction passes, so what remains is
/// `WakeSet` itself — its bitmap and its 64-entry `AlignedBuf` of
/// parkers. If this passes, the deadlock needs the main thread to be
/// the waiter and is a harness artifact; if it deadlocks, the fault is
/// in `WakeSet` and is the crate's.
#[test]
#[ignore = "loom 0.7.2 reports a false deadlock here; see the module docs"]
fn a_wake_set_handshake_completes_with_the_waiter_on_a_spawned_thread() {
    loom::model(|| {
        let wakers = Arc::new(WakeSet::new());
        let work = Arc::new(Work::new());

        let waiter = {
            let wakers = Arc::clone(&wakers);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                Waiter::new(&wakers, &work, ParkSlot::Leased(0)).wait();
            })
        };

        work.publish(&wakers);
        waiter.join().unwrap();
    });
}

/// Seventh calibration: the waiter arms through [`WakeSet`], but the
/// wake goes **directly** to `parkers[0]`, bypassing the bitmap.
///
/// This splits the last two suspects. A bare
/// [`ThreadParker`](super::thread_parker::ThreadParker) passes and a
/// full `WakeSet` deadlocks; what differs is the bitmap *and* the
/// 64-entry `AlignedBuf` the parkers live in. If this passes, loom
/// tracks parkers inside the raw allocation fine and the fault is the
/// bitmap logic. If it deadlocks, loom cannot see the atomics through
/// `AlignedBuf`'s hand-rolled allocation, and every `WakeSet` model
/// here is measuring the harness.
#[test]
fn a_wake_delivered_straight_to_the_slots_parker_reaches_the_waiter() {
    loom::model(|| {
        let wakers = Arc::new(WakeSet::new());
        let armed = Arc::new(AtomicU64::new(0));
        let peer_wakers = Arc::clone(&wakers);
        let peer_armed = Arc::clone(&armed);

        let waiter = thread::spawn(move || {
            peer_wakers.arm(ParkSlot::Leased(0));
            peer_armed.store(1, Ordering::SeqCst);
            thread::park();
        });

        while armed.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
        wakers.parkers[0].wake();
        waiter.join().unwrap();
    });
}

/// Eighth calibration: the full arm/fence/re-check/park sequence, but
/// parking exactly **once** — no loop, no `disarm`.
///
/// The direct-parker wake passes and the bitmap wake deadlocks, but
/// those two models also differ in the waiter: the passing one parks
/// once, while [`Waiter::wait`] loops and disarms. This isolates that
/// difference. If it passes, the single handshake is sound and the
/// defect involves the loop or the `Relaxed` `disarm`; if it
/// deadlocks, one arm/publish/flush pair is already enough to lose a
/// wake.
#[test]
#[ignore = "loom 0.7.2 reports a false deadlock here; see the module docs"]
fn a_single_arm_recheck_park_sequence_cannot_miss_a_flushed_wake() {
    loom::model(|| {
        let wakers = Arc::new(WakeSet::new());
        let work = Arc::new(Work::new());

        let publisher = {
            let wakers = Arc::clone(&wakers);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                work.publish_and_flush(&wakers);
            })
        };

        let slot = ParkSlot::Leased(0);
        if !work.take() {
            wakers.arm(slot);
            super::atomics::fence(Ordering::SeqCst);
            if !work.take() {
                thread::park();
            }
        }
        publisher.join().unwrap();
    });
}

/// Ninth calibration: the deadlocking shape rebuilt from scratch — a
/// bare `AtomicU64` bitmap and one `ThreadParker`, no [`WakeSet`], no
/// `AlignedBuf`.
///
/// Byte for byte the same protocol as
/// [`a_single_arm_recheck_park_sequence_cannot_miss_a_flushed_wake`],
/// which deadlocks. If this passes, the fault is specific to
/// `WakeSet`; if it deadlocks too, loom is failing on a fenced
/// store-buffering pattern that calibration #3 says it handles, and
/// the tool is the problem.
#[test]
#[ignore = "loom 0.7.2 reports a false deadlock here; see the module docs"]
fn a_hand_rolled_bitmap_handshake_cannot_miss_a_wake() {
    loom::model(|| {
        let bitmap = Arc::new(AtomicU64::new(0));
        let parker = Arc::new(ThreadParker::new());
        let work = Arc::new(Work::new());

        let publisher = {
            let bitmap = Arc::clone(&bitmap);
            let parker = Arc::clone(&parker);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                work.publish_silently();
                super::atomics::fence(Ordering::SeqCst);
                if bitmap.swap(0, Ordering::AcqRel) != 0 {
                    parker.wake();
                }
            })
        };

        if !work.take() {
            parker.arm();
            bitmap.fetch_or(1, Ordering::SeqCst);
            super::atomics::fence(Ordering::SeqCst);
            if !work.take() {
                thread::park();
            }
        }
        publisher.join().unwrap();
    });
}

/// Tenth calibration: the identical protocol with **no crate type in
/// it at all** — loom atomics, a loom `Thread` handle passed by hand,
/// loom's park and unpark.
///
/// Hand-trace of the only schedules that exist, with `w` the waiter
/// and `p` the publisher:
///
/// - `p` runs first: `available == 1`, so `w`'s first `take` (a `swap`
///   RMW, which must read the latest value in modification order)
///   returns true and `w` never parks.
/// - `p`'s bitmap read falls before `w`'s `fetch_or`: then `p`'s
///   publish also precedes `w`'s *second* `take`, which returns true
///   and `w` never parks.
/// - `p`'s bitmap read falls after `w`'s `fetch_or`: `p` reads 1 and
///   unparks, and the token holds across `w`'s later `park`.
///
/// No schedule leaves `w` parked, so this must pass. If it deadlocks,
/// loom is reporting a state its own memory model forbids, and every
/// deadlock in this file is an artifact of the tool rather than a
/// property of the ring.
#[test]
#[ignore = "THE FINDING: no crate code, yet loom still reports a deadlock; see the module docs"]
fn a_handshake_built_only_from_loom_primitives_cannot_miss_a_wake() {
    loom::model(|| {
        let available = Arc::new(AtomicU64::new(0));
        let bitmap = Arc::new(AtomicU64::new(0));
        let handle = Arc::new(Mutex::new(None));

        let publisher = {
            let available = Arc::clone(&available);
            let bitmap = Arc::clone(&bitmap);
            let handle = Arc::clone(&handle);
            thread::spawn(move || {
                available.store(1, Ordering::Release);
                super::atomics::fence(Ordering::SeqCst);
                if bitmap.swap(0, Ordering::AcqRel) != 0 {
                    let parked: Option<thread::Thread> = handle.lock().unwrap().take();
                    parked.unwrap().unpark();
                }
            })
        };

        if available.swap(0, Ordering::Acquire) == 0 {
            *handle.lock().unwrap() = Some(thread::current());
            bitmap.fetch_or(1, Ordering::SeqCst);
            super::atomics::fence(Ordering::SeqCst);
            if available.swap(0, Ordering::Acquire) == 0 {
                thread::park();
            }
        }
        publisher.join().unwrap();
    });
}

/// The core claim, with one waiter and one publisher: a waiter can
/// never be left parked on work that has already been published.
///
/// A schedule that loses the wake deadlocks — the waiter parks and no
/// unpark is ever delivered — and loom reports it with the
/// interleaving, rather than the 1ms stall the real backstop produces.
///
/// Deadlocks under loom 0.7.2, as a false positive; see the module
/// docs for the calibration that establishes this.
/// Isolates *which half* of the handshake the deadlock lives in.
///
/// The publisher never calls `wake_one`, so the only thing that can
/// save the waiter is its own re-check after arming. If this passes,
/// the re-check half is sound and the defect is in the wake half; if it
/// deadlocks, loom is simply exploring the schedule where the waiter
/// parks on work published after its last read — which is what the
/// wake exists to cover, and would mean the model, not the ring, is
/// wrong.
#[test]
fn the_recheck_alone_cannot_be_expected_to_catch_a_later_publication() {
    loom::model(|| {
        let wakers = Arc::new(WakeSet::new());
        let work = Arc::new(Work::new());

        let publisher = {
            let work = Arc::clone(&work);
            thread::spawn(move || {
                work.publish_silently();
            })
        };

        publisher.join().unwrap();
        Waiter::new(&wakers, &work, ParkSlot::Leased(0)).wait();
    });
}

/// The same handshake, with `flush` in place of `wake_one`.
///
/// `flush` swaps the whole bitmap to zero and unparks every slot it
/// finds, so it exercises the publish/arm ordering without the
/// round-robin bit selection or the single-handle claim. If this passes
/// while [`a_published_unit_of_work_always_reaches_a_parked_waiter`]
/// deadlocks, the ordering is sound and the defect is in `wake_one`'s
/// selection or in [`ThreadParker::wake`](super::thread_parker::ThreadParker::wake).
#[test]
#[ignore = "loom 0.7.2 reports a false deadlock here; see the module docs"]
fn a_flushed_wake_always_reaches_a_parked_waiter() {
    loom::model(|| {
        let wakers = Arc::new(WakeSet::new());
        let work = Arc::new(Work::new());

        let publisher = {
            let wakers = Arc::clone(&wakers);
            let work = Arc::clone(&work);
            thread::spawn(move || {
                work.publish_and_flush(&wakers);
            })
        };

        Waiter::new(&wakers, &work, ParkSlot::Leased(0)).wait();
        publisher.join().unwrap();
    });
}

#[test]
#[ignore = "loom 0.7.2 reports a false deadlock here; see the module docs"]
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
#[ignore = "loom 0.7.2 reports a false deadlock here; see the module docs"]
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
