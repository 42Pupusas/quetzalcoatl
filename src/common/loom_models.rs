//! Exhaustive-schedule models for the async wake machinery.
//!
//! These run only under `RUSTFLAGS="--cfg loom" cargo test --lib
//! --features async`; they exercise every interleaving the C11 memory
//! model permits, not one sample of them. They complement Miri (one
//! schedule, checked for UB) and the release stress suite (one
//! platform's scheduler, at scale) — neither enumerates schedules.
//!
//! Scope ends at the leaf primitives. The park/blocking machinery
//! calls `std::thread::park`, which loom does not mock (a parked model
//! fiber would block the one real thread), and the ring slot machinery
//! is far too large to enumerate. `wake_async`, `waker_overflow`, and
//! `park_registry` route their atomics through
//! [`crate::common::atomics`], which is what makes them modelable.

use crate::common::atomics::AtomicUsize;
use crate::common::park_registry::ParkRegistry;
use crate::common::park_registration::{ExclusiveRegistration, ParkRegistration};
use crate::common::park_registry::ParkSlot;
use crate::common::wake_async::WakerSet;
use loom::sync::Arc;
use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering;
use std::task::{Context, Wake, Waker};

/// A waker whose wake count is a model-visible atomic, so the final
/// assertion observes exactly the value the modeled schedule produced.
/// The outer `Arc` is std's: `Waker::from` exists only for std's arc,
/// and the waker body is opaque to the model either way.
struct CountingWaker {
    count: AtomicUsize,
}

impl CountingWaker {
    fn new() -> StdArc<Self> {
        StdArc::new(Self {
            count: AtomicUsize::new(0),
        })
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl Wake for CountingWaker {
    fn wake(self: StdArc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

const fn slot0() -> ParkSlot {
    ParkSlot::from_exclusive_index(0)
}

const fn ctx(waker: &Waker) -> Context<'_> {
    Context::from_waker(waker)
}

/// The peer-preservation rule of a withdrawal, under every ordering of
/// a live peer's registration against it: whatever occupies the slot
/// when the cancelled waiter withdraws must reach a wake, never a
/// drop. A schedule that drops the peer leaves `count == 0`.
#[test]
fn a_withdrawn_waiter_never_drops_a_live_peers_waker() {
    loom::model(|| {
        let set = Arc::new(WakerSet::new());
        let peer = CountingWaker::new();

        let parked = {
            let set = Arc::clone(&set);
            let peer = StdArc::clone(&peer);
            loom::thread::spawn(move || {
                let waker = Waker::from(peer);
                set.register(slot0(), &ctx(&waker));
            })
        };

        let mine = CountingWaker::new();
        let waker = Waker::from(mine);
        let mut guard = ParkRegistration::new(&set, slot0());
        guard.arm(&ctx(&waker));
        drop(guard);

        parked.join().unwrap();
        set.wake_all();

        assert_eq!(
            peer.count(),
            1,
            "a live peer's waker was dropped by the withdrawal"
        );
    });
}

/// A wake racing a cancellation must claim the waker at most once:
/// both paths take the registration with a single `swap`, so exactly
/// one of them can find it. A double fire (`count == 2`) means a wake
/// path fired a waker it had not removed. The cancelling thread owns
/// the registration and reaches the set through it, the shape a real
/// cancelled future has.
#[test]
fn a_wake_and_a_withdraw_claim_the_waker_at_most_once() {
    loom::model(|| {
        let set = Arc::new(WakerSet::new());
        let waiter = CountingWaker::new();

        {
            let waker = Waker::from(StdArc::clone(&waiter));
            let mut reg = ExclusiveRegistration::new(slot0());
            reg.arm(&set, &ctx(&waker));
        }

        let canceller = {
            let set = Arc::clone(&set);
            loom::thread::spawn(move || {
                set.wake_all();
            })
        };
        canceller.join().unwrap();

        assert!(
            waiter.count() <= 1,
            "the waker fired {} times",
            waiter.count()
        );
    });
}

/// Registrations and wakes racing on one slot: every waiter must be
/// woken exactly once once the final sweep runs — never twice (a wake
/// that does not remove its waker), never zero (an overflow push lost
/// to a concurrent drain, the classic Treiber-stack race).
#[test]
fn concurrent_registrations_and_wakes_deliver_exactly_one_wake_each() {
    loom::model(|| {
        let set = Arc::new(WakerSet::new());
        let first = CountingWaker::new();
        let second = CountingWaker::new();

        let t1 = {
            let set = Arc::clone(&set);
            let waker = Waker::from(StdArc::clone(&first));
            loom::thread::spawn(move || {
                set.register(slot0(), &ctx(&waker));
                set.wake_all();
            })
        };
        let t2 = {
            let set = Arc::clone(&set);
            let waker = Waker::from(StdArc::clone(&second));
            loom::thread::spawn(move || {
                set.register(slot0(), &ctx(&waker));
                set.wake_all();
            })
        };
        t1.join().unwrap();
        t2.join().unwrap();

        set.wake_all();

        assert_eq!(
            first.count(),
            1,
            "waiter fired {} times",
            first.count()
        );
        assert_eq!(
            second.count(),
            1,
            "waiter fired {} times",
            second.count()
        );
    });
}

/// Two threads leasing from one registry must never receive the same
/// slot — aliasing a live waiter is the lost-wakeup defect the
/// registry exists to prevent — and a released index must return to
/// the free set.
#[test]
fn concurrent_leases_never_alias_and_released_slots_are_reused() {
    loom::model(|| {
        let registry = Arc::new(ParkRegistry::new());

        let t1 = {
            let registry = Arc::clone(&registry);
            loom::thread::spawn(move || registry.lease())
        };
        let t2 = {
            let registry = Arc::clone(&registry);
            loom::thread::spawn(move || registry.lease())
        };
        let first = t1.join().unwrap();
        let second = t2.join().unwrap();

        let (a, b) = (first.index(), second.index());
        assert!(a.is_some(), "the registry was full with two leasers");
        assert!(b.is_some(), "the registry was full with two leasers");
        assert_ne!(a, b, "two live waiters were handed the same slot");

        registry.release(first);
        assert_eq!(registry.lease().index(), a, "the freed slot was not reused");
    });
}
