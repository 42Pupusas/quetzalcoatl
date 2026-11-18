use super::{ExclusiveRegistration, ParkRegistration, ParkSite, ParkedFuture};
use crate::common::park_registry::ParkSlot;
use crate::common::wake_async::WakerSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

struct CountingWaker {
    count: AtomicUsize,
}

impl CountingWaker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
        })
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

fn slot0() -> ParkSlot {
    ParkSlot::from_exclusive_index(0)
}

/// The defect this module exists to fix: without removal on drop,
/// each new future displaces the last one's dead waker into the
/// overflow, which grows without bound.
#[test]
fn dropping_a_guard_leaves_nothing_behind() {
    let set = WakerSet::new();

    for _ in 0..1_000 {
        let waker = Waker::from(CountingWaker::new());
        let mut guard = ParkRegistration::new(&set, slot0());
        guard.arm(&Context::from_waker(&waker));
    }

    assert_eq!(set.overflow.depth(), 0);
}

/// A guard that is still armed must be woken normally.
#[test]
fn a_live_guard_is_still_woken() {
    let set = WakerSet::new();
    let waker = CountingWaker::new();
    let handle = Waker::from(Arc::clone(&waker));
    let mut guard = ParkRegistration::new(&set, slot0());
    guard.arm(&Context::from_waker(&handle));

    set.wake_all();
    assert_eq!(waker.count(), 1);
}

/// Re-arming across polls must not accumulate registrations.
#[test]
fn re_arming_one_guard_holds_one_registration() {
    let set = WakerSet::new();
    let mut guard = ParkRegistration::new(&set, slot0());

    for _ in 0..1_000 {
        let probe = Waker::from(CountingWaker::new());
        guard.arm(&Context::from_waker(&probe));
    }

    assert_eq!(set.overflow.depth(), 0);
}

/// The lost-wakeup defect this module's identity exists to fix.
///
/// Two futures from one endpoint, polled by one task, carry equal
/// wakers. Identifying a registration by waker let the cancelled
/// one's withdrawal clear the sibling's record, leaving it parked
/// with nothing saying it waits. With ids, the sibling survives.
#[test]
fn cancelling_one_future_leaves_a_sibling_from_the_same_task_woken() {
    let set = WakerSet::new();
    let task = CountingWaker::new();
    let waker = Waker::from(Arc::clone(&task));
    let cx = Context::from_waker(&waker);

    let mut sibling = ParkRegistration::new(&set, slot0());
    sibling.arm(&cx);

    {
        let mut cancelled = ParkRegistration::new(&set, slot0());
        cancelled.arm(&cx);
    }

    set.wake_all();
    assert_eq!(
        task.count(),
        1,
        "the surviving sibling was not woken exactly once"
    );
}

/// The same race with the roles reversed: the guard armed *first*
/// is cancelled, and the one that displaced it must still be woken.
#[test]
fn cancelling_the_displaced_future_leaves_the_occupant_woken() {
    let set = WakerSet::new();
    let task = CountingWaker::new();
    let waker = Waker::from(Arc::clone(&task));
    let cx = Context::from_waker(&waker);

    let mut occupant = ParkRegistration::new(&set, slot0());
    {
        let mut cancelled = ParkRegistration::new(&set, slot0());
        cancelled.arm(&cx);
        occupant.arm(&cx);
    }

    set.wake_all();
    assert_eq!(
        task.count(),
        1,
        "the occupant lost its wake to a cancelled peer"
    );
}

/// A cancelled slotless waiter must leave nothing on the overflow:
/// it has no slot, so the withdrawal has to reach the list.
#[test]
fn a_cancelled_slotless_guard_leaves_nothing_behind() {
    let set = WakerSet::new();

    for _ in 0..1_000 {
        let waker = Waker::from(CountingWaker::new());
        let mut guard = ParkRegistration::new(&set, ParkSlot::Shared);
        guard.arm(&Context::from_waker(&waker));
    }

    assert_eq!(set.overflow.depth(), 0);
}

/// A slotless waiter that is still armed must remain reachable.
#[test]
fn a_live_slotless_guard_is_still_woken() {
    let set = WakerSet::new();
    let waker = CountingWaker::new();
    let handle = Waker::from(Arc::clone(&waker));
    let mut guard = ParkRegistration::new(&set, ParkSlot::Shared);
    guard.arm(&Context::from_waker(&handle));

    set.wake_all();
    assert_eq!(waker.count(), 1);
}

/// Re-arming a slotless guard across polls must hold exactly one
/// registration, not one per poll.
#[test]
fn re_arming_a_slotless_guard_holds_one_registration() {
    let set = WakerSet::new();
    let waker = Waker::from(CountingWaker::new());
    let mut guard = ParkRegistration::new(&set, ParkSlot::Shared);

    for _ in 0..1_000 {
        guard.arm(&Context::from_waker(&waker));
        assert_eq!(set.overflow.depth(), 1);
    }
}

/// A guard displaced onto the overflow by a peer must withdraw from
/// there — the slot no longer holds it.
///
/// The peer keeps its registration: taking it out of the slot to
/// look underneath is why the withdrawal hands it to the overflow
/// rather than dropping it. So one registration survives, and it is
/// the peer's.
#[test]
fn a_guard_displaced_onto_the_overflow_still_withdraws() {
    let set = WakerSet::new();
    let peer = CountingWaker::new();
    let cancelled = CountingWaker::new();

    {
        let mut displaced = ParkRegistration::new(&set, slot0());
        displaced.arm(&Context::from_waker(&Waker::from(Arc::clone(&cancelled))));

        // A peer takes the slot, pushing `displaced` to the overflow.
        let peer_waker = Waker::from(Arc::clone(&peer));
        set.register(slot0(), &Context::from_waker(&peer_waker));

        assert_eq!(set.overflow.depth(), 1);
    }

    assert_eq!(
        set.overflow.depth(),
        1,
        "the cancelled guard was left alongside the peer"
    );

    set.wake_all();
    assert_eq!(peer.count(), 1, "the peer lost its wake");
    assert_eq!(cancelled.count(), 0, "a cancelled waiter was woken");
}

/// A peer that displaced this guard is still parked, so dropping
/// the guard must not discard the peer's waker.
#[test]
fn dropping_a_displaced_guard_preserves_the_peer() {
    let set = WakerSet::new();
    let peer = CountingWaker::new();

    {
        let mut guard = ParkRegistration::new(&set, slot0());
        let mine = Waker::from(CountingWaker::new());
        guard.arm(&Context::from_waker(&mine));

        // The peer registered directly on the set, mirroring a live
        // future from the same endpoint displacing this guard.
        let peer_waker = Waker::from(Arc::clone(&peer));
        set.register(slot0(), &Context::from_waker(&peer_waker));
    }

    set.wake_all();
    assert_eq!(peer.count(), 1, "a live peer's waker was discarded");
}

/// Waking an armed guard consumes the registration; the later drop
/// must not then disturb whatever occupies the slot.
#[test]
fn dropping_an_already_woken_guard_is_harmless() {
    let set = WakerSet::new();
    let later = CountingWaker::new();

    {
        let waker = Waker::from(CountingWaker::new());
        let mut guard = ParkRegistration::new(&set, slot0());
        guard.arm(&Context::from_waker(&waker));
        set.wake_all();

        let later_waker = Waker::from(Arc::clone(&later));
        set.register(slot0(), &Context::from_waker(&later_waker));
    }

    set.wake_all();
    assert_eq!(later.count(), 1, "the later occupant lost its wake");
}

struct FakeSite {
    set: WakerSet,
    polled: usize,
}

impl ParkSite for FakeSite {
    fn park_set(&mut self) -> &WakerSet {
        &self.set
    }
}

/// A cancelled `ParkedFuture` must leave no registration: this is
/// the exclusive-endpoint flavour of
/// [`dropping_a_guard_leaves_nothing_behind`].
#[test]
fn a_cancelled_parked_future_leaves_no_registration() {
    let mut site = FakeSite {
        set: WakerSet::new(),
        polled: 0,
    };

    for _ in 0..1_000 {
        let waker = Waker::from(CountingWaker::new());
        let mut cx = Context::from_waker(&waker);
        let mut future = ParkedFuture::new(
            &mut site,
            slot0(),
            |site: &mut FakeSite, parker: &mut ExclusiveRegistration, cx| {
                site.polled += 1;
                parker.arm(site.park_set(), cx);
                Poll::Pending::<()>
            },
        );
        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());
    }

    assert_eq!(site.set.overflow.depth(), 0);
    assert_eq!(site.polled, 1_000);
}

/// Completion withdraws too: a future that returns `Ready` must
/// not leave its waker where a later wake can fire it.
#[test]
fn a_completed_parked_future_leaves_no_registration() {
    let mut site = FakeSite {
        set: WakerSet::new(),
        polled: 0,
    };
    let waker = Waker::from(CountingWaker::new());
    let mut cx = Context::from_waker(&waker);

    let mut future = ParkedFuture::new(
        &mut site,
        slot0(),
        |site: &mut FakeSite, parker: &mut ExclusiveRegistration, cx| {
            site.polled += 1;
            if site.polled == 1 {
                parker.arm(site.park_set(), cx);
                return Poll::Pending;
            }
            Poll::Ready(())
        },
    );
    assert!(Pin::new(&mut future).poll(&mut cx).is_pending());
    assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(()));
    drop(future);

    let survivor = CountingWaker::new();
    site.set.register(
        slot0(),
        &Context::from_waker(&Waker::from(Arc::clone(&survivor))),
    );
    site.set.wake_all();
    assert_eq!(
        survivor.count(),
        1,
        "the set stopped working after withdraw"
    );
    let second = CountingWaker::new();
    site.set.register(
        slot0(),
        &Context::from_waker(&Waker::from(Arc::clone(&second))),
    );
    site.set.wake_all();
    assert_eq!(second.count(), 1, "the second registration was lost");
}
