//! Park registrations owned by the future that made them.
//!
//! [`WakerSet::register`](super::wake_async::WakerSet::register) records
//! a waker and returns. Nothing then removes that record except a wake,
//! so a future that is cancelled while parked leaves its waker behind.
//!
//! That is not merely a wasted wake event. A park slot belongs to an
//! endpoint, and storing into an occupied slot displaces the previous
//! registration into the overflow list rather than dropping it — the
//! fix for two live futures sharing one handle. A cancel/retry loop on
//! one endpoint therefore pushes a fresh entry on every iteration: each
//! new future displaces the dead one before it, and the overflow grows
//! without bound for as long as no wake drains it. A thousand cancelled
//! pushes on an idle ring leave a thousand live `Waker`s.
//!
//! # Identity
//!
//! A registration is named by a [`RegistrationId`] minted per arm, not
//! by its [`Waker`]. `Waker::will_wake` compares *tasks*: two futures
//! polled from one task carry equal wakers, so identifying a
//! registration by waker let a cancelled future withdraw a live
//! sibling's record, leaving it parked with nothing saying it waits.
//! The id also lets a withdrawal find its entry on the overflow, where
//! a peer may have displaced it.
//!
//! Two wrappers tie a registration to a future's lifetime instead.
//!
//! - [`ParkRegistration`] holds a shared borrow of the
//!   [`WakerSet`]; an endpoint method taking `&self` keeps one in its
//!   future's closure.
//! - [`ParkedFuture`] owns the exclusive `&mut` endpoint borrow, so it
//!   cannot also hold a shared borrow of the same endpoint's set. It
//!   stores only the slot and the waker it armed, reaching the set
//!   through [`ParkSite`] at arm and withdraw time.
//!
//! Both withdraw on drop, which also covers completion: a future that
//! returns `Ready` leaves no registration for a later wake to trip
//! over.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::park_registry::ParkSlot;
use super::registration_id::RegistrationId;
use super::wake_async::WakerSet;

/// The record of one armed registration, independent of who owns it.
///
/// `arm` replaces this record's own previous registration in place; one
/// armed by a peer is never touched here. `withdraw` removes the
/// registration wherever it is — the slot, or the overflow a peer may
/// have displaced it onto. A slot may hold a peer's registration rather
/// than ours, and that belongs to a future which is still parked, so it
/// is moved to the overflow list rather than discarded, the same rule
/// [`WakerSet::register`](super::wake_async::WakerSet::register)
/// follows. Every wake drains the overflow, so the peer stays
/// reachable.
pub struct ExclusiveRegistration {
    slot: ParkSlot,
    /// The identity this record published, kept to recognise its own
    /// registration at withdraw. `None` before the first `arm`, and
    /// after a withdrawal.
    armed: Option<RegistrationId>,
}

impl ExclusiveRegistration {
    #[must_use]
    pub const fn new(slot: ParkSlot) -> Self {
        Self { slot, armed: None }
    }

    /// Registers `cx`'s waker, replacing any registration this record
    /// already holds.
    ///
    /// Re-arming reuses the record's own id, so a re-registration
    /// replaces its previous entry in the slot instead of displacing it
    /// onto the overflow. A future that moved tasks brings a different
    /// waker under the same id, which is the same replacement.
    ///
    /// The previous entry may have been displaced onto the overflow by
    /// a peer, so it is withdrawn from there first; otherwise
    /// re-arming across polls would leave one dead entry per poll.
    #[inline]
    pub fn arm(&mut self, set: &WakerSet, cx: &Context<'_>) {
        let id = self.armed.map_or_else(
            || set.mint(),
            |id| {
                set.overflow.withdraw(id);
                id
            },
        );
        set.register_as(id, self.slot, cx);
        self.armed = Some(id);
    }

    /// Drops the registration without waking it.
    #[inline]
    pub fn withdraw(&mut self, set: &WakerSet) {
        let Some(id) = self.armed.take() else {
            return;
        };
        set.withdraw(id, self.slot);
    }
}

impl Drop for ExclusiveRegistration {
    fn drop(&mut self) {
        // `armed` is cleared without touching the set: a record whose
        // set is unreachable can only be naming a registration the
        // set's own drop will free. `ParkRegistration` and
        // `ParkedFuture` both withdraw through the set before this runs.
        self.armed.take();
    }
}

/// A registration guard for endpoints shared by several futures.
///
/// Borrows the [`WakerSet`], so the future's closure holds it alongside
/// the shared endpoint borrow.
pub struct ParkRegistration<'a> {
    set: &'a WakerSet,
    parker: ExclusiveRegistration,
}

impl<'a> ParkRegistration<'a> {
    #[must_use]
    pub const fn new(set: &'a WakerSet, slot: ParkSlot) -> Self {
        Self {
            set,
            parker: ExclusiveRegistration::new(slot),
        }
    }

    /// Registers `cx`'s waker, replacing any registration this guard
    /// already holds.
    #[inline]
    pub fn arm(&mut self, cx: &Context<'_>) {
        self.parker.arm(self.set, cx);
    }
}

impl Drop for ParkRegistration<'_> {
    fn drop(&mut self) {
        self.parker.withdraw(self.set);
    }
}

/// An endpoint whose exclusive (`&mut`) async methods park on a
/// [`WakerSet`].
///
/// Implemented by the single-consumer endpoint types, whose `pop_async`
/// futures own the endpoint for their whole life and therefore reach
/// the set through it.
pub trait ParkSite {
    fn park_set(&mut self) -> &WakerSet;
}

/// A future that parks its body on a [`WakerSet`] it cannot borrow
/// directly.
///
/// `S` is the endpoint, borrowed `&mut` for the future's whole life;
/// the body `F` receives it together with an [`ExclusiveRegistration`]
/// to arm. On drop — cancellation or completion — the registration is
/// withdrawn, so a cancel/retry loop leaves nothing behind.
pub struct ParkedFuture<'a, S: ParkSite, F> {
    site: &'a mut S,
    parker: ExclusiveRegistration,
    body: F,
}

impl<'a, S: ParkSite, Out, F> ParkedFuture<'a, S, F>
where
    F: FnMut(&mut S, &mut ExclusiveRegistration, &mut Context<'_>) -> Poll<Out>,
{
    #[must_use]
    pub const fn new(site: &'a mut S, slot: ParkSlot, body: F) -> Self {
        Self {
            site,
            parker: ExclusiveRegistration::new(slot),
            body,
        }
    }
}

impl<S, F, Out> Future for ParkedFuture<'_, S, F>
where
    S: ParkSite,
    F: FnMut(&mut S, &mut ExclusiveRegistration, &mut Context<'_>) -> Poll<Out> + Unpin,
{
    type Output = Out;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Out> {
        let this = self.get_mut();
        (this.body)(this.site, &mut this.parker, cx)
    }
}

impl<S: ParkSite, F> Drop for ParkedFuture<'_, S, F> {
    fn drop(&mut self) {
        let set = self.site.park_set();
        self.parker.withdraw(set);
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(survivor.count(), 1, "the set stopped working after withdraw");
        let second = CountingWaker::new();
        site.set.register(
            slot0(),
            &Context::from_waker(&Waker::from(Arc::clone(&second))),
        );
        site.set.wake_all();
        assert_eq!(second.count(), 1, "the second registration was lost");
    }
}
