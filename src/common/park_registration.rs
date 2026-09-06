//! Park registrations owned by the future that made them.
//!
//! [`WakerSet::register`](super::wake_async::WakerSet::register) records
//! a waker and returns. Nothing then removes that record except a wake,
//! so a future that is cancelled while parked leaves its waker behind.
//!
//! That is not merely a wasted wake event. A park slot belongs to an
//! endpoint, and storing into an occupied slot displaces the previous
//! waker into the overflow list rather than dropping it — the fix for
//! two live futures sharing one handle. A cancel/retry loop on one
//! endpoint therefore pushes a fresh entry on every iteration: each new
//! future's waker displaces the dead one before it, and the overflow
//! grows without bound for as long as no wake drains it. A thousand
//! cancelled pushes on an idle ring leave a thousand live `Waker`s.
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
use std::task::{Context, Poll, Waker};

use super::park_registry::ParkSlot;
use super::wake_async::WakerSet;

/// The record of one armed registration, independent of who owns it.
///
/// `arm` replaces this registration's own previous waker in place; a
/// waker armed by a peer is never touched here. `withdraw` removes the
/// registration if it is still ours. A slot may hold a peer's waker
/// rather than ours — the peer registered after us and displaced us —
/// and that waker belongs to a future that is still parked, so it is
/// moved to the overflow list rather than discarded, the same rule
/// [`WakerSet::register`](super::wake_async::WakerSet::register)
/// follows. Every wake drains the overflow, so the peer stays
/// reachable.
pub struct ExclusiveRegistration {
    slot: ParkSlot,
    /// The waker this record published, kept to recognise its own
    /// registration at withdraw. `None` before the first `arm`.
    armed: Option<Waker>,
}

impl ExclusiveRegistration {
    #[must_use]
    pub const fn new(slot: ParkSlot) -> Self {
        Self { slot, armed: None }
    }

    /// Registers `cx`'s waker, replacing any registration this record
    /// already holds.
    ///
    /// Re-arming with the same waker is the common case — an executor
    /// polls a pending future with a stable waker — and a changed waker
    /// (the future moved tasks) withdraws the old registration first,
    /// so this record's own stale entry is never displaced into the
    /// overflow.
    #[inline]
    pub fn arm(&mut self, set: &WakerSet, cx: &Context<'_>) {
        let waker = cx.waker();
        if self
            .armed
            .as_ref()
            .is_some_and(|armed| !armed.will_wake(waker))
        {
            self.withdraw(set);
        }
        set.register(self.slot, cx);
        self.armed = Some(waker.clone());
    }

    /// Drops the registration without waking it.
    #[inline]
    pub fn withdraw(&mut self, set: &WakerSet) {
        let Some(waker) = self.armed.take() else {
            return;
        };
        let Some(index) = self.slot.index() else {
            return;
        };
        if let Some(peer) = set.slots[index].clear_if(&waker) {
            set.overflow.register_owned(peer);
        }
    }
}

impl Drop for ExclusiveRegistration {
    fn drop(&mut self) {
        // `armed` is cleared without touching the set: a record whose
        // set is unreachable can only be holding a waker the set's own
        // drop will free. `ParkRegistration` and `ParkedFuture` both
        // withdraw through the set before this runs.
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
