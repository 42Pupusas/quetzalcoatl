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
mod tests;
