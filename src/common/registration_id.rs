//! Identity for async park registrations.
//!
//! A registration used to be identified by its [`Waker`], compared with
//! [`Waker::will_wake`]. That compares *tasks*, not registrations. Two
//! futures polled from one task carry equal wakers, so a cancelled
//! future's withdrawal cleared whichever record occupied the slot —
//! including a sibling's. The sibling stayed parked with nothing
//! recording that it was waiting, which is the lost-wakeup shape the
//! park machinery exists to prevent.
//!
//! An id minted per registration tells the two apart. It is compared
//! only for equality; no ordering or synchronization decision reads it,
//! so the counter that mints it carries no ordering obligations beyond
//! handing out distinct values.
//!
//! Ids are unique within one [`WakerSet`](super::wake_async::WakerSet),
//! which is the only scope in which they are compared.

use super::atomics::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

/// A registration's identity, unique within one set.
///
/// Ordering exists so tests can sort and dedup ids; no protocol
/// decision reads anything but equality.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RegistrationId(u64);

/// Mints [`RegistrationId`]s for one set.
pub struct RegistrationIds {
    next: AtomicU64,
}

impl RegistrationIds {
    #[cfg(not(loom))]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
        }
    }

    // Loom's `AtomicU64::new` is not const.
    #[cfg(loom)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
        }
    }

    /// Returns an id no live registration on this set holds.
    ///
    /// A `u64` counter incremented once per registration does not wrap
    /// in any run this crate can have: at one registration per
    /// nanosecond it takes over five hundred years.
    #[inline]
    pub fn mint(&self) -> RegistrationId {
        RegistrationId(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for RegistrationIds {
    fn default() -> Self {
        Self::new()
    }
}

/// One registration: the waker to fire, and the id that says whose it
/// is.
pub struct Registration {
    id: RegistrationId,
    waker: Waker,
}

impl Registration {
    #[must_use]
    pub const fn new(id: RegistrationId, waker: Waker) -> Self {
        Self { id, waker }
    }

    #[must_use]
    pub const fn id(&self) -> RegistrationId {
        self.id
    }

    #[must_use]
    pub const fn is(&self, id: RegistrationId) -> bool {
        self.id.0 == id.0
    }

    /// Whether this registration's waker would wake the same task as
    /// `other`'s — true for two futures polled from one task, which is
    /// exactly why the waker cannot serve as identity.
    ///
    /// Its only caller is gated out under Miri, where `will_wake` is
    /// unreliable, so this is gated the same way rather than left as
    /// dead code there.
    #[cfg(all(test, not(miri)))]
    #[must_use]
    pub fn will_wake_same_task_as(&self, other: &Self) -> bool {
        self.waker.will_wake(&other.waker)
    }

    /// Fires the waker, consuming the registration.
    #[inline]
    pub fn wake(self) {
        self.waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::{Registration, RegistrationIds};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct CountingWaker {
        count: AtomicUsize,
    }

    impl CountingWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                count: AtomicUsize::new(0),
            })
        }
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn every_minted_id_is_distinct() {
        let ids = RegistrationIds::new();
        let minted: Vec<_> = (0..1_000).map(|_| ids.mint()).collect();

        for (i, id) in minted.iter().enumerate() {
            assert!(
                !minted[..i].iter().any(|earlier| earlier == id),
                "id {id:?} was minted twice"
            );
        }
    }

    /// The defect this type exists to fix: one task's waker cannot
    /// distinguish two of its own registrations, so identity has to
    /// come from somewhere else.
    #[test]
    fn two_registrations_sharing_a_waker_are_still_distinct() {
        let ids = RegistrationIds::new();
        let waker = Waker::from(CountingWaker::new());

        let first = Registration::new(ids.mint(), waker.clone());
        let second = Registration::new(ids.mint(), waker);

        // Miri has no codegen-level vtable dedup, so `will_wake` on a
        // clone is unreliable there; the identity claim below is the
        // point of the test either way.
        #[cfg(not(miri))]
        assert!(
            first.will_wake_same_task_as(&second),
            "the two registrations must share a task waker"
        );
        assert!(!first.is(second.id()));
        assert!(!second.is(first.id()));
    }

    #[test]
    fn a_registration_recognises_its_own_id() {
        let ids = RegistrationIds::new();
        let id = ids.mint();
        let registration = Registration::new(id, Waker::from(CountingWaker::new()));

        assert!(registration.is(id));
    }

    #[test]
    fn waking_fires_the_registered_waker() {
        let ids = RegistrationIds::new();
        let waker = CountingWaker::new();
        let registration =
            Registration::new(ids.mint(), Waker::from(Arc::clone(&waker)));

        registration.wake();
        assert_eq!(waker.count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn ids_from_one_set_are_minted_distinctly_across_threads() {
        let ids = Arc::new(RegistrationIds::new());
        let gate = Arc::new(std::sync::Barrier::new(8));

        // The threads are spawned into a `Vec` before any is joined, so
        // they mint concurrently; joining inside the first iterator
        // would serialise them and test nothing.
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let ids = Arc::clone(&ids);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    (0..256).map(|_| ids.mint()).collect::<Vec<_>>()
                })
            })
            .collect();

        let mut all = Vec::new();
        for thread in threads {
            all.extend(thread.join().unwrap());
        }
        let total = all.len();
        all.sort_unstable();
        all.dedup();

        assert_eq!(all.len(), total, "two threads were handed one id");
    }
}
