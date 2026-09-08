use std::ptr;
use std::sync::atomic::Ordering;

use super::atomics::{fence, AtomicBool, AtomicPtr};

use super::park::PARK_SLOTS;
use super::park_registry::ParkSlot;
use super::registration_id::{Registration, RegistrationId, RegistrationIds};
use super::waker_overflow::WakerOverflow;
use super::AlignedBuf;

/// A single [`Registration`], owned through an [`AtomicPtr`].
///
/// The writer is the producer or consumer that owns the slot (calling
/// `store` before returning `Poll::Pending`). The reader is the peer
/// side calling `wake`. Both sides mutate the slot with a single `swap`,
/// so the thread that swaps a pointer *out* is its unique owner and is
/// the only one that may drop it. A null pointer means "nothing
/// registered", which also makes a fired waker unreachable to later
/// scans — without that, a fast wake-loop would re-fire one slot while
/// its peers starve.
///
/// A seqlock cannot be used here: a `Waker` is not trivially copyable,
/// so a reader that observes a torn write would call through a vtable
/// pointer it must never dereference.
pub struct WakerSlot {
    registration: AtomicPtr<Registration>,
}

impl WakerSlot {
    // Loom's `AtomicPtr::new` is not const, so the loom twin of this
    // constructor takes the weaker form. Std callers keep const use.
    #[cfg(not(loom))]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            registration: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[cfg(loom)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            registration: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Stores `registration`, returning whatever it displaced.
    ///
    /// A displaced registration belongs to a future that is still
    /// parked, so the caller must keep it reachable; dropping it
    /// strands that future. Returns `None` when the slot was empty, or
    /// when the entry replaced was this same registration — the common
    /// case of one future re-registering on each poll.
    #[inline]
    #[must_use]
    pub fn store(&self, registration: Registration) -> Option<Registration> {
        let id = registration.id();
        let next = Box::into_raw(Box::new(registration));
        let prev = self.registration.swap(next, Ordering::AcqRel);
        if prev.is_null() {
            return None;
        }
        // SAFETY: the swap removed `prev` from the slot, so this
        // thread is its sole owner and no peer can observe it again.
        let prev = unsafe { Box::from_raw(prev) };
        if prev.is(id) {
            return None;
        }
        Some(*prev)
    }

    /// Takes the registration out, reporting whose it was.
    ///
    /// Used by a cancelled waiter to withdraw its own registration. The
    /// slot is emptied whatever it held; a [`Withdrawn::Peer`] belongs
    /// to a future that is still parked, so the caller must keep it
    /// reachable.
    ///
    /// Identity is the [`RegistrationId`], not the waker. Comparing
    /// wakers with [`Waker::will_wake`](std::task::Waker::will_wake)
    /// identifies the *task*: two futures polled from one task compare
    /// equal, so a cancelled future would clear a live sibling's
    /// registration and leave it parked with nothing recording that it
    /// waits.
    ///
    /// The three answers are distinct because the caller must act
    /// differently on each — see [`WakerSet::withdraw`].
    #[inline]
    #[must_use]
    pub fn withdraw(&self, id: RegistrationId) -> Withdrawn {
        if self.registration.load(Ordering::Relaxed).is_null() {
            return Withdrawn::Empty;
        }
        let claimed = self.registration.swap(ptr::null_mut(), Ordering::AcqRel);
        if claimed.is_null() {
            return Withdrawn::Empty;
        }
        // SAFETY: the swap removed `claimed` from the slot, so this
        // thread is its sole owner and no peer can observe it again.
        let claimed = unsafe { Box::from_raw(claimed) };
        if claimed.is(id) {
            return Withdrawn::Own;
        }
        Withdrawn::Peer(*claimed)
    }

    /// Wakes the registered waiter. Returns `true` only when this call
    /// claimed the registration and fired it.
    #[inline]
    pub fn wake(&self) -> bool {
        if self.registration.load(Ordering::Relaxed).is_null() {
            return false;
        }
        let claimed = self.registration.swap(ptr::null_mut(), Ordering::AcqRel);
        if claimed.is_null() {
            return false;
        }
        // SAFETY: the swap removed `claimed` from the slot, so this
        // thread is its sole owner and no peer can observe it again.
        let registration = unsafe { Box::from_raw(claimed) };
        registration.wake();
        true
    }
}

/// What a [`WakerSlot::withdraw`] found in the slot.
pub enum Withdrawn {
    /// The withdrawing waiter's own registration; nothing else to do.
    Own,
    /// A peer's, displaced here and still parked. It must be kept
    /// reachable, and the withdrawer's own entry is elsewhere.
    Peer(Registration),
    /// Nothing was registered. The withdrawer's entry may still be on
    /// the overflow, displaced there by a peer.
    Empty,
}

impl Default for WakerSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WakerSlot {
    fn drop(&mut self) {
        // `&mut self` proves no peer holds a reference, so a plain swap
        // takes the pointer with the same uniqueness the old `get_mut`
        // path had (and is the shared form: loom's `AtomicPtr` exposes
        // no `get_mut`).
        let stored = self.registration.swap(ptr::null_mut(), Ordering::Relaxed);
        if !stored.is_null() {
            // SAFETY: the swap removed `stored` from the slot, so this
            // thread is its sole owner and no peer can observe it again.
            drop(unsafe { Box::from_raw(stored) });
        }
    }
}

/// An array of [`WakerSlot`]s with a fast-path `pending` flag.
///
/// Mirrors [`super::park::WakeSet`] but for async `Waker`s instead of
/// parked threads. Each producer or consumer owns a stable slot index
/// (the same index used by the blocking `WakeSet`).
pub struct WakerSet {
    /// Set to `true` when any slot has a registered waker. Checked with
    /// `Relaxed` before scanning slots, so the no-waiter hot path pays
    /// only one atomic load.
    pub pending: AtomicBool,
    pub slots: AlignedBuf<WakerSlot>,
    /// Registrations for waiters holding no park slot, and for those
    /// displaced from one. An async waiter has nothing to rescue
    /// itself, so it must be recorded somewhere even when every slot is
    /// leased. Lock-free, and an empty one costs a single `Relaxed`
    /// load on the wake path.
    pub overflow: WakerOverflow,
    /// Mints the identity each registration is withdrawn by.
    ids: RegistrationIds,
}

impl WakerSet {
    pub fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            slots: AlignedBuf::new_with(PARK_SLOTS, WakerSlot::new),
            overflow: WakerOverflow::new(),
            ids: RegistrationIds::new(),
        }
    }

    /// Returns an id no live registration on this set holds.
    #[inline]
    pub fn mint(&self) -> RegistrationId {
        self.ids.mint()
    }

    /// Registers `cx`'s waker for `slot` under a freshly minted id,
    /// which is returned.
    ///
    /// Every production caller withdraws — through
    /// [`ExclusiveRegistration`] — and so mints its own id and calls
    /// [`register_as`](Self::register_as) to keep one identity across
    /// polls. This is the uncancellable form, for tests that only need
    /// an entry in the set.
    ///
    /// [`ExclusiveRegistration`]: super::park_registration::ExclusiveRegistration
    #[cfg(test)]
    #[inline]
    pub fn register(&self, slot: ParkSlot, cx: &std::task::Context<'_>) -> RegistrationId {
        let id = self.ids.mint();
        self.register_as(id, slot, cx);
        id
    }

    /// Registers `cx`'s waker for `slot` under `id`. Sets
    /// `pending = true`.
    ///
    /// Re-registering under the same id replaces that entry in place,
    /// so a future re-armed on each poll holds one registration rather
    /// than displacing itself onto the overflow.
    ///
    /// A slot is owned by an endpoint, not by a future, and the async
    /// methods take `&self`: two futures from one handle share a slot,
    /// and the second registration displaces the first. The displaced
    /// future is still parked, so it moves to the overflow list rather
    /// than being dropped. The two are told apart by `id`, never by
    /// comparing wakers — wakers name tasks, and both futures may be
    /// polled from one.
    ///
    /// The blocking twin ([`super::thread_parker::ThreadParker`]) needs
    /// no such care. Endpoints are `Send` but not `Sync`, so one handle
    /// is only ever inside one blocking call at a time, and re-arming
    /// can only displace a handle the same thread has finished with.
    /// Futures break that: a single thread can hold several at once.
    ///
    /// Called by the future before returning `Poll::Pending`. The
    /// trailing `SeqCst` fence pairs with the fence in `wake_one`/
    /// `wake_n` so the waker's post-register re-check of the ring and a
    /// peer's wake-scan are totally ordered — without it the register/
    /// recheck and free/wake handshake can miss (Dekker), leaving the
    /// future parked with no further poll. Mirrors the blocking
    /// waiter's `fetch_or(SeqCst); fence(SeqCst)` in `park::WakeSet`.
    #[inline]
    pub fn register_as(&self, id: RegistrationId, slot: ParkSlot, cx: &std::task::Context<'_>) {
        let registration = Registration::new(id, cx.waker().clone());
        match slot.index() {
            Some(index) => {
                if let Some(displaced) = self.slots[index].store(registration) {
                    self.overflow.register(displaced);
                }
            }
            None => self.overflow.register(registration),
        }
        self.pending.store(true, Ordering::Release);
        fence(Ordering::SeqCst);
    }

    /// Removes the registration `id` names, wherever it ended up.
    ///
    /// A registration made against a leased slot can be displaced into
    /// the overflow by a peer, so a withdrawal that misses the slot
    /// still has to sweep the list — otherwise a cancelled future's
    /// entry survives there until some unrelated wake drains it.
    ///
    /// A peer's registration displaced out of the slot by this call is
    /// still parked, so it moves to the overflow rather than being
    /// dropped.
    #[inline]
    pub fn withdraw(&self, id: RegistrationId, slot: ParkSlot) {
        if let Some(index) = slot.index() {
            match self.slots[index].withdraw(id) {
                // Ours was in the slot and is now gone; a registration
                // is only ever in one place, so nothing is left to
                // sweep.
                Withdrawn::Own => return,
                // A peer displaced us and is still parked: keep it
                // reachable, then look for ours on the overflow.
                Withdrawn::Peer(peer) => self.overflow.register(peer),
                // Either we were already woken, or a peer displaced us
                // onto the overflow and was then woken itself.
                Withdrawn::Empty => {}
            }
        }
        self.overflow.withdraw(id);
    }

    /// Wakes every registered waker. No-op if `pending` is false.
    ///
    /// # Why every waker, and not one
    ///
    /// A thread parked by [`super::park::WakeSet`] is reached through
    /// its wake bit or the overflow stack, and a peer that cannot use
    /// a wake re-parks and stays findable. An async waiter has no such
    /// standing registration. Once it returns `Poll::Pending`, only
    /// its waker can poll it again, so a wake spent on the wrong
    /// waiter is lost for good.
    ///
    /// A registered waiter cannot always use the position that was just
    /// freed. An mpmc producer publishes into a per-producer batch, so
    /// the freed position can belong to a different producer than the
    /// one this scan reaches first. That producer re-registers and
    /// returns `Pending`, which consumes the wake, while the producer
    /// that owns the position stays parked. The consumers then find the
    /// ring empty and send no more wake events, and the ring stalls.
    ///
    /// A wake of every registered waiter removes this class of
    /// misdirected wake. A waiter that cannot progress re-registers,
    /// which costs one extra poll and never loses a wakeup.
    #[inline]
    pub fn wake_all(&self) {
        if !self.pending.load(Ordering::Relaxed) {
            return;
        }
        // Drain the caller's store buffer before scanning — the caller
        // issued a Release store on the ring (`ready`/`done`) then
        // called us; without this fence a peer that just registered and
        // re-checked could be missed. See `park::WakeSet::wake_one`.
        fence(Ordering::SeqCst);
        for slot in &*self.slots {
            slot.wake();
        }
        self.overflow.wake_all();
        // Don't clear pending: a waiter may register between the load and
        // the end of this scan. Leave pending=true so the next call
        // retries. The waiter always re-checks the ring after registering,
        // so a spurious pending=true costs one extra scan but never causes
        // a missed wake.
    }

    /// Wakes every registered waker. `n` is advisory only.
    ///
    /// Batched callers free `n` positions at once. This code cannot
    /// know which waiter owns each freed position, so it wakes all of
    /// them — see [`wake_all`](Self::wake_all).
    #[inline]
    pub fn wake_n(&self, _n: usize) {
        self.wake_all();
    }

    /// Wakes every registered waker. Used at close time.
    pub fn flush(&self) {
        fence(Ordering::SeqCst);
        for slot in &*self.slots {
            slot.wake();
        }
        self.overflow.wake_all();
    }
}

impl Default for WakerSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{WakerSet, WakerSlot};
    use crate::common::park_registry::ParkSlot;
    use crate::common::registration_id::{Registration, RegistrationIds};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Wake, Waker};

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

    #[test]
    fn an_empty_slot_displaces_nothing() {
        let slot = WakerSlot::new();
        let ids = RegistrationIds::new();
        assert!(slot
            .store(Registration::new(
                ids.mint(),
                Waker::from(CountingWaker::new())
            ))
            .is_none());
    }

    /// Re-arming under one id replaces the entry in place, so an
    /// executor polling a pending future repeatedly displaces nothing.
    /// This holds under Miri too: identity is the id, not a vtable
    /// pointer comparison that codegen has to deduplicate.
    #[test]
    fn re_registering_under_one_id_displaces_nothing() {
        let slot = WakerSlot::new();
        let ids = RegistrationIds::new();
        let id = ids.mint();
        let waker = Waker::from(CountingWaker::new());

        assert!(slot.store(Registration::new(id, waker.clone())).is_none());
        assert!(slot.store(Registration::new(id, waker)).is_none());
    }

    #[test]
    fn a_second_registration_is_handed_back_rather_than_dropped() {
        let slot = WakerSlot::new();
        let ids = RegistrationIds::new();
        let first = CountingWaker::new();
        let _ = slot.store(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&first)),
        ));

        let displaced = slot
            .store(Registration::new(
                ids.mint(),
                Waker::from(CountingWaker::new()),
            ))
            .expect("the first registration must be handed back");
        displaced.wake();
        assert_eq!(first.count(), 1);
    }

    /// Two registrations from one task share a waker, so only the id
    /// distinguishes them: the second must be reported as displacing
    /// the first rather than mistaken for a re-arm.
    #[test]
    fn two_registrations_from_one_task_displace_each_other() {
        let slot = WakerSlot::new();
        let ids = RegistrationIds::new();
        let task = CountingWaker::new();

        let _ = slot.store(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&task)),
        ));
        let displaced = slot.store(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&task)),
        ));

        assert!(
            displaced.is_some(),
            "a sibling's registration was silently dropped"
        );
    }

    /// A single future parking and being woken must never reach past
    /// the overflow's empty fast path. `pending` is never cleared, so
    /// every later wake on this ring calls into the overflow; only the
    /// null-head check keeps that free.
    #[test]
    fn a_slotted_waiter_never_touches_the_overflow_stack() {
        let set = WakerSet::new();
        let waker = Waker::from(CountingWaker::new());
        let slot = ParkSlot::from_exclusive_index(0);

        for _ in 0..64 {
            set.register(slot, &Context::from_waker(&waker));
            set.wake_all();
        }

        assert_eq!(set.overflow.take_count(), 0);
    }

    /// A cancelled future leaves its registration behind. Each
    /// cancellation from a slotless waiter pushes another entry that
    /// nothing removes until the next wake, so the stack grows with
    /// the number of cancellations rather than the number of waiters.
    #[test]
    fn cancelled_registrations_accumulate_on_the_overflow() {
        let set = WakerSet::new();
        let waker = Waker::from(CountingWaker::new());

        for _ in 0..1_000 {
            set.register(ParkSlot::Shared, &Context::from_waker(&waker));
        }

        assert_eq!(set.overflow.depth(), 1_000);
        set.wake_all();
        assert_eq!(set.overflow.depth(), 0);
    }

    /// Two futures sharing one endpoint's slot must both be woken: the
    /// displaced registration belongs to a future that is still parked.
    #[test]
    fn a_displaced_registration_is_still_woken() {
        let set = WakerSet::new();
        let first = CountingWaker::new();
        let second = CountingWaker::new();
        let slot = ParkSlot::from_exclusive_index(0);

        let first_waker = Waker::from(Arc::clone(&first));
        let second_waker = Waker::from(Arc::clone(&second));
        set.register(slot, &Context::from_waker(&first_waker));
        set.register(slot, &Context::from_waker(&second_waker));

        set.wake_all();
        assert_eq!(first.count(), 1);
        assert_eq!(second.count(), 1);
    }
}
