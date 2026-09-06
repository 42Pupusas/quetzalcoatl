use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::task::Waker;

use super::park::PARK_SLOTS;
use super::park_registry::ParkSlot;
use super::waker_overflow::WakerOverflow;
use super::AlignedBuf;

/// A single registered [`Waker`], owned through an [`AtomicPtr`].
///
/// The writer is the producer or consumer that owns the slot (calling
/// `store` before returning `Poll::Pending`). The reader is the peer
/// side calling `wake`. Both sides mutate the slot with a single `swap`,
/// so the thread that swaps a pointer *out* is its unique owner and is
/// the only one that may drop it. A null pointer means "no waker
/// registered", which also makes a fired waker unreachable to later
/// scans — without that, a fast wake-loop would re-fire one slot while
/// its peers starve.
///
/// A seqlock cannot be used here: a `Waker` is not trivially copyable,
/// so a reader that observes a torn write would call through a vtable
/// pointer it must never dereference.
pub struct WakerSlot {
    waker: AtomicPtr<Waker>,
}

impl WakerSlot {
    pub const fn new() -> Self {
        Self {
            waker: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Stores (or replaces) the waker, returning any waker it displaced.
    ///
    /// A displaced waker belongs to a future that is still parked, so
    /// the caller must keep it reachable; dropping it strands that
    /// future. Returns `None` when the replaced entry was this same
    /// waker, which is the common case of one future re-registering on
    /// each poll.
    #[inline]
    #[must_use]
    pub fn store(&self, waker: &Waker) -> Option<Waker> {
        let next = Box::into_raw(Box::new(waker.clone()));
        let prev = self.waker.swap(next, Ordering::AcqRel);
        if prev.is_null() {
            return None;
        }
        // SAFETY: the swap removed `prev` from the slot, so this
        // thread is its sole owner and no peer can observe it again.
        let prev = unsafe { Box::from_raw(prev) };
        if prev.will_wake(waker) {
            return None;
        }
        Some(*prev)
    }

    /// Wakes the registered waker. Returns `true` only when this call
    /// claimed the waker and fired it.
    #[inline]
    pub fn wake(&self) -> bool {
        if self.waker.load(Ordering::Relaxed).is_null() {
            return false;
        }
        let claimed = self.waker.swap(ptr::null_mut(), Ordering::AcqRel);
        if claimed.is_null() {
            return false;
        }
        // SAFETY: the swap removed `claimed` from the slot, so this
        // thread is its sole owner and no peer can observe it again.
        let waker = unsafe { Box::from_raw(claimed) };
        (*waker).wake();
        true
    }
}

impl Default for WakerSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WakerSlot {
    fn drop(&mut self) {
        let stored = *self.waker.get_mut();
        if !stored.is_null() {
            // SAFETY: `&mut self` proves no peer holds a reference, and
            // the pointer was last published by `store`.
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
    /// Wakers for waiters holding no park slot. An async waiter has no
    /// timeout to rescue itself, so it must be recorded somewhere even
    /// when every slot is leased. Lock-free, and an empty one costs a
    /// single `Relaxed` load on the wake path.
    pub overflow: WakerOverflow,
}

impl WakerSet {
    pub fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            slots: AlignedBuf::new_with(PARK_SLOTS, WakerSlot::new),
            overflow: WakerOverflow::new(),
        }
    }

    /// Registers `cx`'s waker for `slot`. Sets `pending = true`.
    ///
    /// A slot is owned by an endpoint, not by a future, and the async
    /// methods take `&self`: two futures from one handle share a slot,
    /// and the second registration displaces the first. The displaced
    /// future is still parked, so its waker moves to the overflow list
    /// rather than being dropped.
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
    pub fn register(&self, slot: ParkSlot, cx: &std::task::Context<'_>) {
        match slot.index() {
            Some(index) => {
                if let Some(displaced) = self.slots[index].store(cx.waker()) {
                    self.overflow.register_owned(displaced);
                }
            }
            None => self.overflow.register(cx.waker()),
        }
        self.pending.store(true, Ordering::Release);
        std::sync::atomic::fence(Ordering::SeqCst);
    }

    /// Wakes every registered waker. No-op if `pending` is false.
    ///
    /// # Why every waker, and not one
    ///
    /// A thread parked by [`super::park::WakeSet`] re-checks the ring
    /// on a 1 ms `park_timeout`, so a wake sent to a waiter that cannot
    /// progress costs latency only. An async waiter has no such
    /// backstop. Once it returns `Poll::Pending`, only its waker can
    /// poll it again. A wake spent on the wrong waiter is lost for good.
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
        std::sync::atomic::fence(Ordering::SeqCst);
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
        std::sync::atomic::fence(Ordering::SeqCst);
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
        assert!(slot.store(&Waker::from(CountingWaker::new())).is_none());
    }

    #[test]
    fn re_registering_one_waker_displaces_nothing() {
        let slot = WakerSlot::new();
        let waker = Waker::from(CountingWaker::new());
        assert!(slot.store(&waker).is_none());
        assert!(slot.store(&waker).is_none());
    }

    #[test]
    fn a_second_waker_is_handed_back_rather_than_dropped() {
        let slot = WakerSlot::new();
        let first = CountingWaker::new();
        let _ = slot.store(&Waker::from(Arc::clone(&first)));

        let displaced = slot
            .store(&Waker::from(CountingWaker::new()))
            .expect("the first waker must be handed back");
        displaced.wake();
        assert_eq!(first.count(), 1);
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
