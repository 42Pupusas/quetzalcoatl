use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::task::Waker;

use super::park::PARK_SLOTS;
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

    /// Stores (or replaces) the waker.
    #[inline]
    pub fn store(&self, waker: &Waker) {
        let next = Box::into_raw(Box::new(waker.clone()));
        let prev = self.waker.swap(next, Ordering::AcqRel);
        if !prev.is_null() {
            // SAFETY: the swap removed `prev` from the slot, so this
            // thread is its sole owner and no peer can observe it again.
            drop(unsafe { Box::from_raw(prev) });
        }
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
}

impl WakerSet {
    pub fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            slots: AlignedBuf::new_with(PARK_SLOTS, WakerSlot::new),
        }
    }

    /// Registers `cx`'s waker in `slot`. Sets `pending = true`.
    ///
    /// Called by the future before returning `Poll::Pending`. The
    /// trailing `SeqCst` fence pairs with the fence in `wake_one`/
    /// `wake_n` so the waker's post-register re-check of the ring and a
    /// peer's wake-scan are totally ordered — without it the register/
    /// recheck and free/wake handshake can miss (Dekker), leaving the
    /// future parked with no further poll. Mirrors the blocking
    /// waiter's `fetch_or(SeqCst); fence(SeqCst)` in `park::WakeSet`.
    #[inline]
    pub fn register(&self, slot: usize, cx: &std::task::Context<'_>) {
        self.slots[slot & (PARK_SLOTS - 1)].store(cx.waker());
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
    }
}

impl Default for WakerSet {
    fn default() -> Self {
        Self::new()
    }
}
