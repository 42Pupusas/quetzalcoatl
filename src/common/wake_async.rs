use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Waker;

use super::park::PARK_SLOTS;
use super::AlignedBuf;

/// Seqlock-protected waker slot (single-writer, single-reader).
///
/// The writer is the producer or consumer that owns the slot (calling
/// `store` before returning `Poll::Pending`). The reader is the peer
/// side calling `wake`. Even sequence = stable; odd = write in progress.
///
/// `fresh` distinguishes "waker present and not yet fired" from "waker
/// present but already fired" without mutating the slot from the reader
/// side. `store` sets it true (a fresh waker needs waking); `wake` flips
/// it false via CAS so subsequent reader scans skip a stale slot —
/// otherwise a fast wake-loop would re-fire the same slot's waker over
/// and over while peer slots starve.
pub struct WakerSlot {
    seq: AtomicUsize,
    fresh: AtomicBool,
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: `waker` is accessed only under the seqlock protocol.
// `Waker` is `Send + Sync`.
unsafe impl Send for WakerSlot {}
unsafe impl Sync for WakerSlot {}

impl WakerSlot {
    pub const fn new() -> Self {
        Self {
            seq: AtomicUsize::new(0),
            fresh: AtomicBool::new(false),
            waker: UnsafeCell::new(None),
        }
    }

    /// Stores (or replaces) the waker. Only the slot's designated writer
    /// may call this.
    #[inline]
    pub fn store(&self, waker: &Waker) {
        let seq = self.seq.load(Ordering::Relaxed);
        // Mark write-in-progress (odd).
        self.seq.store(seq | 1, Ordering::Relaxed);
        // SAFETY: we hold the write lock (seq is odd); the reader bails
        // if it sees an odd seq.
        unsafe { *self.waker.get() = Some(waker.clone()) };
        // Publish: advance to the next even value. Release so the reader's
        // Acquire on seq_before sees the completed write.
        self.seq.store(seq.wrapping_add(2), Ordering::Release);
        // Mark fresh AFTER the seq publish so a reader that sees fresh=true
        // is guaranteed to also see the new waker contents.
        self.fresh.store(true, Ordering::Release);
    }

    /// Wakes the stored waker if it's fresh. Returns `true` only when
    /// this call atomically claimed the slot's `fresh` flag and fired.
    ///
    /// # Correctness
    ///
    /// 1. Acquire-load `fresh`. If false the slot was empty or already
    ///    fired by a previous wake — caller must scan the next slot.
    /// 2. Read `seq_before` (Acquire). Bail if odd (writer active).
    /// 3. Clone the waker (no mutation — clearing the slot here would
    ///    race with a concurrent `store` and clobber a fresh waker).
    /// 4. Verify `seq_after == seq_before`; otherwise the writer raced
    ///    during step 3 and we bail (the writer's recheck catches the
    ///    progress signal we just emitted).
    /// 5. Compare-and-swap `fresh` from true to false. If we lose the
    ///    CAS another wake fired first; bail.
    /// 6. Wake the cloned waker.
    ///
    /// Steps 5 and 6 are the key bit: by claiming `fresh` atomically
    /// before firing, we guarantee at most one wake per `store`, so
    /// repeated `wake_one` scans don't re-fire the same slot while
    /// other slots starve.
    #[inline]
    pub fn wake(&self) -> bool {
        if !self.fresh.load(Ordering::Acquire) {
            return false;
        }
        let seq_before = self.seq.load(Ordering::Acquire);
        if seq_before & 1 != 0 {
            return false;
        }
        // SAFETY: seq even and fresh — the waker is fully published.
        let waker = unsafe { (*self.waker.get()).clone() };
        let seq_after = self.seq.load(Ordering::Acquire);
        if seq_after != seq_before {
            return false;
        }
        // Atomically claim the wake. If another reader got here first
        // (only possible across distinct WakerSet calls), let it fire.
        if self
            .fresh
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        if let Some(w) = waker {
            w.wake();
            return true;
        }
        false
    }
}

impl Default for WakerSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WakerSlot {
    fn drop(&mut self) {
        drop(self.waker.get_mut().take());
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
    /// Called by the future before returning `Poll::Pending`.
    #[inline]
    pub fn register(&self, slot: usize, cx: &std::task::Context<'_>) {
        self.slots[slot & (PARK_SLOTS - 1)].store(cx.waker());
        self.pending.store(true, Ordering::Release);
    }

    /// Wakes one registered waker. No-op if `pending` is false.
    #[inline]
    pub fn wake_one(&self) {
        if !self.pending.load(Ordering::Relaxed) {
            return;
        }
        for slot in &*self.slots {
            if slot.wake() {
                return;
            }
        }
        // Don't clear pending: wake() returning false may mean the seqlock
        // raced and the waker is still registered. Leave pending=true so the
        // next call retries. The waiter always re-checks the ring after
        // registering, so a spurious pending=true costs one extra scan but
        // never causes a missed wake.
    }

    /// Wakes up to `n` registered wakers.
    #[allow(dead_code)]
    #[inline]
    pub fn wake_n(&self, mut n: usize) {
        if !self.pending.load(Ordering::Relaxed) {
            return;
        }
        for slot in &*self.slots {
            if n == 0 {
                return;
            }
            if slot.wake() {
                n -= 1;
            }
        }
        // Same reasoning as wake_one: don't clear pending.
    }

    /// Wakes every registered waker. Used at close time.
    pub fn flush(&self) {
        self.pending.store(false, Ordering::Relaxed);
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
