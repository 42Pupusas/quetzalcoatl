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
pub struct WakerSlot {
    seq: AtomicUsize,
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
    }

    /// Takes and wakes the stored waker, if any.
    ///
    /// Returns `true` if a waker was present and fired, `false` if the
    /// slot was empty or the writer was active (caller may retry).
    ///
    /// # Correctness
    ///
    /// The reader reads `seq_before` (Acquire), then reads the waker pointer,
    /// then reads `seq_after` (Acquire). If seq changed the read was torn —
    /// the waker is left in place and we return false. The writer will
    /// re-register a fresh waker and the peer will wake it on the next
    /// opportunity. This is safe because:
    ///
    /// - We only call `wake` when we know the ring made progress; the
    ///   waiter will observe that progress on its next poll regardless.
    /// - The writer always re-checks the ring condition after storing
    ///   its waker, so it cannot miss the progress notification.
    #[inline]
    pub fn wake(&self) -> bool {
        let seq_before = self.seq.load(Ordering::Acquire);
        if seq_before & 1 != 0 {
            // Writer active — slot in flux.
            return false;
        }
        // SAFETY: seq is even (stable). Clone (don't take) so we never
        // mutate the slot — clearing it would race with a concurrent
        // store() and clobber a freshly-registered waker. Leaving the
        // waker in place is harmless: re-firing a stale waker is a no-op
        // (the task either re-polls or is gone), and the next register
        // overwrites it via store().
        let waker = unsafe { (*self.waker.get()).clone() };
        let seq_after = self.seq.load(Ordering::Acquire);
        if seq_after != seq_before {
            // Writer raced during our clone — bail; the writer's re-check
            // will catch the progress, or it'll register again.
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
