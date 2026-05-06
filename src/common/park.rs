//! Futex-style park/unpark infrastructure shared by the blocking
//! variants of the ring buffers.
//!
//! Each ring that exposes blocking `push_block` / `pop_block` keeps a
//! [`WakeSet`] per side (producer / consumer). A waiter takes a stable
//! park slot, sets its bit on the wake bitmap, parks via
//! [`std::thread::park`], and the peer side wakes one slot's thread
//! with [`WakeSet::wake_one`] gated on the bitmap being non-zero (a
//! single `Relaxed` load on the fast path).
//!
//! The ordering invariant is closed by a `SeqCst` `fetch_or` on the
//! waiter's bit *before* the final emptiness/full-ness re-check, paired
//! with the peer's `Relaxed` load of the same bitmap *after* the
//! release that would let the waiter make progress. Either the waiter
//! sees progress in its re-check, or the peer sees the bit and unparks
//! it. Close paths additionally `flush()` both wake sets so a parked
//! waiter never observes "closed but still parked."

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::thread::Thread;

use super::AlignedBuf;

/// Park-slot count. Each waiter (producer or consumer) takes a stable
/// slot at clone time; bit `i` of the wake bitmap flags "slot `i`
/// parked." 64 fits in a single [`AtomicU64`]. Beyond 64 waiters of
/// one kind, slots alias and a wake on bit `i` rouses any waiter
/// mapped there (benign false wake; the woken waiter re-checks and
/// re-parks).
pub const PARK_SLOTS: usize = 64;
pub const PARK_MASK: usize = PARK_SLOTS - 1;

/// `cas_backoff` failure-counter threshold past which the slow path
/// stops spinning and parks. The schedule in
/// [`crate::common::cas_backoff`] saturates at f=12 (64 pauses per
/// call + sparse `yield_now`); waiting that long means we've already
/// burned ~tens of microseconds and a futex round-trip (1–10μs) is
/// amortized.
pub const BACKOFF_PARK_THRESHOLD: u32 = 12;

/// One side's park state — a 64-bit wake bitmap plus a parker table
/// of `Thread` handles indexed by park slot. Producers and consumers
/// have independent [`WakeSet`]s.
pub struct WakeSet {
    /// Futex-style wake bitmap. Bit `i` set ↔ a waiter in park slot
    /// `i` is parked. Peers read this `Relaxed` after each release
    /// and wake one parked waiter if non-zero.
    pub wake: AtomicU64,
    /// `Thread` handle table. Idempotently set by the first waiter
    /// that parks at each slot; readers (peers issuing wakes)
    /// obtain a `&Thread` via `OnceLock::get`.
    pub parkers: AlignedBuf<OnceLock<Thread>>,
}

impl WakeSet {
    #[must_use]
    pub fn new() -> Self {
        Self {
            wake: AtomicU64::new(0),
            parkers: AlignedBuf::new_with(PARK_SLOTS, OnceLock::new),
        }
    }

    /// Wakes one peer parked on the bitmap.
    ///
    /// Concurrent peers racing on the same set bit: only one's
    /// `fetch_and` actually clears it; the loser observes the bit
    /// already clear and skips the unpark.
    ///
    /// The fast-path load uses `Acquire` (not `Relaxed`) so it
    /// synchronizes with the peer's `fetch_or(bit, SeqCst)`. With
    /// `Relaxed` the load could miss a freshly-set bit, leaving the
    /// peer permanently parked when no further progress events
    /// follow — surfaces under high-volume bench loops at small
    /// capacities (cap=16 with 2 producers + 2 consumers reliably
    /// hits this).
    #[inline]
    pub fn wake_one(&self) {
        // Loop instead of try-once so we don't lose a wake when our
        // first picked bit was already cleared by a concurrent
        // wake_one from a peer producer/consumer. Without the loop
        // (and especially when wake_one is called by a contending pair
        // of producers/consumers), one wake event is silently dropped
        // per simultaneous call — a parked counterpart at a higher bit
        // is starved until the next progress event, which under
        // saturated bench loops doesn't come reliably and reproducibly
        // deadlocks at high iter counts.
        loop {
            let ws = self.wake.load(Ordering::SeqCst);
            if ws == 0 {
                return;
            }
            let bit = ws.trailing_zeros();
            let mask = 1u64 << bit;
            let prev = self.wake.fetch_and(!mask, Ordering::SeqCst);
            if prev & mask == 0 {
                // Concurrent wake_one beat us to this bit; pick another.
                continue;
            }
            if let Some(handle) = self.parkers[bit as usize].get() {
                handle.unpark();
            }
            return;
        }
    }

    /// Wakes up to `n` parked waiters. Used by drain-style operations
    /// that free `n` slots in one batch and want to release roughly
    /// `n` parkers at once. Caps at the number of currently parked
    /// waiters; extra wakes (when `n` exceeds parkers) are no-ops.
    ///
    /// Each iteration is `wake_one`'s logic: pick the lowest set bit,
    /// CAS it clear, unpark the slot. Implemented as a loop rather
    /// than swap-bits-once because we want to wake *exactly* `n` if
    /// available, not all of them.
    #[inline]
    pub fn wake_n(&self, n: usize) {
        for _ in 0..n {
            // Acquire (not Relaxed) — see wake_one for the rationale.
            let ws = self.wake.load(Ordering::SeqCst);
            if ws == 0 {
                return;
            }
            let bit = ws.trailing_zeros();
            let mask = 1u64 << bit;
            let prev = self.wake.fetch_and(!mask, Ordering::Relaxed);
            if prev & mask == 0 {
                // Lost the race on this bit — try again. Don't count
                // this iteration since we didn't wake anything.
                continue;
            }
            if let Some(handle) = self.parkers[bit as usize].get() {
                handle.unpark();
            }
        }
    }

    /// Wakes every waiter parked on the bitmap, swapping it to zero
    /// in the process. Cold-path helper for close-time draining.
    pub fn flush(&self) {
        let mut bits = self.wake.swap(0, Ordering::AcqRel);
        while bits != 0 {
            let b = bits.trailing_zeros() as usize;
            if let Some(handle) = self.parkers[b].get() {
                handle.unpark();
            }
            bits &= bits - 1;
        }
    }

    /// Idempotently installs the current thread's `Thread` handle at
    /// `slot`. Subsequent calls observe the slot already set and
    /// no-op. Slot aliasing (>`PARK_SLOTS` waiters) means the first
    /// installer wins; later wakes on that bit may unpark the wrong
    /// waiter (benign — it just re-checks and re-parks).
    #[inline]
    pub fn ensure_handle_installed(&self, slot: usize) {
        let _ = self.parkers[slot].set(std::thread::current());
    }
}

impl Default for WakeSet {
    fn default() -> Self {
        Self::new()
    }
}
