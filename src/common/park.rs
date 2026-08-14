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

/// Park-slot count.
///
/// Each waiter (producer or consumer) takes a stable slot at clone time;
/// bit `i` of the wake bitmap flags "slot `i` parked." 64 fits in a
/// single [`AtomicU64`]. Beyond 64 waiters of one kind, slots alias and
/// a wake on bit `i` rouses any waiter mapped there (benign false wake;
/// the woken waiter re-checks and re-parks).
pub const PARK_SLOTS: usize = 64;
pub const PARK_MASK: usize = PARK_SLOTS - 1;
/// 32-bit version of [`PARK_MASK`] for `u32::rotate_right` shift
/// counts. `PARK_SLOTS = 64` fits comfortably in `u32`.
#[allow(clippy::cast_possible_truncation)]
const PARK_MASK_U32: u32 = (PARK_SLOTS as u32) - 1;

/// `cas_backoff` failure-counter threshold past which the slow path
/// stops spinning and parks.
///
/// The schedule in [`crate::common::cas_backoff`] saturates at f=12
/// (64 pauses per call + sparse `yield_now`); waiting that long means
/// we've already burned ~tens of microseconds and a futex round-trip
/// (1–10μs) is amortized.
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
    /// Round-robin cursor: index *just after* the last slot we woke.
    /// `wake_one` rotates the bitmap by `cursor` before picking the
    /// trailing-zero bit, so a stuck low-bit waiter that re-parks
    /// immediately can't monopolize wake events and starve a
    /// higher-bit waiter who could actually progress. Without this,
    /// `bench_blocking_mpmc/block` deadlocked: producer at bit 2
    /// (waiting on a refill that couldn't succeed yet) consumed
    /// every consumer wake event, while the producer at bit 3
    /// (holding the position the refill needed) stayed parked
    /// forever.
    pub cursor: AtomicU64,
}

impl WakeSet {
    #[must_use]
    pub fn new() -> Self {
        Self {
            wake: AtomicU64::new(0),
            parkers: AlignedBuf::new_with(PARK_SLOTS, OnceLock::new),
            cursor: AtomicU64::new(0),
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
        // Drain the caller's store buffer before sampling the wake
        // bitmap. Callers (Producer::push, Consumer::pop, etc.) issue a
        // Release store on `ready`/`done` and then call wake_one. Without
        // this fence, x86 store-load reordering lets the SeqCst load on
        // `wake` below complete before the Release store reaches cache,
        // producing a Dekker-style missed wake: a peer that just parked
        // (fetch_or(wake, SeqCst); fence(SeqCst); recheck) sees the stale
        // value of `ready`/`done` and parks indefinitely while we read 0
        // from `wake` and skip the unpark. SeqCst load alone is insufficient
        // — it doesn't drain the store buffer; only an mfence (or RMW)
        // does.
        //
        // Callers that publish with a `SeqCst` *store* (an `xchg` on
        // x86, which drains the store buffer itself) already satisfy
        // this and must call [`wake_one_published`] instead.
        std::sync::atomic::fence(Ordering::SeqCst);
        self.wake_one_published();
    }

    /// [`wake_one`](Self::wake_one) without the leading `SeqCst` fence.
    ///
    /// For callers whose publish store is itself `SeqCst` (`xchg` on
    /// x86) and therefore already drains the store buffer, making the
    /// fence redundant. The bitmap load stays `SeqCst` so the pairing
    /// with the waiter's `fetch_or(bit, SeqCst)` is unchanged.
    ///
    /// Calling this after a mere `Release` publish reopens the
    /// Dekker-style missed wake described on [`wake_one`].
    #[inline]
    pub fn wake_one_published(&self) {
        // Round-robin: rotate the bitmap so the slot just past
        // `cursor` is the new lowest bit, and pick its trailing
        // zero. Without this, `trailing_zeros` always picks the
        // lowest set slot; a stuck low-bit waiter that re-parks
        // immediately consumes every wake event and starves a
        // higher-bit waiter who could actually progress. This is the
        // saturated mpmc/block deadlock pattern: producer at bit 2
        // parks on a refill that can't succeed yet (waiting on
        // round-N+1 release), producer at bit 3 has the unpublished
        // round-N position; every consumer pop wakes producer 2 (it
        // just re-parks), producer 3 stays parked indefinitely.
        let cursor = self.cursor.load(Ordering::Relaxed);
        loop {
            let ws = self.wake.load(Ordering::SeqCst);
            if ws == 0 {
                return;
            }
            // Rotate so the cursor's "next" slot becomes bit 0, then
            // un-rotate the chosen index back to its real slot.
            #[allow(clippy::cast_possible_truncation)]
            let shift = cursor as u32 & PARK_MASK_U32;
            let rotated = ws.rotate_right(shift);
            let rel_bit = rotated.trailing_zeros();
            let bit = (rel_bit + shift) & PARK_MASK_U32;
            let mask = 1u64 << bit;
            let prev = self.wake.fetch_and(!mask, Ordering::SeqCst);
            if prev & mask == 0 {
                // Concurrent wake_one beat us to this bit; pick another.
                continue;
            }
            // Advance cursor past this slot for the next caller.
            self.cursor
                .store(u64::from(bit).wrapping_add(1), Ordering::Relaxed);
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
        // See wake_one — drain the caller's store buffer before
        // sampling the bitmap so prior Release stores on
        // `ready`/`done` are globally visible.
        std::sync::atomic::fence(Ordering::SeqCst);
        let mut cursor = self.cursor.load(Ordering::Relaxed);
        for _ in 0..n {
            // Acquire (not Relaxed) — see wake_one for the rationale.
            let ws = self.wake.load(Ordering::SeqCst);
            if ws == 0 {
                self.cursor.store(cursor, Ordering::Relaxed);
                return;
            }
            // Round-robin via cursor — see wake_one.
            #[allow(clippy::cast_possible_truncation)]
            let shift = cursor as u32 & PARK_MASK_U32;
            let rotated = ws.rotate_right(shift);
            let rel_bit = rotated.trailing_zeros();
            let bit = (rel_bit + shift) & PARK_MASK_U32;
            let mask = 1u64 << bit;
            let prev = self.wake.fetch_and(!mask, Ordering::Relaxed);
            if prev & mask == 0 {
                // Lost the race on this bit — try again. Don't count
                // this iteration since we didn't wake anything.
                continue;
            }
            cursor = u64::from(bit).wrapping_add(1);
            if let Some(handle) = self.parkers[bit as usize].get() {
                handle.unpark();
            }
        }
        self.cursor.store(cursor, Ordering::Relaxed);
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
