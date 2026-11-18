//! Park state for a side that has exactly one waiter.
//!
//! [`WakeSet`] is the multi-waiter form: a bitmap of park slots and a
//! table of handles, so a peer can wake one waiter of many. The
//! single-producer and single-consumer sides need none of that — there
//! is one waiter, so a single flag answers "is anyone parked?" — and
//! each of them had grown its own copy of the same two fields, a
//! [`ThreadParker`] and a `parked` boolean, with the protocol spelled
//! out again at every site that touched them.
//!
//! [`SoleParker`] is that pair with the protocol attached, and it takes
//! the same `_park` field name its multi-waiter twin uses, so every
//! side of every ring is now `producer_park` / `consumer_park` and the
//! type says how many waiters the side admits.
//!
//! # The handshake
//!
//! Parking and waking is a Dekker handshake across two locations. The
//! waiter publishes "I am parked" and then re-checks the ring; the peer
//! publishes progress (a slot, a close) and then loads "is anyone
//! parked?". Each side stores to one location and loads the other, so
//! the pair is ordered only under a total order over both: the waiter's
//! store is `SeqCst` and the peer's load in [`wake`](SoleParker::wake)
//! is `SeqCst`. Weaken either and both sides can read the stale value —
//! the peer sees nobody parked and issues no wake, the waiter sees no
//! progress and parks with nothing left to wake it.
//!
//! # Why `arm` ends in a fence
//!
//! The store alone is not enough. A waiter's re-check reads the ring
//! cursors with `Acquire`, which the `SeqCst` store does not order
//! against, so the peer can still read `parked == false` while the
//! waiter reads a stale full (or empty) ring. Every site that armed
//! this pair therefore issued a `SeqCst` fence immediately afterwards,
//! and a site that forgot would deadlock only under load on a weak
//! architecture. The fence is part of arming, so it lives in
//! [`arm`](SoleParker::arm) rather than in the four callers who must
//! otherwise remember it.
//!
//! # Why the clears are `Relaxed`
//!
//! Clearing is never load-bearing. The waiter clears its own flag after
//! it wakes, and the peer clears it when claiming the wake; a clear
//! that is late or lost costs at most one redundant unpark of a waiter
//! that re-checks and carries on. Only the *set* has to be ordered,
//! because only the set can be missed.
//!
//! [`WakeSet`]: super::park::WakeSet

use super::thread_parker::ThreadParker;
use super::CachePadded;
use std::sync::atomic::{fence, AtomicBool, Ordering};

/// Park state for a side with exactly one waiter: a re-armable thread
/// handle plus the flag that says whether it is in use.
///
/// Parking routes through the [`atomics`](super::atomics) shim, so
/// loom's mocked `park`/`unpark` apply under the model.
pub struct SoleParker {
    parker: ThreadParker,
    /// `true` ↔ the sole waiter is parked, or is committed to parking.
    /// Kept on its own cache line: peers load it after every release,
    /// so it must not share with the cursors they just wrote.
    parked: CachePadded<AtomicBool>,
}

impl SoleParker {
    // Loom's atomics are not const-constructible, so the loom twin
    // takes the weaker form; std callers keep const use.
    #[cfg(not(loom))]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            parker: ThreadParker::new(),
            parked: CachePadded(AtomicBool::new(false)),
        }
    }

    #[cfg(loom)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            parker: ThreadParker::new(),
            parked: CachePadded(AtomicBool::new(false)),
        }
    }

    /// Publishes the calling thread as the parked waiter and commits to
    /// parking, then fences.
    ///
    /// Call this *before* the final re-check of the ring, never after:
    /// the ordering only excludes the lost wake if the peer can see the
    /// flag by the time the waiter looks for the progress it would have
    /// missed. On return, the caller must either park or
    /// [`disarm`](Self::disarm).
    #[inline]
    pub fn arm(&self) {
        self.parker.arm();
        self.parked.0.store(true, Ordering::SeqCst);
        // Orders the store above against the caller's Acquire re-check
        // of the ring; see the module docs.
        fence(Ordering::SeqCst);
    }

    /// Withdraws the commitment to park, after a re-check found
    /// progress or after waking.
    #[inline]
    pub fn disarm(&self) {
        self.parked.0.store(false, Ordering::Relaxed);
    }

    /// Wakes the waiter if one is parked, for a caller whose publish
    /// store was `Release`.
    ///
    /// The leading fence drains the caller's store buffer before the
    /// bitmap-equivalent load below. Without it, x86 store-load
    /// reordering lets the `SeqCst` load of `parked` complete before
    /// the `Release` store reaches cache: the waiter reads the stale
    /// ring and parks, this caller reads `parked == false` and skips
    /// the unpark, and both sleep. A `SeqCst` load alone does not fix
    /// this — it does not drain the store buffer; only an `mfence` (or
    /// an RMW) does.
    #[inline]
    pub fn wake(&self) {
        fence(Ordering::SeqCst);
        self.wake_published();
    }

    /// [`wake`](Self::wake) without the leading fence, for a caller
    /// whose publish store is itself `SeqCst` (an `xchg` on x86, which
    /// drains the store buffer on its own) and for whom the fence is
    /// therefore redundant.
    ///
    /// Calling this after a mere `Release` publish reopens the missed
    /// wake described on [`wake`](Self::wake).
    #[inline]
    pub fn wake_published(&self) {
        if !self.parked.0.load(Ordering::SeqCst) {
            return;
        }
        self.parked.0.store(false, Ordering::Relaxed);
        self.parker.wake();
    }

    /// Whether the waiter is parked or committed to parking.
    ///
    /// Test-only, like the [`ParkProbe`] that polls it. This is the
    /// latching observable such a probe needs: it is set before the
    /// waiter's final re-checks and stays set while it sleeps. The
    /// armed handle is not a substitute — a wake claims it out of its
    /// slot, so it vanishes the moment the peer acts.
    ///
    /// [`ParkProbe`]: super::park_probe::ParkProbe
    #[cfg(test)]
    #[must_use]
    #[inline]
    pub fn is_parked(&self) -> bool {
        self.parked.0.load(Ordering::SeqCst)
    }
}

impl Default for SoleParker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
