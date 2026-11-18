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

use super::atomics::{fence, AtomicU64};
use std::sync::atomic::Ordering;

use super::park_gate::ParkGate;
use super::park_overflow::ParkOverflow;
use super::replay_point::ReplayPoint;
use super::thread_parker::ThreadParker;
use super::wake_interest::WakeInterest;
use super::AlignedBuf;

/// Re-exported so external callers can name the argument type of
/// [`WakeSet::arm`], [`WakeSet::disarm`], [`WakeSet::arm_handle`],
/// [`WakeSet::is_armed`], and [`WakeSet::others_parked`] — all public
/// methods on a public struct whose parameter type must itself be
/// reachable, or no downstream crate can call them at all.
///
/// [`ParkRegistry`] is re-exported alongside it: a caller that leases
/// its own park slot by hand (e.g. a monotonic counter masked to
/// `PARK_MASK` on every clone) reintroduces the exact aliasing hazard
/// this module exists to close — two live waiters sharing one bit lose
/// a wakeup rather than merely receiving a spurious one. `ParkRegistry`
/// is the only correct way to hand out slots that never alias while
/// live, and every ring in this crate already uses it internally.
pub use super::park_registry::{ParkRegistry, ParkSlot};

/// Park-slot count.
///
/// Each waiter (producer or consumer) leases a stable slot at clone
/// time; bit `i` of the wake bitmap flags "slot `i` parked." 64 fits in
/// a single [`AtomicU64`].
///
/// Beyond 64 live waiters of one kind the slots do **not** alias: a
/// lease is refused and the waiter becomes
/// [`ParkSlot::Shared`](super::park_registry::ParkSlot::Shared), which
/// publishes no bit and is reached through the overflow stack every
/// wake path drains. Aliasing would lose wakeups rather than cause
/// merely spurious ones — two waiters on one bit means a wake clears it,
/// rouses one, and leaves the other parked with nothing recording that
/// it waits. See [`ParkRegistry`](super::park_registry::ParkRegistry).
pub const PARK_SLOTS: usize = 64;
pub const PARK_MASK: usize = PARK_SLOTS - 1;
/// 32-bit version of [`PARK_MASK`] for `u32::rotate_right` shift
/// counts. `PARK_SLOTS = 64` fits comfortably in `u32`.
#[allow(clippy::cast_possible_truncation)]
const PARK_MASK_U32: u32 = (PARK_SLOTS as u32) - 1;

/// One side's park state — a 64-bit wake bitmap plus a parker table
/// of `Thread` handles indexed by park slot. Producers and consumers
/// have independent [`WakeSet`]s.
pub struct WakeSet {
    /// Futex-style wake bitmap. Bit `i` set ↔ a waiter in park slot
    /// `i` is parked. Peers read this `Relaxed` after each release
    /// and wake one parked waiter if non-zero.
    pub wake: AtomicU64,
    /// Park handle table. Re-armed by each waiter that parks at a
    /// slot, so a waiter that migrates between threads is woken on
    /// whichever thread is parked now; peers issuing wakes claim the
    /// handle out of the slot.
    pub parkers: AlignedBuf<ThreadParker>,
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
    /// Waiters that could not lease a park slot. The bitmap has room
    /// for exactly [`PARK_SLOTS`], so past that a waiter is
    /// [`ParkSlot::Shared`] and has no bit to publish; it registers
    /// here instead and every wake path drains the stack. Empty in
    /// all but heavily over-subscribed rings, where it costs one
    /// `SeqCst` load per wake.
    pub overflow: ParkOverflow,
    /// Test stops inside the handshake; empty outside tests.
    pub gate: ParkGate,
}

impl WakeSet {
    #[must_use]
    pub fn new() -> Self {
        Self {
            wake: AtomicU64::new(0),
            parkers: AlignedBuf::new_with(PARK_SLOTS, ThreadParker::new),
            cursor: AtomicU64::new(0),
            overflow: ParkOverflow::new(),
            gate: ParkGate::new(),
        }
    }

    /// Wakes one peer parked on the bitmap.
    ///
    /// Guarantees an unpark was *delivered*, not merely that a bit
    /// was retired. A set bit does not imply a waiter is reachable:
    /// `arm` publishes the handle then the bit, and a waker clears
    /// the bit then claims the handle, so a bit can outlive the
    /// handle it advertised. Clearing such a bit wakes nobody, so
    /// this keeps searching rather than counting it as the wake.
    ///
    /// Concurrent peers racing on the same set bit: only one's
    /// `fetch_and` actually clears it; the loser observes the bit
    /// already clear and moves to another.
    ///
    /// The fast-path load is `SeqCst` so it participates in the total
    /// order with the peer's `fetch_or(bit, SeqCst)` in
    /// [`arm`](Self::arm). With `Relaxed` the load could miss a
    /// freshly-set bit, leaving the peer permanently parked when no
    /// further progress events follow — surfaces under high-volume
    /// bench loops at small capacities (cap=16 with 2 producers + 2
    /// consumers reliably hits this).
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
        fence(Ordering::SeqCst);
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
        self.drain_overflow(1);
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
            self.gate.reach(ReplayPoint::Scan { cursor, count: 1 });
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
            self.gate.reach(ReplayPoint::Clear {
                slot: bit,
                count: 1,
                cursor,
            });
            let prev = self.wake.fetch_and(!mask, Ordering::SeqCst);
            if prev & mask == 0 {
                // Concurrent wake_one beat us to this bit; pick another.
                continue;
            }
            // Advance cursor past this slot for the next caller.
            self.cursor
                .store(u64::from(bit).wrapping_add(1), Ordering::Relaxed);
            self.gate.reach(ReplayPoint::Claim {
                slot: bit,
                count: 1,
                cursor,
            });
            if self.parkers[bit as usize].wake() {
                return;
            }
            // The bit was set but its handle was already claimed, so
            // this cleared a bit without delivering anything. Keep
            // looking: retiring the stale bit is not the wake the
            // caller asked for. See `a_wake_is_not_consumed_by_a_slot
            // _whose_handle_is_already_claimed`.
        }
    }

    /// Wakes the parked waiter with the strongest claim on this
    /// release, asking `interest` what each one can do with it.
    ///
    /// For releases that only a specific waiter can use. Round-robin
    /// serves one waiter per release, so a release delivered to a
    /// waiter that cannot use it is spent: the woken one re-parks and
    /// the one it was for is not woken again until something else
    /// releases. When nothing else does, that is a deadlock with a
    /// free slot in the ring.
    ///
    /// The predicate is read under no lock, so a waiter can announce
    /// a want, be passed over here, and then park — but its own
    /// re-check after arming sees the release, which is the same
    /// handshake that protects the untargeted path.
    ///
    /// Two passes, strongest claim first: the waiter that reserved the
    /// position, then any waiter that can take whatever frees. A
    /// waiter that declines is never woken, because waking it delivers
    /// nothing and spends the release.
    ///
    /// The blind fallback this replaces treated "declines" and "wants
    /// anything" alike, so a release nobody reserved could go to a
    /// waiter pinned elsewhere while one that could use it stayed
    /// parked. If every parked waiter declines, nothing is woken:
    /// there is nobody this release can help, and retiring a bit to
    /// prove it only strands the waiter behind it.
    ///
    /// Ordering matches [`wake_one`](Self::wake_one) — the leading
    /// fence drains the caller's store buffer so a waiter that armed
    /// before the release is visible here, and one that armed after
    /// catches the release in its own post-arm re-check.
    #[inline]
    pub fn wake_one_interested(&self, interest: impl Fn(usize) -> WakeInterest) {
        fence(Ordering::SeqCst);
        self.drain_overflow(1);
        self.reach_scan();
        for rank in [WakeInterest::Reserved, WakeInterest::Any] {
            if self.wake_one_ranked(rank, &interest) {
                return;
            }
        }
    }

    /// One pass of [`wake_one_interested`](Self::wake_one_interested),
    /// waking a waiter whose interest is exactly `rank`. Returns
    /// whether an unpark was delivered.
    ///
    /// Starts at the round-robin cursor so that equally-ranked waiters
    /// take turns, rather than the lowest bit absorbing every release.
    ///
    /// The replay stops before a reload only: a pass that finds nothing
    /// hands straight on to the next, so the loads of both passes count
    /// as one scan.
    #[inline]
    fn wake_one_ranked(
        &self,
        rank: WakeInterest,
        interest: &impl Fn(usize) -> WakeInterest,
    ) -> bool {
        let mut reload = false;
        loop {
            if std::mem::replace(&mut reload, true) {
                self.reach_scan();
            }
            let ws = self.wake.load(Ordering::SeqCst);
            if ws == 0 {
                return false;
            }
            #[allow(clippy::cast_possible_truncation)]
            let shift = self.cursor.load(Ordering::Relaxed) as u32 & PARK_MASK_U32;
            let Some(bit) = Self::next_matching(ws, shift, rank, interest) else {
                return false;
            };
            let mask = 1u64 << bit;
            let cursor = self.cursor.load(Ordering::Relaxed);
            self.gate.reach(ReplayPoint::Clear {
                slot: bit,
                count: 1,
                cursor,
            });
            let prev = self.wake.fetch_and(!mask, Ordering::SeqCst);
            if prev & mask == 0 {
                continue;
            }
            self.cursor
                .store(u64::from(bit).wrapping_add(1), Ordering::Relaxed);
            self.gate.reach(ReplayPoint::Claim {
                slot: bit,
                count: 1,
                cursor,
            });
            if self.parkers[bit as usize].wake() {
                return true;
            }
        }
    }

    /// Stops a single-wake loop before it loads the bitmap.
    fn reach_scan(&self) {
        self.gate.reach(ReplayPoint::Scan {
            cursor: self.cursor.load(Ordering::Relaxed),
            count: 1,
        });
    }

    /// The first set bit at or after the cursor whose interest is
    /// `rank`, scanning in round-robin order.
    #[inline]
    fn next_matching(
        ws: u64,
        shift: u32,
        rank: WakeInterest,
        interest: &impl Fn(usize) -> WakeInterest,
    ) -> Option<u32> {
        let mut rotated = ws.rotate_right(shift);
        while rotated != 0 {
            let rel_bit = rotated.trailing_zeros();
            rotated &= !(1u64 << rel_bit);
            let bit = (rel_bit + shift) & PARK_MASK_U32;
            if interest(bit as usize) == rank {
                return Some(bit);
            }
        }
        None
    }

    /// Wakes up to `n` parked waiters. Used by drain-style operations
    /// that free `n` slots in one batch and want to release roughly
    /// `n` parkers at once. Caps at the number of currently parked
    /// waiters; extra wakes (when `n` exceeds parkers) are no-ops.
    ///
    /// Each iteration is `wake_one`'s logic: pick the next set bit in
    /// round-robin order, clear it, unpark the slot. Implemented as a
    /// loop rather than swap-bits-once because we want to wake
    /// *exactly* `n` if available, not all of them.
    ///
    /// `n` counts unparks delivered, not bits retired — a bit whose
    /// handle was already claimed advertises a waiter that is not
    /// there, and stopping on it would strand a real one. See
    /// [`wake_one`](Self::wake_one).
    #[inline]
    pub fn wake_n(&self, n: usize) {
        // See wake_one — drain the caller's store buffer before
        // sampling the bitmap so prior Release stores on
        // `ready`/`done` are globally visible.
        fence(Ordering::SeqCst);
        self.drain_overflow(n);
        let mut cursor = self.cursor.load(Ordering::Relaxed);
        let mut woken = 0usize;
        while woken < n {
            self.gate.reach(ReplayPoint::Scan {
                cursor,
                count: n - woken,
            });
            let ws = self.wake.load(Ordering::SeqCst);
            if ws == 0 {
                self.cursor.store(cursor, Ordering::Relaxed);
                return;
            }
            #[allow(clippy::cast_possible_truncation)]
            let shift = cursor as u32 & PARK_MASK_U32;
            let rotated = ws.rotate_right(shift);
            let rel_bit = rotated.trailing_zeros();
            let bit = (rel_bit + shift) & PARK_MASK_U32;
            let mask = 1u64 << bit;
            let next = u64::from(bit).wrapping_add(1);
            self.gate.reach(ReplayPoint::Clear {
                slot: bit,
                count: n - woken,
                cursor: next,
            });
            let prev = self.wake.fetch_and(!mask, Ordering::SeqCst);
            cursor = next;
            if prev & mask == 0 {
                // Lost the race on this bit, or it was stale — either
                // way nothing was delivered, so this does not count
                // against `n`. `woken` is what advances the count.
                continue;
            }
            self.gate.reach(ReplayPoint::Claim {
                slot: bit,
                count: n - woken,
                cursor,
            });
            if self.parkers[bit as usize].wake() {
                woken += 1;
            }
        }
        self.cursor.store(cursor, Ordering::Relaxed);
    }

    /// Wakes every waiter parked on the bitmap, swapping it to zero
    /// in the process. Cold-path helper for close-time draining.
    pub fn flush(&self) {
        self.drain_overflow(0);
        self.gate.reach(ReplayPoint::Flush);
        let mut bits = self.wake.swap(0, Ordering::AcqRel);
        while bits != 0 {
            self.gate.reach(ReplayPoint::Flushing { bits });
            let b = bits.trailing_zeros() as usize;
            self.parkers[b].wake();
            bits &= bits - 1;
        }
        self.gate.reach(ReplayPoint::FlushingDone);
    }

    /// Wakes every waiter on the overflow stack, as the first part of a
    /// wake loop owing `count` wakes (0 for a flush).
    #[inline]
    fn drain_overflow(&self, count: usize) {
        self.gate.reach(ReplayPoint::Drain { count });
        self.overflow.wake_all();
        self.gate.reach(ReplayPoint::UnparkDone);
    }

    /// Publishes the calling thread's handle at `slot`, replacing any
    /// handle left by an earlier park.
    ///
    /// Re-arming is what lets a waiter migrate between threads: the
    /// handle names the thread parked now, not the one that parked
    /// first. A [`ParkSlot::Shared`] holder has no slot to arm and
    /// rescues itself on a timeout instead.
    #[inline]
    pub fn arm_handle(&self, slot: ParkSlot) {
        if let Some(index) = slot.index() {
            self.parkers[index].arm();
        }
    }

    /// Whether a handle is armed at `slot`.
    #[must_use]
    #[inline]
    pub fn is_armed(&self, slot: ParkSlot) -> bool {
        slot.index()
            .is_some_and(|index| self.parkers[index].is_armed())
    }

    /// Arms this waiter's handle and publishes its wake bit, so a peer
    /// can find it.
    ///
    /// A slotless waiter has no bit; it registers on the
    /// [`overflow`](Self::overflow) stack instead, which every wake
    /// path drains. Either way the waiter is reachable by a peer
    /// before it parks, so its park needs no timeout.
    ///
    /// The `SeqCst` `fetch_or` — and the `SeqCst` CAS that pushes an
    /// overflow registration — is the ordering half of the handshake:
    /// it must precede the caller's final re-check of the ring.
    #[inline]
    pub fn arm(&self, slot: ParkSlot) {
        self.gate.reach_arm(slot);
        self.gate.reach(ReplayPoint::Handle);
        if !slot.is_leased() {
            self.overflow.register();
            return;
        }
        self.arm_handle(slot);
        self.gate.reach(ReplayPoint::Bit);
        self.wake.fetch_or(slot.mask(), Ordering::SeqCst);
    }

    /// Parks the calling thread until a peer wakes it.
    ///
    /// No timeout, and none is needed: [`arm`](Self::arm) leaves every
    /// waiter reachable before it parks — a leased one through its
    /// wake bit, a slotless one through the overflow stack. A wake
    /// that never comes is therefore a hang rather than a stall, so a
    /// lost-wake defect cannot hide as latency.
    #[inline]
    pub fn park(&self) {
        if self.gate.park_in_replay() {
            return;
        }
        super::atomics::thread::park();
    }

    /// Clears this waiter's wake bit.
    #[inline]
    pub fn disarm(&self, slot: ParkSlot) {
        self.gate.reach_clear(slot);
        self.wake.fetch_and(!slot.mask(), Ordering::Relaxed);
    }

    /// Clears this waiter's wake bit after its post-arm re-check made
    /// progress, and passes on a wake that a peer spent on it.
    ///
    /// The bit stays set from [`arm`](Self::arm) until now, so a waker
    /// can retire it and unpark this waiter after the re-check has
    /// already succeeded. That release is then spent on a waiter that
    /// is not parked, while one that is stays parked beside a free
    /// slot. The `fetch_and` and the waker's own are on one word, so
    /// exactly one of them finds the bit set: either the waker skips
    /// this waiter, or this finds the bit gone and wakes another.
    ///
    /// A wake passed on needlessly only makes a parked waiter re-check
    /// and park again. Use [`disarm`](Self::disarm) after a park, where
    /// the wake was meant for this waiter.
    #[inline]
    pub fn stand_down(&self, slot: ParkSlot) {
        self.gate.reach_clear(slot);
        let mask = slot.mask();
        if mask != 0 && self.wake.fetch_and(!mask, Ordering::SeqCst) & mask == 0 {
            self.wake_one();
        }
    }

    /// [`stand_down`](Self::stand_down) for waiters that are woken by
    /// position, as mpmc producers are: the release spent on this
    /// waiter was for one position, which it cannot name, and handing
    /// it to an arbitrary waiter could reach one pinned to another
    /// position. So every parked waiter is woken and re-checks.
    #[inline]
    pub fn stand_down_waking_all(&self, slot: ParkSlot) {
        self.gate.reach_clear(slot);
        let mask = slot.mask();
        if mask != 0 && self.wake.fetch_and(!mask, Ordering::SeqCst) & mask == 0 {
            self.wake_n(PARK_SLOTS);
        }
    }

    /// Watches `slot`'s handle for up to `limit`, reporting whether a
    /// waker claimed it.
    ///
    /// Diagnostic. Only a waker claims a handle, and it unparks the
    /// thread in the same call, so a handle that goes unarmed while
    /// its owner is still watching proves a wake was delivered —
    /// late, but delivered. A handle still armed at the end of the
    /// window had no waker coming for it in that time.
    ///
    /// The unpark that follows the claim lands as a token on a thread
    /// that is not parked; the caller must drain it so the next park
    /// does not return on it.
    #[cfg(feature = "backstop-metrics")]
    #[must_use]
    pub fn handle_claimed_within(&self, slot: ParkSlot, limit: std::time::Duration) -> bool {
        if !slot.is_leased() {
            return false;
        }
        let deadline = std::time::Instant::now() + limit;
        loop {
            if !self.is_armed(slot) {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::yield_now();
        }
    }

    /// How many waiters *other* than `slot` are parked right now.
    ///
    /// Diagnostic: [`wake_one`](Self::wake_one) serves one slot per
    /// event, so a waiter can be passed over rather than lost. A zero
    /// here says no peer was available to absorb a wake meant for us.
    #[must_use]
    #[inline]
    pub fn others_parked(&self, slot: ParkSlot) -> u32 {
        (self.wake.load(Ordering::SeqCst) & !slot.mask()).count_ones()
    }
}

impl Default for WakeSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
