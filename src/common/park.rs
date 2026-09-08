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

use super::park_overflow::ParkOverflow;
use super::park_registry::ParkSlot;
use super::thread_parker::ThreadParker;
use super::wake_interest::WakeInterest;
use super::AlignedBuf;

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
}

impl WakeSet {
    #[must_use]
    pub fn new() -> Self {
        Self {
            wake: AtomicU64::new(0),
            parkers: AlignedBuf::new_with(PARK_SLOTS, ThreadParker::new),
            cursor: AtomicU64::new(0),
            overflow: ParkOverflow::new(),
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
        self.overflow.wake_all();
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
        self.overflow.wake_all();
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
    #[inline]
    fn wake_one_ranked(&self, rank: WakeInterest, interest: &impl Fn(usize) -> WakeInterest) -> bool {
        loop {
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
            let prev = self.wake.fetch_and(!mask, Ordering::SeqCst);
            if prev & mask == 0 {
                continue;
            }
            self.cursor
                .store(u64::from(bit).wrapping_add(1), Ordering::Relaxed);
            if self.parkers[bit as usize].wake() {
                return true;
            }
        }
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
        self.overflow.wake_all();
        let mut cursor = self.cursor.load(Ordering::Relaxed);
        let mut woken = 0usize;
        while woken < n {
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
            let prev = self.wake.fetch_and(!mask, Ordering::SeqCst);
            cursor = u64::from(bit).wrapping_add(1);
            if prev & mask == 0 {
                // Lost the race on this bit, or it was stale — either
                // way nothing was delivered, so this does not count
                // against `n`. `woken` is what advances the count.
                continue;
            }
            if self.parkers[bit as usize].wake() {
                woken += 1;
            }
        }
        self.cursor.store(cursor, Ordering::Relaxed);
    }

    /// Wakes every waiter parked on the bitmap, swapping it to zero
    /// in the process. Cold-path helper for close-time draining.
    pub fn flush(&self) {
        self.overflow.wake_all();
        let mut bits = self.wake.swap(0, Ordering::AcqRel);
        while bits != 0 {
            let b = bits.trailing_zeros() as usize;
            self.parkers[b].wake();
            bits &= bits - 1;
        }
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
        if !slot.is_leased() {
            self.overflow.register();
            return;
        }
        self.arm_handle(slot);
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
    #[allow(clippy::unused_self)]
    pub fn park(&self) {
        super::atomics::thread::park();
    }

    /// Clears this waiter's wake bit.
    #[inline]
    pub fn disarm(&self, slot: ParkSlot) {
        self.wake.fetch_and(!slot.mask(), Ordering::Relaxed);
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
mod tests {
    use super::{ParkSlot, WakeInterest, WakeSet, PARK_SLOTS};
    use std::sync::atomic::Ordering;

    impl WakeSet {
        fn parked_bits(&self) -> u64 {
            self.wake.load(Ordering::Relaxed)
        }

        fn arm_all(&self, slots: &[u32]) {
            for &bit in slots {
                self.arm(ParkSlot::Leased(bit));
            }
        }

        fn parked_slots(&self) -> Vec<u32> {
            (0..u32::try_from(PARK_SLOTS).unwrap())
                .filter(|bit| self.parked_bits() & (1u64 << bit) != 0)
                .collect()
        }
    }

    #[test]
    fn a_fresh_set_has_nobody_parked() {
        let set = WakeSet::new();
        assert_eq!(set.parked_bits(), 0);
        assert!(!set.is_armed(ParkSlot::Leased(0)));
    }

    #[test]
    fn arming_publishes_the_slots_bit_and_its_handle() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Leased(7));
        assert_eq!(set.parked_bits(), 1 << 7);
        assert!(set.is_armed(ParkSlot::Leased(7)));
    }

    #[test]
    fn disarming_clears_only_its_own_bit() {
        let set = WakeSet::new();
        set.arm_all(&[3, 9]);
        set.disarm(ParkSlot::Leased(3));
        assert_eq!(set.parked_slots(), vec![9]);
    }

    #[test]
    fn a_slotless_waiter_publishes_no_bit_and_arms_no_handle() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Shared);
        assert_eq!(set.parked_bits(), 0);
        assert!(!set.is_armed(ParkSlot::Shared));
    }

    /// Having no bit is why it must be registered somewhere else: the
    /// bitmap cannot represent it, so the overflow stack does.
    #[test]
    fn a_slotless_waiter_registers_on_the_overflow() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Shared);
        assert_eq!(set.overflow.depth(), 1);
    }

    #[test]
    fn a_leased_waiter_never_touches_the_overflow() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Leased(4));
        assert!(set.overflow.is_empty());
    }

    /// Every wake path has to drain the overflow, or a slotless
    /// waiter parks with nobody able to reach it.
    #[test]
    fn every_wake_path_drains_the_overflow() {
        for (name, wake) in [
            ("wake_one", &(|s: &WakeSet| s.wake_one()) as &dyn Fn(&WakeSet)),
            ("wake_n", &|s: &WakeSet| s.wake_n(1)),
            ("flush", &|s: &WakeSet| s.flush()),
            ("wake_one_interested", &|s: &WakeSet| {
                s.wake_one_interested(|_| WakeInterest::Reserved);
            }),
        ] {
            let set = WakeSet::new();
            set.arm(ParkSlot::Shared);
            assert_eq!(set.overflow.depth(), 1, "{name}: registration lost");
            wake(&set);
            assert!(
                set.overflow.is_empty(),
                "{name} left a slotless waiter registered and unreachable"
            );
        }
    }

    /// The behaviour the timeout used to provide, now provided by a
    /// peer: a slotless waiter parks untimed and a wake releases it.
    /// Without the overflow drain this hangs.
    #[cfg(not(loom))]
    #[test]
    fn a_slotless_waiter_parks_untimed_and_a_peer_wake_releases_it() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let set = Arc::new(WakeSet::new());
        let released = Arc::new(AtomicBool::new(false));

        let waiter = {
            let set = Arc::clone(&set);
            let released = Arc::clone(&released);
            std::thread::spawn(move || {
                set.arm(ParkSlot::Shared);
                set.park();
                released.store(true, Ordering::Release);
            })
        };

        while set.overflow.is_empty() {
            std::thread::yield_now();
        }
        assert!(
            !released.load(Ordering::Acquire),
            "the waiter returned before any wake — the park was not untimed"
        );
        set.wake_one();
        waiter.join().unwrap();
        assert!(released.load(Ordering::Acquire));
    }

    #[test]
    fn waking_an_empty_set_is_a_no_op() {
        let set = WakeSet::new();
        set.wake_one();
        set.wake_n(8);
        set.flush();
        assert_eq!(set.parked_bits(), 0);
    }

    #[test]
    fn a_wake_releases_exactly_one_slot() {
        let set = WakeSet::new();
        set.arm_all(&[1, 2, 3]);
        set.wake_one();
        assert_eq!(set.parked_slots().len(), 2);
    }

    #[test]
    fn waking_a_slot_claims_the_handle_it_armed() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Leased(0));
        set.wake_one();
        assert!(!set.is_armed(ParkSlot::Leased(0)));
    }

    #[test]
    fn successive_wakes_walk_the_parked_slots() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1]);
        set.wake_one();
        set.wake_one();
        assert_eq!(set.parked_bits(), 0, "both parked slots must be woken");
    }

    /// The saturated-mpmc deadlock the round-robin cursor exists to
    /// fix: the waiter at the lower slot cannot make progress and
    /// re-parks immediately, while the higher one holds the position
    /// everybody needs. Picking the lowest set bit every time feeds
    /// each wake to the stuck waiter and the higher slot parks forever.
    #[test]
    fn a_reparking_low_slot_does_not_starve_a_higher_one() {
        let set = WakeSet::new();
        set.arm_all(&[2, 3]);

        set.wake_one();
        assert_eq!(set.parked_slots(), vec![3], "the lower slot wakes first");

        set.arm(ParkSlot::Leased(2));
        set.wake_one();

        assert_eq!(
            set.parked_slots(),
            vec![2],
            "the second wake must reach slot 3, not the slot that just re-parked"
        );
    }

    #[test]
    fn a_batch_wake_releases_the_count_it_was_given() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1, 2, 3, 4]);
        set.wake_n(3);
        assert_eq!(set.parked_slots().len(), 2);
    }

    #[test]
    fn a_batch_wake_stops_at_the_number_parked() {
        let set = WakeSet::new();
        set.arm_all(&[5, 6]);
        set.wake_n(32);
        assert_eq!(set.parked_bits(), 0);
    }

    /// A slot's bit can be set while its handle is already claimed.
    /// `arm` publishes the handle *then* the bit, and a waker claims
    /// the handle *after* clearing the bit, so the two are not
    /// updated atomically together: a waker that clears a bit and
    /// then finds the handle gone has consumed a wake it never
    /// delivered. If it stops there, a genuinely parked waiter on
    /// another slot is never woken, and nothing records the debt.
    ///
    /// Reached in the ring like this, with waiter `w` on bit 0 and
    /// peers `p1`/`p2`:
    ///
    /// - `w` arms bit 0 and parks.
    /// - `p1` clears bit 0, taking ownership of the wake, and is
    ///   descheduled before it claims the handle.
    /// - `w` wakes for an unrelated reason (a spurious unpark, or the
    ///   1 ms backstop the ring had at the time) and re-arms: a fresh
    ///   handle, and bit 0 set again.
    /// - `p1` resumes and claims that handle. Bit 0 is now set with
    ///   no handle behind it.
    /// - `p2` publishes work and wakes. It picks bit 0, clears it,
    ///   finds no handle, and returns having woken nobody — while
    ///   the waiter on bit 1 stays parked on work that is ready.
    #[test]
    fn a_wake_is_not_consumed_by_a_slot_whose_handle_is_already_claimed() {
        let set = WakeSet::new();
        set.arm(ParkSlot::Leased(0));
        set.arm(ParkSlot::Leased(1));

        assert!(
            set.parkers[0].wake(),
            "claim slot 0's handle, leaving its bit set behind it"
        );
        assert_eq!(set.parked_bits(), 0b11, "both bits are still published");

        set.wake_one();

        assert!(
            !set.is_armed(ParkSlot::Leased(1)),
            "the wake must reach slot 1, the only slot with a waiter behind it"
        );
    }

    /// The routing the mpmc producer side needs: a release only one
    /// parked waiter can use must reach that waiter, whatever the
    /// round-robin cursor would have picked.
    #[test]
    fn a_targeted_wake_reaches_the_waiter_that_wants_it() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1, 2]);
        // Cursor would pick slot 0 next.
        set.cursor.store(0, Ordering::Relaxed);

        set.wake_one_interested(|slot| {
            if slot == 2 {
                WakeInterest::Reserved
            } else {
                WakeInterest::Any
            }
        });

        assert!(set.is_armed(ParkSlot::Leased(0)));
        assert!(set.is_armed(ParkSlot::Leased(1)));
        assert!(
            !set.is_armed(ParkSlot::Leased(2)),
            "the wake must reach the one waiter that wanted it"
        );
        assert_eq!(set.parked_bits(), 0b011);
    }

    #[test]
    fn a_targeted_wake_with_no_taker_falls_back_to_round_robin() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1]);
        set.cursor.store(1, Ordering::Relaxed);

        set.wake_one_interested(|_| WakeInterest::Any);

        assert!(
            !set.is_armed(ParkSlot::Leased(1)),
            "nobody reserved it, so the cursor's pick among the takers is served"
        );
        assert!(set.is_armed(ParkSlot::Leased(0)));
    }

    /// A release every parked waiter declines must wake nobody.
    /// Waking one anyway spends the release on a waiter that re-parks,
    /// and the waiter it was owed to is never reached.
    #[test]
    fn a_release_every_waiter_declines_wakes_nobody() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1, 2]);

        set.wake_one_interested(|_| WakeInterest::Declines);

        assert_eq!(
            set.parked_bits(),
            0b111,
            "a waiter that cannot use the release must stay parked"
        );
    }

    /// Ranking is by claim, not by position: the reserving waiter is
    /// served even when a waiter that would take anything sits ahead
    /// of it in round-robin order.
    #[test]
    fn a_reserving_waiter_outranks_one_that_would_take_anything() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1]);
        set.cursor.store(0, Ordering::Relaxed);

        set.wake_one_interested(|slot| {
            if slot == 1 {
                WakeInterest::Reserved
            } else {
                WakeInterest::Any
            }
        });

        assert!(
            set.is_armed(ParkSlot::Leased(0)),
            "the waiter that would take anything must yield to the one that reserved it"
        );
        assert!(!set.is_armed(ParkSlot::Leased(1)));
    }

    /// With nobody reserving, the release still reaches a waiter that
    /// can use it rather than stopping at one that declines.
    #[test]
    fn a_declining_waiter_does_not_absorb_a_release_another_can_use() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1]);
        set.cursor.store(0, Ordering::Relaxed);

        set.wake_one_interested(|slot| {
            if slot == 0 {
                WakeInterest::Declines
            } else {
                WakeInterest::Any
            }
        });

        assert!(
            set.is_armed(ParkSlot::Leased(0)),
            "the declining waiter must not be woken"
        );
        assert!(
            !set.is_armed(ParkSlot::Leased(1)),
            "the waiter that could use the release must get it"
        );
    }

    #[test]
    fn a_targeted_wake_on_an_empty_set_wakes_nobody() {
        let set = WakeSet::new();
        set.wake_one_interested(|_| WakeInterest::Reserved);
        assert_eq!(set.parked_bits(), 0);
    }

    /// A wanting slot whose handle is already claimed is a bit with
    /// nobody behind it; the wake must move on to the next taker.
    #[test]
    fn a_targeted_wake_skips_a_wanting_slot_whose_handle_is_claimed() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1]);
        assert!(set.parkers[0].wake());

        set.wake_one_interested(|_| WakeInterest::Reserved);

        assert!(
            !set.is_armed(ParkSlot::Leased(1)),
            "slot 0 advertised a waiter that was not there"
        );
        assert_eq!(set.parked_bits(), 0);
    }

    /// The same defect on the batch path: `wake_n(1)` must deliver
    /// one wake, not merely retire one bit.
    #[test]
    fn a_batch_wake_skips_slots_whose_handles_are_already_claimed() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1, 2]);
        assert!(set.parkers[0].wake());
        assert!(set.parkers[1].wake());

        set.wake_n(1);

        assert!(
            !set.is_armed(ParkSlot::Leased(2)),
            "the one requested wake must reach the one real waiter"
        );
    }

    /// `wake_n` counts delivered unparks, so a set of bits that can
    /// never deliver one must still terminate: every iteration clears
    /// the bit it picked, so the bitmap drains and the `ws == 0` exit
    /// is reached rather than the loop spinning on undeliverable bits.
    /// Round-robin is a fairness policy, not a delivery guarantee,
    /// and the difference matters because mpmc producers are not
    /// interchangeable waiters. Each parks holding a batch of
    /// *specific* reserved positions, and `DoneWord::is_free_for`
    /// matches an exact position, so a producer woken for a slot
    /// outside its batch cannot use it and re-parks.
    ///
    /// So "the wake went to another waiter" is only harmless when
    /// that waiter could actually consume it. When it could not, the
    /// event is spent: `wake_one` delivers one unpark per publish,
    /// and the producer that *was* waiting on that exact position
    /// stays parked. This is the starvation the cursor was added to
    /// fix, and the cursor does not close it — it only changes which
    /// waiter is passed over.
    ///
    /// Here slot 0 is woken and re-parks (it cannot use the freed
    /// position), while slot 1 is the one that needed it. The second
    /// wake must reach slot 1 rather than returning to slot 0.
    #[test]
    fn a_waiter_that_cannot_use_its_wake_does_not_reclaim_the_next_one() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1]);

        set.wake_one();
        assert_eq!(set.parked_slots(), vec![1], "the first wake serves slot 0");

        set.arm(ParkSlot::Leased(0));
        set.wake_one();

        assert_eq!(
            set.parked_slots(),
            vec![0],
            "a waiter that re-parked must not consume the wake owed to slot 1"
        );
    }

    /// The cursor is shared per side, so it is advanced by whichever
    /// waker ran last rather than per waiter. Two publishers waking
    /// concurrently must still walk both parked slots rather than
    /// both landing on the same one.
    #[test]
    fn two_wakes_from_one_cursor_reach_two_distinct_slots() {
        let set = WakeSet::new();
        set.arm_all(&[4, 5]);

        set.wake_one();
        set.wake_one();

        assert_eq!(
            set.parked_bits(),
            0,
            "both waiters were parked and two wakes were issued, so neither may be skipped"
        );
    }

    #[test]
    fn a_batch_wake_terminates_when_no_bit_can_deliver() {
        let set = WakeSet::new();
        let every: Vec<u32> = (0..u32::try_from(PARK_SLOTS).unwrap()).collect();
        set.arm_all(&every);
        for bit in 0..PARK_SLOTS {
            assert!(set.parkers[bit].wake());
        }

        set.wake_n(8);

        assert_eq!(set.parked_bits(), 0);
    }

    #[test]
    fn a_wake_with_no_reachable_waiter_clears_the_stale_bits() {
        let set = WakeSet::new();
        set.arm_all(&[0, 1]);
        assert!(set.parkers[0].wake());
        assert!(set.parkers[1].wake());

        set.wake_one();

        assert_eq!(
            set.parked_bits(),
            0,
            "bits with no handle behind them must not persist as wake sinks"
        );
    }

    #[test]
    fn a_flush_releases_every_parked_slot() {
        let set = WakeSet::new();
        let every: Vec<u32> = (0..u32::try_from(PARK_SLOTS).unwrap()).collect();
        set.arm_all(&every);
        assert_eq!(set.parked_bits(), u64::MAX);
        set.flush();
        assert_eq!(set.parked_bits(), 0);
    }

    /// A shift or mask that truncated would leave the highest slot
    /// parked with its bit still set.
    #[test]
    fn the_last_slot_is_reachable() {
        let set = WakeSet::new();
        let last = u32::try_from(PARK_SLOTS).unwrap() - 1;
        set.arm(ParkSlot::Leased(last));
        set.wake_one();
        assert_eq!(set.parked_bits(), 0);
    }

    #[test]
    fn a_wake_reaches_a_waiter_that_parked_on_another_thread() {
        let set = std::sync::Arc::new(WakeSet::new());
        let peer = std::sync::Arc::clone(&set);

        let waiter = std::thread::spawn(move || {
            peer.arm(ParkSlot::Leased(1));
            std::thread::park();
            peer.disarm(ParkSlot::Leased(1));
        });

        crate::common::park_probe::ParkProbe::expect_until("the waiter to park", || {
            set.parked_bits() & (1 << 1) != 0
        });
        set.wake_one();
        waiter.join().unwrap();
    }
}
