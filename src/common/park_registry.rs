//! Exclusive park-slot allocation.
//!
//! A waiter needs a park slot no other *live* waiter shares. The wake
//! bitmap holds one bit per slot, and a wake clears that bit and unparks
//! the slot's handle — so two waiters on one slot is a lost wakeup, not
//! a spurious one: the waker clears the single shared bit, unparks one
//! of them, and the other is left parked with nothing recording that it
//! is waiting. Nobody wakes it again.
//!
//! Handing out `index & PARK_MASK` from a monotonic counter aliases in
//! two ways. Past `PARK_SLOTS` concurrent waiters it wraps onto live
//! slots, and because the counter never decreases it also wraps for a
//! ring that merely *creates* that many endpoints over its lifetime,
//! however few are alive at once.
//!
//! [`ParkRegistry`] instead leases slots and takes them back, so an
//! index is reused only after its previous holder is gone. A lease is
//! refused when every slot is held; the caller falls back to
//! [`ParkSlot::Shared`], which self-rescues on a timeout rather than
//! relying on a wake that has nowhere to land.

use std::sync::atomic::{AtomicU64, Ordering};

use super::park::PARK_SLOTS;

/// How long a slotless waiter sleeps before re-checking on its own.
///
/// It holds no bit in the wake bitmap, so no peer can find it. The
/// timeout is its only path back to the ring, and it bounds the extra
/// latency such a waiter can suffer.
const SHARED_PARK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);

/// A waiter's claim on a park slot.
///
/// `Leased` carries an index no other live waiter holds. `Shared` means
/// the registry was full: the waiter parks on a timeout and re-checks
/// the ring itself.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParkSlot {
    Leased(u32),
    Shared,
}

impl ParkSlot {
    /// The slot for a side that has exactly one endpoint by
    /// construction (the single producer of spsc/spmc, the single
    /// consumer of spsc/mpsc). Exclusivity is structural there, so no
    /// lease is needed.
    pub const SOLE: Self = Self::Leased(0);

    /// The slot for a waiter that already owns an exclusive index from
    /// some other allocation (a broadcast consumer's registry slot).
    ///
    /// Indices past the wake table become [`Shared`](Self::Shared)
    /// rather than wrapping onto it: masking would alias a live waiter
    /// and lose its wakeups, while a shared waiter is still reachable
    /// through the overflow list and the re-check timeout.
    #[must_use]
    #[inline]
    pub const fn from_exclusive_index(index: usize) -> Self {
        if index < PARK_SLOTS {
            #[allow(clippy::cast_possible_truncation)]
            Self::Leased(index as u32)
        } else {
            Self::Shared
        }
    }

    /// This slot's bit in the wake bitmap; `0` when slotless, which
    /// makes the caller's `fetch_or` / `fetch_and` no-ops.
    #[must_use]
    #[inline]
    pub const fn mask(self) -> u64 {
        match self {
            Self::Leased(bit) => 1u64 << bit,
            Self::Shared => 0,
        }
    }

    /// The slot index, if one is leased.
    #[must_use]
    #[inline]
    pub const fn index(self) -> Option<usize> {
        match self {
            Self::Leased(bit) => Some(bit as usize),
            Self::Shared => None,
        }
    }

    /// Whether a peer can find this waiter through the wake bitmap.
    #[must_use]
    #[inline]
    pub const fn is_leased(self) -> bool {
        matches!(self, Self::Leased(_))
    }

    /// Parks the calling thread until woken, or — with no slot — until
    /// the re-check interval elapses.
    #[inline]
    pub fn park(self) {
        match self {
            Self::Leased(_) => std::thread::park(),
            Self::Shared => std::thread::park_timeout(SHARED_PARK_INTERVAL),
        }
    }

    /// Parks with a wake-independent upper bound on the sleep.
    ///
    /// For call sites that keep a timeout as a backstop against a
    /// suspected residual missed-wake race, so the bound survives
    /// unchanged rather than being removed on the assumption that it is
    /// now unnecessary.
    #[inline]
    pub fn park_bounded(self, limit: std::time::Duration) {
        let bound = match self {
            Self::Leased(_) => limit,
            Self::Shared => limit.min(SHARED_PARK_INTERVAL),
        };
        std::thread::park_timeout(bound);
    }
}

/// Allocator for the `PARK_SLOTS` park-slot indices of one wake set.
pub struct ParkRegistry {
    /// Bit `i` set ↔ slot `i` is leased.
    held: AtomicU64,
}

/// The lease bitmap is one `u64`, so it can only track exactly
/// [`super::park::PARK_SLOTS`] slots.
const _: () = assert!(super::park::PARK_SLOTS == u64::BITS as usize);

impl ParkRegistry {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            held: AtomicU64::new(0),
        }
    }

    /// Leases a slot no other live holder has, or [`ParkSlot::Shared`]
    /// when all are taken.
    pub fn lease(&self) -> ParkSlot {
        let mut held = self.held.load(Ordering::Relaxed);
        loop {
            let free = !held;
            if free == 0 {
                return ParkSlot::Shared;
            }
            let bit = free.trailing_zeros();
            let mask = 1u64 << bit;
            match self.held.compare_exchange_weak(
                held,
                held | mask,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return ParkSlot::Leased(bit),
                Err(observed) => held = observed,
            }
        }
    }

    /// Returns a leased slot for reuse. A [`ParkSlot::Shared`] holder
    /// has nothing to return.
    pub fn release(&self, slot: ParkSlot) {
        if let ParkSlot::Leased(bit) = slot {
            self.held.fetch_and(!(1u64 << bit), Ordering::AcqRel);
        }
    }

    /// How many slots are currently leased.
    #[cfg(test)]
    fn leased(&self) -> u32 {
        self.held.load(Ordering::Relaxed).count_ones()
    }
}

impl Default for ParkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{ParkRegistry, ParkSlot, PARK_SLOTS};

    #[test]
    fn a_fresh_registry_leases_the_first_slot() {
        let registry = ParkRegistry::new();
        assert_eq!(registry.lease(), ParkSlot::Leased(0));
        assert_eq!(registry.leased(), 1);
    }

    #[test]
    fn concurrent_leases_are_distinct() {
        let registry = ParkRegistry::new();
        let held: Vec<_> = (0..PARK_SLOTS).map(|_| registry.lease()).collect();
        let mut indices: Vec<_> = held.iter().filter_map(|s| s.index()).collect();
        indices.sort_unstable();
        indices.dedup();
        assert_eq!(indices.len(), PARK_SLOTS);
    }

    #[test]
    fn a_full_registry_hands_out_shared_slots() {
        let registry = ParkRegistry::new();
        for _ in 0..PARK_SLOTS {
            assert!(registry.lease().is_leased());
        }
        assert_eq!(registry.lease(), ParkSlot::Shared);
        assert_eq!(registry.lease(), ParkSlot::Shared);
    }

    #[test]
    fn a_released_slot_is_leased_again() {
        let registry = ParkRegistry::new();
        let first = registry.lease();
        registry.release(first);
        assert_eq!(registry.lease(), first);
        assert_eq!(registry.leased(), 1);
    }

    /// The defect a monotonic counter has: creating and dropping
    /// endpoints repeatedly must not start aliasing live waiters.
    #[test]
    fn churning_endpoints_never_aliases_a_live_slot() {
        let registry = ParkRegistry::new();
        let long_lived = registry.lease();

        for _ in 0..(PARK_SLOTS * 4) {
            let transient = registry.lease();
            assert_ne!(transient, long_lived);
            registry.release(transient);
        }
        assert_eq!(registry.leased(), 1);
    }

    #[test]
    fn releasing_a_shared_slot_frees_nothing() {
        let registry = ParkRegistry::new();
        let leased = registry.lease();
        registry.release(ParkSlot::Shared);
        assert_eq!(registry.leased(), 1);
        registry.release(leased);
        assert_eq!(registry.leased(), 0);
    }

    #[test]
    fn a_shared_slot_is_invisible_to_the_wake_bitmap() {
        assert_eq!(ParkSlot::Shared.mask(), 0);
        assert_eq!(ParkSlot::Shared.index(), None);
        assert!(!ParkSlot::Shared.is_leased());
    }

    #[test]
    fn an_index_past_the_wake_table_overflows_rather_than_aliasing() {
        assert_eq!(ParkSlot::from_exclusive_index(0), ParkSlot::Leased(0));
        assert_eq!(
            ParkSlot::from_exclusive_index(PARK_SLOTS - 1),
            ParkSlot::Leased(u32::try_from(PARK_SLOTS).unwrap() - 1)
        );
        assert_eq!(ParkSlot::from_exclusive_index(PARK_SLOTS), ParkSlot::Shared);
        assert_eq!(
            ParkSlot::from_exclusive_index(PARK_SLOTS + 1),
            ParkSlot::Shared,
            "masking would alias slot 1 and steal a live waiter's wakeups"
        );
    }

    #[test]
    fn a_leased_slot_masks_only_its_own_bit() {
        assert_eq!(ParkSlot::Leased(0).mask(), 1);
        assert_eq!(ParkSlot::Leased(5).mask(), 1 << 5);
        assert_eq!(ParkSlot::Leased(5).index(), Some(5));
    }

    #[test]
    #[allow(clippy::needless_collect)]
    fn leases_taken_from_many_threads_are_distinct() {
        let registry = std::sync::Arc::new(ParkRegistry::new());
        // Collected deliberately: the threads must all be spawned, and
        // so contending, before any is joined.
        let taken: Vec<_> = (0..PARK_SLOTS)
            .map(|_| {
                let registry = std::sync::Arc::clone(&registry);
                std::thread::spawn(move || registry.lease())
            })
            .collect();

        let mut indices: Vec<_> = taken
            .into_iter()
            .filter_map(|h| h.join().unwrap().index())
            .collect();
        indices.sort_unstable();
        indices.dedup();
        assert_eq!(indices.len(), PARK_SLOTS);
    }
}
