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
//! [`ParkSlot::Shared`], which has no bit to publish and is instead
//! found through [`ParkOverflow`], the stack every wake path drains.
//!
//! [`ParkOverflow`]: super::park_overflow::ParkOverflow

use super::atomics::AtomicU64;
use std::sync::atomic::Ordering;

use super::park::PARK_SLOTS;

/// A waiter's claim on a park slot.
///
/// `Leased` carries an index no other live waiter holds. `Shared` means
/// the registry was full: the waiter has no bit in the wake bitmap and
/// is reached through the overflow stack instead.
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
    /// through the overflow list every wake path drains.
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
    // Loom's `AtomicU64::new` is not const, so the loom twin of this
    // constructor takes the weaker form. Std callers keep const use.
    #[cfg(not(loom))]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            held: AtomicU64::new(0),
        }
    }

    #[cfg(loom)]
    #[must_use]
    pub fn new() -> Self {
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
mod tests;
