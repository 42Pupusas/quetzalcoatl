//! Test control over where a waiter stops inside the park handshake.
//!
//! The window the handshake has to close sits between a waiter's
//! post-arm re-check succeeding and its bit being cleared. A peer
//! release that lands there is spent on a waiter that is no longer
//! parked, and nothing outside the waiter can hold that window open.
//!
//! [`WakeSet`](super::park::WakeSet) calls [`ParkGate::reach_arm`] as
//! its `arm` begins and [`ParkGate::reach_clear`] as `disarm`,
//! `stand_down` and `stand_down_waking_all` begin. A test holds one of
//! them for a slot, and the waiter owning that slot blocks there once,
//! inside the real `push_block` or `pop_block`.
//!
//! Outside tests the gate is an empty struct and both calls compile to
//! nothing.

use super::park_registry::ParkSlot;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// One stopping point: blocks the first arrival from a chosen slot
/// until opened.
#[cfg(test)]
pub struct Hold {
    mask: AtomicU64,
    reached: AtomicBool,
    open: AtomicBool,
}

#[cfg(test)]
impl Hold {
    const fn new() -> Self {
        Self {
            mask: AtomicU64::new(0),
            reached: AtomicBool::new(false),
            open: AtomicBool::new(false),
        }
    }

    /// Stops the next arrival from `slot`. One-shot.
    pub fn hold(&self, slot: ParkSlot) {
        self.mask.store(slot.mask(), Ordering::SeqCst);
    }

    /// Whether a waiter is stopped here, or has been.
    pub fn is_reached(&self) -> bool {
        self.reached.load(Ordering::SeqCst)
    }

    /// Lets the stopped waiter continue.
    pub fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
    }

    fn reach(&self, slot: ParkSlot) {
        let mask = slot.mask();
        if mask == 0 || self.mask.load(Ordering::SeqCst) != mask {
            return;
        }
        if self.reached.swap(true, Ordering::SeqCst) {
            return;
        }
        while !self.open.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }
}

/// The two stopping points of the park handshake.
#[cfg(test)]
pub struct ParkGate {
    /// Before the waiter publishes its bit.
    pub before_arm: Hold,
    /// Before the waiter clears its bit, on any exit.
    pub before_clear: Hold,
}

#[cfg(not(test))]
pub struct ParkGate;

#[cfg(test)]
impl ParkGate {
    pub const fn new() -> Self {
        Self {
            before_arm: Hold::new(),
            before_clear: Hold::new(),
        }
    }

    pub fn reach_arm(&self, slot: ParkSlot) {
        self.before_arm.reach(slot);
    }

    pub fn reach_clear(&self, slot: ParkSlot) {
        self.before_clear.reach(slot);
    }
}

#[cfg(not(test))]
impl ParkGate {
    pub const fn new() -> Self {
        Self
    }

    #[inline]
    #[allow(clippy::unused_self)]
    pub const fn reach_arm(&self, _slot: ParkSlot) {}

    #[inline]
    #[allow(clippy::unused_self)]
    pub const fn reach_clear(&self, _slot: ParkSlot) {}
}
