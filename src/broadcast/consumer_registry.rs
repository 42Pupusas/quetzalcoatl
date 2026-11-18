//! The fixed-size table of subscribed consumers and their heads.

use super::consumer_floor::ConsumerFloor;
use crate::common::position_order::PositionOrder;
use crate::common::CachePadded;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// One consumer's registration: whether the seat is taken, and how far
/// its occupant has read.
///
/// `head` is cache-padded because each consumer stores to its own on
/// every delivery while producers scan all of them; unpadded, one
/// consumer's progress would invalidate its neighbours' line on every
/// pop.
struct ConsumerSlot {
    head: CachePadded<AtomicUsize>,
    active: AtomicBool,
}

impl ConsumerSlot {
    const fn vacant() -> Self {
        Self {
            head: CachePadded(AtomicUsize::new(0)),
            active: AtomicBool::new(false),
        }
    }

    /// Takes this seat if it is free.
    fn take(&self) -> bool {
        self.active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// This slot's head if its occupant is still subscribed.
    ///
    /// The `active` load is `Relaxed` but the `head` load is `Acquire`:
    /// only the head orders against the reads it bounds, and a stale
    /// `active` is safe in both directions — a departed consumer reports
    /// a floor no lower than the one it last published, and a consumer
    /// not yet seen has published nothing to wait for.
    fn subscribed_head(&self) -> Option<usize> {
        self.active
            .load(Ordering::Relaxed)
            .then(|| self.head.load(Ordering::Acquire))
    }
}

/// The consumer registry: a fixed number of seats, each holding one
/// subscriber's read position.
///
/// The `active` flags are the ring's only record of who is subscribed —
/// there is no separate consumer count — so this type answers both
/// "may a producer still make progress" and "how far behind is the
/// slowest reader".
pub(super) struct ConsumerRegistry {
    slots: Box<[ConsumerSlot]>,
}

impl ConsumerRegistry {
    /// # Panics
    /// Panics if `seats` is zero: a broadcast ring with nowhere to
    /// subscribe can never accept a push.
    pub(super) fn new(seats: usize) -> Self {
        assert!(seats > 0, "max_consumers must be > 0");
        Self {
            slots: (0..seats).map(|_| ConsumerSlot::vacant()).collect(),
        }
    }

    /// Seats the consumer created by `split`, which is the first and so
    /// always takes seat zero.
    ///
    /// Uncontended by construction — the ring is not yet shared — so
    /// this stores rather than competing for the seat.
    pub(super) fn subscribe_first(&self) -> usize {
        self.slots[0].active.store(true, Ordering::Relaxed);
        self.slots[0].head.store(0, Ordering::Relaxed);
        0
    }

    /// Seats a consumer created by cloning, starting it at `tail` so it
    /// sees only what is published from now on.
    ///
    /// # Panics
    /// Panics if every seat is taken.
    pub(super) fn subscribe_at(&self, tail: usize) -> usize {
        let index = self.claim();
        self.slots[index].head.store(tail, Ordering::Relaxed);
        index
    }

    fn claim(&self) -> usize {
        for (index, slot) in self.slots.iter().enumerate() {
            if slot.take() {
                return index;
            }
        }
        panic!("broadcast: max consumer limit ({}) exceeded", self.seats());
    }

    /// Gives up seat `index`.
    ///
    /// `Release` so the departing consumer's reads cannot be reordered
    /// after the flag that lets a producer overwrite what it was
    /// reading.
    #[inline]
    pub(super) fn unsubscribe(&self, index: usize) {
        self.slots[index].active.store(false, Ordering::Release);
    }

    /// Seat `index`'s read position.
    ///
    /// `Relaxed`: only this seat's occupant calls it, and a thread
    /// always sees its own last store.
    #[inline]
    pub(super) fn head_of(&self, index: usize) -> usize {
        self.slots[index].head.load(Ordering::Relaxed)
    }

    /// Publishes seat `index` as having finished with `head`.
    ///
    /// `Release` so the value read at `head` is fully read before the
    /// producer may see the slot freed and overwrite it.
    #[inline]
    pub(super) fn publish_head_past(&self, index: usize, head: usize) {
        self.slots[index]
            .head
            .store(head.wrapping_add(1), Ordering::Release);
    }

    /// The slowest subscribed consumer's position.
    ///
    /// Compared in position order, not numerically: consumers that
    /// straddle the `usize` rollover are at most a capacity apart, and
    /// the numerically smaller head is then the one that is ahead.
    #[inline]
    pub(super) fn floor(&self) -> ConsumerFloor {
        self.slots
            .iter()
            .filter_map(ConsumerSlot::subscribed_head)
            .reduce(PositionOrder::earliest)
            .map_or(ConsumerFloor::NoConsumers, ConsumerFloor::At)
    }

    /// Whether anyone is still subscribed.
    ///
    /// Producers use this to tell "the ring is full" from "there is
    /// nobody left to read it", the second of which is terminal.
    #[inline]
    pub(super) fn any_subscribed(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.active.load(Ordering::Acquire))
    }

    pub(super) fn seats(&self) -> usize {
        self.slots.len()
    }
}

#[cfg(test)]
mod tests;
