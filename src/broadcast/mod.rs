//! Multi-producer, multi-consumer (MPMC) broadcast ring buffer.
//!
//! Every consumer sees every item published after it subscribes.
//! Multiple producers push via an atomic compare-and-swap on the tail;
//! consumers are dynamically created by cloning an existing
//! [`Consumer`].
//!
//! Items require `T: Clone` for [`Consumer::pop`], or use
//! [`Consumer::pop_ref`] for zero-copy reads. For large types with many
//! consumers, see the [`arc`] sub-module which wraps values in `Arc<T>`.
//!
//! # No consumers
//!
//! A buffer with no registered consumers rejects pushes: [`push`] and
//! [`reserve`] return `Err`/`None`, and the blocking variants return
//! `Err` immediately. Consumer progress is what proves a slot's previous
//! occupant has been read, so without it producers would lap the ring
//! and overwrite one another.
//!
//! [`push`]: Producer::push
//! [`reserve`]: Producer::reserve
//!
//! # Example
//!
//! ```
//! use quetzalcoatl::broadcast::SplitRing;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut c1) = SplitRing::new(Capacity::exact(16), 4).split();
//! let mut c2 = c1.clone();
//!
//! producer.push(10u32).unwrap();
//! producer.push(20).unwrap();
//!
//! assert_eq!(c1.pop(), Some(10));
//! assert_eq!(c1.pop(), Some(20));
//! assert_eq!(c2.pop(), Some(10));
//! assert_eq!(c2.pop(), Some(20));
//! ```

pub mod arc;
mod consumer;
mod consumer_floor;
mod consumer_registry;
mod floor_cache;
mod head_advance;
mod producer;
#[cfg(all(test, not(loom)))]
mod replay;
mod reservation_abandon;
mod slot_reuse;
mod slot_state;
mod split_ring;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};
pub use split_ring::SplitRing;

use crate::capacity::Capacity;
#[cfg(feature = "async")]
use crate::common::close_state::CloseState;
#[cfg(feature = "async")]
use crate::common::endpoint_count::EndpointCount;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkRegistry;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::{AlignedBuf, CachePadded};

use consumer_floor::ConsumerFloor;
use consumer_registry::ConsumerRegistry;
use floor_cache::SharedFloor;
use slot_state::SequenceWord;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct BroadcastSlot<T> {
    pub data: UnsafeCell<MaybeUninit<T>>,
    /// See [`SlotState`](slot_state::SlotState) for the encoding. Unlike
    /// the MPSC/SPMC rings, broadcast markers must name their position:
    /// a consumer never clears one (the other consumers still have to
    /// see it), so a positionless marker would be re-read on every later
    /// lap.
    sequence: SequenceWord,
}

impl<T> BroadcastSlot<T> {
    /// Classifies this slot for logical position `pos`.
    #[inline]
    pub(super) fn classify(
        &self,
        pos: usize,
    ) -> crate::common::SlotSnapshot<*const MaybeUninit<T>> {
        self.sequence.classify(&self.data, pos)
    }
}

/// Lock-free MPMC broadcast ring buffer.
///
/// Every consumer sees every item published after it subscribes.
/// Multiple producers claim positions by CAS on the tail. Consumers
/// clone to subscribe.
// repr(C) locks field order: shared immutable fields first (same cache
// line), then the contended tail on its own cache-padded line.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) buf: AlignedBuf<BroadcastSlot<T>>,
    pub(crate) capacity: Capacity,
    consumers: ConsumerRegistry,
    pub(crate) tail: CachePadded<AtomicUsize>,
    min_head_cache: SharedFloor,
    /// Sync producer-side park state. Bit `i` of `producer_park.wake` is
    /// set while the producer in park slot `i` is blocked in
    /// [`Producer::push_block`] waiting for the slowest consumer to
    /// advance its head. Consumers `wake_one` after advancing a head.
    /// Independent of the async `producer_waker` below.
    pub(crate) producer_park: WakeSet,
    /// Leases park-slot indices to blocking producers, reclaiming each
    /// on drop so no two live producers share one bitmap bit. Sharing a
    /// bit loses wakeups outright: a wake clears the single shared bit
    /// and unparks one holder, leaving the other parked with nothing
    /// recording that it waits.
    pub(crate) producer_park_slots: ParkRegistry,
    /// Live producer count (only tracked when `async` is enabled). When
    /// it reaches zero `closed` is set so `pop_async` can resolve to `None`.
    #[cfg(feature = "async")]
    pub(crate) producer_count: EndpointCount,
    /// Set by the last producer drop. Consumers' `pop_async` observe
    /// it and resolve to `None` once their backlog drains.
    #[cfg(feature = "async")]
    pub(crate) closed: CloseState,
    /// Wakers registered by parked `push_async` futures. Slot index =
    /// the position's slot index — producers waiting on slot `s`
    /// register there, and
    /// consumers wake the matching slot's waker after advancing head.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Wakers registered by parked `pop_async` futures. Slot index =
    /// consumer slot index — each consumer registers in its own waker
    /// slot. Producers `wake_one` after every push (one slot per push
    /// is sufficient because every consumer needs the same publish).
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
}

// SAFETY: RingBuffer is shared via Arc between producers and consumers on
// different threads. T: Send because values cross thread boundaries.
// T: Sync because multiple consumers hold &T to the same slot concurrently.
unsafe impl<T: Send + Sync> Send for RingBuffer<T> {}
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new broadcast ring buffer; see [`SplitRing::new`].
    pub(super) fn new(capacity: Capacity, max_consumers: usize) -> Self {
        let cap = capacity.get();
        let buf = AlignedBuf::new_with(cap, || BroadcastSlot {
            data: UnsafeCell::new(MaybeUninit::uninit()),
            sequence: SequenceWord::unwritten(),
        });
        Self {
            buf,
            capacity,
            tail: CachePadded(AtomicUsize::new(0)),
            min_head_cache: SharedFloor::new(),
            producer_park: WakeSet::new(),
            producer_park_slots: ParkRegistry::new(),
            consumers: ConsumerRegistry::new(max_consumers),
            #[cfg(feature = "async")]
            producer_count: EndpointCount::new(),
            #[cfg(feature = "async")]
            closed: CloseState::new(),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
        }
    }

    /// Wakes one blocking producer parked in [`Producer::push_block`] /
    /// [`Producer::reserve_block`], if any. Self-gates on the wake
    /// bitmap being non-zero (a single load on the fast path), so it's
    /// cheap to call after every consumer head advance. A producer's
    /// unblock condition is `min_head` moving, which depends on the
    /// slowest consumer — so any consumer advancing may free the
    /// producer; a false wake just re-checks and re-parks.
    #[inline]
    pub(crate) fn wake_producer(&self) {
        self.producer_park.wake_one();
    }

    /// Wakes every async producer task waiting for space.
    ///
    /// Call sites are unconditional; this pair and
    /// [`notify_consumers`](Self::notify_consumers) are the only place
    /// the async feature enters the notification path.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_producers(&self) {
        self.producer_waker.wake_all();
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_producers(&self) {}

    /// Wakes every async consumer task waiting for a publication.
    ///
    /// Called on every capacity- or position-releasing transition, an
    /// abandoned reservation included: a consumer blocked at that
    /// position advances past the marker, so the transition is real
    /// progress even though it delivers no value.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_consumers(&self) {
        self.consumer_waker.wake_all();
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_consumers(&self) {}

    /// Returns a reference to the slot at logical position `pos`.
    ///
    /// Single point of unsafety.
    #[inline]
    pub(crate) fn slot(&self, pos: usize) -> &BroadcastSlot<T> {
        let idx = self.capacity.index_of(pos);
        // SAFETY: index_of < cap == buf.len().
        unsafe { std::hint::assert_unchecked(idx < self.buf.len()) };
        &self.buf[idx]
    }

    /// The slowest active consumer's position; see [`ConsumerFloor`].
    pub(super) fn consumer_floor(&self) -> ConsumerFloor {
        self.consumers.floor()
    }

    /// Returns the number of items between the slowest consumer and the tail.
    #[must_use]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let floor = self.consumer_floor().position_or_tail(tail);
        tail.wrapping_sub(floor)
    }

    /// Returns `true` if no items are pending for any consumer.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the slowest consumer's backlog has reached capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity.get()
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Every slot is asked, not a range derived from `tail`: once
        // `tail` has rolled over `usize`, a range from zero misses the
        // slots still holding values from before it.
        //
        // Only a published marker owns a value. Vacant means cleared by
        // a later claim, abandoned means the reservation resolved
        // without one, and unwritten means never used.
        for slot in self.buf.iter_mut() {
            if slot.sequence.state_mut().holds_value() {
                // SAFETY: a published marker means data was initialized
                // by a producer and not cleared by a subsequent
                // claim_slot. Exclusive access in drop (&mut self)
                // guarantees no concurrency.
                unsafe {
                    slot.data.get().cast::<T>().drop_in_place();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
