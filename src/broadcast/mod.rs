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
//! use quetzalcoatl::broadcast::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut c1) = RingBuffer::new(Capacity::exact(16), 4).split();
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
mod producer;
mod reservation_abandon;
mod slot_reuse;
mod slot_state;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};

use crate::capacity::Capacity;
#[cfg(feature = "async")]
use crate::common::close_state::CloseState;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkRegistry;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
#[cfg(feature = "async")]
use crate::common::endpoint_count::EndpointCount;
use crate::common::{AlignedBuf, CachePadded};

use consumer_floor::ConsumerFloor;
use consumer_registry::ConsumerRegistry;
use floor_cache::{FloorCache, SharedFloor};
use slot_state::SequenceWord;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    /// Creates a new broadcast ring buffer.
    ///
    /// `max_consumers` sets the maximum number of concurrent consumers.
    /// The first consumer is created by [`split`](Self::split). Additional consumers
    /// are created by cloning an existing consumer.
    ///
    /// # Panics
    ///
    /// Panics if `max_consumers` is 0.
    #[must_use]
    pub fn new(capacity: Capacity, max_consumers: usize) -> Self {
        let cap = capacity.get();
        let buf = AlignedBuf::new_with(cap, || BroadcastSlot {
            data: UnsafeCell::new(MaybeUninit::uninit()),
            sequence: SequenceWord::vacant(),
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

    /// Splits the ring buffer into a [`Producer`] and the first [`Consumer`].
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self);

        let slot_index = arc.consumers.subscribe_first();

        let park_slot = arc.producer_park_slots.lease();
        let producer = Producer {
            queue: Arc::clone(&arc),
            floor_cache: FloorCache::new(),
            park_slot,
        };
        let consumer = Consumer {
            queue: arc,
            slot_index,
        };
        (producer, consumer)
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
        // Determine the range of slots that may contain initialized data.
        // `tail` is the total number of items ever pushed. A slot at
        // position `pos` was last written by push number `pos`, so the
        // most recent `min(tail, cap)` slots may hold live data.
        //
        // We use the per-slot sequence number to decide: only a
        // published marker owns a value. Vacant means never written or
        // cleared by a later claim; abandoned means the reservation
        // resolved without one.
        //
        // The start of the range is `tail - min(tail, cap)` to avoid
        // scanning the entire buffer, and the wrapping_sub handles the
        // (astronomically unlikely) usize wraparound case.
        let tail = *self.tail.0.get_mut();
        let start = tail.wrapping_sub(tail.min(self.capacity.get()));
        for pos in start..tail {
            let slot = &mut self.buf[self.capacity.index_of(pos)];
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
mod tests {
    use super::*;
    use crate::capacity::Capacity;

    use crate::common::DropCounter;

    // -----------------------------------------------------------------------
    // Basic tests (ported from MPSC/SPSC)
    // -----------------------------------------------------------------------

    #[test]
    fn capacity_one() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(1), 4).split();
        assert!(producer.push(42).is_ok());
        assert_eq!(consumer.pop(), Some(42));
    }

    #[test]
    fn zero_sized_types() {
        let (producer, mut consumer) = RingBuffer::<()>::new(Capacity::exact(4), 4).split();
        producer.push(()).unwrap();
        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
        assert_eq!(consumer.pop(), Some(()));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn pop_empty_buffer() {
        let (_producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn pop_empty_is_idempotent() {
        let (_producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for _ in 0..10 {
            assert_eq!(consumer.pop(), None);
        }
    }

    #[test]
    fn push_to_buffer() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn fill_to_capacity() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        assert!(producer.push(99).is_err());
        for i in 0..4 {
            assert_eq!(consumer.pop(), Some(i));
        }
    }

    #[test]
    fn length_invariants() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert!(producer.is_empty());
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        producer.push(2).unwrap();
        assert_eq!(producer.len(), 2);
        let _ = consumer.pop();
        assert_eq!(consumer.len(), 1);
        let _ = consumer.pop();
        assert!(consumer.is_empty());
    }

    #[test]
    fn wraparound_behavior() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        for i in 0..4 {
            assert_eq!(consumer.pop(), Some(i));
        }
        // Second lap
        for i in 10..14 {
            producer.push(i).unwrap();
        }
        for i in 10..14 {
            assert_eq!(consumer.pop(), Some(i));
        }
    }

    #[test]
    fn wraparound_exercises_all_slots() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let mut val = 0u32;
        for _lap in 0..3 {
            for _ in 0..4 {
                producer.push(val).unwrap();
                val += 1;
            }
            for expected in (val - 4)..val {
                assert_eq!(consumer.pop(), Some(expected));
            }
        }
    }

    #[test]
    fn overwrite_oldest_element() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        assert!(producer.push(99).is_err());
        // Pop all, then fill again (overwrites)
        for _ in 0..4 {
            let _ = consumer.pop();
        }
        for i in 10..14 {
            producer.push(i).unwrap();
        }
        for i in 10..14 {
            assert_eq!(consumer.pop(), Some(i));
        }
    }

    // -----------------------------------------------------------------------
    // Broadcast-specific tests
    // -----------------------------------------------------------------------

    #[test]
    fn two_consumers_see_same_data() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(8), 4).split();
        let mut c2 = c1.clone();
        producer.push(10).unwrap();
        producer.push(20).unwrap();
        producer.push(30).unwrap();
        assert_eq!(c1.pop(), Some(10));
        assert_eq!(c1.pop(), Some(20));
        assert_eq!(c1.pop(), Some(30));
        assert_eq!(c2.pop(), Some(10));
        assert_eq!(c2.pop(), Some(20));
        assert_eq!(c2.pop(), Some(30));
    }

    #[test]
    fn cloned_consumer_sees_only_future_items() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(8), 4).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        // Clone after two items published
        let mut c2 = c1.clone();
        producer.push(3).unwrap();

        assert_eq!(c1.pop(), Some(1));
        assert_eq!(c1.pop(), Some(2));
        assert_eq!(c1.pop(), Some(3));
        // c2 starts at tail=2, so only sees item 3
        assert_eq!(c2.pop(), Some(3));
        assert_eq!(c2.pop(), None);
    }

    #[test]
    fn consumer_drop_does_not_drain() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (producer, mut c1) = RingBuffer::<DropCounter>::new(Capacity::exact(8), 4).split();
        let c2 = c1.clone();
        producer
            .push(DropCounter {
                counter: counter.clone(),
            })
            .unwrap();
        // Drop c2 — should NOT drop the value in the slot
        drop(c2);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        // c1 can still read it
        let val = c1.pop().unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        drop(val);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    /// A broadcast with no consumers refuses writes rather than
    /// discarding them. Only a consumer's progress proves a slot's
    /// previous occupant has been read; without that, producers would
    /// lap the ring and two of them would write the same slot.
    #[test]
    fn no_consumers_rejects_push() {
        let (producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        drop(consumer);
        for i in 0..100 {
            assert_eq!(producer.push(i), Err(i));
        }
    }

    /// Producers racing the last consumer's departure must all finish,
    /// and must never write the same slot concurrently. Run under Miri
    /// this is the data-race check; run natively it is a liveness check.
    #[test]
    fn concurrent_push_with_no_consumers_is_race_free() {
        let (seed, consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 2).split();
        drop(consumer);
        let mut handles = Vec::new();
        for _ in 0..2 {
            let p = seed.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..8u64 {
                    assert_eq!(p.push(i), Err(i));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    /// A reservation is outstanding storage: the producer owns
    /// position `pos` and will write it, but no consumer has read the
    /// previous occupant of the aliasing slot. A consumer subscribing
    /// *after* the reservation starts at `tail`, which is already past
    /// `pos`, so the floor jumps ahead of a slot that is still being
    /// written. Nothing may reclaim that slot until the reservation
    /// resolves.
    #[test]
    fn late_consumer_does_not_free_an_outstanding_reservation() {
        let (mut producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(2), 4).split();
        let mut peer = producer.clone();

        let writer = producer.reserve().expect("first claim fits");

        // Subscribes at tail == 1, ahead of the reservation at 0.
        let late = consumer.clone();
        drop(consumer);

        assert!(
            peer.reserve().is_some(),
            "position 1 is free and must be claimable"
        );

        // Position 2 aliases position 0, which is still reserved.
        assert!(
            peer.reserve().is_none(),
            "a slot with an outstanding reservation must not be reclaimed"
        );

        drop(writer);
        drop(late);
    }

    /// The same hazard with no consumers left at all: the reservation
    /// is still outstanding, so its slot stays off-limits.
    #[test]
    fn zero_consumers_does_not_free_an_outstanding_reservation() {
        let (mut producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(2), 4).split();
        let mut peer = producer.clone();

        let writer = producer.reserve().expect("first claim fits");
        drop(consumer);

        assert!(
            peer.reserve().is_none(),
            "no consumers must not permit reclaiming a reserved slot"
        );

        drop(writer);
    }

    /// An abandoned reservation must be skipped exactly once. The
    /// marker names its position, so a consumer that has already walked
    /// past it reads the slot as empty rather than as another
    /// abandonment — with `cap == 1` every later position aliases that
    /// one slot, so a positionless marker made `pop` advance forever
    /// instead of reporting empty.
    #[test]
    fn capacity_one_abandoned_reservation_is_skipped_once() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(1), 4).split();

        drop(producer.reserve().expect("the empty ring has a slot"));

        assert_eq!(consumer.pop(), None, "the ring holds no value");
        assert_eq!(consumer.pop(), None, "and still holds none");

        producer.push(7).unwrap();
        assert_eq!(consumer.pop(), Some(7));
        assert_eq!(consumer.pop(), None);
    }

    /// The same at `cap == 2`, alternating abandoned and published
    /// positions so the consumer must skip markers interleaved with
    /// real values rather than at the head of the ring.
    #[test]
    fn capacity_two_alternating_abandon_and_publish() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(2), 4).split();

        for round in 0..8u32 {
            drop(producer.reserve().expect("a slot is free"));
            producer.push(round).expect("the other slot is free");
            assert_eq!(consumer.pop(), Some(round));
            assert_eq!(consumer.pop(), None, "round {round} left a stale marker");
        }
    }

    /// A consumer whose head sits at an abandoned position must not
    /// treat every later position as abandoned too.
    #[test]
    fn a_stale_abandoned_marker_does_not_swallow_later_positions() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();

        drop(producer.reserve().expect("the empty ring has a slot"));
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), None);
    }

    /// Abandoning a written-but-uncommitted reservation drops the value
    /// and releases the position, exactly like abandoning an unwritten
    /// one.
    /// The broadcast twin of the MPSC case: an uncommitted reservation
    /// whose value panics on drop must still publish its abandonment
    /// marker, or a consumer parked at that position waits for a
    /// publication that never comes.
    #[test]
    fn a_panicking_payload_still_abandons_its_uncommitted_reservation() {
        struct PanicOnDropGuard {
            panics: bool,
        }

        impl PanicOnDropGuard {
            const fn armed() -> Self {
                Self { panics: true }
            }
            const fn disarmed() -> Self {
                Self { panics: false }
            }
        }

        impl Drop for PanicOnDropGuard {
            fn drop(&mut self) {
                assert!(!self.panics, "destructor failure");
            }
        }

        let ring = RingBuffer::<PanicOnDropGuard>::new(Capacity::exact(2), 2);
        let (mut producer, mut consumer) = ring.split();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let writer = producer.reserve().unwrap();
            drop(writer.write(PanicOnDropGuard::armed()));
        }));
        assert!(result.is_err(), "the destructor panic must propagate");

        assert!(
            consumer.pop_ref().is_none(),
            "the abandoned position delivers no value"
        );

        // The marker is what lets the consumer advance past the
        // position. Without it the consumer stops at the unresolved
        // claim and never reaches anything published behind it.
        producer
            .reserve()
            .expect("the ring must still accept a reservation")
            .write(PanicOnDropGuard::disarmed())
            .commit();

        let reader = consumer
            .pop_ref()
            .expect("the consumer must advance past the abandoned position");
        drop(reader);
    }

    #[test]
    fn abandoning_a_written_slot_releases_its_position() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (mut producer, mut consumer) =
            RingBuffer::<DropCounter>::new(Capacity::exact(1), 4).split();

        {
            let w = producer.reserve().expect("the empty ring has a slot");
            w.write(DropCounter {
                counter: counter.clone(),
            });
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1, "value dropped in place");

        assert_eq!(consumer.pop().map(|_| ()), None);
        producer
            .push(DropCounter { counter })
            .expect("the abandoned position was released");
        assert!(consumer.pop().is_some());
    }

    #[test]
    fn slow_consumer_blocks_producer() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let mut c2 = c1.clone();
        // Fill buffer
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        // Buffer full because c1 hasn't popped
        assert!(producer.push(99).is_err());
        // c2 pops all, but c1 is still slow
        for _ in 0..4 {
            let _ = c2.pop();
        }
        // Still full because c1 is the bottleneck
        assert!(producer.push(99).is_err());
        // c1 pops one, freeing one slot
        let _ = c1.pop();
        assert!(producer.push(99).is_ok());
    }

    #[test]
    fn producer_drops_old_values_on_overwrite() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (producer, mut consumer) =
            RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
        // Fill buffer
        for _ in 0..4 {
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        // Pop all (clones come out, originals stay in slots)
        for _ in 0..4 {
            let val = consumer.pop().unwrap();
            drop(val); // Drop the clone
        }
        // 4 clones dropped, but originals still in slots
        assert_eq!(counter.load(Ordering::Relaxed), 4);
        // Push new items, overwriting old slots — producer drops originals
        for _ in 0..4 {
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
        }
        // 4 original values + 4 clones = 8 total drops so far
        assert_eq!(counter.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn multiple_producers_single_consumer() {
        let (p1, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(64), 4).split();
        let p2 = p1.clone();
        for i in 0..10 {
            p1.push(i).unwrap();
            p2.push(100 + i).unwrap();
        }
        let mut values: Vec<u32> = std::iter::from_fn(|| consumer.pop()).collect();
        values.sort_unstable();
        let mut expected: Vec<u32> = (0..10).chain(100..110).collect();
        expected.sort_unstable();
        assert_eq!(values, expected);
    }

    #[test]
    fn pop_requires_clone() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let mut c2 = c1.clone();
        producer.push(42).unwrap();
        // Both consumers get the same value
        assert_eq!(c1.pop(), Some(42));
        assert_eq!(c2.pop(), Some(42));
    }

    // -----------------------------------------------------------------------
    // Zero-copy API tests
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_write_commit_pop_ref_cycle() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let w = producer.reserve().unwrap();
        w.write(42).commit();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 42);
    }

    #[test]
    fn reserve_slot_mut_commit() {
        let (mut producer, mut consumer) =
            RingBuffer::<[u8; 256]>::new(Capacity::exact(4), 4).split();
        let mut w = producer.reserve().unwrap();
        w.slot_mut().write([0xAB; 256]);
        // SAFETY: slot_mut().write() initialized the slot above.
        unsafe { w.commit_unchecked() };
        let reader = consumer.pop_ref().unwrap();
        assert_eq!((*reader)[0], 0xAB);
        assert_eq!((*reader)[255], 0xAB);
    }

    #[test]
    fn reserve_returns_none_when_full() {
        let (mut producer, _consumer) = RingBuffer::<u32>::new(Capacity::exact(2), 4).split();

        let w1 = producer.reserve().unwrap();
        w1.write(0).commit();

        let w2 = producer.reserve().unwrap();
        w2.write(0).commit();

        assert!(producer.reserve().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_empty() {
        let (_producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_not_committed() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let w = producer.reserve().unwrap();
        let written = w.write(1);
        // Not committed yet
        assert!(consumer.pop_ref().is_none());
        written.commit();
        assert!(consumer.pop_ref().is_some());
    }

    #[test]
    fn slot_reader_does_not_drop_value() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (producer, mut c1) = RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
        let mut c2 = c1.clone();
        producer
            .push(DropCounter {
                counter: counter.clone(),
            })
            .unwrap();

        // pop_ref from c1, drop the reader
        {
            let reader = c1.pop_ref().unwrap();
            assert_eq!(counter.load(Ordering::Relaxed), 0);
            drop(reader);
        }
        // Value NOT dropped — c2 still needs it
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        // c2 can still clone it
        let val = c2.pop().unwrap();
        drop(val);
        assert_eq!(counter.load(Ordering::Relaxed), 1); // clone dropped
    }

    #[test]
    fn mixed_push_reserve_pop_pop_ref() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(8), 4).split();
        producer.push(1).unwrap();
        let w = producer.reserve().unwrap();
        w.write(2).commit();
        producer.push(3).unwrap();

        assert_eq!(consumer.pop(), Some(1));
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 2);
        drop(reader);
        assert_eq!(consumer.pop(), Some(3));
    }

    #[test]
    fn reserve_pop_ref_wraparound() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for lap in 0u32..3 {
            for j in 0..4 {
                let w = producer.reserve().unwrap();
                w.write(lap * 10 + j).commit();
            }
            for j in 0..4 {
                let reader = consumer.pop_ref().unwrap();
                assert_eq!(*reader, lap * 10 + j);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Drop correctness
    // -----------------------------------------------------------------------

    #[test]
    fn drop_items_on_ringbuffer_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let (producer, _consumer) =
                RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
            for _ in 0..4 {
                producer
                    .push(DropCounter {
                        counter: counter.clone(),
                    })
                    .unwrap();
            }
            // Drop everything — RingBuffer::drop should clean up slots
        }
        assert_eq!(counter.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn consumer_drop_unregisters() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let c2 = c1.clone();
        // Fill buffer
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        // c1 pops all, c2 is slow
        for _ in 0..4 {
            let _ = c1.pop();
        }
        // Can't push because c2 is blocking
        assert!(producer.push(99).is_err());
        // Drop c2 — should unregister, allowing producer to advance
        drop(c2);
        assert!(producer.push(99).is_ok());
    }

    // -----------------------------------------------------------------------
    // Concurrent tests
    // -----------------------------------------------------------------------

    #[test]
    fn concurrent_data_race_check() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let handle = std::thread::spawn(move || {
            for i in 0u64..100 {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        let mut received = 0u64;
        while received < 100 {
            if let Some(val) = consumer.pop() {
                assert_eq!(val, received);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        handle.join().unwrap();
    }

    #[test]
    fn concurrent_broadcast_two_consumers() {
        let (producer, mut c1) = RingBuffer::<u64>::new(Capacity::exact(8), 4).split();
        let mut c2 = c1.clone();
        let total = 100u64;

        let handle = std::thread::spawn(move || {
            for i in 0..total {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let h1 = std::thread::spawn(move || {
            let mut received = 0u64;
            while received < total {
                if let Some(val) = c1.pop() {
                    assert_eq!(val, received);
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let mut received = 0u64;
        while received < total {
            if let Some(val) = c2.pop() {
                assert_eq!(val, received);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        handle.join().unwrap();
        h1.join().unwrap();
    }

    #[test]
    fn concurrent_mpmc_data_race_check() {
        let (p1, mut c1) = RingBuffer::<u64>::new(Capacity::exact(8), 4).split();
        let p2 = p1.clone();
        let mut c2 = c1.clone();
        let items_per_producer = 50u64;

        let h1 = std::thread::spawn(move || {
            for i in 0..items_per_producer {
                while p1.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let h2 = std::thread::spawn(move || {
            for i in 0..items_per_producer {
                while p2.push(1000 + i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let total = items_per_producer * 2;

        let h3 = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                if c1.pop().is_some() {
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let mut count = 0u64;
        while count < total {
            if c2.pop().is_some() {
                count += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    }

    #[test]
    fn concurrent_reserve_pop_ref() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8), 4).split();
        let total = 100u64;

        let handle = std::thread::spawn(move || {
            for i in 0..total {
                loop {
                    if let Some(w) = producer.reserve() {
                        w.write(i).commit();
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        });

        let mut received = 0u64;
        while received < total {
            if let Some(reader) = consumer.pop_ref() {
                assert_eq!(*reader, received);
                drop(reader);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        handle.join().unwrap();
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn stress_broadcast() {
        let (producer, mut c1) = RingBuffer::<u64>::new(Capacity::exact(1024), 8).split();
        let mut c2 = c1.clone();
        let mut c3 = c1.clone();
        let total = 100_000u64;

        let handle = std::thread::spawn(move || {
            for i in 0..total {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let h1 = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                if c1.pop().is_some() {
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let h2 = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                if c2.pop().is_some() {
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let mut count = 0u64;
        while count < total {
            if c3.pop().is_some() {
                count += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        handle.join().unwrap();
        h1.join().unwrap();
        h2.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Audit gap scenarios (Miri-friendly)
    // -----------------------------------------------------------------------

    /// Holding a `SlotReader` blocks the producer's overwrite via `min_head`.
    /// This exercises the liveness contract: as long as a reader exists,
    /// the producer cannot reclaim that slot, so a fill-then-push fails.
    #[test]
    fn pop_ref_blocks_producer_overwrite() {
        let cap = 4u32;
        let (producer, mut consumer) =
            RingBuffer::<u32>::new(Capacity::exact(cap as usize), 4).split();

        for i in 0..cap {
            producer.push(i).unwrap();
        }
        assert!(producer.push(99).is_err(), "buffer is full");

        // Hold the first item via pop_ref — consumer's head does NOT
        // advance until the reader drops.
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 0);

        // Producer still cannot push: this consumer's head is at 0,
        // so min_head == 0 and the buffer appears full.
        assert!(
            producer.push(99).is_err(),
            "producer must be blocked while a reader holds slot 0"
        );

        // Drop the reader → consumer head advances → producer can push.
        drop(reader);
        assert!(producer.push(99).is_ok());

        let _ = consumer.pop().unwrap(); // 1
        let _ = consumer.pop().unwrap(); // 2
        let _ = consumer.pop().unwrap(); // 3
        assert_eq!(consumer.pop(), Some(99));
    }

    /// `RingBuffer::Drop` must drop only slots whose sequence number
    /// indicates real published data — not tombstones.
    #[test]
    fn drop_skips_tombstoned_slots() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let (mut producer, _consumer) =
                RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();

            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();

            // Reserve + drop without commit → tombstone (no value to drop)
            {
                let _w = producer.reserve().unwrap();
            }

            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();

            // Reserve + write but no commit → WrittenSlot::Drop drops
            // the value immediately and tombstones the slot.
            {
                let w = producer.reserve().unwrap();
                w.write(DropCounter {
                    counter: counter.clone(),
                });
            }
            assert_eq!(counter.load(Ordering::Relaxed), 1);

            // Drop the RingBuffer: must drop the 2 published values
            // (positions 0 and 2), skip the 2 tombstoned slots.
        }
        assert_eq!(
            counter.load(Ordering::Relaxed),
            3,
            "1 in-flight + 2 still-published = 3 total drops"
        );
    }

    /// Capacity-1 broadcast with a producer thread + consumer thread.
    #[test]
    fn capacity_one_concurrent() {
        let n = 8u32;
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(1), 4).split();

        let h = std::thread::spawn(move || {
            for i in 0..n {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        let mut got = Vec::new();
        while got.len() < n as usize {
            if let Some(v) = consumer.pop() {
                got.push(v);
            } else {
                std::thread::yield_now();
            }
        }
        h.join().unwrap();
        assert_eq!(got, (0..n).collect::<Vec<_>>());
    }

    // -----------------------------------------------------------------------
    // Async API
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(feature = "async")]
    #[cfg_attr(miri, ignore = "too slow for Miri: threads + tokio runtimes")]
    fn async_push_pop_cross_thread() {
        async_push_pop_cross_thread_run(1, 5_000, 8, 60);
    }

    #[test]
    #[cfg(feature = "async")]
    #[ignore = "slow stress: 20k iters x 1k items x 2 consumers, ~40s alone"]
    fn async_push_pop_cross_thread_iters_unsaturated() {
        // Stress: many iterations of (push 1k items, drop producer) with
        // cap >> total so the producer never parks. Catches register/close
        // races that single-iter tests miss.
        async_push_pop_cross_thread_run(20_000, 1_000, 4096, 60);
    }

    #[cfg(feature = "async")]
    fn async_push_pop_cross_thread_run(iters: usize, total: u64, cap: usize, _deadline_secs: u64) {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let progress = Arc::new(AtomicU64::new(0));
        let _watchdog = crate::common::progress_watchdog::ProgressWatchdog::spawn(
            progress.clone(),
            "broadcast async_push_pop_cross_thread",
        );

        for _ in 0..iters {
            let (producer, c1) = RingBuffer::<u64>::new(Capacity::exact(cap), 4).split();
            let c2 = c1.clone();

            let consumer_threads: Vec<_> = [c1, c2]
                .into_iter()
                .map(|mut c| {
                    let progress = progress.clone();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .unwrap();
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            let mut received = 0u64;
                            while c.pop_async().await.is_some() {
                                received += 1;
                                progress.fetch_add(1, Ordering::Relaxed);
                            }
                            assert_eq!(received, total);
                        }));
                    })
                })
                .collect();

            let ph = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let local = tokio::task::LocalSet::new();
                rt.block_on(local.run_until(async move {
                    for i in 0..total {
                        producer.push_async(i).await.expect("all consumers dropped");
                    }
                    // Producer dropped here → consumers' pop_async resolve to None
                    // after they drain.
                }));
            });

            ph.join().unwrap();
            for h in consumer_threads {
                h.join().unwrap();
            }
        }

    }

    /// A `Waker` is user code and may panic. `commit` publishes the
    /// slot and only then wakes, so an unwind out of the wake path must
    /// not run the guard's rollback — that would drop and tombstone a
    /// value the consumers can already see.
    #[test]
    #[cfg(feature = "async")]
    fn panicking_waker_during_commit_does_not_drop_published_value() {
        use std::task::{Context, RawWaker, RawWakerVTable, Waker};

        struct PanicWaker;

        impl PanicWaker {
            const VTABLE: RawWakerVTable = RawWakerVTable::new(
                |_| RawWaker::new(std::ptr::null(), &Self::VTABLE),
                |_| panic!("waker failure"),
                |_| panic!("waker failure"),
                |_| {},
            );

            fn waker() -> Waker {
                // SAFETY: the vtable's functions are consistent with a
                // stateless waker carrying a null data pointer.
                unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &Self::VTABLE)) }
            }
        }

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ring = RingBuffer::<crate::common::DropCounter>::new(Capacity::exact(4), 4);

        let waker = PanicWaker::waker();
        ring.consumer_waker.register(
            crate::common::park_registry::ParkSlot::from_exclusive_index(0),
            &Context::from_waker(&waker),
        );

        let (mut producer, mut consumer) = ring.split();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let w = producer.reserve().unwrap();
            w.write(crate::common::DropCounter {
                counter: std::sync::Arc::clone(&counter),
            })
            .commit();
        }));
        assert!(result.is_err(), "the waker panic must propagate");

        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the committed value must survive the wake-path unwind"
        );
        assert!(
            consumer.pop().is_some(),
            "the committed value must still reach the consumer"
        );
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_pop_returns_none_on_close() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(producer);
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            assert_eq!(consumer.pop_async().await, None);
        }));
        h.join().unwrap();
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_push_returns_err_on_consumer_close() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        // Fill so the producer must park on the next push.
        for i in 0..4u64 {
            producer.push(i).unwrap();
        }
        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(consumer);
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            assert_eq!(producer.push_async(99).await, Err(99));
        }));
        h.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Blocking push_block / reserve_block (sync park, no busy spin)
    // -----------------------------------------------------------------------

    #[test]
    fn push_block_empty_succeeds_immediately() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert!(producer.push_block(1).is_ok());
        assert!(producer.push_block(2).is_ok());
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
    }

    #[test]
    fn push_block_parks_until_consumer_advances() {
        // cap=2: fill it, then a blocking push must park until the
        // consumer pops, freeing a slot. No value is lost or spun on.
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        producer.push(10).unwrap();
        producer.push(20).unwrap();
        assert!(producer.is_full());

        let h = std::thread::spawn(move || {
            // Blocks here: ring is full. Wakes when main pops below.
            producer.push_block(30).unwrap();
            producer
        });

        // Give the producer time to actually park rather than win a race.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(consumer.pop(), Some(10)); // frees a slot, wakes producer

        let producer = h.join().unwrap();
        // The parked push went through.
        assert_eq!(consumer.pop(), Some(20));
        assert_eq!(consumer.pop(), Some(30));
        drop(producer);
    }

    #[test]
    fn push_block_returns_err_when_all_consumers_gone() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert!(producer.is_full());

        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(consumer); // last consumer gone → producer must unblock with Err
        });

        // Parks (full), then the consumer drop flushes it; no consumer
        // means the value can never be observed, so Err is returned.
        assert_eq!(producer.push_block(99), Err(99));
        h.join().unwrap();
    }

    #[test]
    fn reserve_block_parks_until_consumer_advances() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        producer.push(10).unwrap();
        producer.push(20).unwrap();

        let h = std::thread::spawn(move || {
            let slot = producer.reserve_block().expect("consumer still active");
            slot.write(30).commit();
            producer
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(consumer.pop(), Some(10));

        let producer = h.join().unwrap();
        assert_eq!(consumer.pop(), Some(20));
        assert_eq!(consumer.pop(), Some(30));
        drop(producer);
    }

    /// Several producers push concurrently while the only consumer is
    /// dropped mid-flight.
    ///
    /// A producer that has already claimed `pos`, but is waiting
    /// for the slot's previous occupant to be released, asks `min_head()`
    /// whether it may proceed. With no consumer left, `min_head()`
    /// reports `tail` — which by then is *past* `pos` — so the
    /// `pos - min_head` backlog check underflows to a huge value that is
    /// always `>= cap`, and the producer spins forever.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "detects an infinite spin via a wall-clock budget, which Miri's interpretation speed invalidates"
    )]
    fn push_races_last_consumer_drop_and_terminates() {
        const PRODUCERS: usize = 16;

        for _ in 0..20 {
            let (seed, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 2).split();

            let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut handles = Vec::new();
            for _ in 0..PRODUCERS {
                let p = seed.clone();
                let done_in = std::sync::Arc::clone(&done);
                handles.push(std::thread::spawn(move || {
                    for i in 0..2000u64 {
                        let _ = p.push(i);
                    }
                    done_in.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }));
            }

            for _ in 0..64 {
                let _ = consumer.pop();
            }
            drop(consumer);

            for _ in 0..500 {
                if done.load(std::sync::atomic::Ordering::SeqCst) == PRODUCERS {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert_eq!(
                done.load(std::sync::atomic::Ordering::SeqCst),
                PRODUCERS,
                "a producer spun forever after the last consumer went away"
            );
            for h in handles {
                h.join().unwrap();
            }
        }
    }

    /// `push` is documented to return `Err(val)` when the ring is full,
    /// so it must not block. The fullness pre-check is not atomic with
    /// the `tail.fetch_add` that follows it, so concurrent producers can
    /// all pass the check and claim positions beyond capacity; the
    /// losers then wait for a consumer that may never advance.
    #[test]
    fn push_does_not_block_when_full_under_contention() {
        const PRODUCERS: usize = 8;

        // Exactly one free slot, and a consumer that never advances.
        // Every producer's pre-check sees room, so they all reach the
        // `fetch_add`; only one can legitimately have it.
        let (seed, _consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 2).split();
        seed.push(1).unwrap();

        let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let start = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handles = Vec::new();
        for _ in 0..PRODUCERS {
            let p = seed.clone();
            let done_in = std::sync::Arc::clone(&done);
            let start_in = std::sync::Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                while !start_in.load(std::sync::atomic::Ordering::SeqCst) {
                    std::hint::spin_loop();
                }
                let _ = p.push(99);
                done_in.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }
        start.store(true, std::sync::atomic::Ordering::SeqCst);

        for _ in 0..200 {
            if done.load(std::sync::atomic::Ordering::SeqCst) == PRODUCERS {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            done.load(std::sync::atomic::Ordering::SeqCst),
            PRODUCERS,
            "non-blocking push blocked on a full ring"
        );
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn push_block_multi_consumer_waits_for_slowest() {
        // Two consumers; the producer can't advance past the slowest.
        // Draining only the fast consumer must NOT unblock the producer;
        // the slow consumer popping is what frees the slot.
        let (producer, mut fast) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        let mut slow = fast.clone();
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        let h = std::thread::spawn(move || {
            producer.push_block(3).unwrap();
            producer
        });

        std::thread::sleep(std::time::Duration::from_millis(30));
        // Fast consumer drains fully; min_head still pinned by `slow`.
        assert_eq!(fast.pop(), Some(1));
        assert_eq!(fast.pop(), Some(2));
        std::thread::sleep(std::time::Duration::from_millis(30));
        // Now the slow consumer advances, raising min_head → producer wakes.
        assert_eq!(slow.pop(), Some(1));

        let producer = h.join().unwrap();
        assert_eq!(slow.pop(), Some(2));
        assert_eq!(slow.pop(), Some(3));
        assert_eq!(fast.pop(), Some(3));
        drop(producer);
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow for Miri")]
    fn push_block_streams_far_past_capacity() {
        // Regression for the relay burst hang: a single producer pushes
        // many multiples of capacity through push_block while two
        // consumers drain. Pre-fix, push_block delegated to push, whose
        // claim_slot did an unconditional FAA then spun unboundedly on a
        // full ring — so the producer wedged instead of parking. The wrap
        // (N ≫ cap) is what exercises the "claimed slot still occupied"
        // branch.
        const CAP: usize = 16;
        const N: u64 = 100_000;
        let (producer, c1) = RingBuffer::<u64>::new(Capacity::exact(CAP), 2).split();
        let c2 = c1.clone();

        let prod = std::thread::spawn(move || {
            for i in 0..N {
                producer.push_block(i).expect("consumers alive");
            }
        });

        // Move each consumer into its own drain thread (no lingering
        // handle that would pin min_head at 0 and stall the producer).
        let drain = |mut c: Consumer<u64>| {
            std::thread::spawn(move || {
                let mut next = 0u64;
                while next < N {
                    match c.pop() {
                        Some(v) => {
                            assert_eq!(v, next, "broadcast consumer saw out-of-order item");
                            next += 1;
                        }
                        None => std::thread::yield_now(),
                    }
                }
            })
        };
        let d1 = drain(c1);
        let d2 = drain(c2);

        prod.join().unwrap();
        d1.join().unwrap();
        d2.join().unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow for Miri")]
    fn push_block_multi_producer_streams_past_capacity() {
        // Mirrors the SubRepl ring: N producers (one per shard) sharing
        // one broadcast ring, blocking-pushing concurrently while
        // consumers drain. Verifies the multi-producer race path in
        // push_block (has_space true, then a peer takes the slot → Err →
        // loop) terminates rather than wedging or recursing.
        const CAP: usize = 8;
        const PER: u64 = 20_000;
        const PRODUCERS: u64 = 3;
        // One consumer only: a second idle consumer would pin min_head at
        // 0 and (correctly) block the producers forever.
        let (seed, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(CAP), 2).split();

        // Producer is !Sync (per-handle Cell cache + park slot), so each
        // thread takes its own clone — same as the relay's per-shard
        // SubRepl producers.
        let mut prod_handles = Vec::new();
        for _ in 0..PRODUCERS {
            let p = seed.clone();
            prod_handles.push(std::thread::spawn(move || {
                for i in 0..PER {
                    p.push_block(i).expect("consumer alive");
                }
            }));
        }
        drop(seed);

        let total = PER * PRODUCERS;
        let drained = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                match consumer.pop() {
                    Some(_) => count += 1,
                    None => std::thread::yield_now(),
                }
            }
            count
        });

        for h in prod_handles {
            h.join().unwrap();
        }
        assert_eq!(drained.join().unwrap(), total);
    }
}
