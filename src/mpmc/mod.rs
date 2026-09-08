//! Multi-producer, multi-consumer ring with relaxed-FIFO ordering.
//!
//! Producers reserve batches of logical positions via FAA on a shared
//! `claim` cursor, then publish out-of-order into whichever slot in
//! their batch becomes free first. Consumers scan the ring from a
//! private cursor and CAS-claim the first published slot they see.
//! No shared consumer-side cursor exists — contention is distributed
//! across `cap` per-slot atomics rather than concentrated on a single
//! `head` line.
//!
//! # Ordering guarantees
//!
//! - **No FIFO** across producers, across consumers, or even within
//!   a single producer's own stream. Items are returned in the order
//!   their slots become published (which depends on consumer release
//!   order, scheduling, and contention).
//! - Use [`spsc`](crate::spsc) or [`spmc`](crate::spmc) if strict
//!   ordering matters.
//!
//! # Capacity
//!
//! Requires `cap >= 4` (the per-slot tri-state encoding aliases at
//! `cap < 3`). [`RingBuffer::new`] panics on smaller capacities.
//!
//! # Thread-count guidance
//!
//! Producers fall back to OS-level park (futex-style wake bitmap)
//! when their batch's slots aren't yet released by consumers. This
//! lets the kernel reschedule the core to a runnable consumer
//! instead of burning cycles on a spin loop.
//!
//! Even so, throughput becomes **highly variable** when the total
//! thread count (P producers + Q consumers) saturates the machine.
//! On an N-physical-core SMT machine you have 2N logical CPUs; once
//! P + Q approaches 2N, every spinning thread shares decode
//! bandwidth with its SMT sibling and OS scheduling decisions
//! dominate run-to-run variance.
//!
//! Rules of thumb:
//! - **P + Q ≤ N (physical cores)**: tight, predictable throughput.
//! - **N < P + Q < 2N**: still good, mild SMT-pairing variance.
//! - **P + Q ≈ 2N**: bimodal — peak throughput is high, but worst
//!   case can be 5–10× slower depending on how the scheduler maps
//!   threads onto SMT-paired cores. The futex park/unpark mitigates
//!   this but doesn't eliminate it.
//! - **P + Q > 2N**: oversubscription — threads time-slice on
//!   spin loops and throughput collapses. Avoid this regime.
//!
//! In practice, size your producer + consumer pool to leave at
//! least a few logical CPUs idle for the OS and any background
//! work. Benchmark the specific (P, Q) shape you intend to deploy;
//! mismatched shapes (e.g. P=8, Q=4) often outperform balanced
//! ones at the same total thread count because there's scheduling
//! slack.

mod awaited_batch;
mod backstop_monitor;
mod batch_abandon;
mod config;
mod consumed_watermark;
mod consumer;
mod done_word;
mod drain_wake;
mod producer;
mod ready_word;
mod reservation_return;
#[cfg(feature = "backstop-metrics")]
mod rescue_evidence;
#[cfg(test)]
mod ring_snapshot;
mod scan_budget;
mod slot_release;

pub use config::{Cfg, Config, DefaultConfig};
pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};

use awaited_batch::AwaitedBatch;
use config::ConfigBounds;
use consumed_watermark::ConsumedWatermark;
use done_word::DoneWord;
use ready_word::{ReadyState, ReadyWord};

use crate::capacity::Capacity;
use crate::common::park::WakeSet;
use crate::common::close_state::CloseState;
use crate::common::park_registry::ParkRegistry;

#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::endpoint_count::EndpointCount;
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(C)]
pub struct RingBuffer<T, C: Config = DefaultConfig> {
    pub(crate) data: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    /// Per-slot tri-state with round encoding (free / published /
    /// claimed). Packed 8 per cacheline (not `CachePadded`) so a
    /// consumer scan amortizes one line load across 8 slots.
    pub(in crate::mpmc) ready: AlignedBuf<ReadyWord>,
    /// Per-slot "consumer released" marker, handing each slot from one
    /// round's consumer to the next round's producer.
    pub(in crate::mpmc) done: AlignedBuf<DoneWord>,
    pub(crate) capacity: Capacity,
    /// Producer claim cursor. FAA'd to reserve batches of positions.
    pub(crate) claim: CachePadded<AtomicUsize>,
    /// Coarse "items consumed" watermark — a lower bound, flushed
    /// from per-consumer locals every `CONSUMED_FLUSH` pops.
    /// Producers use it to bound their per-batch FAA on `claim`.
    consumed: ConsumedWatermark,
    /// Live producer count; last-drop sets `producer_closed`.
    pub(crate) producer_count: EndpointCount,
    /// Live consumer count; last-drop sets `consumer_closed`.
    pub(crate) consumer_count_live: EndpointCount,
    /// Set by the last `Consumer` drop. `Producer::push_block`
    /// observes this and returns `Err(val)` instead of hanging
    /// (no consumer left to drain).
    pub(crate) consumer_closed: CloseState,
    /// Monotonic counter, bumped on each `Consumer::clone`. Used
    /// to stagger new consumers' starting scan offsets.
    pub(crate) clone_counter: CachePadded<AtomicUsize>,
    pub(crate) closed: CloseState,
    /// Producer-side park state. Bit `i` of `producer_park.wake` is
    /// set ↔ a producer in slot `i` is parked waiting for a `done[s]`
    /// release. Consumers wake one parked producer after each release
    /// (gated on the bitmap being non-zero).
    pub(crate) producer_park: WakeSet,
    /// What each parked producer is waiting for, by park slot, so a
    /// release can be routed to the producer it frees; see
    /// [`awaited_batch`].
    pub(in crate::mpmc) awaited: AlignedBuf<AwaitedBatch>,
    /// Consumer-side park state. Bit `i` of `consumer_park.wake` is
    /// set ↔ a consumer in slot `i` is parked waiting for any
    /// `ready[s]` publish. Producers wake one parked consumer after
    /// each publish (gated on the bitmap being non-zero).
    pub(crate) consumer_park: WakeSet,
    /// Leases park-slot indices to `Producer` handles, reclaiming each
    /// on drop so a slot is never shared by two live producers.
    pub(crate) producer_slots: ParkRegistry,
    /// Consumer counterpart of `producer_slots`.
    pub(crate) consumer_slots: ParkRegistry,
    /// Async equivalent of `producer_park`: per-producer-slot wakers
    /// registered from `Poll::Pending` in `push_async`. Woken by any
    /// consumer after a successful pop / commit / batched drain.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Async equivalent of `consumer_park`: per-consumer-slot wakers
    /// registered from `Poll::Pending` in `pop_async`. Woken by any
    /// producer after a successful push / commit.
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
    /// Backstop timeout counters. Present only under the diagnostic
    /// feature; see [`backstop_monitor`].
    #[cfg(feature = "backstop-metrics")]
    pub(crate) backstop: backstop_monitor::BackstopMonitor,
    pub(crate) _config: std::marker::PhantomData<fn() -> C>,
}

// SAFETY: All shared state is atomic; per-slot CAS gives unique
// ownership of each (slot, round) pair to exactly one consumer.
// `C` is a ZST type marker — its Send/Sync are irrelevant.
unsafe impl<T: Send, C: Config> Send for RingBuffer<T, C> {}
unsafe impl<T: Send, C: Config> Sync for RingBuffer<T, C> {}

impl<T, C: Config> RingBuffer<T, C> {
    /// Creates a new MPMC ring buffer.
    ///
    /// # Panics
    ///
    /// Panics if `capacity.get() < 4`. The per-slot tri-state
    /// encoding aliases at smaller capacities.
    ///
    /// `C`'s tunables are validated at monomorphization via
    /// [`ConfigBounds`]: `PRODUCER_BATCH` must be in `1..=32`,
    /// the others must be `>= 1`.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        // Force monomorphization-time evaluation of the bounds.
        let () = ConfigBounds::<C>::VALIDATE;
        let cap = capacity.get();
        assert!(cap >= 4, "mpmc requires capacity >= 4");
        let data = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        let mut ridx = 0usize;
        let ready = AlignedBuf::new_with(cap, || {
            let r = ReadyWord::free_at(ridx);
            ridx += 1;
            r
        });
        let mut didx = 0usize;
        let done = AlignedBuf::new_with(cap, || {
            let d = DoneWord::released_for(didx);
            didx += 1;
            d
        });

        Self {
            data,
            ready,
            done,
            claim: CachePadded(AtomicUsize::new(0)),
            consumed: ConsumedWatermark::new(),
            producer_count: EndpointCount::new(),
            consumer_count_live: EndpointCount::new(),
            clone_counter: CachePadded(AtomicUsize::new(0)),
            closed: CloseState::new(),
            consumer_closed: CloseState::new(),
            capacity,
            producer_park: WakeSet::new(),
            awaited: AlignedBuf::new_with(crate::common::park::PARK_SLOTS, AwaitedBatch::new),
            producer_slots: ParkRegistry::new(),
            consumer_slots: ParkRegistry::new(),
            consumer_park: WakeSet::new(),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
            #[cfg(feature = "backstop-metrics")]
            backstop: backstop_monitor::BackstopMonitor::new(),
            _config: std::marker::PhantomData,
        }
    }

    /// Starts watching one waiter's park loop for backstop rescues.
    ///
    /// Call sites are unconditional; this pair is where the
    /// `backstop-metrics` feature enters the park path.
    #[cfg(feature = "backstop-metrics")]
    pub(crate) const fn watch_backstop_producer(&self) -> backstop_monitor::BackstopWatch {
        // SAFETY: the monitor lives in this ring, which every caller
        // holds through an `Arc` for the whole of its park loop.
        unsafe {
            backstop_monitor::BackstopWatch::new(
                std::ptr::from_ref(&self.backstop),
                backstop_monitor::WaiterSide::Producer,
            )
        }
    }

    #[cfg(feature = "backstop-metrics")]
    pub(crate) const fn watch_backstop_consumer(&self) -> backstop_monitor::BackstopWatch {
        // SAFETY: as above.
        unsafe {
            backstop_monitor::BackstopWatch::new(
                std::ptr::from_ref(&self.backstop),
                backstop_monitor::WaiterSide::Consumer,
            )
        }
    }

    #[cfg(not(feature = "backstop-metrics"))]
    #[allow(
        clippy::unused_self,
        reason = "signature mirrors the instrumented twin so call sites need no cfg"
    )]
    pub(crate) const fn watch_backstop_producer(&self) -> backstop_monitor::BackstopWatch {
        backstop_monitor::BackstopWatch
    }

    #[cfg(not(feature = "backstop-metrics"))]
    #[allow(
        clippy::unused_self,
        reason = "signature mirrors the instrumented twin so call sites need no cfg"
    )]
    pub(crate) const fn watch_backstop_consumer(&self) -> backstop_monitor::BackstopWatch {
        backstop_monitor::BackstopWatch
    }

    /// Reads this ring's backstop counters.
    ///
    /// A non-zero rescue count means the timeout released a waiter that
    /// had work available, which is what a lost wake looks like from
    /// the outside.
    #[cfg(feature = "backstop-metrics")]
    #[must_use]
    pub fn backstop_stats(&self) -> backstop_monitor::BackstopStats {
        self.backstop.stats()
    }

    /// Wakes the parked producer that reserved `pos`, if there is one,
    /// and otherwise whichever producer round-robin picks.
    ///
    /// Every release of a slot goes through here. A producer parked
    /// holding a partial batch can only use the release of one of its
    /// own positions, and a wake spent on any other producer leaves
    /// it parked with its slot free.
    ///
    /// Routing reads each parked producer's announcement as a
    /// three-way answer, because "did not reserve `pos`" hides two
    /// different producers: one waiting for a refill, which any
    /// release serves, and one pinned to other positions, which this
    /// release cannot serve at all. Waking the latter spends the
    /// release on a producer that re-parks immediately.
    #[inline]
    pub(crate) fn wake_producer_for(&self, pos: usize) {
        self.producer_park
            .wake_one_interested(|slot| self.awaited[slot].interest(pos));
    }

    /// Wakes the async producer tasks waiting for space.
    ///
    /// Call sites are unconditional; this family is where the async
    /// feature enters the notification path.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_producers(&self) {
        self.producer_waker.wake_all();
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_producers(&self) {}

    /// Wakes up to `n` async producer tasks.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_producers_n(&self, n: usize) {
        self.producer_waker.wake_n(n);
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_producers_n(&self, _n: usize) {}

    /// Wakes the async consumer tasks waiting for a publication.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_consumers(&self) {
        self.consumer_waker.wake_all();
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_consumers(&self) {}

    #[inline]
    pub(crate) fn data_slot(&self, pos: usize) -> &UnsafeCell<MaybeUninit<T>> {
        let idx = self.capacity.index_of(pos);
        // SAFETY: index_of < cap == data.len().
        unsafe { std::hint::assert_unchecked(idx < self.data.len()) };
        &self.data[idx]
    }

    #[inline]
    pub(in crate::mpmc) fn ready_slot(&self, pos: usize) -> &ReadyWord {
        let idx = self.capacity.index_of(pos);
        unsafe { std::hint::assert_unchecked(idx < self.ready.len()) };
        &self.ready[idx]
    }

    /// The state of the slot serving `pos`, decoded.
    #[inline]
    pub(in crate::mpmc) fn ready_state(&self, pos: usize) -> ReadyState {
        self.ready_slot(pos)
            .state(self.capacity.index_of(pos), self.capacity)
    }

    #[inline]
    pub(in crate::mpmc) fn done_slot(&self, pos: usize) -> &DoneWord {
        let idx = self.capacity.index_of(pos);
        unsafe { std::hint::assert_unchecked(idx < self.done.len()) };
        &self.done[idx]
    }

    /// Diagnostic snapshot: `(claim, ready[..], done[..])`.
    #[doc(hidden)]
    pub fn debug_snapshot(&self) -> (usize, Vec<usize>, Vec<usize>) {
        let claim = self.claim.load(Ordering::Acquire);
        let r: Vec<usize> = (0..self.capacity.get())
            .map(|s| self.ready[s].word())
            .collect();
        let d: Vec<usize> = (0..self.capacity.get())
            .map(|s| self.done[s].word())
            .collect();
        (claim, r, d)
    }

    /// Diagnostic snapshot of park-state bitmaps.
    #[doc(hidden)]
    pub fn debug_park_snapshot(&self) -> (u64, u64) {
        (
            self.producer_park.wake.load(Ordering::Acquire),
            self.consumer_park.wake.load(Ordering::Acquire),
        )
    }


    /// How many producers are currently parked.
    ///
    /// A bit is set per parked producer, so the population count is
    /// the number of waiters. Reads a bitmap that peers mutate, so
    /// treat it as a sample rather than an invariant.
    #[doc(hidden)]
    pub fn parked_producer_count(&self) -> u32 {
        self.producer_park.wake.load(Ordering::Acquire).count_ones()
    }

    /// Splits the ring into a [`Producer`] and a [`Consumer`].
    /// Both handles are cloneable for additional producer/consumer
    /// threads.
    #[must_use]
    pub fn split(self) -> (Producer<T, C>, Consumer<T, C>) {
        let arc = Arc::new(self);
        let producer = Producer::new(arc.clone());
        let consumer = Consumer::new(arc);
        (producer, consumer)
    }
}

impl<T, C: Config> Drop for RingBuffer<T, C> {
    fn drop(&mut self) {
        // All Producer/Consumer handles are gone (we hold &mut self),
        // so the words can be read non-atomically.
        let capacity = self.capacity;
        for s in 0..capacity.get() {
            let state = self.ready[s].state_mut(s, capacity);
            let holds_value = match state {
                // Published and never claimed: initialized, never moved.
                ReadyState::Published(_) => true,
                // Claimed, but the consumer never released the slot, so
                // the value is still in it. A release means either the
                // consumer finished reading or a producer abandoned a
                // reservation it had not written.
                ReadyState::Claimed(round_pos) => {
                    !self.done[s].was_released_after(round_pos, capacity)
                }
                ReadyState::Free(_) => false,
            };
            if holds_value {
                // SAFETY: the state above says the slot holds a value
                // that no consumer took.
                unsafe {
                    self.data[s].get().cast::<T>().drop_in_place();
                }
            }
        }
        // ThreadParker entries in the park tables free their handles.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::Capacity;
    use crate::common::park_probe::ParkProbe;
    use crate::common::park_registry::ParkSlot;

    #[test]
    #[should_panic(expected = "mpmc requires capacity >= 4")]
    fn rejects_small_capacity() {
        let _ = RingBuffer::<u8>::new(Capacity::exact(2));
    }

    /// A counter that never fires proves nothing unless it *can* fire.
    /// This stages a lost wake: a consumer parks, a producer publishes
    /// but its wake is stolen before it reaches the parked thread. With
    /// no timeout on the park, something else has to end the sleep for
    /// the test to observe anything, so the test unparks the thread
    /// itself — bypassing the ring, exactly as a spurious wakeup would.
    /// The consumer then finds work with its handle still armed. That
    /// is a rescue, and the monitor must say so.
    #[test]
    #[cfg(feature = "backstop-metrics")]
    fn the_monitor_counts_a_wake_that_never_arrived() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let ring = std::sync::Arc::clone(&producer.queue);

        let reader = std::thread::spawn(move || consumer.pop_block());

        // Wait for the consumer to commit to parking, then clear its
        // wake bit. `wake_one` reads the bitmap to decide who to
        // unpark, so the publish below finds nobody and the parked
        // thread's handle is left armed — a wake that goes missing.
        //
        // Clearing the bit is the one way to stage this without
        // going through the ring's wake path: claiming the handle
        // would deliver the very unpark whose absence is the point.
        ParkProbe::expect_until("the consumer to park", || {
            ring.consumer_park.wake.load(Ordering::Relaxed) != 0
        });
        ring.consumer_park.wake.store(0, Ordering::SeqCst);
        producer.push(7).unwrap();
        reader.thread().unpark();

        assert_eq!(reader.join().unwrap(), Some(7));

        let stats = ring.backstop_stats();
        assert!(
            stats.saw_unexplained_rescue(),
            "the consumer was the only waiter, so nothing can explain the lost wake away: {stats:?}"
        );
        assert!(
            stats.saw_rescue(),
            "the backstop released a waiter whose wake went missing, so it must be counted: {stats:?}"
        );
        assert_eq!(
            stats.bit_taken_rescues, 1,
            "a cleared bit over an armed handle is what a stolen wake leaves behind, \
             and is why that reading cannot be treated as benign: {stats:?}"
        );
    }

    /// The futile-wake counter must be able to fire, and must not fire
    /// on an ordinary wake that the waiter used.
    ///
    /// A consumer parks on an empty ring; a bare `wake_one` with
    /// nothing published delivers a real unpark that buys the consumer
    /// nothing, so it loops and parks again. That is a futile wake. The
    /// later `push` ends the call for real and must not add another.
    #[test]
    #[cfg(feature = "backstop-metrics")]
    fn the_monitor_counts_a_wake_that_bought_the_waiter_nothing() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let ring = std::sync::Arc::clone(&producer.queue);

        let reader = std::thread::spawn(move || consumer.pop_block());

        ParkProbe::expect_until("the consumer to park", || {
            ring.consumer_park.wake.load(Ordering::Relaxed) != 0
        });
        // A real unpark, with nothing for the consumer to find.
        ring.consumer_park.wake_one();
        ParkProbe::expect_until("the consumer to park again", || {
            ring.consumer_park.wake.load(Ordering::Relaxed) != 0
        });

        producer.push(7).unwrap();
        assert_eq!(reader.join().unwrap(), Some(7));

        let stats = ring.backstop_stats();
        assert!(
            stats.saw_futile_wake(),
            "the consumer was woken, found nothing, and re-parked: {stats:?}"
        );
        assert_eq!(
            stats.futile_wakes, 1,
            "the push that ended the call delivered a wake the consumer used: {stats:?}"
        );
        assert!(
            !stats.saw_blind_futile_wake(),
            "the ring was empty when the consumer re-parked, so it was not \
             blind to anything: {stats:?}"
        );
    }

    /// A producer holding a partial batch is pinned to one exact
    /// position, and freeing every *other* slot in the ring does not
    /// release it.
    ///
    /// This is the claim the "round-robin explains it" dismissal needs
    /// to be false, so it is built without threads, timing or parking:
    /// only the batch and slot bookkeeping decide the outcome.
    ///
    /// A held `SlotReader` keeps slot 0 unreleased while its
    /// neighbours are consumed, which is what lets a later batch span
    /// a blocked slot and a free one. The producer publishes into the
    /// free half out of order and is left holding the blocked half.
    #[test]
    fn a_producer_holding_a_partial_batch_is_pinned_to_its_own_position() {
        let (producer, consumer) =
            RingBuffer::<u64, Cfg<2, 128, 1>>::new(Capacity::exact(4)).split();
        let filler = producer.clone();
        // Both consumers start at slot 0 (the stagger is capacity/8,
        // which floors to 0 here), so claim order is deterministic.
        let drainer = consumer.clone();
        let mut holder = consumer;

        producer.push(0).unwrap();
        producer.push(1).unwrap();
        filler.push(2).unwrap();
        filler.push(3).unwrap();

        // Slot 0 stays unreleased for as long as this lives.
        let held = holder.pop_ref().unwrap();
        assert_eq!(*held, 0);

        assert_eq!(drainer.pop(), Some(1));
        assert_eq!(drainer.pop(), Some(2));

        // Two slots free, so the next batch is positions 4 and 5.
        // Position 4 is slot 0 (still held); position 5 is slot 1.
        // The producer publishes into 5 and keeps 4 reserved.
        producer.push(99).unwrap();
        assert!(
            producer.push(100).is_err(),
            "the batch's remaining position is slot 0, which is still held"
        );

        // Free every other slot in the ring.
        let mut freed = vec![drainer.pop(), drainer.pop()];
        freed.sort_unstable();
        assert_eq!(freed, vec![Some(3), Some(99)]);

        assert!(
            producer.push(100).is_err(),
            "three of four slots are now free, and none of them is the \
             position this producer reserved: a wake delivered here buys \
             nothing"
        );

        drop(held);
        assert!(
            producer.push(100).is_ok(),
            "only the producer's own position releases it"
        );
    }

    /// The consequence of the pin: a release of a position the sole
    /// parked producer did not reserve must not be delivered to it.
    ///
    /// `pinned` holds position 4 (slot 0) and is parked in
    /// `push_block`. The freed position is 5 (slot 1), which it cannot
    /// use. Round-robin routing spent the wake on it and it re-parked,
    /// which the futile-wake counter recorded; routing on a yes/no
    /// predicate did the same, because "did not reserve 5" fell
    /// through to the cursor's pick and the cursor pointed back at the
    /// only waiter there was.
    ///
    /// Reading the announcement as three-way ends that: `pinned`
    /// declines position 5 outright, and a release no parked producer
    /// can use wakes nobody. The waiter stays parked until its own
    /// position frees, and the wake is not spent proving it could not
    /// be used.
    ///
    /// The counter is the evidence, so it is asserted to stay at zero
    /// across the whole episode rather than merely at the end.
    #[test]
    #[cfg(feature = "backstop-metrics")]
    fn a_release_a_pinned_producer_cannot_use_does_not_reach_it() {
        let (producer, consumer) =
            RingBuffer::<u64, Cfg<2, 128, 1>>::new(Capacity::exact(4)).split();
        let filler = producer.clone();
        let ring = std::sync::Arc::clone(&producer.queue);
        let drainer = consumer.clone();
        let mut holder = consumer;

        producer.push(0).unwrap();
        producer.push(1).unwrap();
        filler.push(2).unwrap();
        filler.push(3).unwrap();

        let held = holder.pop_ref().unwrap();
        assert_eq!(*held, 0);
        assert_eq!(drainer.pop(), Some(1));
        assert_eq!(drainer.pop(), Some(2));

        producer.push(99).unwrap();
        assert!(producer.push(100).is_err(), "pinned to slot 0");

        let park_slot = producer.park_slot;
        let pinned = std::thread::spawn(move || {
            producer.push_block(100).expect("consumers dropped");
        });

        ParkProbe::expect_until("the pinned producer to park", || {
            ring.producer_park.wake.load(Ordering::Relaxed) != 0
        });

        // Frees position 5 (slot 1). The only parked producer needs
        // slot 0, so this release has no taker.
        //
        // Routing decides and clears the wake bit inside `pop`, on
        // this thread, so the check needs no waiting: if the release
        // had been delivered, the bit would already be gone.
        assert_eq!(drainer.pop(), Some(3));

        assert!(
            ring.producer_park.is_armed(park_slot),
            "a release the only parked producer had not reserved was \
             delivered to it anyway"
        );

        // Its own position at last.
        drop(held);
        pinned.join().unwrap();

        let stats = ring.backstop_stats();
        assert_eq!(
            stats.futile_wakes, 0,
            "no wake should have been spent on a producer that could not \
             use the freed position: {stats:?}"
        );
    }

    /// Two producers parked, one pinned to the freed position and one
    /// pinned elsewhere: the release must wake the first, whatever the
    /// round-robin cursor would have picked. This is the deadlock the
    /// 1 ms backstop used to hide — with the wrong producer woken and
    /// no further release coming, the right one slept for good.
    ///
    /// Both park slots are given `Leased` indices in cursor order, so
    /// the cursor's pick is the *wrong* producer; only routing gets
    /// the wake to the right one.
    #[test]
    fn a_release_wakes_the_producer_that_reserved_the_position() {
        let (producer, consumer) =
            RingBuffer::<u64, Cfg<2, 128, 1>>::new(Capacity::exact(4)).split();
        let other = producer.clone();
        let other_park_slot = other.park_slot;
        let ring = std::sync::Arc::clone(&producer.queue);
        let drainer = consumer.clone();
        let mut holder = consumer;

        producer.push(0).unwrap();
        producer.push(1).unwrap();
        other.push(2).unwrap();
        other.push(3).unwrap();

        // Slot 0 is held; slots 1 and 2 are consumed and released.
        let held = holder.pop_ref().unwrap();
        assert_eq!(*held, 0);
        assert_eq!(drainer.pop(), Some(1));
        assert_eq!(drainer.pop(), Some(2));

        // `producer` takes batch {4, 5}: publishes 5 (slot 1), keeps 4
        // (slot 0, held). `other` then takes batch {6, 7}: 6 is slot 2
        // (free), 7 is slot 3 (unconsumed).
        producer.push(99).unwrap();
        assert!(producer.push(100).is_err(), "pinned to slot 0");
        other.push(98).unwrap();
        assert!(other.push(97).is_err(), "pinned to slot 3");

        let pinned_to_0 = std::thread::spawn(move || {
            producer.push_block(100).expect("consumers dropped");
        });
        let pinned_to_3 = std::thread::spawn(move || {
            other.push_block(97).expect("consumers dropped");
        });
        ParkProbe::expect_until("both producers to park", || {
            ring.parked_producer_count() == 2
        });

        // `other` is the second lease, park slot 1. Point the cursor
        // at it so a round-robin wake would go there.
        assert_eq!(other_park_slot, ParkSlot::Leased(1));
        ring.producer_park.cursor.store(1, Ordering::Relaxed);

        // Frees position 8 = slot 0, which only `pinned_to_0` wants.
        drop(held);

        // Polled rather than joined: with the wake misrouted, the
        // producer never returns and a join would hang the test
        // instead of failing it.
        assert!(
            ParkProbe::wait_until(|| ring.parked_producer_count() == 1),
            "the release of slot 0 must wake the producer pinned to it, not the cursor's pick"
        );
        assert!(
            ring.producer_park.is_armed(other_park_slot),
            "the producer that wanted slot 3 must not have been woken for slot 0"
        );
        pinned_to_0.join().unwrap();

        // Free slot 3 so the second producer can finish.
        assert_eq!(drainer.pop(), Some(3));
        pinned_to_3.join().unwrap();

        let mut rest = vec![drainer.pop(), drainer.pop(), drainer.pop(), drainer.pop()];
        rest.sort_unstable();
        assert_eq!(rest, vec![Some(97), Some(98), Some(99), Some(100)]);
    }

    /// A park slot outlives the producer that leased it, so an
    /// announcement must not.
    ///
    /// `wake_producer_for` routes by reading `awaited[slot]` for every
    /// slot whose bit is set. A producer that stops parking leaves its
    /// last announcement published; `ParkRegistry` then hands the slot
    /// to a new producer, which arms the same bit. The new holder is
    /// now advertising positions the *previous* holder reserved, and a
    /// release of one of those is routed to it. It finds nothing, and
    /// re-parks having consumed the release.
    ///
    /// The stale word is only reachable through a live bit, so the
    /// test drives it through the lease: park a producer, let it
    /// announce, drop it, and check the recycled slot answers as a
    /// fresh one.
    #[test]
    fn a_recycled_park_slot_advertises_nothing_from_its_last_holder() {
        let (producer, consumer) =
            RingBuffer::<u64, Cfg<2, 128, 1>>::new(Capacity::exact(4)).split();
        let ring = std::sync::Arc::clone(&producer.queue);
        let drainer = consumer.clone();
        let mut holder = consumer;

        producer.push(0).unwrap();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();

        let held = holder.pop_ref().unwrap();
        assert_eq!(*held, 0);
        assert_eq!(drainer.pop(), Some(1));
        assert_eq!(drainer.pop(), Some(2));

        producer.push(99).unwrap();
        assert!(producer.push(100).is_err(), "pinned to slot 0");

        let slot = producer.park_slot;
        let parked = std::thread::spawn(move || {
            producer.push_block(100).expect("consumers dropped");
        });
        ParkProbe::expect_until("the producer to park and announce", || {
            ring.producer_park.wake.load(Ordering::Relaxed) != 0
        });
        assert!(
            slot.index()
                .is_some_and(|index| ring.awaited[index].announced().is_some()),
            "the parked producer must have announced its batch"
        );

        drop(held);
        parked.join().unwrap();
        drop(drainer);
        drop(holder);

        assert_eq!(
            slot.index().and_then(|index| ring.awaited[index].announced()),
            None,
            "a departed producer left a want behind for the next lease to inherit"
        );
    }

    /// A release nobody reserved must go to a producer that can use
    /// it, not to one that provably cannot.
    ///
    /// Two producers park for different reasons. `pinned` holds a
    /// partial batch and can only continue on position 4. `refilling`
    /// has an exhausted batch and no reservation at all, so whatever
    /// frees first lets it refill. The release of position 6 belongs
    /// to neither, but only one of them can act on it.
    ///
    /// Routing on a yes/no predicate cannot tell them apart: both
    /// answer "no", so the release fell through to round-robin, which
    /// the cursor here points at `pinned`. It wakes, finds position 4
    /// still held, and re-parks — and `refilling` is never woken,
    /// because nothing else is going to release. Reading the
    /// announcement as three-way separates "pinned elsewhere" from
    /// "wants anything" and sends the wake to the producer that can
    /// use it.
    ///
    /// The discriminating assertion is that `refilling` completes
    /// while `pinned` is still parked. It is polled rather than
    /// joined: with the wake misrouted this hangs, and a join would
    /// hang the suite instead of failing the test.
    #[test]
    fn a_release_no_producer_reserved_goes_to_one_that_can_use_it() {
        let (producer, consumer) =
            RingBuffer::<u64, Cfg<2, 128, 1>>::new(Capacity::exact(4)).split();
        let refilling = producer.clone();
        let ring = std::sync::Arc::clone(&producer.queue);
        let drainer = consumer.clone();
        let mut holder = consumer;

        producer.push(0).unwrap();
        producer.push(1).unwrap();
        refilling.push(2).unwrap();
        refilling.push(3).unwrap();

        let held = holder.pop_ref().unwrap();
        assert_eq!(*held, 0);
        assert_eq!(drainer.pop(), Some(1));

        // Takes batch {4, 5}: publishes 5, keeps 4 (slot 0, held).
        producer.push(99).unwrap();
        assert!(producer.push(100).is_err(), "pinned to slot 0");

        let pinned_slot = producer.park_slot;
        assert_eq!(pinned_slot, ParkSlot::Leased(0));

        let pinned = std::thread::spawn(move || {
            producer.push_block(100).expect("consumers dropped");
        });
        let refiller = std::thread::spawn(move || {
            refilling.push_block(101).expect("consumers dropped");
        });
        ParkProbe::expect_until("both producers to park", || {
            ring.parked_producer_count() == 2
        });

        // Round-robin would pick the pinned producer, which cannot use
        // what this release frees.
        ring.producer_park.cursor.store(0, Ordering::Relaxed);

        // Frees position 2 (slot 2), releasing it for round 6. The
        // pinned producer wants 4 and declines; only the refilling one
        // can take it.
        assert_eq!(drainer.pop(), Some(2));

        assert!(
            ParkProbe::wait_until(|| ring.parked_producer_count() == 1),
            "a release nobody reserved was spent on the producer pinned elsewhere"
        );
        assert!(
            ring.producer_park.is_armed(pinned_slot),
            "the pinned producer must still be waiting for its own position"
        );
        refiller.join().unwrap();

        // Its own position at last, which only it wants.
        drop(held);
        pinned.join().unwrap();

        let mut rest = vec![drainer.pop(), drainer.pop(), drainer.pop(), drainer.pop()];
        rest.sort_unstable();
        assert_eq!(rest, vec![Some(3), Some(99), Some(100), Some(101)]);
    }

    /// The bound on the pin: producers blocked by a *full ring* are
    /// interchangeable, so misrouting is harmless there.
    ///
    /// This is the case that limits the argument. `refill_batch`
    /// returns `None` before touching the `claim` cursor when the ring
    /// is full, so a producer that parks in that state holds no
    /// reservation and will take whichever position frees first. Any
    /// parked producer can absorb the wake.
    ///
    /// So the misrouting only bites when a producer parks holding a
    /// *partial batch* — which needs out-of-order consumption to
    /// arise, as in
    /// [`a_release_a_pinned_producer_cannot_use_does_not_reach_it`].
    /// Recording the boundary keeps the claim from being stated more
    /// broadly than the evidence supports.
    #[test]
    #[cfg(feature = "backstop-metrics")]
    fn producers_blocked_by_a_full_ring_can_absorb_each_others_wakes() {
        let (producer, consumer) =
            RingBuffer::<u64, Cfg<1, 128, 1>>::new(Capacity::exact(4)).split();
        let second = producer.clone();
        let ring = std::sync::Arc::clone(&producer.queue);

        for v in 0..4u64 {
            producer.push(v).unwrap();
        }

        // Both producers park: the ring is full, so neither holds a
        // reservation and each will take the next position it can.
        let a = std::thread::spawn(move || producer.push_block(100));
        let b = std::thread::spawn(move || second.push_block(101));

        ParkProbe::expect_until("both producers to park", || {
            ring.parked_producer_count() == 2
        });

        // One pop, one freed position, one wake.
        assert_eq!(consumer.pop(), Some(0));

        ParkProbe::expect_until("a producer to take the freed position", || {
            ring.parked_producer_count() == 1
        });

        for expected in [1, 2, 3] {
            assert_eq!(consumer.pop(), Some(expected));
        }
        a.join().unwrap().unwrap();
        b.join().unwrap().unwrap();

        let stats = ring.backstop_stats();
        assert!(
            !stats.saw_unexplained_rescue(),
            "a producer was released only by the timeout: {stats:?}"
        );
    }

    /// The complement: a wake the waiter *could* use must not be
    /// counted, or the metric would just track park counts.
    #[test]
    #[cfg(feature = "backstop-metrics")]
    fn a_wake_the_waiter_uses_is_not_counted_as_futile() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let ring = std::sync::Arc::clone(&producer.queue);

        let reader = std::thread::spawn(move || consumer.pop_block());

        ParkProbe::expect_until("the consumer to park", || {
            ring.consumer_park.wake.load(Ordering::Relaxed) != 0
        });
        producer.push(7).unwrap();

        assert_eq!(reader.join().unwrap(), Some(7));
        let stats = ring.backstop_stats();
        assert!(!stats.saw_futile_wake(), "{stats:?}");
    }

    #[test]
    fn basic_push_pop() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        let mut got = vec![c.pop().unwrap(), c.pop().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn fill_and_drain() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        for i in 0..8 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let mut got: Vec<u32> = (0..8).map(|_| c.pop().unwrap()).collect();
        got.sort_unstable();
        assert_eq!(got, (0..8).collect::<Vec<_>>());
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn wraparound() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for lap in 0..3u32 {
            for i in 0..4 {
                p.push(lap * 4 + i).unwrap();
            }
            let mut got: Vec<u32> = (0..4).map(|_| c.pop().unwrap()).collect();
            got.sort_unstable();
            assert_eq!(got, (lap * 4..lap * 4 + 4).collect::<Vec<_>>());
        }
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn empty_returns_none() {
        let (_p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn pop_block_wakes_on_push() {
        // Producer publishes after consumer is parked; consumer
        // must wake and observe the value.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        p.push(7).unwrap();
        assert_eq!(h.join().unwrap(), Some(7));
    }

    /// More blocking producers than `PARK_SLOTS`, so the registry is
    /// exhausted and the surplus are [`ParkSlot::Shared`]. Those hold
    /// no wake bit, so they are only reachable through the overflow
    /// stack; without it they park untimed and never return.
    #[test]
    fn push_block_completes_with_more_producers_than_park_slots() {
        use crate::common::park::PARK_SLOTS;

        const EXTRA: usize = 8;
        let n_producers = PARK_SLOTS + EXTRA;
        let (p, c) = RingBuffer::<usize>::new(Capacity::exact(4)).split();

        let handles: Vec<_> = (0..n_producers)
            .map(|i| {
                let p = p.clone();
                std::thread::spawn(move || p.push_block(i))
            })
            .collect();
        drop(p);

        let mut got = 0usize;
        while got < n_producers {
            if c.pop().is_some() {
                got += 1;
            } else {
                std::thread::yield_now();
            }
        }
        for handle in handles {
            assert_eq!(handle.join().unwrap(), Ok(()));
        }
    }

    /// The consumer half of the same property: mpmc leases park slots
    /// on both sides, so an over-subscribed consumer side must also
    /// be reachable without a timeout.
    #[test]
    #[cfg_attr(miri, ignore = "too slow for Miri: 72 spinning consumers on a 4-slot ring")]
    fn pop_block_completes_with_more_consumers_than_park_slots() {
        use crate::common::park::PARK_SLOTS;

        const EXTRA: usize = 8;
        let n_consumers = PARK_SLOTS + EXTRA;
        let (p, c) = RingBuffer::<usize>::new(Capacity::exact(4)).split();

        let handles: Vec<_> = (0..n_consumers)
            .map(|_| {
                let c = c.clone();
                std::thread::spawn(move || c.pop_block())
            })
            .collect();
        drop(c);

        for i in 0..n_consumers {
            while p.push(i).is_err() {
                std::thread::yield_now();
            }
        }

        let mut received: Vec<usize> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().expect("every consumer got a value"))
            .collect();
        received.sort_unstable();
        assert_eq!(received, (0..n_consumers).collect::<Vec<_>>());
    }

    #[test]
    fn pop_block_returns_none_on_close() {
        // After all producers drop and the queue is empty,
        // pop_block must return None instead of hanging.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(p);
        assert_eq!(h.join().unwrap(), None);
    }

    #[test]
    fn push_block_unblocks_on_pop() {
        // Fill the ring, then verify push_block waits for a pop
        // and then succeeds.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let p2 = p.clone();
        let h = std::thread::spawn(move || p2.push_block(99));
        std::thread::sleep(std::time::Duration::from_millis(50));
        let v = c.pop().unwrap();
        assert!(v < 4);
        assert_eq!(h.join().unwrap(), Ok(()));
        // Keep `p` alive past the join so the ring isn't closed
        // while `push_block` is racing to publish.
        drop(p);
    }

    #[test]
    fn push_block_returns_err_on_consumer_close() {
        // Drop the consumer; push_block should observe close and
        // return Err, returning the value back to the caller.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let p2 = p.clone();
        let h = std::thread::spawn(move || p2.push_block(99));
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(c);
        assert_eq!(h.join().unwrap(), Err(99));
        drop(p);
    }

    #[test]
    fn pop_block_drains_before_close() {
        // Items pushed before the producer drops must still be
        // returned, even when the producer drops between push
        // and pop_block.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        drop(p);
        let mut got = vec![c.pop_block().unwrap(), c.pop_block().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        assert_eq!(c.pop_block(), None);
    }

    #[test]
    fn concurrent_two_producers_two_consumers() {
        let total = 16usize;
        let (p, c) = RingBuffer::<u64>::new(Capacity::exact(8)).split();
        let p2 = p.clone();
        let c2 = c.clone();
        let recv = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let r1 = recv.clone();
        let h1 = std::thread::spawn(move || {
            while r1.load(std::sync::atomic::Ordering::Relaxed) < total {
                if c.pop().is_some() {
                    r1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }
        });
        let r2 = recv.clone();
        let h2 = std::thread::spawn(move || {
            while r2.load(std::sync::atomic::Ordering::Relaxed) < total {
                if c2.pop().is_some() {
                    r2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }
        });

        let h3 = std::thread::spawn(move || {
            for i in 0..(total / 2) as u64 {
                while p.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });
        let h4 = std::thread::spawn(move || {
            for i in 0..(total / 2) as u64 {
                while p2.push(100 + i).is_err() {
                    std::thread::yield_now();
                }
            }
        });
        h3.join().unwrap();
        h4.join().unwrap();
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(recv.load(std::sync::atomic::Ordering::Relaxed), total);
    }

    use crate::common::DropCounter;

    #[test]
    fn drop_left_in_buffer() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (p, _c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
            for _ in 0..4 {
                p.push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
            }
            // _c dropped first, then p, then RingBuffer.
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[test]
    fn drop_after_partial_drain() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (p, c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
            for _ in 0..4 {
                p.push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
            }
            drop(c.pop().unwrap());
            drop(c.pop().unwrap());
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[test]
    fn many_wraps_single_threaded() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        for batch in 0..8 {
            let base = batch * 4;
            for i in 0..4 {
                p.push(base + i).unwrap();
            }
            for i in 0..4 {
                let v = c.pop().unwrap();
                assert_eq!(v, base + i, "batch {batch} item {i}");
            }
        }
    }

    #[test]
    #[ignore = "stress test — slow under Miri"]
    fn stress_mpmc() {
        let total: u64 = 4000;
        let p_count: u64 = 4;
        let c_count: u64 = 4;
        let per_p = total / p_count;
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let p = prod.clone();
                std::thread::spawn(move || {
                    for i in 0..per_p {
                        while p.push(tid * per_p + i).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        let cc: Vec<_> = (0..c_count)
            .map(|_| {
                let c = cons.clone();
                let r = received.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    while r.load(std::sync::atomic::Ordering::Relaxed)
                        < usize::try_from(total).unwrap()
                    {
                        if let Some(v) = c.pop() {
                            r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            got.push(v);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), usize::try_from(total).unwrap());
    }

    /// Run a full correctness check for a given (P, Q, total, cap)
    /// shape. Each producer pushes a disjoint contiguous range of
    /// values; consumers collect everything; we verify the union
    /// equals exactly the union of all producer ranges (no
    /// duplicates, no gaps, no extras). Producers wait for the ring
    /// to make space via busy-loop on Err.
    fn run_correctness(p_count: u64, c_count: u64, total: u64, cap: usize) {
        let per_p = total / p_count;
        let actual_total = per_p * p_count;
        let actual_total_us = usize::try_from(actual_total).unwrap();
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let p = prod.clone();
                std::thread::spawn(move || {
                    let base = tid * per_p;
                    for i in 0..per_p {
                        let v = base + i;
                        while p.push(v).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        let cc: Vec<_> = (0..c_count)
            .map(|_| {
                let c = cons.clone();
                let r = received.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    while r.load(std::sync::atomic::Ordering::Relaxed) < actual_total_us {
                        if let Some(v) = c.pop() {
                            r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            got.push(v);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        // Exact-count check: no duplicates, no losses, no extras.
        assert_eq!(
            all.len(),
            actual_total_us,
            "P={p_count} Q={c_count}: got {} items, expected {}",
            all.len(),
            actual_total
        );
        all.sort_unstable();
        let mut prev: Option<u64> = None;
        for v in &all {
            if let Some(p) = prev {
                assert_ne!(p, *v, "duplicate value {v} (P={p_count} Q={c_count})");
            }
            prev = Some(*v);
        }
        // The set of values is exactly [0, actual_total).
        for (i, v) in all.iter().enumerate() {
            assert_eq!(
                *v, i as u64,
                "missing or unexpected value at sorted index {i} (P={p_count} Q={c_count})"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Matrix correctness tests across P x Q including mismatched shapes.
    // Each test verifies: no items lost, no duplicates, exact value set.
    // Sized to be reasonable for a regular `cargo test` run; the stress
    // module above takes 4000 items, these take 10000.
    // -----------------------------------------------------------------------

    macro_rules! correctness_test {
        ($name:ident, $p:expr, $q:expr) => {
            #[test]
            #[ignore = "matrix correctness — too slow for default run"]
            fn $name() {
                run_correctness($p, $q, 10_000, 1024);
            }
        };
    }

    correctness_test!(matrix_p1_q1, 1, 1);
    correctness_test!(matrix_p1_q2, 1, 2);
    correctness_test!(matrix_p1_q4, 1, 4);
    correctness_test!(matrix_p1_q8, 1, 8);
    correctness_test!(matrix_p2_q1, 2, 1);
    correctness_test!(matrix_p2_q2, 2, 2);
    correctness_test!(matrix_p2_q4, 2, 4);
    correctness_test!(matrix_p2_q8, 2, 8);
    correctness_test!(matrix_p4_q1, 4, 1);
    correctness_test!(matrix_p4_q2, 4, 2);
    correctness_test!(matrix_p4_q4, 4, 4);
    correctness_test!(matrix_p4_q8, 4, 8);
    correctness_test!(matrix_p8_q1, 8, 1);
    correctness_test!(matrix_p8_q2, 8, 2);
    correctness_test!(matrix_p8_q4, 8, 4);
    correctness_test!(matrix_p8_q8, 8, 8);

    // Larger items-per-producer to catch infrequent races. ~4M items
    // total, runs in ~1s.
    macro_rules! heavy_correctness_test {
        ($name:ident, $p:expr, $q:expr) => {
            #[test]
            #[ignore = "heavy correctness — minutes-scale runtime"]
            fn $name() {
                run_correctness($p, $q, 1_000_000, 1024);
            }
        };
    }

    heavy_correctness_test!(heavy_p2_q2, 2, 2);
    heavy_correctness_test!(heavy_p4_q4, 4, 4);
    heavy_correctness_test!(heavy_p8_q8, 8, 8);
    heavy_correctness_test!(heavy_p1_q8, 1, 8);
    heavy_correctness_test!(heavy_p8_q1, 8, 1);

    /// Counter-free correctness check: producers push a known set,
    /// drop, consumers drain until `is_closed && pop returns None`.
    /// No shared atomic counter masks throughput, so this stresses
    /// the queue at full speed and exposes any item-loss races.
    fn run_correctness_no_counter(p_count: u64, c_count: u64, total: u64, cap: usize) {
        let per_p = total / p_count;
        let actual_total = usize::try_from(per_p * p_count).unwrap();
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();

        // Consumers drain until producers are dropped AND queue is
        // empty. They collect everything they pop into a local vec.
        let cc: Vec<_> = (0..c_count)
            .map(|_| {
                let c = cons.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    loop {
                        if let Some(v) = c.pop() {
                            got.push(v);
                        } else if c.is_closed() {
                            // Drain remaining items, then exit.
                            while let Some(v) = c.pop() {
                                got.push(v);
                            }
                            break;
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let p = prod.clone();
                std::thread::spawn(move || {
                    let base = tid * per_p;
                    for i in 0..per_p {
                        let v = base + i;
                        while p.push(v).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        assert_eq!(
            all.len(),
            actual_total,
            "P={p_count} Q={c_count}: got {} items, expected {}",
            all.len(),
            actual_total
        );
        all.sort_unstable();
        for (i, v) in all.iter().enumerate() {
            assert_eq!(
                *v, i as u64,
                "missing/extra at sorted index {i} (P={p_count} Q={c_count}): got {v}, expected {i}"
            );
        }
    }

    macro_rules! no_counter_test {
        ($name:ident, $p:expr, $q:expr, $total:expr) => {
            #[test]
            #[ignore = "counter-free correctness — minute-scale runtime"]
            fn $name() {
                run_correctness_no_counter($p, $q, $total, 1024);
            }
        };
    }

    no_counter_test!(no_counter_p2_q2, 2, 2, 5_000_000);
    no_counter_test!(no_counter_p4_q4, 4, 4, 5_000_000);
    no_counter_test!(no_counter_p8_q8, 8, 8, 5_000_000);
    no_counter_test!(no_counter_p1_q8, 1, 8, 5_000_000);
    no_counter_test!(no_counter_p8_q1, 8, 1, 5_000_000);
    no_counter_test!(no_counter_p2_q4, 2, 4, 5_000_000);
    no_counter_test!(no_counter_p4_q2, 4, 2, 5_000_000);

    // -----------------------------------------------------------------------
    // Zero-copy API
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_write_commit_cycle() {
        let (mut p, c) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let w = p.reserve().unwrap();
        w.write(42).commit();
        assert_eq!(c.pop(), Some(42));
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn reserve_slot_mut_commit_unchecked() {
        let (mut p, c) = RingBuffer::<[u8; 32]>::new(Capacity::exact(4)).split();
        let mut w = p.reserve().unwrap();
        w.slot_mut().write([0xAB; 32]);
        // SAFETY: slot_mut().write initialized the slot.
        unsafe { w.commit_unchecked() };
        let v = c.pop().unwrap();
        assert_eq!(v[0], 0xAB);
        assert_eq!(v[31], 0xAB);
    }

    /// The mpmc twin of the mpsc and broadcast cases: an uncommitted
    /// reservation whose value panics on drop must still hand its
    /// position back to the producer's batch bitmap, or the position is
    /// lost until the handle is dropped.
    #[test]
    fn a_panicking_payload_still_returns_its_uncommitted_reservation() {
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

        let (mut p, c) = RingBuffer::<PanicOnDropGuard>::new(Capacity::exact(4)).split();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let w = p.reserve().unwrap();
            drop(w.write(PanicOnDropGuard::armed()));
        }));
        assert!(result.is_err(), "the destructor panic must propagate");

        // The returned bit is what lets this producer reuse the
        // position. A lost one costs a slot for the handle's lifetime,
        // which only shows up once the ring has to wrap through it.
        for _ in 0..16 {
            p.reserve()
                .expect("the released position must be reusable")
                .write(PanicOnDropGuard::disarmed())
                .commit();
            assert!(
                c.pop().is_some(),
                "the consumer must reach each value published after the panic"
            );
        }
    }

    #[test]
    fn pop_ref_zero_copy_read() {
        let (mut p, mut c) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        p.reserve().unwrap().write(99).commit();
        let r = c.pop_ref().unwrap();
        assert_eq!(*r, 99);
        drop(r);
        assert!(c.pop_ref().is_none());
    }

    #[test]
    fn reserve_drop_without_commit_restores_bit() {
        // Capacity 4, batch size = 32 (DefaultConfig::PRODUCER_BATCH).
        // The first reserve claims a batch starting at pos 0, takes
        // bit 0. Dropping rolls back the bit; a subsequent reserve
        // from the same handle should reuse pos 0 (and write succeeds).
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        {
            let _w = p.reserve().unwrap();
            // _w dropped here without commit — bit must be restored.
        }
        // The next reserve reuses the same batch position.
        p.reserve().unwrap().write(7).commit();
        assert_eq!(c.pop(), Some(7));
    }

    #[test]
    fn reserve_drop_does_not_leak() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (mut p, _c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
            let w = p.reserve().unwrap();
            w.write(DropCounter {
                counter: counter.clone(),
            });
            // WrittenSlot dropped without commit → must drop the value.
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn reserve_uncommitted_then_producer_drop_tombstones() {
        // SlotWriter dropped without commit leaves the bit in
        // batch_unused. Producer::drop must tombstone it so the
        // ring is left in a consistent state (not leaking a slot).
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        {
            let _w = p.reserve().unwrap();
            // SlotWriter dropped without commit; bit restored.
        }
        // Drop the producer — its drop tombstones the unused bits,
        // including the one we restored. Consumer should see closed
        // and observe an empty queue cleanly.
        drop(p);
        assert!(c.is_closed());
        assert_eq!(c.pop(), None);
    }

    /// Dropping a producer must not wait on a consumer that no longer
    /// exists.
    ///
    /// A batch position whose previous round is still unreleased can
    /// only be handed back once `done` catches up, and only a consumer
    /// advances `done`. Waiting for that unconditionally made the drop
    /// depend on a peer still running: with the consumers gone the
    /// producer's destructor never returned. The wait now stops when
    /// the consumers close.
    #[test]
    fn producer_drop_completes_after_consumers_are_gone() {
        // A full lap so the batch's positions alias slots whose
        // previous round the (departed) consumer never released.
        let (p, c) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        drop(c);

        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&dropped);
        let h = std::thread::spawn(move || {
            drop(p);
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        h.join().expect("the producer drop must not wait forever");
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// The same with an outstanding reservation: the abandoned batch
    /// position is exactly the state whose release needs a consumer.
    #[test]
    fn producer_drop_with_reservation_completes_after_consumers_are_gone() {
        let (mut p, c) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        let writer = p.reserve();
        drop(c);
        drop(writer);

        let h = std::thread::spawn(move || drop(p));
        h.join().expect("the producer drop must not wait forever");
    }

    #[test]
    fn pop_ref_drop_releases_slot_for_reuse() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        // Fill the ring.
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());

        // pop_ref claims a slot but does not release done[s] until
        // drop — the slot is reusable only after the reader drops.
        {
            let _r = c.pop_ref().unwrap();
            // While _r is alive, the producer at the next-round
            // position for this slot should still see done[s] = pos
            // (not pos+cap), so push to that specific position would
            // block. We don't poke at internals; just confirm
            // post-drop reuse works.
        }
        // After drop, the producer can push again into the freed slot.
        p.push(99).unwrap();
        // We can drain everything — order is publish-order, not
        // push-order, so just check the multiset.
        let mut got = Vec::new();
        while let Some(v) = c.pop() {
            got.push(v);
        }
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3, 99]);
    }

    #[test]
    fn pop_ref_drops_value_on_reader_drop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (mut p, mut c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
        p.reserve()
            .unwrap()
            .write(DropCounter {
                counter: counter.clone(),
            })
            .commit();
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
        {
            let _r = c.pop_ref().unwrap();
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
        }
        // SlotReader::drop must drop the value exactly once.
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn mixed_push_reserve_pop_pop_ref() {
        let (mut p, mut c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        p.push(1).unwrap();
        p.reserve().unwrap().write(2).commit();
        p.push(3).unwrap();

        // Order is publish order. With a single producer and no
        // contention, that matches push order.
        assert_eq!(c.pop(), Some(1));
        let r = c.pop_ref().unwrap();
        assert_eq!(*r, 2);
        drop(r);
        assert_eq!(c.pop(), Some(3));
        assert_eq!(c.pop(), None);
    }

    #[test]
    #[ignore = "stress test — slow under Miri"]
    fn concurrent_reserve_pop_ref() {
        // Two producers using reserve(), two consumers using pop_ref().
        // Verify item-count and value-set are exactly correct.
        let total: u64 = 4000;
        let p_count: u64 = 2;
        let per_p = total / p_count;
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(64)).split();

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let mut p = prod.clone();
                std::thread::spawn(move || {
                    for i in 0..per_p {
                        loop {
                            if let Some(w) = p.reserve() {
                                w.write(tid * per_p + i).commit();
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cc: Vec<_> = (0..2)
            .map(|_| {
                let mut c = cons.clone();
                let r = received.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    while r.load(std::sync::atomic::Ordering::Relaxed)
                        < usize::try_from(total).unwrap()
                    {
                        if let Some(reader) = c.pop_ref() {
                            got.push(*reader);
                            drop(reader);
                            r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), usize::try_from(total).unwrap());
        for (i, v) in all.iter().enumerate() {
            assert_eq!(*v, i as u64);
        }
    }

    // -----------------------------------------------------------------------
    // Zero-copy blocking API
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_block_unblocks_on_pop() {
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        // Fill — 4 slots and DefaultConfig::PRODUCER_BATCH=32, so the
        // first push claims a batch of 4 (capped by free space) and
        // commits all 4 in turn. After 4 pushes the next reserve()
        // would need a fresh batch, which the watermark says is empty.
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.reserve().is_none());
        let h = std::thread::spawn(move || {
            let w = p.reserve_block().unwrap();
            w.write(99).commit();
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        // Drain one — wakes the parked producer.
        assert!(c.pop().is_some());
        h.join().unwrap();
        // Drain the rest.
        let mut got = Vec::new();
        while let Some(v) = c.pop() {
            got.push(v);
        }
        got.sort_unstable();
        // We popped one of {0,1,2,3} above; the remaining 3 plus 99
        // are in `got`.
        assert_eq!(got.len(), 4);
        assert!(got.contains(&99));
    }

    #[test]
    fn reserve_block_returns_none_on_consumer_close() {
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.reserve().is_none());
        let h = std::thread::spawn(move || p.reserve_block().is_some());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(c);
        assert!(!h.join().unwrap());
    }

    #[test]
    fn pop_ref_block_wakes_on_push() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || {
            let r = c.pop_ref_block().unwrap();
            *r
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        p.push(7).unwrap();
        assert_eq!(h.join().unwrap(), 7);
    }

    #[test]
    fn pop_ref_block_returns_none_on_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_ref_block().is_some());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(p);
        assert!(!h.join().unwrap());
    }

    #[test]
    fn pop_ref_block_drains_before_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        drop(p);
        let v1 = *c.pop_ref_block().unwrap();
        let v2 = *c.pop_ref_block().unwrap();
        let mut got = vec![v1, v2];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        assert!(c.pop_ref_block().is_none());
    }

    // -----------------------------------------------------------------------
    // Drain
    // -----------------------------------------------------------------------

    #[test]
    fn drain_empty_returns_zero() {
        let (_p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let n = c.drain(|_| panic!("unexpected"));
        assert_eq!(n, 0);
    }

    /// A `T` whose destructor panics must still release its slot. The
    /// `SlotReader` drops the value before storing `done`, so an unwind
    /// in between leaves the slot in "claimed but not released", which
    /// `RingBuffer::drop` explicitly treats as still holding live data
    /// and drops a second time.
    #[test]
    fn slot_reader_panicking_drop_does_not_double_drop() {
        struct PanicOnDrop {
            counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            panics: bool,
        }

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                self.counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(!self.panics, "destructor failure");
            }
        }

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) = RingBuffer::<PanicOnDrop>::new(Capacity::exact(4)).split();

        assert!(producer
            .push(PanicOnDrop {
                counter: std::sync::Arc::clone(&counter),
                panics: true,
            })
            .is_ok());
        assert!(producer
            .push(PanicOnDrop {
                counter: std::sync::Arc::clone(&counter),
                panics: false,
            })
            .is_ok());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let r = consumer.pop_ref().unwrap();
            drop(r);
        }));
        assert!(result.is_err(), "the destructor panic must propagate");

        drop(consumer);
        drop(producer);

        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "each item must be dropped exactly once"
        );
    }

    #[test]
    fn drain_all_published() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        let mut got = Vec::new();
        let n = c.drain(|v| got.push(v));
        assert_eq!(n, 4);
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3]);
    }

    #[test]
    fn drain_up_to_limit() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        let mut got = Vec::new();
        let n = c.drain_up_to(2, |v| got.push(v));
        assert_eq!(n, 2);
        // Remaining 2 still claimable.
        let mut rest = Vec::new();
        c.drain(|v| rest.push(v));
        let mut all: Vec<u32> = got.into_iter().chain(rest).collect();
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3]);
    }

    #[test]
    fn drain_block_drains_to_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || {
            let mut got = Vec::new();
            c.drain_block(|v| got.push(v));
            got
        });
        for i in 0..3u32 {
            p.push(i).unwrap();
        }
        ParkProbe::expect_until("the consumer to drain the first wave and park", || {
            p.queue.consumer_park.wake.load(Ordering::SeqCst) != 0
        });
        for i in 100..103u32 {
            p.push(i).unwrap();
        }
        drop(p);
        let mut got = h.join().unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 100, 101, 102]);
    }

    /// `drain` releases each slot inside its loop but wakes producers
    /// after it. A panicking callback unwinds past `wake_n`, leaving
    /// producers parked on a ring that now has space — a permanent
    /// hang, since `push_block` parks untimed.
    #[test]
    fn a_panicking_drain_callback_still_wakes_the_blocked_producers() {
        const CAP: u32 = 4;

        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(CAP as usize)).split();
        for i in 0..CAP {
            p.push(i).unwrap();
        }

        let pusher = {
            let p = p.clone();
            std::thread::spawn(move || p.push_block(99).expect("consumers dropped"))
        };
        drop(p);
        ParkProbe::expect_until("push_block to set its wake bit", || {
            c.queue.producer_park.wake.load(Ordering::SeqCst) != 0
        });

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c.drain(|v| assert!(v != 2, "callback panic"));
        }));
        assert!(result.is_err(), "the callback panic must propagate");

        // Slots were released before the unwind, so a producer has
        // space. Without the wake it sleeps forever and this join never
        // returns.
        pusher.join().unwrap();
    }

    /// mpmc analog of `mpsc::drain_wakes_all_parked_producers` — same
    /// regression class. With `wake_n(count)` all parked producers
    /// wake after a drain; with the old `wake_one` only one would,
    /// stranding the rest.
    #[test]
    fn drain_wakes_all_parked_producers() {
        const CAP: u32 = 4;
        const N_PRODUCERS: u32 = 4;

        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(CAP as usize)).split();
        for i in 0..CAP {
            p.push(i).unwrap();
        }

        let producers: Vec<_> = (0..N_PRODUCERS)
            .map(|tid| {
                let p = p.clone();
                std::thread::spawn(move || {
                    p.push_block(1000 + tid).expect("consumers dropped");
                })
            })
            .collect();
        drop(p);
        // Every producer must set its wake bit before the drain runs.
        // Otherwise a producer still spinning can push into a slot the
        // drain just freed, and the drain collects more than CAP.
        ParkProbe::expect_until("all producers to set their wake bit", || {
            c.queue
                .producer_park
                .wake
                .load(Ordering::SeqCst)
                .count_ones()
                == N_PRODUCERS
        });

        // Single drain — releases done[s] for all 4 slots and wakes
        // up to 4 producers via wake_n. It collects at least CAP; a
        // woken producer may refill a freed slot before the drain
        // reaches the end, so the exact count is not fixed.
        let n = c.drain(|_| {});
        assert!(
            n >= CAP as usize,
            "drain collected {n}, expected at least {CAP}"
        );

        // The regression this guards: with wake_one only one producer
        // woke and the rest stayed parked forever. Draining until every
        // producer finishes keeps that a hang-free failure.
        for (i, h) in producers.into_iter().enumerate() {
            ParkProbe::expect_until(
                &format!("drain to wake parked producer {i} of {N_PRODUCERS}"),
                || {
                    c.drain(|_| {});
                    h.is_finished()
                },
            );
            h.join().unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // Async API
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(feature = "async")]
    #[cfg_attr(miri, ignore = "too slow for Miri: threads + tokio runtimes")]
    fn async_push_pop_cross_thread() {
        // M producers + N consumers, each on its own thread with a
        // current_thread runtime + LocalSet. Watchdog aborts within 5s
        // if anything deadlocks.
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let n_producers: u64 = 2;
        let n_consumers: u64 = 2;
        let per_producer: u64 = 2_500;
        let total = n_producers * per_producer;
        let received = Arc::new(AtomicU64::new(0));
        let _watchdog = crate::common::progress_watchdog::ProgressWatchdog::spawn(
            received.clone(),
            "mpmc async_push_pop_cross_thread",
        );

        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();

        let consumer_threads: Vec<_> = (0..n_consumers)
            .map(|_| {
                let c = consumer.clone();
                let received = received.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(async move {
                        while c.pop_async().await.is_some() {
                            received.fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                })
            })
            .collect();
        drop(consumer);

        let producer_threads: Vec<_> = (0..n_producers)
            .map(|tid| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(async move {
                        for i in 0..per_producer {
                            p.push_async(tid * per_producer + i)
                                .await
                                .expect("all consumers dropped");
                        }
                    }));
                })
            })
            .collect();
        drop(producer);

        for h in producer_threads {
            h.join().unwrap();
        }
        for h in consumer_threads {
            h.join().unwrap();
        }
        assert_eq!(received.load(Ordering::Relaxed), total);

    }

    #[test]
    #[cfg(feature = "async")]
    fn async_pop_returns_none_on_close() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
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

    /// Async twin of
    /// [`a_panicking_drain_callback_still_wakes_the_blocked_producers`]:
    /// a waker skipped by an unwinding drain callback strands the task
    /// permanently.
    #[test]
    #[cfg(feature = "async")]
    fn a_panicking_drain_callback_still_wakes_the_pending_async_producer() {
        // Asserting on the wake itself, not on a re-poll: awaiting the
        // future would poll it again and find the space regardless,
        // passing whether or not the drain ever woke anyone.
        struct CountingWaker {
            count: std::sync::atomic::AtomicUsize,
        }

        impl std::task::Wake for CountingWaker {
            fn wake(self: std::sync::Arc<Self>) {
                self.count.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        for i in 0..4u64 {
            producer.push(i).unwrap();
        }

        let counter = std::sync::Arc::new(CountingWaker {
            count: std::sync::atomic::AtomicUsize::new(0),
        });
        let waker = std::task::Waker::from(std::sync::Arc::clone(&counter));
        let mut cx = std::task::Context::from_waker(&waker);

        let mut pending = std::pin::pin!(producer.push_async(99));
        assert!(
            std::future::Future::poll(pending.as_mut(), &mut cx).is_pending(),
            "the ring is full, so the push must register and park"
        );

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            consumer.drain(|v| assert!(v != 2, "callback panic"));
        }));
        assert!(result.is_err(), "the callback panic must propagate");

        assert_eq!(
            counter.count.load(Ordering::Relaxed),
            1,
            "the freed space must wake the registered task, even on the unwind path"
        );
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_push_returns_err_on_consumer_close() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
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

    #[test]
    #[ignore = "diagnostic — instruments the saturated mpmc/block deadlock; runs 200 iters of 5k items each"]
    #[allow(clippy::too_many_lines)]
    fn block_stress_diagnostic() {
        // Instrumented variant: when the watchdog fires (10s without
        // any iter completing), dump the ring's debug snapshot so we
        // can see what state the threads were in when everything
        // parked.
        //
        // The deadlock this was written to catch is fixed as of
        // 4463d52 and pinned by `a_release_wakes_the_producer_that
        // _reserved_the_position`. It was not a lost wake: a release
        // issued one round-robin wake_one, which landed on a producer
        // that was not the one holding the reserved position, so the
        // reserver slept beside a free slot while the claim cursor
        // waited on that same slot. Releases are routed by position
        // now.
        //
        // The harness stays because that is one mechanism, not a
        // proof that there are no others: every blocking park in the
        // crate is untimed, so any remaining lost wake is a hang
        // rather than a latency blip. The watchdog aborts on 10s
        // without a completed iter, which is what makes a stall a
        // failed job rather than a slow one.
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::Arc;

        let test_done = Arc::new(AtomicBool::new(false));
        let test_done_watchdog = test_done.clone();
        let progress = Arc::new(AtomicU64::new(0)); // bumped every completed iter
        let progress_watchdog = progress.clone();
        let snapshot_holder: Arc<std::sync::Mutex<Option<ring_snapshot::RingSnapshot>>> =
            Arc::new(std::sync::Mutex::new(None));
        let snapshot_holder_w = snapshot_holder.clone();

        let watchdog = std::thread::spawn(move || {
            // Idle watchdog: fires if no iter completes within 10s.
            let mut last = progress_watchdog.load(Ordering::Acquire);
            let mut last_t = std::time::Instant::now();
            while !test_done_watchdog.load(Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(100));
                let cur = progress_watchdog.load(Ordering::Acquire);
                if cur != last {
                    last = cur;
                    last_t = std::time::Instant::now();
                    continue;
                }
                if last_t.elapsed() > std::time::Duration::from_secs(10) {
                    use std::io::Write;
                    let mut stderr = std::io::stderr().lock();
                    let _ = writeln!(stderr, "\n\n=== DEADLOCK at iter {cur} ===");
                    if let Ok(g) = snapshot_holder_w.lock() {
                        if let Some(snapshot) = &*g {
                            let _ = write!(stderr, "{snapshot}");
                        } else {
                            let _ = writeln!(stderr, "(no snapshot captured)");
                        }
                    }
                    let _ = writeln!(stderr, "aborting\n");
                    let _ = stderr.flush();
                    drop(stderr);
                    std::process::abort();
                }
            }
        });

        #[cfg(feature = "backstop-metrics")]
        let mut totals = backstop_monitor::BackstopStats::ZERO;
        let iters = crate::common::stress_iters::StressIters::from_env(200).get();
        for iter in 0..iters {
            if iter % 10 == 0 {
                eprintln!("iter {iter}");
            }
            let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
            let stats_q = producer.queue.clone();
            // Stash a snapshot of the queue's state every 100ms so the
            // watchdog has something to dump when it fires.
            let snap_q = producer.queue.clone();
            let snap_holder = snapshot_holder.clone();
            let snap_done = Arc::new(AtomicBool::new(false));
            let snap_done2 = snap_done.clone();
            let snap_thread = std::thread::spawn(move || {
                while !snap_done2.load(Ordering::Acquire) {
                    let snapshot = ring_snapshot::RingSnapshot::take(&*snap_q);
                    if let Ok(mut g) = snap_holder.lock() {
                        *g = Some(snapshot);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            });

            let consumers: Vec<_> = (0..2)
                .map(|_| {
                    let c = consumer.clone();
                    std::thread::spawn(move || {
                        let mut local = 0u64;
                        while let Some(v) = c.pop_block() {
                            let mut x = v;
                            for _ in 0..64 {
                                x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                                x = std::hint::black_box(x);
                            }
                            local += 1;
                        }
                        local
                    })
                })
                .collect();
            drop(consumer);

            let producers: Vec<_> = (0..2u64)
                .map(|tid| {
                    let p = producer.clone();
                    std::thread::spawn(move || {
                        for i in 0..5000u64 {
                            p.push_block(tid * 5000 + i).expect("consumers dropped");
                        }
                    })
                })
                .collect();
            drop(producer);

            for h in producers {
                h.join().unwrap();
            }
            let mut sum = 0u64;
            for h in consumers {
                sum += h.join().unwrap();
            }
            // Stop snapshot thread for this iter
            snap_done.store(true, Ordering::Release);
            snap_thread.join().unwrap();
            assert_eq!(sum, 10000);
            // Quiesced: every waiter has finished, so the two counters
            // are a consistent pair.
            #[cfg(feature = "backstop-metrics")]
            {
                let stats = stats_q.backstop_stats();
                // Reported, not asserted: the event this harness
                // measures is rare, and aborting on the first one
                // produced a small, early-truncated sample that
                // overstated its rate. The sole-waiter total is
                // printed at the end alongside the timeouts it is a
                // fraction of. The raw count is the one to read — a
                // taken bit over an armed handle is equally the
                // signature of a stolen wake, see `the_monitor_counts
                // _a_wake_that_never_arrived`.
                if stats.saw_unexplained_rescue() {
                    eprintln!("iter {iter}: SOLE-WAITER RESCUE {stats:?}");
                } else if stats.saw_rescue() {
                    eprintln!("iter {iter}: {stats:?}");
                }
                totals += stats;
            }
            drop(stats_q);
            progress.fetch_add(1, Ordering::Release);
        }
        test_done.store(true, Ordering::Release);
        watchdog.join().unwrap();
        #[cfg(feature = "backstop-metrics")]
        eprintln!(
            "TOTALS over {iters} iters: futile={} (producer={}, blind={}, producer_blind={}) \
             rescues={} (sole={}: producer={} consumer={}; blind={} late={} waiting={}; bit_taken={}) \
             unwoken_timeouts={} wakes_after_timeout={}",
            totals.futile_wakes,
            totals.producer_futile_wakes,
            totals.blind_futile_wakes,
            totals.producer_blind_futile_wakes,
            totals.rescues,
            totals.sole_waiter_rescues,
            totals.producer_sole_waiter_rescues,
            totals.consumer_sole_waiter_rescues(),
            totals.blind_sole_waiter_rescues,
            totals.late_sole_waiter_rescues,
            totals.waiting_sole_waiter_rescues,
            totals.bit_taken_rescues,
            totals.unwoken_timeouts,
            totals.wakes_arriving_after_timeout,
        );
    }

    #[test]
    #[cfg(feature = "async")]
    #[ignore = "slow stress: 2k iters x 500 items"]
    fn async_push_pop_cross_thread_iters_unsaturated() {
        async_mpmc_stress_iters(2_000, 4096, 500);
    }

    #[test]
    #[cfg(feature = "async")]
    #[ignore = "slow stress: 2k iters x 500 items through a 16-slot ring"]
    fn async_push_pop_cross_thread_iters_saturated() {
        // Saturated: small ring forces producers to park, drain wakes
        // multiple parked producers per batch. A missed wake here is
        // a hang the watchdog reports, not a stall.
        async_mpmc_stress_iters(2_000, 16, 500);
    }

    #[cfg(feature = "async")]
    fn async_mpmc_stress_iters(iters: usize, cap: usize, per_producer: u64) {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        // Monotonic across all iterations so the watchdog sees continuous
        // progress; `received` below is per-iteration (for the assert).
        let progress = Arc::new(AtomicU64::new(0));
        let _watchdog = crate::common::progress_watchdog::ProgressWatchdog::spawn(
            progress.clone(),
            "mpmc async stress",
        );

        let n_producers: u64 = 2;
        let n_consumers: u64 = 2;
        let total = n_producers * per_producer;
        for _ in 0..iters {
            let received = Arc::new(AtomicU64::new(0));
            let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();

            let consumer_threads: Vec<_> = (0..n_consumers)
                .map(|_| {
                    let c = consumer.clone();
                    let received = received.clone();
                    let progress = progress.clone();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .unwrap();
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            while c.pop_async().await.is_some() {
                                received.fetch_add(1, Ordering::Relaxed);
                                progress.fetch_add(1, Ordering::Relaxed);
                            }
                        }));
                    })
                })
                .collect();
            drop(consumer);

            let producer_threads: Vec<_> = (0..n_producers)
                .map(|tid| {
                    let p = producer.clone();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .unwrap();
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            for i in 0..per_producer {
                                p.push_async(tid * per_producer + i)
                                    .await
                                    .expect("all consumers dropped");
                            }
                        }));
                    })
                })
                .collect();
            drop(producer);

            for h in producer_threads {
                h.join().unwrap();
            }
            for h in consumer_threads {
                h.join().unwrap();
            }
            assert_eq!(received.load(Ordering::Relaxed), total);
        }

    }
}
