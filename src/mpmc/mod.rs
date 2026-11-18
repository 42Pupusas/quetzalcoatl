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
#[cfg(all(test, not(loom)))]
mod replay;
#[cfg(feature = "backstop-metrics")]
mod rescue_evidence;
mod reservation_return;
#[cfg(test)]
mod ring_snapshot;
mod scan_budget;
mod slot_release;
mod split_ring;

pub use config::{Cfg, Config, DefaultConfig};
pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};
pub use split_ring::SplitRing;

use awaited_batch::AwaitedBatch;
use config::ConfigBounds;
use consumed_watermark::ConsumedWatermark;
use done_word::DoneWord;
use ready_word::{ReadyState, ReadyWord};

use crate::capacity::Capacity;
use crate::common::close_state::CloseState;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkRegistry;

use crate::common::endpoint_count::EndpointCount;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    /// Also panics if the capacity leaves no room for a producer:
    /// `usize::MAX / capacity / 32` must be at least 1.
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
        assert!(
            Self::producer_limit_for(capacity) >= 1,
            "mpmc capacity leaves no room for a producer"
        );
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

    /// The most producer handles that may be live at once.
    ///
    /// A slot's `done` word names a round only modulo
    /// `wraps = 2^usize::BITS / capacity`, so a producer can tell its
    /// round from the slot's current one only while they are fewer than
    /// `wraps` apart. Each producer holds at most one position per slot
    /// in a batch of at most `u32::BITS` positions, so the rounds
    /// reserved ahead of a slot stay below `wraps` while
    /// `u32::BITS * producers < wraps`.
    pub(crate) const fn producer_limit_for(capacity: Capacity) -> usize {
        // `usize::MAX / cap` is `wraps - 1` because `cap` is a power of two.
        usize::MAX / capacity.get() / u32::BITS as usize
    }

    /// [`producer_limit_for`](Self::producer_limit_for) this ring's capacity.
    pub(crate) const fn producer_limit(&self) -> usize {
        Self::producer_limit_for(self.capacity)
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
mod tests;
