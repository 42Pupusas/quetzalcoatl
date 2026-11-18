//! Single-producer, multiple-consumer (SPMC) lock-free ring buffer.
//!
//! One producer pushes items; multiple consumers compete to pop them.
//! Each item is consumed by exactly one consumer, making this ideal
//! for work-distribution patterns.
//!
//! The consumer handle is [`Clone`], so new consumers can join at any
//! time. The producer is **not** cloneable — single-producer is enforced
//! at compile time.
//!
//! # Ordering guarantees
//!
//! Items are claimed in batches: each consumer reserves up to `BATCH_SIZE`
//! consecutive positions in one bounded CAS on the shared `head` cursor,
//! then drains its batch from a private cursor. The implications:
//!
//! * **Per-consumer FIFO**: within a single consumer's stream, items are
//!   delivered in the order they were pushed.
//! * **Cross-consumer order is best-effort**: the interleaving between
//!   consumers' streams is determined by who wins each batch CAS, not by
//!   item-arrival order. With `BATCH_SIZE = 4`, one consumer can grab four
//!   consecutive items before another sees any.
//! * **Each item is delivered to exactly one consumer.** No duplication,
//!   no loss.
//!
//! If you need strict global FIFO across consumers, use one consumer; the
//! batched claim is a work-distribution optimization that trades cross-
//! consumer ordering for substantially higher throughput under contention.
//!
//! # Example
//!
//! ```
//! use quetzalcoatl::spmc::SplitRing;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = SplitRing::new(Capacity::exact(16)).split();
//! let c2 = consumer.clone();
//!
//! producer.push(1u32).unwrap();
//! producer.push(2).unwrap();
//!
//! // Each item goes to exactly one consumer
//! let a = consumer.pop();
//! let b = c2.pop();
//! assert!(a.is_some() || b.is_some());
//! ```

mod consumer;
mod consumer_close;
mod done_word;
mod drain_wake;
mod producer;
mod slot_release;
mod split_ring;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};
pub use split_ring::SplitRing;

use crate::capacity::Capacity;
use crate::common::close_state::CloseState;
use crate::common::cursors::Cursors;
use crate::common::endpoint_count::EndpointCount;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkRegistry;
use crate::common::sole_parker::SoleParker;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::AlignedBuf;
use done_word::DoneWord;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

/// A lock-free SPMC ring buffer.
///
/// The state a [`Producer`] / [`Consumer`] pair shares. Obtain the pair
/// from a [`SplitRing`]. The `Consumer` is [`Clone`]; the `Producer` is
/// not (single-producer).
///
/// # Layout
///
/// Data and completion markers use separate packed arrays.
///
/// # Synchronization
///
/// The producer writes data and then Release-stores the next position to
/// `tail`. Consumers Acquire-load `tail` before they claim published positions.
/// A consumer Release-stores `done[s] = p + cap` after it reads position `p`.
/// The producer Acquire-loads this marker before it reuses the slot.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) data: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    /// Consumer-write, producer-read free-for-reuse marker.
    done: AlignedBuf<DoneWord>,
    pub(crate) capacity: Capacity,
    /// Consumer positions are claimed by compare-exchange on `head`,
    /// so the pair's single-consumer publish helper does not apply
    /// here; `tail` is published Release by the lone producer, and
    /// consumers Acquire-load it to bound their batched claim so they
    /// never take a position the producer has not yet published.
    pub(crate) cursors: Cursors,
    /// Set by `Producer::Drop` so that consumers can distinguish
    /// "transiently empty" from "permanently drained" via `is_closed()`.
    pub(crate) closed: CloseState,
    /// Set by the last `Consumer` drop. `Producer::push_block`
    /// observes this and returns `Err(val)` instead of hanging
    /// (no consumer left to drain).
    pub(crate) consumer_closed: CloseState,
    /// Live consumer count; last-drop sets `consumer_closed`.
    pub(crate) consumer_count_live: EndpointCount,
    /// Leases park-slot indices to `Consumer` handles, reclaiming each
    /// on drop so a slot is never shared by two live consumers.
    pub(crate) consumer_slots: ParkRegistry,
    /// Producer-side park state. One producer, so a single flag
    /// answers "is anyone parked?"; consumers gate their unpark on it
    /// to keep the no-park hot path free.
    pub(crate) producer_park: SoleParker,
    /// Consumer-side park state. Bit `i` of `consumer_park.wake` is
    /// set ↔ a consumer in slot `i` is parked waiting for a publish.
    /// The producer wakes one parked consumer after each publish
    /// (gated on the bitmap being non-zero).
    pub(crate) consumer_park: WakeSet,
    /// Async equivalent of `producer_park`: single waker registered
    /// from `Poll::Pending` in `push_async`. Woken by any consumer
    /// after a successful pop.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Async equivalent of `consumer_park`: per-consumer-slot wakers
    /// registered from `Poll::Pending` in `pop_async`. Woken by the
    /// producer after every push / commit.
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
}

// Safety: The single producer writes slot data before it publishes tail.
// Multiple consumers acquire tail and CAS head before they read distinct slots.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new SPMC ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let data = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        let mut idx = 0usize;
        let done = AlignedBuf::new_with(cap, || {
            let d = DoneWord::released_for(idx);
            idx += 1;
            d
        });

        Self {
            data,
            done,
            cursors: Cursors::new(),
            closed: CloseState::new(),
            consumer_closed: CloseState::new(),
            consumer_count_live: EndpointCount::new(),
            consumer_slots: ParkRegistry::new(),
            producer_park: SoleParker::new(),
            consumer_park: WakeSet::new(),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
            capacity,
        }
    }

    /// Wakes the parked producer, if any.
    ///
    /// Fenced: `Consumer::pop` and the drains publish `slot_done` with
    /// `Release`, so the store buffer must be drained before sampling
    /// the parked flag. See [`SoleParker::wake`].
    #[inline]
    pub(crate) fn wake_producer(&self) {
        self.producer_park.wake();
    }

    /// Wakes the async producer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_producer_async(&self) {
        self.producer_waker.wake_all();
    }

    /// Wakes one async consumer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_consumer_async(&self) {
        self.consumer_waker.wake_all();
    }

    /// Returns the approximate number of items currently in the buffer.
    ///
    /// Both cursors are loaded with `Relaxed` ordering, so the result may
    /// transiently exceed `capacity` when observed from another thread.
    /// Additionally, consumers reserve positions in batches via the head
    /// cursor — items reserved-but-not-yet-popped are counted as
    /// consumed by this method, so `len()` may transiently underestimate
    /// the number of pending items by up to `BATCH_SIZE` per consumer.
    /// Use this for heuristics, not for precise invariants.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cursors.len()
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.cursors.is_full(self.capacity.get())
    }

    /// Returns a reference to the data slot at logical position `pos`.
    ///
    /// Single point of unsafety for indexing. Both per-slot arrays
    /// have length `cap`.
    #[inline]
    pub(crate) fn data_slot(&self, pos: usize) -> &UnsafeCell<MaybeUninit<T>> {
        let idx = self.capacity.index_of(pos);
        // SAFETY: index_of < cap == data.len().
        unsafe { std::hint::assert_unchecked(idx < self.data.len()) };
        &self.data[idx]
    }

    /// Returns a reference to the done marker at logical position `pos`.
    #[inline]
    fn done_slot(&self, pos: usize) -> &DoneWord {
        let idx = self.capacity.index_of(pos);
        // SAFETY: index_of < cap == done.len().
        unsafe { std::hint::assert_unchecked(idx < self.done.len()) };
        &self.done[idx]
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        for pos in self.cursors.occupied(self.capacity) {
            let s = self.capacity.index_of(pos);
            if self.done[s].holds_unconsumed_value_at(pos, self.capacity) {
                // SAFETY: data is initialized and unconsumed.
                unsafe {
                    self.data[s].get().cast::<T>().drop_in_place();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
