//! Multi-producer, single-consumer (MPSC) lock-free ring buffer.
//!
//! Multiple producers push concurrently through one atomic tail cursor.
//! A single consumer pops items in FIFO order. The producer handle is
//! [`Clone`], so new producers can be created at any time.
//!
//! A compare-and-exchange operation checks capacity and advances the tail
//! as one operation. Thus, a non-blocking push cannot claim a full slot.
//!
//! # Example
//!
//! ```
//! use quetzalcoatl::mpsc::SplitRing;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = SplitRing::new(Capacity::exact(16)).split();
//! let p2 = producer.clone();
//!
//! producer.push(1u32).unwrap();
//! p2.push(2).unwrap();
//!
//! // Order depends on scheduling; both values arrive
//! let mut v = vec![consumer.pop().unwrap(), consumer.pop().unwrap()];
//! v.sort();
//! assert_eq!(v, [1, 2]);
//! ```

mod batch_release;
mod claim_ledger;
mod consumer;
mod producer;
#[cfg(all(test, not(loom)))]
mod replay;
mod reservation_release;
mod slot_release;
mod split_ring;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};
pub use split_ring::SplitRing;

use crate::capacity::Capacity;
use crate::common::close_state::CloseState;
use crate::common::endpoint_count::EndpointCount;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkRegistry;
use crate::common::sole_parker::SoleParker;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::{AlignedBuf, SeqSlot};
use claim_ledger::ClaimLedger;

/// A lock-free MPSC ring buffer.
///
/// The state a [`Producer`] / [`Consumer`] pair shares. Obtain the pair
/// from a [`SplitRing`]. The `Producer` is [`Clone`]; the `Consumer` is
/// not (single-consumer).
// repr(C) locks field order: shared immutable fields first (same cache
// line), then the cursor pair, which pads each cursor onto its own line.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) buf: AlignedBuf<SeqSlot<T>>,
    pub(crate) capacity: Capacity,
    ledger: ClaimLedger,
    /// Live producer count; last-drop sets [`closed`].
    pub(crate) producer_count: EndpointCount,
    /// Set by the last [`Producer`] drop. [`Consumer::pop_block`]
    /// observes this and returns `None` once the queue drains.
    pub(crate) closed: CloseState,
    /// Set when the [`Consumer`] is dropped. [`Producer::push_block`]
    /// observes this and returns `Err(val)` instead of hanging.
    pub(crate) consumer_closed: CloseState,
    /// Producer-side park state. Bit `i` of `producer_park.wake` is
    /// set ↔ a producer in slot `i` is parked waiting for free
    /// space. The consumer wakes one parked producer after each pop
    /// (gated on the bitmap being non-zero).
    pub(crate) producer_park: WakeSet,
    /// Leases park-slot indices to `Producer` handles, reclaiming each
    /// on drop so a slot is never shared by two live producers.
    pub(crate) producer_slots: ParkRegistry,
    /// Consumer-side park state. One consumer, so a single flag
    /// answers "is anyone parked?"; producers gate their unpark on it
    /// to keep the no-park hot path free.
    pub(crate) consumer_park: SoleParker,
    /// Async equivalent of `producer_park`: per-producer-slot wakers
    /// registered by `Poll::Pending` from `push_async`. Woken by the
    /// consumer after every pop / drain.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Async equivalent of `consumer_park`: single waker registered by
    /// `Poll::Pending` from `pop_async`. Woken by any producer after a
    /// successful push or commit.
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
}

// Safety: Multiple producers atomically claim unique tail slots.
// Single consumer touches head. Sequence numbers ensure proper synchronization.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new MPSC ring buffer with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if the capacity exceeds 2^30.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let mut idx = 0usize;
        let buf = AlignedBuf::new_with(cap, || {
            let slot = SeqSlot::free_at(idx);
            idx += 1;
            slot
        });

        Self {
            buf,
            ledger: ClaimLedger::new(capacity),
            capacity,
            producer_count: EndpointCount::new(),
            closed: CloseState::new(),
            consumer_closed: CloseState::new(),
            producer_park: WakeSet::new(),
            producer_slots: ParkRegistry::new(),
            consumer_park: SoleParker::new(),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
        }
    }

    /// Returns the number of positions claimed and not yet released.
    ///
    /// It counts a position from its claim, before its value is
    /// published. Under concurrent use it is an estimate, and never
    /// exceeds the capacity.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ledger.len(self.capacity)
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity.get()
    }

    /// Externally closes the ring, causing a blocked
    /// [`pop_block`](Consumer::pop_block) to return `None` once the ring
    /// has drained.
    ///
    /// This signals the consumer; it does not close the producers.
    /// Pushes after it still succeed, and any value left unread is
    /// dropped when the ring is. Callers who need pushes to fail should
    /// drop the consumer, which is what
    /// [`push_block`](Producer::push_block) reports through `Err`.
    pub fn close(&self) {
        self.closed.close();
        self.wake_consumer();
        #[cfg(feature = "async")]
        self.consumer_waker.flush();
    }

    /// Returns a reference to the slot at logical position `pos`.
    ///
    /// Single point of unsafety.
    #[inline]
    pub(crate) fn slot(&self, pos: usize) -> &SeqSlot<T> {
        let idx = self.capacity.index_of(pos);
        // SAFETY: index_of < cap == buf.len().
        unsafe { std::hint::assert_unchecked(idx < self.buf.len()) };
        &self.buf[idx]
    }
}

impl<T> RingBuffer<T> {
    /// Wakes the single parked consumer, if any.
    ///
    /// Fenced: `Producer::push` publishes the slot sequence with
    /// `Release`, so the store buffer must be drained before sampling
    /// the parked flag. See [`SoleParker::wake`].
    ///
    /// Idempotent: clearing the flag means any racing producer that
    /// observes it already cleared simply skips the unpark. The
    /// post-clear unpark is still issued by the winner — a stale
    /// `unpark()` after the consumer rechecks is benign (it makes
    /// the next park return immediately, which is fine because the
    /// consumer re-checks the queue after every wake).
    #[inline]
    pub(crate) fn wake_consumer(&self) {
        self.consumer_park.wake();
    }

    /// Wakes the async consumer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_consumer_async(&self) {
        self.consumer_waker.wake_all();
    }

    /// Wakes the async producer tasks waiting for space.
    ///
    /// Call sites are unconditional; this and
    /// [`notify_producers_n`](Self::notify_producers_n) are where the
    /// async feature enters the producer notification path.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_producers(&self) {
        self.producer_waker.wake_all();
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_producers(&self) {}

    /// Wakes up to `n` async producer tasks. Mirrors
    /// `producer_park.wake_n` for batched releases.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_producers_n(&self, n: usize) {
        self.producer_waker.wake_n(n);
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_producers_n(&self, _n: usize) {}
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        for pos in self.ledger.occupied(self.capacity) {
            self.buf[self.capacity.index_of(pos)].drop_value_at(pos);
        }
    }
}

#[cfg(test)]
mod tests;
