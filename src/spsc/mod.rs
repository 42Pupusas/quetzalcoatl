//! Single-producer, single-consumer (SPSC) lock-free ring buffer.
//!
//! The fastest variant — no CAS loops, no per-slot flags. The producer
//! writes sequentially and advances `tail`; the consumer reads and
//! advances `head`. Synchronization uses only `Acquire`/`Release` on
//! the shared cursors.
//!
//! Neither [`Producer`] nor [`Consumer`] is [`Clone`]; this enforces the
//! single-producer, single-consumer invariant at compile time.
//!
//! # Example
//!
//! ```
//! use quetzalcoatl::spsc::SplitRing;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = SplitRing::new(Capacity::exact(16)).split();
//!
//! producer.push(42u64).unwrap();
//! producer.push(43).unwrap();
//!
//! assert_eq!(consumer.pop(), Some(42));
//! assert_eq!(consumer.pop(), Some(43));
//! assert_eq!(consumer.pop(), None);
//! ```

mod consumer;
mod consumer_close;
mod head_publisher;
#[cfg(test)]
mod pinned_ring;
mod producer;
mod split_ring;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};
pub use split_ring::SplitRing;

use crate::capacity::Capacity;
use crate::common::close_state::CloseState;
use crate::common::cursors::Cursors;
use crate::common::sole_parker::SoleParker;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::AlignedBuf;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

/// A lock-free SPSC ring buffer.
///
/// The state a [`Producer`] / [`Consumer`] pair shares. Obtain the pair
/// from a [`SplitRing`]. Neither handle is [`Clone`], enforcing the
/// single-producer, single-consumer invariant.
// repr(C) locks field order: shared immutable fields first (same cache
// line), then the cursor pair, which pads each cursor onto its own line.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) buf: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    pub(crate) capacity: Capacity,
    pub(crate) cursors: Cursors,
    /// Set when the [`Producer`] is dropped. [`Consumer::pop_block`]
    /// observes this and returns `None` once the queue drains.
    pub(crate) producer_closed: CloseState,
    /// Set when the [`Consumer`] is dropped. [`Producer::push_block`]
    /// observes this and returns `Err(val)` instead of hanging.
    pub(crate) consumer_closed: CloseState,
    /// Producer-side park state. One producer, so a single flag
    /// answers "is anyone parked?"; the consumer gates its unpark on
    /// it to keep the no-park hot path free.
    pub(crate) producer_park: SoleParker,
    /// Consumer-side park state. Symmetric to `producer_park`.
    pub(crate) consumer_park: SoleParker,
    /// Async waker for the single producer. Registered before returning
    /// `Poll::Pending` from `push_async`; woken by the consumer after
    /// each `pop` or `drain`.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Async waker for the single consumer. Registered before returning
    /// `Poll::Pending` from `pop_async`; woken by the producer after
    /// each `push`.
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
}

// Safety: Single producer writes via tail, single consumer reads via head.
// Atomic head/tail with Acquire/Release ordering provide synchronization.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // The consumer drains on drop, so normally nothing is left. But
        // the producer outlives it: a push that lands after the consumer
        // is gone has no one to take the value, and without this the
        // storage was freed with the value still in it — a leak for any
        // `T` that owns resources.
        //
        // Handles are all gone (we hold `&mut self`), so the cursors can
        // be read non-atomically, and everything between them is an
        // initialized value nobody consumed.
        for pos in self.cursors.occupied(self.capacity) {
            // SAFETY: the producer published `pos < tail` and the
            // consumer never advanced `head` past it, so the slot holds
            // an initialized value. `&mut self` rules out concurrency.
            unsafe {
                self.buf[self.capacity.index_of(pos)]
                    .get()
                    .cast::<T>()
                    .drop_in_place();
            }
        }
    }
}

impl<T> RingBuffer<T> {
    /// Creates a new SPSC ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let buf = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));

        Self {
            buf,
            cursors: Cursors::new(),
            capacity,
            producer_closed: CloseState::new(),
            consumer_closed: CloseState::new(),
            producer_park: SoleParker::new(),
            consumer_park: SoleParker::new(),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
        }
    }

    /// Wakes the parked producer, if any. Call after publishing the
    /// progress being announced — the freed slot in pop/drain, or the
    /// close in `Consumer::drop`.
    ///
    /// Fence-free: every caller publishes with a `SeqCst` store
    /// (`head`, or the close flag), which drains the store buffer
    /// itself. A caller that switched to `Release` would have to move
    /// to [`SoleParker::wake`].
    #[inline]
    pub(crate) fn wake_producer(&self) {
        self.producer_park.wake_published();
    }

    /// Wakes the parked consumer, if any. Symmetric to
    /// [`wake_producer`](Self::wake_producer), and fence-free for the
    /// same reason: the callers publish `tail` with `SeqCst`.
    #[inline]
    pub(crate) fn wake_consumer(&self) {
        self.consumer_park.wake_published();
    }

    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_producer_async(&self) {
        self.producer_waker.wake_all();
    }

    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_consumer_async(&self) {
        self.consumer_waker.wake_all();
    }

    /// Returns the approximate number of items currently in the buffer.
    ///
    /// Both cursors are loaded with `Relaxed` ordering, so the result may
    /// transiently exceed `capacity` when observed from a thread that is
    /// neither the producer nor the consumer. Use this for heuristics
    /// (e.g., "is there work?"), not for precise invariants.
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

    /// Externally closes the ring, causing a blocked
    /// [`pop_block`](Consumer::pop_block) to return `None` once the ring
    /// has drained.
    ///
    /// This signals the consumer; it does not close the producer.
    /// Pushes after it still succeed, and a value pushed after the
    /// consumer has stopped reading stays in the ring until the ring
    /// itself is dropped, which reclaims it. Callers who need pushes to
    /// fail should drop the consumer, which is what
    /// [`push_block`](Producer::push_block) reports through `Err`.
    pub fn close(&self) {
        self.producer_closed.close();
        self.wake_consumer();
        #[cfg(feature = "async")]
        self.consumer_waker.flush();
    }

    /// Returns a reference to the slot at logical position `pos`.
    ///
    /// Single point of unsafety. Hinting `index_of`'s bound to the
    /// optimizer lets safe indexing compile to the same code as
    /// `get_unchecked`, while keeping every call site in safe Rust.
    #[inline]
    pub(crate) fn slot(&self, pos: usize) -> &UnsafeCell<MaybeUninit<T>> {
        let idx = self.capacity.index_of(pos);
        // SAFETY: index_of < cap == buf.len().
        unsafe { std::hint::assert_unchecked(idx < self.buf.len()) };
        &self.buf[idx]
    }
}

#[cfg(test)]
mod tests;
