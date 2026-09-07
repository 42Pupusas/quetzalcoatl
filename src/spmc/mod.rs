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
//! use quetzalcoatl::spmc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = RingBuffer::new(Capacity::exact(16)).split();
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
mod producer;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};

use crate::capacity::Capacity;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkRegistry;
use crate::common::close_state::CloseState;
use crate::common::sole_parker::SoleParker;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::cursors::Cursors;
use crate::common::endpoint_count::EndpointCount;
use crate::common::AlignedBuf;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

/// A lock-free SPMC ring buffer.
///
/// Created via [`RingBuffer::new`], then [`split`](RingBuffer::split) into
/// a [`Producer`] / [`Consumer`] pair. The `Consumer` is [`Clone`]; the
/// `Producer` is not (single-producer).
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
    pub(crate) done: AlignedBuf<AtomicUsize>,
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
        // done[s] = s — slot s is "free for producer at logical position
        // s" on the first lap. After a consumer at position p reads slot
        // s, it stores done[s] = p + cap, marking it free for the next
        // lap (logical position p + cap == s + cap == s + cap).
        let mut idx = 0usize;
        let done = AlignedBuf::new_with(cap, || {
            let d = AtomicUsize::new(idx);
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
    pub(crate) fn done_slot(&self, pos: usize) -> &AtomicUsize {
        let idx = self.capacity.index_of(pos);
        // SAFETY: index_of < cap == done.len().
        unsafe { std::hint::assert_unchecked(idx < self.done.len()) };
        &self.done[idx]
    }

    /// Splits the ring buffer into a [`Producer`] and [`Consumer`] pair.
    ///
    /// Both handles hold an `Arc` to the ring buffer, so `T` must be
    /// `'static`. For borrowed data (non-`'static` `T`), use
    /// [`split_borrowed`](Self::split_borrowed).
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self);
        let producer = Producer {
            queue: arc.clone(),
            write_pos: std::cell::Cell::new(0),
        };
        let consumer = Consumer::new(arc);
        (producer, consumer)
    }

    /// Splits a *borrowed* ring buffer into a producer/consumer pair
    /// whose lifetimes are tied to the ring.
    ///
    /// Unlike [`split`](Self::split), the handles hold `&RingBuffer<T>`
    /// instead of `Arc<RingBuffer<T>>`. This lets `T` carry lifetimes
    /// shorter than `'static`.
    ///
    /// The returned consumer is not `Clone`. Create additional consumers
    /// via [`Consumer::new_consumer`].
    ///
    /// Takes `&mut self` so the ring can be split only once. A second
    /// split would mint a second *producer* — breaking the single-
    /// producer contract, since each producer keeps a private write
    /// cursor and two of them publish a `tail` past a slot neither
    /// initialized. The exclusive borrow makes that a borrow-check error
    /// rather than undefined behavior.
    ///
    /// ```compile_fail
    /// use quetzalcoatl::spmc::RingBuffer;
    /// use quetzalcoatl::capacity::Capacity;
    ///
    /// let mut ring = RingBuffer::<String>::new(Capacity::exact(4));
    /// let (p1, c1) = ring.split_borrowed();
    /// let (p2, c2) = ring.split_borrowed();
    /// p1.push("a".to_string()).unwrap();
    /// ```
    #[must_use]
    pub fn split_borrowed(&mut self) -> (Producer<T, &Self>, Consumer<T, &Self>) {
        // No increment: the count is seeded at one for the consumer a
        // split hands out, exactly as `split` relies on. Counting this
        // one twice left the tally at one after the only consumer
        // dropped, so `consumer_closed` was never set and a producer
        // blocked in `push_block` waited for a consumer that no longer
        // existed.
        let shared: &Self = self;
        let producer = Producer::new_with(shared);
        let consumer = Consumer::new_with(shared, shared.consumer_slots.lease());
        (producer, consumer)
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        for pos in self.cursors.occupied() {
            let s = self.capacity.index_of(pos);
            let d = *self.done[s].get_mut();
            if d != pos + self.capacity.get() {
                // SAFETY: data is initialized and unconsumed.
                unsafe {
                    self.data[s].get().cast::<T>().drop_in_place();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::Capacity;

    #[test]
    fn capacity_one() {
        let (producer, consumer) = RingBuffer::<u8>::new(Capacity::exact(1)).split();
        assert_eq!(producer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn zero_sized_types() {
        let (producer, consumer) = RingBuffer::<()>::new(Capacity::exact(4)).split();

        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn stress_push_pop() {
        let cap = Capacity::exact(128 * 1024 * 1024);
        let n = cap.get() as u64;
        let instant = std::time::Instant::now();
        let (producer, _consumer) = RingBuffer::<u64>::new(cap).split();

        for i in 0..n {
            producer
                .push(i)
                .unwrap_or_else(|_| panic!("Failed to push {i}"));
        }
        println!("Took {}ms", instant.elapsed().as_millis());
    }

    #[test]
    fn pop_empty_is_idempotent() {
        let (producer, consumer) = RingBuffer::<u8>::new(Capacity::exact(1)).split();
        assert_eq!(consumer.pop(), None);
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
        assert_eq!(consumer.pop(), None);
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn length_invariants() {
        let (producer, consumer) = RingBuffer::<u8>::new(Capacity::exact(2)).split();
        assert_eq!(producer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        producer.push(2).unwrap();
        assert_eq!(producer.len(), 2);
        assert_eq!(producer.push(3), Err(3));
        assert_eq!(producer.len(), 2);
        // After pop, len() may underestimate by up to BATCH_SIZE per
        // consumer because consumers reserve positions in batches via
        // the head cursor. We only check that len() is bounded by the
        // count of remaining items.
        assert_eq!(consumer.pop(), Some(1));
        assert!(consumer.len() <= 1);
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn overwrite_oldest_element() {
        let (producer, consumer) = RingBuffer::<u8>::new(Capacity::exact(4)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();
        producer.push(4).unwrap();

        assert_eq!(producer.push(5), Err(5));
        assert_eq!(consumer.pop(), Some(1));
        producer.push(5).unwrap();

        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), Some(5));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn wraparound_behavior() {
        let (producer, consumer) = RingBuffer::<u8>::new(Capacity::exact(4)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        producer.push(3).unwrap();
        producer.push(4).unwrap();
        producer.push(5).unwrap(); // wraps here
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), Some(5));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn fill_to_capacity() {
        let (producer, consumer) = RingBuffer::<u8>::new(Capacity::exact(16)).split();

        for i in 0..16 {
            producer.push(i).unwrap();
        }
        assert_eq!(producer.len(), 16);
        assert!(!producer.is_empty());
        assert!(producer.is_full());

        for i in 0..16 {
            assert_eq!(consumer.pop(), Some(i));
        }
        assert_eq!(consumer.len(), 0);
        assert!(consumer.is_empty());
        assert!(!consumer.is_full());
    }

    // -----------------------------------------------------------------------
    // SPMC-specific tests
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "too slow for Miri"]
    fn multiple_consumers_concurrent() {
        let total = 1000usize;
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();
        let remaining = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(total));

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let c = consumer.clone();
                let rem = remaining.clone();
                std::thread::spawn(move || {
                    let mut received = Vec::new();
                    while rem.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                        if let Some(v) = c.pop() {
                            rem.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            received.push(v);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    received
                })
            })
            .collect();

        for i in 0..total as u64 {
            while producer.push(i).is_err() {
                std::thread::yield_now();
            }
        }

        // Wait for consumers and collect all received values
        let mut all_received: Vec<u64> = Vec::new();
        for h in handles {
            all_received.extend(h.join().unwrap());
        }

        all_received.sort_unstable();
        all_received.dedup();

        // Each item consumed exactly once — dedup should not change the count
        // (no duplicates), and we should have all 1000 values.
        assert_eq!(all_received.len(), total);
        let expected: Vec<u64> = (0..total as u64).collect();
        assert_eq!(all_received, expected);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn dynamic_consumer_creation() {
        let (producer, consumer) = RingBuffer::<usize>::new(Capacity::exact(128)).split();

        // Push items first
        for i in 0..5 {
            producer.push(i).unwrap();
        }

        // Create consumers dynamically and let them compete
        let mut handles = Vec::new();
        for _ in 0..5 {
            let c = consumer.clone();
            let handle = std::thread::spawn(move || {
                let mut received = Vec::new();
                while let Some(v) = c.pop() {
                    received.push(v);
                }
                received
            });
            handles.push(handle);
        }

        let mut all_received: Vec<usize> = Vec::new();
        for h in handles {
            all_received.extend(h.join().unwrap());
        }

        all_received.sort_unstable();
        assert_eq!(all_received, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn spmc_stress_test() {
        let total_items = 4000usize;
        let num_consumers = 4;
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::at_least(10000)).split();
        let remaining = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(total_items));

        let consumer_handles: Vec<_> = (0..num_consumers)
            .map(|_| {
                let c = consumer.clone();
                let rem = remaining.clone();
                std::thread::spawn(move || {
                    let mut received = Vec::new();
                    while rem.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                        if let Some(v) = c.pop() {
                            rem.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            received.push(v);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    received
                })
            })
            .collect();

        for i in 0..total_items as u64 {
            while producer.push(i).is_err() {
                std::thread::yield_now();
            }
        }

        let mut all_received: Vec<u64> = Vec::new();
        for h in consumer_handles {
            all_received.extend(h.join().unwrap());
        }

        all_received.sort_unstable();
        all_received.dedup();
        assert_eq!(all_received.len(), total_items);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn clone_consumer_competes() {
        let n = 100usize;
        let (producer, consumer) = RingBuffer::<usize>::new(Capacity::exact(64)).split();
        let remaining = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(n));

        let c2 = consumer.clone();
        let rem1 = remaining.clone();
        let h1 = std::thread::spawn(move || {
            let mut received = Vec::new();
            while rem1.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                if let Some(v) = consumer.pop() {
                    rem1.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    received.push(v);
                } else {
                    std::thread::yield_now();
                }
            }
            received
        });
        let rem2 = remaining;
        let h2 = std::thread::spawn(move || {
            let mut received = Vec::new();
            while rem2.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                if let Some(v) = c2.pop() {
                    rem2.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    received.push(v);
                } else {
                    std::thread::yield_now();
                }
            }
            received
        });

        for i in 0..n {
            while producer.push(i).is_err() {
                std::thread::yield_now();
            }
        }

        let mut all: Vec<usize> = Vec::new();
        all.extend(h1.join().unwrap());
        all.extend(h2.join().unwrap());
        all.sort_unstable();
        all.dedup();

        // All items consumed, no duplicates
        assert_eq!(all.len(), n);
    }

    // -----------------------------------------------------------------------
    // Miri-targeted tests: small sizes exercising all unsafe code paths
    // -----------------------------------------------------------------------

    use crate::common::DropCounter;

    /// Items still in the buffer must be dropped when the `RingBuffer` drops.
    #[test]
    fn drop_items_on_consumer_drop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (producer, consumer) = RingBuffer::new(Capacity::exact(4)).split();

            for _ in 0..4 {
                producer
                    .push(DropCounter {
                        counter: counter.clone(),
                    })
                    .unwrap();
            }

            // Consumer::Drop does NOT drain — RingBuffer::Drop handles cleanup.
            drop(consumer);
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);

            // RingBuffer drops when last Arc (producer) is released.
            drop(producer);
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    /// Items popped normally should be dropped exactly once.
    #[test]
    fn drop_items_on_pop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (producer, consumer) = RingBuffer::new(Capacity::exact(4)).split();

            for _ in 0..3 {
                producer
                    .push(DropCounter {
                        counter: counter.clone(),
                    })
                    .unwrap();
            }

            // Pop 2 — should drop when they go out of scope
            let a = consumer.pop().unwrap();
            let b = consumer.pop().unwrap();
            drop(a);
            drop(b);
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);

            // Remaining 1 dropped when RingBuffer drops (all Arcs released)
            drop(consumer);
            drop(producer);
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    /// Exercise `get_unchecked` on every index by wrapping around multiple times.
    #[test]
    fn wraparound_exercises_all_slots() {
        let (producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        // 3 full laps = 12 push/pops, covering slot indices 0-3 three times
        for lap in 0..3u32 {
            for i in 0..4 {
                producer.push(lap * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(consumer.pop(), Some(lap * 4 + i));
            }
        }
        assert_eq!(consumer.pop(), None);
    }

    /// Concurrent push/pop with a tiny buffer — Miri checks for data races.
    #[test]
    fn concurrent_data_race_check() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 16u64;

        let handle = std::thread::spawn(move || {
            for i in 0..n {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        let mut received = 0u64;
        while received < n {
            if consumer.pop().is_some() {
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }

        handle.join().unwrap();
        assert_eq!(received, n);
    }

    /// Two consumers, tiny buffer — checks CAS + sequence synchronization.
    #[test]
    fn concurrent_spmc_data_race_check() {
        let total = 16usize;
        let (producer, consumer) = RingBuffer::<usize>::new(Capacity::exact(4)).split();

        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let c2 = consumer.clone();
        let r1 = received.clone();
        let h1 = std::thread::spawn(move || {
            while r1.load(std::sync::atomic::Ordering::Relaxed) < total {
                if consumer.pop().is_some() {
                    r1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }
        });
        let r2 = received.clone();
        let h2 = std::thread::spawn(move || {
            while r2.load(std::sync::atomic::Ordering::Relaxed) < total {
                if c2.pop().is_some() {
                    r2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }
        });

        for i in 0..total {
            while producer.push(i).is_err() {
                std::thread::yield_now();
            }
        }

        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(received.load(std::sync::atomic::Ordering::Relaxed), total);
    }

    // -----------------------------------------------------------------------
    // Zero-copy API tests
    // -----------------------------------------------------------------------

    /// See the spsc twin: a `SlotWriter` leaked with `mem::forget` runs
    /// no destructor, so the reservation must not have advanced the
    /// producer cursor or `tail` would publish an uninitialized slot.
    #[test]
    fn forgotten_writer_does_not_publish_uninitialized_slot() {
        let (mut producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        std::mem::forget(producer.reserve().unwrap());

        producer.push(7).unwrap();

        assert_eq!(
            consumer.pop(),
            Some(7),
            "the forgotten slot must be reused, not published uninitialized"
        );
        assert_eq!(consumer.pop(), None, "no phantom item may become visible");
    }

    #[test]
    fn forgotten_written_slot_leaks_without_publishing() {
        // `BorrowedDropCounter` owns no heap memory, so the deliberate
        // `mem::forget` below leaks only the destructor call this test
        // asserts about — nothing for Miri's leak checker to flag.
        let counter = std::sync::atomic::AtomicUsize::new(0);
        let mut ring =
            RingBuffer::<crate::common::BorrowedDropCounter<'_>>::new(Capacity::exact(4));
        let (mut producer, consumer) = ring.split_borrowed();

        std::mem::forget(
            producer
                .reserve()
                .unwrap()
                .write(crate::common::BorrowedDropCounter::new(&counter)),
        );

        assert!(
            consumer.pop().is_none(),
            "an uncommitted value must not be visible"
        );
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a forgotten guard runs no destructor — the value leaks in place"
        );

        producer
            .push(crate::common::BorrowedDropCounter::new(&counter))
            .unwrap();
        assert!(consumer.pop().is_some(), "the slot must be reusable");
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "only the popped value is dropped; the leaked one is never freed"
        );
    }

    #[test]
    fn forgotten_writer_does_not_consume_capacity() {
        let (mut producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        std::mem::forget(producer.reserve().unwrap());

        for i in 0..4 {
            producer.push(i).unwrap();
        }
        assert!(producer.push(99).is_err(), "ring should now be full");

        let mut got = vec![];
        while let Some(v) = consumer.pop() {
            got.push(v);
        }
        assert_eq!(got, vec![0, 1, 2, 3]);
    }

    #[test]
    fn reserve_write_commit_pop_ref_cycle() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

        let writer = producer.reserve().unwrap();
        writer.write(42).commit();

        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 42);
        drop(reader);

        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn reserve_slot_mut_commit() {
        let (mut producer, mut consumer) = RingBuffer::<[u8; 64]>::new(Capacity::exact(4)).split();

        let mut writer = producer.reserve().unwrap();
        writer.slot_mut().write([0xAB; 64]);
        // SAFETY: slot_mut().write() initialized the slot above.
        unsafe { writer.commit_unchecked() };

        let reader = consumer.pop_ref().unwrap();
        assert_eq!(reader[0], 0xAB);
        assert_eq!(reader[63], 0xAB);
    }

    #[test]
    fn reserve_returns_none_when_full() {
        let (mut producer, _consumer) = RingBuffer::<u64>::new(Capacity::exact(2)).split();

        let w1 = producer.reserve().unwrap();
        w1.write(1).commit();

        let w2 = producer.reserve().unwrap();
        w2.write(2).commit();

        assert!(producer.reserve().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_empty() {
        let (_producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn mixed_push_reserve_pop_pop_ref() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();

        // Mix of push and reserve
        producer.push(1).unwrap();
        let w = producer.reserve().unwrap();
        w.write(2).commit();
        producer.push(3).unwrap();

        // Mix of pop and pop_ref
        assert_eq!(consumer.pop(), Some(1));
        let r = consumer.pop_ref().unwrap();
        assert_eq!(*r, 2);
        drop(r);
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn slot_reader_drops_value() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) = RingBuffer::new(Capacity::exact(4)).split();

        producer
            .push(DropCounter {
                counter: counter.clone(),
            })
            .unwrap();

        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);

        {
            let reader = consumer.pop_ref().unwrap();
            // Value is alive while reader exists
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
            drop(reader);
        }

        // Value should be dropped when SlotReader is dropped
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn reserve_pop_ref_wraparound() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        // 3 full laps via reserve/pop_ref
        for lap in 0..3u32 {
            for i in 0..4 {
                let w = producer.reserve().unwrap();
                w.write(lap * 4 + i).commit();
            }
            for i in 0..4 {
                let r = consumer.pop_ref().unwrap();
                assert_eq!(*r, lap * 4 + i);
                drop(r);
            }
        }
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn concurrent_reserve_pop_ref() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 16u64;

        let handle = std::thread::spawn(move || {
            for i in 0..n {
                loop {
                    if let Some(w) = producer.reserve() {
                        w.write(i).commit();
                        break;
                    }
                    std::thread::yield_now();
                }
            }
        });

        let mut received = 0u64;
        while received < n {
            if let Some(reader) = consumer.pop_ref() {
                assert_eq!(*reader, received);
                drop(reader);
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }

        handle.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Audit gap scenarios (small-N, Miri-friendly)
    // -----------------------------------------------------------------------

    /// Producer drop sets `closed`; consumer drains then sees None and
    /// `is_closed() == true`. Exercises the closed-flag synchronization edge.
    #[test]
    fn producer_drop_marks_closed_after_drain() {
        let (producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        for i in 0..3 {
            producer.push(i).unwrap();
        }
        drop(producer);

        assert!(consumer.is_closed());

        let mut got = Vec::new();
        while let Some(v) = consumer.pop() {
            got.push(v);
        }
        assert_eq!(got, vec![0, 1, 2]);
        assert!(consumer.is_closed());
        assert_eq!(consumer.pop(), None);
    }

    /// Consumer dropped mid-batch must release `done` so a subsequent
    /// producer push at the same slot succeeds. Without proper Drop, the
    /// producer would stall forever.
    #[test]
    fn consumer_drop_mid_batch_releases_done() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cap = 4;
        let (producer, consumer) = RingBuffer::new(Capacity::exact(cap)).split();

        // Fill the buffer.
        for _ in 0..cap {
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
        }

        // Pop one item, claiming a batch of size cap. The consumer's
        // private cursor now sits mid-batch; positions [1, cap) are
        // claimed-but-unread.
        let v = consumer.pop().unwrap();
        drop(v);
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the popped item should be dropped"
        );

        // Drop the consumer mid-batch: must drop the unread items AND
        // release each `done[s] = pos + cap` so the producer can reuse.
        drop(consumer);
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            cap,
            "all unread items in the batch must be dropped"
        );

        // Producer can now push `cap` more items into the released slots.
        for _ in 0..cap {
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
        }

        // Cleanup: drop producer + ringbuffer drops the new cap items.
        drop(producer);
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            cap * 2,
            "all items ever pushed should be dropped exactly once"
        );
    }

    /// Capacity-1 with concurrent producer/consumer threads. Tightest
    /// possible synchronization — every push must wait for every pop.
    #[test]
    fn capacity_one_concurrent() {
        let n = 8u64;
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(1)).split();

        let h = std::thread::spawn(move || {
            for i in 0..n {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        let mut got = Vec::new();
        while got.len() < usize::try_from(n).unwrap() {
            if let Some(v) = consumer.pop() {
                got.push(v);
            } else {
                std::thread::yield_now();
            }
        }
        h.join().unwrap();
        assert_eq!(got, (0..n).collect::<Vec<_>>());
    }

    /// ZST that has Drop glue. ZSTs use a dangling pointer in `AlignedBuf`;
    /// Drop must still run for each slot.
    #[test]
    fn zst_with_drop_glue() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct RealZst;
        impl Drop for RealZst {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        assert_eq!(std::mem::size_of::<RealZst>(), 0);

        DROPS.store(0, Ordering::Relaxed);
        {
            let (producer, consumer) = RingBuffer::<RealZst>::new(Capacity::exact(4)).split();
            for _ in 0..4 {
                producer.push(RealZst).unwrap();
            }
            drop(consumer.pop().unwrap());
            drop(consumer.pop().unwrap());
            assert_eq!(DROPS.load(Ordering::Relaxed), 2);
            // RingBuffer::Drop drops the remaining 2
            drop(consumer);
            drop(producer);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 4);
    }

    // -----------------------------------------------------------------------
    // Blocking API
    // -----------------------------------------------------------------------

    #[test]
    fn pop_block_wakes_on_push() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        p.push(7).unwrap();
        assert_eq!(h.join().unwrap(), Some(7));
    }

    #[test]
    fn pop_block_returns_none_on_close() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(p);
        assert_eq!(h.join().unwrap(), None);
    }

    #[test]
    fn pop_block_drains_before_close() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        drop(p);
        assert_eq!(c.pop_block(), Some(1));
        assert_eq!(c.pop_block(), Some(2));
        assert_eq!(c.pop_block(), None);
    }

    #[test]
    fn push_block_unblocks_on_pop() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let h = std::thread::spawn(move || p.push_block(99));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(c.pop(), Some(0));
        assert_eq!(h.join().unwrap(), Ok(()));
        // Drain the rest so RingBuffer::Drop has nothing live to handle.
        assert_eq!(c.pop(), Some(1));
        assert_eq!(c.pop(), Some(2));
        assert_eq!(c.pop(), Some(3));
        assert_eq!(c.pop(), Some(99));
    }

    #[test]
    fn push_block_returns_err_on_consumer_close() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let h = std::thread::spawn(move || p.push_block(99));
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(c);
        assert_eq!(h.join().unwrap(), Err(99));
    }

    #[test]
    fn pop_block_multi_consumer_wakes_all_on_close() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let c2 = c.clone();
        let h1 = std::thread::spawn(move || c.pop_block());
        let h2 = std::thread::spawn(move || c2.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(p);
        assert_eq!(h1.join().unwrap(), None);
        assert_eq!(h2.join().unwrap(), None);
    }

    // -----------------------------------------------------------------------
    // Zero-copy blocking API
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_block_unblocks_on_pop() {
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(2)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        assert!(p.reserve().is_none());
        let h = std::thread::spawn(move || {
            let w = p.reserve_block().unwrap();
            w.write(99).commit();
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(c.pop(), Some(1));
        h.join().unwrap();
        assert_eq!(c.pop(), Some(2));
        assert_eq!(c.pop(), Some(99));
    }

    #[test]
    fn reserve_block_returns_none_on_consumer_close() {
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(2)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
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
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert!(c.pop_ref_block().is_none());
    }

    #[test]
    fn drain_empty_returns_zero() {
        let (_p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        assert_eq!(c.drain(|_| {}), 0);
    }

    /// A `T` whose destructor panics must still release its slot. The
    /// `SlotReader` drops the value before storing `done`, so an unwind
    /// in between leaves the slot marked unconsumed and
    /// `RingBuffer::drop` drops the same value again.
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
    fn drain_collects_all_items() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        for i in 0..5u32 {
            p.push(i).unwrap();
        }
        let mut out = Vec::new();
        let count = c.drain(|v| out.push(v));
        assert_eq!(count, 5);
        assert_eq!(out, [0, 1, 2, 3, 4]);
    }

    #[test]
    fn drain_up_to_respects_limit() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        for i in 0..5u32 {
            p.push(i).unwrap();
        }
        let mut out = Vec::new();
        let count = c.drain_up_to(3, |v| out.push(v));
        assert_eq!(count, 3);
        assert_eq!(out, [0, 1, 2]);
        // Remaining items still poppable.
        assert_eq!(c.pop(), Some(3));
        assert_eq!(c.pop(), Some(4));
    }

    #[test]
    fn drain_frees_slots_for_producer() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4u32 {
            p.push(i).unwrap();
        }
        // Buffer full — producer can't push.
        assert!(p.push(99).is_err());
        c.drain(|_| {});
        // Slots freed — producer can push again.
        p.push(99).unwrap();
        assert_eq!(c.pop(), Some(99));
    }

    #[test]
    fn drain_block_runs_until_producer_drops() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        let h = std::thread::spawn(move || {
            let mut out = Vec::new();
            let mut c = c;
            c.drain_block(|v| out.push(v));
            out
        });
        for i in 0..5u32 {
            p.push(i).unwrap();
        }
        drop(p);
        let out = h.join().unwrap();
        assert_eq!(out, [0, 1, 2, 3, 4]);
    }

    #[test]
    fn drain_multi_consumer() {
        // Two consumers each drain concurrently; combined they get all items.
        let (p, c1) = RingBuffer::<u32>::new(Capacity::exact(64)).split();
        let c2 = c1.clone();
        for i in 0..32u32 {
            p.push(i).unwrap();
        }
        let h1 = std::thread::spawn(move || {
            let mut out = Vec::new();
            c1.drain(|v| out.push(v));
            out
        });
        let h2 = std::thread::spawn(move || {
            let mut out = Vec::new();
            c2.drain(|v| out.push(v));
            out
        });
        let mut combined: Vec<u32> = h1
            .join()
            .unwrap()
            .into_iter()
            .chain(h2.join().unwrap())
            .collect();
        combined.sort_unstable();
        assert_eq!(combined, (0..32).collect::<Vec<_>>());
    }

    // -----------------------------------------------------------------------
    // Async API
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(feature = "async")]
    #[cfg_attr(miri, ignore = "too slow for Miri: threads + tokio runtimes")]
    fn async_push_pop_cross_thread() {
        // One producer + N consumers, each on its own thread with a
        // current_thread runtime + LocalSet. Watchdog aborts within 5s
        // if anything deadlocks.
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let n_consumers: u64 = 4;
        let total: u64 = 10_000;
        let received = Arc::new(AtomicU64::new(0));
        let _watchdog = crate::common::progress_watchdog::ProgressWatchdog::spawn(
            received.clone(),
            "spmc async_push_pop_cross_thread",
        );

        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

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
        // Drop the original consumer handle so the live count is exactly
        // `n_consumers`; otherwise the producer drop never closes the queue.
        drop(consumer);

        let ph = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                for i in 0..total {
                    producer.push_async(i).await.expect("all consumers dropped");
                }
                // Producer dropped here → consumers' pop_async resolve to None.
            }));
        });

        ph.join().unwrap();
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

    #[test]
    #[cfg(feature = "async")]
    fn async_push_returns_err_on_consumer_close() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(2)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
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

    /// The consumer-side twin of the mpsc producer case: two
    /// `pop_async` futures from *one* consumer handle, driven by
    /// separate tasks. Both park on the handle's single slot, so the
    /// second registration must not discard the first.
    #[test]
    #[cfg(feature = "async")]
    fn two_async_pops_from_one_handle_both_complete() {
        use std::rc::Rc;
        use std::time::Duration;

        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let h = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            let got = rt.block_on(local.run_until(async move {
                let consumer = Rc::new(consumer);
                let first = {
                    let c = Rc::clone(&consumer);
                    tokio::task::spawn_local(async move { c.pop_async().await })
                };
                let second = {
                    let c = Rc::clone(&consumer);
                    tokio::task::spawn_local(async move { c.pop_async().await })
                };
                (first.await.unwrap(), second.await.unwrap())
            }));
            let _ = done_tx.send(got);
        });

        // Both futures park on an empty ring before anything is pushed.
        std::thread::sleep(Duration::from_millis(100));
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        let (first, second) = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a pop_async future was stranded");
        let mut got = [first.unwrap(), second.unwrap()];
        got.sort_unstable();
        assert_eq!(got, [1, 2]);
        h.join().unwrap();
        drop(producer);
    }

    // -----------------------------------------------------------------------
    // Borrowed split (non-'static T)
    // -----------------------------------------------------------------------

    #[test]
    fn borrowed_split_push_pop() {
        let mut ring = RingBuffer::<u32>::new(Capacity::exact(4));
        let (producer, consumer) = ring.split_borrowed();

        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn borrowed_split_non_static_lifetime() {
        let wire_buf = [10u8, 20, 30, 40];
        let mut ring = RingBuffer::<&[u8]>::new(Capacity::exact(4));
        let (producer, consumer) = ring.split_borrowed();

        producer.push(&wire_buf[0..2]).unwrap();
        producer.push(&wire_buf[2..4]).unwrap();

        assert_eq!(consumer.pop().unwrap(), &[10, 20]);
        assert_eq!(consumer.pop().unwrap(), &[30, 40]);
    }

    /// A borrowed split hands out one consumer, so dropping it must
    /// reach zero and close the ring. The count was seeded at one and
    /// incremented again by the split, leaving a phantom consumer that
    /// kept a blocked producer waiting forever.
    #[test]
    fn borrowed_split_last_consumer_drop_closes_the_ring() {
        let mut ring = RingBuffer::<u32>::new(Capacity::exact(2));
        let (producer, consumer) = ring.split_borrowed();

        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert!(producer.is_full());
        drop(consumer);

        assert_eq!(
            producer.push_block(3),
            Err(3),
            "a full ring with no consumers must not block the producer"
        );
    }

    /// The same once extra handles have been minted: closure follows
    /// the last of them, not the first.
    #[test]
    fn borrowed_consumers_close_the_ring_on_the_last_drop() {
        let mut ring = RingBuffer::<u32>::new(Capacity::exact(2));
        let (producer, c1) = ring.split_borrowed();
        let c2 = c1.new_consumer();

        producer.push(1).unwrap();
        producer.push(2).unwrap();
        drop(c1);
        assert!(!c2.is_closed(), "one consumer still holds the ring open");

        assert_eq!(c2.pop(), Some(1));
        drop(c2);
        assert_eq!(producer.push_block(3), Err(3));
    }

    #[test]
    fn borrowed_split_multiple_consumers() {
        let mut ring = RingBuffer::<u32>::new(Capacity::exact(8));
        let (producer, c1) = ring.split_borrowed();
        let c2 = c1.new_consumer();

        std::thread::scope(|s| {
            s.spawn(move || {
                for i in 0..8u32 {
                    while producer.push(i).is_err() {
                        std::thread::yield_now();
                    }
                }
            });
            let got1 = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let got2 = got1.clone();
            s.spawn(move || {
                loop {
                    if let Some(v) = c1.pop() {
                        got1.lock().unwrap().push(v);
                    } else if c1.is_closed() {
                        // Final drain
                        while let Some(v) = c1.pop() {
                            got1.lock().unwrap().push(v);
                        }
                        break;
                    } else {
                        std::thread::yield_now();
                    }
                }
            });
            s.spawn(move || loop {
                if let Some(v) = c2.pop() {
                    got2.lock().unwrap().push(v);
                } else if c2.is_closed() {
                    while let Some(v) = c2.pop() {
                        got2.lock().unwrap().push(v);
                    }
                    break;
                } else {
                    std::thread::yield_now();
                }
            });
        });
    }

    #[test]
    fn borrowed_split_struct_with_lifetime() {
        #[derive(Debug, PartialEq)]
        struct Req<'a> {
            payload: &'a [u8],
            kind: u8,
        }

        let wire = vec![0xCC; 128];
        let mut ring = RingBuffer::<Req<'_>>::new(Capacity::exact(4));
        let (producer, consumer) = ring.split_borrowed();
        let wire_ref = &wire;

        std::thread::scope(|s| {
            s.spawn(move || {
                producer
                    .push(Req {
                        payload: &wire_ref[..64],
                        kind: 1,
                    })
                    .unwrap();
                producer
                    .push(Req {
                        payload: &wire_ref[64..],
                        kind: 2,
                    })
                    .unwrap();
            });
            s.spawn(move || {
                let mut got = Vec::new();
                while got.len() < 2 {
                    if let Some(req) = consumer.pop() {
                        got.push(req.kind);
                    } else {
                        std::thread::yield_now();
                    }
                }
                assert_eq!(got, vec![1, 2]);
            });
        });
    }
}
