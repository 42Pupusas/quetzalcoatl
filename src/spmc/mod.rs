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
//!   item-arrival order. With `BATCH_SIZE = 32`, one consumer can grab 32
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
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// A lock-free SPMC ring buffer.
///
/// Created via [`RingBuffer::new`], then [`split`](RingBuffer::split) into
/// a [`Producer`] / [`Consumer`] pair. The `Consumer` is [`Clone`]; the
/// `Producer` is not (single-producer).
///
/// # Layout
///
/// Data and per-slot readiness markers live in **separate arrays**. Data
/// is packed (one element per slot, no padding); the readiness array is
/// cache-line padded so a consumer's release-store on slot `s` does not
/// invalidate the producer's lines for adjacent slots.
///
/// # Synchronization (split-plane protocol)
///
/// Two per-slot atomic arrays, on separate cache lines:
///
/// * `ready[s]` — **producer-write, consumer-read**. Producer Release-stores
///   `ready[s] = p + 1` to publish data at logical position `p`; consumer
///   Acquire-loads to see when data is available.
///
/// * `done[s]` — **consumer-write, producer-read**. Consumer Release-stores
///   `done[s] = h + cap` after reading data, telling the producer the slot
///   is free for reuse at logical position `h + cap` (the next lap).
///   Initial: `done[s] = s` so the first lap is free.
///
/// Crucially, the producer's hot path never writes the `done` line and the
/// consumer's release-store never touches the `ready` line. The two
/// producer↔consumer handoff streams ping-pong on **different** physical
/// cache lines, eliminating the symmetric ping-pong of the previous design
/// where both directions wrote `ready[s]`.
///
/// The producer never reads `head`; consumers never read `tail` except
/// to bound their batched CAS claim (see `Consumer::claim_batch`) and for
/// `len()` diagnostics.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) data: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    /// Producer-write, consumer-read publish marker. Each on its own
    /// cache line so a producer write to ready[s] does not invalidate
    /// adjacent slots.
    pub(crate) ready: AlignedBuf<CachePadded<AtomicUsize>>,
    /// Consumer-write, producer-read free-for-reuse marker. Distinct
    /// cache lines from `ready` to break the producer↔consumer ping-pong.
    pub(crate) done: AlignedBuf<CachePadded<AtomicUsize>>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    pub(crate) head: CachePadded<AtomicUsize>,
    /// Producer cursor, published Release. Consumers Acquire-load it to
    /// bound their batched CAS claim, ensuring they never claim a
    /// position the producer has not yet published.
    pub(crate) tail: CachePadded<AtomicUsize>,
    /// Set by `Producer::Drop` so that consumers can distinguish
    /// "transiently empty" from "permanently drained" via `is_closed()`.
    pub(crate) closed: CachePadded<AtomicBool>,
}

// Safety: Single producer writes to slot data, gated by per-slot `ready`
// markers. Multiple consumers CAS on `head` to claim slots and synchronize
// with the producer via Acquire/Release on `ready`.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new SPMC ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let data = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        // ready[s] = 0 — producer doesn't read this for freeness, only
        // writes it for publish. Consumers Acquire-load and compare
        // against (h + 1), so the initial 0 just means "not yet
        // published"; consumers won't reach it until the producer has
        // pushed at least s+1 items (head can't FAA past tail==0).
        let ready = AlignedBuf::new_with(cap, || CachePadded(AtomicUsize::new(0)));
        // done[s] = s — slot s is "free for producer at logical position
        // s" on the first lap. After a consumer at position p reads slot
        // s, it stores done[s] = p + cap, marking it free for the next
        // lap (logical position p + cap == s + cap == s + cap).
        let mut idx = 0usize;
        let done = AlignedBuf::new_with(cap, || {
            let d = CachePadded(AtomicUsize::new(idx));
            idx += 1;
            d
        });

        Self {
            data,
            ready,
            done,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            closed: CachePadded(AtomicBool::new(false)),
            cap,
            mask: capacity.mask,
        }
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
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() == self.cap
    }

    /// Splits the ring buffer into a [`Producer`] and [`Consumer`] pair.
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
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        // With FAA-based consumers, `head` can transiently exceed
        // `tail` if consumers overshot (abandoned slots). The actual
        // live-data range is positions `pos in head..tail` whose slot
        // has `ready[s] == pos + 1` (i.e. producer published, consumer
        // hasn't claimed). Slots with any other `ready` value are
        // either consumed (`pos + cap`) or not yet published.
        if head >= tail {
            return;
        }
        for pos in head..tail {
            let s = pos & self.mask;
            let r = *self.ready[s].0.get_mut();
            let d = *self.done[s].0.get_mut();
            // Slot at logical position `pos` holds live data iff the
            // producer published (ready == pos+1) AND no consumer has
            // released it yet (done < pos+cap).
            if r == pos + 1 && d != pos + self.cap {
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
                .unwrap_or_else(|_| panic!("Failed to push {i}",));
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
}
