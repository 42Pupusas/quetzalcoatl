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
use crate::common::{AlignedBuf, CachePadded, SeqSlot};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A lock-free SPMC ring buffer.
///
/// Created via [`RingBuffer::new`], then [`split`](RingBuffer::split) into
/// a [`Producer`] / [`Consumer`] pair. The `Consumer` is [`Clone`]; the
/// `Producer` is not (single-producer).
// repr(C) locks field order: shared immutable fields first (same cache
// line), then head and tail each on their own cache-padded line.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) buf: AlignedBuf<SeqSlot<T>>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    pub(crate) head: CachePadded<AtomicUsize>,
    pub(crate) tail: CachePadded<AtomicUsize>,
}

// Safety: Single producer advances tail and writes to slots guarded by
// sequence numbers. Multiple consumers use CAS on head to claim slots.
// Sequence numbers ensure proper synchronization between producer and
// consumers.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new SPMC ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let mut idx = 0usize;
        let buf = AlignedBuf::new_with(cap, || {
            let slot = SeqSlot {
                data: UnsafeCell::new(MaybeUninit::uninit()),
                sequence: AtomicUsize::new(idx * 2),
            };
            idx += 1;
            slot
        });

        Self {
            buf,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            cap,
            mask: capacity.mask,
        }
    }

    /// Returns the approximate number of items currently in the buffer.
    ///
    /// Both cursors are loaded with `Relaxed` ordering, so the result may
    /// transiently exceed `capacity` when observed from another thread.
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
        let consumer = Consumer { queue: arc };
        (producer, consumer)
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        for pos in head..tail {
            let slot = &self.buf[pos & self.mask];
            // SAFETY: slots between head and tail contain initialized data.
            unsafe {
                slot.data.get().cast::<T>().drop_in_place();
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
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.len(), 1);
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
