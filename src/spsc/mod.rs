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
//! use quetzalcoatl::spsc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = RingBuffer::new(Capacity::exact(16)).split();
//!
//! producer.push(42u64).unwrap();
//! producer.push(43).unwrap();
//!
//! assert_eq!(consumer.pop(), Some(42));
//! assert_eq!(consumer.pop(), Some(43));
//! assert_eq!(consumer.pop(), None);
//! ```

mod producer;
mod consumer;

pub use producer::{Producer, SlotWriter};
pub use consumer::{Consumer, SlotReader};

use crate::capacity::Capacity;
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A lock-free SPSC ring buffer.
///
/// Created via [`RingBuffer::new`], then [`split`](RingBuffer::split) into
/// a [`Producer`] / [`Consumer`] pair. Neither handle is [`Clone`],
/// enforcing the single-producer, single-consumer invariant.
pub struct RingBuffer<T> {
    pub(crate) buf: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    pub(crate) head: CachePadded<AtomicUsize>,
    pub(crate) tail: CachePadded<AtomicUsize>,
}

// Safety: Single producer writes via tail, single consumer reads via head.
// Atomic head/tail with Acquire/Release ordering provide synchronization.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new SPSC ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let buf = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));

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
    /// transiently exceed `capacity` when observed from a thread that is
    /// neither the producer nor the consumer. Use this for heuristics
    /// (e.g., "is there work?"), not for precise invariants.
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
        let producer = Producer::new(arc.clone());
        let consumer = Consumer {
            queue: arc,
            cached_tail: std::cell::Cell::new(0),
        };
        (producer, consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::Capacity;

    #[test]
    fn capacity_one() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(1)).split();
        assert_eq!(producer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn zero_sized_types() {
        let (producer, mut consumer) = RingBuffer::<()>::new(Capacity::exact(4)).split();

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
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(1)).split();
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
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(2)).split();
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
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(4)).split();
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
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(4)).split();
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
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(16)).split();

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

    #[test]
    fn pop_empty_buffer() {
        let (_, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(16)).split();
        assert_eq!(consumer.pop(), None);
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn push_to_buffer() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(16)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    // -----------------------------------------------------------------------
    // Miri-targeted tests: small sizes exercising all unsafe code paths
    // -----------------------------------------------------------------------

    use crate::common::DropCounter;

    /// Items still in the buffer when Consumer is dropped must be dropped.
    #[test]
    fn drop_items_on_consumer_drop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, consumer) = RingBuffer::new(Capacity::exact(4)).split();

        for _ in 0..4 {
            producer.push(DropCounter { counter: counter.clone() }).unwrap();
        }

        // Dropping consumer should drain and drop all 4 items
        drop(consumer);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    /// Items popped normally should be dropped exactly once.
    #[test]
    fn drop_items_on_pop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) = RingBuffer::new(Capacity::exact(4)).split();

        for _ in 0..3 {
            producer.push(DropCounter { counter: counter.clone() }).unwrap();
        }

        // Pop 2 — should drop when they go out of scope
        let a = consumer.pop().unwrap();
        let b = consumer.pop().unwrap();
        drop(a);
        drop(b);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);

        // Remaining 1 dropped when consumer is dropped
        drop(consumer);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    /// Exercise `get_unchecked` on every index by wrapping around multiple times.
    #[test]
    fn wraparound_exercises_all_slots() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

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
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
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

    // -----------------------------------------------------------------------
    // Zero-copy API tests
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_write_commit_pop_ref_cycle() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

        let mut writer = producer.reserve().unwrap();
        writer.write(42);
        writer.commit();

        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 42);
        drop(reader);

        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn reserve_slot_mut_commit() {
        let (producer, mut consumer) = RingBuffer::<[u8; 64]>::new(Capacity::exact(4)).split();

        let mut writer = producer.reserve().unwrap();
        writer.slot_mut().write([0xAB; 64]);
        writer.commit();

        let reader = consumer.pop_ref().unwrap();
        assert_eq!(reader[0], 0xAB);
        assert_eq!(reader[63], 0xAB);
    }

    #[test]
    fn reserve_returns_none_when_full() {
        let (producer, _consumer) = RingBuffer::<u64>::new(Capacity::exact(2)).split();

        // Commit each writer immediately to avoid panicking on drop if
        // a later assertion fails.
        let mut w1 = producer.reserve().unwrap();
        w1.write(1);
        w1.commit();

        let mut w2 = producer.reserve().unwrap();
        w2.write(2);
        w2.commit();

        assert!(producer.reserve().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_empty() {
        let (_producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_not_ready() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

        // Reserve but don't commit — slot is claimed but not visible
        let mut writer = producer.reserve().unwrap();
        assert!(consumer.pop_ref().is_none());

        // Now commit and it should be readable
        writer.write(99);
        writer.commit();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 99);
    }

    #[test]
    fn mixed_push_reserve_pop_pop_ref() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();

        // Mix of push and reserve
        producer.push(1).unwrap();
        let mut w = producer.reserve().unwrap();
        w.write(2);
        w.commit();
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
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        // 3 full laps via reserve/pop_ref
        for lap in 0..3u32 {
            for i in 0..4 {
                let mut w = producer.reserve().unwrap();
                w.write(lap * 4 + i);
                w.commit();
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
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 16u64;

        let handle = std::thread::spawn(move || {
            for i in 0..n {
                loop {
                    if let Some(mut w) = producer.reserve() {
                        w.write(i);
                        w.commit();
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
