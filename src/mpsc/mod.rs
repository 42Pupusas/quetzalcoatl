//! Multi-producer, single-consumer (MPSC) lock-free ring buffer.
//!
//! Multiple producers push concurrently via atomic fetch-and-add (FAA);
//! a single consumer pops items in FIFO order. The producer handle is
//! [`Clone`], so new producers can be created at any time.
//!
//! FAA eliminates inter-producer contention on the tail pointer — each
//! producer claims a unique position on the first try, then spins on its
//! own slot's sequence number (a separate cache line) until the slot is
//! free. This scales significantly better than CAS under high producer
//! counts (8+).
//!
//! # Example
//!
//! ```
//! use quetzalcoatl::mpsc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = RingBuffer::new(Capacity::exact(16)).split();
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

/// A lock-free MPSC ring buffer.
///
/// Created via [`RingBuffer::new`], then [`split`](RingBuffer::split) into
/// a [`Producer`] / [`Consumer`] pair. The `Producer` is [`Clone`]; the
/// `Consumer` is not (single-consumer).
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

// Safety: Multiple producers use FAA to atomically claim tail slots.
// Single consumer touches head. Sequence numbers ensure proper synchronization.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new MPSC ring buffer with the given capacity.
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

    /// Returns a reference to the slot at logical position `pos`.
    ///
    /// Single point of unsafety: `pos & mask` is always `< cap == buf.len()`
    /// because `mask = cap - 1` and `cap` is a power of two.
    #[inline]
    pub(crate) fn slot(&self, pos: usize) -> &SeqSlot<T> {
        let idx = pos & self.mask;
        // SAFETY: mask = cap - 1, cap = buf.len(), so idx < buf.len().
        unsafe { std::hint::assert_unchecked(idx < self.buf.len()) };
        &self.buf[idx]
    }

    /// Splits the ring buffer into a [`Producer`] and [`Consumer`] pair.
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self);
        let producer = Producer {
            queue: arc.clone(),
            cached_head: std::cell::Cell::new(0),
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
            let slot = &mut self.buf[pos & self.mask];
            let seq = *slot.sequence.get_mut();
            // Skip tombstoned slots (abandoned reservations) and slots
            // that were claimed but never had their sequence published.
            if seq == pos * 2 + 1 {
                // SAFETY: sequence == pos * 2 + 1 means this slot was
                // published with valid data. Exclusive access in drop
                // (&mut self) guarantees no concurrency.
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
    // MPSC-specific tests
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "too slow for Miri"]
    fn multiple_producers_concurrent() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();

        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    for i in 0..100 {
                        p.push(thread_id * 100 + i).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let mut received = Vec::new();
        while let Some(v) = consumer.pop() {
            received.push(v);
        }

        assert_eq!(received.len(), 1000);
        // Verify all values are present (order may vary due to concurrency)
        received.sort_unstable();
        let expected: Vec<u64> = (0..1000).collect();
        assert_eq!(received, expected);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn dynamic_producer_creation() {
        let (producer, mut consumer) = RingBuffer::<usize>::new(Capacity::exact(128)).split();

        // Simulate dynamic producer creation (e.g., new connections)
        let mut handles = Vec::new();
        for i in 0..5 {
            let p = producer.clone();
            let handle = std::thread::spawn(move || {
                p.push(i).unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut received = Vec::new();
        while let Some(v) = consumer.pop() {
            received.push(v);
        }

        assert_eq!(received.len(), 5);
        received.sort_unstable();
        assert_eq!(received, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn mpsc_stress_test() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::at_least(10000)).split();

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    for i in 0..1000 {
                        while p.push(thread_id * 1000 + i).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let mut received = Vec::new();
        while let Some(v) = consumer.pop() {
            received.push(v);
        }

        assert_eq!(received.len(), 4000);
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
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
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

    /// Two producers, tiny buffer — checks FAA + sequence synchronization.
    #[test]
    fn concurrent_mpsc_data_race_check() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 2u64;

        let p2 = producer.clone();
        let h1 = std::thread::spawn(move || {
            for i in 0..n {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });
        let h2 = std::thread::spawn(move || {
            for i in 0..n {
                while p2.push(100 + i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        let mut received = 0u64;
        while received < n * 2 {
            if consumer.pop().is_some() {
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }

        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(received, n * 2);
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
    fn pop_ref_returns_none_when_not_ready() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

        // Reserve but don't commit — slot is claimed but not ready
        let writer = producer.reserve().unwrap();
        assert!(consumer.pop_ref().is_none());

        // Now commit and it should be readable
        writer.write(99).commit();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 99);
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
    fn reserve_drop_without_commit_tombstones() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        // Push a value, then reserve-and-drop (tombstone), then push again
        producer.push(1).unwrap();
        {
            let w = producer.reserve().unwrap();
            w.write(2);
            // drop without commit — should tombstone, not panic
        }
        producer.push(3).unwrap();

        // Consumer should see 1, skip tombstone, see 3
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn reserve_drop_without_write_tombstones() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        producer.push(10).unwrap();
        {
            let _w = producer.reserve().unwrap();
            // drop without write or commit
        }
        producer.push(20).unwrap();

        assert_eq!(consumer.pop(), Some(10));
        assert_eq!(consumer.pop(), Some(20));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn reserve_drop_does_not_leak() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (mut producer, _consumer) =
            RingBuffer::<crate::common::DropCounter>::new(Capacity::exact(4)).split();

        {
            let w = producer.reserve().unwrap();
            w.write(crate::common::DropCounter {
                counter: counter.clone(),
            });
            // drop without commit — value should be dropped
        }
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "written value should be dropped on SlotWriter drop"
        );
    }

    // -----------------------------------------------------------------------
    // Drain API tests
    // -----------------------------------------------------------------------

    #[test]
    fn drain_empty_returns_zero() {
        let (_producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        assert_eq!(consumer.drain(|_| unreachable!()), 0);
    }

    #[test]
    fn drain_all_available() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();

        for i in 0..5 {
            producer.push(i).unwrap();
        }

        let mut items = Vec::new();
        let count = consumer.drain(|v| items.push(v));
        assert_eq!(count, 5);
        assert_eq!(items, vec![0, 1, 2, 3, 4]);
        assert!(consumer.is_empty());
    }

    #[test]
    fn drain_up_to_limits() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();

        for i in 0..5 {
            producer.push(i).unwrap();
        }

        let mut items = Vec::new();
        let count = consumer.drain_up_to(3, |v| items.push(v));
        assert_eq!(count, 3);
        assert_eq!(items, vec![0, 1, 2]);

        // Remaining 2 items still available
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn drain_drops_values() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) = RingBuffer::new(Capacity::exact(4)).split();

        for _ in 0..3 {
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
        }

        let count = consumer.drain(drop);
        assert_eq!(count, 3);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[test]
    fn drain_skips_tombstones() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        producer.push(1).unwrap();
        {
            let _w = producer.reserve().unwrap();
            // dropped without commit → tombstone
        }
        producer.push(3).unwrap();

        let mut items = Vec::new();
        let count = consumer.drain(|v| items.push(v));
        assert_eq!(count, 2);
        assert_eq!(items, vec![1, 3]);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn concurrent_drain() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();
        let total = 100_000u64;
        let num_producers = 4u64;
        let per_producer = total / num_producers;

        let handles: Vec<_> = (0..num_producers)
            .map(|_| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    for i in 0..per_producer {
                        while p.push(i).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        let mut received = 0u64;
        while received < total {
            let drained = consumer.drain(|_| {});
            if drained > 0 {
                received += drained as u64;
            } else {
                std::thread::yield_now();
            }
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received, total);
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
