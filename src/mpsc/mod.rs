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
use crate::common::park::WakeSet;
use crate::common::{AlignedBuf, CachePadded, SeqSlot};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::Thread;

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
    /// Live producer count; last-drop sets [`closed`].
    pub(crate) producer_count: CachePadded<AtomicUsize>,
    /// Set by the last [`Producer`] drop. [`Consumer::pop_block`]
    /// observes this and returns `None` once the queue drains.
    pub(crate) closed: CachePadded<AtomicBool>,
    /// Set when the [`Consumer`] is dropped. [`Producer::push_block`]
    /// observes this and returns `Err(val)` instead of hanging.
    pub(crate) consumer_closed: CachePadded<AtomicBool>,
    /// Producer-side park state. Bit `i` of `producer_park.wake` is
    /// set ↔ a producer in slot `i` is parked waiting for free
    /// space. The consumer wakes one parked producer after each pop
    /// (gated on the bitmap being non-zero).
    pub(crate) producer_park: WakeSet,
    /// Monotonic counter for assigning stable park-slot indices to
    /// `Producer` clones. Bumped at clone time only.
    pub(crate) producer_park_idx: CachePadded<AtomicUsize>,
    /// Single-consumer park handle. Idempotently set by the consumer
    /// the first time it parks; producers `OnceLock::get` it to
    /// unpark.
    pub(crate) consumer_parker: OnceLock<Thread>,
    /// `true` ↔ the consumer is currently parked on `consumer_parker`.
    /// Producers gate their unpark on this `Relaxed` load to keep the
    /// no-park hot path free.
    pub(crate) consumer_parked: CachePadded<AtomicBool>,
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
            producer_count: CachePadded(AtomicUsize::new(1)),
            closed: CachePadded(AtomicBool::new(false)),
            consumer_closed: CachePadded(AtomicBool::new(false)),
            producer_park: WakeSet::new(),
            producer_park_idx: CachePadded(AtomicUsize::new(0)),
            consumer_parker: OnceLock::new(),
            consumer_parked: CachePadded(AtomicBool::new(false)),
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
            park_slot: 0,
        };
        let consumer = Consumer { queue: arc };
        (producer, consumer)
    }
}

impl<T> RingBuffer<T> {
    /// Wakes the single parked consumer, if any. Gated internally on
    /// `consumer_parked.load(Relaxed) != 0` for the no-cost fast
    /// path.
    ///
    /// Idempotent: clearing the flag means any racing producer that
    /// observes it already cleared simply skips the unpark. The
    /// post-clear unpark is still issued by the winner — a stale
    /// `unpark()` after the consumer rechecks is benign (it makes
    /// the next park return immediately, which is fine because the
    /// consumer re-checks the queue after every wake).
    #[inline]
    pub(crate) fn wake_consumer(&self) {
        if !self.consumer_parked.0.load(Ordering::Relaxed) {
            return;
        }
        // Clear the flag first so concurrent producers all skip past it.
        self.consumer_parked.0.store(false, Ordering::Relaxed);
        if let Some(handle) = self.consumer_parker.get() {
            handle.unpark();
        }
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

    // -----------------------------------------------------------------------
    // Audit gap scenarios (Miri-friendly)
    // -----------------------------------------------------------------------

    /// `RingBuffer::Drop` walks `head..tail` and drops only slots whose
    /// sequence is the published value. Verifies that interleaving real
    /// pushes with abandoned reservations doesn't double-drop or skip.
    #[test]
    fn drop_skips_tombstoned_slots() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (mut producer, _consumer) =
                RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();

            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();

            // Reserve + drop without commit → tombstone, no value
            {
                let _w = producer.reserve().unwrap();
            }

            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();

            // Reserve + write but no commit → value dropped now,
            // slot tombstoned
            {
                let w = producer.reserve().unwrap();
                w.write(DropCounter {
                    counter: counter.clone(),
                });
            }
            assert_eq!(
                counter.load(std::sync::atomic::Ordering::Relaxed),
                1,
                "WrittenSlot::Drop runs immediately"
            );
        }
        // Consumer's Drop drains via pop, walking past the tombstones
        // and dropping 2 published values.
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "1 in-flight + 2 published = 3 drops"
        );
    }

    /// Concurrent multi-producer + drain consumer at small N (Miri-tractable).
    #[test]
    fn concurrent_drain_small_n() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();
        let n_per = 4u64;
        let n_producers = 2u64;

        let handles: Vec<_> = (0..n_producers)
            .map(|tid| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    for i in 0..n_per {
                        while p.push(tid * n_per + i).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        let mut received = 0u64;
        let total = n_per * n_producers;
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

    /// Capacity-1 with concurrent producer/consumer.
    #[test]
    fn capacity_one_concurrent() {
        let n = 8u64;
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(1)).split();

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

    // -----------------------------------------------------------------------
    // Blocking-API tests (push_block / pop_block).
    // -----------------------------------------------------------------------

    #[test]
    fn pop_block_wakes_on_push() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        p.push(7).unwrap();
        assert_eq!(h.join().unwrap(), Some(7));
    }

    #[test]
    fn pop_block_returns_none_on_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(p);
        assert_eq!(h.join().unwrap(), None);
    }

    #[test]
    fn push_block_unblocks_on_pop() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
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
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        drop(p);
        let mut got = vec![c.pop_block().unwrap(), c.pop_block().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        assert_eq!(c.pop_block(), None);
    }

    /// Regression: `drain` and `drain_up_to` previously called
    /// `wake_one` after a batch, which only released ONE parked
    /// producer even when the drain freed many slots. With many
    /// producers waiting on `push_block`, all but one would stall
    /// until the next pop or push fired another wake. Fix:
    /// `wake_n(count)`.
    ///
    /// Test design: pre-fill the ring so all `N` producers park on
    /// their first `push_block`. Drain ONCE (releasing every slot
    /// in one batch), then DO NOT call drain or pop again — count
    /// how many producers complete their single push within a
    /// deadline.
    ///
    /// With the fix (`wake_n(N)`): all N parked producers wake,
    /// each pushes once into the now-empty ring (N <= CAP so they
    /// all fit), all join.
    ///
    /// With the bug (`wake_one`): only one producer wakes. It
    /// pushes (ring has CAP-1 free). No further wake fires
    /// because the consumer is silent. The other N-1 producers
    /// stay parked → test panics on the deadline. Verified by
    /// temporarily flipping `wake_n(count)` back to `wake_one()`
    /// during development; this test reliably hangs.
    #[test]
    fn drain_wakes_all_parked_producers() {
        const CAP: u32 = 4;
        // N == CAP so the post-drain pushes all fit exactly.
        const N_PRODUCERS: u32 = 4;

        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(CAP as usize)).split();
        // Fill the ring so the next push from any producer must park.
        for i in 0..CAP {
            p.push(i).unwrap();
        }

        // Each producer pushes exactly once. Their single push will
        // initially fail (ring full) → push_block parks. We rely on
        // drain's wake fan-out to release them.
        let producers: Vec<_> = (0..N_PRODUCERS)
            .map(|tid| {
                let p = p.clone();
                std::thread::spawn(move || {
                    p.push_block(1000 + tid).expect("consumer dropped");
                })
            })
            .collect();
        drop(p);

        // Give producers time to attempt the push and park. 50ms is
        // plenty for them to escalate cas_backoff and reach park.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Single drain — frees CAP=4 slots. With wake_n(4) all 4
        // parked producers wake; with the buggy wake_one(1) only
        // one does and the test fails on the deadline below.
        let drained = c.drain(|_| {});
        assert_eq!(drained, CAP as usize);

        // Wait for all producers to complete WITHOUT calling drain
        // or pop again — those would each emit additional wakes and
        // mask the bug.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        for (i, h) in producers.into_iter().enumerate() {
            while !h.is_finished() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "drain woke fewer than {N_PRODUCERS} parked producers — \
                     producer {i} (and possibly later ones) still parked at deadline"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            h.join().unwrap();
        }
    }

    #[test]
    fn drain_block_drains_to_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        let p2 = p.clone();
        let h = std::thread::spawn(move || {
            let mut got = Vec::new();
            c.drain_block(|v| got.push(v));
            got
        });
        for i in 0..3u32 {
            p.push(i).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        for i in 100..103u32 {
            p2.push(i).unwrap();
        }
        drop(p);
        drop(p2);
        let mut got = h.join().unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 100, 101, 102]);
    }

    #[test]
    fn push_block_two_producers_serialize_through_capacity_one() {
        // Two producers contending for a single slot. Each takes its
        // park slot; the consumer drains slowly. All values must
        // eventually arrive.
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(1)).split();
        let p2 = p.clone();
        let h1 = std::thread::spawn(move || {
            for i in 0..16u32 {
                p.push_block(i).unwrap();
            }
        });
        let h2 = std::thread::spawn(move || {
            for i in 0..16u32 {
                p2.push_block(100 + i).unwrap();
            }
        });
        let mut got = Vec::new();
        while got.len() < 32 {
            if let Some(v) = c.pop_block() {
                got.push(v);
            } else {
                break;
            }
        }
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(got.len(), 32);
        got.sort_unstable();
        let expected: Vec<u32> = (0..16u32).chain(100..116u32).collect();
        assert_eq!(got, expected);
    }

    // -----------------------------------------------------------------------
    // Zero-copy blocking API
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_block_unblocks_on_pop() {
        let (mut p, mut c) = RingBuffer::<u32>::new(Capacity::exact(2)).split();
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
        let mut got = vec![v1, v2];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        assert!(c.pop_ref_block().is_none());
    }
}
