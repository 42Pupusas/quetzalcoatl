//! Multi-producer, multi-consumer (MPMC) broadcast ring buffer.
//!
//! Every consumer sees every item published after it subscribes. Multiple
//! producers push via CAS; consumers are dynamically created by cloning
//! an existing [`Consumer`].
//!
//! Items require `T: Clone` for [`Consumer::pop`], or use
//! [`Consumer::pop_ref`] for zero-copy reads. For large types with many
//! consumers, see the [`arc`] sub-module which wraps values in `Arc<T>`.
//!
//! # Example
//!
//! ```
//! use quetzalcoatl::broadcast::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut c1) = RingBuffer::new(Capacity::exact(16), 4).split();
//! let mut c2 = c1.clone();
//!
//! producer.push(10u32).unwrap();
//! producer.push(20).unwrap();
//!
//! assert_eq!(c1.pop(), Some(10));
//! assert_eq!(c1.pop(), Some(20));
//! assert_eq!(c2.pop(), Some(10));
//! assert_eq!(c2.pop(), Some(20));
//! ```

mod producer;
mod consumer;
pub mod arc;

pub use producer::{Producer, SlotWriter};
pub use consumer::{Consumer, SlotReader};

use crate::capacity::Capacity;
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

pub(super) struct BroadcastSlot<T> {
    pub data: UnsafeCell<MaybeUninit<T>>,
    /// Sequence number: 0 = empty, `pos * 2 + 1` = published at position `pos`.
    /// Uses the same 2x encoding as MPSC/SPMC for consistency.
    pub sequence: AtomicUsize,
}

/// Per-consumer slot in the fixed-size consumer registry.
pub(super) struct ConsumerSlot {
    pub head: CachePadded<AtomicUsize>,
    pub active: AtomicBool,
}

/// Lock-free MPMC broadcast ring buffer.
///
/// Every consumer sees every item published after it subscribes.
/// Multiple producers push via CAS. Consumers clone to subscribe.
// repr(C) locks field order: shared immutable fields first (same cache
// line), then the contended tail on its own cache-padded line.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) buf: AlignedBuf<BroadcastSlot<T>>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    pub(crate) consumer_slots: Box<[ConsumerSlot]>,
    pub(crate) tail: CachePadded<AtomicUsize>,
    /// Shared L2 cache of `min_head()`. Updated by any producer after a full
    /// scan; read by all producers to avoid redundant O(N) scans.
    /// Always ≤ actual min_head (conservative), so a stale value is safe.
    pub(crate) min_head_cache: CachePadded<AtomicUsize>,
}

// SAFETY: RingBuffer is shared via Arc between producers and consumers on
// different threads. T: Send because values cross thread boundaries.
// T: Sync because multiple consumers hold &T to the same slot concurrently.
unsafe impl<T: Send + Sync> Send for RingBuffer<T> {}
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new broadcast ring buffer.
    ///
    /// `max_consumers` sets the maximum number of concurrent consumers.
    /// The first consumer is created by [`split`](Self::split). Additional consumers
    /// are created by cloning an existing consumer.
    ///
    /// # Panics
    ///
    /// Panics if `max_consumers` is 0.
    #[must_use]
    pub fn new(capacity: Capacity, max_consumers: usize) -> Self {
        assert!(max_consumers > 0, "max_consumers must be > 0");
        let cap = capacity.cap;
        let buf = AlignedBuf::new_with(cap, || BroadcastSlot {
            data: UnsafeCell::new(MaybeUninit::uninit()),
            sequence: AtomicUsize::new(0),
        });
        let consumer_slots: Vec<ConsumerSlot> = (0..max_consumers)
            .map(|_| ConsumerSlot {
                head: CachePadded(AtomicUsize::new(0)),
                active: AtomicBool::new(false),
            })
            .collect();
        Self {
            buf,
            cap,
            mask: capacity.mask,
            tail: CachePadded(AtomicUsize::new(0)),
            min_head_cache: CachePadded(AtomicUsize::new(0)),
            consumer_slots: consumer_slots.into_boxed_slice(),
        }
    }

    /// Splits the ring buffer into a [`Producer`] and the first [`Consumer`].
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self);

        // Claim the first consumer slot.
        arc.consumer_slots[0].active.store(true, Ordering::Relaxed);
        arc.consumer_slots[0].head.store(0, Ordering::Relaxed);

        let producer = Producer {
            queue: Arc::clone(&arc),
            cached_min_head: std::cell::Cell::new(0),
        };
        let consumer = Consumer {
            queue: arc,
            slot_index: 0,
        };
        (producer, consumer)
    }

    /// Scans all active consumer heads and returns the minimum.
    ///
    /// Returns `tail` if no consumers are active (black hole mode —
    /// the producer can always write).
    pub(crate) fn min_head(&self) -> usize {
        let mut min = usize::MAX;
        for slot in &*self.consumer_slots {
            if slot.active.load(Ordering::Relaxed) {
                let head = slot.head.load(Ordering::Acquire);
                if head < min {
                    min = head;
                }
            }
        }
        if min == usize::MAX {
            self.tail.load(Ordering::Relaxed)
        } else {
            min
        }
    }

    /// Returns the number of items between the slowest consumer and the tail.
    #[must_use]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let min_head = self.min_head();
        tail.wrapping_sub(min_head)
    }

    /// Returns `true` if no items are pending for any consumer.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the slowest consumer's backlog has reached capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() >= self.cap
    }

    /// Claims an inactive consumer slot. Returns the slot index.
    ///
    /// Panics if all consumer slots are taken.
    pub(crate) fn claim_consumer_slot(&self) -> usize {
        for (i, slot) in self.consumer_slots.iter().enumerate() {
            if slot
                .active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return i;
            }
        }
        panic!(
            "broadcast: max consumer limit ({}) exceeded",
            self.consumer_slots.len()
        );
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Determine the range of slots that may contain initialized data.
        // `tail` is the total number of items ever pushed. A slot at
        // position `pos` was last written by push number `pos`, so the
        // most recent `min(tail, cap)` slots may hold live data.
        //
        // We use the per-slot sequence number to decide: seq > 0 means
        // the producer wrote data and hasn't cleared it. seq == 0 means
        // either never written or already cleared by a subsequent claim_slot.
        //
        // The start of the range is `tail - min(tail, cap)` to avoid
        // scanning the entire buffer, and the wrapping_sub handles the
        // (astronomically unlikely) usize wraparound case.
        let tail = *self.tail.0.get_mut();
        let start = tail.wrapping_sub(tail.min(self.cap));
        for pos in start..tail {
            let slot = &mut self.buf[pos & self.mask];
            if *slot.sequence.get_mut() > 0 {
                // SAFETY: sequence > 0 means data was initialized by a producer
                // and not cleared by a subsequent claim_slot. Exclusive access
                // in drop (&mut self) guarantees no concurrency.
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

    use crate::common::DropCounter;

    // -----------------------------------------------------------------------
    // Basic tests (ported from MPSC/SPSC)
    // -----------------------------------------------------------------------

    #[test]
    fn capacity_one() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(1), 4).split();
        assert!(producer.push(42).is_ok());
        assert_eq!(consumer.pop(), Some(42));
    }

    #[test]
    fn zero_sized_types() {
        let (producer, mut consumer) = RingBuffer::<()>::new(Capacity::exact(4), 4).split();
        producer.push(()).unwrap();
        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
        assert_eq!(consumer.pop(), Some(()));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn pop_empty_buffer() {
        let (_producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn pop_empty_is_idempotent() {
        let (_producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for _ in 0..10 {
            assert_eq!(consumer.pop(), None);
        }
    }

    #[test]
    fn push_to_buffer() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn fill_to_capacity() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        assert!(producer.push(99).is_err());
        for i in 0..4 {
            assert_eq!(consumer.pop(), Some(i));
        }
    }

    #[test]
    fn length_invariants() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert!(producer.is_empty());
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        producer.push(2).unwrap();
        assert_eq!(producer.len(), 2);
        let _ = consumer.pop();
        assert_eq!(consumer.len(), 1);
        let _ = consumer.pop();
        assert!(consumer.is_empty());
    }

    #[test]
    fn wraparound_behavior() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        for i in 0..4 {
            assert_eq!(consumer.pop(), Some(i));
        }
        // Second lap
        for i in 10..14 {
            producer.push(i).unwrap();
        }
        for i in 10..14 {
            assert_eq!(consumer.pop(), Some(i));
        }
    }

    #[test]
    fn wraparound_exercises_all_slots() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let mut val = 0u32;
        for _lap in 0..3 {
            for _ in 0..4 {
                producer.push(val).unwrap();
                val += 1;
            }
            for expected in (val - 4)..val {
                assert_eq!(consumer.pop(), Some(expected));
            }
        }
    }

    #[test]
    fn overwrite_oldest_element() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        assert!(producer.push(99).is_err());
        // Pop all, then fill again (overwrites)
        for _ in 0..4 {
            let _ = consumer.pop();
        }
        for i in 10..14 {
            producer.push(i).unwrap();
        }
        for i in 10..14 {
            assert_eq!(consumer.pop(), Some(i));
        }
    }

    // -----------------------------------------------------------------------
    // Broadcast-specific tests
    // -----------------------------------------------------------------------

    #[test]
    fn two_consumers_see_same_data() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(8), 4).split();
        let mut c2 = c1.clone();
        producer.push(10).unwrap();
        producer.push(20).unwrap();
        producer.push(30).unwrap();
        assert_eq!(c1.pop(), Some(10));
        assert_eq!(c1.pop(), Some(20));
        assert_eq!(c1.pop(), Some(30));
        assert_eq!(c2.pop(), Some(10));
        assert_eq!(c2.pop(), Some(20));
        assert_eq!(c2.pop(), Some(30));
    }

    #[test]
    fn cloned_consumer_sees_only_future_items() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(8), 4).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        // Clone after two items published
        let mut c2 = c1.clone();
        producer.push(3).unwrap();

        assert_eq!(c1.pop(), Some(1));
        assert_eq!(c1.pop(), Some(2));
        assert_eq!(c1.pop(), Some(3));
        // c2 starts at tail=2, so only sees item 3
        assert_eq!(c2.pop(), Some(3));
        assert_eq!(c2.pop(), None);
    }

    #[test]
    fn consumer_drop_does_not_drain() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (producer, mut c1) = RingBuffer::<DropCounter>::new(Capacity::exact(8), 4).split();
        let c2 = c1.clone();
        producer
            .push(DropCounter {
                counter: counter.clone(),
            })
            .unwrap();
        // Drop c2 — should NOT drop the value in the slot
        drop(c2);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        // c1 can still read it
        let val = c1.pop().unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        drop(val);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn no_consumers_black_hole() {
        let (producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        drop(consumer);
        // With no consumers, push should succeed (buffer acts as black hole)
        for i in 0..100 {
            assert!(producer.push(i).is_ok());
        }
    }

    #[test]
    fn slow_consumer_blocks_producer() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let mut c2 = c1.clone();
        // Fill buffer
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        // Buffer full because c1 hasn't popped
        assert!(producer.push(99).is_err());
        // c2 pops all, but c1 is still slow
        for _ in 0..4 {
            let _ = c2.pop();
        }
        // Still full because c1 is the bottleneck
        assert!(producer.push(99).is_err());
        // c1 pops one, freeing one slot
        let _ = c1.pop();
        assert!(producer.push(99).is_ok());
    }

    #[test]
    fn producer_drops_old_values_on_overwrite() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (producer, mut consumer) = RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
        // Fill buffer
        for _ in 0..4 {
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        // Pop all (clones come out, originals stay in slots)
        for _ in 0..4 {
            let val = consumer.pop().unwrap();
            drop(val); // Drop the clone
        }
        // 4 clones dropped, but originals still in slots
        assert_eq!(counter.load(Ordering::Relaxed), 4);
        // Push new items, overwriting old slots — producer drops originals
        for _ in 0..4 {
            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
        }
        // 4 original values + 4 clones = 8 total drops so far
        assert_eq!(counter.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn multiple_producers_single_consumer() {
        let (p1, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(64), 4).split();
        let p2 = p1.clone();
        for i in 0..10 {
            p1.push(i).unwrap();
            p2.push(100 + i).unwrap();
        }
        let mut values: Vec<u32> = std::iter::from_fn(|| consumer.pop()).collect();
        values.sort_unstable();
        let mut expected: Vec<u32> = (0..10).chain(100..110).collect();
        expected.sort_unstable();
        assert_eq!(values, expected);
    }

    #[test]
    fn pop_requires_clone() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let mut c2 = c1.clone();
        producer.push(42).unwrap();
        // Both consumers get the same value
        assert_eq!(c1.pop(), Some(42));
        assert_eq!(c2.pop(), Some(42));
    }

    // -----------------------------------------------------------------------
    // Zero-copy API tests
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_write_commit_pop_ref_cycle() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let mut w = producer.reserve().unwrap();
        w.write(42);
        w.commit();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 42);
    }

    #[test]
    fn reserve_slot_mut_commit() {
        let (producer, mut consumer) = RingBuffer::<[u8; 256]>::new(Capacity::exact(4), 4).split();
        let mut w = producer.reserve().unwrap();
        w.slot_mut().write([0xAB; 256]);
        w.commit();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!((*reader)[0], 0xAB);
        assert_eq!((*reader)[255], 0xAB);
    }

    #[test]
    fn reserve_returns_none_when_full() {
        let (producer, _consumer) = RingBuffer::<u32>::new(Capacity::exact(2), 4).split();

        let w1 = producer.reserve().unwrap();
        w1.commit();

        let w2 = producer.reserve().unwrap();
        w2.commit();

        assert!(producer.reserve().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_empty() {
        let (_producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_not_committed() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let mut w = producer.reserve().unwrap();
        w.write(1);
        // Not committed yet
        assert!(consumer.pop_ref().is_none());
        w.commit();
        assert!(consumer.pop_ref().is_some());
    }

    #[test]
    fn slot_reader_does_not_drop_value() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (producer, mut c1) = RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
        let mut c2 = c1.clone();
        producer
            .push(DropCounter {
                counter: counter.clone(),
            })
            .unwrap();

        // pop_ref from c1, drop the reader
        {
            let reader = c1.pop_ref().unwrap();
            assert_eq!(counter.load(Ordering::Relaxed), 0);
            drop(reader);
        }
        // Value NOT dropped — c2 still needs it
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        // c2 can still clone it
        let val = c2.pop().unwrap();
        drop(val);
        assert_eq!(counter.load(Ordering::Relaxed), 1); // clone dropped
    }

    #[test]
    fn mixed_push_reserve_pop_pop_ref() {
        let (producer, mut consumer) =
            RingBuffer::<u32>::new(Capacity::exact(8), 4).split();
        producer.push(1).unwrap();
        let mut w = producer.reserve().unwrap();
        w.write(2);
        w.commit();
        producer.push(3).unwrap();

        assert_eq!(consumer.pop(), Some(1));
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 2);
        drop(reader);
        assert_eq!(consumer.pop(), Some(3));
    }

    #[test]
    fn reserve_pop_ref_wraparound() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for lap in 0u32..3 {
            for j in 0..4 {
                let mut w = producer.reserve().unwrap();
                w.write(lap * 10 + j);
                w.commit();
            }
            for j in 0..4 {
                let reader = consumer.pop_ref().unwrap();
                assert_eq!(*reader, lap * 10 + j);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Drop correctness
    // -----------------------------------------------------------------------

    #[test]
    fn drop_items_on_ringbuffer_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let (producer, _consumer) =
                RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
            for _ in 0..4 {
                producer
                    .push(DropCounter {
                        counter: counter.clone(),
                    })
                    .unwrap();
            }
            // Drop everything — RingBuffer::drop should clean up slots
        }
        assert_eq!(counter.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn consumer_drop_unregisters() {
        let (producer, mut c1) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let c2 = c1.clone();
        // Fill buffer
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        // c1 pops all, c2 is slow
        for _ in 0..4 {
            let _ = c1.pop();
        }
        // Can't push because c2 is blocking
        assert!(producer.push(99).is_err());
        // Drop c2 — should unregister, allowing producer to advance
        drop(c2);
        assert!(producer.push(99).is_ok());
    }

    // -----------------------------------------------------------------------
    // Concurrent tests
    // -----------------------------------------------------------------------

    #[test]
    fn concurrent_data_race_check() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let handle = std::thread::spawn(move || {
            for i in 0u64..100 {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        let mut received = 0u64;
        while received < 100 {
            if let Some(val) = consumer.pop() {
                assert_eq!(val, received);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        handle.join().unwrap();
    }

    #[test]
    fn concurrent_broadcast_two_consumers() {
        let (producer, mut c1) = RingBuffer::<u64>::new(Capacity::exact(8), 4).split();
        let mut c2 = c1.clone();
        let total = 100u64;

        let handle = std::thread::spawn(move || {
            for i in 0..total {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let h1 = std::thread::spawn(move || {
            let mut received = 0u64;
            while received < total {
                if let Some(val) = c1.pop() {
                    assert_eq!(val, received);
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let mut received = 0u64;
        while received < total {
            if let Some(val) = c2.pop() {
                assert_eq!(val, received);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        handle.join().unwrap();
        h1.join().unwrap();
    }

    #[test]
    fn concurrent_mpmc_data_race_check() {
        let (p1, mut c1) = RingBuffer::<u64>::new(Capacity::exact(8), 4).split();
        let p2 = p1.clone();
        let mut c2 = c1.clone();
        let items_per_producer = 50u64;

        let h1 = std::thread::spawn(move || {
            for i in 0..items_per_producer {
                while p1.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let h2 = std::thread::spawn(move || {
            for i in 0..items_per_producer {
                while p2.push(1000 + i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let total = items_per_producer * 2;

        let h3 = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                if c1.pop().is_some() {
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let mut count = 0u64;
        while count < total {
            if c2.pop().is_some() {
                count += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    }

    #[test]
    fn concurrent_reserve_pop_ref() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8), 4).split();
        let total = 100u64;

        let handle = std::thread::spawn(move || {
            for i in 0..total {
                loop {
                    if let Some(mut w) = producer.reserve() {
                        w.write(i);
                        w.commit();
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        });

        let mut received = 0u64;
        while received < total {
            if let Some(reader) = consumer.pop_ref() {
                assert_eq!(*reader, received);
                drop(reader);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        handle.join().unwrap();
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn stress_broadcast() {
        let (producer, mut c1) = RingBuffer::<u64>::new(Capacity::exact(1024), 8).split();
        let mut c2 = c1.clone();
        let mut c3 = c1.clone();
        let total = 100_000u64;

        let handle = std::thread::spawn(move || {
            for i in 0..total {
                while producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let h1 = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                if c1.pop().is_some() {
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let h2 = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                if c2.pop().is_some() {
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        let mut count = 0u64;
        while count < total {
            if c3.pop().is_some() {
                count += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        handle.join().unwrap();
        h1.join().unwrap();
        h2.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Arc wrapper tests
    // -----------------------------------------------------------------------

    #[test]
    fn arc_basic_push_pop() {
        let (producer, mut consumer) =
            arc::ArcRingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        producer.push(42).unwrap();
        let val = consumer.pop().unwrap();
        assert_eq!(*val, 42);
    }

    #[test]
    fn arc_two_consumers_cheap_clone() {
        let (producer, mut c1) =
            arc::ArcRingBuffer::<[u8; 2048]>::new(Capacity::exact(8), 4).split();
        let mut c2 = c1.clone();
        producer.push([0xAB; 2048]).unwrap();

        let v1 = c1.pop().unwrap();
        let v2 = c2.pop().unwrap();
        // Both Arc pointers point to the same allocation
        assert!(Arc::ptr_eq(&v1, &v2));
        assert_eq!(v1[0], 0xAB);
    }

    #[test]
    fn arc_push_returns_val_on_full() {
        let (producer, _consumer) =
            arc::ArcRingBuffer::<u32>::new(Capacity::exact(2), 4).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        let err = producer.push(3).unwrap_err();
        assert_eq!(err, 3);
    }

    #[test]
    fn arc_no_clone_needed() {
        // This type is NOT Clone — Arc wrapper makes broadcast work anyway
        #[derive(Debug)]
        struct NotClone(u64);
        let (producer, mut consumer) =
            arc::ArcRingBuffer::<NotClone>::new(Capacity::exact(4), 4).split();
        producer.push(NotClone(99)).unwrap();
        let val = consumer.pop().unwrap();
        assert_eq!(val.0, 99);
    }

    #[test]
    fn arc_pop_ref_derefs_to_inner() {
        let (producer, mut consumer) =
            arc::ArcRingBuffer::<String>::new(Capacity::exact(4), 4).split();
        producer.push("hello".to_string()).unwrap();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(&*reader, "hello");
    }

    #[test]
    fn arc_reserve_write_commit() {
        let (producer, mut consumer) =
            arc::ArcRingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let mut w = producer.reserve().unwrap();
        w.write(42);
        w.commit();
        let val = consumer.pop().unwrap();
        assert_eq!(*val, 42);
    }

    #[test]
    fn arc_drop_correctness() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let (producer, mut c1) =
                arc::ArcRingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
            let mut c2 = c1.clone();
            producer
                .push(DropCounter { counter: counter.clone() })
                .unwrap();
            let v1 = c1.pop().unwrap();
            let v2 = c2.pop().unwrap();
            // Same Arc, two refs
            assert_eq!(counter.load(Ordering::Relaxed), 0);
            drop(v1);
            assert_eq!(counter.load(Ordering::Relaxed), 0); // Still held by v2
            drop(v2);
            // v2 dropped, but original Arc still in the slot (producer hasn't overwritten)
            // The slot's Arc drops when the RingBuffer drops
        }
        // RingBuffer dropped — the Arc in the slot is released, dropping the DropCounter
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}
