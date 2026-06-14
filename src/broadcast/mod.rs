//! Multi-producer, multi-consumer (MPMC) broadcast ring buffer.
//!
//! Every consumer sees every item published after it subscribes. Multiple
//! producers push via atomic fetch-and-add (FAA); consumers are dynamically created by cloning
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

pub mod arc;
mod consumer;
mod producer;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};

use crate::capacity::Capacity;
use crate::common::park::WakeSet;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
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
/// Multiple producers push via atomic FAA. Consumers clone to subscribe.
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
    /// Always ≤ actual `min_head` (conservative), so a stale value is safe.
    pub(crate) min_head_cache: CachePadded<AtomicUsize>,
    /// Sync producer-side park state. Bit `i` of `producer_park.wake` is
    /// set while the producer in park slot `i` is blocked in
    /// [`Producer::push_block`] waiting for the slowest consumer to
    /// advance its head. Consumers `wake_one` after advancing a head.
    /// Independent of the async `producer_waker` below.
    pub(crate) producer_park: WakeSet,
    /// Round-robin park-slot allocator for blocking producers. Each
    /// `push_block`/`reserve_block` caller takes a stable slot
    /// `idx & PARK_MASK` so concurrent blocking producers don't collide
    /// on one bitmap bit (aliasing past 64 is benign — false wake,
    /// re-check, re-park).
    pub(crate) producer_park_idx: CachePadded<AtomicUsize>,
    /// Live producer count (only tracked when `async` is enabled). When
    /// it reaches zero `closed` is set so `pop_async` can resolve to `None`.
    #[cfg(feature = "async")]
    pub(crate) producer_count: CachePadded<AtomicUsize>,
    /// Set by the last producer drop. Consumers' `pop_async` observe
    /// it and resolve to `None` once their backlog drains.
    #[cfg(feature = "async")]
    pub(crate) closed: CachePadded<AtomicBool>,
    /// Wakers registered by parked `push_async` futures. Slot index =
    /// pos & mask — producers waiting on slot `s` register there, and
    /// consumers wake the matching slot's waker after advancing head.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Wakers registered by parked `pop_async` futures. Slot index =
    /// consumer slot index — each consumer registers in its own waker
    /// slot. Producers `wake_one` after every push (one slot per push
    /// is sufficient because every consumer needs the same publish).
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
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
            producer_park: WakeSet::new(),
            producer_park_idx: CachePadded(AtomicUsize::new(0)),
            consumer_slots: consumer_slots.into_boxed_slice(),
            #[cfg(feature = "async")]
            producer_count: CachePadded(AtomicUsize::new(1)),
            #[cfg(feature = "async")]
            closed: CachePadded(AtomicBool::new(false)),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
        }
    }

    /// Wakes one blocking producer parked in [`Producer::push_block`] /
    /// [`Producer::reserve_block`], if any. Self-gates on the wake
    /// bitmap being non-zero (a single load on the fast path), so it's
    /// cheap to call after every consumer head advance. A producer's
    /// unblock condition is `min_head` moving, which depends on the
    /// slowest consumer — so any consumer advancing may free the
    /// producer; a false wake just re-checks and re-parks.
    #[inline]
    pub(crate) fn wake_producer(&self) {
        self.producer_park.wake_one();
    }

    /// Wakes one async producer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_producer_async(&self) {
        self.producer_waker.wake_one();
    }

    /// Wakes one async consumer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_consumer_async(&self) {
        self.consumer_waker.wake_one();
    }

    /// Returns a reference to the slot at logical position `pos`.
    ///
    /// Single point of unsafety: `pos & mask < cap == buf.len()`.
    #[inline]
    pub(crate) fn slot(&self, pos: usize) -> &BroadcastSlot<T> {
        let idx = pos & self.mask;
        // SAFETY: mask = cap - 1, cap = buf.len(), so idx < buf.len().
        unsafe { std::hint::assert_unchecked(idx < self.buf.len()) };
        &self.buf[idx]
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
            park_slot: 0,
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
            let seq = *slot.sequence.get_mut();
            if seq > 0 && seq != crate::common::TOMBSTONE {
                // SAFETY: sequence > 0 and not tombstoned means data was
                // initialized by a producer and not cleared by a subsequent
                // claim_slot. Exclusive access in drop (&mut self) guarantees
                // no concurrency.
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
        let (producer, mut consumer) =
            RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();
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
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let w = producer.reserve().unwrap();
        w.write(42).commit();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 42);
    }

    #[test]
    fn reserve_slot_mut_commit() {
        let (mut producer, mut consumer) =
            RingBuffer::<[u8; 256]>::new(Capacity::exact(4), 4).split();
        let mut w = producer.reserve().unwrap();
        w.slot_mut().write([0xAB; 256]);
        // SAFETY: slot_mut().write() initialized the slot above.
        unsafe { w.commit_unchecked() };
        let reader = consumer.pop_ref().unwrap();
        assert_eq!((*reader)[0], 0xAB);
        assert_eq!((*reader)[255], 0xAB);
    }

    #[test]
    fn reserve_returns_none_when_full() {
        let (mut producer, _consumer) = RingBuffer::<u32>::new(Capacity::exact(2), 4).split();

        let w1 = producer.reserve().unwrap();
        w1.write(0).commit();

        let w2 = producer.reserve().unwrap();
        w2.write(0).commit();

        assert!(producer.reserve().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_empty() {
        let (_producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_not_committed() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        let w = producer.reserve().unwrap();
        let written = w.write(1);
        // Not committed yet
        assert!(consumer.pop_ref().is_none());
        written.commit();
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
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(8), 4).split();
        producer.push(1).unwrap();
        let w = producer.reserve().unwrap();
        w.write(2).commit();
        producer.push(3).unwrap();

        assert_eq!(consumer.pop(), Some(1));
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 2);
        drop(reader);
        assert_eq!(consumer.pop(), Some(3));
    }

    #[test]
    fn reserve_pop_ref_wraparound() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        for lap in 0u32..3 {
            for j in 0..4 {
                let w = producer.reserve().unwrap();
                w.write(lap * 10 + j).commit();
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
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8), 4).split();
        let total = 100u64;

        let handle = std::thread::spawn(move || {
            for i in 0..total {
                loop {
                    if let Some(w) = producer.reserve() {
                        w.write(i).commit();
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
        let (producer, _consumer) = arc::ArcRingBuffer::<u32>::new(Capacity::exact(2), 4).split();
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
        let (mut producer, mut consumer) =
            arc::ArcRingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        let w = producer.reserve().unwrap();
        w.write(42).commit();
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
                .push(DropCounter {
                    counter: counter.clone(),
                })
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

    // -----------------------------------------------------------------------
    // Audit gap scenarios (Miri-friendly)
    // -----------------------------------------------------------------------

    /// Holding a `SlotReader` blocks the producer's overwrite via `min_head`.
    /// This exercises the liveness contract: as long as a reader exists,
    /// the producer cannot reclaim that slot, so a fill-then-push fails.
    #[test]
    fn pop_ref_blocks_producer_overwrite() {
        let cap = 4u32;
        let (producer, mut consumer) =
            RingBuffer::<u32>::new(Capacity::exact(cap as usize), 4).split();

        for i in 0..cap {
            producer.push(i).unwrap();
        }
        assert!(producer.push(99).is_err(), "buffer is full");

        // Hold the first item via pop_ref — consumer's head does NOT
        // advance until the reader drops.
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 0);

        // Producer still cannot push: this consumer's head is at 0,
        // so min_head == 0 and the buffer appears full.
        assert!(
            producer.push(99).is_err(),
            "producer must be blocked while a reader holds slot 0"
        );

        // Drop the reader → consumer head advances → producer can push.
        drop(reader);
        assert!(producer.push(99).is_ok());

        let _ = consumer.pop().unwrap(); // 1
        let _ = consumer.pop().unwrap(); // 2
        let _ = consumer.pop().unwrap(); // 3
        assert_eq!(consumer.pop(), Some(99));
    }

    /// `RingBuffer::Drop` must drop only slots whose sequence number
    /// indicates real published data — not tombstones.
    #[test]
    fn drop_skips_tombstoned_slots() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let (mut producer, _consumer) =
                RingBuffer::<DropCounter>::new(Capacity::exact(4), 4).split();

            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();

            // Reserve + drop without commit → tombstone (no value to drop)
            {
                let _w = producer.reserve().unwrap();
            }

            producer
                .push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();

            // Reserve + write but no commit → WrittenSlot::Drop drops
            // the value immediately and tombstones the slot.
            {
                let w = producer.reserve().unwrap();
                w.write(DropCounter {
                    counter: counter.clone(),
                });
            }
            assert_eq!(counter.load(Ordering::Relaxed), 1);

            // Drop the RingBuffer: must drop the 2 published values
            // (positions 0 and 2), skip the 2 tombstoned slots.
        }
        assert_eq!(
            counter.load(Ordering::Relaxed),
            3,
            "1 in-flight + 2 still-published = 3 total drops"
        );
    }

    /// Capacity-1 broadcast with a producer thread + consumer thread.
    #[test]
    fn capacity_one_concurrent() {
        let n = 8u32;
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(1), 4).split();

        let h = std::thread::spawn(move || {
            for i in 0..n {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        let mut got = Vec::new();
        while got.len() < n as usize {
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
    // Async API
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(feature = "async")]
    fn async_push_pop_cross_thread() {
        async_push_pop_cross_thread_run(1, 5_000, 8, 60);
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_push_pop_cross_thread_iters_unsaturated() {
        // Stress: many iterations of (push 1k items, drop producer) with
        // cap >> total so the producer never parks. Catches register/close
        // races that single-iter tests miss.
        async_push_pop_cross_thread_run(20_000, 1_000, 4096, 60);
    }

    #[cfg(feature = "async")]
    fn async_push_pop_cross_thread_run(iters: usize, total: u64, cap: usize, _deadline_secs: u64) {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::Arc;

        let done = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(AtomicU64::new(0));
        let watchdog = crate::common::spawn_progress_watchdog(
            progress.clone(),
            done.clone(),
            "broadcast async_push_pop_cross_thread",
        );

        for _ in 0..iters {
            let (producer, c1) = RingBuffer::<u64>::new(Capacity::exact(cap), 4).split();
            let c2 = c1.clone();

            let consumer_threads: Vec<_> = [c1, c2]
                .into_iter()
                .map(|mut c| {
                    let progress = progress.clone();
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .unwrap();
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            let mut received = 0u64;
                            while c.pop_async().await.is_some() {
                                received += 1;
                                progress.fetch_add(1, Ordering::Relaxed);
                            }
                            assert_eq!(received, total);
                        }));
                    })
                })
                .collect();

            let ph = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let local = tokio::task::LocalSet::new();
                rt.block_on(local.run_until(async move {
                    for i in 0..total {
                        producer.push_async(i).await.expect("all consumers dropped");
                    }
                    // Producer dropped here → consumers' pop_async resolve to None
                    // after they drain.
                }));
            });

            ph.join().unwrap();
            for h in consumer_threads {
                h.join().unwrap();
            }
        }
        done.store(true, Ordering::Release);
        watchdog.join().unwrap();
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_pop_returns_none_on_close() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
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
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4), 4).split();
        // Fill so the producer must park on the next push.
        for i in 0..4u64 {
            producer.push(i).unwrap();
        }
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

    // -----------------------------------------------------------------------
    // Blocking push_block / reserve_block (sync park, no busy spin)
    // -----------------------------------------------------------------------

    #[test]
    fn push_block_empty_succeeds_immediately() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4), 4).split();
        assert!(producer.push_block(1).is_ok());
        assert!(producer.push_block(2).is_ok());
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
    }

    #[test]
    fn push_block_parks_until_consumer_advances() {
        // cap=2: fill it, then a blocking push must park until the
        // consumer pops, freeing a slot. No value is lost or spun on.
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        producer.push(10).unwrap();
        producer.push(20).unwrap();
        assert!(producer.is_full());

        let h = std::thread::spawn(move || {
            // Blocks here: ring is full. Wakes when main pops below.
            producer.push_block(30).unwrap();
            producer
        });

        // Give the producer time to actually park rather than win a race.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(consumer.pop(), Some(10)); // frees a slot, wakes producer

        let producer = h.join().unwrap();
        // The parked push went through.
        assert_eq!(consumer.pop(), Some(20));
        assert_eq!(consumer.pop(), Some(30));
        drop(producer);
    }

    #[test]
    fn push_block_returns_err_when_all_consumers_gone() {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert!(producer.is_full());

        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(consumer); // last consumer gone → producer must unblock with Err
        });

        // Parks (full), then the consumer drop flushes it; no consumer
        // means the value can never be observed, so Err is returned.
        assert_eq!(producer.push_block(99), Err(99));
        h.join().unwrap();
    }

    #[test]
    fn reserve_block_parks_until_consumer_advances() {
        let (mut producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        producer.push(10).unwrap();
        producer.push(20).unwrap();

        let h = std::thread::spawn(move || {
            let slot = producer.reserve_block().expect("consumer still active");
            slot.write(30).commit();
            producer
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(consumer.pop(), Some(10));

        let producer = h.join().unwrap();
        assert_eq!(consumer.pop(), Some(20));
        assert_eq!(consumer.pop(), Some(30));
        drop(producer);
    }

    #[test]
    fn push_block_multi_consumer_waits_for_slowest() {
        // Two consumers; the producer can't advance past the slowest.
        // Draining only the fast consumer must NOT unblock the producer;
        // the slow consumer popping is what frees the slot.
        let (producer, mut fast) = RingBuffer::<u64>::new(Capacity::exact(2), 4).split();
        let mut slow = fast.clone();
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        let h = std::thread::spawn(move || {
            producer.push_block(3).unwrap();
            producer
        });

        std::thread::sleep(std::time::Duration::from_millis(30));
        // Fast consumer drains fully; min_head still pinned by `slow`.
        assert_eq!(fast.pop(), Some(1));
        assert_eq!(fast.pop(), Some(2));
        std::thread::sleep(std::time::Duration::from_millis(30));
        // Now the slow consumer advances, raising min_head → producer wakes.
        assert_eq!(slow.pop(), Some(1));

        let producer = h.join().unwrap();
        assert_eq!(slow.pop(), Some(2));
        assert_eq!(slow.pop(), Some(3));
        assert_eq!(fast.pop(), Some(3));
        drop(producer);
    }

    #[test]
    fn push_block_streams_far_past_capacity() {
        // Regression for the relay burst hang: a single producer pushes
        // many multiples of capacity through push_block while two
        // consumers drain. Pre-fix, push_block delegated to push, whose
        // claim_slot did an unconditional FAA then spun unboundedly on a
        // full ring — so the producer wedged instead of parking. The wrap
        // (N ≫ cap) is what exercises the "claimed slot still occupied"
        // branch.
        const CAP: usize = 16;
        const N: u64 = 100_000;
        let (producer, c1) = RingBuffer::<u64>::new(Capacity::exact(CAP), 2).split();
        let c2 = c1.clone();

        let prod = std::thread::spawn(move || {
            for i in 0..N {
                producer.push_block(i).expect("consumers alive");
            }
        });

        // Move each consumer into its own drain thread (no lingering
        // handle that would pin min_head at 0 and stall the producer).
        let drain = |mut c: Consumer<u64>| {
            std::thread::spawn(move || {
                let mut next = 0u64;
                while next < N {
                    match c.pop() {
                        Some(v) => {
                            assert_eq!(v, next, "broadcast consumer saw out-of-order item");
                            next += 1;
                        }
                        None => std::thread::yield_now(),
                    }
                }
            })
        };
        let d1 = drain(c1);
        let d2 = drain(c2);

        prod.join().unwrap();
        d1.join().unwrap();
        d2.join().unwrap();
    }

    #[test]
    fn push_block_multi_producer_streams_past_capacity() {
        // Mirrors the SubRepl ring: N producers (one per shard) sharing
        // one broadcast ring, blocking-pushing concurrently while
        // consumers drain. Verifies the multi-producer race path in
        // push_block (has_space true, then a peer takes the slot → Err →
        // loop) terminates rather than wedging or recursing.
        const CAP: usize = 8;
        const PER: u64 = 20_000;
        const PRODUCERS: u64 = 3;
        // One consumer only: a second idle consumer would pin min_head at
        // 0 and (correctly) block the producers forever.
        let (seed, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(CAP), 2).split();

        // Producer is !Sync (per-handle Cell cache + park slot), so each
        // thread takes its own clone — same as the relay's per-shard
        // SubRepl producers.
        let mut prod_handles = Vec::new();
        for _ in 0..PRODUCERS {
            let p = seed.clone();
            prod_handles.push(std::thread::spawn(move || {
                for i in 0..PER {
                    p.push_block(i).expect("consumer alive");
                }
            }));
        }
        drop(seed);

        let total = PER * PRODUCERS;
        let drained = std::thread::spawn(move || {
            let mut count = 0u64;
            while count < total {
                match consumer.pop() {
                    Some(_) => count += 1,
                    None => std::thread::yield_now(),
                }
            }
            count
        });

        for h in prod_handles {
            h.join().unwrap();
        }
        assert_eq!(drained.join().unwrap(), total);
    }
}
