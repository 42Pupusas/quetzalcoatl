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

mod consumer;
mod producer;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};

use crate::capacity::Capacity;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::Thread;

/// A lock-free SPSC ring buffer.
///
/// Created via [`RingBuffer::new`], then [`split`](RingBuffer::split) into
/// a [`Producer`] / [`Consumer`] pair. Neither handle is [`Clone`],
/// enforcing the single-producer, single-consumer invariant.
// repr(C) locks field order: shared immutable fields first (same cache
// line), then head and tail each on their own cache-padded line.
#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) buf: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    pub(crate) head: CachePadded<AtomicUsize>,
    pub(crate) tail: CachePadded<AtomicUsize>,
    /// Set when the [`Producer`] is dropped. [`Consumer::pop_block`]
    /// observes this and returns `None` once the queue drains.
    pub(crate) producer_closed: CachePadded<AtomicBool>,
    /// Set when the [`Consumer`] is dropped. [`Producer::push_block`]
    /// observes this and returns `Err(val)` instead of hanging.
    pub(crate) consumer_closed: CachePadded<AtomicBool>,
    /// Single-producer park handle. Idempotently set the first time
    /// the producer parks; the consumer reads it via `OnceLock::get`
    /// to issue the unpark.
    pub(crate) producer_parker: OnceLock<Thread>,
    /// `true` ↔ producer is currently parked. Consumers gate their
    /// unpark on this `Relaxed` load to keep the no-park hot path free.
    pub(crate) producer_parked: CachePadded<AtomicBool>,
    /// Single-consumer park handle. Symmetric to `producer_parker`.
    pub(crate) consumer_parker: OnceLock<Thread>,
    /// `true` ↔ consumer is currently parked.
    pub(crate) consumer_parked: CachePadded<AtomicBool>,
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
            producer_closed: CachePadded(AtomicBool::new(false)),
            consumer_closed: CachePadded(AtomicBool::new(false)),
            producer_parker: OnceLock::new(),
            producer_parked: CachePadded(AtomicBool::new(false)),
            consumer_parker: OnceLock::new(),
            consumer_parked: CachePadded(AtomicBool::new(false)),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
        }
    }

    /// Wakes the parked producer, if any. Gated on a `Relaxed` load
    /// of `producer_parked` so the no-park hot path stays branch-free
    /// in the common case.
    #[inline]
    pub(crate) fn wake_producer(&self) {
        if !self.producer_parked.0.load(Ordering::Relaxed) {
            return;
        }
        self.producer_parked.0.store(false, Ordering::Relaxed);
        if let Some(handle) = self.producer_parker.get() {
            handle.unpark();
        }
    }

    /// Wakes the parked consumer, if any. Symmetric to `wake_producer`.
    #[inline]
    pub(crate) fn wake_consumer(&self) {
        if !self.consumer_parked.0.load(Ordering::Relaxed) {
            return;
        }
        self.consumer_parked.0.store(false, Ordering::Relaxed);
        if let Some(handle) = self.consumer_parker.get() {
            handle.unpark();
        }
    }

    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_producer_async(&self) {
        self.producer_waker.wake_one();
    }

    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_consumer_async(&self) {
        self.consumer_waker.wake_one();
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

    /// Returns a reference to the slot at logical position `pos`.
    ///
    /// Single point of unsafety: `pos & mask` is always `< cap == buf.len()`
    /// because `mask = cap - 1` and `cap` is a power of two. Hinting this
    /// to the optimizer lets safe indexing compile to the same code as
    /// `get_unchecked`, while keeping every call site in safe Rust.
    #[inline]
    pub(crate) fn slot(&self, pos: usize) -> &UnsafeCell<MaybeUninit<T>> {
        let idx = pos & self.mask;
        // SAFETY: mask = cap - 1, cap = buf.len(), so idx < buf.len().
        unsafe { std::hint::assert_unchecked(idx < self.buf.len()) };
        &self.buf[idx]
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
        let cap = Capacity::exact(1024);
        let n = 128 * 1024 * 1024_u64;
        let instant = std::time::Instant::now();
        let (producer, mut consumer) = RingBuffer::<u64>::new(cap).split();

        let handle = std::thread::spawn(move || {
            let mut received = 0u64;
            while received < n {
                if let Some(val) = consumer.pop() {
                    assert_eq!(val, received);
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        for i in 0..n {
            while producer.push(i).is_err() {
                std::hint::spin_loop();
            }
        }

        handle.join().unwrap();
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

        // Commit each writer immediately to avoid panicking on drop if
        // a later assertion fails.
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

        // Reserve but don't commit — slot is claimed but not visible
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
    fn reserve_drop_without_commit_rolls_back() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        producer.push(1).unwrap();
        {
            let w = producer.reserve().unwrap();
            w.write(2);
            // drop without commit — should roll back, not panic
        }
        producer.push(3).unwrap();

        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn reserve_drop_without_write_rolls_back() {
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
            "written value should be dropped on SlotWriter rollback"
        );
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

    /// Capacity-1 with concurrent producer/consumer threads. Tightest
    /// possible synchronization — every push must wait for every pop.
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
    // Blocking API
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
    fn pop_block_drains_before_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        drop(p);
        assert_eq!(c.pop_block(), Some(1));
        assert_eq!(c.pop_block(), Some(2));
        assert_eq!(c.pop_block(), None);
    }

    #[test]
    fn push_block_unblocks_on_pop() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let h = std::thread::spawn(move || p.push_block(99));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(c.pop(), Some(0));
        assert_eq!(h.join().unwrap(), Ok(()));
        // Drain remaining including the unblocked push.
        assert_eq!(c.pop(), Some(1));
        assert_eq!(c.pop(), Some(2));
        assert_eq!(c.pop(), Some(3));
        assert_eq!(c.pop(), Some(99));
    }

    #[test]
    fn push_block_returns_err_on_consumer_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let h = std::thread::spawn(move || p.push_block(99));
        std::thread::sleep(std::time::Duration::from_millis(50));
        // Drain the buffer so consumer's Drop only sets consumer_closed
        // after we've seen push_block parked.
        while c.pop().is_some() {}
        drop(c);
        assert_eq!(h.join().unwrap(), Err(99));
    }

    #[test]
    fn is_closed_reflects_producer_drop() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        assert!(!c.is_closed());
        drop(p);
        assert!(c.is_closed());
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
        let (mut p, mut c) = RingBuffer::<u32>::new(Capacity::exact(2)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        let h = std::thread::spawn(move || p.reserve_block().is_some());
        std::thread::sleep(std::time::Duration::from_millis(50));
        // Drain so consumer drop only signals after producer is parked.
        while c.pop().is_some() {}
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
        // First two should yield SlotReaders; third returns None.
        let v1 = *c.pop_ref_block().unwrap();
        let v2 = *c.pop_ref_block().unwrap();
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert!(c.pop_ref_block().is_none());
    }

    // -----------------------------------------------------------------------
    // Drain
    // -----------------------------------------------------------------------

    #[test]
    fn drain_empty_returns_zero() {
        let (_p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let n = c.drain(|_| panic!("unexpected"));
        assert_eq!(n, 0);
    }

    #[test]
    fn drain_all_available() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        let mut got = Vec::new();
        let n = c.drain(|v| got.push(v));
        assert_eq!(n, 4);
        assert_eq!(got, vec![0, 1, 2, 3]);
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn drain_up_to_limit() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        let mut got = Vec::new();
        let n = c.drain_up_to(2, |v| got.push(v));
        assert_eq!(n, 2);
        assert_eq!(got, vec![0, 1]);
        // Remaining are still there.
        assert_eq!(c.pop(), Some(2));
        assert_eq!(c.pop(), Some(3));
    }

    #[test]
    fn drain_block_drains_to_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || {
            let mut got = Vec::new();
            c.drain_block(|v| got.push(v));
            got
        });
        // Push, sleep, push, drop. drain_block should pick everything up.
        p.push(1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        p.push(2).unwrap();
        p.push(3).unwrap();
        drop(p);
        let got = h.join().unwrap();
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[test]
    fn drain_wakes_producer() {
        // SPSC has only one producer. Test that drain releasing the
        // ring wakes it from push_block.
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        let h = std::thread::spawn(move || {
            for i in 4..8u32 {
                p.push_block(i).expect("consumer dropped");
            }
        });
        // Producer parks waiting for space.
        std::thread::sleep(std::time::Duration::from_millis(50));
        // Single drain frees all 4 slots and wakes producer.
        let n = c.drain(|_| {});
        assert_eq!(n, 4);
        // Producer should complete its 4 more pushes promptly.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !h.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "drain failed to wake producer"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        h.join().unwrap();
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_push_pop_cross_thread() {
        // Producer and consumer on separate threads. The watchdog thread
        // aborts the process if either side hangs longer than 5s, so the
        // test fails fast instead of leaving the harness blocked on join.
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let done = Arc::new(AtomicBool::new(false));
        let done_watchdog = done.clone();
        let watchdog = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !done_watchdog.load(Ordering::Acquire) {
                if std::time::Instant::now() > deadline {
                    eprintln!("\n\nasync_push_pop_cross_thread: deadlocked, aborting\n");
                    std::process::abort();
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        });

        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let total: u64 = 1_000_000;

        let ph = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                for i in 0..total {
                    producer.push_async(i).await.expect("consumer dropped");
                }
            }));
        });

        let ch = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                let mut received = 0u64;
                while received < total {
                    match consumer.pop_async().await {
                        Some(_) => received += 1,
                        None => break,
                    }
                }
                assert_eq!(received, total);
            }));
        });

        ph.join().unwrap();
        ch.join().unwrap();
        done.store(true, Ordering::Release);
        watchdog.join().unwrap();
    }
}
