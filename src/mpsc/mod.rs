//! Multi-producer, single-consumer (MPSC) lock-free ring buffer.
//!
//! Multiple producers push concurrently through one atomic tail cursor.
//! A single consumer pops items in FIFO order. The producer handle is
//! [`Clone`], so new producers can be created at any time.
//!
//! A compare-and-exchange operation checks capacity and advances the tail
//! as one operation. Thus, a non-blocking push cannot claim a full slot.
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

mod batch_release;
mod consumer;
mod producer;
mod slot_release;

pub use consumer::{Consumer, SlotReader};
pub use producer::{Producer, SlotWriter, WrittenSlot};

use crate::capacity::Capacity;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkRegistry;
use crate::common::thread_parker::ThreadParker;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::{AlignedBuf, CachePadded, SeqSlot};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    /// Leases park-slot indices to `Producer` handles, reclaiming each
    /// on drop so a slot is never shared by two live producers.
    pub(crate) producer_slots: ParkRegistry,
    /// Single-consumer park handle. Re-armed by the consumer each time
    /// it parks, so a consumer that moves between threads is woken on
    /// whichever thread is parked now; producers claim it to unpark.
    pub(crate) consumer_parker: ThreadParker,
    /// `true` ↔ the consumer is currently parked on `consumer_parker`.
    /// Producers gate their unpark on this `Relaxed` load to keep the
    /// no-park hot path free.
    pub(crate) consumer_parked: CachePadded<AtomicBool>,
    /// Async equivalent of `producer_park`: per-producer-slot wakers
    /// registered by `Poll::Pending` from `push_async`. Woken by the
    /// consumer after every pop / drain.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Async equivalent of `consumer_parker`: single waker registered by
    /// `Poll::Pending` from `pop_async`. Woken by any producer after a
    /// successful push or commit.
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
}

// Safety: Multiple producers atomically claim unique tail slots.
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
            producer_slots: ParkRegistry::new(),
            consumer_parker: ThreadParker::new(),
            consumer_parked: CachePadded(AtomicBool::new(false)),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
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

    /// Externally closes the ring, causing a blocked
    /// [`pop_block`](Consumer::pop_block) to return `None` once the ring
    /// has drained.
    ///
    /// This signals the consumer; it does not close the producers.
    /// Pushes after it still succeed, and any value left unread is
    /// dropped when the ring is. Callers who need pushes to fail should
    /// drop the consumer, which is what
    /// [`push_block`](Producer::push_block) reports through `Err`.
    pub fn close(&self) {
        // SeqCst: see Producer::drop.
        self.closed.0.store(true, Ordering::SeqCst);
        self.wake_consumer();
        #[cfg(feature = "async")]
        self.consumer_waker.flush();
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
    ///
    /// Both handles hold an `Arc` to the ring buffer, so `T` must be
    /// `'static`. For borrowed data (non-`'static` `T`), use
    /// [`split_borrowed`](Self::split_borrowed).
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self);
        let park_slot = arc.producer_slots.lease();
        let producer = Producer::new_with(arc.clone(), park_slot);
        let consumer = Consumer { queue: arc };
        (producer, consumer)
    }

    /// Splits a *borrowed* ring buffer into a producer/consumer pair
    /// whose lifetimes are tied to the ring.
    ///
    /// Unlike [`split`](Self::split), the handles hold `&RingBuffer<T>`
    /// instead of `Arc<RingBuffer<T>>`. This lets `T` carry lifetimes
    /// shorter than `'static`.
    ///
    /// The returned producer is not `Clone`. Create additional producers
    /// via [`Producer::new_producer`], which assigns each one its own
    /// park slot.
    ///
    /// Takes `&mut self` so the ring can be split only once. A second
    /// split would mint a second *consumer* — breaking the single-
    /// consumer contract this queue's `head` protocol relies on — and a
    /// second producer sharing park slot 0 with the first. The exclusive
    /// borrow makes that a borrow-check error rather than undefined
    /// behavior.
    ///
    /// ```compile_fail
    /// use quetzalcoatl::mpsc::RingBuffer;
    /// use quetzalcoatl::capacity::Capacity;
    ///
    /// let mut ring = RingBuffer::<String>::new(Capacity::exact(4));
    /// let (p1, mut c1) = ring.split_borrowed();
    /// let (p2, mut c2) = ring.split_borrowed();
    /// p1.push("a".to_string()).unwrap();
    /// ```
    #[must_use]
    pub fn split_borrowed(&mut self) -> (Producer<T, &Self>, Consumer<T, &Self>) {
        let shared: &Self = self;
        let park_slot = shared.producer_slots.lease();
        let producer = Producer::new_with(shared, park_slot);
        let consumer = Consumer { queue: shared };
        (producer, consumer)
    }
}

impl<T> RingBuffer<T> {
    /// Wakes the single parked consumer, if any. Gated on a `Relaxed`
    /// load of `consumer_parked` so the no-park hot path stays
    /// branch-free in the common case.
    ///
    /// Idempotent: clearing the flag means any racing producer that
    /// observes it already cleared simply skips the unpark. The
    /// post-clear unpark is still issued by the winner — a stale
    /// `unpark()` after the consumer rechecks is benign (it makes
    /// the next park return immediately, which is fine because the
    /// consumer re-checks the queue after every wake).
    #[inline]
    pub(crate) fn wake_consumer(&self) {
        std::sync::atomic::fence(Ordering::SeqCst);
        if !self.consumer_parked.0.load(Ordering::SeqCst) {
            return;
        }
        self.consumer_parked.0.store(false, Ordering::Relaxed);
        self.consumer_parker.wake();
    }

    /// Wakes the async consumer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_consumer_async(&self) {
        self.consumer_waker.wake_all();
    }

    /// Wakes the async producer tasks waiting for space.
    ///
    /// Call sites are unconditional; this and
    /// [`notify_producers_n`](Self::notify_producers_n) are where the
    /// async feature enters the producer notification path.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_producers(&self) {
        self.producer_waker.wake_all();
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_producers(&self) {}

    /// Wakes up to `n` async producer tasks. Mirrors
    /// `producer_park.wake_n` for batched releases.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn notify_producers_n(&self, n: usize) {
        self.producer_waker.wake_n(n);
    }

    #[cfg(not(feature = "async"))]
    #[inline]
    #[allow(clippy::unused_self)]
    pub(crate) const fn notify_producers_n(&self, _n: usize) {}
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
                .unwrap_or_else(|_| panic!("Failed to push {i}"));
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

    /// Two producers, tiny buffer — checks tail + sequence synchronization.
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

    /// A batch that finds only tombstones still releases their
    /// positions. Publishing `head` only when a value came out cleared
    /// the slot metadata while leaving the cursor on the tombstone, so
    /// the capacity was never handed back and the ring stayed full
    /// forever.
    #[test]
    fn tombstone_only_drain_releases_capacity() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(2)).split();

        for _ in 0..2 {
            drop(producer.reserve().expect("a slot is free"));
        }
        assert!(producer.is_full(), "both positions are claimed");

        assert_eq!(consumer.drain(drop), 0, "tombstones deliver no values");
        assert!(consumer.is_empty(), "but their positions must be released");

        producer.push(1).expect("the released capacity is reusable");
        producer.push(2).expect("and so is the second position");
        let mut items = Vec::new();
        assert_eq!(consumer.drain(|v| items.push(v)), 2);
        assert_eq!(items, vec![1, 2]);
    }

    /// The same for `drain_up_to`, whose limit counts delivered values
    /// and so never bounds the tombstone skipping.
    #[test]
    fn tombstone_only_drain_up_to_releases_capacity() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(2)).split();

        for _ in 0..2 {
            drop(producer.reserve().expect("a slot is free"));
        }

        assert_eq!(consumer.drain_up_to(8, drop), 0);
        assert!(consumer.is_empty(), "positions released");
        producer.push(7).expect("capacity came back");
        assert_eq!(consumer.pop(), Some(7));
    }

    /// A blocking zero-copy read must not report an empty ring because
    /// the position at its head happened to hold an abandoned
    /// reservation. The producer is live and publishes right after, so
    /// the contract says block and deliver that value.
    #[test]
    fn pop_ref_block_skips_a_tombstone_and_waits_for_the_value() {
        let (mut producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        drop(producer.reserve().expect("the empty ring has a slot"));

        let writer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            producer.push(99).unwrap();
            producer
        });

        let reader = consumer
            .pop_ref_block()
            .expect("the producer is live, so this must block rather than end");
        assert_eq!(*reader, 99);
        drop(reader);
        drop(writer.join().unwrap());
    }

    /// A panicking `drain` callback must not lose track of the items
    /// already taken. The values are moved out of their slots one at a
    /// time, but `head` is published only after the whole batch; if the
    /// unwind skips that publication, `Consumer::drop` drains from the
    /// stale cursor and drops the same values a second time.
    #[test]
    fn drain_callback_panic_does_not_double_drop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) =
            RingBuffer::<crate::common::DropCounter>::new(Capacity::exact(4)).split();

        for _ in 0..4 {
            producer
                .push(crate::common::DropCounter {
                    counter: std::sync::Arc::clone(&counter),
                })
                .unwrap();
        }

        let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen_in = std::sync::Arc::clone(&seen);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            consumer.drain(|item| {
                drop(item);
                assert!(
                    seen_in.fetch_add(1, std::sync::atomic::Ordering::Relaxed) != 1,
                    "callback failure"
                );
            });
        }));
        assert!(result.is_err(), "the panic must propagate to the caller");

        drop(consumer);
        drop(producer);

        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            4,
            "each item must be dropped exactly once"
        );
    }

    /// Same hazard reached through `drain_up_to`.
    #[test]
    fn drain_up_to_callback_panic_does_not_double_drop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) =
            RingBuffer::<crate::common::DropCounter>::new(Capacity::exact(4)).split();

        for _ in 0..4 {
            producer
                .push(crate::common::DropCounter {
                    counter: std::sync::Arc::clone(&counter),
                })
                .unwrap();
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            consumer.drain_up_to(3, |item| {
                drop(item);
                panic!("callback failure");
            });
        }));
        assert!(result.is_err());

        drop(consumer);
        drop(producer);

        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            4,
            "each item must be dropped exactly once"
        );
    }

    /// A `T` whose destructor panics must still release its slot. The
    /// `SlotReader` drops the value before publishing the sequence and
    /// `head`, so an unwind in between leaves a destroyed value marked
    /// readable.
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

    /// The consumer parks on thread B after having parked on thread A.
    /// `Consumer` is `Send`, so this is reachable from safe code; the
    /// wake must reach whichever thread is parked *now*.
    #[test]
    fn pop_block_wakes_a_consumer_that_moved_threads() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        let first = std::thread::spawn(move || {
            let mut c = c;
            assert_eq!(c.pop_block(), Some(1));
            c
        });
        // Long enough that the first consumer exhausts its spin budget
        // and genuinely parks, installing its handle.
        std::thread::sleep(std::time::Duration::from_millis(50));
        p.push(1).unwrap();
        let c = first.join().unwrap();

        let second = std::thread::spawn(move || {
            let mut c = c;
            c.pop_block()
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        p.push(2).unwrap();
        assert_eq!(second.join().unwrap(), Some(2));
    }

    /// More producers than `PARK_SLOTS`, so at least two share a park
    /// slot and can be parked on it at the same time. Every producer
    /// must eventually complete.
    #[test]
    fn push_block_completes_with_more_producers_than_park_slots() {
        use crate::common::park::PARK_SLOTS;

        const EXTRA: usize = 8;
        let n_producers = PARK_SLOTS + EXTRA;
        let (p, mut c) = RingBuffer::<usize>::new(Capacity::exact(2)).split();

        let handles: Vec<_> = (0..n_producers)
            .map(|i| {
                let p = p.clone();
                std::thread::spawn(move || p.push_block(i))
            })
            .collect();
        drop(p);

        let mut got = 0usize;
        while got < n_producers {
            if c.pop().is_some() {
                got += 1;
            } else {
                std::thread::yield_now();
            }
        }
        for h in handles {
            assert_eq!(h.join().unwrap(), Ok(()));
        }
    }

    /// Creating and dropping producers past `PARK_SLOTS` times must not
    /// alias a live blocking producer's slot. A monotonic counter wraps
    /// on total handles ever made, not on how many are alive, so a
    /// long-lived waiter eventually shares its bit with a newcomer.
    #[test]
    fn park_slots_do_not_alias_after_churning_producers() {
        use crate::common::park::PARK_SLOTS;

        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(2)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();

        for _ in 0..(PARK_SLOTS * 2) {
            drop(p.clone());
        }

        let blocked = p.clone();
        let h = std::thread::spawn(move || blocked.push_block(3));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(c.pop(), Some(1));
        assert_eq!(h.join().unwrap(), Ok(()));
        drop(p);
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
        let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
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

    /// One round of concurrent [`Producer::push`] calls on a ring that
    /// no consumer drains.
    ///
    /// A [`std::sync::Barrier`] releases every producer at the same
    /// moment, which is what makes them race for the same slot.
    /// Producers that spawn one after another do not overlap, and the
    /// overclaim never happens.
    struct FullRingProbe {
        capacity: usize,
        producers: usize,
    }

    /// Result of one [`FullRingProbe`] round.
    struct ProbeOutcome {
        stuck: usize,
        accepted: usize,
    }

    impl FullRingProbe {
        const fn new(capacity: usize, producers: usize) -> Self {
            Self {
                capacity,
                producers,
            }
        }

        /// Runs one round and waits up to `deadline` for the producers
        /// to return.
        ///
        /// The threads are detached on purpose. A producer stuck
        /// inside `push` never returns, so a join would hang the test
        /// instead of failing it. The counters report the outcome.
        fn run(&self, deadline: std::time::Duration) -> ProbeOutcome {
            use std::sync::atomic::{AtomicUsize, Ordering};
            use std::sync::{Arc, Barrier};

            let (producer, _consumer) =
                RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();
            let gate = Arc::new(Barrier::new(self.producers));
            let returned = Arc::new(AtomicUsize::new(0));
            let accepted = Arc::new(AtomicUsize::new(0));

            for id in 0..self.producers {
                let handle = producer.clone();
                let gate = Arc::clone(&gate);
                let returned = Arc::clone(&returned);
                let accepted = Arc::clone(&accepted);
                std::thread::spawn(move || {
                    gate.wait();
                    if handle.push(id as u64).is_ok() {
                        accepted.fetch_add(1, Ordering::Relaxed);
                    }
                    returned.fetch_add(1, Ordering::Relaxed);
                });
            }
            drop(producer);

            let start = std::time::Instant::now();
            while start.elapsed() < deadline && returned.load(Ordering::Relaxed) != self.producers {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }

            ProbeOutcome {
                stuck: self.producers - returned.load(Ordering::Relaxed),
                accepted: accepted.load(Ordering::Relaxed),
            }
        }
    }

    /// [`Producer::push`] must return on a full ring instead of
    /// waiting for a consumer.
    ///
    /// `push` is documented non-blocking: it returns `Err(val)` when
    /// the ring is full. Only `push_block` may wait.
    ///
    /// Test design: start more producers than the ring can hold, gate
    /// them on a barrier, and never pop. Every producer must return,
    /// and the ring must accept no more values than its capacity.
    ///
    /// The former bug: `claim_slot` checked fullness and then advanced
    /// `tail` with a separate fetch-and-add. Other producers could move
    /// `tail` between those steps. A producer then waited for a full slot.
    ///
    /// Measured overclaim rate on the unfixed code: 0% at 1-2
    /// producers, 5.4% at 4, 42.9% at 8, 63.7% at 16. The race needs
    /// several rounds and several shapes to show up reliably, so this
    /// test runs a small matrix.
    #[test]
    #[cfg_attr(miri, ignore = "too slow for Miri: 16 spinning producers")]
    fn push_returns_on_full_ring_without_consumer() {
        const ROUNDS: usize = 8;
        const SHAPES: [(usize, usize); 5] = [(1, 8), (1, 16), (2, 16), (4, 16), (16, 32)];

        for round in 0..ROUNDS {
            for (capacity, producers) in SHAPES {
                let outcome =
                    FullRingProbe::new(capacity, producers).run(std::time::Duration::from_secs(2));

                assert_eq!(
                    outcome.stuck, 0,
                    "round {round}, capacity {capacity}, {producers} producers: \
                     {} producer(s) never returned from push(). push() is \
                     non-blocking and must return Err(val) when the ring is full",
                    outcome.stuck
                );
                assert!(
                    outcome.accepted <= capacity,
                    "round {round}, capacity {capacity}, {producers} producers: \
                     push() accepted {} values into {capacity} slot(s)",
                    outcome.accepted
                );
                assert!(
                    outcome.accepted > 0,
                    "round {round}, capacity {capacity}, {producers} producers: \
                     an empty ring accepted no values at all"
                );
            }
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

    // -----------------------------------------------------------------------
    // Async API
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(feature = "async")]
    #[cfg_attr(miri, ignore = "too slow for Miri: 10k items through a 4-slot ring")]
    fn async_push_pop_cross_thread() {
        // Multiple producers and one consumer on separate threads. Watchdog
        // aborts the process if anything deadlocks within 5s so the test
        // fails fast.
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::Arc;

        let done = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(AtomicU64::new(0));
        let watchdog = crate::common::spawn_progress_watchdog(
            progress.clone(),
            done.clone(),
            "mpsc async_push_pop_cross_thread",
        );

        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n_producers: u64 = 4;
        let per_producer: u64 = 2_500;
        let total = n_producers * per_producer;

        let producer_threads: Vec<_> = (0..n_producers)
            .map(|tid| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(async move {
                        for i in 0..per_producer {
                            p.push_async(tid * per_producer + i)
                                .await
                                .expect("consumer dropped");
                        }
                    }));
                })
            })
            .collect();
        drop(producer);

        let progress_c = progress;
        let ch = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                let mut received = 0u64;
                while received < total {
                    match consumer.pop_async().await {
                        Some(_) => {
                            received += 1;
                            progress_c.fetch_add(1, Ordering::Relaxed);
                        }
                        None => break,
                    }
                }
                assert_eq!(received, total);
            }));
        });

        for h in producer_threads {
            h.join().unwrap();
        }
        ch.join().unwrap();
        done.store(true, Ordering::Release);
        watchdog.join().unwrap();
    }

    /// A `Waker` is user code and may panic. `commit` publishes the
    /// slot and only then wakes, so an unwind out of the wake path must
    /// not run the guard's rollback — that would drop a value the
    /// consumer already owns.
    #[test]
    #[cfg(feature = "async")]
    fn panicking_waker_during_commit_does_not_drop_published_value() {
        use std::task::{Context, RawWaker, RawWakerVTable, Waker};

        struct PanicWaker;

        impl PanicWaker {
            const VTABLE: RawWakerVTable = RawWakerVTable::new(
                |_| RawWaker::new(std::ptr::null(), &Self::VTABLE),
                |_| panic!("waker failure"),
                |_| panic!("waker failure"),
                |_| {},
            );

            fn waker() -> Waker {
                // SAFETY: the vtable's functions are consistent with a
                // stateless waker carrying a null data pointer.
                unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &Self::VTABLE)) }
            }
        }

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ring = RingBuffer::<crate::common::DropCounter>::new(Capacity::exact(4));

        let waker = PanicWaker::waker();
        ring.consumer_waker.register(
            crate::common::park_registry::ParkSlot::SOLE,
            &Context::from_waker(&waker),
        );

        let (mut producer, mut consumer) = ring.split();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let w = producer.reserve().unwrap();
            w.write(crate::common::DropCounter {
                counter: std::sync::Arc::clone(&counter),
            })
            .commit();
        }));
        assert!(result.is_err(), "the waker panic must propagate");

        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the committed value must survive the wake-path unwind"
        );
        assert!(
            consumer.pop().is_some(),
            "the committed value must still reach the consumer"
        );
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the value is dropped exactly once, by its consumer"
        );
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_push_pop_cross_thread_small() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(2)).split();
        let n_producers: u64 = 2;
        let per_producer: u64 = 3;
        let total = n_producers * per_producer;

        let producer_threads: Vec<_> = (0..n_producers)
            .map(|tid| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(async move {
                        for i in 0..per_producer {
                            p.push_async(tid * per_producer + i)
                                .await
                                .expect("consumer dropped");
                        }
                    }));
                })
            })
            .collect();
        drop(producer);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let received = rt.block_on(local.run_until(async move {
            let mut seen = Vec::new();
            while (seen.len() as u64) < total {
                match consumer.pop_async().await {
                    Some(v) => seen.push(v),
                    None => break,
                }
            }
            seen
        }));

        for h in producer_threads {
            h.join().unwrap();
        }

        let mut sorted = received;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..total).collect::<Vec<_>>());
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_pop_returns_none_on_close() {
        // Producer drops without pushing; pop_async should resolve to None.
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(producer);
        });
        rt.block_on(local.run_until(async move {
            assert_eq!(consumer.pop_async().await, None);
        }));
        h.join().unwrap();
    }

    #[test]
    #[cfg(feature = "async")]
    fn async_push_returns_err_on_consumer_close() {
        // Consumer drops; a parked async push should resolve to Err(val).
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

    /// A `push_async` that is cancelled must not leave its
    /// registration behind. This is the timeout-loop pattern reduced
    /// to its essentials: park the future once, then drop it, with no
    /// consumer to drain what the park registered.
    ///
    /// The producer here is slotless, which is the case that
    /// accumulates without bound — a leased slot holds one waker and
    /// overwrites it, while the overflow keeps every registration
    /// until something drains it.
    #[test]
    #[cfg(feature = "async")]
    fn cancelled_pushes_do_not_accumulate_registrations() {
        use std::future::Future;
        use std::sync::Arc;
        use std::task::{Context, Poll, Wake, Waker};

        struct Noop;
        // `Waker::noop()` would not do here: every noop waker is
        // `will_wake`-identical to every other, which would collapse the
        // distinct tasks this test needs.
        #[allow(clippy::manual_noop_waker)]
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }

        let ring = RingBuffer::<u64>::new(Capacity::exact(2));
        let (producer, _consumer) = ring.split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        for _ in 0..1_000 {
            // A fresh waker each round: distinct tasks poll distinct
            // futures, so a shared slot displaces rather than matching
            // `will_wake`.
            let waker = Waker::from(Arc::new(Noop));
            let mut cx = Context::from_waker(&waker);
            let mut future = Box::pin(producer.push_async(99));
            assert!(
                matches!(future.as_mut().poll(&mut cx), Poll::Pending),
                "the ring must stay full"
            );
        }

        let depth = producer.overflow_depth();
        assert!(
            depth <= 1,
            "1000 cancelled pushes left {depth} registrations behind"
        );
    }

    /// Two `push_async` futures from *one* producer handle, driven by
    /// separate tasks with separate wakers. `push_async` takes `&self`,
    /// so this is reachable from safe code, and both futures park on
    /// the handle's single slot: the second registration displaces and
    /// drops the first waker, stranding that future.
    #[test]
    #[cfg(feature = "async")]
    fn two_async_pushes_from_one_handle_both_complete() {
        use std::rc::Rc;
        use std::time::{Duration, Instant};

        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(2)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let h = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                let producer = Rc::new(producer);
                let first = {
                    let p = Rc::clone(&producer);
                    tokio::task::spawn_local(async move { p.push_async(10).await })
                };
                let second = {
                    let p = Rc::clone(&producer);
                    tokio::task::spawn_local(async move { p.push_async(11).await })
                };
                first.await.unwrap().unwrap();
                second.await.unwrap().unwrap();
            }));
            let _ = done_tx.send(());
        });

        // Let both futures exhaust their fast path and park before any
        // space appears, so each must be woken to make progress.
        std::thread::sleep(Duration::from_millis(100));

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = 0;
        while got < 4 && Instant::now() < deadline {
            if consumer.pop().is_some() {
                got += 1;
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert_eq!(got, 4, "a queued value never became visible");

        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a push_async future was stranded");
        h.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Borrowed split (non-'static T)
    // -----------------------------------------------------------------------

    #[test]
    fn borrowed_split_push_pop() {
        let mut ring = RingBuffer::<u32>::new(Capacity::exact(4));
        let (producer, mut consumer) = ring.split_borrowed();

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
        let (producer, mut consumer) = ring.split_borrowed();

        producer.push(&wire_buf[0..2]).unwrap();
        producer.push(&wire_buf[2..4]).unwrap();

        assert_eq!(consumer.pop().unwrap(), &[10, 20]);
        assert_eq!(consumer.pop().unwrap(), &[30, 40]);
    }

    #[test]
    fn borrowed_split_multiple_producers() {
        let mut ring = RingBuffer::<u32>::new(Capacity::exact(8));
        let (p1, mut consumer) = ring.split_borrowed();
        let p2 = p1.new_producer();

        std::thread::scope(|s| {
            s.spawn(move || {
                for i in 0..4u32 {
                    while p1.push(i).is_err() {
                        std::thread::yield_now();
                    }
                }
            });
            s.spawn(move || {
                for i in 100..104u32 {
                    while p2.push(i).is_err() {
                        std::thread::yield_now();
                    }
                }
            });
            s.spawn(move || {
                let mut got = Vec::new();
                while got.len() < 8 {
                    if let Some(v) = consumer.pop() {
                        got.push(v);
                    } else {
                        std::thread::yield_now();
                    }
                }
                got.sort_unstable();
                assert_eq!(got, vec![0, 1, 2, 3, 100, 101, 102, 103]);
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

        let wire = vec![0xBB; 128];
        let mut ring = RingBuffer::<Req<'_>>::new(Capacity::exact(4));
        let (producer, mut consumer) = ring.split_borrowed();
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
