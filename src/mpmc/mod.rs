//! Multi-producer, multi-consumer ring with relaxed-FIFO ordering.
//!
//! Producers reserve batches of logical positions via FAA on a shared
//! `claim` cursor, then publish out-of-order into whichever slot in
//! their batch becomes free first. Consumers scan the ring from a
//! private cursor and CAS-claim the first published slot they see.
//! No shared consumer-side cursor exists — contention is distributed
//! across `cap` per-slot atomics rather than concentrated on a single
//! `head` line.
//!
//! # Ordering guarantees
//!
//! - **No FIFO** across producers, across consumers, or even within
//!   a single producer's own stream. Items are returned in the order
//!   their slots become published (which depends on consumer release
//!   order, scheduling, and contention).
//! - Use [`spsc`](crate::spsc) or [`spmc`](crate::spmc) if strict
//!   ordering matters.
//!
//! # Capacity
//!
//! Requires `cap >= 4` (the per-slot tri-state encoding aliases at
//! `cap < 3`). [`RingBuffer::new`] panics on smaller capacities.
//!
//! # Thread-count guidance
//!
//! Producers fall back to OS-level park (futex-style wake bitmap)
//! when their batch's slots aren't yet released by consumers. This
//! lets the kernel reschedule the core to a runnable consumer
//! instead of burning cycles on a spin loop.
//!
//! Even so, throughput becomes **highly variable** when the total
//! thread count (P producers + Q consumers) saturates the machine.
//! On an N-physical-core SMT machine you have 2N logical CPUs; once
//! P + Q approaches 2N, every spinning thread shares decode
//! bandwidth with its SMT sibling and OS scheduling decisions
//! dominate run-to-run variance.
//!
//! Rules of thumb:
//! - **P + Q ≤ N (physical cores)**: tight, predictable throughput.
//! - **N < P + Q < 2N**: still good, mild SMT-pairing variance.
//! - **P + Q ≈ 2N**: bimodal — peak throughput is high, but worst
//!   case can be 5–10× slower depending on how the scheduler maps
//!   threads onto SMT-paired cores. The futex park/unpark mitigates
//!   this but doesn't eliminate it.
//! - **P + Q > 2N**: oversubscription — threads time-slice on
//!   spin loops and throughput collapses. Avoid this regime.
//!
//! In practice, size your producer + consumer pool to leave at
//! least a few logical CPUs idle for the OS and any background
//! work. Benchmark the specific (P, Q) shape you intend to deploy;
//! mismatched shapes (e.g. P=8, Q=4) often outperform balanced
//! ones at the same total thread count because there's scheduling
//! slack.

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

/// Tunable parameters baked into a [`RingBuffer`]'s type. Implement
/// this on a zero-sized type to customize batching / scan behavior
/// at compile time; the default values live in [`DefaultConfig`].
///
/// All values must satisfy:
/// - `PRODUCER_BATCH` in `1..=32` (bounded by the producer's
///   `u32` per-batch bitmap).
/// - `CAS_FAIL_SKIP > 0` and `CONSUMED_FLUSH > 0`.
///
/// Bounds are checked at monomorphization via `const _ = assert!`.
pub trait Config: 'static {
    /// Positions reserved per FAA on `claim`. Caps the per-batch
    /// `u32` bitmap; must be in `1..=32`. Larger values reduce
    /// `claim`-line traffic at the cost of producer-progress
    /// imbalance (a slow producer holds more positions out of
    /// reach of others).
    const PRODUCER_BATCH: usize;

    /// On CAS-failure during `pop`, advance the scan cursor by
    /// this many slots before retrying. A larger skip dramatically
    /// reduces CAS re-collisions because the contended slot's
    /// cacheline stays hot for hundreds of cycles. Empirical
    /// peaks land near 64–128 at `cap=1024`.
    const CAS_FAIL_SKIP: usize;

    /// Pops between flushes of each consumer's local count to the
    /// shared `consumed` watermark. Producers use the watermark
    /// to bound their batch FAA, so a stale flush only forces
    /// smaller producer batches (safe direction). Larger values
    /// reduce hot-line traffic on `consumed` at the cost of more
    /// pessimistic producer batching.
    const CONSUMED_FLUSH: usize;
}

/// Default tuning for [`RingBuffer`].
///
/// `PRODUCER_BATCH = 32`, `CAS_FAIL_SKIP = 128`,
/// `CONSUMED_FLUSH = 64`. These were chosen by sweeping at
/// `cap = 1024` across `(P, Q)` shapes from `(2, 2)` through
/// `(8, 8)`; see the empirical table in [`Config::CAS_FAIL_SKIP`]
/// docs.
#[derive(Debug, Clone, Copy)]
pub struct DefaultConfig;

impl Config for DefaultConfig {
    const PRODUCER_BATCH: usize = 32;
    const CAS_FAIL_SKIP: usize = 128;
    const CONSUMED_FLUSH: usize = 64;
}

/// Inline-customization helper. Lets callers tune a ring without
/// declaring a named config type:
///
/// ```ignore
/// use quetzalcoatl::mpmc::{Cfg, RingBuffer};
/// let (p, c) = RingBuffer::<u64, Cfg<16, 64, 32>>::new(cap).split();
/// ```
///
/// Type parameters: `<PRODUCER_BATCH, CAS_FAIL_SKIP, CONSUMED_FLUSH>`.
#[derive(Debug, Clone, Copy)]
pub struct Cfg<const B: usize, const S: usize, const F: usize>;

impl<const B: usize, const S: usize, const F: usize> Config for Cfg<B, S, F> {
    const PRODUCER_BATCH: usize = B;
    const CAS_FAIL_SKIP: usize = S;
    const CONSUMED_FLUSH: usize = F;
}

#[repr(C)]
pub struct RingBuffer<T, C: Config = DefaultConfig> {
    pub(crate) data: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    /// Per-slot tri-state with round encoding (free / published /
    /// claimed). Packed 8 per cacheline (not `CachePadded`) so a
    /// consumer scan amortizes one line load across 8 slots.
    pub(crate) ready: AlignedBuf<AtomicUsize>,
    /// Per-slot "consumer released" marker. Producer for logical
    /// `pos` waits on `done[s] == pos`; consumer for `pos` stores
    /// `done[s] = pos + cap`.
    pub(crate) done: AlignedBuf<AtomicUsize>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    /// Producer claim cursor. FAA'd to reserve batches of positions.
    pub(crate) claim: CachePadded<AtomicUsize>,
    /// Coarse "items consumed" watermark — a lower bound, flushed
    /// from per-consumer locals every `CONSUMED_FLUSH` pops.
    /// Producers use it to bound their per-batch FAA on `claim`.
    pub(crate) consumed: CachePadded<AtomicUsize>,
    /// Live producer count; last-drop sets `producer_closed`.
    pub(crate) producer_count: CachePadded<AtomicUsize>,
    /// Live consumer count; last-drop sets `consumer_closed`.
    pub(crate) consumer_count_live: CachePadded<AtomicUsize>,
    /// Set by the last `Consumer` drop. `Producer::push_block`
    /// observes this and returns `Err(val)` instead of hanging
    /// (no consumer left to drain).
    pub(crate) consumer_closed: CachePadded<AtomicBool>,
    /// Monotonic counter, bumped on each `Consumer::clone`. Used
    /// to stagger new consumers' starting scan offsets.
    pub(crate) clone_counter: CachePadded<AtomicUsize>,
    pub(crate) closed: CachePadded<AtomicBool>,
    /// Producer-side park state. Bit `i` of `producer_park.wake` is
    /// set ↔ a producer in slot `i` is parked waiting for a `done[s]`
    /// release. Consumers wake one parked producer after each release
    /// (gated on the bitmap being non-zero).
    pub(crate) producer_park: WakeSet,
    /// Consumer-side park state. Bit `i` of `consumer_park.wake` is
    /// set ↔ a consumer in slot `i` is parked waiting for any
    /// `ready[s]` publish. Producers wake one parked consumer after
    /// each publish (gated on the bitmap being non-zero).
    pub(crate) consumer_park: WakeSet,
    /// Monotonic counter for assigning stable park-slot indices to
    /// `Consumer` clones. Bumped at clone time only.
    pub(crate) consumer_count: CachePadded<AtomicUsize>,
    /// Async equivalent of `producer_park`: per-producer-slot wakers
    /// registered from `Poll::Pending` in `push_async`. Woken by any
    /// consumer after a successful pop / commit / batched drain.
    #[cfg(feature = "async")]
    pub(crate) producer_waker: WakerSet,
    /// Async equivalent of `consumer_park`: per-consumer-slot wakers
    /// registered from `Poll::Pending` in `pop_async`. Woken by any
    /// producer after a successful push / commit.
    #[cfg(feature = "async")]
    pub(crate) consumer_waker: WakerSet,
    pub(crate) _config: std::marker::PhantomData<fn() -> C>,
}

// SAFETY: All shared state is atomic; per-slot CAS gives unique
// ownership of each (slot, round) pair to exactly one consumer.
// `C` is a ZST type marker — its Send/Sync are irrelevant.
unsafe impl<T: Send, C: Config> Send for RingBuffer<T, C> {}
unsafe impl<T: Send, C: Config> Sync for RingBuffer<T, C> {}

/// Compile-time-validated bounds for a [`Config`]. Forces a
/// monomorphization-time error when `C` violates one of:
/// - `PRODUCER_BATCH` in `1..=32` (capped by the producer's `u32`
///   per-batch bitmap),
/// - `CAS_FAIL_SKIP > 0`,
/// - `CONSUMED_FLUSH > 0`.
struct ConfigBounds<C: Config>(std::marker::PhantomData<C>);

impl<C: Config> ConfigBounds<C> {
    const VALIDATE: () = {
        assert!(
            C::PRODUCER_BATCH >= 1 && C::PRODUCER_BATCH <= 32,
            "Config::PRODUCER_BATCH must be in 1..=32 (bounded by the u32 per-batch bitmap)",
        );
        assert!(C::CAS_FAIL_SKIP >= 1, "Config::CAS_FAIL_SKIP must be >= 1",);
        assert!(
            C::CONSUMED_FLUSH >= 1,
            "Config::CONSUMED_FLUSH must be >= 1",
        );
    };
}

impl<T, C: Config> RingBuffer<T, C> {
    /// Creates a new MPMC ring buffer.
    ///
    /// # Panics
    ///
    /// Panics if `capacity.get() < 4`. The per-slot tri-state
    /// encoding aliases at smaller capacities.
    ///
    /// `C`'s tunables are validated at monomorphization via
    /// [`ConfigBounds`]: `PRODUCER_BATCH` must be in `1..=32`,
    /// the others must be `>= 1`.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        // Force monomorphization-time evaluation of the bounds.
        let () = ConfigBounds::<C>::VALIDATE;
        let cap = capacity.get();
        assert!(cap >= 4, "mpmc requires capacity >= 4");
        let data = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        // ready[s] = s → free for round 0 at logical pos s.
        let mut ridx = 0usize;
        let ready = AlignedBuf::new_with(cap, || {
            let r = AtomicUsize::new(ridx);
            ridx += 1;
            r
        });
        // done[s] = s → previous round (-1) released this slot, so
        // the round-0 producer's check `done[s] == s` holds.
        let mut didx = 0usize;
        let done = AlignedBuf::new_with(cap, || {
            let d = AtomicUsize::new(didx);
            didx += 1;
            d
        });

        Self {
            data,
            ready,
            done,
            claim: CachePadded(AtomicUsize::new(0)),
            consumed: CachePadded(AtomicUsize::new(0)),
            producer_count: CachePadded(AtomicUsize::new(1)),
            consumer_count_live: CachePadded(AtomicUsize::new(1)),
            clone_counter: CachePadded(AtomicUsize::new(0)),
            closed: CachePadded(AtomicBool::new(false)),
            consumer_closed: CachePadded(AtomicBool::new(false)),
            cap,
            mask: capacity.mask,
            producer_park: WakeSet::new(),
            consumer_park: WakeSet::new(),
            consumer_count: CachePadded(AtomicUsize::new(0)),
            #[cfg(feature = "async")]
            producer_waker: WakerSet::new(),
            #[cfg(feature = "async")]
            consumer_waker: WakerSet::new(),
            _config: std::marker::PhantomData,
        }
    }

    /// Wakes one async producer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_producer_async(&self) {
        self.producer_waker.wake_one();
    }

    /// Wakes up to `n` async producer tasks, if any. Self-gates.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_producer_async_n(&self, n: usize) {
        self.producer_waker.wake_n(n);
    }

    /// Wakes one async consumer task, if any. Self-gates on `pending`.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn wake_consumer_async(&self) {
        self.consumer_waker.wake_one();
    }

    /// Wakes up to `n` async consumer tasks, if any. Self-gates.
    #[cfg(feature = "async")]
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn wake_consumer_async_n(&self, n: usize) {
        self.consumer_waker.wake_n(n);
    }

    #[inline]
    pub(crate) fn data_slot(&self, pos: usize) -> &UnsafeCell<MaybeUninit<T>> {
        let idx = pos & self.mask;
        // SAFETY: mask = cap - 1.
        unsafe { std::hint::assert_unchecked(idx < self.data.len()) };
        &self.data[idx]
    }

    #[inline]
    pub(crate) fn ready_slot(&self, pos: usize) -> &AtomicUsize {
        let idx = pos & self.mask;
        unsafe { std::hint::assert_unchecked(idx < self.ready.len()) };
        &self.ready[idx]
    }

    #[inline]
    pub(crate) fn done_slot(&self, pos: usize) -> &AtomicUsize {
        let idx = pos & self.mask;
        unsafe { std::hint::assert_unchecked(idx < self.done.len()) };
        &self.done[idx]
    }

    /// Diagnostic snapshot: `(claim, ready[..], done[..])`.
    #[doc(hidden)]
    pub fn debug_snapshot(&self) -> (usize, Vec<usize>, Vec<usize>) {
        let claim = self.claim.load(Ordering::Acquire);
        let r: Vec<usize> = (0..self.cap)
            .map(|s| self.ready[s].load(Ordering::Acquire))
            .collect();
        let d: Vec<usize> = (0..self.cap)
            .map(|s| self.done[s].load(Ordering::Acquire))
            .collect();
        (claim, r, d)
    }

    /// Scans the bits in `unused` for a slot whose `done[s]`
    /// indicates it is free for the current round. Returns
    /// `(bit, pos)` of the first free slot, or `None` if none are
    /// ready.
    #[inline]
    pub(crate) fn scan_unused(&self, start: usize, unused: u32) -> Option<(u32, usize)> {
        let mut bits = unused;
        while bits != 0 {
            let b = bits.trailing_zeros();
            let p = start + b as usize;
            if self.done_slot(p).load(Ordering::Acquire) == p {
                return Some((b, p));
            }
            bits &= bits - 1;
        }
        None
    }

    /// Splits the ring into a [`Producer`] and a [`Consumer`].
    /// Both handles are cloneable for additional producer/consumer
    /// threads.
    #[must_use]
    pub fn split(self) -> (Producer<T, C>, Consumer<T, C>) {
        let arc = Arc::new(self);
        let producer = Producer::new(arc.clone());
        let consumer = Consumer::new(arc);
        (producer, consumer)
    }
}

impl<T, C: Config> Drop for RingBuffer<T, C> {
    fn drop(&mut self) {
        // All Producer/Consumer handles are gone (we hold &mut self),
        // so we can read state non-atomically. A slot holds live
        // data when ready[s] is `published` (state 1) or `claimed
        // but not released` (state 2 with done[s] != round_pos+cap).
        let cap = self.cap;
        for s in 0..cap {
            let r = *self.ready[s].get_mut();
            let d = *self.done[s].get_mut();
            let delta = r.wrapping_sub(s);
            let state = delta & self.mask;
            let round = delta / cap;
            let round_pos = s + round * cap;
            if state == 1 {
                // SAFETY: published, never claimed → initialized,
                // never moved.
                unsafe {
                    self.data[s].get().cast::<T>().drop_in_place();
                }
            } else if state == 2 && d != round_pos + cap {
                // SAFETY: claimed but consumer never released
                // done[s], so the value is still in the slot.
                unsafe {
                    self.data[s].get().cast::<T>().drop_in_place();
                }
            }
        }
        // OnceLock<Thread> entries in `producer_parkers` clean themselves up.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::Capacity;

    #[test]
    #[should_panic(expected = "mpmc requires capacity >= 4")]
    fn rejects_small_capacity() {
        let _ = RingBuffer::<u8>::new(Capacity::exact(2));
    }

    #[test]
    fn basic_push_pop() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        let mut got = vec![c.pop().unwrap(), c.pop().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn fill_and_drain() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        for i in 0..8 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());
        let mut got: Vec<u32> = (0..8).map(|_| c.pop().unwrap()).collect();
        got.sort_unstable();
        assert_eq!(got, (0..8).collect::<Vec<_>>());
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn wraparound() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for lap in 0..3u32 {
            for i in 0..4 {
                p.push(lap * 4 + i).unwrap();
            }
            let mut got: Vec<u32> = (0..4).map(|_| c.pop().unwrap()).collect();
            got.sort_unstable();
            assert_eq!(got, (lap * 4..lap * 4 + 4).collect::<Vec<_>>());
        }
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn empty_returns_none() {
        let (_p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn pop_block_wakes_on_push() {
        // Producer publishes after consumer is parked; consumer
        // must wake and observe the value.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        p.push(7).unwrap();
        assert_eq!(h.join().unwrap(), Some(7));
    }

    #[test]
    fn pop_block_returns_none_on_close() {
        // After all producers drop and the queue is empty,
        // pop_block must return None instead of hanging.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let h = std::thread::spawn(move || c.pop_block());
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(p);
        assert_eq!(h.join().unwrap(), None);
    }

    #[test]
    fn push_block_unblocks_on_pop() {
        // Fill the ring, then verify push_block waits for a pop
        // and then succeeds.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
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
        // Drop the consumer; push_block should observe close and
        // return Err, returning the value back to the caller.
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
        // Items pushed before the producer drops must still be
        // returned, even when the producer drops between push
        // and pop_block.
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        p.push(1).unwrap();
        p.push(2).unwrap();
        drop(p);
        let mut got = vec![c.pop_block().unwrap(), c.pop_block().unwrap()];
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
        assert_eq!(c.pop_block(), None);
    }

    #[test]
    fn concurrent_two_producers_two_consumers() {
        let total = 16usize;
        let (p, c) = RingBuffer::<u64>::new(Capacity::exact(8)).split();
        let p2 = p.clone();
        let c2 = c.clone();
        let recv = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let r1 = recv.clone();
        let h1 = std::thread::spawn(move || {
            while r1.load(std::sync::atomic::Ordering::Relaxed) < total {
                if c.pop().is_some() {
                    r1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }
        });
        let r2 = recv.clone();
        let h2 = std::thread::spawn(move || {
            while r2.load(std::sync::atomic::Ordering::Relaxed) < total {
                if c2.pop().is_some() {
                    r2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }
        });

        let h3 = std::thread::spawn(move || {
            for i in 0..(total / 2) as u64 {
                while p.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });
        let h4 = std::thread::spawn(move || {
            for i in 0..(total / 2) as u64 {
                while p2.push(100 + i).is_err() {
                    std::thread::yield_now();
                }
            }
        });
        h3.join().unwrap();
        h4.join().unwrap();
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(recv.load(std::sync::atomic::Ordering::Relaxed), total);
    }

    use crate::common::DropCounter;

    #[test]
    fn drop_left_in_buffer() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (p, _c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
            for _ in 0..4 {
                p.push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
            }
            // _c dropped first, then p, then RingBuffer.
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[test]
    fn drop_after_partial_drain() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (p, c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
            for _ in 0..4 {
                p.push(DropCounter {
                    counter: counter.clone(),
                })
                .unwrap();
            }
            drop(c.pop().unwrap());
            drop(c.pop().unwrap());
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[test]
    fn many_wraps_single_threaded() {
        let (p, c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        for batch in 0..8 {
            let base = batch * 4;
            for i in 0..4 {
                p.push(base + i).unwrap();
            }
            for i in 0..4 {
                let v = c.pop().unwrap();
                assert_eq!(v, base + i, "batch {batch} item {i}");
            }
        }
    }

    #[test]
    #[ignore = "stress test — slow under Miri"]
    fn stress_mpmc() {
        let total: u64 = 4000;
        let p_count: u64 = 4;
        let c_count: u64 = 4;
        let per_p = total / p_count;
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let p = prod.clone();
                std::thread::spawn(move || {
                    for i in 0..per_p {
                        while p.push(tid * per_p + i).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        let cc: Vec<_> = (0..c_count)
            .map(|_| {
                let c = cons.clone();
                let r = received.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    while r.load(std::sync::atomic::Ordering::Relaxed)
                        < usize::try_from(total).unwrap()
                    {
                        if let Some(v) = c.pop() {
                            r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            got.push(v);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), usize::try_from(total).unwrap());
    }

    /// Run a full correctness check for a given (P, Q, total, cap)
    /// shape. Each producer pushes a disjoint contiguous range of
    /// values; consumers collect everything; we verify the union
    /// equals exactly the union of all producer ranges (no
    /// duplicates, no gaps, no extras). Producers wait for the ring
    /// to make space via busy-loop on Err.
    fn run_correctness(p_count: u64, c_count: u64, total: u64, cap: usize) {
        let per_p = total / p_count;
        let actual_total = per_p * p_count;
        let actual_total_us = usize::try_from(actual_total).unwrap();
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let p = prod.clone();
                std::thread::spawn(move || {
                    let base = tid * per_p;
                    for i in 0..per_p {
                        let v = base + i;
                        while p.push(v).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        let cc: Vec<_> = (0..c_count)
            .map(|_| {
                let c = cons.clone();
                let r = received.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    while r.load(std::sync::atomic::Ordering::Relaxed) < actual_total_us {
                        if let Some(v) = c.pop() {
                            r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            got.push(v);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        // Exact-count check: no duplicates, no losses, no extras.
        assert_eq!(
            all.len(),
            actual_total_us,
            "P={p_count} Q={c_count}: got {} items, expected {}",
            all.len(),
            actual_total
        );
        all.sort_unstable();
        let mut prev: Option<u64> = None;
        for v in &all {
            if let Some(p) = prev {
                assert_ne!(p, *v, "duplicate value {v} (P={p_count} Q={c_count})");
            }
            prev = Some(*v);
        }
        // The set of values is exactly [0, actual_total).
        for (i, v) in all.iter().enumerate() {
            assert_eq!(
                *v, i as u64,
                "missing or unexpected value at sorted index {i} (P={p_count} Q={c_count})"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Matrix correctness tests across P x Q including mismatched shapes.
    // Each test verifies: no items lost, no duplicates, exact value set.
    // Sized to be reasonable for a regular `cargo test` run; the stress
    // module above takes 4000 items, these take 10000.
    // -----------------------------------------------------------------------

    macro_rules! correctness_test {
        ($name:ident, $p:expr, $q:expr) => {
            #[test]
            #[ignore = "matrix correctness — too slow for default run"]
            fn $name() {
                run_correctness($p, $q, 10_000, 1024);
            }
        };
    }

    correctness_test!(matrix_p1_q1, 1, 1);
    correctness_test!(matrix_p1_q2, 1, 2);
    correctness_test!(matrix_p1_q4, 1, 4);
    correctness_test!(matrix_p1_q8, 1, 8);
    correctness_test!(matrix_p2_q1, 2, 1);
    correctness_test!(matrix_p2_q2, 2, 2);
    correctness_test!(matrix_p2_q4, 2, 4);
    correctness_test!(matrix_p2_q8, 2, 8);
    correctness_test!(matrix_p4_q1, 4, 1);
    correctness_test!(matrix_p4_q2, 4, 2);
    correctness_test!(matrix_p4_q4, 4, 4);
    correctness_test!(matrix_p4_q8, 4, 8);
    correctness_test!(matrix_p8_q1, 8, 1);
    correctness_test!(matrix_p8_q2, 8, 2);
    correctness_test!(matrix_p8_q4, 8, 4);
    correctness_test!(matrix_p8_q8, 8, 8);

    // Larger items-per-producer to catch infrequent races. ~4M items
    // total, runs in ~1s.
    macro_rules! heavy_correctness_test {
        ($name:ident, $p:expr, $q:expr) => {
            #[test]
            #[ignore = "heavy correctness — minutes-scale runtime"]
            fn $name() {
                run_correctness($p, $q, 1_000_000, 1024);
            }
        };
    }

    heavy_correctness_test!(heavy_p2_q2, 2, 2);
    heavy_correctness_test!(heavy_p4_q4, 4, 4);
    heavy_correctness_test!(heavy_p8_q8, 8, 8);
    heavy_correctness_test!(heavy_p1_q8, 1, 8);
    heavy_correctness_test!(heavy_p8_q1, 8, 1);

    /// Counter-free correctness check: producers push a known set,
    /// drop, consumers drain until `is_closed && pop returns None`.
    /// No shared atomic counter masks throughput, so this stresses
    /// the queue at full speed and exposes any item-loss races.
    fn run_correctness_no_counter(p_count: u64, c_count: u64, total: u64, cap: usize) {
        let per_p = total / p_count;
        let actual_total = usize::try_from(per_p * p_count).unwrap();
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();

        // Consumers drain until producers are dropped AND queue is
        // empty. They collect everything they pop into a local vec.
        let cc: Vec<_> = (0..c_count)
            .map(|_| {
                let c = cons.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    loop {
                        if let Some(v) = c.pop() {
                            got.push(v);
                        } else if c.is_closed() {
                            // Drain remaining items, then exit.
                            while let Some(v) = c.pop() {
                                got.push(v);
                            }
                            break;
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let p = prod.clone();
                std::thread::spawn(move || {
                    let base = tid * per_p;
                    for i in 0..per_p {
                        let v = base + i;
                        while p.push(v).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        assert_eq!(
            all.len(),
            actual_total,
            "P={p_count} Q={c_count}: got {} items, expected {}",
            all.len(),
            actual_total
        );
        all.sort_unstable();
        for (i, v) in all.iter().enumerate() {
            assert_eq!(
                *v, i as u64,
                "missing/extra at sorted index {i} (P={p_count} Q={c_count}): got {v}, expected {i}"
            );
        }
    }

    macro_rules! no_counter_test {
        ($name:ident, $p:expr, $q:expr, $total:expr) => {
            #[test]
            #[ignore = "counter-free correctness — minute-scale runtime"]
            fn $name() {
                run_correctness_no_counter($p, $q, $total, 1024);
            }
        };
    }

    no_counter_test!(no_counter_p2_q2, 2, 2, 5_000_000);
    no_counter_test!(no_counter_p4_q4, 4, 4, 5_000_000);
    no_counter_test!(no_counter_p8_q8, 8, 8, 5_000_000);
    no_counter_test!(no_counter_p1_q8, 1, 8, 5_000_000);
    no_counter_test!(no_counter_p8_q1, 8, 1, 5_000_000);
    no_counter_test!(no_counter_p2_q4, 2, 4, 5_000_000);
    no_counter_test!(no_counter_p4_q2, 4, 2, 5_000_000);

    // -----------------------------------------------------------------------
    // Zero-copy API
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_write_commit_cycle() {
        let (mut p, c) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let w = p.reserve().unwrap();
        w.write(42).commit();
        assert_eq!(c.pop(), Some(42));
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn reserve_slot_mut_commit_unchecked() {
        let (mut p, c) = RingBuffer::<[u8; 32]>::new(Capacity::exact(4)).split();
        let mut w = p.reserve().unwrap();
        w.slot_mut().write([0xAB; 32]);
        // SAFETY: slot_mut().write initialized the slot.
        unsafe { w.commit_unchecked() };
        let v = c.pop().unwrap();
        assert_eq!(v[0], 0xAB);
        assert_eq!(v[31], 0xAB);
    }

    #[test]
    fn pop_ref_zero_copy_read() {
        let (mut p, mut c) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        p.reserve().unwrap().write(99).commit();
        let r = c.pop_ref().unwrap();
        assert_eq!(*r, 99);
        drop(r);
        assert!(c.pop_ref().is_none());
    }

    #[test]
    fn reserve_drop_without_commit_restores_bit() {
        // Capacity 4, batch size = 32 (DefaultConfig::PRODUCER_BATCH).
        // The first reserve claims a batch starting at pos 0, takes
        // bit 0. Dropping rolls back the bit; a subsequent reserve
        // from the same handle should reuse pos 0 (and write succeeds).
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        {
            let _w = p.reserve().unwrap();
            // _w dropped here without commit — bit must be restored.
        }
        // The next reserve reuses the same batch position.
        p.reserve().unwrap().write(7).commit();
        assert_eq!(c.pop(), Some(7));
    }

    #[test]
    fn reserve_drop_does_not_leak() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (mut p, _c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
            let w = p.reserve().unwrap();
            w.write(DropCounter {
                counter: counter.clone(),
            });
            // WrittenSlot dropped without commit → must drop the value.
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn reserve_uncommitted_then_producer_drop_tombstones() {
        // SlotWriter dropped without commit leaves the bit in
        // batch_unused. Producer::drop must tombstone it so the
        // ring is left in a consistent state (not leaking a slot).
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        {
            let _w = p.reserve().unwrap();
            // SlotWriter dropped without commit; bit restored.
        }
        // Drop the producer — its drop tombstones the unused bits,
        // including the one we restored. Consumer should see closed
        // and observe an empty queue cleanly.
        drop(p);
        assert!(c.is_closed());
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn pop_ref_drop_releases_slot_for_reuse() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        // Fill the ring.
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.push(99).is_err());

        // pop_ref claims a slot but does not release done[s] until
        // drop — the slot is reusable only after the reader drops.
        {
            let _r = c.pop_ref().unwrap();
            // While _r is alive, the producer at the next-round
            // position for this slot should still see done[s] = pos
            // (not pos+cap), so push to that specific position would
            // block. We don't poke at internals; just confirm
            // post-drop reuse works.
        }
        // After drop, the producer can push again into the freed slot.
        p.push(99).unwrap();
        // We can drain everything — order is publish-order, not
        // push-order, so just check the multiset.
        let mut got = Vec::new();
        while let Some(v) = c.pop() {
            got.push(v);
        }
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3, 99]);
    }

    #[test]
    fn pop_ref_drops_value_on_reader_drop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (mut p, mut c) = RingBuffer::<DropCounter>::new(Capacity::exact(4)).split();
        p.reserve()
            .unwrap()
            .write(DropCounter {
                counter: counter.clone(),
            })
            .commit();
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
        {
            let _r = c.pop_ref().unwrap();
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
        }
        // SlotReader::drop must drop the value exactly once.
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn mixed_push_reserve_pop_pop_ref() {
        let (mut p, mut c) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        p.push(1).unwrap();
        p.reserve().unwrap().write(2).commit();
        p.push(3).unwrap();

        // Order is publish order. With a single producer and no
        // contention, that matches push order.
        assert_eq!(c.pop(), Some(1));
        let r = c.pop_ref().unwrap();
        assert_eq!(*r, 2);
        drop(r);
        assert_eq!(c.pop(), Some(3));
        assert_eq!(c.pop(), None);
    }

    #[test]
    #[ignore = "stress test — slow under Miri"]
    fn concurrent_reserve_pop_ref() {
        // Two producers using reserve(), two consumers using pop_ref().
        // Verify item-count and value-set are exactly correct.
        let total: u64 = 4000;
        let p_count: u64 = 2;
        let per_p = total / p_count;
        let (prod, cons) = RingBuffer::<u64>::new(Capacity::exact(64)).split();

        let pp: Vec<_> = (0..p_count)
            .map(|tid| {
                let mut p = prod.clone();
                std::thread::spawn(move || {
                    for i in 0..per_p {
                        loop {
                            if let Some(w) = p.reserve() {
                                w.write(tid * per_p + i).commit();
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(prod);

        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cc: Vec<_> = (0..2)
            .map(|_| {
                let mut c = cons.clone();
                let r = received.clone();
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    while r.load(std::sync::atomic::Ordering::Relaxed)
                        < usize::try_from(total).unwrap()
                    {
                        if let Some(reader) = c.pop_ref() {
                            got.push(*reader);
                            drop(reader);
                            r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    got
                })
            })
            .collect();
        drop(cons);

        for h in pp {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cc {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), usize::try_from(total).unwrap());
        for (i, v) in all.iter().enumerate() {
            assert_eq!(*v, i as u64);
        }
    }

    // -----------------------------------------------------------------------
    // Zero-copy blocking API
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_block_unblocks_on_pop() {
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        // Fill — 4 slots and DefaultConfig::PRODUCER_BATCH=32, so the
        // first push claims a batch of 4 (capped by free space) and
        // commits all 4 in turn. After 4 pushes the next reserve()
        // would need a fresh batch, which the watermark says is empty.
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.reserve().is_none());
        let h = std::thread::spawn(move || {
            let w = p.reserve_block().unwrap();
            w.write(99).commit();
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        // Drain one — wakes the parked producer.
        assert!(c.pop().is_some());
        h.join().unwrap();
        // Drain the rest.
        let mut got = Vec::new();
        while let Some(v) = c.pop() {
            got.push(v);
        }
        got.sort_unstable();
        // We popped one of {0,1,2,3} above; the remaining 3 plus 99
        // are in `got`.
        assert_eq!(got.len(), 4);
        assert!(got.contains(&99));
    }

    #[test]
    fn reserve_block_returns_none_on_consumer_close() {
        let (mut p, c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert!(p.reserve().is_none());
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
    // Drain
    // -----------------------------------------------------------------------

    #[test]
    fn drain_empty_returns_zero() {
        let (_p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let n = c.drain(|_| panic!("unexpected"));
        assert_eq!(n, 0);
    }

    #[test]
    fn drain_all_published() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        for i in 0..4 {
            p.push(i).unwrap();
        }
        let mut got = Vec::new();
        let n = c.drain(|v| got.push(v));
        assert_eq!(n, 4);
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3]);
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
        // Remaining 2 still claimable.
        let mut rest = Vec::new();
        c.drain(|v| rest.push(v));
        let mut all: Vec<u32> = got.into_iter().chain(rest).collect();
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3]);
    }

    #[test]
    fn drain_block_drains_to_close() {
        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
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
            p.push(i).unwrap();
        }
        drop(p);
        let mut got = h.join().unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 100, 101, 102]);
    }

    /// mpmc analog of `mpsc::drain_wakes_all_parked_producers` — same
    /// regression class. With `wake_n(count)` all parked producers
    /// wake after a drain; with the old `wake_one` only one would,
    /// stranding the rest.
    #[test]
    fn drain_wakes_all_parked_producers() {
        const CAP: u32 = 4;
        const N_PRODUCERS: u32 = 4;

        let (p, mut c) = RingBuffer::<u32>::new(Capacity::exact(CAP as usize)).split();
        for i in 0..CAP {
            p.push(i).unwrap();
        }

        let producers: Vec<_> = (0..N_PRODUCERS)
            .map(|tid| {
                let p = p.clone();
                std::thread::spawn(move || {
                    p.push_block(1000 + tid).expect("consumers dropped");
                })
            })
            .collect();
        drop(p);
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Single drain — releases done[s] for all 4 slots and wakes
        // up to 4 producers via wake_n.
        let n = c.drain(|_| {});
        assert_eq!(n, CAP as usize);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        for (i, h) in producers.into_iter().enumerate() {
            while !h.is_finished() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "drain woke fewer than {N_PRODUCERS} parked producers — \
                     producer {i} (and possibly later ones) still parked"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            h.join().unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // Async API
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(feature = "async")]
    fn async_push_pop_cross_thread() {
        // M producers + N consumers, each on its own thread with a
        // current_thread runtime + LocalSet. Watchdog aborts within 5s
        // if anything deadlocks.
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::Arc;

        let done = Arc::new(AtomicBool::new(false));
        let done_watchdog = done.clone();
        let watchdog = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !done_watchdog.load(Ordering::Acquire) {
                if std::time::Instant::now() > deadline {
                    eprintln!("\n\nmpmc async_push_pop_cross_thread: deadlocked, aborting\n");
                    std::process::abort();
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        });

        let n_producers: u64 = 2;
        let n_consumers: u64 = 2;
        let per_producer: u64 = 2_500;
        let total = n_producers * per_producer;
        let received = Arc::new(AtomicU64::new(0));

        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();

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
        drop(consumer);

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
                                .expect("all consumers dropped");
                        }
                    }));
                })
            })
            .collect();
        drop(producer);

        for h in producer_threads {
            h.join().unwrap();
        }
        for h in consumer_threads {
            h.join().unwrap();
        }
        assert_eq!(received.load(Ordering::Relaxed), total);
        done.store(true, Ordering::Release);
        watchdog.join().unwrap();
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
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
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
}
