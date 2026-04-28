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

pub use consumer::Consumer;
pub use producer::Producer;

use crate::capacity::Capacity;
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::Thread;

/// Park-slot count. Each producer takes a stable slot at clone
/// time; bit `i` of `producer_wake` flags "slot `i` parked." 64 fits
/// in a single `AtomicU64`. Beyond 64 producers, slots alias and
/// a wake on bit `i` rouses any producer mapped there (benign
/// false wake; the woken producer re-checks and re-parks).
pub(crate) const PARK_SLOTS: usize = 64;
pub(crate) const PARK_MASK: usize = PARK_SLOTS - 1;

/// `cas_backoff` failure-counter threshold past which the slow
/// path stops spinning and parks. The schedule in
/// [`crate::common::cas_backoff`] saturates at f=12 (64 pauses
/// per call + sparse `yield_now`); waiting that long means we've
/// already burned ~tens of microseconds and a futex round-trip
/// (1–10μs) is amortized.
pub(crate) const BACKOFF_PARK_THRESHOLD: u32 = 12;

/// Safety-net park timeout (microseconds). The wake protocol's
/// `SeqCst` pairing should make missed wakes impossible, but this
/// timeout caps wait latency if a wake is somehow lost — defensive
/// against future refactors of the ordering invariants.
pub(crate) const PARK_TIMEOUT_MICROS: u64 = 200;

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
    /// Producer-side futex-style wake bitmap. Bit `i` set ↔ a
    /// producer in park slot `i` is parked waiting for a `done[s]`
    /// release. Consumers read this `Relaxed` after each release
    /// and wake one parked producer if non-zero.
    pub(crate) producer_wake: CachePadded<AtomicU64>,
    /// Producer-side `Thread` handle table. Idempotently set by
    /// the first producer that parks at each slot; readers
    /// (consumers issuing wakes) obtain a `&Thread` via
    /// `OnceLock::get`.
    pub(crate) producer_parkers: AlignedBuf<OnceLock<Thread>>,
    /// Consumer-side futex-style wake bitmap. Bit `i` set ↔ a
    /// consumer in park slot `i` is parked waiting for any
    /// `ready[s]` publish. Producers read this `Relaxed` after
    /// each publish and wake one parked consumer if non-zero.
    pub(crate) consumer_wake: CachePadded<AtomicU64>,
    /// Consumer-side `Thread` handle table. Mirror of
    /// [`producer_parkers`].
    pub(crate) consumer_parkers: AlignedBuf<OnceLock<Thread>>,
    /// Monotonic counter for assigning stable park-slot indices to
    /// `Consumer` clones. Bumped at clone time only.
    pub(crate) consumer_count: CachePadded<AtomicUsize>,
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
            producer_wake: CachePadded(AtomicU64::new(0)),
            producer_parkers: AlignedBuf::new_with(PARK_SLOTS, OnceLock::new),
            consumer_wake: CachePadded(AtomicU64::new(0)),
            consumer_parkers: AlignedBuf::new_with(PARK_SLOTS, OnceLock::new),
            consumer_count: CachePadded(AtomicUsize::new(0)),
            _config: std::marker::PhantomData,
        }
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

/// Wakes one peer parked on a wake bitmap. Caller must gate this
/// on `wake.load(Relaxed) != 0` for the fast-path no-cost case.
///
/// Used twice: by the consumer hot path to wake a producer parked
/// in `push`, and by the producer hot path to wake a consumer
/// parked in `pop_block`. The two sides have independent wake
/// bitmaps (`producer_wake` / `consumer_wake`) and parker tables.
///
/// Concurrent peers racing on the same set bit: only one's
/// `fetch_and` actually clears it; the loser observes the bit
/// already clear and skips the unpark.
#[inline]
fn wake_one(wake: &AtomicU64, parkers: &[OnceLock<Thread>]) {
    let ws = wake.load(Ordering::Relaxed);
    if ws == 0 {
        return;
    }
    let bit = ws.trailing_zeros();
    let mask = 1u64 << bit;
    let prev = wake.fetch_and(!mask, Ordering::Relaxed);
    if prev & mask == 0 {
        return;
    }
    if let Some(handle) = parkers[bit as usize].get() {
        handle.unpark();
    }
    // OnceLock None: peer set its bit before installing its handle.
    // Benign — its own re-check after install catches the wake.
}

/// Wakes one parked producer. Mirror of [`wake_one_consumer`].
#[inline]
pub(crate) fn wake_one_producer<T, C: Config>(q: &RingBuffer<T, C>) {
    wake_one(&q.producer_wake, &q.producer_parkers);
}

/// Wakes one parked consumer. Mirror of [`wake_one_producer`].
#[inline]
pub(crate) fn wake_one_consumer<T, C: Config>(q: &RingBuffer<T, C>) {
    wake_one(&q.consumer_wake, &q.consumer_parkers);
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
                    while r.load(std::sync::atomic::Ordering::Relaxed) < total as usize {
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
        assert_eq!(all.len(), total as usize);
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
                    while r.load(std::sync::atomic::Ordering::Relaxed) < actual_total as usize {
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
            actual_total as usize,
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
        let actual_total = (per_p * p_count) as usize;
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
}
