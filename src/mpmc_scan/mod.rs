//! Multi-producer, multi-consumer ring with relaxed-FIFO scan-based
//! consumer claim. Trades strict cross-consumer FIFO for elimination of
//! the shared `head` cursor and its M-way CAS contention.
//!
//! # Design
//!
//! No shared consumer-side cursor. Each consumer maintains a private
//! `next_scan` position and walks the ring looking for slots in the
//! "published" state, claiming via CAS on the per-slot `ready` atomic.
//! Consumers naturally drift apart over time (each one only advances
//! its private cursor on successful claim), so contention on any
//! individual slot's `ready` line is rare — typically just the producer
//! handoff to the first consumer to find it.
//!
//! The shared cacheline traffic shifts from "M consumers fighting over
//! one `head` line" to "one producer + at most one consumer per slot,
//! distributed across `cap` independent lines." That's the contention
//! profile modern CPUs are best at.
//!
//! # Per-slot state encoding
//!
//! `ready[s]` cycles through three states per round, encoded so the
//! state and round are both recoverable from the value alone:
//!
//! | State | Value | Meaning |
//! |---|---|---|
//! | Free | `s + R*cap` | producer for round R may fill |
//! | Published | `s + R*cap + 1` | available for any consumer |
//! | Claimed | `s + R*cap + 2` | a consumer owns this position |
//!
//! Detecting the state from `ready[s] = v`:
//! - `(v - s) % cap == 0` → free
//! - `(v - s) % cap == 1` → published
//! - `(v - s) % cap == 2` → claimed
//!
//! Wraparound aliasing: round R "claimed" (`s + R*cap + 2`) and round
//! R+1 "free" (`s + R*cap + cap`) are distinct iff `cap >= 3`. We
//! require `cap >= 4` as a safety margin (and to avoid degenerate
//! single-slot semantics).
//!
//! `done[s]` is unchanged from the FIFO MPMC: cycles `s, s+cap,
//! s+2*cap, ...`. Producer for logical `pos` waits for `done[s] ==
//! pos`; consumer at logical `pos` stores `done[s] = pos + cap`.
//!
//! # Trade-offs vs strict-FIFO MPMC
//!
//! - **No FIFO of any kind.** Consumers find published items by
//!   scanning the ring, so cross-producer order is lost. Within a
//!   single producer's stream, push order is also not preserved:
//!   each producer reserves a batch of positions and publishes
//!   *out of order* into them — picking whichever position has its
//!   slot already free, falling back to spinning on the lowest only
//!   if no slot is ready. This converts producer-spin time into
//!   productive publishing under contention.
//! - **Empty-queue cost is O(scan bound) per `pop`,** vs O(1) for the
//!   FIFO variant's tail check. Workloads with frequent empty polls
//!   pay more here.
//! - **No `tail` watermark** for `is_empty`/`is_full`/`len` — these
//!   become approximate or scan-based. We expose them as best-effort.
//!
//! # When to choose this variant
//!
//! Use when:
//! - You have many consumers (≥4) all draining the same logical work.
//! - Throughput matters more than strict ordering.
//! - The queue is rarely empty (sustained producer pressure).
//!
//! Use the strict-FIFO `mpmc` variant when:
//! - You need cross-consumer ordering guarantees.
//! - Empty-queue polling is frequent.
//! - Capacity is small (< 4) or consumer count is low (≤ 2).

mod consumer;
mod producer;

pub use consumer::Consumer;
pub use producer::Producer;

use crate::capacity::Capacity;
use crate::common::{AlignedBuf, CachePadded};

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(C)]
pub struct RingBuffer<T> {
    pub(crate) data: AlignedBuf<UnsafeCell<MaybeUninit<T>>>,
    /// Per-slot tri-state with round encoding. See module docs.
    ///
    /// **Not** `CachePadded` — slots are packed 8 per cacheline
    /// (64-byte line / 8-byte `AtomicUsize`). The scan-based consumer
    /// benefits from spatial locality: walking 8 consecutive slots is
    /// 1 line load instead of 8. The cost is producer invalidating
    /// nearby slots' lines on publish, but in steady-state (consumers
    /// keeping up), producers move linearly through the ring and
    /// touch each line exactly 8× before moving to the next, vs the
    /// `CachePadded` layout where they touch each line 1× — same
    /// total invalidations, just spread differently.
    pub(crate) ready: AlignedBuf<AtomicUsize>,
    /// Per-slot "consumer released" marker. Producer for logical `pos`
    /// spins on `done[s] == pos`. Same packed layout as `ready` — the
    /// access pattern is symmetric.
    pub(crate) done: AlignedBuf<AtomicUsize>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    /// Producer claim cursor. FAA'd to assign unique logical positions.
    pub(crate) claim: CachePadded<AtomicUsize>,
    /// Coarse "consumed" watermark — a lower bound on the number of
    /// items consumers have popped. Consumers buffer local counts and
    /// flush to this counter every `CONSUMED_FLUSH` pops. Producers
    /// use it (loosely) to bound batched FAA on `claim` so a single
    /// FAA never claims more positions than the ring can hold.
    /// Flushing is rare and the producer's batch sizing only needs an
    /// approximate bound, so the line stays mostly cool.
    pub(crate) consumed: CachePadded<AtomicUsize>,
    /// Number of live producers; last-drop sets `closed`.
    pub(crate) producer_count: CachePadded<AtomicUsize>,
    /// Monotonic counter incremented on each Consumer clone. Each
    /// new consumer derives a starting `next_scan` offset from this
    /// to spread consumers across the ring instead of stacking them
    /// all at scan=0. Touched only at clone time, so it doesn't
    /// affect the hot path.
    pub(crate) clone_counter: CachePadded<AtomicUsize>,
    pub(crate) closed: CachePadded<AtomicBool>,
}

// SAFETY: All shared state is atomic; per-slot CAS gives unique
// ownership of each (slot, round) pair to exactly one consumer.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// # Panics
    /// Panics if `capacity.get() < 4`. The encoding requires `cap >= 3`
    /// for correctness; we require `>= 4` (the smallest power of two
    /// satisfying that) to keep round arithmetic non-degenerate.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        assert!(
            cap >= 4,
            "mpmc_scan requires capacity >= 4; use mpmc for smaller rings"
        );
        let data = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        // ready[s] = s → "free for round 0" (logical pos s).
        let mut ridx = 0usize;
        let ready = AlignedBuf::new_with(cap, || {
            let r = AtomicUsize::new(ridx);
            ridx += 1;
            r
        });
        // done[s] = s → "previous round (-1) consumer at logical pos s
        // - cap released this slot." For round 0, producer at logical s
        // checks done[s] == s, which holds.
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
            clone_counter: CachePadded(AtomicUsize::new(0)),
            closed: CachePadded(AtomicBool::new(false)),
            cap,
            mask: capacity.mask,
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

    /// Diagnostic: returns `(claim, snapshot of ready[..], snapshot of done[..])`.
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

    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self);
        let producer = Producer::new(arc.clone());
        let consumer = Consumer::new(arc);
        (producer, consumer)
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // At drop time, all Producer/Consumer handles are gone (we
        // hold &mut self). So no consumer is mid-pop. A slot has
        // live data iff:
        //   - state == 1 (published), and consumer never claimed it
        //   - state == 2 (claimed) where the claiming consumer's read
        //     completed but never released done[s]. In our protocol,
        //     consumer always reads-then-stores done in the same call,
        //     so post-release done[s] == round_pos + cap and the value
        //     has already been moved out. We detect this via done[s].
        let cap = self.cap;
        let _ = self.mask;
        for s in 0..cap {
            let r = *self.ready[s].get_mut();
            let d = *self.done[s].get_mut();
            // Decode: r = s + R*cap + state, where state ∈ {0,1,2}.
            let delta = r.wrapping_sub(s);
            let state = delta & self.mask;
            let round = delta / cap;
            let round_pos = s + round * cap;
            if state == 1 {
                // Published. Consumer never claimed → value is live.
                // (If consumer had claimed, ready would be at state 2.)
                // Sanity: done[s] should equal round_pos (producer's
                // round, not yet released). Either way, value is live.
                let _ = d;
                // SAFETY: published, never claimed → initialized,
                // never moved.
                unsafe {
                    self.data[s].get().cast::<T>().drop_in_place();
                }
            } else if state == 2 {
                // Claimed. Consumer either fully completed (read +
                // released done) or aborted before reading. Distinguish
                // via done[s]: if done[s] == round_pos + cap, the
                // consumer released after reading → value moved out,
                // do NOT drop. Otherwise the value is still in the
                // slot (consumer claimed but never read; in our
                // protocol that doesn't happen, but be defensive).
                if d != round_pos + cap {
                    // SAFETY: claimed but not released → producer
                    // wrote, consumer never moved out.
                    unsafe {
                        self.data[s].get().cast::<T>().drop_in_place();
                    }
                }
            }
            // state == 0 → free, no live data.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::Capacity;

    #[test]
    #[should_panic(expected = "mpmc_scan requires capacity >= 4")]
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
            let (p, _c) = RingBuffer::new(Capacity::exact(4)).split();
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
            let (p, c) = RingBuffer::new(Capacity::exact(4)).split();
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
    fn stress_mpmc_scan() {
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
