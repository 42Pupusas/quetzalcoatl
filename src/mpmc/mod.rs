//! Multi-producer, multi-consumer (MPMC) lock-free ring buffer.
//!
//! Multiple producers push concurrently via atomic fetch-and-add (FAA);
//! multiple consumers compete to pop via batched bounded CAS on the head
//! cursor. Each item is delivered to exactly one consumer.
//!
//! Both [`Producer`] and [`Consumer`] are [`Clone`].
//!
//! # Layout (split-plane)
//!
//! Same protocol as the SPMC variant, extended for multi-producer:
//!
//! * `ready[s]` — producer-write, consumer-read. Producer Release-stores
//!   `ready[s] = pos + 1` after writing data at logical position `pos`.
//! * `done[s]` — consumer-write, producer-read. Consumer Release-stores
//!   `done[s] = pos + cap` after reading.
//!
//! Round numbers are encoded in `ready` and `done` (rather than as a
//! sentinel tag bit in the value), so payloads have no bit-width
//! constraint. The "versioned null" sentinel idea is realized as
//! `done[s] == pos` meaning "free for round `pos / cap`."
//!
//! # Producer claim
//!
//! Producers FAA the shared `claim` cursor to get a unique `pos`. They
//! then spin on `done[s] == pos` until the previous round's consumer
//! releases the slot. Different producers spin on different slots
//! (different cache lines), so no inter-producer contention during the
//! wait.
//!
//! After publishing `ready[s]`, each producer attempts to advance the
//! shared `tail` watermark monotonically. `tail` is a *loose* upper
//! bound: it may lag behind the highest published position. Consumers
//! verify each per-slot `ready[s]` after claiming, so an out-of-order
//! producer publishing pattern is handled correctly.
//!
//! # Consumer claim
//!
//! Consumers do bounded-batched CAS on `head` clamped by `tail`. After
//! claiming a position, each consumer spins on `ready[s] == pos + 1`
//! before reading — this handles the case where the producer for `pos`
//! is slower than a producer for `pos + 1` who already raised `tail`.
//!
//! # Example
//!
//! ```
//! use quetzalcoatl::mpmc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, consumer) = RingBuffer::new(Capacity::exact(16)).split();
//! let p2 = producer.clone();
//! let _c2 = consumer.clone(); // additional consumers can join concurrently
//!
//! producer.push(1u32).unwrap();
//! p2.push(2).unwrap();
//!
//! // Each item is delivered to exactly one consumer; a single consumer
//! // here will claim both since batches are reserved per-consumer.
//! let mut got = vec![consumer.pop().unwrap(), consumer.pop().unwrap()];
//! got.sort();
//! assert_eq!(got, [1, 2]);
//! ```

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
    pub(crate) ready: AlignedBuf<CachePadded<AtomicUsize>>,
    pub(crate) done: AlignedBuf<CachePadded<AtomicUsize>>,
    pub(crate) cap: usize,
    pub(crate) mask: usize,
    /// Consumer claim cursor. Advanced via bounded CAS clamped by `tail`.
    pub(crate) head: CachePadded<AtomicUsize>,
    /// Producer claim cursor. Advanced via FAA — each producer gets a
    /// unique logical position. May be ahead of `tail` if a producer has
    /// claimed but not yet published.
    pub(crate) claim: CachePadded<AtomicUsize>,
    /// Loose published-watermark. Producers fetch_max-advance after
    /// publishing `ready[s]`. Consumers Acquire-load to bound their
    /// batched CAS on `head`. Per-slot `ready` is the authoritative
    /// publication signal; `tail` is just a hint to avoid the consumer
    /// spinning on an unpublished slot.
    pub(crate) tail: CachePadded<AtomicUsize>,
    /// Number of live producers. The last producer to drop sets `closed`.
    pub(crate) producer_count: CachePadded<AtomicUsize>,
    /// Number of live consumers. Read by `claim_batch` (under
    /// `mpmc-instrument`) to compute the FAA safety threshold.
    pub(crate) consumer_count: CachePadded<AtomicUsize>,
    pub(crate) closed: CachePadded<AtomicBool>,

    #[cfg(feature = "mpmc-instrument")]
    pub(crate) instr_faa_eligible: CachePadded<AtomicUsize>,
    #[cfg(feature = "mpmc-instrument")]
    pub(crate) instr_cas_fallback: CachePadded<AtomicUsize>,
}

// SAFETY: Multi-producer claim via FAA on `claim` and split-plane
// publication via `ready[s]`. Multi-consumer claim via bounded CAS on
// `head` and split-plane release via `done[s]`. Sequence numbers
// (round-tagged) ensure ABA safety across wrap-around.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let data = AlignedBuf::new_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        let ready = AlignedBuf::new_with(cap, || CachePadded(AtomicUsize::new(0)));
        let mut idx = 0usize;
        let done = AlignedBuf::new_with(cap, || {
            let d = CachePadded(AtomicUsize::new(idx));
            idx += 1;
            d
        });

        Self {
            data,
            ready,
            done,
            head: CachePadded(AtomicUsize::new(0)),
            claim: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            producer_count: CachePadded(AtomicUsize::new(1)),
            consumer_count: CachePadded(AtomicUsize::new(1)),
            closed: CachePadded(AtomicBool::new(false)),
            #[cfg(feature = "mpmc-instrument")]
            instr_faa_eligible: CachePadded(AtomicUsize::new(0)),
            #[cfg(feature = "mpmc-instrument")]
            instr_cas_fallback: CachePadded(AtomicUsize::new(0)),
            cap,
            mask: capacity.mask,
        }
    }

    /// Snapshot of the FAA-eligibility instrumentation counters.
    /// Returns `(faa_eligible, cas_fallback)`.
    #[cfg(feature = "mpmc-instrument")]
    #[must_use]
    pub fn instrument_counts(&self) -> (usize, usize) {
        (
            self.instr_faa_eligible.0.load(Ordering::Relaxed),
            self.instr_cas_fallback.0.load(Ordering::Relaxed),
        )
    }

    #[must_use]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() == self.cap
    }

    #[inline]
    pub(crate) fn data_slot(&self, pos: usize) -> &UnsafeCell<MaybeUninit<T>> {
        let idx = pos & self.mask;
        // SAFETY: mask = cap - 1, cap = data.len(), so idx < data.len().
        unsafe { std::hint::assert_unchecked(idx < self.data.len()) };
        &self.data[idx]
    }

    #[inline]
    pub(crate) fn ready_slot(&self, pos: usize) -> &CachePadded<AtomicUsize> {
        let idx = pos & self.mask;
        // SAFETY: mask = cap - 1, cap = ready.len(), so idx < ready.len().
        unsafe { std::hint::assert_unchecked(idx < self.ready.len()) };
        &self.ready[idx]
    }

    #[inline]
    pub(crate) fn done_slot(&self, pos: usize) -> &CachePadded<AtomicUsize> {
        let idx = pos & self.mask;
        // SAFETY: mask = cap - 1, cap = done.len(), so idx < done.len().
        unsafe { std::hint::assert_unchecked(idx < self.done.len()) };
        &self.done[idx]
    }

    /// Monotonically advances `tail` to at least `new_tail`, using a CAS
    /// loop (we don't have stable `fetch_max` for `AtomicUsize` on all
    /// targets). Producers call this after publishing `ready[s]`; a
    /// concurrent producer for a higher position may have already
    /// advanced `tail` past us, in which case we exit immediately.
    #[inline]
    fn advance_tail(&self, new_tail: usize) {
        let mut current = self.tail.load(Ordering::Relaxed);
        while current < new_tail {
            match self.tail.compare_exchange_weak(
                current,
                new_tail,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self);
        let producer = Producer { queue: arc.clone() };
        let consumer = Consumer::new(arc);
        (producer, consumer)
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let claim = *self.claim.0.get_mut();
        // Live data lives at positions in [head, claim) where:
        //   ready[s] == pos + 1   (producer published)
        //   done[s]  != pos + cap (no consumer has released)
        // Positions claimed-but-never-published (producer panicked or
        // dropped a SlotWriter) leak ready < pos+1 — those slots have
        // no live data to drop.
        if head >= claim {
            return;
        }
        for pos in head..claim {
            let s = pos & self.mask;
            let r = *self.ready[s].0.get_mut();
            let d = *self.done[s].0.get_mut();
            if r == pos + 1 && d != pos + self.cap {
                // SAFETY: producer published valid data, no consumer has
                // released it.
                unsafe {
                    self.data[s].get().cast::<T>().drop_in_place();
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
        let (producer, consumer) = RingBuffer::<u8>::new(Capacity::exact(1)).split();
        producer.push(1).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert!(consumer.is_empty());
    }

    #[test]
    fn zero_sized_types() {
        let (producer, consumer) = RingBuffer::<()>::new(Capacity::exact(4)).split();
        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
    }

    #[test]
    fn fill_to_capacity_and_drain() {
        let (producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(8)).split();
        for i in 0..8 {
            producer.push(i).unwrap();
        }
        assert_eq!(producer.push(99), Err(99));
        for i in 0..8 {
            assert_eq!(consumer.pop(), Some(i));
        }
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn wraparound_exercises_all_slots() {
        let (producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
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

    #[test]
    fn pop_empty_returns_none() {
        let (_p, c) = RingBuffer::<u8>::new(Capacity::exact(4)).split();
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn concurrent_data_race_check() {
        // 1 producer, 1 consumer, tiny buffer — Miri-tractable.
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 16u64;

        let h = std::thread::spawn(move || {
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
        h.join().unwrap();
    }

    #[test]
    fn concurrent_two_producers_two_consumers() {
        let total = 8usize;
        let (producer, consumer) = RingBuffer::<usize>::new(Capacity::exact(4)).split();
        let p2 = producer.clone();
        let c2 = consumer.clone();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let r1 = received.clone();
        let h1 = std::thread::spawn(move || {
            while r1.load(std::sync::atomic::Ordering::Relaxed) < total {
                if consumer.pop().is_some() {
                    r1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }
        });
        let r2 = received.clone();
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
            for i in 0..(total / 2) {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });
        let h4 = std::thread::spawn(move || {
            for i in 0..(total / 2) {
                while p2.push(100 + i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        h3.join().unwrap();
        h4.join().unwrap();
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(received.load(std::sync::atomic::Ordering::Relaxed), total);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn stress_mpmc() {
        let total: u64 = 4000;
        let n_p: u64 = 4;
        let n_c: u64 = 4;
        let per_p = total / n_p;
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let prods: Vec<_> = (0..n_p)
            .map(|tid| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    for i in 0..per_p {
                        while p.push(tid * per_p + i).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(producer);

        let cons: Vec<_> = (0..n_c)
            .map(|_| {
                let c = consumer.clone();
                let r = received.clone();
                std::thread::spawn(move || {
                    let mut local = Vec::new();
                    while r.load(std::sync::atomic::Ordering::Relaxed) < total as usize {
                        if let Some(v) = c.pop() {
                            r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            local.push(v);
                        } else {
                            std::thread::yield_now();
                        }
                    }
                    local
                })
            })
            .collect();
        drop(consumer);

        for h in prods {
            h.join().unwrap();
        }
        let mut all: Vec<u64> = Vec::new();
        for h in cons {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total as usize);
    }

    use crate::common::DropCounter;

    #[test]
    fn drop_items_left_in_buffer() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (producer, consumer) = RingBuffer::new(Capacity::exact(4)).split();
            for _ in 0..4 {
                producer
                    .push(DropCounter {
                        counter: counter.clone(),
                    })
                    .unwrap();
            }
            drop(consumer);
            drop(producer);
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[test]
    fn drop_items_after_partial_drain() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let (producer, consumer) = RingBuffer::new(Capacity::exact(4)).split();
            for _ in 0..4 {
                producer
                    .push(DropCounter {
                        counter: counter.clone(),
                    })
                    .unwrap();
            }
            drop(consumer.pop().unwrap());
            drop(consumer.pop().unwrap());
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);
            drop(consumer);
            drop(producer);
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[test]
    fn last_producer_drop_marks_closed() {
        let (producer, consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();
        let p2 = producer.clone();
        producer.push(1).unwrap();
        drop(producer);
        assert!(!consumer.is_closed());
        p2.push(2).unwrap();
        drop(p2);
        assert!(consumer.is_closed());
        let mut got = vec![consumer.pop().unwrap(), consumer.pop().unwrap()];
        got.sort();
        assert_eq!(got, vec![1, 2]);
        assert_eq!(consumer.pop(), None);
    }
}
