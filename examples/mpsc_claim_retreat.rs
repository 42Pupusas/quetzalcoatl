//! Head-to-head of three MPSC tail-claim protocols on one shared ring.
//!
//! The ring, the slot sequence protocol and the consumer are identical
//! across the three arms. Only the claim policy changes, so the numbers
//! isolate the claim itself.
//!
//! Payload is an `AtomicU64` rather than `UnsafeCell<MaybeUninit<T>>`.
//! The claim protocol is what this probe measures, and the payload
//! write is one store in either representation.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const TOMBSTONE: usize = usize::MAX;

/// A slot that is free at `pos` carries `pos * 2`, and a slot that
/// holds a published value at `pos` carries `pos * 2 + 1`.
struct Slot {
    sequence: AtomicUsize,
    data: AtomicU64,
}

impl Slot {
    fn free_at(pos: usize) -> Self {
        Self {
            sequence: AtomicUsize::new(pos * 2),
            data: AtomicU64::new(0),
        }
    }
}

#[repr(align(128))]
struct Padded(AtomicUsize);

struct Ring {
    slots: Vec<Slot>,
    cap: usize,
    mask: usize,
    head: Padded,
    tail: Padded,
    tombstones: AtomicU64,
    retreats: AtomicU64,
}

impl Ring {
    fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two());
        Self {
            slots: (0..cap).map(Slot::free_at).collect(),
            cap,
            mask: cap - 1,
            head: Padded(AtomicUsize::new(0)),
            tail: Padded(AtomicUsize::new(0)),
            tombstones: AtomicU64::new(0),
            retreats: AtomicU64::new(0),
        }
    }

    #[inline]
    fn slot(&self, pos: usize) -> &Slot {
        &self.slots[pos & self.mask]
    }

    #[inline]
    fn publish(&self, pos: usize, val: u64) {
        let slot = self.slot(pos);
        slot.data.store(val, Ordering::Relaxed);
        slot.sequence.store(pos * 2 + 1, Ordering::Release);
    }

    /// Abandons a claimed position so the consumer skips past it.
    #[inline]
    fn tombstone(&self, pos: usize) {
        self.tombstones.fetch_add(1, Ordering::Relaxed);
        self.slot(pos).sequence.store(TOMBSTONE, Ordering::Release);
    }

    /// Single consumer. Returns the value, or `None` when the slot at
    /// `head` is not yet published.
    fn pop(&self) -> Option<u64> {
        let head = self.head.0.load(Ordering::Relaxed);
        let slot = self.slot(head);
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq == TOMBSTONE {
            self.release(head);
            return None;
        }
        if seq != head * 2 + 1 {
            return None;
        }
        let val = slot.data.load(Ordering::Relaxed);
        self.release(head);
        Some(val)
    }

    #[inline]
    fn release(&self, head: usize) {
        self.slot(head)
            .sequence
            .store((head + self.cap) * 2, Ordering::Release);
        self.head.0.store(head + 1, Ordering::Release);
    }
}

/// How a producer reserves a tail position.
trait ClaimPolicy: Send + Sync + 'static {
    const NAME: &'static str;

    /// Returns the claimed position, or `None` when the ring is full.
    /// Must never wait for a consumer: `push` is non-blocking.
    fn claim(ring: &Ring, cached_head: &Cell<usize>) -> Option<usize>;
}

/// The shipped protocol: compare-and-exchange the tail, retry on loss.
struct CasClaim;

impl ClaimPolicy for CasClaim {
    const NAME: &'static str = "cas";

    fn claim(ring: &Ring, cached_head: &Cell<usize>) -> Option<usize> {
        let mut current_tail = ring.tail.0.load(Ordering::Relaxed);
        loop {
            if current_tail.wrapping_sub(cached_head.get()) >= ring.cap {
                let head = ring.head.0.load(Ordering::Acquire);
                cached_head.set(head);
                if current_tail.wrapping_sub(head) >= ring.cap {
                    return None;
                }
            }
            match ring.tail.0.compare_exchange_weak(
                current_tail,
                current_tail + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(pos) => return Some(pos),
                Err(observed) => {
                    current_tail = observed;
                    std::hint::spin_loop();
                }
            }
        }
    }
}

/// The proposal: claim with one fetch-and-add, then validate.
///
/// An invalid claim is given back by moving the tail down again, which
/// only the last claimant can do. A producer that loses that race
/// abandons the slot so no consumer waits on it forever.
struct FaaRetreatClaim;

impl ClaimPolicy for FaaRetreatClaim {
    const NAME: &'static str = "faa+retreat";

    fn claim(ring: &Ring, cached_head: &Cell<usize>) -> Option<usize> {
        let current_tail = ring.tail.0.load(Ordering::Relaxed);
        if current_tail.wrapping_sub(cached_head.get()) >= ring.cap {
            let head = ring.head.0.load(Ordering::Acquire);
            cached_head.set(head);
            if current_tail.wrapping_sub(head) >= ring.cap {
                return None;
            }
        }

        let pos = ring.tail.0.fetch_add(1, Ordering::Relaxed);

        let head = ring.head.0.load(Ordering::Acquire);
        cached_head.set(head);
        if pos.wrapping_sub(head) < ring.cap {
            return Some(pos);
        }

        ring.retreats.fetch_add(1, Ordering::Relaxed);
        if ring
            .tail
            .0
            .compare_exchange(pos + 1, pos, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return None;
        }

        ring.tombstone(pos);
        None
    }
}

/// The original protocol, kept as the speed ceiling. It can hand out a
/// position whose slot is still occupied, so it is not correct.
struct FaaRawClaim;

impl ClaimPolicy for FaaRawClaim {
    const NAME: &'static str = "faa (buggy)";

    fn claim(ring: &Ring, cached_head: &Cell<usize>) -> Option<usize> {
        let current_tail = ring.tail.0.load(Ordering::Relaxed);
        if current_tail.wrapping_sub(cached_head.get()) >= ring.cap {
            let head = ring.head.0.load(Ordering::Acquire);
            cached_head.set(head);
            if current_tail.wrapping_sub(head) >= ring.cap {
                return None;
            }
        }
        let pos = ring.tail.0.fetch_add(1, Ordering::Relaxed);
        let slot = ring.slot(pos);
        let mut spins = 0u32;
        while slot.sequence.load(Ordering::Acquire) != pos * 2 {
            spins += 1;
            if spins > 20_000_000 {
                ring.tombstone(pos);
                return None;
            }
            std::hint::spin_loop();
        }
        Some(pos)
    }
}

struct Throughput {
    producers: usize,
    items_per_producer: u64,
    capacity: usize,
}

struct Measurement {
    elapsed: Duration,
    received: u64,
    tombstones: u64,
    retreats: u64,
    timed_out: bool,
}

impl Throughput {
    const fn new(producers: usize, items_per_producer: u64, capacity: usize) -> Self {
        Self {
            producers,
            items_per_producer,
            capacity,
        }
    }

    fn total(&self) -> u64 {
        self.items_per_producer * self.producers as u64
    }

    /// Producers retry until every item lands, so the consumer always
    /// receives exactly `total()` items. A tombstone burns a slot but
    /// loses no item, so it must not change this count.
    ///
    /// `abort` releases the producers if the consumer gives up, so a
    /// protocol that fails to make progress reports instead of hanging
    /// the join below.
    fn measure<P: ClaimPolicy>(&self, deadline: Duration) -> Measurement {
        let ring = Arc::new(Ring::new(self.capacity));
        let gate = Arc::new(Barrier::new(self.producers + 1));
        let abort = Arc::new(AtomicBool::new(false));
        let per = self.items_per_producer;

        let workers: Vec<_> = (0..self.producers)
            .map(|_| {
                let ring = Arc::clone(&ring);
                let gate = Arc::clone(&gate);
                let abort = Arc::clone(&abort);
                std::thread::spawn(move || {
                    let cached_head = Cell::new(0usize);
                    gate.wait();
                    for i in 0..per {
                        loop {
                            if let Some(pos) = P::claim(&ring, &cached_head) {
                                ring.publish(pos, i);
                                break;
                            }
                            if abort.load(Ordering::Relaxed) {
                                return;
                            }
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();

        gate.wait();
        let start = Instant::now();

        let expected = self.total();
        let mut received = 0u64;
        let mut timed_out = false;
        while received < expected {
            if ring.pop().is_some() {
                received += 1;
            } else if start.elapsed() > deadline {
                timed_out = true;
                break;
            } else {
                std::hint::spin_loop();
            }
        }
        let elapsed = start.elapsed();

        abort.store(true, Ordering::Relaxed);
        for w in workers {
            w.join().unwrap();
        }

        Measurement {
            elapsed,
            received,
            tombstones: ring.tombstones.load(Ordering::Relaxed),
            retreats: ring.retreats.load(Ordering::Relaxed),
            timed_out,
        }
    }

    fn report<P: ClaimPolicy>(&self) {
        let m = self.measure::<P>(Duration::from_secs(20));
        if m.timed_out {
            println!(
                "  {:<12} producers={:<3} TIMED OUT after {:?} with {}/{} items",
                P::NAME,
                self.producers,
                m.elapsed,
                m.received,
                self.total()
            );
            return;
        }
        let ns = m.elapsed.as_secs_f64() * 1e9 / m.received as f64;
        println!(
            "  {:<12} producers={:<3} {:>9.3?}  {:>7.2} ns/item  {:>7.2} Mitem/s  retreats={:<8} tombstones={}",
            P::NAME,
            self.producers,
            m.elapsed,
            ns,
            1000.0 / ns,
            m.retreats,
            m.tombstones
        );
    }
}

/// Starts more producers than the ring can hold, with no consumer.
/// Every producer must return, and the ring must accept no more than
/// its capacity.
struct FullRingProbe {
    capacity: usize,
    producers: usize,
}

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

    fn run<P: ClaimPolicy>(&self, deadline: Duration) -> ProbeOutcome {
        let ring = Arc::new(Ring::new(self.capacity));
        let gate = Arc::new(Barrier::new(self.producers));
        let returned = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));

        for id in 0..self.producers {
            let ring = Arc::clone(&ring);
            let gate = Arc::clone(&gate);
            let returned = Arc::clone(&returned);
            let accepted = Arc::clone(&accepted);
            std::thread::spawn(move || {
                let cached_head = Cell::new(0usize);
                gate.wait();
                if let Some(pos) = P::claim(&ring, &cached_head) {
                    ring.publish(pos, id as u64);
                    accepted.fetch_add(1, Ordering::Relaxed);
                }
                returned.fetch_add(1, Ordering::Relaxed);
            });
        }

        let start = Instant::now();
        while start.elapsed() < deadline && returned.load(Ordering::Relaxed) != self.producers {
            std::thread::sleep(Duration::from_millis(5));
        }

        ProbeOutcome {
            stuck: self.producers - returned.load(Ordering::Relaxed),
            accepted: accepted.load(Ordering::Relaxed),
        }
    }

    fn report<P: ClaimPolicy>(&self) {
        let outcome = self.run::<P>(Duration::from_secs(2));
        let verdict = if outcome.stuck > 0 {
            "STUCK"
        } else if outcome.accepted > self.capacity {
            "OVERFILL"
        } else {
            "ok"
        };
        println!(
            "  {:<12} cap={:<4} producers={:<3} accepted={:<3} stuck={:<3} {}",
            P::NAME,
            self.capacity,
            self.producers,
            outcome.accepted,
            outcome.stuck,
            verdict
        );
    }
}

struct Suite;

impl Suite {
    fn run(&self) {
        println!("== correctness: non-blocking claim on a full ring, no consumer ==");
        for (cap, producers) in [(1usize, 8usize), (1, 16), (4, 16), (16, 32)] {
            let probe = FullRingProbe::new(cap, producers);
            probe.report::<CasClaim>();
            probe.report::<FaaRetreatClaim>();
            println!();
        }

        println!("== throughput: producers + one consumer, capacity 8192 ==");
        for producers in [1usize, 2, 4, 8, 16] {
            let t = Throughput::new(producers, 200_000, 8192);
            t.report::<FaaRawClaim>();
            t.report::<CasClaim>();
            t.report::<FaaRetreatClaim>();
            println!();
        }
    }
}

fn main() {
    Suite.run();
}
