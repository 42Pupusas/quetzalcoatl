use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Reimplements the MPSC claim protocol over a plain atomic pair so the
/// overclaim distance is observable. Mirrors `Producer::claim_slot`:
/// check `tail - head >= cap`, then `fetch_add` as a separate step.
struct ClaimRace {
    head: AtomicU64,
    tail: AtomicU64,
    cap: u64,
    overclaims: AtomicU64,
    claims: AtomicU64,
    worst_distance: AtomicU64,
}

impl ClaimRace {
    fn new(cap: u64) -> Self {
        Self {
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            cap,
            overclaims: AtomicU64::new(0),
            claims: AtomicU64::new(0),
            worst_distance: AtomicU64::new(0),
        }
    }

    fn try_claim(&self) -> Option<u64> {
        let current_tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if current_tail.wrapping_sub(head) >= self.cap {
            return None;
        }
        let pos = self.tail.fetch_add(1, Ordering::Relaxed);
        self.claims.fetch_add(1, Ordering::Relaxed);

        let head_now = self.head.load(Ordering::Acquire);
        if pos.wrapping_sub(head_now) >= self.cap {
            let distance = pos.wrapping_sub(head_now) - self.cap + 1;
            self.overclaims.fetch_add(1, Ordering::Relaxed);
            self.worst_distance.fetch_max(distance, Ordering::Relaxed);
        }
        Some(pos)
    }

    fn release(&self) {
        self.head.fetch_add(1, Ordering::Release);
    }
}

struct RaceTrial {
    producers: u64,
    capacity: u64,
    items_per_producer: u64,
}

impl RaceTrial {
    const fn new(producers: u64, capacity: u64, items_per_producer: u64) -> Self {
        Self {
            producers,
            capacity,
            items_per_producer,
        }
    }

    fn run(&self) {
        let race = Arc::new(ClaimRace::new(self.capacity));
        let total = self.producers * self.items_per_producer;
        let per = self.items_per_producer;

        let workers: Vec<_> = (0..self.producers)
            .map(|_| {
                let race = Arc::clone(&race);
                std::thread::spawn(move || {
                    for _ in 0..per {
                        while race.try_claim().is_none() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();

        let consumer = {
            let race = Arc::clone(&race);
            std::thread::spawn(move || {
                for _ in 0..total {
                    loop {
                        let head = race.head.load(Ordering::Relaxed);
                        if head < race.tail.load(Ordering::Acquire) {
                            race.release();
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            })
        };

        let start = Instant::now();
        for w in workers {
            w.join().unwrap();
        }
        consumer.join().unwrap();

        let claims = race.claims.load(Ordering::Relaxed);
        let over = race.overclaims.load(Ordering::Relaxed);
        let pct = (over as f64 / claims as f64) * 100.0;
        println!(
            "producers={:<3} cap={:<6} claims={:<9} overclaimed={:<9} ({:>6.2}%)  worst_overshoot={:<5} elapsed={:?}",
            self.producers,
            self.capacity,
            claims,
            over,
            pct,
            race.worst_distance.load(Ordering::Relaxed),
            start.elapsed()
        );
    }
}

struct ClaimStats;

impl ClaimStats {
    fn run(&self) {
        println!(
            "Overclaim = FAA returned a position whose slot is still occupied.\n\
             Each one makes push() spin on slot.sequence instead of returning Err.\n"
        );
        for producers in [1u64, 2, 4, 8, 12, 16] {
            RaceTrial::new(producers, 8192, 5_000).run();
        }
        println!();
        for capacity in [16u64, 64, 256, 8192] {
            RaceTrial::new(8, capacity, 5_000).run();
        }
    }
}

fn main() {
    ClaimStats.run();
}
