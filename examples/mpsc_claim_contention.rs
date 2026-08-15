use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

/// Counts the atomic operations each claim protocol issues to hand out
/// the same number of positions. FAA always succeeds once; CAS retries
/// whenever a peer wins the race.
struct ClaimCost {
    tail: AtomicUsize,
    attempts: AtomicU64,
}

impl ClaimCost {
    fn new() -> Self {
        Self {
            tail: AtomicUsize::new(0),
            attempts: AtomicU64::new(0),
        }
    }

    fn claim_faa(&self) -> usize {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.tail.fetch_add(1, Ordering::Relaxed)
    }

    fn claim_cas(&self) -> usize {
        let mut pos = self.tail.load(Ordering::Relaxed);
        loop {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            match self.tail.compare_exchange_weak(
                pos,
                pos + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return pos,
                Err(observed) => pos = observed,
            }
        }
    }
}

struct ContentionTrial {
    producers: usize,
    claims_each: usize,
}

impl ContentionTrial {
    const fn new(producers: usize, claims_each: usize) -> Self {
        Self {
            producers,
            claims_each,
        }
    }

    fn measure(&self, use_cas: bool) -> (f64, u64, std::time::Duration) {
        let cost = Arc::new(ClaimCost::new());
        let gate = Arc::new(Barrier::new(self.producers + 1));
        let claims_each = self.claims_each;

        let workers: Vec<_> = (0..self.producers)
            .map(|_| {
                let cost = Arc::clone(&cost);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    for _ in 0..claims_each {
                        if use_cas {
                            cost.claim_cas();
                        } else {
                            cost.claim_faa();
                        }
                    }
                })
            })
            .collect();

        gate.wait();
        let start = Instant::now();
        for w in workers {
            w.join().unwrap();
        }
        let elapsed = start.elapsed();

        let issued = (self.producers * self.claims_each) as u64;
        let attempts = cost.attempts.load(Ordering::Relaxed);
        (attempts as f64 / issued as f64, attempts, elapsed)
    }

    fn report(&self) {
        let (faa_ratio, _, faa_time) = self.measure(false);
        let (cas_ratio, _, cas_time) = self.measure(true);
        println!(
            "producers={:<3}  FAA {:>5.2} ops/claim {:>10.3?}   CAS {:>5.2} ops/claim {:>10.3?}   CAS is {:>4.1}x slower",
            self.producers,
            faa_ratio,
            faa_time,
            cas_ratio,
            cas_time,
            cas_time.as_secs_f64() / faa_time.as_secs_f64()
        );
    }
}

struct ContentionSuite;

impl ContentionSuite {
    fn run(&self) {
        println!(
            "Cost of claiming a position: unconditional FAA vs compare-exchange.\n\
             FAA always lands in one op. CAS retries once per lost race, so the\n\
             attempt count grows with the number of contending producers.\n"
        );
        for producers in [1usize, 2, 4, 8, 12, 16] {
            ContentionTrial::new(producers, 200_000).report();
        }
    }
}

fn main() {
    ContentionSuite.run();
}
