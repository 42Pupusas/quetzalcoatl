//! Paired A/B of the `claim` refill: `fetch_add` against `compare_exchange`.
//!
//! Both variants run in one process, alternating inside every round and
//! swapping which goes first, so background load and frequency drift
//! reach both sides alike. The reported ratio is CAS time over
//! `fetch_add` time per round: below 1.0 means CAS is faster.

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpmc::{Cfg, Config, SplitRing};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const ITEMS_PER_PRODUCER: u64 = 20_000;
const ROUNDS: usize = 200;

type FetchAdd<const BATCH: usize> = Cfg<BATCH, 128, 64, false>;
type CompareExchange<const BATCH: usize> = Cfg<BATCH, 128, 64, true>;

struct Shape {
    producers: usize,
    capacity: usize,
}

impl Shape {
    fn run<C: Config>(&self) -> Duration {
        let total = self.producers * ITEMS_PER_PRODUCER as usize;
        let (producer, consumer) = SplitRing::<u64, C>::new(Capacity::exact(self.capacity)).split();
        let received = Arc::new(AtomicUsize::new(0));
        let started = Instant::now();

        let consumers: Vec<_> = (0..self.producers)
            .map(|_| {
                let consumer = consumer.clone();
                let received = received.clone();
                thread::spawn(move || loop {
                    if let Some(value) = consumer.pop() {
                        black_box(value);
                        received.fetch_add(1, Ordering::Relaxed);
                    } else if received.load(Ordering::Relaxed) >= total {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                })
            })
            .collect();
        drop(consumer);

        let producers: Vec<_> = (0..self.producers)
            .map(|id| {
                let producer = producer.clone();
                thread::spawn(move || {
                    for i in 0..ITEMS_PER_PRODUCER {
                        let value = id as u64 * ITEMS_PER_PRODUCER + i;
                        while producer.push(value).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        drop(producer);

        for handle in producers {
            handle.join().unwrap();
        }
        for handle in consumers {
            handle.join().unwrap();
        }
        started.elapsed()
    }

    fn paired<Add: Config, Cas: Config>(&self, label: &str) {
        let mut add_times = Vec::with_capacity(ROUNDS);
        let mut cas_times = Vec::with_capacity(ROUNDS);
        let mut ratios = Vec::with_capacity(ROUNDS);
        for round in 0..ROUNDS {
            let (add, cas) = if round % 2 == 0 {
                let add = self.run::<Add>();
                (add, self.run::<Cas>())
            } else {
                let cas = self.run::<Cas>();
                (self.run::<Add>(), cas)
            };
            add_times.push(add.as_secs_f64());
            cas_times.push(cas.as_secs_f64());
            ratios.push(cas.as_secs_f64() / add.as_secs_f64());
        }
        let cas_wins = ratios.iter().filter(|ratio| **ratio < 1.0).count();
        println!(
            "{label:<10} p{:<2} cap{:<5} add {:>7.3} ms  cas {:>7.3} ms  ratio p25 {:.3} med {:.3} p75 {:.3}  cas faster {:>3}/{ROUNDS}",
            self.producers,
            self.capacity,
            Self::median(&mut add_times) * 1e3,
            Self::median(&mut cas_times) * 1e3,
            Self::quantile(&mut ratios, 0.25),
            Self::quantile(&mut ratios, 0.5),
            Self::quantile(&mut ratios, 0.75),
            cas_wins,
        );
    }

    fn quantile(values: &mut [f64], at: f64) -> f64 {
        values.sort_by(f64::total_cmp);
        values[((values.len() - 1) as f64 * at).round() as usize]
    }

    fn median(values: &mut [f64]) -> f64 {
        Self::quantile(values, 0.5)
    }
}

fn main() {
    for (producers, capacity) in [(2, 64), (4, 64), (8, 64), (2, 1024), (4, 1024), (8, 1024)] {
        let shape = Shape {
            producers,
            capacity,
        };
        shape.paired::<FetchAdd<32>, CompareExchange<32>>("batch 32");
    }
    for (producers, capacity) in [(4, 64), (8, 64), (4, 1024), (8, 1024)] {
        let shape = Shape {
            producers,
            capacity,
        };
        shape.paired::<FetchAdd<1>, CompareExchange<1>>("batch 1");
    }
}
