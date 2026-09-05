// Probe: why does `mpsc::Consumer::drain` lose to `pop` in
// benches/fanin.rs::drain_strategy?
//
// Hypothesis: `drain` keeps `head` in a local and publishes it once,
// after the batch ends. Producers test fullness against the shared
// `head`, so a long drain hides every freed slot until it finishes.
// `pop` publishes `head` per item, so producers refill continuously.
//
// If the hypothesis holds, `drain_up_to(k)` with small `k` recovers
// throughput, because it bounds how long `head` stays stale.
//
// Usage: mpsc_drain_staleness <producers> <per_producer> <iters>

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::RingBuffer;
use std::hint::black_box;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Strategy {
    Pop,
    Drain,
    DrainUpTo(usize),
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pop => write!(f, "pop"),
            Self::Drain => write!(f, "drain"),
            Self::DrainUpTo(k) => write!(f, "drain_up_to({k})"),
        }
    }
}

/// Batch-size distribution for one strategy. `mean` is the average
/// number of items a single consumer call returned, which is also
/// how many slots stay invisible to producers per call.
struct BatchStats {
    batches: u64,
    items: u64,
    empty: u64,
    max: usize,
}

impl BatchStats {
    const fn new() -> Self {
        Self {
            batches: 0,
            items: 0,
            empty: 0,
            max: 0,
        }
    }

    fn record(&mut self, n: usize) {
        if n == 0 {
            self.empty += 1;
            return;
        }
        self.batches += 1;
        self.items += n as u64;
        if n > self.max {
            self.max = n;
        }
    }

    fn merge(&mut self, other: &Self) {
        self.batches += other.batches;
        self.items += other.items;
        self.empty += other.empty;
        if other.max > self.max {
            self.max = other.max;
        }
    }

    fn mean(&self) -> f64 {
        if self.batches == 0 {
            0.0
        } else {
            self.items as f64 / self.batches as f64
        }
    }

    /// Consumer calls that found nothing, per item actually received.
    /// High values mean the consumer outruns the producers and the
    /// loop degenerates into a spin.
    fn empty_ratio(&self) -> f64 {
        if self.items == 0 {
            0.0
        } else {
            self.empty as f64 / self.items as f64
        }
    }
}

struct Samples {
    durations: Vec<Duration>,
    items: u64,
}

impl Samples {
    const fn new(items: u64) -> Self {
        Self {
            durations: Vec::new(),
            items,
        }
    }

    fn push(&mut self, d: Duration) {
        self.durations.push(d);
    }

    fn rate(&self, d: Duration) -> f64 {
        self.items as f64 / d.as_secs_f64() / 1e6
    }

    fn report(&mut self, strategy: Strategy, stats: &BatchStats) {
        self.durations.sort_unstable();
        let n = self.durations.len();
        let fast = self.rate(self.durations[0]);
        let med = self.rate(self.durations[n / 2]);
        let slow = self.rate(self.durations[n - 1]);
        eprintln!(
            "{strategy:<18} fast={fast:>6.1} med={med:>6.1} slow={slow:>6.1} M/s  \
             mean_batch={:>7.1} max_batch={:<6} empty/item={:>6.2}",
            stats.mean(),
            stats.max,
            stats.empty_ratio()
        );
    }
}

struct Probe {
    producers: u64,
    per_producer: u64,
    cap: usize,
    /// Per-item consumer work. Raising this stops the consumer from
    /// outrunning the producers, which is what lets batches grow.
    consumer_work: u32,
}

impl Probe {
    fn new(producers: u64, per_producer: u64, consumer_work: u32) -> Self {
        let cap = (1024 * producers as usize).next_power_of_two();
        Self {
            producers,
            per_producer,
            cap,
            consumer_work,
        }
    }

    #[inline]
    fn work(&self, v: u64) -> u64 {
        let mut x = v;
        for _ in 0..self.consumer_work {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            x = black_box(x);
        }
        x
    }

    const fn total(&self) -> u64 {
        self.producers * self.per_producer
    }

    fn run(&self, strategy: Strategy) -> (Duration, BatchStats) {
        let total = self.total();
        let per = self.per_producer;
        let start = Instant::now();

        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::at_least(self.cap)).split();

        let handles: Vec<_> = (0..self.producers)
            .map(|_| {
                let p = producer.clone();
                thread::spawn(move || {
                    for i in 0..per {
                        while p.push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();

        let mut stats = BatchStats::new();
        let mut received = 0u64;
        while received < total {
            let n = match strategy {
                Strategy::Pop => match consumer.pop() {
                    Some(v) => {
                        black_box(self.work(v));
                        1
                    }
                    None => 0,
                },
                Strategy::Drain => consumer.drain(|v| {
                    black_box(self.work(v));
                }),
                Strategy::DrainUpTo(k) => consumer.drain_up_to(k, |v| {
                    black_box(self.work(v));
                }),
            };
            stats.record(n);
            if n > 0 {
                received += n as u64;
            } else {
                std::hint::spin_loop();
            }
        }

        for h in handles {
            h.join().unwrap();
        }
        (start.elapsed(), stats)
    }

    fn sweep(&self, strategy: Strategy, iters: usize) {
        let mut samples = Samples::new(self.total());
        let mut stats = BatchStats::new();
        for _ in 0..iters {
            let (d, s) = self.run(strategy);
            samples.push(d);
            stats.merge(&s);
        }
        samples.report(strategy, &stats);
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let producers: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(12);
    let per_producer: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(5_000);
    let iters: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);
    let consumer_work: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(0);

    let probe = Probe::new(producers, per_producer, consumer_work);
    eprintln!(
        "producers={producers} per_producer={per_producer} total={} cap={} iters={iters} \
         consumer_work={consumer_work}",
        probe.total(),
        probe.cap
    );

    probe.sweep(Strategy::Pop, iters);
    probe.sweep(Strategy::Drain, iters);
    for k in [8usize, 32, 64, 256, 1024] {
        probe.sweep(Strategy::DrainUpTo(k), iters);
    }
}
