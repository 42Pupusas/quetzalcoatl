// Probe: why does broadcast `arc_2kb_4consumers` lose to
// `clone_2kb_4consumers` when the Arc path exists to avoid the copy?
//
// The Arc path trades one 2KB memcpy per consumer for:
//   1. one heap allocation per push, freed on a different thread,
//   2. contended refcount RMWs on a single shared cacheline
//      (1 clone per consumer pop + 1 decrement per drop).
//
// Item size decides the winner: the memcpy cost scales with the
// payload, the refcount cost does not. This sweeps payload size to
// find the crossover, and reports allocation cost separately so the
// two components can be told apart.
//
// Usage: broadcast_arc_cost <consumers> <items> <iters>

use quetzalcoatl::broadcast::arc::ArcRingBuffer;
use quetzalcoatl::broadcast::RingBuffer;
use quetzalcoatl::capacity::Capacity;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const CAP: usize = 256;

/// A payload whose size is fixed at compile time, so the memcpy the
/// clone path pays is real and not optimized away.
#[derive(Clone)]
struct Payload<const N: usize> {
    data: [u8; N],
}

impl<const N: usize> Payload<N> {
    fn new(seed: u8) -> Self {
        Self { data: [seed; N] }
    }
}

struct Timings {
    samples: Vec<Duration>,
    items: u64,
}

impl Timings {
    const fn new(items: u64) -> Self {
        Self {
            samples: Vec::new(),
            items,
        }
    }

    fn push(&mut self, d: Duration) {
        self.samples.push(d);
    }

    fn median_rate(&mut self) -> f64 {
        self.samples.sort_unstable();
        let d = self.samples[self.samples.len() / 2];
        self.items as f64 / d.as_secs_f64() / 1e6
    }
}

/// One (payload size) measurement across both ring flavours.
struct SizeProbe {
    consumers: usize,
    items: u64,
    iters: usize,
}

impl SizeProbe {
    const fn new(consumers: usize, items: u64, iters: usize) -> Self {
        Self {
            consumers,
            items,
            iters,
        }
    }

    /// Plain ring: every consumer pop performs a full `T::clone()`.
    fn run_clone<const N: usize>(&self) -> Duration {
        let start = Instant::now();
        let (producer, consumer) =
            RingBuffer::<Payload<N>>::new(Capacity::exact(CAP), self.consumers + 1).split();
        let mut consumers: Vec<_> = (0..self.consumers - 1).map(|_| consumer.clone()).collect();
        consumers.push(consumer);

        let items = self.items;
        let producer_handle = thread::spawn(move || {
            for i in 0..items {
                while producer
                    .push(black_box(Payload::<N>::new(i as u8)))
                    .is_err()
                {
                    std::hint::spin_loop();
                }
            }
        });

        let handles: Vec<_> = consumers
            .into_iter()
            .map(|mut c| {
                thread::spawn(move || {
                    let mut received = 0u64;
                    while received < items {
                        if let Some(v) = c.pop() {
                            black_box(v.data[0]);
                            received += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();

        producer_handle.join().unwrap();
        for h in handles {
            h.join().unwrap();
        }
        start.elapsed()
    }

    /// Arc ring: one allocation per push, refcount clone per pop.
    fn run_arc<const N: usize>(&self) -> Duration {
        let start = Instant::now();
        let (producer, consumer) =
            ArcRingBuffer::<Payload<N>>::new(Capacity::exact(CAP), self.consumers + 1).split();
        let mut consumers: Vec<_> = (0..self.consumers - 1).map(|_| consumer.clone()).collect();
        consumers.push(consumer);

        let items = self.items;
        let producer_handle = thread::spawn(move || {
            for i in 0..items {
                while producer
                    .push(black_box(Payload::<N>::new(i as u8)))
                    .is_err()
                {
                    std::hint::spin_loop();
                }
            }
        });

        let handles: Vec<_> = consumers
            .into_iter()
            .map(|mut c| {
                thread::spawn(move || {
                    let mut received = 0u64;
                    while received < items {
                        if let Some(v) = c.pop() {
                            black_box(v.data[0]);
                            received += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();

        producer_handle.join().unwrap();
        for h in handles {
            h.join().unwrap();
        }
        start.elapsed()
    }

    /// Allocation cost in isolation: same `Arc::new` per item and the
    /// same cross-thread free, with no ring and no contention. This
    /// separates "malloc is slow" from "the refcount line is hot".
    fn run_alloc_only<const N: usize>(&self) -> Duration {
        let start = Instant::now();
        let items = self.items;
        let (tx, rx) = std::sync::mpsc::channel::<Arc<Payload<N>>>();

        let producer_handle = thread::spawn(move || {
            for i in 0..items {
                tx.send(Arc::new(black_box(Payload::<N>::new(i as u8))))
                    .unwrap();
            }
        });
        let consumer_handle = thread::spawn(move || {
            let mut received = 0u64;
            while let Ok(v) = rx.recv() {
                black_box(v.data[0]);
                received += 1;
            }
            received
        });

        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
        start.elapsed()
    }

    fn sweep<const N: usize>(&self) {
        let mut clone_t = Timings::new(self.items);
        let mut arc_t = Timings::new(self.items);
        let mut alloc_t = Timings::new(self.items);
        for _ in 0..self.iters {
            clone_t.push(self.run_clone::<N>());
            arc_t.push(self.run_arc::<N>());
            alloc_t.push(self.run_alloc_only::<N>());
        }
        let clone_r = clone_t.median_rate();
        let arc_r = arc_t.median_rate();
        let alloc_r = alloc_t.median_rate();
        let winner = if arc_r > clone_r { "arc" } else { "clone" };
        eprintln!(
            "{N:>6}B  clone={clone_r:>6.2}  arc={arc_r:>6.2}  \
             alloc_only={alloc_r:>6.2} M/s  ratio={:>5.2}x  winner={winner}",
            arc_r / clone_r
        );
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let consumers: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);
    let items: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let iters: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(15);

    eprintln!("consumers={consumers} items={items} iters={iters} cap={CAP}");
    eprintln!("ratio > 1 means the Arc path wins");

    let probe = SizeProbe::new(consumers, items, iters);
    probe.sweep::<64>();
    probe.sweep::<256>();
    probe.sweep::<1024>();
    probe.sweep::<2048>();
    probe.sweep::<8192>();
    probe.sweep::<32768>();
}
