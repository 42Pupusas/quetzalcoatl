// Standalone driver for perf-profiling the mpsc hot path.
//
// The matched bench measures a whole workload: thread spawn, ring
// allocation, and the spin/park policy all land inside the timed
// region, and divan's sample loop buries the ring operations under
// closure frames that perf reports as one opaque symbol. That shape
// answers "which channel is faster". It cannot answer "where does a
// push spend its cycles".
//
// This driver runs one mode per process. Each mode isolates one cost:
//
//   solo    one thread, no peer. Every line it touches stays in its
//           own L1 in the M state. Whatever this costs is the
//           instruction and barrier cost of the algorithm alone.
//   pair    one producer, one consumer, both spinning. Adds exactly
//           one thing to solo: the lines now move between cores.
//   multi   N producers, one consumer. Adds contention between
//           producers on the shared tail.
//
// solo isolates barrier cost. pair minus solo is the coherence cost.
// multi minus pair is the contention cost.
//
// Usage: mpsc_hot <solo|pair|multi> <items> [producers] [capacity] [qz|crossbeam]

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::RingBuffer;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const DEFAULT_CAPACITY: usize = 8192;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Impl {
    Qz,
    Crossbeam,
}

impl Impl {
    fn parse(raw: &str) -> Self {
        match raw {
            "qz" | "quetzalcoatl" => Self::Qz,
            "crossbeam" | "cb" => Self::Crossbeam,
            other => panic!("unknown impl {other}; want qz or crossbeam"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Qz => "qz",
            Self::Crossbeam => "crossbeam",
        }
    }
}

struct Config {
    mode: Mode,
    items: u64,
    producers: usize,
    capacity: usize,
    which: Impl,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Solo,
    Pair,
    Multi,
}

impl Mode {
    fn parse(raw: &str) -> Self {
        match raw {
            "solo" => Self::Solo,
            "pair" => Self::Pair,
            "multi" => Self::Multi,
            other => panic!("unknown mode {other}; want solo, pair, or multi"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Solo => "solo",
            Self::Pair => "pair",
            Self::Multi => "multi",
        }
    }
}

impl Config {
    fn from_args() -> Self {
        let mut args = std::env::args().skip(1);
        let mode = Mode::parse(&args.next().expect("mode: solo, pair, or multi"));
        let items: u64 = args.next().and_then(|s| s.parse().ok()).expect("items");
        let producers: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(1);
        let capacity: usize = args
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_CAPACITY);
        let which = args.next().map_or(Impl::Qz, |s| Impl::parse(&s));
        Self {
            mode,
            items,
            producers,
            capacity,
            which,
        }
    }

    fn run(&self) -> Report {
        match (self.which, self.mode) {
            (Impl::Qz, Mode::Solo) => self.run_solo(),
            (Impl::Qz, Mode::Pair) => self.run_pair(),
            (Impl::Qz, Mode::Multi) => self.run_multi(),
            (Impl::Crossbeam, Mode::Solo) => self.run_solo_crossbeam(),
            (Impl::Crossbeam, Mode::Pair) => self.run_threaded_crossbeam(1),
            (Impl::Crossbeam, Mode::Multi) => self.run_threaded_crossbeam(self.producers),
        }
    }

    // One thread. Push then pop, one item at a time, so the ring never
    // holds more than one item and the same slot stays hot in L1.
    // No peer thread exists, so no line ever leaves this core.
    fn run_solo(&self) -> Report {
        let (producer, mut consumer) =
            RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();

        let start = Instant::now();
        for i in 0..self.items {
            assert!(producer.push(black_box(i)).is_ok());
            assert!(consumer.pop().is_some());
        }
        let elapsed = start.elapsed();

        Report::new(self.mode, self.which, self.items, 1, elapsed)
    }

    fn run_solo_crossbeam(&self) -> Report {
        let (tx, rx) = crossbeam_channel::bounded::<u64>(self.capacity);

        let start = Instant::now();
        for i in 0..self.items {
            assert!(tx.try_send(black_box(i)).is_ok());
            assert!(rx.try_recv().is_ok());
        }
        let elapsed = start.elapsed();

        Report::new(self.mode, self.which, self.items, 1, elapsed)
    }

    fn run_threaded_crossbeam(&self, producers: usize) -> Report {
        let per_producer = self.items / producers as u64;
        let total = per_producer * producers as u64;

        let (tx, rx) = crossbeam_channel::bounded::<u64>(self.capacity);
        let go = Arc::new(AtomicBool::new(false));

        let handles: Vec<_> = (0..producers)
            .map(|_| {
                let t = tx.clone();
                let go = go.clone();
                thread::spawn(move || {
                    while !go.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    for i in 0..per_producer {
                        while t.try_send(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        drop(tx);

        let start = Instant::now();
        go.store(true, Ordering::Release);

        let mut received = 0u64;
        while received < total {
            if let Ok(v) = rx.try_recv() {
                black_box(v);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        let elapsed = start.elapsed();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received, total);

        Report::new(self.mode, self.which, total, producers, elapsed)
    }

    fn run_pair(&self) -> Report {
        self.run_threaded(1)
    }

    fn run_multi(&self) -> Report {
        self.run_threaded(self.producers)
    }

    // Threads spawn before the clock starts and are joined after it
    // stops, so spawn cost stays out of the measurement. Producers
    // wait on `go` so they all start inside the timed region.
    fn run_threaded(&self, producers: usize) -> Report {
        let per_producer = self.items / producers as u64;
        let total = per_producer * producers as u64;

        let (producer, mut consumer) =
            RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();
        let go = Arc::new(AtomicBool::new(false));

        let handles: Vec<_> = (0..producers)
            .map(|_| {
                let p = producer.clone();
                let go = go.clone();
                thread::spawn(move || {
                    while !go.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    for i in 0..per_producer {
                        while p.push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        drop(producer);

        let start = Instant::now();
        go.store(true, Ordering::Release);

        let mut received = 0u64;
        while received < total {
            if let Some(v) = consumer.pop() {
                black_box(v);
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        let elapsed = start.elapsed();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received, total);

        Report::new(self.mode, self.which, total, producers, elapsed)
    }
}

struct Report {
    mode: Mode,
    which: Impl,
    items: u64,
    producers: usize,
    elapsed: std::time::Duration,
}

impl Report {
    const fn new(
        mode: Mode,
        which: Impl,
        items: u64,
        producers: usize,
        elapsed: std::time::Duration,
    ) -> Self {
        Self {
            mode,
            which,
            items,
            producers,
            elapsed,
        }
    }

    fn print(&self) {
        let secs = self.elapsed.as_secs_f64();
        let per_item_ns = secs * 1e9 / self.items as f64;
        let mitems = self.items as f64 / secs / 1e6;
        println!(
            "{:<10} {:<6} producers={:<2} items={:<10} {:>8.3} ms {:>8.2} ns/item {:>8.2} Mitem/s",
            self.which.name(),
            self.mode.name(),
            self.producers,
            self.items,
            secs * 1e3,
            per_item_ns,
            mitems,
        );
    }
}

fn main() {
    let config = Config::from_args();
    config.run().print();
}
