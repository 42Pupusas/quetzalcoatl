// Compares the four (push|pop) × (spin|block) combinations at
// the same workload, to show what each blocking variant costs
// when the matching backpressure regime actually fires.
//
// - pop_spin / pop_block: large ring (cap=1024) — consumer
//   sometimes idle. Tests pop_block vs spin-harness on consumer.
// - push_spin / push_block: small ring (cap=16) — producer
//   often blocked on full. Tests push_block vs spin-retry on
//   producer.
//
// Usage: mpmc_block <P> <Q> <total_items> <iters> <mode>
//   mode = pop_spin | pop_block | push_spin | push_block | all

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpmc::RingBuffer;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Ring capacity for the consumer-blocking comparisons. Large
/// enough that producers rarely backpressure.
const CAP_LARGE: usize = 1024;
/// Ring capacity for the producer-blocking comparisons. Small
/// enough that 8 producers × 32-batch frequently hit the
/// watermark-full path.
const CAP_SMALL: usize = 16;
const FLUSH_EVERY: usize = 64;

/// Consumer spins on `pop`; producer is unblocked (cap is large).
fn run_pop_spin(p: usize, q: usize, total_items: u64) -> Duration {
    let per = total_items / p as u64;
    let target = (per * p as u64) as usize;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(CAP_LARGE)).split();
    let received = Arc::new(AtomicUsize::new(0));

    let consumers: Vec<_> = (0..q)
        .map(|_| {
            let c = consumer.clone();
            let r = received.clone();
            thread::spawn(move || {
                let mut local = 0usize;
                loop {
                    if let Some(v) = c.pop() {
                        black_box(v);
                        local += 1;
                        if local == FLUSH_EVERY {
                            r.fetch_add(local, Ordering::Relaxed);
                            local = 0;
                        }
                    } else {
                        if local > 0 {
                            r.fetch_add(local, Ordering::Relaxed);
                            local = 0;
                        }
                        if r.load(Ordering::Relaxed) >= target {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(consumer);

    let producers: Vec<_> = (0..p)
        .map(|tid| {
            let prod = producer.clone();
            thread::spawn(move || {
                for i in 0..per {
                    let v = (tid as u64) * per + i;
                    while prod.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(producer);

    let start = Instant::now();
    for h in producers {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
    start.elapsed()
}

/// Consumer uses `pop_block`; producer is unblocked (cap is large).
fn run_pop_block(p: usize, q: usize, total_items: u64) -> Duration {
    let per = total_items / p as u64;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(CAP_LARGE)).split();

    let consumers: Vec<_> = (0..q)
        .map(|_| {
            let c = consumer.clone();
            thread::spawn(move || {
                let mut count = 0u64;
                while let Some(v) = c.pop_block() {
                    black_box(v);
                    count += 1;
                }
                count
            })
        })
        .collect();
    drop(consumer);

    let producers: Vec<_> = (0..p)
        .map(|tid| {
            let prod = producer.clone();
            thread::spawn(move || {
                for i in 0..per {
                    let v = (tid as u64) * per + i;
                    while prod.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(producer);

    let start = Instant::now();
    for h in producers {
        h.join().unwrap();
    }
    let mut total = 0u64;
    for h in consumers {
        total += h.join().unwrap();
    }
    let elapsed = start.elapsed();
    assert_eq!(total, per * p as u64, "lost or duplicated items");
    elapsed
}

/// Producer spins on `push` retry; ring is small (frequent full).
fn run_push_spin(p: usize, q: usize, total_items: u64) -> Duration {
    let per = total_items / p as u64;
    let target = (per * p as u64) as usize;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(CAP_SMALL)).split();
    let received = Arc::new(AtomicUsize::new(0));

    let consumers: Vec<_> = (0..q)
        .map(|_| {
            let c = consumer.clone();
            let r = received.clone();
            thread::spawn(move || {
                let mut local = 0usize;
                loop {
                    if let Some(v) = c.pop() {
                        black_box(v);
                        local += 1;
                        if local == FLUSH_EVERY {
                            r.fetch_add(local, Ordering::Relaxed);
                            local = 0;
                        }
                    } else {
                        if local > 0 {
                            r.fetch_add(local, Ordering::Relaxed);
                            local = 0;
                        }
                        if r.load(Ordering::Relaxed) >= target {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(consumer);

    let producers: Vec<_> = (0..p)
        .map(|tid| {
            let prod = producer.clone();
            thread::spawn(move || {
                for i in 0..per {
                    let v = (tid as u64) * per + i;
                    while prod.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(producer);

    let start = Instant::now();
    for h in producers {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
    start.elapsed()
}

/// Producer uses `push_block`; ring is small (frequent full).
fn run_push_block(p: usize, q: usize, total_items: u64) -> Duration {
    let per = total_items / p as u64;
    let target = (per * p as u64) as usize;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(CAP_SMALL)).split();
    let received = Arc::new(AtomicUsize::new(0));

    let consumers: Vec<_> = (0..q)
        .map(|_| {
            let c = consumer.clone();
            let r = received.clone();
            thread::spawn(move || {
                let mut local = 0usize;
                loop {
                    if let Some(v) = c.pop() {
                        black_box(v);
                        local += 1;
                        if local == FLUSH_EVERY {
                            r.fetch_add(local, Ordering::Relaxed);
                            local = 0;
                        }
                    } else {
                        if local > 0 {
                            r.fetch_add(local, Ordering::Relaxed);
                            local = 0;
                        }
                        if r.load(Ordering::Relaxed) >= target {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(consumer);

    let producers: Vec<_> = (0..p)
        .map(|tid| {
            let prod = producer.clone();
            thread::spawn(move || {
                for i in 0..per {
                    let v = (tid as u64) * per + i;
                    // push_block returns Err only if all consumers
                    // dropped — shouldn't happen here.
                    prod.push_block(v).expect("consumers dropped early");
                }
            })
        })
        .collect();
    drop(producer);

    let start = Instant::now();
    for h in producers {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
    start.elapsed()
}

fn percentiles(samples: &[Duration], items: u64) -> String {
    let mut s: Vec<Duration> = samples.to_vec();
    s.sort();
    let n = s.len();
    let to_mps = |d: Duration| (items as f64) / d.as_secs_f64() / 1e6;
    format!(
        "fast={:.1} p25={:.1} median={:.1} p75={:.1} slow={:.1} M/s",
        to_mps(s[0]),
        to_mps(s[n / 4]),
        to_mps(s[n / 2]),
        to_mps(s[(n * 3) / 4]),
        to_mps(s[n - 1]),
    )
}

fn main() {
    let mut args = std::env::args().skip(1);
    let p: usize = args.next().and_then(|s| s.parse().ok()).expect("P");
    let q: usize = args.next().and_then(|s| s.parse().ok()).expect("Q");
    let total: u64 = args.next().and_then(|s| s.parse().ok()).expect("total");
    let iters: usize = args.next().and_then(|s| s.parse().ok()).expect("iters");
    let mode = args.next().unwrap_or_else(|| "all".into());

    let run_mode = |label: &str, run: fn(usize, usize, u64) -> Duration| {
        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            samples.push(run(p, q, total));
        }
        eprintln!("  {label:<11} {}", percentiles(&samples, total));
    };

    eprintln!("P={p} Q={q} total={total} iters={iters}");
    match mode.as_str() {
        "pop_spin" => run_mode("pop_spin", run_pop_spin),
        "pop_block" => run_mode("pop_block", run_pop_block),
        "push_spin" => run_mode("push_spin", run_push_spin),
        "push_block" => run_mode("push_block", run_push_block),
        _ => {
            eprintln!("[cap={CAP_LARGE} — consumer blocking]");
            run_mode("pop_spin", run_pop_spin);
            run_mode("pop_block", run_pop_block);
            eprintln!("[cap={CAP_SMALL} — producer blocking]");
            run_mode("push_spin", run_push_spin);
            run_mode("push_block", run_push_block);
        }
    }
}
