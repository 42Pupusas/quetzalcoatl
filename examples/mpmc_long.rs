// Stacked-iteration version of mpmc_perf for flamegraph capture.
// Runs the scenario `iters` times back-to-back so the profile spans
// long enough to be statistically representative.
//
// Usage: mpmc_long <P> <Q> <total_items> <iters>

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpmc::RingBuffer;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

fn run_once(p: usize, q: usize, total_items: u64, cap: usize) -> std::time::Duration {
    let per_producer = total_items / p as u64;
    let actual_total = per_producer * p as u64;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
    let received = Arc::new(AtomicUsize::new(0));
    let target = actual_total as usize;
    const FLUSH_EVERY: usize = 64;

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
                for i in 0..per_producer {
                    let v = (tid as u64) * per_producer + i;
                    while prod.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(producer);

    let start = std::time::Instant::now();
    for h in producers {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
    start.elapsed()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let p: usize = args.next().and_then(|s| s.parse().ok()).expect("P");
    let q: usize = args.next().and_then(|s| s.parse().ok()).expect("Q");
    let total_items: u64 = args.next().and_then(|s| s.parse().ok()).expect("total");
    let iters: usize = args.next().and_then(|s| s.parse().ok()).expect("iters");
    let cap: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(1024);

    let mut samples = Vec::with_capacity(iters);
    let global_start = std::time::Instant::now();
    for _ in 0..iters {
        samples.push(run_once(p, q, total_items, cap));
    }
    let global = global_start.elapsed();

    samples.sort();
    let to_mps = |d: std::time::Duration| (total_items as f64) / d.as_secs_f64() / 1e6;
    let n = samples.len();
    let min = samples[0];
    let p10 = samples[n / 10];
    let med = samples[n / 2];
    let p90 = samples[(n * 9) / 10];
    let max = samples[n - 1];

    eprintln!("MPMC_SCAN P={p} Q={q} iters={iters} total_wall={global:?}");
    eprintln!("  fast (max thrpt): {:>6.2} M/s ({:?})", to_mps(min), min);
    eprintln!("  p90 thrpt:        {:>6.2} M/s ({:?})", to_mps(p10), p10);
    eprintln!("  median thrpt:     {:>6.2} M/s ({:?})", to_mps(med), med);
    eprintln!("  p10 thrpt:        {:>6.2} M/s ({:?})", to_mps(p90), p90);
    eprintln!("  slow (min thrpt): {:>6.2} M/s ({:?})", to_mps(max), max);
}
