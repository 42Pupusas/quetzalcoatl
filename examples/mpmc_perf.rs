// Standalone driver for perf-profiling the relaxed-FIFO MPMC scan
// design. Mirror of mpmc_perf.rs for direct comparison.
//
// Usage: mpmc_perf <P> <Q> <total_items>

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpmc::RingBuffer;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const TOTAL_CAPACITY: usize = 1024;

fn main() {
    let mut args = std::env::args().skip(1);
    let p: usize = args.next().and_then(|s| s.parse().ok()).expect("P");
    let q: usize = args.next().and_then(|s| s.parse().ok()).expect("Q");
    let total_items: u64 = args.next().and_then(|s| s.parse().ok()).expect("total");

    let per_producer = total_items / p as u64;
    let actual_total = per_producer * p as u64;

    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(TOTAL_CAPACITY)).split();
    let received = Arc::new(AtomicUsize::new(0));
    let target = actual_total as usize;

    // Consumers buffer local counts and only flush to `received` every
    // FLUSH_EVERY items. This keeps the test-harness counter from
    // becoming the throughput bottleneck — without batching, both
    // consumers contend on the shared `received.fetch_add` line on
    // every pop, masking actual queue performance.
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
                        // Flush before checking termination so we
                        // don't undercount near the end.
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
    let elapsed = start.elapsed();

    eprintln!(
        "MPMC_SCAN P={} Q={}: {} items in {:?} ({:.2} M/s)",
        p,
        q,
        actual_total,
        elapsed,
        actual_total as f64 / elapsed.as_secs_f64() / 1e6
    );
}
