// Standalone driver for `perf stat` on the SPMC producer hot path.
//
// Usage:
//   spmc_perf <num_consumers> <total_items>
//
// num_consumers == 0 → producer-only baseline (drains inline, no other threads).
// Otherwise spawns N consumer threads racing on pop().

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spmc::RingBuffer;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

fn main() {
    let mut args = std::env::args().skip(1);
    let num_consumers: usize = args
        .next()
        .and_then(|s| s.parse().ok())
        .expect("usage: spmc_perf <num_consumers> <total_items>");
    let total_items: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .expect("usage: spmc_perf <num_consumers> <total_items>");

    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(8192)).split();
    let remaining = Arc::new(AtomicUsize::new(total_items as usize));

    let start;

    if num_consumers == 0 {
        drop(consumer);
        // Fresh ring so we own both ends on this thread.
        let (p, c) = RingBuffer::<u64>::new(Capacity::exact(8192)).split();
        start = std::time::Instant::now();
        for i in 0..total_items {
            while p.push(black_box(i)).is_err() {
                while c.pop().is_some() {}
            }
        }
        while c.pop().is_some() {}
    } else {
        let handles: Vec<_> = (0..num_consumers)
            .map(|_| {
                let c = consumer.clone();
                let rem = remaining.clone();
                thread::spawn(move || {
                    while rem.load(Ordering::Relaxed) > 0 {
                        if c.pop().is_some() {
                            rem.fetch_sub(1, Ordering::Relaxed);
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        drop(consumer);

        start = std::time::Instant::now();
        for i in 0..total_items {
            while producer.push(black_box(i)).is_err() {
                std::hint::spin_loop();
            }
        }
        // Drop producer so any overshooting consumer sees `closed`
        // and exits cleanly instead of hanging.
        drop(producer);
        for h in handles {
            h.join().unwrap();
        }
    }

    let elapsed = start.elapsed();
    let mps = total_items as f64 / elapsed.as_secs_f64() / 1e6;
    eprintln!(
        "consumers={num_consumers} items={total_items} elapsed={:.3}ms throughput={mps:.1} Melem/s",
        elapsed.as_secs_f64() * 1e3
    );
}
