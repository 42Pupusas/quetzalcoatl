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
        // Per-consumer quotas: each consumer counts its own pops, no
        // shared atomic counter on the hot path. Consumers loop until
        // they observe a `None` after the producer is closed (which
        // signals "no more items will ever arrive"). Total work is
        // distributed dynamically — fast consumers will pop more, slow
        // consumers fewer; we just need the sum to match total_items.
        let popped_counters: Vec<std::sync::Arc<std::sync::atomic::AtomicU64>> = (0..num_consumers)
            .map(|_| std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)))
            .collect();

        let handles: Vec<_> = popped_counters
            .iter()
            .cloned()
            .map(|popped| {
                let c = consumer.clone();
                thread::spawn(move || {
                    let mut local: u64 = 0;
                    loop {
                        if c.pop().is_some() {
                            local += 1;
                        } else if c.is_closed() {
                            // Producer is gone. Drain any remaining items
                            // (the producer may have published items the
                            // consumer hasn't seen yet), then exit.
                            while c.pop().is_some() {
                                local += 1;
                            }
                            break;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                    popped.store(local, std::sync::atomic::Ordering::Relaxed);
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
        let total_popped: u64 = popped_counters
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .sum();
        assert_eq!(
            total_popped, total_items,
            "consumers must pop exactly the items the producer pushed"
        );
    }

    let elapsed = start.elapsed();
    let mps = total_items as f64 / elapsed.as_secs_f64() / 1e6;
    eprintln!(
        "consumers={num_consumers} items={total_items} elapsed={:.3}ms throughput={mps:.1} Melem/s",
        elapsed.as_secs_f64() * 1e3
    );
}
