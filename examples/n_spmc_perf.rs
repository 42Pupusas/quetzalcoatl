// Standalone driver for perf-profiling the N-SPMC-with-egress design.
//
// Usage: n_spmc_perf <P> <Q> <total_items>
//
// Mirror of mpmc_perf for direct cycles/IPC/cache-miss comparison.

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spmc::RingBuffer as SpmcRing;
use quetzalcoatl::spsc::RingBuffer as SpscRing;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const TOTAL_CAPACITY: usize = 1024;

fn shard_cap(total: usize, n_rings: usize) -> Capacity {
    let per = (total / n_rings).max(2).next_power_of_two();
    Capacity::exact(per)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let p: usize = args.next().and_then(|s| s.parse().ok()).expect("P");
    let q: usize = args.next().and_then(|s| s.parse().ok()).expect("Q");
    let total_items: u64 = args.next().and_then(|s| s.parse().ok()).expect("total");

    let per_producer = total_items / p as u64;
    let actual_total = per_producer * p as u64;
    let target = actual_total as usize;
    let cap = shard_cap(TOTAL_CAPACITY, p + q);

    let mut spsc_p = Vec::with_capacity(p);
    let mut spsc_c = Vec::with_capacity(p);
    for _ in 0..p {
        let (sp, sc) = SpscRing::<u64>::new(cap).split();
        spsc_p.push(sp);
        spsc_c.push(sc);
    }
    let mut spmc_p = Vec::with_capacity(q);
    let mut spmc_c = Vec::with_capacity(q);
    for _ in 0..q {
        let (sp, sc) = SpmcRing::<u64>::new(cap).split();
        spmc_p.push(sp);
        spmc_c.push(sc);
    }

    let received = Arc::new(AtomicUsize::new(0));

    let consumers: Vec<_> = spmc_c
        .into_iter()
        .map(|c| {
            let r = received.clone();
            thread::spawn(move || loop {
                if let Some(v) = c.pop() {
                    black_box(v);
                    r.fetch_add(1, Ordering::Relaxed);
                } else if r.load(Ordering::Relaxed) >= target
                    || (c.is_closed() && c.pop().is_none())
                {
                    while c.pop().is_some() {}
                    break;
                } else {
                    std::hint::spin_loop();
                }
            })
        })
        .collect();

    let egress = {
        let r = received.clone();
        thread::spawn(move || {
            let n_in = spsc_c.len();
            let n_out = spmc_p.len();
            let mut i = 0usize;
            let mut j = 0usize;
            let mut forwarded = 0usize;
            loop {
                let v = spsc_c[i].pop();
                i = (i + 1) % n_in;
                if let Some(mut v) = v {
                    loop {
                        match spmc_p[j].push(v) {
                            Ok(()) => {
                                j = (j + 1) % n_out;
                                forwarded += 1;
                                break;
                            }
                            Err(ret) => {
                                v = ret;
                                std::hint::spin_loop();
                            }
                        }
                    }
                } else if forwarded >= target && r.load(Ordering::Relaxed) >= target {
                    break;
                } else {
                    std::hint::spin_loop();
                }
            }
        })
    };

    let producers: Vec<_> = spsc_p
        .into_iter()
        .enumerate()
        .map(|(tid, sp)| {
            thread::spawn(move || {
                for i in 0..per_producer {
                    let v = (tid as u64) * per_producer + i;
                    while sp.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    let start = std::time::Instant::now();
    for h in producers {
        h.join().unwrap();
    }
    egress.join().unwrap();
    for h in consumers {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    eprintln!(
        "N-SPMC P={} Q={}: {} items in {:?} ({:.2} M/s)",
        p,
        q,
        actual_total,
        elapsed,
        actual_total as f64 / elapsed.as_secs_f64() / 1e6
    );
}
