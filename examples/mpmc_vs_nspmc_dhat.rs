//! dhat heap profiling: mpmc vs N-SPMC round-robin.
//!
//! Runs both designs back-to-back at identical (P, Q, total_items)
//! shape, gated on the `dhat-heap` feature so `dhat::Profiler` is
//! installed as the global allocator. Output goes to `dhat-heap.json`
//! in the cwd; load it at https://nnethercote.github.io/dh_view/dh_view.html
//! to compare allocation profiles.
//!
//! Usage:
//!   cargo run --release --example mpmc_vs_nspmc_dhat \
//!     --features dhat-heap -- <P> <Q> <total_items>
//!
//! Without the feature, the example still compiles and runs but
//! produces no profile (the global-allocator hook is gated out).

use quetzalcoatl::capacity::Capacity;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const TOTAL_CAPACITY: usize = 1024;
const FLUSH_EVERY: usize = 64;

fn run_mpmc(p: usize, q: usize, total_items: u64) -> std::time::Duration {
    use quetzalcoatl::mpmc::RingBuffer;
    let per = total_items / p as u64;
    let actual = per * p as u64;
    let target = actual as usize;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(TOTAL_CAPACITY)).split();
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

fn shard_cap(total: usize, n_rings: usize) -> Capacity {
    let per = (total / n_rings).max(2).next_power_of_two();
    Capacity::exact(per)
}

fn run_n_spmc(p: usize, q: usize, total_items: u64) -> std::time::Duration {
    use quetzalcoatl::spmc::RingBuffer as SpmcRing;
    use quetzalcoatl::spsc::RingBuffer as SpscRing;
    let per = total_items / p as u64;
    let actual = per * p as u64;
    let target = actual as usize;
    let cap = shard_cap(TOTAL_CAPACITY, p + q);

    let mut spsc_producers = Vec::with_capacity(p);
    let mut spsc_consumers = Vec::with_capacity(p);
    for _ in 0..p {
        let (sp, sc) = SpscRing::<u64>::new(cap).split();
        spsc_producers.push(sp);
        spsc_consumers.push(sc);
    }
    let mut spmc_producers = Vec::with_capacity(q);
    let mut spmc_consumers = Vec::with_capacity(q);
    for _ in 0..q {
        let (sp, sc) = SpmcRing::<u64>::new(cap).split();
        spmc_producers.push(sp);
        spmc_consumers.push(sc);
    }
    let received = Arc::new(AtomicUsize::new(0));

    let consumer_handles: Vec<_> = spmc_consumers
        .into_iter()
        .map(|c| {
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
                        if r.load(Ordering::Relaxed) >= target
                            || (c.is_closed() && c.pop().is_none())
                        {
                            while c.pop().is_some() {}
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    let egress_handle = {
        let r = received.clone();
        thread::spawn(move || {
            let n_in = spsc_consumers.len();
            let n_out = spmc_producers.len();
            let mut i = 0usize;
            let mut j = 0usize;
            let mut forwarded = 0usize;
            loop {
                let val_opt = spsc_consumers[i].pop();
                i = (i + 1) % n_in;
                if let Some(mut v) = val_opt {
                    loop {
                        match spmc_producers[j].push(v) {
                            Ok(()) => {
                                j = (j + 1) % n_out;
                                forwarded += 1;
                                break;
                            }
                            Err(returned) => {
                                v = returned;
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

    let producer_handles: Vec<_> = spsc_producers
        .into_iter()
        .enumerate()
        .map(|(tid, sp)| {
            thread::spawn(move || {
                for i in 0..per {
                    let v = (tid as u64) * per + i;
                    while sp.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    let start = Instant::now();
    for h in producer_handles {
        h.join().unwrap();
    }
    egress_handle.join().unwrap();
    for h in consumer_handles {
        h.join().unwrap();
    }
    start.elapsed()
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "both".into());
    let p: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(8);
    let q: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(8);
    let total_items: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4_000_000);

    let report = |name: &str, d: std::time::Duration| {
        eprintln!(
            "  {name} P={p} Q={q} {total_items} items in {d:?} ({:.2} M/s)",
            total_items as f64 / d.as_secs_f64() / 1e6
        );
    };

    match mode.as_str() {
        "scan" => {
            eprintln!("=== mpmc only ===");
            report("mpmc", run_mpmc(p, q, total_items));
        }
        "nspmc" => {
            eprintln!("=== n_spmc only ===");
            report("n_spmc", run_n_spmc(p, q, total_items));
        }
        _ => {
            eprintln!("=== mpmc ===");
            report("mpmc", run_mpmc(p, q, total_items));
            eprintln!("=== n_spmc round-robin ===");
            report("n_spmc", run_n_spmc(p, q, total_items));
        }
    }

    #[cfg(feature = "dhat-heap")]
    eprintln!(
        "\nWrote dhat-heap.json — open https://nnethercote.github.io/dh_view/dh_view.html to view"
    );
    #[cfg(not(feature = "dhat-heap"))]
    eprintln!("\n(re-run with `--features dhat-heap` to capture an allocation profile)");
}
