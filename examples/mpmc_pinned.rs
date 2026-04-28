// Pinned-thread version of mpmc_perf for testing whether
// scheduling/SMT placement explains the p8_q8 variance.
//
// Usage: mpmc_pinned <P> <Q> <total_items> <iters> <strategy>
//
// Strategies:
//   spread     — pin to even cpus (one per physical core); will oversubscribe at p+q>8
//   smt_pair   — pair (producer_i, consumer_i) on same physical core (cpu i*2, cpu i*2+1)
//   smt_split  — producers on cpus 0..p, consumers on cpus 8..(8+q) (other SMT siblings)
//   none       — no pinning (baseline)

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpmc::RingBuffer;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const TOTAL_CAPACITY: usize = 1024;

fn pin_to_cpu(cpu: usize) {
    // Raw syscall sched_setaffinity (Linux x86_64 = 203). Avoids
    // a dev-dependency on `libc`. cpu_set_t is just a bitmask; we
    // only ever set one bit, so a u128 (128 bits ≥ 16 cpus) suffices.
    unsafe {
        let mask: u128 = 1u128 << cpu;
        let mask_bytes = mask.to_le_bytes();
        // sys_sched_setaffinity(pid=0, size, mask_ptr)
        let ret: i64;
        std::arch::asm!(
            "syscall",
            inout("rax") 203i64 => ret,
            in("rdi") 0i64,
            in("rsi") mask_bytes.len() as i64,
            in("rdx") mask_bytes.as_ptr(),
            out("rcx") _,
            out("r11") _,
        );
        let _ = ret;
    }
}

#[derive(Clone, Copy)]
enum Strategy {
    Spread,
    SmtPair,
    SmtSplit,
    None,
}

fn cpu_for(strategy: Strategy, role: Role, idx: usize, p: usize, q: usize) -> Option<usize> {
    let _ = (p, q);
    match strategy {
        Strategy::None => None,
        Strategy::Spread => {
            // One thread per physical core (use cpu 0,2,4,...). For p+q>8 this oversubscribes.
            let total_idx = match role {
                Role::Producer => idx,
                Role::Consumer => p + idx,
            };
            Some((total_idx * 2) % 16)
        }
        Strategy::SmtPair => {
            // Producer i on cpu (2*i), consumer i on cpu (2*i + 1) — same physical core.
            match role {
                Role::Producer => Some((idx * 2) % 16),
                Role::Consumer => Some(((idx * 2) % 16) + 1),
            }
        }
        Strategy::SmtSplit => {
            // Producers on cpu 0,2,4,...; consumers on cpu 1,3,5,... (different physical cores
            // — actually no: cpu 1 and cpu 0 are SMT siblings). Let me reverse:
            // Producers on the "first" SMT sibling of each core (0,2,4...); consumers on
            // the "second" (1,3,5...). With p=q=8 every core has 1 producer + 1 consumer.
            match role {
                Role::Producer => Some((idx * 2) % 16),
                Role::Consumer => Some((idx * 2 + 1) % 16),
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Role {
    Producer,
    Consumer,
}

fn run_once(p: usize, q: usize, total_items: u64, strategy: Strategy) -> std::time::Duration {
    let per_producer = total_items / p as u64;
    let actual_total = per_producer * p as u64;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(TOTAL_CAPACITY)).split();
    let received = Arc::new(AtomicUsize::new(0));
    let target = actual_total as usize;
    const FLUSH_EVERY: usize = 64;

    let consumers: Vec<_> = (0..q)
        .map(|cidx| {
            let c = consumer.clone();
            let r = received.clone();
            thread::spawn(move || {
                if let Some(cpu) = cpu_for(strategy, Role::Consumer, cidx, p, q) {
                    pin_to_cpu(cpu);
                }
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
                if let Some(cpu) = cpu_for(strategy, Role::Producer, tid, p, q) {
                    pin_to_cpu(cpu);
                }
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
    let strategy_s = args
        .next()
        .expect("strategy: spread|smt_pair|smt_split|none");
    let strategy = match strategy_s.as_str() {
        "spread" => Strategy::Spread,
        "smt_pair" => Strategy::SmtPair,
        "smt_split" => Strategy::SmtSplit,
        "none" => Strategy::None,
        s => panic!("unknown strategy {s}"),
    };

    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        samples.push(run_once(p, q, total_items, strategy));
    }
    samples.sort();
    let to_mps = |d: std::time::Duration| (total_items as f64) / d.as_secs_f64() / 1e6;
    let n = samples.len();
    let min = samples[0];
    let p10 = samples[n / 10];
    let med = samples[n / 2];
    let p90 = samples[(n * 9) / 10];
    let max = samples[n - 1];
    eprintln!(
        "P={p} Q={q} strategy={strategy_s} fast={:.2} p90={:.2} med={:.2} p10={:.2} slow={:.2}",
        to_mps(min),
        to_mps(p10),
        to_mps(med),
        to_mps(p90),
        to_mps(max)
    );
}
