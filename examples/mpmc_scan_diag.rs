// Diagnostic: run mpmc_scan workload, dump ring state on stall.

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpmc_scan::RingBuffer;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const TOTAL_CAPACITY: usize = 1024;

fn main() {
    let total: u64 = 5_000_000;
    let p = 2usize;
    let q = 2usize;
    let per_p = total / p as u64;
    let actual_total = (per_p * p as u64) as usize;

    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(TOTAL_CAPACITY)).split();
    let received = Arc::new(AtomicUsize::new(0));
    let target = actual_total;

    let consumer_handles: Vec<(thread::JoinHandle<()>, Arc<AtomicUsize>)> = (0..q)
        .map(|cid| {
            let c = consumer.clone();
            let r = received.clone();
            let popped_count = Arc::new(AtomicUsize::new(0));
            let pc = popped_count.clone();
            let h = thread::spawn(move || {
                let _ = cid;
                loop {
                    if let Some(v) = c.pop() {
                        black_box(v);
                        pc.fetch_add(1, Ordering::Relaxed);
                        r.fetch_add(1, Ordering::Relaxed);
                    } else if r.load(Ordering::Relaxed) >= target {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
            (h, popped_count)
        })
        .collect();

    // Diagnostic snapshot consumer (read-only access to queue state).
    let diag_consumer = consumer.clone();
    drop(consumer);

    let producers: Vec<_> = (0..p)
        .map(|tid| {
            let prod = producer.clone();
            thread::spawn(move || {
                for i in 0..per_p {
                    let v = (tid as u64) * per_p + i;
                    while prod.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(producer);

    let watchdog = {
        let r = received.clone();
        thread::spawn(move || {
            let mut prev = 0;
            let mut stall_ticks = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(2));
                let cur = r.load(Ordering::Relaxed);
                eprintln!("[wd] received={cur}/{target}");
                if cur >= target {
                    break;
                }
                if cur == prev {
                    stall_ticks += 1;
                    if stall_ticks >= 2 {
                        eprintln!("\n=== STALL DUMP at received={cur} ===");
                        let (scan, claim, ready, done) = diag_consumer.debug_snapshot();
                        eprintln!("diag consumer: next_scan={scan} claim={claim}");
                        // Find slots with state==1 (published, unclaimed).
                        let mut published = Vec::new();
                        for s in 0..TOTAL_CAPACITY {
                            let v = ready[s];
                            let delta = v.wrapping_sub(s);
                            let state = delta % TOTAL_CAPACITY;
                            if state == 1 {
                                let round_pos = v - 1;
                                published.push((s, round_pos));
                            }
                        }
                        eprintln!("published-unclaimed slots: {}", published.len());
                        for (s, pos) in published.iter().take(20) {
                            eprintln!(
                                "  slot[{s}] pos={pos} ready={} done[s]={}",
                                ready[*s], done[*s]
                            );
                        }
                        // Slots whose done lags pos+cap (consumer never released).
                        let mut blocked_done = Vec::new();
                        for s in 0..TOTAL_CAPACITY {
                            let v = ready[s];
                            let delta = v.wrapping_sub(s);
                            let state = delta % TOTAL_CAPACITY;
                            // For each slot, find the ready's encoded round_pos.
                            let round_pos_in_ready = match state {
                                0 => v,     // free at v
                                1 => v - 1, // published at v-1
                                2 => v - 2, // claimed at v-2
                                _ => continue,
                            };
                            // done[s] should be round_pos_in_ready (free for
                            // current round) or round_pos_in_ready+cap (released).
                            let d = done[s];
                            if d != round_pos_in_ready && d != round_pos_in_ready + TOTAL_CAPACITY {
                                blocked_done.push((s, round_pos_in_ready, d, state));
                            }
                        }
                        eprintln!("done-state mismatches: {}", blocked_done.len());
                        for (s, rp, d, state) in blocked_done.iter().take(20) {
                            eprintln!("  slot[{s}] state={state} ready_round_pos={rp} done={d}");
                        }
                        std::process::exit(2);
                    }
                } else {
                    stall_ticks = 0;
                }
                prev = cur;
            }
        })
    };

    for h in producers {
        h.join().unwrap();
    }
    for (h, count) in consumer_handles {
        h.join().unwrap();
        eprintln!("consumer popped {}", count.load(Ordering::Relaxed));
    }
    drop(watchdog);
    eprintln!("DONE");
}
