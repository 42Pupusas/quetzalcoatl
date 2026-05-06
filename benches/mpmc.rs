//! MPMC throughput vs N-SPMC-with-egress-fanout.
//!
//! Tests the architectural choice raised in user feedback: is a single
//! MPMC ring (M producers + N consumers all sharing one head/claim) faster
//! than a fan-out design where producers each push into a private SPSC,
//! a single egress thread round-robins into per-consumer SPMC rings, and
//! consumers pop from their dedicated rings?
//!
//! The MPMC design pays cacheline ping-pong on `head`/`claim` between all
//! consumers/producers. The N-SPMC design avoids that by giving each
//! consumer a private ring, but adds an egress-thread hop. Whether MPMC
//! wins depends on whether the contention saved exceeds the hop added —
//! and how much CPU work the egress does per item.
//!
//! # Layout parity
//!
//! Total buffering held equal across designs:
//! - MPMC: 1 ring of capacity C
//! - N-SPMC: P SPSC rings + Q SPMC rings, each capacity C/(P+Q)
//!
//! # Variants
//!
//! - `egress=quick`: egress thread is a no-op forwarder. Best case for
//!   N-SPMC (egress hop is just two queue ops).
//! - `egress=slow`: egress does ~500ns of black-box CPU work per item.
//!   Models real fan-out logic (routing, hashing, light parsing).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::capacity::Capacity;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Items pushed by each producer thread.
const ITEMS_PER_PRODUCER: u64 = 10_000;

/// Total ring buffering — split equally between rings in N-SPMC for
/// layout parity.
const TOTAL_CAPACITY: usize = 1024;

/// Egress per-item work for the `slow` variant. Tuned so each call is
/// ~500ns on a typical x86_64; black-box prevents the optimizer from
/// folding the loop away.
#[inline(never)]
fn slow_work(seed: u64) -> u64 {
    let mut x = seed;
    for _ in 0..64 {
        x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        x = black_box(x);
    }
    x
}

/// Round up `total` to next power of two ≥ `min_per_ring`. Ensures each
/// ring satisfies `Capacity::exact`'s power-of-two requirement while
/// keeping total buffering close to TOTAL_CAPACITY.
fn shard_cap(total: usize, n_rings: usize) -> Capacity {
    let per = (total / n_rings).max(2).next_power_of_two();
    Capacity::exact(per)
}

// ---------------------------------------------------------------------------
// Variant A: MPMC — P producers, Q consumers, one ring.
// ---------------------------------------------------------------------------

fn run_mpmc(p: usize, q: usize) -> Duration {
    use quetzalcoatl::mpmc::RingBuffer;

    let total_items = (p as u64) * ITEMS_PER_PRODUCER;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(TOTAL_CAPACITY)).split();
    let received = Arc::new(AtomicUsize::new(0));

    let consumers: Vec<_> = (0..q)
        .map(|_| {
            let c = consumer.clone();
            let r = received.clone();
            thread::spawn(move || {
                let target = total_items as usize;
                loop {
                    if let Some(v) = c.pop() {
                        black_box(v);
                        r.fetch_add(1, Ordering::Relaxed);
                    } else if r.load(Ordering::Relaxed) >= target {
                        break;
                    } else {
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
                for i in 0..ITEMS_PER_PRODUCER {
                    let v = (tid as u64) * ITEMS_PER_PRODUCER + i;
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

// ---------------------------------------------------------------------------
// Variant B: N-SPMC with egress fan-out.
//
// P producers → P SPSC rings → 1 egress thread → Q SPMC rings → Q consumers.
// ---------------------------------------------------------------------------

fn run_n_spmc(p: usize, q: usize, slow: bool) -> Duration {
    use quetzalcoatl::spmc::RingBuffer as SpmcRing;
    use quetzalcoatl::spsc::RingBuffer as SpscRing;

    let total_items = (p as u64) * ITEMS_PER_PRODUCER;
    let cap = shard_cap(TOTAL_CAPACITY, p + q);

    // Producer-side: P SPSC rings.
    let mut spsc_producers = Vec::with_capacity(p);
    let mut spsc_consumers = Vec::with_capacity(p);
    for _ in 0..p {
        let (sp, sc) = SpscRing::<u64>::new(cap).split();
        spsc_producers.push(sp);
        spsc_consumers.push(sc);
    }

    // Consumer-side: Q SPMC rings.
    let mut spmc_producers = Vec::with_capacity(q);
    let mut spmc_consumers = Vec::with_capacity(q);
    for _ in 0..q {
        let (sp, sc) = SpmcRing::<u64>::new(cap).split();
        spmc_producers.push(sp);
        spmc_consumers.push(sc);
    }

    let received = Arc::new(AtomicUsize::new(0));
    let target = total_items as usize;

    // Consumer threads: each owns one SPMC ring.
    let consumer_handles: Vec<_> = spmc_consumers
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

    // Egress thread: round-robin pop from SPSC[i], push into SPMC[j].
    // i advances on every poll attempt; j advances on every successful
    // forward (round-robins across consumers).
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
                    if slow {
                        v = slow_work(v);
                    }
                    // Round-robin push into the next SPMC ring; if it's
                    // full, retry on the same target (don't drop work).
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
                } else if forwarded >= target {
                    // All forwarded but consumers still draining.
                    std::hint::spin_loop();
                } else {
                    std::hint::spin_loop();
                }
            }
            // Dropping spmc_producers signals close to consumer SPMC rings.
        })
    };

    // Producer threads: each owns one SPSC ring.
    let producer_handles: Vec<_> = spsc_producers
        .into_iter()
        .enumerate()
        .map(|(tid, sp)| {
            thread::spawn(move || {
                for i in 0..ITEMS_PER_PRODUCER {
                    let v = (tid as u64) * ITEMS_PER_PRODUCER + i;
                    while sp.push(v).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    let start = std::time::Instant::now();
    for h in producer_handles {
        h.join().unwrap();
    }
    egress_handle.join().unwrap();
    for h in consumer_handles {
        h.join().unwrap();
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn bench_mpmc_vs_n_spmc(c: &mut Criterion) {
    // Representative shapes: balanced, producer-heavy, consumer-heavy.
    let shapes: &[(usize, usize)] = &[(2, 2), (4, 4), (8, 8), (4, 2), (2, 4)];

    for &(p, q) in shapes {
        let label = format!("p{p}_q{q}");
        let total_items = (p as u64) * ITEMS_PER_PRODUCER;

        let mut group = c.benchmark_group(format!("mpmc_vs_nspmc/{label}"));
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(3));
        group.throughput(Throughput::Elements(total_items));

        group.bench_with_input(BenchmarkId::new("mpmc", "quick"), &(p, q), |b, &(p, q)| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_mpmc(p, q);
                }
                total
            });
        });

        group.bench_with_input(
            BenchmarkId::new("n_spmc", "quick"),
            &(p, q),
            |b, &(p, q)| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_n_spmc(p, q, false);
                    }
                    total
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("n_spmc", "slow"), &(p, q), |b, &(p, q)| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_n_spmc(p, q, true);
                }
                total
            });
        });

        group.finish();
    }
}

// ---------------------------------------------------------------------------
// Large-struct throughput: copy (push/pop) vs zero-copy (reserve/pop_ref).
// Single producer, single consumer pattern — the zero-copy benefit is from
// avoiding the 2KB memcpy per item, not from contention. Other shapes are
// covered above.
// ---------------------------------------------------------------------------

#[derive(Clone)]
#[allow(dead_code)]
struct LargeStruct {
    data: [u8; 2048],
}

impl LargeStruct {
    fn new(seed: u8) -> Self {
        Self { data: [seed; 2048] }
    }
}

fn bench_large_struct_mpmc(c: &mut Criterion) {
    use quetzalcoatl::mpmc::RingBuffer;

    let mut group = c.benchmark_group("large_struct_mpmc");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let total_items = 10_000u64;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("2kb_copy", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (producer, consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();
                let start = std::time::Instant::now();
                let producer_handle = thread::spawn(move || {
                    for i in 0..total_items {
                        while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                });
                let mut received = 0u64;
                while received < total_items {
                    if consumer.pop().is_some() {
                        received += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                producer_handle.join().unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("2kb_zero_copy", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (mut producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();
                let start = std::time::Instant::now();
                let producer_handle = thread::spawn(move || {
                    for i in 0..total_items {
                        loop {
                            if let Some(w) = producer.reserve() {
                                w.write(black_box(LargeStruct::new(i as u8))).commit();
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                });
                let mut received = 0u64;
                while received < total_items {
                    if let Some(reader) = consumer.pop_ref() {
                        black_box(&*reader);
                        received += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                producer_handle.join().unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Blocking API: push_block / pop_block under contention.
//
// 2P x 2C, small ring (16), each consumer doing slow work. Compares
// spin-retry vs push_block / pop_block. Throughput should be in line under
// saturation; idle-CPU savings are not measured here (see examples/mpmc_block.rs).
// ---------------------------------------------------------------------------

fn bench_blocking_mpmc(c: &mut Criterion) {
    use quetzalcoatl::mpmc::RingBuffer;

    let mut group = c.benchmark_group("blocking_mpmc");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let p_count = 2u64;
    let q_count = 2usize;
    let per_p = 5_000u64;
    let total_items = p_count * per_p;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("spin", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let received = Arc::new(AtomicUsize::new(0));
                let consumers: Vec<_> = (0..q_count)
                    .map(|_| {
                        let c = consumer.clone();
                        let r = received.clone();
                        thread::spawn(move || {
                            while r.load(Ordering::Relaxed) < total_items as usize {
                                if let Some(v) = c.pop() {
                                    black_box(slow_work(v));
                                    r.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();
                drop(consumer);

                let start = std::time::Instant::now();
                let producers: Vec<_> = (0..p_count)
                    .map(|tid| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..per_p {
                                while p.push(tid * per_p + i).is_err() {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();
                drop(producer);
                for h in producers {
                    h.join().unwrap();
                }
                for h in consumers {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("block", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let consumers: Vec<_> = (0..q_count)
                    .map(|_| {
                        let c = consumer.clone();
                        thread::spawn(move || {
                            let mut local = 0u64;
                            while let Some(v) = c.pop_block() {
                                black_box(slow_work(v));
                                local += 1;
                            }
                            local
                        })
                    })
                    .collect();
                drop(consumer);

                let start = std::time::Instant::now();
                let producers: Vec<_> = (0..p_count)
                    .map(|tid| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..per_p {
                                p.push_block(tid * per_p + i).expect("consumers dropped");
                            }
                        })
                    })
                    .collect();
                drop(producer);
                for h in producers {
                    h.join().unwrap();
                }
                let mut sum = 0u64;
                for h in consumers {
                    sum += h.join().unwrap();
                }
                assert_eq!(sum, total_items);
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Large struct + zero-copy blocking: reserve_block / pop_ref_block under
// contention. 2P x 2C, small ring (16), 2KB items.
// ---------------------------------------------------------------------------

fn bench_large_struct_mpmc_zero_copy_blocking(c: &mut Criterion) {
    use quetzalcoatl::mpmc::RingBuffer;

    let mut group = c.benchmark_group("large_struct_mpmc_zero_copy_blocking");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let p_count = 2u64;
    let q_count = 2usize;
    let per_p = 2_500u64;
    let total_items = p_count * per_p;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("2kb_items", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (producer, consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(16)).split();

                let consumers: Vec<_> = (0..q_count)
                    .map(|_| {
                        let mut c = consumer.clone();
                        thread::spawn(move || {
                            let mut local = 0u64;
                            while let Some(reader) = c.pop_ref_block() {
                                black_box(slow_work(reader.data[0] as u64));
                                local += 1;
                            }
                            local
                        })
                    })
                    .collect();
                drop(consumer);

                let start = std::time::Instant::now();
                let producers: Vec<_> = (0..p_count)
                    .map(|_| {
                        let mut p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..per_p {
                                let w = p.reserve_block().expect("consumers dropped");
                                w.write(black_box(LargeStruct::new(i as u8))).commit();
                            }
                        })
                    })
                    .collect();
                drop(producer);
                for h in producers {
                    h.join().unwrap();
                }
                let mut sum = 0u64;
                for h in consumers {
                    sum += h.join().unwrap();
                }
                assert_eq!(sum, total_items);
                total += start.elapsed();
            }
            total
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Async API: push_async / pop_async, M producers + N consumers.
//
// Each producer/consumer task runs on its own thread with a current_thread
// runtime + LocalSet (Producer/Consumer are !Send).
//
// Run with: cargo bench --bench mpmc --features async
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
fn run_async_mpmc_bench(
    b: &mut criterion::Bencher,
    cap: usize,
    p_count: u64,
    c_count: usize,
    per_p: u64,
    consumer_work: fn(u64) -> u64,
) {
    use quetzalcoatl::mpmc::RingBuffer;

    let total_items = p_count * per_p;
    b.iter_custom(|iters| {
        let mut total = Duration::ZERO;
        for _ in 0..iters {
            let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
            let received = Arc::new(AtomicUsize::new(0));
            let start = std::time::Instant::now();

            let consumer_threads: Vec<_> = (0..c_count)
                .map(|_| {
                    let c = consumer.clone();
                    let received = received.clone();
                    thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .unwrap();
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            while let Some(v) = c.pop_async().await {
                                black_box(consumer_work(v));
                                received.fetch_add(1, Ordering::Relaxed);
                            }
                        }));
                    })
                })
                .collect();
            drop(consumer);

            let producer_threads: Vec<_> = (0..p_count)
                .map(|tid| {
                    let p = producer.clone();
                    thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .unwrap();
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            for i in 0..per_p {
                                p.push_async(tid * per_p + i)
                                    .await
                                    .expect("all consumers dropped");
                            }
                        }));
                    })
                })
                .collect();
            drop(producer);

            for h in producer_threads {
                h.join().unwrap();
            }
            for h in consumer_threads {
                h.join().unwrap();
            }
            assert_eq!(received.load(Ordering::Relaxed), total_items as usize);
            total += start.elapsed();
        }
        total
    });
}

#[cfg(feature = "async")]
fn bench_async_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("blocking_mpmc");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let p_count = 2u64;
    let q_count = 2usize;
    let per_p = 5_000u64;
    let total_items = p_count * per_p;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("async_saturated", |b| {
        run_async_mpmc_bench(b, 16, p_count, q_count, per_p, slow_work);
    });

    group.bench_function("async_unsaturated", |b| {
        run_async_mpmc_bench(b, 4096, p_count, q_count, per_p, |v| v);
    });

    group.finish();
}

#[cfg(not(feature = "async"))]
criterion_group!(
    benches,
    bench_mpmc_vs_n_spmc,
    bench_large_struct_mpmc,
    bench_blocking_mpmc,
    bench_large_struct_mpmc_zero_copy_blocking,
);

#[cfg(feature = "async")]
criterion_group!(
    benches,
    bench_mpmc_vs_n_spmc,
    bench_large_struct_mpmc,
    bench_blocking_mpmc,
    bench_large_struct_mpmc_zero_copy_blocking,
    bench_async_mpmc,
);

criterion_main!(benches);
