use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spmc::RingBuffer;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

// ---------------------------------------------------------------------------
// 1. SPMC – scale consumers from 1 to 16
//
// Measures end-to-end throughput as the number of competing consumers grows.
// The producer runs on the bench thread; consumer threads race on CAS(head).
// ---------------------------------------------------------------------------

fn bench_spmc_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc_scaling");
    let total_items = 200_000u64;

    for num_consumers in [1, 2, 4, 8, 12, 16] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::new("consumers", num_consumers),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) =
                            RingBuffer::<u64>::new(Capacity::exact(8192)).split();
                        let remaining = Arc::new(AtomicUsize::new(total_items as usize));

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

                        // Drop the original consumer handle so only the workers hold refs.
                        drop(consumer);

                        let start = std::time::Instant::now();
                        for i in 0..total_items {
                            while producer.push(black_box(i)).is_err() {
                                std::hint::spin_loop();
                            }
                        }
                        for h in handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Producer-only throughput – isolate the push hot path.
//
// No consumer threads run: the bench drains via a single consumer between
// pushes only when full. This isolates the cost of push() itself, exposing
// any per-push overhead (e.g. unnecessary atomic stores) without consumer
// noise.
// ---------------------------------------------------------------------------

fn bench_spmc_producer_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc_producer_only");
    let total_items = 1_000_000u64;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("push_drain", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(8192)).split();

                let start = std::time::Instant::now();
                for i in 0..total_items {
                    while producer.push(black_box(i)).is_err() {
                        // Drain inline — no other thread is consuming.
                        while consumer.pop().is_some() {}
                    }
                }
                while consumer.pop().is_some() {}
                total += start.elapsed();
            }
            total
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Producer push latency under consumer pressure.
//
// Mirrors `bench_contention` from the MPSC suite: small buffer, many
// consumers, measures the producer's push throughput when the slot
// sequence cache lines are heavily shared.
// ---------------------------------------------------------------------------

fn bench_spmc_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc_contention");
    let total_items = 80_000u64;
    let num_consumers = 8usize;

    for cap in [64, 256, 1024] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(BenchmarkId::new("cap", cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
                    let remaining = Arc::new(AtomicUsize::new(total_items as usize));

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

                    let start = std::time::Instant::now();
                    for i in 0..total_items {
                        while producer.push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Large struct SPMC (~2KB per element)
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

fn bench_large_struct_spmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_spmc");
    let total_items = 10_000u64;

    for num_consumers in [1, 2, 4] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::new("consumers", num_consumers),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) =
                            RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();
                        let remaining = Arc::new(AtomicUsize::new(total_items as usize));

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

                        let start = std::time::Instant::now();
                        for i in 0..total_items {
                            while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                                std::hint::spin_loop();
                            }
                        }
                        for h in handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Large struct zero-copy SPMC (reserve / pop_ref)
// ---------------------------------------------------------------------------

fn bench_large_struct_spmc_zero_copy(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_spmc_zero_copy");
    let total_items = 10_000u64;

    for num_consumers in [1, 2, 4] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::new("consumers", num_consumers),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (mut producer, consumer) =
                            RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();
                        let remaining = Arc::new(AtomicUsize::new(total_items as usize));

                        let handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let mut c = consumer.clone();
                                let rem = remaining.clone();
                                thread::spawn(move || {
                                    while rem.load(Ordering::Relaxed) > 0 {
                                        if let Some(r) = c.pop_ref() {
                                            black_box(&*r);
                                            drop(r);
                                            rem.fetch_sub(1, Ordering::Relaxed);
                                        } else {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        drop(consumer);

                        let start = std::time::Instant::now();
                        for i in 0..total_items {
                            loop {
                                if let Some(w) = producer.reserve() {
                                    w.write(black_box(LargeStruct::new(i as u8))).commit();
                                    break;
                                }
                                std::hint::spin_loop();
                            }
                        }
                        for h in handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 6. SPMC vs N×SPSC round-robin – the work-distribution comparison.
//
// One producer hands out `total_items` to N consumers two ways:
//   - via a single SPMC ring (consumers race CAS on head)
//   - via N independent SPSC rings, producer round-robins pushes
//
// Same total work, same consumer count. This is the bench that justifies
// (or invalidates) the SPMC redesign.
// ---------------------------------------------------------------------------

fn bench_work_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("work_distribution");
    let total_items = 200_000u64;

    for num_consumers in [2, 4, 8] {
        group.throughput(Throughput::Elements(total_items));

        // SPMC: one ring, N competing consumers.
        group.bench_with_input(
            BenchmarkId::new("spmc", num_consumers),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) =
                            RingBuffer::<u64>::new(Capacity::exact(8192)).split();
                        let remaining = Arc::new(AtomicUsize::new(total_items as usize));

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

                        let start = std::time::Instant::now();
                        for i in 0..total_items {
                            while producer.push(black_box(i)).is_err() {
                                std::hint::spin_loop();
                            }
                        }
                        // Drop producer so any overshooting consumer
                        // sees `closed` and exits cleanly.
                        drop(producer);
                        for h in handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // N×SPSC: producer round-robins across N rings, each consumer owns one.
        group.bench_with_input(
            BenchmarkId::new("nx_spsc", num_consumers),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        // Per-ring capacity: keep aggregate buffering ≈ SPMC's 8192
                        // so neither side gets an unfair backpressure advantage.
                        let per_ring_cap = (8192usize / num_consumers).next_power_of_two();
                        let mut producers = Vec::with_capacity(num_consumers);
                        let mut consumers = Vec::with_capacity(num_consumers);
                        for _ in 0..num_consumers {
                            let (p, c) = quetzalcoatl::spsc::RingBuffer::<u64>::new(
                                Capacity::exact(per_ring_cap),
                            )
                            .split();
                            producers.push(p);
                            consumers.push(c);
                        }

                        let handles: Vec<_> = consumers
                            .into_iter()
                            .map(|mut c| {
                                let per_consumer_items = total_items / num_consumers as u64;
                                thread::spawn(move || {
                                    let mut got = 0u64;
                                    while got < per_consumer_items {
                                        if c.pop().is_some() {
                                            got += 1;
                                        } else {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        let start = std::time::Instant::now();
                        for i in 0..total_items {
                            let target = (i as usize) % num_consumers;
                            while producers[target].push(black_box(i)).is_err() {
                                std::hint::spin_loop();
                            }
                        }
                        for h in handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_spmc_scaling,
    bench_spmc_producer_only,
    bench_spmc_contention,
    bench_large_struct_spmc,
    bench_large_struct_spmc_zero_copy,
    bench_work_distribution,
);
criterion_main!(benches);
