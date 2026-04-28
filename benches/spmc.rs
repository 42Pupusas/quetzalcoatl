use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spmc::RingBuffer;
use std::thread;

// ---------------------------------------------------------------------------
// 1. SPMC – scale consumers from 1 to 16
//
// Measures end-to-end throughput as the number of competing consumers grows.
// The producer runs on the bench thread; consumer threads race on CAS(head).
// ---------------------------------------------------------------------------

fn bench_spmc_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc_scaling");
    let total_items = 20_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    // Boundary cases only — 2, 4, 12 produced redundant scaling info
    // on top of (1, 8, 16). Restore them if you ever need to chase
    // non-monotonic regressions in the middle of the range.
    for num_consumers in [1, 8, 16] {
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

                        let handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let c = consumer.clone();
                                thread::spawn(move || {
                                    loop {
                                        if c.pop().is_some() {
                                            // counted via natural exit
                                        } else if c.is_closed() {
                                            while c.pop().is_some() {}
                                            break;
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
                        // Signal workers to drain and exit.
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
    let total_items = 100_000u64;
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
    let total_items = 10_000u64;
    let num_consumers = 8usize;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for cap in [64, 256, 1024] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(BenchmarkId::new("cap", cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();

                    let handles: Vec<_> = (0..num_consumers)
                        .map(|_| {
                            let c = consumer.clone();
                            thread::spawn(move || loop {
                                if c.pop().is_some() {
                                    // counted via natural exit
                                } else if c.is_closed() {
                                    while c.pop().is_some() {}
                                    break;
                                } else {
                                    std::hint::spin_loop();
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
                    drop(producer);
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
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

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

                        let handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let c = consumer.clone();
                                thread::spawn(move || loop {
                                    if c.pop().is_some() {
                                        // counted via natural exit
                                    } else if c.is_closed() {
                                        while c.pop().is_some() {}
                                        break;
                                    } else {
                                        std::hint::spin_loop();
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
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Large struct zero-copy SPMC (reserve / pop_ref)
// ---------------------------------------------------------------------------

fn bench_large_struct_spmc_zero_copy(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_spmc_zero_copy");
    let total_items = 10_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

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

                        let handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let mut c = consumer.clone();
                                thread::spawn(move || loop {
                                    let got = {
                                        if let Some(r) = c.pop_ref() {
                                            black_box(&*r);
                                            drop(r);
                                            true
                                        } else {
                                            false
                                        }
                                    };
                                    if got {
                                        continue;
                                    }
                                    if c.is_closed() {
                                        while let Some(r) = c.pop_ref() {
                                            black_box(&*r);
                                            drop(r);
                                        }
                                        break;
                                    }
                                    std::hint::spin_loop();
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
    let total_items = 20_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

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

                        let handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let c = consumer.clone();
                                thread::spawn(move || loop {
                                    if c.pop().is_some() {
                                        // counted via natural exit
                                    } else if c.is_closed() {
                                        while c.pop().is_some() {}
                                        break;
                                    } else {
                                        std::hint::spin_loop();
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
                        // Drop producer so consumers see `closed` and exit.
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

// ---------------------------------------------------------------------------
// 7. Slow-work parallelism — the verify-pool workload.
//
// Mirrors the real consumer pathology: per-item work is ~50µs (e.g. a Schnorr
// verify), much slower than queue overhead. Producer pushes at ~10µs cadence,
// fast enough to keep the queue non-empty so consumers actually contend.
//
// Ideal parallelism: time ≈ (items × work) / consumers. If a single consumer
// claims a long run of items at once (batched-CAS monopoly window), other
// consumers stall and effective parallelism collapses toward 1, regardless
// of how many consumer threads we spawned.
//
// Compared against N×SPSC round-robin — the workload-correct baseline for
// "I want M parallel verifies." If SPMC is significantly slower than N×SPSC
// here, the SPMC claim path is the bug, not the workload.
// ---------------------------------------------------------------------------

/// Calibrate a spin-loop iteration count that takes approximately `target_ns`
/// nanoseconds on the current machine. Run once per bench process.
fn calibrate_spin_iters(target_ns: u64) -> u64 {
    // Measure 1M spin_loop iters, scale.
    const PROBE: u64 = 1_000_000;
    let start = std::time::Instant::now();
    for _ in 0..PROBE {
        std::hint::spin_loop();
    }
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    if elapsed_ns == 0 {
        return target_ns * 10; // pathological; assume 0.1 ns/iter
    }
    let ns_per_iter_x1000 = (elapsed_ns * 1000) / PROBE;
    if ns_per_iter_x1000 == 0 {
        return target_ns * 10;
    }
    (target_ns * 1000) / ns_per_iter_x1000
}

#[inline(never)]
fn busy_work(iters: u64) {
    for _ in 0..iters {
        std::hint::spin_loop();
    }
}

fn bench_slow_work_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow_work_scaling");
    let total_items = 1_000u64;
    // Per-item work: ~50µs (Schnorr-verify scale).
    let consumer_work_iters = calibrate_spin_iters(50_000);
    // Producer cadence: ~10µs between pushes — fast enough that the queue
    // stays non-empty and consumers genuinely contend, slow enough that the
    // producer doesn't drown the consumers in pure push throughput.
    let producer_gap_iters = calibrate_spin_iters(10_000);

    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for num_consumers in [1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(total_items));

        // SPMC: one ring, N competing consumers doing slow per-item work.
        group.bench_with_input(
            BenchmarkId::new("spmc", num_consumers),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) =
                            RingBuffer::<u64>::new(Capacity::exact(1024)).split();

                        let handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let c = consumer.clone();
                                thread::spawn(move || loop {
                                    if let Some(v) = c.pop() {
                                        black_box(v);
                                        busy_work(consumer_work_iters);
                                    } else if c.is_closed() {
                                        while let Some(v) = c.pop() {
                                            black_box(v);
                                            busy_work(consumer_work_iters);
                                        }
                                        break;
                                    } else {
                                        std::hint::spin_loop();
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
                            busy_work(producer_gap_iters);
                        }
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

        // N×SPSC: round-robin baseline. Each consumer owns its own ring,
        // producer fans out by `i % N`. No claim contention possible.
        group.bench_with_input(
            BenchmarkId::new("nx_spsc", num_consumers),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let per_ring_cap = (1024usize / num_consumers).next_power_of_two();
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
                                        if let Some(v) = c.pop() {
                                            black_box(v);
                                            busy_work(consumer_work_iters);
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
                            busy_work(producer_gap_iters);
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
// 8. Burst-producer slow-work — try to trigger the monopoly window.
//
// The smooth-cadence variant (#7) showed near-linear SPMC scaling because
// the queue stays shallow: bounded-CAS clamps `take = min(BATCH_SIZE, tail
// - head)`, so when only a handful of items are queued, no consumer can
// claim more than its fair share.
//
// The monopoly window is supposed to open when the queue is *bursty and
// shallow*: producer publishes a small burst (K ≤ BATCH_SIZE items), then
// idles. The first consumer to CAS grabs the whole burst; the others see
// `head == tail` and re-park or spin. By the time the lucky consumer
// finishes its 1.6ms of work, the producer has emitted the next burst,
// and the same consumer (already polling) wins again.
//
// If this bench shows SPMC stuck near 1c-time while N×SPSC scales, the
// pathology is real and the FAA-per-item rewrite is justified. If SPMC
// still scales here, the diagnosis as written is wrong on this hardware
// and we should look harder before redesigning.
// ---------------------------------------------------------------------------

fn bench_burst_producer_slow_work(c: &mut Criterion) {
    let mut group = c.benchmark_group("burst_producer_slow_work");
    let total_items = 1_024u64; // multiple of all burst sizes & consumer counts
    let consumer_work_iters = calibrate_spin_iters(50_000);
    // After each burst, idle long enough for ~1 consumer to drain it. With
    // a burst of 8 items × 50µs = 400µs, idle 350µs (intentionally a bit
    // less than full drain time) so the queue never sits truly empty for
    // long — keeps the lucky-consumer feedback loop tight.
    let idle_iters = calibrate_spin_iters(350_000);

    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    // Bursts ≤ BATCH_SIZE (32) are the regime where one consumer can
    // monopolize the whole burst in a single CAS. Burst=64 should let a
    // second consumer get a fair claim. We keep the boundary cases (8,
    // 64) and the low/high consumer counts (2, 8) — the middle points
    // were redundant.
    for burst in [8usize, 64] {
        for num_consumers in [2usize, 8] {
            group.throughput(Throughput::Elements(total_items));

            let label = format!("burst{}_c{}", burst, num_consumers);

            // SPMC variant.
            group.bench_with_input(
                BenchmarkId::new("spmc", &label),
                &(burst, num_consumers),
                |b, &(burst, num_consumers)| {
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let (producer, consumer) =
                                RingBuffer::<u64>::new(Capacity::exact(1024)).split();

                            let handles: Vec<_> = (0..num_consumers)
                                .map(|_| {
                                    let c = consumer.clone();
                                    thread::spawn(move || loop {
                                        if let Some(v) = c.pop() {
                                            black_box(v);
                                            busy_work(consumer_work_iters);
                                        } else if c.is_closed() {
                                            while let Some(v) = c.pop() {
                                                black_box(v);
                                                busy_work(consumer_work_iters);
                                            }
                                            break;
                                        } else {
                                            std::hint::spin_loop();
                                        }
                                    })
                                })
                                .collect();
                            drop(consumer);

                            let start = std::time::Instant::now();
                            let mut pushed = 0u64;
                            while pushed < total_items {
                                let burst_n = burst.min((total_items - pushed) as usize);
                                for _ in 0..burst_n {
                                    while producer.push(black_box(pushed)).is_err() {
                                        std::hint::spin_loop();
                                    }
                                    pushed += 1;
                                }
                                if pushed < total_items {
                                    busy_work(idle_iters);
                                }
                            }
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

            // N×SPSC round-robin baseline. Producer assigns each item
            // round-robin to a per-consumer ring; bursts are split evenly.
            group.bench_with_input(
                BenchmarkId::new("nx_spsc", &label),
                &(burst, num_consumers),
                |b, &(burst, num_consumers)| {
                    b.iter_custom(|iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let per_ring_cap = (1024usize / num_consumers).next_power_of_two();
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
                                            if let Some(v) = c.pop() {
                                                black_box(v);
                                                busy_work(consumer_work_iters);
                                                got += 1;
                                            } else {
                                                std::hint::spin_loop();
                                            }
                                        }
                                    })
                                })
                                .collect();

                            let start = std::time::Instant::now();
                            let mut pushed = 0u64;
                            while pushed < total_items {
                                let burst_n = burst.min((total_items - pushed) as usize);
                                for _ in 0..burst_n {
                                    let target = (pushed as usize) % num_consumers;
                                    while producers[target].push(black_box(pushed)).is_err() {
                                        std::hint::spin_loop();
                                    }
                                    pushed += 1;
                                }
                                if pushed < total_items {
                                    busy_work(idle_iters);
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
    bench_slow_work_scaling,
    bench_burst_producer_slow_work,
);
criterion_main!(benches);
