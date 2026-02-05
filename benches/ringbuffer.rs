use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::RingBuffer;
use std::thread;

// ---------------------------------------------------------------------------
// 1. Single-thread push-only throughput
// ---------------------------------------------------------------------------

fn bench_push_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_only");

    for cap in [1024, 4096, 65536] {
        group.throughput(Throughput::Elements(cap as u64));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, _consumer) = RingBuffer::<u64>::new(cap).split();
                    let start = std::time::Instant::now();
                    for i in 0..cap as u64 {
                        let _ = producer.push(black_box(i));
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
// 2. Single-thread pop-only throughput
// ---------------------------------------------------------------------------

fn bench_pop_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pop_only");

    for cap in [1024, 4096, 65536] {
        group.throughput(Throughput::Elements(cap as u64));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, mut consumer) = RingBuffer::<u64>::new(cap).split();
                    for i in 0..cap as u64 {
                        producer.push(i).unwrap();
                    }
                    let start = std::time::Instant::now();
                    while consumer.pop().is_some() {}
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Single-thread push/pop ping-pong (alternating)
// ---------------------------------------------------------------------------

fn bench_push_pop_alternating(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_pop_alternating");
    let ops = 10_000u64;

    group.throughput(Throughput::Elements(ops));
    group.bench_function("10k_ops", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = RingBuffer::<u64>::new(64).split();
                let start = std::time::Instant::now();
                for i in 0..ops {
                    let _ = producer.push(black_box(i));
                    black_box(consumer.pop());
                }
                total += start.elapsed();
            }
            total
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 4. SPSC – producer and consumer on separate threads
// ---------------------------------------------------------------------------

fn bench_spsc_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_concurrent");

    for total_items in [10_000u64, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::from_parameter(total_items),
            &total_items,
            |b, &total_items| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) =
                            RingBuffer::<u64>::new(4096).split();

                        let start = std::time::Instant::now();

                        let producer_handle = thread::spawn(move || {
                            for i in 0..total_items {
                                while producer.push(black_box(i)).is_err() {
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
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 5. MPSC – scale producers from 1 to 8
// ---------------------------------------------------------------------------

fn bench_mpsc_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_scaling");
    let items_per_producer = 50_000u64;

    for num_producers in [1, 2, 4, 8] {
        let total_items = items_per_producer * num_producers as u64;
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::new("producers", num_producers),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) =
                            RingBuffer::<u64>::new(8192).split();

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|_| {
                                let p = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        while p.push(black_box(i)).is_err() {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        let mut received = 0u64;
                        while received < total_items {
                            if consumer.pop().is_some() {
                                received += 1;
                            } else {
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
// 6. Contention – many producers on a small buffer (high CAS retry rate)
// ---------------------------------------------------------------------------

fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");
    let items_per_producer = 10_000u64;
    let num_producers = 8u64;
    let total_items = items_per_producer * num_producers;

    for cap in [64, 256, 1024] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::new("cap", cap),
            &cap,
            |b, &cap| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) =
                            RingBuffer::<u64>::new(cap).split();

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|_| {
                                let p = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        while p.push(black_box(i)).is_err() {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        let mut received = 0u64;
                        while received < total_items {
                            if consumer.pop().is_some() {
                                received += 1;
                            } else {
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
// 7. Buffer capacity impact on throughput (power-of-two vs non-power-of-two)
// ---------------------------------------------------------------------------

fn bench_capacity_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_impact");
    let ops = 50_000u64;

    for cap in [1000, 1024, 4000, 4096] {
        group.throughput(Throughput::Elements(ops));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, mut consumer) = RingBuffer::<u64>::new(cap).split();

                    let start = std::time::Instant::now();
                    for i in 0..ops {
                        let _ = producer.push(black_box(i));
                        if i % 2 == 0 {
                            black_box(consumer.pop());
                        }
                    }
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_push_only,
    bench_pop_only,
    bench_push_pop_alternating,
    bench_spsc_concurrent,
    bench_mpsc_scaling,
    bench_contention,
    bench_capacity_impact,
);
criterion_main!(benches);
