use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::broadcast::arc::ArcRingBuffer;
use quetzalcoatl::broadcast::RingBuffer;
use quetzalcoatl::capacity::Capacity;
use std::thread;

// ---------------------------------------------------------------------------
// 1. Consumer scaling: 1 producer, N consumers
// ---------------------------------------------------------------------------

fn bench_consumer_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_consumer_scaling");
    let total_items = 10_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for num_consumers in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_consumers}_consumers")),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) =
                            RingBuffer::<u64>::new(Capacity::exact(4096), num_consumers + 1)
                                .split();
                        let mut consumers: Vec<_> =
                            (0..num_consumers - 1).map(|_| consumer.clone()).collect();
                        consumers.push(consumer);

                        let start = std::time::Instant::now();

                        let producer_handle = thread::spawn(move || {
                            for i in 0..total_items {
                                while producer.push(black_box(i)).is_err() {
                                    std::hint::spin_loop();
                                }
                            }
                        });

                        let handles: Vec<_> = consumers
                            .into_iter()
                            .map(|mut c| {
                                thread::spawn(move || {
                                    let mut received = 0u64;
                                    while received < total_items {
                                        if c.pop().is_some() {
                                            received += 1;
                                        } else {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        producer_handle.join().unwrap();
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
// 2. Producer scaling: N producers, 2 consumers
// ---------------------------------------------------------------------------

fn bench_producer_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_producer_scaling");
    let items_per_producer = 5_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for num_producers in [1, 2, 4, 8] {
        let total_items = items_per_producer * num_producers as u64;
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_producers}_producers")),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) =
                            RingBuffer::<u64>::new(Capacity::exact(4096), 4).split();
                        let mut c2 = consumer.clone();
                        let mut c1 = consumer;

                        let start = std::time::Instant::now();

                        let producer_handles: Vec<_> = (0..num_producers)
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

                        let h1 = thread::spawn(move || {
                            let mut received = 0u64;
                            while received < total_items {
                                if c1.pop().is_some() {
                                    received += 1;
                                } else {
                                    std::hint::spin_loop();
                                }
                            }
                        });

                        let mut received = 0u64;
                        while received < total_items {
                            if c2.pop().is_some() {
                                received += 1;
                            } else {
                                std::hint::spin_loop();
                            }
                        }

                        for h in producer_handles {
                            h.join().unwrap();
                        }
                        h1.join().unwrap();
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
// 3. Large struct: pop (clone) vs pop_ref (zero-copy)
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

fn bench_large_struct_clone_vs_ref(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_large_struct");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let total_items = 10_000u64;

    group.throughput(Throughput::Elements(total_items));

    // Clone path (pop)
    group.bench_function("2kb_clone", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256), 4).split();

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

    // Zero-copy path (pop_ref)
    group.bench_function("2kb_zero_copy", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (mut producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256), 4).split();

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
                    if let Some(_reader) = consumer.pop_ref() {
                        black_box(&*_reader);
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
// 4. Arc wrapper: large struct with multiple consumers
// ---------------------------------------------------------------------------

fn bench_arc_large_struct(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_arc_large_struct");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let total_items = 10_000u64;
    let num_consumers = 4usize;

    group.throughput(Throughput::Elements(total_items));

    // Clone-based (regular broadcast)
    group.bench_function("2kb_clone_4consumers", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256), num_consumers + 1).split();
                let mut consumers: Vec<_> =
                    (0..num_consumers - 1).map(|_| consumer.clone()).collect();
                consumers.push(consumer);

                let start = std::time::Instant::now();

                let producer_handle = thread::spawn(move || {
                    for i in 0..total_items {
                        while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                });

                let handles: Vec<_> = consumers
                    .into_iter()
                    .map(|mut c| {
                        thread::spawn(move || {
                            let mut received = 0u64;
                            while received < total_items {
                                if c.pop().is_some() {
                                    received += 1;
                                } else {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();

                producer_handle.join().unwrap();
                for h in handles {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    // Arc-based (cheap refcount clone)
    group.bench_function("2kb_arc_4consumers", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, consumer) =
                    ArcRingBuffer::<LargeStruct>::new(Capacity::exact(256), num_consumers + 1)
                        .split();
                let mut consumers: Vec<_> =
                    (0..num_consumers - 1).map(|_| consumer.clone()).collect();
                consumers.push(consumer);

                let start = std::time::Instant::now();

                let producer_handle = thread::spawn(move || {
                    for i in 0..total_items {
                        while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                });

                let handles: Vec<_> = consumers
                    .into_iter()
                    .map(|mut c| {
                        thread::spawn(move || {
                            let mut received = 0u64;
                            while received < total_items {
                                if c.pop().is_some() {
                                    received += 1;
                                } else {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();

                producer_handle.join().unwrap();
                for h in handles {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Async API: push_async / pop_async, 1 producer + N consumers (broadcast).
//
// Run with: cargo bench --bench broadcast --features async
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
fn bench_async_broadcast(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_broadcast");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(3));
    let n_consumers: usize = 2;
    let total_items: u64 = 1_000;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("saturated", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, c1) =
                    RingBuffer::<u64>::new(Capacity::exact(16), n_consumers + 1).split();
                let mut consumers = vec![c1.clone()];
                for _ in 1..n_consumers {
                    consumers.push(c1.clone());
                }
                drop(c1);

                let start = std::time::Instant::now();

                let consumer_threads: Vec<_> = consumers
                    .into_iter()
                    .map(|mut c| {
                        thread::spawn(move || {
                            let rt = tokio::runtime::Builder::new_current_thread()
                                .build()
                                .unwrap();
                            let local = tokio::task::LocalSet::new();
                            rt.block_on(local.run_until(async move {
                                while let Some(v) = c.pop_async().await {
                                    black_box(v);
                                }
                            }));
                        })
                    })
                    .collect();

                let producer_thread = thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(async move {
                        for i in 0..total_items {
                            producer.push_async(i).await.expect("all consumers dropped");
                        }
                    }));
                });

                producer_thread.join().unwrap();
                for h in consumer_threads {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    // NOTE: an "unsaturated" variant (cap >> total, producer never parks)
    // exposes a rare deadlock at high iteration counts. Investigation
    // pending — see project_broadcast_async_unsaturated_race.md.

    group.finish();
}

#[cfg(not(feature = "async"))]
criterion_group!(
    benches,
    bench_consumer_scaling,
    bench_producer_scaling,
    bench_large_struct_clone_vs_ref,
    bench_arc_large_struct,
);

#[cfg(feature = "async")]
criterion_group!(
    benches,
    bench_consumer_scaling,
    bench_producer_scaling,
    bench_large_struct_clone_vs_ref,
    bench_arc_large_struct,
    bench_async_broadcast,
);

criterion_main!(benches);
