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
    let total_items = 100_000u64;

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
    let items_per_producer = 50_000u64;

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
                            if let Some(mut w) = producer.reserve() {
                                w.write(black_box(LargeStruct::new(i as u8)));
                                w.commit();
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

criterion_group!(
    benches,
    bench_consumer_scaling,
    bench_producer_scaling,
    bench_large_struct_clone_vs_ref,
    bench_arc_large_struct,
);
criterion_main!(benches);
