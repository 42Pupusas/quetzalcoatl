use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::RingBuffer;
use std::thread;

// ---------------------------------------------------------------------------
// 1. MPSC – scale producers from 1 to 8
// ---------------------------------------------------------------------------

fn bench_mpsc_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_scaling");
    let items_per_producer = 5_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    // Boundary cases only — 2, 4, 12 produced redundant scaling info.
    for num_producers in [1, 8, 16] {
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
                            RingBuffer::<u64>::new(Capacity::exact(8192)).split();

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
// 2. Contention – many producers on a small buffer (high CAS retry rate)
// ---------------------------------------------------------------------------

fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let items_per_producer = 10_000u64;
    let num_producers = 8u64;
    let total_items = items_per_producer * num_producers;

    for cap in [64, 256, 1024] {
        group.throughput(Throughput::Elements(total_items));
        group.bench_with_input(BenchmarkId::new("cap", cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, mut consumer) =
                        RingBuffer::<u64>::new(Capacity::exact(cap)).split();

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
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Large struct MPSC benchmarks (~2KB per element)
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

fn bench_large_struct_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_mpsc");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let items_per_producer = 2_500u64;

    for num_producers in [1, 2, 4] {
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
                            RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|p| {
                                let prod = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        while prod
                                            .push(black_box(LargeStruct::new(
                                                (p as u64 * 100 + i) as u8,
                                            )))
                                            .is_err()
                                        {
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
// 4. Large struct zero-copy MPSC (reserve/pop_ref)
// ---------------------------------------------------------------------------

fn bench_large_struct_mpsc_zero_copy(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_mpsc_zero_copy");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let items_per_producer = 2_500u64;

    for num_producers in [1, 2, 4] {
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
                            RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|p| {
                                let mut prod = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        loop {
                                            if let Some(w) = prod.reserve() {
                                                w.write(black_box(LargeStruct::new(
                                                    (p as u64 * 100 + i) as u8,
                                                )))
                                                .commit();
                                                break;
                                            }
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        let mut received = 0u64;
                        while received < total_items {
                            if let Some(_reader) = consumer.pop_ref() {
                                black_box(&*_reader);
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
// Blocking API: push_block / pop_block under contention.
//
// 2 producers, 1 consumer, small ring (16), consumer doing slow work.
// Compares spin-retry vs push_block / pop_block.
// ---------------------------------------------------------------------------

#[inline(never)]
fn slow_consume_work(seed: u64) -> u64 {
    let mut x = seed;
    for _ in 0..64 {
        x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        x = black_box(x);
    }
    x
}

fn bench_blocking_mpsc(c: &mut Criterion) {
    use quetzalcoatl::mpsc::RingBuffer;

    let mut group = c.benchmark_group("blocking_mpsc");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let p_count = 2u64;
    let per_p = 5_000u64;
    let total_items = p_count * per_p;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("spin", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
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

                let start = std::time::Instant::now();
                let mut received = 0u64;
                while received < total_items {
                    if let Some(v) = consumer.pop() {
                        black_box(slow_consume_work(v));
                        received += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                for h in producers {
                    h.join().unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("block", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let producers: Vec<_> = (0..p_count)
                    .map(|tid| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..per_p {
                                p.push_block(tid * per_p + i).expect("consumer dropped");
                            }
                        })
                    })
                    .collect();
                drop(producer);

                let start = std::time::Instant::now();
                let mut received = 0u64;
                while let Some(v) = consumer.pop_block() {
                    black_box(slow_consume_work(v));
                    received += 1;
                }
                assert_eq!(received, total_items);
                for h in producers {
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
// Large struct + zero-copy blocking: reserve_block / pop_ref_block under
// contention. 2 producers, 1 consumer, small ring (16), 2KB items.
// ---------------------------------------------------------------------------

fn bench_large_struct_mpsc_zero_copy_blocking(c: &mut Criterion) {
    use quetzalcoatl::mpsc::RingBuffer;

    let mut group = c.benchmark_group("large_struct_mpsc_zero_copy_blocking");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let p_count = 2u64;
    let per_p = 2_500u64;
    let total_items = p_count * per_p;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("2kb_items", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(16)).split();
                let producers: Vec<_> = (0..p_count)
                    .map(|_| {
                        let mut p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..per_p {
                                let w = p.reserve_block().expect("consumer dropped");
                                w.write(black_box(LargeStruct::new(i as u8))).commit();
                            }
                        })
                    })
                    .collect();
                drop(producer);

                let start = std::time::Instant::now();
                let mut received = 0u64;
                while let Some(reader) = consumer.pop_ref_block() {
                    black_box(slow_consume_work(reader.data[0] as u64));
                    received += 1;
                }
                assert_eq!(received, total_items);
                for h in producers {
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
// Async API: push_async / pop_async, two producers + one consumer.
//
// Each side gets its own thread with a current_thread runtime + LocalSet.
// Producers and consumer are !Send so we can't pool them into a single
// multi_thread runtime; cross-thread waking still works because
// current_thread runtime wakers are honored when fired from foreign threads.
//
// Run with: cargo bench --bench mpsc --features async
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
fn run_async_mpsc_bench(
    b: &mut criterion::Bencher,
    cap: usize,
    p_count: u64,
    per_p: u64,
    consumer_work: fn(u64) -> u64,
) {
    let total_items = p_count * per_p;
    b.iter_custom(|iters| {
        let mut total = std::time::Duration::ZERO;
        for _ in 0..iters {
            let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
            let start = std::time::Instant::now();

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
                                    .expect("consumer dropped");
                            }
                        }));
                    })
                })
                .collect();
            drop(producer);

            let consumer_thread = thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let local = tokio::task::LocalSet::new();
                rt.block_on(local.run_until(async move {
                    let mut received = 0u64;
                    while received < total_items {
                        match consumer.pop_async().await {
                            Some(v) => {
                                black_box(consumer_work(v));
                                received += 1;
                            }
                            None => break,
                        }
                    }
                }));
            });

            for h in producer_threads {
                h.join().unwrap();
            }
            consumer_thread.join().unwrap();
            total += start.elapsed();
        }
        total
    });
}

#[cfg(feature = "async")]
fn bench_async_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("blocking_mpsc");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let p_count = 2u64;
    let per_p = 5_000u64;
    let total_items = p_count * per_p;
    group.throughput(Throughput::Elements(total_items));

    // Saturated: small ring, slow consumer. Same shape as the spin/block
    // variants in this group so the comparison is direct.
    group.bench_function("async_saturated", |b| {
        run_async_mpsc_bench(b, 16, p_count, per_p, slow_consume_work);
    });

    // Unsaturated: large ring, fast consumer. Measures async overhead
    // when the ring rarely fills.
    group.bench_function("async_unsaturated", |b| {
        run_async_mpsc_bench(b, 4096, p_count, per_p, |v| v);
    });

    group.finish();
}

#[cfg(not(feature = "async"))]
criterion_group!(
    benches,
    bench_mpsc_scaling,
    bench_contention,
    bench_large_struct_mpsc,
    bench_large_struct_mpsc_zero_copy,
    bench_blocking_mpsc,
    bench_large_struct_mpsc_zero_copy_blocking,
);

#[cfg(feature = "async")]
criterion_group!(
    benches,
    bench_mpsc_scaling,
    bench_contention,
    bench_large_struct_mpsc,
    bench_large_struct_mpsc_zero_copy,
    bench_blocking_mpsc,
    bench_large_struct_mpsc_zero_copy_blocking,
    bench_async_mpsc,
);

criterion_main!(benches);
