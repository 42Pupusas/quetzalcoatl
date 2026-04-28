use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

// =============================================================================
// SPSC: quetzalcoatl vs crossbeam (1 producer, 1 consumer)
// =============================================================================

fn bench_spsc_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cmp_spsc");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for total_items in [100_000u64, 1_000_000] {
        group.throughput(Throughput::Elements(total_items));

        // --- quetzalcoatl ---
        group.bench_with_input(
            BenchmarkId::new("quetzalcoatl", total_items),
            &total_items,
            |b, &total_items| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) = quetzalcoatl::spsc::RingBuffer::<u64>::new(
                            quetzalcoatl::capacity::Capacity::exact(4096),
                        )
                        .split();

                        let start = std::time::Instant::now();

                        let ph = thread::spawn(move || {
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

                        ph.join().unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // --- crossbeam ---
        group.bench_with_input(
            BenchmarkId::new("crossbeam", total_items),
            &total_items,
            |b, &total_items| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (tx, rx) = crossbeam_channel::bounded::<u64>(4096);

                        let start = std::time::Instant::now();

                        let ph = thread::spawn(move || {
                            for i in 0..total_items {
                                tx.send(black_box(i)).unwrap();
                            }
                        });

                        let mut received = 0u64;
                        while received < total_items {
                            if rx.recv().is_ok() {
                                received += 1;
                            }
                        }

                        ph.join().unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// =============================================================================
// MPSC: quetzalcoatl vs crossbeam vs tokio (N producers, 1 consumer)
// =============================================================================

fn bench_mpsc_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cmp_mpsc");
    let items_per_producer = 5_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for num_producers in [1u64, 2, 4, 8, 12, 16] {
        let total_items = items_per_producer * num_producers;
        group.throughput(Throughput::Elements(total_items));

        let param = format!("{num_producers}p");

        // --- quetzalcoatl ---
        group.bench_with_input(
            BenchmarkId::new("quetzalcoatl", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                            quetzalcoatl::capacity::Capacity::exact(8192),
                        )
                        .split();

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

        // --- crossbeam ---
        group.bench_with_input(
            BenchmarkId::new("crossbeam", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (tx, rx) = crossbeam_channel::bounded::<u64>(8192);

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|_| {
                                let t = tx.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        t.send(black_box(i)).unwrap();
                                    }
                                })
                            })
                            .collect();

                        drop(tx);

                        let mut received = 0u64;
                        while received < total_items {
                            if rx.recv().is_ok() {
                                received += 1;
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

        // --- tokio mpsc ---
        group.bench_with_input(
            BenchmarkId::new("tokio", &param),
            &num_producers,
            |b, &num_producers| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_producers as usize + 1)
                    .build()
                    .unwrap();

                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(async {
                            let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(8192);

                            let start = std::time::Instant::now();

                            let handles: Vec<_> = (0..num_producers)
                                .map(|_| {
                                    let t = tx.clone();
                                    tokio::spawn(async move {
                                        for i in 0..items_per_producer {
                                            t.send(black_box(i)).await.unwrap();
                                        }
                                    })
                                })
                                .collect();

                            drop(tx);

                            let mut received = 0u64;
                            while received < total_items {
                                if rx.recv().await.is_some() {
                                    received += 1;
                                }
                            }

                            for h in handles {
                                h.await.unwrap();
                            }
                            start.elapsed()
                        });
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// =============================================================================
// SPMC: quetzalcoatl vs crossbeam (1 producer, N consumers)
// =============================================================================

fn bench_spmc_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cmp_spmc");
    let total_items = 20_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for num_consumers in [1u64, 2, 4, 8] {
        group.throughput(Throughput::Elements(total_items));

        let param = format!("{num_consumers}c");

        // --- quetzalcoatl ---
        group.bench_with_input(
            BenchmarkId::new("quetzalcoatl", &param),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) = quetzalcoatl::spmc::RingBuffer::<u64>::new(
                            quetzalcoatl::capacity::Capacity::exact(8192),
                        )
                        .split();

                        // Use a read-only done flag instead of a shared
                        // decrement counter. Consumers only READ this flag
                        // (no writes = no cache line invalidation between
                        // consumer threads).
                        let done = Arc::new(AtomicBool::new(false));

                        let start = std::time::Instant::now();

                        let consumer_handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let c = consumer.clone();
                                let d = Arc::clone(&done);
                                thread::spawn(move || {
                                    loop {
                                        if c.pop().is_some() {
                                            // consumed
                                        } else if d.load(Ordering::Relaxed) {
                                            // Producer finished — drain stragglers
                                            while c.pop().is_some() {}
                                            break;
                                        } else {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        // Producer pushes all items
                        for i in 0..total_items {
                            while producer.push(black_box(i)).is_err() {
                                std::hint::spin_loop();
                            }
                        }

                        done.store(true, Ordering::Relaxed);

                        for h in consumer_handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // --- crossbeam (MPMC channel used as SPMC) ---
        group.bench_with_input(
            BenchmarkId::new("crossbeam", &param),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (tx, rx) = crossbeam_channel::bounded::<u64>(8192);

                        let done = Arc::new(AtomicBool::new(false));

                        let start = std::time::Instant::now();

                        let consumer_handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let r = rx.clone();
                                let d = Arc::clone(&done);
                                thread::spawn(move || {
                                    loop {
                                        if r.try_recv().is_ok() {
                                            // consumed
                                        } else if d.load(Ordering::Relaxed) {
                                            while r.try_recv().is_ok() {}
                                            break;
                                        } else {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        for i in 0..total_items {
                            tx.send(black_box(i)).unwrap();
                        }

                        done.store(true, Ordering::Relaxed);

                        for h in consumer_handles {
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

// =============================================================================
// Broadcast: quetzalcoatl vs tokio::sync::broadcast
// (every consumer sees every message)
// =============================================================================

fn bench_broadcast_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cmp_broadcast");
    let total_items = 10_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for num_consumers in [1u64, 2, 4, 8] {
        group.throughput(Throughput::Elements(total_items));

        let param = format!("{num_consumers}c");

        // --- quetzalcoatl ---
        group.bench_with_input(
            BenchmarkId::new("quetzalcoatl", &param),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) = quetzalcoatl::broadcast::RingBuffer::<u64>::new(
                            quetzalcoatl::capacity::Capacity::exact(4096),
                            num_consumers as usize + 1,
                        )
                        .split();
                        let mut consumers: Vec<_> =
                            (0..num_consumers - 1).map(|_| consumer.clone()).collect();
                        consumers.push(consumer);

                        let start = std::time::Instant::now();

                        let ph = thread::spawn(move || {
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

                        ph.join().unwrap();
                        for h in handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // --- tokio broadcast ---
        group.bench_with_input(
            BenchmarkId::new("tokio", &param),
            &num_consumers,
            |b, &num_consumers| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_consumers as usize + 1)
                    .build()
                    .unwrap();

                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(async {
                            let (tx, _) = tokio::sync::broadcast::channel::<u64>(4096);

                            let mut receivers: Vec<_> =
                                (0..num_consumers).map(|_| tx.subscribe()).collect();

                            let start = std::time::Instant::now();

                            let ph = tokio::spawn(async move {
                                for i in 0..total_items {
                                    // Spin-retry if the channel is full (lagged receivers)
                                    loop {
                                        match tx.send(black_box(i)) {
                                            Ok(_) => break,
                                            Err(_) => tokio::task::yield_now().await,
                                        }
                                    }
                                }
                            });

                            let handles: Vec<_> = receivers
                                .drain(..)
                                .map(|mut rx| {
                                    tokio::spawn(async move {
                                        let mut received = 0u64;
                                        while received < total_items {
                                            match rx.recv().await {
                                                Ok(_) => received += 1,
                                                Err(
                                                    tokio::sync::broadcast::error::RecvError::Lagged(
                                                        n,
                                                    ),
                                                ) => received += n,
                                                Err(_) => break,
                                            }
                                        }
                                    })
                                })
                                .collect();

                            ph.await.unwrap();
                            for h in handles {
                                h.await.unwrap();
                            }
                            start.elapsed()
                        });
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// =============================================================================
// Large struct (~2KB) — the zero-copy differentiator
// =============================================================================

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

// ---------------------------------------------------------------------------
// Large struct SPSC: push/pop vs reserve/pop_ref vs crossbeam
// ---------------------------------------------------------------------------

fn bench_large_spsc_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cmp_large_spsc");
    let total_items = 5_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.throughput(Throughput::Elements(total_items));

    // --- quetzalcoatl push/pop (copies both ways) ---
    group.bench_function("quetzalcoatl_copy", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = quetzalcoatl::spsc::RingBuffer::<LargeStruct>::new(
                    quetzalcoatl::capacity::Capacity::exact(256),
                )
                .split();

                let start = std::time::Instant::now();

                let ph = thread::spawn(move || {
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

                ph.join().unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    // --- quetzalcoatl reserve/pop_ref (zero-copy both ways) ---
    group.bench_function("quetzalcoatl_zerocopy", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (mut producer, mut consumer) =
                    quetzalcoatl::spsc::RingBuffer::<LargeStruct>::new(
                        quetzalcoatl::capacity::Capacity::exact(256),
                    )
                    .split();

                let start = std::time::Instant::now();

                let ph = thread::spawn(move || {
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

                ph.join().unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    // --- crossbeam (always copies) ---
    group.bench_function("crossbeam", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = crossbeam_channel::bounded::<LargeStruct>(256);

                let start = std::time::Instant::now();

                let ph = thread::spawn(move || {
                    for i in 0..total_items {
                        tx.send(black_box(LargeStruct::new(i as u8))).unwrap();
                    }
                });

                let mut received = 0u64;
                while received < total_items {
                    if rx.recv().is_ok() {
                        received += 1;
                    }
                }

                ph.join().unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Large struct MPSC: quetzalcoatl (copy + zerocopy) vs crossbeam vs tokio
// ---------------------------------------------------------------------------

fn bench_large_mpsc_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cmp_large_mpsc");
    let items_per_producer = 1_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for num_producers in [2u64, 4] {
        let total_items = items_per_producer * num_producers;
        group.throughput(Throughput::Elements(total_items));
        let param = format!("{num_producers}p");

        // --- quetzalcoatl push/pop ---
        group.bench_with_input(
            BenchmarkId::new("quetzalcoatl_copy", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) =
                            quetzalcoatl::mpsc::RingBuffer::<LargeStruct>::new(
                                quetzalcoatl::capacity::Capacity::exact(256),
                            )
                            .split();

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|p| {
                                let prod = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        while prod
                                            .push(black_box(LargeStruct::new((p * 100 + i) as u8)))
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

        // --- quetzalcoatl reserve/pop_ref ---
        group.bench_with_input(
            BenchmarkId::new("quetzalcoatl_zerocopy", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) =
                            quetzalcoatl::mpsc::RingBuffer::<LargeStruct>::new(
                                quetzalcoatl::capacity::Capacity::exact(256),
                            )
                            .split();

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|p| {
                                let mut prod = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        loop {
                                            if let Some(w) = prod.reserve() {
                                                w.write(black_box(LargeStruct::new(
                                                    (p * 100 + i) as u8,
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

        // --- crossbeam ---
        group.bench_with_input(
            BenchmarkId::new("crossbeam", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (tx, rx) = crossbeam_channel::bounded::<LargeStruct>(256);

                        let start = std::time::Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|p| {
                                let t = tx.clone();
                                thread::spawn(move || {
                                    for i in 0..items_per_producer {
                                        t.send(black_box(LargeStruct::new((p * 100 + i) as u8)))
                                            .unwrap();
                                    }
                                })
                            })
                            .collect();

                        drop(tx);

                        let mut received = 0u64;
                        while received < total_items {
                            if rx.recv().is_ok() {
                                received += 1;
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

        // --- tokio mpsc ---
        group.bench_with_input(
            BenchmarkId::new("tokio", &param),
            &num_producers,
            |b, &num_producers| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_producers as usize + 1)
                    .build()
                    .unwrap();

                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(async {
                            let (tx, mut rx) = tokio::sync::mpsc::channel::<LargeStruct>(256);

                            let start = std::time::Instant::now();

                            let handles: Vec<_> = (0..num_producers)
                                .map(|p| {
                                    let t = tx.clone();
                                    tokio::spawn(async move {
                                        for i in 0..items_per_producer {
                                            t.send(black_box(LargeStruct::new(
                                                (p * 100 + i) as u8,
                                            )))
                                            .await
                                            .unwrap();
                                        }
                                    })
                                })
                                .collect();

                            drop(tx);

                            let mut received = 0u64;
                            while received < total_items {
                                if rx.recv().await.is_some() {
                                    received += 1;
                                }
                            }

                            for h in handles {
                                h.await.unwrap();
                            }
                            start.elapsed()
                        });
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Large struct SPMC: quetzalcoatl (copy + zerocopy) vs crossbeam
// ---------------------------------------------------------------------------

fn bench_large_spmc_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cmp_large_spmc");
    let total_items = 2_000u64;
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for num_consumers in [2u64, 4] {
        group.throughput(Throughput::Elements(total_items));
        let param = format!("{num_consumers}c");

        // --- quetzalcoatl push/pop ---
        group.bench_with_input(
            BenchmarkId::new("quetzalcoatl_copy", &param),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, consumer) =
                            quetzalcoatl::spmc::RingBuffer::<LargeStruct>::new(
                                quetzalcoatl::capacity::Capacity::exact(256),
                            )
                            .split();

                        let remaining = Arc::new(AtomicU64::new(total_items));

                        let start = std::time::Instant::now();

                        let consumer_handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let c = consumer.clone();
                                let rem = Arc::clone(&remaining);
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

                        for i in 0..total_items {
                            while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                                std::hint::spin_loop();
                            }
                        }

                        for h in consumer_handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // --- crossbeam ---
        group.bench_with_input(
            BenchmarkId::new("crossbeam", &param),
            &num_consumers,
            |b, &num_consumers| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (tx, rx) = crossbeam_channel::bounded::<LargeStruct>(256);

                        let remaining = Arc::new(AtomicU64::new(total_items));

                        let start = std::time::Instant::now();

                        let consumer_handles: Vec<_> = (0..num_consumers)
                            .map(|_| {
                                let r = rx.clone();
                                let rem = Arc::clone(&remaining);
                                thread::spawn(move || {
                                    while rem.load(Ordering::Relaxed) > 0 {
                                        if r.try_recv().is_ok() {
                                            rem.fetch_sub(1, Ordering::Relaxed);
                                        } else {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        for i in 0..total_items {
                            tx.send(black_box(LargeStruct::new(i as u8))).unwrap();
                        }

                        for h in consumer_handles {
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
    bench_spsc_comparison,
    bench_mpsc_comparison,
    bench_spmc_comparison,
    bench_broadcast_comparison,
    bench_large_spsc_comparison,
    bench_large_mpsc_comparison,
    bench_large_spmc_comparison,
);
criterion_main!(benches);
