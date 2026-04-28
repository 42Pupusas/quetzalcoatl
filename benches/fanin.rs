//! Fan-in benchmarks: N producers → 1 consumer.
//!
//! Compares strategies for aggregating messages from many worker threads:
//!
//! 1. **MPSC ring buffer** — single shared buffer, producers compete via FAA
//! 2. **N×SPSC fan-in** — one SPSC ring per producer, consumer round-robin drains
//! 3. **tokio::sync::mpsc** — async-native channels (bounded + unbounded)
//! 4. **crossbeam** — bounded MPMC channel used as MPSC
//!
//! Buffer capacity is equalized: each SPSC ring gets `PER_RING_CAP` slots,
//! and the MPSC/crossbeam/tokio get `at_least(PER_RING_CAP * N)` so total
//! buffer memory is comparable across strategies.
//!
//! **FIFO note**: MPSC preserves a global FIFO order (FAA establishes a
//! total order on positions). N×SPSC fan-in only preserves per-producer
//! FIFO — cross-producer ordering depends on the consumer's drain schedule.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::thread;
use std::time::{Duration, Instant};

const ITEMS_PER_PRODUCER: u64 = 5_000;
/// Per-ring capacity for SPSC fan-in. MPSC total = PER_RING_CAP * N.
const PER_RING_CAP: usize = 1024;

/// Compute the total buffer capacity for a given number of producers.
/// Returns a power-of-two >= PER_RING_CAP * num_producers.
fn total_cap(num_producers: u64) -> usize {
    let raw = PER_RING_CAP * num_producers as usize;
    raw.next_power_of_two()
}

// =============================================================================
// 1. MPSC (FAA) vs N×SPSC fan-in vs tokio vs crossbeam
// =============================================================================

fn bench_fanin_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanin");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    // Boundary cases only — 8 / 12 produced redundant scaling info on
    // top of (4, 16). Drop them to halve runtime; widen the matrix
    // back if we ever need to chase non-monotonic regressions in the
    // 8-12 producer range.
    for num_producers in [4u64, 16] {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let mpsc_cap = total_cap(num_producers);
        group.throughput(Throughput::Elements(total_items));

        let param = format!("{num_producers}p");

        // --- quetzalcoatl MPSC (pop) ---
        group.bench_with_input(
            BenchmarkId::new("mpsc", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                            quetzalcoatl::capacity::Capacity::at_least(mpsc_cap),
                        )
                        .split();

                        let start = Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|_| {
                                let p = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..ITEMS_PER_PRODUCER {
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

        // --- quetzalcoatl MPSC with drain (amortized head update) ---
        group.bench_with_input(
            BenchmarkId::new("mpsc_drain", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                            quetzalcoatl::capacity::Capacity::at_least(mpsc_cap),
                        )
                        .split();

                        let start = Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|_| {
                                let p = producer.clone();
                                thread::spawn(move || {
                                    for i in 0..ITEMS_PER_PRODUCER {
                                        while p.push(black_box(i)).is_err() {
                                            std::hint::spin_loop();
                                        }
                                    }
                                })
                            })
                            .collect();

                        let mut received = 0u64;
                        while received < total_items {
                            let drained = consumer.drain(|val| {
                                black_box(val);
                            });
                            if drained > 0 {
                                received += drained as u64;
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

        // --- N×SPSC fan-in (round-robin drain) ---
        group.bench_with_input(
            BenchmarkId::new("spsc_fanin", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut consumers: Vec<quetzalcoatl::spsc::Consumer<u64>> = Vec::new();
                        let mut producer_handles: Vec<thread::JoinHandle<()>> = Vec::new();

                        let start = Instant::now();

                        for _ in 0..num_producers {
                            let (producer, consumer) = quetzalcoatl::spsc::RingBuffer::<u64>::new(
                                quetzalcoatl::capacity::Capacity::exact(PER_RING_CAP),
                            )
                            .split();
                            consumers.push(consumer);

                            producer_handles.push(thread::spawn(move || {
                                for i in 0..ITEMS_PER_PRODUCER {
                                    while producer.push(black_box(i)).is_err() {
                                        std::hint::spin_loop();
                                    }
                                }
                            }));
                        }

                        // Consumer: round-robin drain all SPSC rings
                        let mut received = 0u64;
                        while received < total_items {
                            let mut made_progress = false;
                            for consumer in consumers.iter_mut() {
                                if consumer.pop().is_some() {
                                    received += 1;
                                    made_progress = true;
                                }
                            }
                            if !made_progress {
                                std::hint::spin_loop();
                            }
                        }

                        for h in producer_handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // --- N×SPSC fan-in (batch drain) ---
        group.bench_with_input(
            BenchmarkId::new("spsc_fanin_batch", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut consumers: Vec<quetzalcoatl::spsc::Consumer<u64>> = Vec::new();
                        let mut producer_handles: Vec<thread::JoinHandle<()>> = Vec::new();

                        let start = Instant::now();

                        for _ in 0..num_producers {
                            let (producer, consumer) = quetzalcoatl::spsc::RingBuffer::<u64>::new(
                                quetzalcoatl::capacity::Capacity::exact(PER_RING_CAP),
                            )
                            .split();
                            consumers.push(consumer);

                            producer_handles.push(thread::spawn(move || {
                                for i in 0..ITEMS_PER_PRODUCER {
                                    while producer.push(black_box(i)).is_err() {
                                        std::hint::spin_loop();
                                    }
                                }
                            }));
                        }

                        // Consumer: drain each ring in bursts of up to 64 items
                        let mut received = 0u64;
                        while received < total_items {
                            let mut made_progress = false;
                            for consumer in consumers.iter_mut() {
                                for _ in 0..64 {
                                    if consumer.pop().is_some() {
                                        received += 1;
                                        made_progress = true;
                                    } else {
                                        break;
                                    }
                                }
                            }
                            if !made_progress {
                                std::hint::spin_loop();
                            }
                        }

                        for h in producer_handles {
                            h.join().unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // --- tokio mpsc unbounded ---
        group.bench_with_input(
            BenchmarkId::new("tokio_unbounded", &param),
            &num_producers,
            |b, &num_producers| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_producers as usize + 1)
                    .build()
                    .unwrap();

                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(async {
                            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();

                            let start = Instant::now();

                            let handles: Vec<_> = (0..num_producers)
                                .map(|_| {
                                    let t = tx.clone();
                                    tokio::spawn(async move {
                                        for i in 0..ITEMS_PER_PRODUCER {
                                            t.send(black_box(i)).unwrap();
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

        // --- tokio mpsc bounded ---
        group.bench_with_input(
            BenchmarkId::new("tokio_bounded", &param),
            &num_producers,
            |b, &num_producers| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_producers as usize + 1)
                    .build()
                    .unwrap();

                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(async {
                            let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(mpsc_cap);

                            let start = Instant::now();

                            let handles: Vec<_> = (0..num_producers)
                                .map(|_| {
                                    let t = tx.clone();
                                    tokio::spawn(async move {
                                        for i in 0..ITEMS_PER_PRODUCER {
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

        // --- crossbeam bounded ---
        group.bench_with_input(
            BenchmarkId::new("crossbeam", &param),
            &num_producers,
            |b, &num_producers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (tx, rx) = crossbeam_channel::bounded::<u64>(mpsc_cap);

                        let start = Instant::now();

                        let handles: Vec<_> = (0..num_producers)
                            .map(|_| {
                                let t = tx.clone();
                                thread::spawn(move || {
                                    for i in 0..ITEMS_PER_PRODUCER {
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
    }
    group.finish();
}

// =============================================================================
// 2. Buffer capacity impact on MPSC at high producer counts
// =============================================================================

fn bench_fanin_capacity_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanin_capacity");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let num_producers = 12u64;
    let total_items = ITEMS_PER_PRODUCER * num_producers;

    for cap in [256, 1024, 4096, 8192, 16384] {
        group.throughput(Throughput::Elements(total_items));

        group.bench_with_input(BenchmarkId::new("mpsc", cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                        quetzalcoatl::capacity::Capacity::exact(cap),
                    )
                    .split();

                    let start = Instant::now();

                    let handles: Vec<_> = (0..num_producers)
                        .map(|_| {
                            let p = producer.clone();
                            thread::spawn(move || {
                                for i in 0..ITEMS_PER_PRODUCER {
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

// =============================================================================
// 3. Consumer batch drain: pop one-at-a-time vs drain (amortized head)
// =============================================================================

fn bench_consumer_drain_strategy(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain_strategy");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let num_producers = 12u64;
    let total_items = ITEMS_PER_PRODUCER * num_producers;
    let cap = total_cap(num_producers);
    group.throughput(Throughput::Elements(total_items));

    // --- pop one-at-a-time ---
    group.bench_function("mpsc_single_pop", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::at_least(cap),
                )
                .split();

                let start = Instant::now();

                let handles: Vec<_> = (0..num_producers)
                    .map(|_| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..ITEMS_PER_PRODUCER {
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

    // --- batch drain (amortized head update) ---
    group.bench_function("mpsc_batch_drain", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::at_least(cap),
                )
                .split();

                let start = Instant::now();

                let handles: Vec<_> = (0..num_producers)
                    .map(|_| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..ITEMS_PER_PRODUCER {
                                while p.push(black_box(i)).is_err() {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();

                let mut received = 0u64;
                while received < total_items {
                    let drained = consumer.drain(|_val| {
                        black_box(_val);
                    });
                    if drained > 0 {
                        received += drained as u64;
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

    group.finish();
}

criterion_group!(
    benches,
    bench_fanin_throughput,
    bench_fanin_capacity_impact,
    bench_consumer_drain_strategy,
);
criterion_main!(benches);
