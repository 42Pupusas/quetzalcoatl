use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::RingBuffer;
use std::thread;

// ---------------------------------------------------------------------------
// 1. MPSC – scale producers from 1 to 8
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

criterion_group!(
    benches,
    bench_mpsc_scaling,
    bench_contention,
    bench_large_struct_mpsc,
    bench_large_struct_mpsc_zero_copy,
);
criterion_main!(benches);
