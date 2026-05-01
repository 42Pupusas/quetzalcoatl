use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spsc::RingBuffer;
use std::thread;

// ---------------------------------------------------------------------------
// 1. Single-thread push-only throughput
// ---------------------------------------------------------------------------

fn bench_push_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_only");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for cap in [1024, 4096, 65536] {
        group.throughput(Throughput::Elements(cap as u64));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, _consumer) =
                        RingBuffer::<u64>::new(Capacity::exact(cap)).split();
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
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

    for cap in [1024, 4096, 65536] {
        group.throughput(Throughput::Elements(cap as u64));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, mut consumer) =
                        RingBuffer::<u64>::new(Capacity::exact(cap)).split();
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
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let ops = 10_000u64;

    group.throughput(Throughput::Elements(ops));
    group.bench_function("10k_ops", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(64)).split();
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
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));

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
                            RingBuffer::<u64>::new(Capacity::exact(4096)).split();

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
// 5. Buffer capacity scaling
// ---------------------------------------------------------------------------

fn bench_capacity_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_impact");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let ops = 50_000u64;

    for cap in [1000, 1024, 4000, 4096] {
        group.throughput(Throughput::Elements(ops));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &cap| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (producer, mut consumer) =
                        RingBuffer::<u64>::new(Capacity::at_least(cap)).split();

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

// ---------------------------------------------------------------------------
// 6. Large struct benchmarks (~2KB per element)
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

fn bench_large_struct_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_spsc");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let total_items = 10_000u64;

    group.throughput(Throughput::Elements(total_items));
    group.bench_function("2kb_items", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) =
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
    group.finish();
}

// ---------------------------------------------------------------------------
// 7. Large struct zero-copy SPSC (reserve/pop_ref)
// ---------------------------------------------------------------------------

fn bench_large_struct_spsc_zero_copy(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_spsc_zero_copy");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let total_items = 10_000u64;

    group.throughput(Throughput::Elements(total_items));
    group.bench_function("2kb_items", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
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
// Blocking API: push_block / pop_block under contention.
//
// Small ring + slow consumer so the ring fills up frequently, forcing the
// producer to wait. Compares spin-retry vs push_block / pop_block: the
// blocking variants should not regress throughput vs spin-retry under
// saturation, and should burn far less CPU when truly idle (not measured
// here — see `examples/mpmc_block.rs` for that).
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

fn bench_blocking_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("blocking_spsc");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let total_items = 10_000u64;
    group.throughput(Throughput::Elements(total_items));

    // Spin baseline: producer retries on Err, consumer spin-loops on None.
    group.bench_function("spin", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let start = std::time::Instant::now();
                let h = thread::spawn(move || {
                    for i in 0..total_items {
                        while producer.push(i).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                });
                let mut received = 0u64;
                while received < total_items {
                    if let Some(v) = consumer.pop() {
                        black_box(slow_consume_work(v));
                        received += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                h.join().unwrap();
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
                let start = std::time::Instant::now();
                let h = thread::spawn(move || {
                    for i in 0..total_items {
                        producer.push_block(i).expect("consumer dropped");
                    }
                });
                let mut received = 0u64;
                while received < total_items {
                    match consumer.pop_block() {
                        Some(v) => {
                            black_box(slow_consume_work(v));
                            received += 1;
                        }
                        None => break,
                    }
                }
                h.join().unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Large struct + zero-copy blocking: reserve_block / pop_ref_block under
// contention. Small ring (16) and 2KB items so the producer waits often.
// ---------------------------------------------------------------------------

fn bench_large_struct_spsc_zero_copy_blocking(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_struct_spsc_zero_copy_blocking");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let total_items = 5_000u64;
    group.throughput(Throughput::Elements(total_items));

    group.bench_function("2kb_items", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (mut producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(16)).split();
                let start = std::time::Instant::now();
                let h = thread::spawn(move || {
                    for i in 0..total_items {
                        let w = producer.reserve_block().expect("consumer dropped");
                        w.write(black_box(LargeStruct::new(i as u8))).commit();
                    }
                });
                let mut received = 0u64;
                while received < total_items {
                    match consumer.pop_ref_block() {
                        Some(reader) => {
                            black_box(slow_consume_work(reader.data[0] as u64));
                            received += 1;
                        }
                        None => break,
                    }
                }
                h.join().unwrap();
                total += start.elapsed();
            }
            total
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_push_only,
    bench_pop_only,
    bench_push_pop_alternating,
    bench_spsc_concurrent,
    bench_capacity_scaling,
    bench_large_struct_spsc,
    bench_large_struct_spsc_zero_copy,
    bench_blocking_spsc,
    bench_large_struct_spsc_zero_copy_blocking,
);
criterion_main!(benches);
