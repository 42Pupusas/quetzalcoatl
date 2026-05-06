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

// ---------------------------------------------------------------------------
// Async API: push_async / pop_async
//
// Two scenarios mirroring the blocking bench:
//
// "saturated" — small ring (16), slow consumer (64 mul-add rounds).
//   The ring fills frequently; the producer task yields and the executor
//   reschedules the consumer. Measures steady-state throughput when the
//   channel is the bottleneck.
//
// "unsaturated" — large ring (4096), fast consumer (no extra work).
//   The ring rarely fills; measures overhead of the async machinery
//   (poll_fn overhead, waker registration) vs a raw spin loop.
//
// Both variants are placed in the same group as "spin" and "block" so a
// single criterion run produces a side-by-side comparison table.
//
// Run with: cargo bench --bench spsc --features async
// ---------------------------------------------------------------------------

// Each side runs on a long-lived thread with a single current_thread runtime
// that persists across all iterations — avoids the cost (and glibc tcache
// issues) of creating/destroying a runtime per iteration.
// Iteration handoff uses std channels: main sends a ring half to each worker,
// workers run to completion, then signal done via a barrier.
#[cfg(feature = "async")]
fn run_async_bench(
    b: &mut criterion::Bencher,
    cap: usize,
    total_items: u64,
    consumer_work: fn(u64) -> u64,
) {
    use std::sync::{mpsc, Arc, Barrier};

    type ProducerMsg = quetzalcoatl::spsc::Producer<u64>;
    type ConsumerMsg = quetzalcoatl::spsc::Consumer<u64>;

    let (p_tx, p_rx) = mpsc::channel::<ProducerMsg>();
    let (c_tx, c_rx) = mpsc::channel::<ConsumerMsg>();
    let barrier = Arc::new(Barrier::new(3)); // main + producer + consumer

    let p_barrier = barrier.clone();
    let producer_thread = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        while let Ok(producer) = p_rx.recv() {
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                for i in 0..total_items {
                    producer.push_async(i).await.expect("consumer dropped");
                }
            }));
            p_barrier.wait();
        }
    });

    let c_barrier = barrier.clone();
    let consumer_thread = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        while let Ok(mut consumer) = c_rx.recv() {
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
            c_barrier.wait();
        }
    });

    b.iter_custom(|iters| {
        let mut total = std::time::Duration::ZERO;
        for _ in 0..iters {
            let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
            let start = std::time::Instant::now();
            p_tx.send(producer).unwrap();
            c_tx.send(consumer).unwrap();
            barrier.wait(); // wait for both sides to finish
            total += start.elapsed();
        }
        total
    });

    drop(p_tx);
    drop(c_tx);
    producer_thread.join().unwrap();
    consumer_thread.join().unwrap();
}

#[cfg(feature = "async")]
fn bench_async_spsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("blocking_spsc");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    let total_items = 10_000u64;
    group.throughput(Throughput::Elements(total_items));

    // Saturated: small ring (16), slow consumer. Matches spin/block topology.
    group.bench_function("async_saturated", |b| {
        run_async_bench(b, 16, total_items, slow_consume_work);
    });

    // Unsaturated: large ring (4096), fast consumer.
    group.bench_function("async_unsaturated", |b| {
        run_async_bench(b, 4096, total_items, |v| v);
    });

    group.finish();
}

#[cfg(not(feature = "async"))]
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

#[cfg(feature = "async")]
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
    bench_async_spsc,
);

criterion_main!(benches);
