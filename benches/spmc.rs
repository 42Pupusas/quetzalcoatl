use divan::black_box;
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spmc::RingBuffer;
use std::thread;

fn main() {
    divan::main();
}

// ---------------------------------------------------------------------------
// Helpers
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

#[inline(never)]
fn slow_consume_work(seed: u64) -> u64 {
    let mut x = seed;
    for _ in 0..64 {
        x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        x = black_box(x);
    }
    x
}

// ---------------------------------------------------------------------------
// 1. SPMC -- scale consumers from 1 to 16
//
// Measures end-to-end throughput as the number of competing consumers grows.
// The producer runs on the bench thread; consumer threads race on CAS(head).
// ---------------------------------------------------------------------------

mod spmc_scaling {
    use super::*;

    const TOTAL_ITEMS: u64 = 20_000;

    #[divan::bench(args = [1, 8, 16])]
    fn consumers(bencher: divan::Bencher, num_consumers: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(8192)).split();

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

                // Drop the original consumer handle so only the workers hold refs.
                drop(consumer);

                for i in 0..TOTAL_ITEMS {
                    while producer.push(black_box(i)).is_err() {
                        std::hint::spin_loop();
                    }
                }
                // Signal workers to drain and exit.
                drop(producer);
                for h in handles {
                    h.join().unwrap();
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 2. Producer-only throughput -- isolate the push hot path.
//
// No consumer threads run: the bench drains via a single consumer between
// pushes only when full. This isolates the cost of push() itself, exposing
// any per-push overhead (e.g. unnecessary atomic stores) without consumer
// noise.
// ---------------------------------------------------------------------------

mod spmc_producer_only {
    use super::*;

    const TOTAL_ITEMS: u64 = 100_000;

    #[divan::bench]
    fn push_drain(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(8192)).split();

                for i in 0..TOTAL_ITEMS {
                    while producer.push(black_box(i)).is_err() {
                        // Drain inline -- no other thread is consuming.
                        while consumer.pop().is_some() {}
                    }
                }
                while consumer.pop().is_some() {}
            });
    }
}

// ---------------------------------------------------------------------------
// 3. Producer push latency under consumer pressure.
//
// Mirrors `bench_contention` from the MPSC suite: small buffer, many
// consumers, measures the producer's push throughput when the slot
// sequence cache lines are heavily shared.
// ---------------------------------------------------------------------------

mod spmc_contention {
    use super::*;

    const TOTAL_ITEMS: u64 = 10_000;
    const NUM_CONSUMERS: usize = 8;

    #[divan::bench(args = [64, 256, 1024])]
    fn cap(bencher: divan::Bencher, cap: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();

                let handles: Vec<_> = (0..NUM_CONSUMERS)
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

                for i in 0..TOTAL_ITEMS {
                    while producer.push(black_box(i)).is_err() {
                        std::hint::spin_loop();
                    }
                }
                drop(producer);
                for h in handles {
                    h.join().unwrap();
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 4. Large struct SPMC (~2KB per element)
// ---------------------------------------------------------------------------

mod large_struct_spmc {
    use super::*;

    const TOTAL_ITEMS: u64 = 10_000;

    #[divan::bench(args = [1, 2, 4])]
    fn consumers(bencher: divan::Bencher, num_consumers: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
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

                for i in 0..TOTAL_ITEMS {
                    while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                        std::hint::spin_loop();
                    }
                }
                drop(producer);
                for h in handles {
                    h.join().unwrap();
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 5. Large struct zero-copy SPMC (reserve / pop_ref)
// ---------------------------------------------------------------------------

mod large_struct_spmc_zero_copy {
    use super::*;

    const TOTAL_ITEMS: u64 = 10_000;

    #[divan::bench(args = [1, 2, 4])]
    fn consumers(bencher: divan::Bencher, num_consumers: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
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

                for i in 0..TOTAL_ITEMS {
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
            });
    }
}

// ---------------------------------------------------------------------------
// 6. SPMC vs N x SPSC round-robin -- the work-distribution comparison.
//
// One producer hands out `total_items` to N consumers two ways:
//   - via a single SPMC ring (consumers race CAS on head)
//   - via N independent SPSC rings, producer round-robins pushes
//
// Same total work, same consumer count. This is the bench that justifies
// (or invalidates) the SPMC redesign.
// ---------------------------------------------------------------------------

mod work_distribution {
    use super::*;

    const TOTAL_ITEMS: u64 = 20_000;

    mod spmc {
        use super::*;

        #[divan::bench(args = [2, 4, 8])]
        fn consumers(bencher: divan::Bencher, num_consumers: usize) {
            bencher
                .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
                .bench(|| {
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

                    for i in 0..TOTAL_ITEMS {
                        while producer.push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                    // Drop producer so consumers see `closed` and exit.
                    drop(producer);
                    for h in handles {
                        h.join().unwrap();
                    }
                });
        }
    }

    mod nx_spsc {
        use super::*;

        #[divan::bench(args = [2, 4, 8])]
        fn consumers(bencher: divan::Bencher, num_consumers: usize) {
            bencher
                .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
                .bench(|| {
                    // Per-ring capacity: keep aggregate buffering ~ SPMC's 8192
                    // so neither side gets an unfair backpressure advantage.
                    let per_ring_cap = (8192usize / num_consumers).next_power_of_two();
                    let mut producers = Vec::with_capacity(num_consumers);
                    let mut consumers = Vec::with_capacity(num_consumers);
                    for _ in 0..num_consumers {
                        let (p, c) = quetzalcoatl::spsc::RingBuffer::<u64>::new(Capacity::exact(
                            per_ring_cap,
                        ))
                        .split();
                        producers.push(p);
                        consumers.push(c);
                    }

                    let handles: Vec<_> = consumers
                        .into_iter()
                        .map(|mut c| {
                            let per_consumer_items = TOTAL_ITEMS / num_consumers as u64;
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

                    for i in 0..TOTAL_ITEMS {
                        let target = (i as usize) % num_consumers;
                        while producers[target].push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                });
        }
    }
}

// ---------------------------------------------------------------------------
// 7. Slow-work parallelism -- the verify-pool workload.
//
// Mirrors the real consumer pathology: per-item work is ~50us (e.g. a Schnorr
// verify), much slower than queue overhead. Producer pushes at ~10us cadence,
// fast enough to keep the queue non-empty so consumers actually contend.
//
// Ideal parallelism: time ~ (items x work) / consumers. If a single consumer
// claims a long run of items at once (batched-CAS monopoly window), other
// consumers stall and effective parallelism collapses toward 1, regardless
// of how many consumer threads we spawned.
//
// Compared against N x SPSC round-robin -- the workload-correct baseline for
// "I want M parallel verifies." If SPMC is significantly slower than N x SPSC
// here, the SPMC claim path is the bug, not the workload.
// ---------------------------------------------------------------------------

mod slow_work_scaling {
    use super::*;

    const TOTAL_ITEMS: u64 = 1_000;

    mod spmc {
        use super::*;

        #[divan::bench(args = [1, 2, 4, 8])]
        fn consumers(bencher: divan::Bencher, num_consumers: usize) {
            let consumer_work_iters = calibrate_spin_iters(50_000);
            let producer_gap_iters = calibrate_spin_iters(10_000);

            bencher
                .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
                .bench(|| {
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

                    for i in 0..TOTAL_ITEMS {
                        while producer.push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                        busy_work(producer_gap_iters);
                    }
                    drop(producer);
                    for h in handles {
                        h.join().unwrap();
                    }
                });
        }
    }

    mod nx_spsc {
        use super::*;

        #[divan::bench(args = [1, 2, 4, 8])]
        fn consumers(bencher: divan::Bencher, num_consumers: usize) {
            let consumer_work_iters = calibrate_spin_iters(50_000);
            let producer_gap_iters = calibrate_spin_iters(10_000);

            bencher
                .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
                .bench(|| {
                    let per_ring_cap = (1024usize / num_consumers).next_power_of_two();
                    let mut producers = Vec::with_capacity(num_consumers);
                    let mut consumers = Vec::with_capacity(num_consumers);
                    for _ in 0..num_consumers {
                        let (p, c) = quetzalcoatl::spsc::RingBuffer::<u64>::new(Capacity::exact(
                            per_ring_cap,
                        ))
                        .split();
                        producers.push(p);
                        consumers.push(c);
                    }

                    let handles: Vec<_> = consumers
                        .into_iter()
                        .map(|mut c| {
                            let per_consumer_items = TOTAL_ITEMS / num_consumers as u64;
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

                    for i in 0..TOTAL_ITEMS {
                        let target = (i as usize) % num_consumers;
                        while producers[target].push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                        busy_work(producer_gap_iters);
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                });
        }
    }
}

// ---------------------------------------------------------------------------
// 8. Burst-producer slow-work -- try to trigger the monopoly window.
//
// The smooth-cadence variant (#7) showed near-linear SPMC scaling because
// the queue stays shallow: bounded-CAS clamps `take = min(BATCH_SIZE, tail
// - head)`, so when only a handful of items are queued, no consumer can
// claim more than its fair share.
//
// The monopoly window is supposed to open when the queue is *bursty and
// shallow*: producer publishes a small burst (K <= BATCH_SIZE items), then
// idles. The first consumer to CAS grabs the whole burst; the others see
// `head == tail` and re-park or spin. By the time the lucky consumer
// finishes its 1.6ms of work, the producer has emitted the next burst,
// and the same consumer (already polling) wins again.
//
// If this bench shows SPMC stuck near 1c-time while N x SPSC scales, the
// pathology is real and the FAA-per-item rewrite is justified. If SPMC
// still scales here, the diagnosis as written is wrong on this hardware
// and we should look harder before redesigning.
// ---------------------------------------------------------------------------

mod burst_producer_slow_work {
    use super::*;

    const TOTAL_ITEMS: u64 = 1_024; // multiple of all burst sizes & consumer counts

    // Double-parameterized: burst x consumers. Divan's `args` only supports a
    // single parameter, so we flatten into (burst, num_consumers) tuples.
    const CONFIGS: [(usize, usize); 4] = [(8, 2), (8, 8), (64, 2), (64, 8)];

    mod spmc {
        use super::*;

        #[divan::bench(args = CONFIGS)]
        fn burst_x_consumers(bencher: divan::Bencher, (burst, num_consumers): (usize, usize)) {
            let consumer_work_iters = calibrate_spin_iters(50_000);
            let idle_iters = calibrate_spin_iters(350_000);

            bencher
                .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
                .bench(|| {
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

                    let mut pushed = 0u64;
                    while pushed < TOTAL_ITEMS {
                        let burst_n = burst.min((TOTAL_ITEMS - pushed) as usize);
                        for _ in 0..burst_n {
                            while producer.push(black_box(pushed)).is_err() {
                                std::hint::spin_loop();
                            }
                            pushed += 1;
                        }
                        if pushed < TOTAL_ITEMS {
                            busy_work(idle_iters);
                        }
                    }
                    drop(producer);
                    for h in handles {
                        h.join().unwrap();
                    }
                });
        }
    }

    mod nx_spsc {
        use super::*;

        #[divan::bench(args = CONFIGS)]
        fn burst_x_consumers(bencher: divan::Bencher, (burst, num_consumers): (usize, usize)) {
            let consumer_work_iters = calibrate_spin_iters(50_000);
            let idle_iters = calibrate_spin_iters(350_000);

            bencher
                .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
                .bench(|| {
                    let per_ring_cap = (1024usize / num_consumers).next_power_of_two();
                    let mut producers = Vec::with_capacity(num_consumers);
                    let mut consumers = Vec::with_capacity(num_consumers);
                    for _ in 0..num_consumers {
                        let (p, c) = quetzalcoatl::spsc::RingBuffer::<u64>::new(Capacity::exact(
                            per_ring_cap,
                        ))
                        .split();
                        producers.push(p);
                        consumers.push(c);
                    }

                    let handles: Vec<_> = consumers
                        .into_iter()
                        .map(|mut c| {
                            let per_consumer_items = TOTAL_ITEMS / num_consumers as u64;
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

                    let mut pushed = 0u64;
                    while pushed < TOTAL_ITEMS {
                        let burst_n = burst.min((TOTAL_ITEMS - pushed) as usize);
                        for _ in 0..burst_n {
                            let target = (pushed as usize) % num_consumers;
                            while producers[target].push(black_box(pushed)).is_err() {
                                std::hint::spin_loop();
                            }
                            pushed += 1;
                        }
                        if pushed < TOTAL_ITEMS {
                            busy_work(idle_iters);
                        }
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                });
        }
    }
}

// ---------------------------------------------------------------------------
// Blocking API: push_block / pop_block under contention.
//
// 1 producer, 2 consumers, small ring (16), each consumer doing slow work.
// Compares spin-retry vs push_block / pop_block.
// ---------------------------------------------------------------------------

mod blocking_spmc {
    use super::*;

    const TOTAL_ITEMS: u64 = 10_000;
    const Q_COUNT: usize = 2;

    #[divan::bench]
    fn spin(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let consumers: Vec<_> = (0..Q_COUNT)
                    .map(|_| {
                        let c = consumer.clone();
                        let r = received.clone();
                        thread::spawn(move || {
                            while r.load(std::sync::atomic::Ordering::Relaxed)
                                < TOTAL_ITEMS as usize
                            {
                                if let Some(v) = c.pop() {
                                    black_box(slow_consume_work(v));
                                    r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                } else {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();
                drop(consumer);

                for i in 0..TOTAL_ITEMS {
                    while producer.push(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
                for h in consumers {
                    h.join().unwrap();
                }
            });
    }

    #[divan::bench]
    fn block(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let consumers: Vec<_> = (0..Q_COUNT)
                    .map(|_| {
                        let c = consumer.clone();
                        thread::spawn(move || {
                            let mut local = 0u64;
                            while let Some(v) = c.pop_block() {
                                black_box(slow_consume_work(v));
                                local += 1;
                            }
                            local
                        })
                    })
                    .collect();
                drop(consumer);

                for i in 0..TOTAL_ITEMS {
                    producer.push_block(i).expect("consumers dropped");
                }
                drop(producer);
                let mut sum = 0u64;
                for h in consumers {
                    sum += h.join().unwrap();
                }
                assert_eq!(sum, TOTAL_ITEMS);
            });
    }
}

// ---------------------------------------------------------------------------
// Large struct + zero-copy blocking: reserve_block / pop_ref_block under
// contention. 1 producer, 2 consumers, small ring (16), 2KB items.
// ---------------------------------------------------------------------------

mod large_struct_spmc_zero_copy_blocking {
    use super::*;

    const TOTAL_ITEMS: u64 = 5_000;
    const Q_COUNT: usize = 2;

    #[divan::bench]
    fn items_2kb(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (mut producer, consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(16)).split();
                let consumers: Vec<_> = (0..Q_COUNT)
                    .map(|_| {
                        let mut c = consumer.clone();
                        thread::spawn(move || {
                            let mut local = 0u64;
                            while let Some(reader) = c.pop_ref_block() {
                                black_box(slow_consume_work(reader.data[0] as u64));
                                local += 1;
                            }
                            local
                        })
                    })
                    .collect();
                drop(consumer);

                for i in 0..TOTAL_ITEMS {
                    let w = producer.reserve_block().expect("consumers dropped");
                    w.write(black_box(LargeStruct::new(i as u8))).commit();
                }
                drop(producer);
                let mut sum = 0u64;
                for h in consumers {
                    sum += h.join().unwrap();
                }
                assert_eq!(sum, TOTAL_ITEMS);
            });
    }
}

// ---------------------------------------------------------------------------
// Async API: push_async / pop_async, one producer + N consumers.
//
// Each side gets its own thread with a current_thread runtime + LocalSet.
// Run with: cargo bench --bench spmc --features async
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
mod async_spmc {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    const TOTAL_ITEMS: u64 = 10_000;
    const C_COUNT: usize = 2;

    fn run_async_spmc_inner(cap: usize, consumer_work: fn(u64) -> u64) {
        let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
        let received = Arc::new(AtomicU64::new(0));

        let consumer_threads: Vec<_> = (0..C_COUNT)
            .map(|_| {
                let c = consumer.clone();
                let received = received.clone();
                thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(async move {
                        while let Some(v) = c.pop_async().await {
                            black_box(consumer_work(v));
                            received.fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                })
            })
            .collect();
        drop(consumer);

        let producer_thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async move {
                for i in 0..TOTAL_ITEMS {
                    producer.push_async(i).await.expect("all consumers dropped");
                }
            }));
        });

        producer_thread.join().unwrap();
        for h in consumer_threads {
            h.join().unwrap();
        }
    }

    // Saturated: small ring, slow consumer. Same shape as spin/block above.
    #[divan::bench]
    fn async_saturated(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                run_async_spmc_inner(16, slow_consume_work);
            });
    }

    // Unsaturated: large ring, fast consumer.
    #[divan::bench]
    fn async_unsaturated(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                run_async_spmc_inner(4096, |v| v);
            });
    }
}
