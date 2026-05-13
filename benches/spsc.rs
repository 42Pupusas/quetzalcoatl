use divan::black_box;
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::spsc::RingBuffer;
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
// 1. Single-thread push-only throughput
// ---------------------------------------------------------------------------

mod push_only {
    use super::*;

    const CAPS: &[usize] = &[1024, 4096, 65536];

    #[divan::bench(args = CAPS)]
    fn push_only(bencher: divan::Bencher, cap: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(cap))
            .bench(|| {
                let (producer, _consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
                for i in 0..cap as u64 {
                    let _ = producer.push(black_box(i));
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 2. Single-thread pop-only throughput
// ---------------------------------------------------------------------------

mod pop_only {
    use super::*;

    const CAPS: &[usize] = &[1024, 4096, 65536];

    #[divan::bench(args = CAPS)]
    fn pop_only(bencher: divan::Bencher, cap: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(cap))
            .bench(|| {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
                for i in 0..cap as u64 {
                    producer.push(i).unwrap();
                }
                while consumer.pop().is_some() {}
            });
    }
}

// ---------------------------------------------------------------------------
// 3. Single-thread push/pop ping-pong (alternating)
// ---------------------------------------------------------------------------

mod push_pop_alternating {
    use super::*;

    #[divan::bench]
    fn push_pop_alternating(bencher: divan::Bencher) {
        let ops = 10_000u64;
        bencher
            .counter(divan::counter::ItemsCount::new(ops))
            .bench(|| {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(64)).split();
                for i in 0..ops {
                    let _ = producer.push(black_box(i));
                    black_box(consumer.pop());
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 4. SPSC - producer and consumer on separate threads
// ---------------------------------------------------------------------------

mod spsc_concurrent {
    use super::*;

    const TOTAL_ITEMS: &[u64] = &[10_000, 100_000, 1_000_000];

    #[divan::bench(args = TOTAL_ITEMS)]
    fn spsc_concurrent(bencher: divan::Bencher, total_items: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (producer, mut consumer) =
                    RingBuffer::<u64>::new(Capacity::exact(4096)).split();

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
            });
    }
}

// ---------------------------------------------------------------------------
// 5. Buffer capacity scaling
// ---------------------------------------------------------------------------

mod capacity_impact {
    use super::*;

    const CAPS: &[usize] = &[1000, 1024, 4000, 4096];

    #[divan::bench(args = CAPS)]
    fn capacity_impact(bencher: divan::Bencher, cap: usize) {
        let ops = 50_000u64;
        bencher
            .counter(divan::counter::ItemsCount::new(ops))
            .bench(|| {
                let (producer, mut consumer) =
                    RingBuffer::<u64>::new(Capacity::at_least(cap)).split();
                for i in 0..ops {
                    let _ = producer.push(black_box(i));
                    if i % 2 == 0 {
                        black_box(consumer.pop());
                    }
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 6. Large struct benchmarks (~2KB per element)
// ---------------------------------------------------------------------------

mod large_struct_spsc {
    use super::*;

    #[divan::bench]
    fn large_struct_spsc(bencher: divan::Bencher) {
        let total_items = 10_000u64;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();

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
            });
    }
}

// ---------------------------------------------------------------------------
// 7. Large struct zero-copy SPSC (reserve/pop_ref)
// ---------------------------------------------------------------------------

mod large_struct_spsc_zero_copy {
    use super::*;

    #[divan::bench]
    fn large_struct_spsc_zero_copy(bencher: divan::Bencher) {
        let total_items = 10_000u64;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (mut producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();

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
            });
    }
}

// ---------------------------------------------------------------------------
// 8. Blocking API: spin vs block under contention
// ---------------------------------------------------------------------------

mod blocking_spsc {
    use super::*;

    #[divan::bench]
    fn spin(bencher: divan::Bencher) {
        let total_items = 10_000u64;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
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
            });
    }

    #[divan::bench]
    fn block(bencher: divan::Bencher) {
        let total_items = 10_000u64;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
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
            });
    }
}

// ---------------------------------------------------------------------------
// 9. Large struct + zero-copy blocking: reserve_block / pop_ref_block
// ---------------------------------------------------------------------------

mod large_struct_spsc_zero_copy_blocking {
    use super::*;

    #[divan::bench]
    fn large_struct_spsc_zero_copy_blocking(bencher: divan::Bencher) {
        let total_items = 5_000u64;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (mut producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(16)).split();
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
            });
    }
}

// ---------------------------------------------------------------------------
// 10. Async API: push_async / pop_async
//
// Run with: cargo bench --bench spsc --features async
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
mod async_spsc {
    use super::*;
    use std::sync::{mpsc, Arc, Barrier};

    type ProducerMsg = quetzalcoatl::spsc::Producer<u64>;
    type ConsumerMsg = quetzalcoatl::spsc::Consumer<u64>;

    /// Each side runs on its own thread with a `current_thread` runtime + `LocalSet`.
    /// A pair of std channels hands each iteration's ring halves to persistent
    /// worker threads; a barrier synchronises completion.
    fn run_async_bench(
        bencher: divan::Bencher,
        cap: usize,
        total_items: u64,
        consumer_work: fn(u64) -> u64,
    ) {
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

        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
                p_tx.send(producer).unwrap();
                c_tx.send(consumer).unwrap();
                barrier.wait(); // wait for both sides to finish
            });

        drop(p_tx);
        drop(c_tx);
        producer_thread.join().unwrap();
        consumer_thread.join().unwrap();
    }

    #[divan::bench]
    fn async_saturated(bencher: divan::Bencher) {
        run_async_bench(bencher, 16, 10_000, slow_consume_work);
    }

    #[divan::bench]
    fn async_unsaturated(bencher: divan::Bencher) {
        run_async_bench(bencher, 4096, 10_000, |v| v);
    }
}
