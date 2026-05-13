use divan::black_box;
use quetzalcoatl::broadcast::arc::ArcRingBuffer;
use quetzalcoatl::broadcast::RingBuffer;
use quetzalcoatl::capacity::Capacity;
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

// ---------------------------------------------------------------------------
// 1. Consumer scaling: 1 producer, N consumers
// ---------------------------------------------------------------------------

mod consumer_scaling {
    use super::*;

    const NUM_CONSUMERS: &[usize] = &[1, 2, 4, 8];
    const TOTAL_ITEMS: u64 = 10_000;

    #[divan::bench(args = NUM_CONSUMERS)]
    fn consumer_scaling(bencher: divan::Bencher, num_consumers: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) =
                    RingBuffer::<u64>::new(Capacity::exact(4096), num_consumers + 1).split();
                let mut consumers: Vec<_> =
                    (0..num_consumers - 1).map(|_| consumer.clone()).collect();
                consumers.push(consumer);

                let producer_handle = thread::spawn(move || {
                    for i in 0..TOTAL_ITEMS {
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
                            while received < TOTAL_ITEMS {
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
            });
    }
}

// ---------------------------------------------------------------------------
// 2. Producer scaling: N producers, 2 consumers
// ---------------------------------------------------------------------------

mod producer_scaling {
    use super::*;

    const NUM_PRODUCERS: &[usize] = &[1, 2, 4, 8];
    const ITEMS_PER_PRODUCER: u64 = 5_000;

    #[divan::bench(args = NUM_PRODUCERS)]
    fn producer_scaling(bencher: divan::Bencher, num_producers: usize) {
        let total_items = ITEMS_PER_PRODUCER * num_producers as u64;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4096), 4).split();
                let mut c2 = consumer.clone();
                let mut c1 = consumer;

                let producer_handles: Vec<_> = (0..num_producers)
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
            });
    }
}

// ---------------------------------------------------------------------------
// 3. Large struct: pop (clone) vs pop_ref (zero-copy)
// ---------------------------------------------------------------------------

mod large_struct {
    use super::*;

    const TOTAL_ITEMS: u64 = 10_000;

    #[divan::bench]
    fn clone_2kb(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256), 4).split();

                let producer_handle = thread::spawn(move || {
                    for i in 0..TOTAL_ITEMS {
                        while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                });

                let mut received = 0u64;
                while received < TOTAL_ITEMS {
                    if consumer.pop().is_some() {
                        received += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }

                producer_handle.join().unwrap();
            });
    }

    #[divan::bench]
    fn zero_copy_2kb(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (mut producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256), 4).split();

                let producer_handle = thread::spawn(move || {
                    for i in 0..TOTAL_ITEMS {
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
                while received < TOTAL_ITEMS {
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
// 4. Arc wrapper: large struct with multiple consumers
// ---------------------------------------------------------------------------

mod arc_large_struct {
    use super::*;

    const TOTAL_ITEMS: u64 = 10_000;
    const NUM_CONSUMERS: usize = 4;

    #[divan::bench]
    fn clone_2kb_4consumers(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256), NUM_CONSUMERS + 1).split();
                let mut consumers: Vec<_> =
                    (0..NUM_CONSUMERS - 1).map(|_| consumer.clone()).collect();
                consumers.push(consumer);

                let producer_handle = thread::spawn(move || {
                    for i in 0..TOTAL_ITEMS {
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
                            while received < TOTAL_ITEMS {
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
            });
    }

    #[divan::bench]
    fn arc_2kb_4consumers(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, consumer) =
                    ArcRingBuffer::<LargeStruct>::new(Capacity::exact(256), NUM_CONSUMERS + 1)
                        .split();
                let mut consumers: Vec<_> =
                    (0..NUM_CONSUMERS - 1).map(|_| consumer.clone()).collect();
                consumers.push(consumer);

                let producer_handle = thread::spawn(move || {
                    for i in 0..TOTAL_ITEMS {
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
                            while received < TOTAL_ITEMS {
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
            });
    }
}

// ---------------------------------------------------------------------------
// Async API: push_async / pop_async, 1 producer + N consumers (broadcast).
//
// Run with: cargo bench --bench broadcast --features async
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
mod async_broadcast {
    use super::*;
    use std::sync::{mpsc, Arc, Barrier};

    type ProducerMsg = quetzalcoatl::broadcast::Producer<u64>;
    type ConsumerMsg = quetzalcoatl::broadcast::Consumer<u64>;

    const N_CONSUMERS: usize = 2;
    const TOTAL_ITEMS: u64 = 1_000;

    /// Each side runs on its own thread with a `current_thread` runtime + `LocalSet`.
    /// A pair of std channels hands each iteration's ring halves to persistent
    /// worker threads; a barrier synchronises completion.
    fn run_async_bench(bencher: divan::Bencher, cap: usize) {
        // Producer channel + thread
        let (p_tx, p_rx) = mpsc::channel::<ProducerMsg>();
        // Per-consumer channels + threads
        let mut per_consumer_txs = Vec::with_capacity(N_CONSUMERS);
        let barrier = Arc::new(Barrier::new(1 + 1 + N_CONSUMERS)); // main + producer + consumers

        let p_barrier = barrier.clone();
        let producer_thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            while let Ok(producer) = p_rx.recv() {
                let local = tokio::task::LocalSet::new();
                rt.block_on(local.run_until(async move {
                    for i in 0..TOTAL_ITEMS {
                        producer.push_async(i).await.expect("all consumers dropped");
                    }
                }));
                p_barrier.wait();
            }
        });

        let consumer_threads: Vec<_> = (0..N_CONSUMERS)
            .map(|_| {
                let (tx, rx) = mpsc::channel::<ConsumerMsg>();
                per_consumer_txs.push(tx);
                let b = barrier.clone();
                thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    while let Ok(mut consumer) = rx.recv() {
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            while let Some(v) = consumer.pop_async().await {
                                black_box(v);
                            }
                        }));
                        b.wait();
                    }
                })
            })
            .collect();

        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench(|| {
                let (producer, c1) =
                    RingBuffer::<u64>::new(Capacity::exact(cap), N_CONSUMERS + 1).split();
                let mut consumers: Vec<_> = (0..N_CONSUMERS - 1).map(|_| c1.clone()).collect();
                consumers.push(c1);

                for (tx, consumer) in per_consumer_txs.iter().zip(consumers) {
                    tx.send(consumer).unwrap();
                }
                p_tx.send(producer).unwrap();
                barrier.wait();
            });

        drop(p_tx);
        drop(per_consumer_txs);
        producer_thread.join().unwrap();
        for h in consumer_threads {
            h.join().unwrap();
        }
    }

    #[divan::bench]
    fn saturated(bencher: divan::Bencher) {
        run_async_bench(bencher, 16);
    }

    #[divan::bench]
    fn unsaturated(bencher: divan::Bencher) {
        run_async_bench(bencher, 4096);
    }
}
