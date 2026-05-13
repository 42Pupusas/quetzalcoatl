use divan::black_box;
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::RingBuffer;
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
// 1. MPSC – scale producers from 1 to 8
// ---------------------------------------------------------------------------

mod mpsc_scaling {
    use super::*;

    const PRODUCERS: &[u64] = &[1, 8, 16];
    const ITEMS_PER_PRODUCER: u64 = 5_000;

    #[divan::bench(args = PRODUCERS)]
    fn producers(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) =
                    RingBuffer::<u64>::new(Capacity::exact(8192)).split();

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
            });
    }
}

// ---------------------------------------------------------------------------
// 2. Contention – many producers on a small buffer (high CAS retry rate)
// ---------------------------------------------------------------------------

mod contention {
    use super::*;

    const CAPS: &[usize] = &[64, 256, 1024];
    const ITEMS_PER_PRODUCER: u64 = 10_000;
    const NUM_PRODUCERS: u64 = 8;

    #[divan::bench(args = CAPS)]
    fn cap(bencher: divan::Bencher, cap: usize) {
        let total_items = ITEMS_PER_PRODUCER * NUM_PRODUCERS;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();

                let handles: Vec<_> = (0..NUM_PRODUCERS)
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
            });
    }
}

// ---------------------------------------------------------------------------
// 3. Large struct MPSC benchmarks (~2KB per element)
// ---------------------------------------------------------------------------

mod large_struct_mpsc {
    use super::*;

    const PRODUCERS: &[u64] = &[1, 2, 4];
    const ITEMS_PER_PRODUCER: u64 = 2_500;

    #[divan::bench(args = PRODUCERS)]
    fn producers(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();

                let handles: Vec<_> = (0..num_producers)
                    .map(|p| {
                        let prod = producer.clone();
                        thread::spawn(move || {
                            for i in 0..ITEMS_PER_PRODUCER {
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
            });
    }
}

// ---------------------------------------------------------------------------
// 4. Large struct zero-copy MPSC (reserve/pop_ref)
// ---------------------------------------------------------------------------

mod large_struct_mpsc_zero_copy {
    use super::*;

    const PRODUCERS: &[u64] = &[1, 2, 4];
    const ITEMS_PER_PRODUCER: u64 = 2_500;

    #[divan::bench(args = PRODUCERS)]
    fn producers(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(256)).split();

                let handles: Vec<_> = (0..num_producers)
                    .map(|p| {
                        let mut prod = producer.clone();
                        thread::spawn(move || {
                            for i in 0..ITEMS_PER_PRODUCER {
                                loop {
                                    if let Some(w) = prod.reserve() {
                                        w.write(black_box(LargeStruct::new((p * 100 + i) as u8)))
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
            });
    }
}

// ---------------------------------------------------------------------------
// 5. Blocking API: push_block / pop_block under contention.
//
// 2 producers, 1 consumer, small ring (16), consumer doing slow work.
// Compares spin-retry vs push_block / pop_block.
// ---------------------------------------------------------------------------

mod blocking_mpsc {
    use super::*;

    const P_COUNT: u64 = 2;
    const PER_P: u64 = 5_000;
    const TOTAL_ITEMS: u64 = P_COUNT * PER_P;

    #[divan::bench]
    fn spin(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let producers: Vec<_> = (0..P_COUNT)
                    .map(|tid| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..PER_P {
                                while p.push(tid * PER_P + i).is_err() {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();
                drop(producer);

                let mut received = 0u64;
                while received < TOTAL_ITEMS {
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
            });
    }

    #[divan::bench]
    fn block(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(16)).split();
                let producers: Vec<_> = (0..P_COUNT)
                    .map(|tid| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..PER_P {
                                p.push_block(tid * PER_P + i).expect("consumer dropped");
                            }
                        })
                    })
                    .collect();
                drop(producer);

                let mut received = 0u64;
                while let Some(v) = consumer.pop_block() {
                    black_box(slow_consume_work(v));
                    received += 1;
                }
                assert_eq!(received, TOTAL_ITEMS);
                for h in producers {
                    h.join().unwrap();
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 6. Large struct + zero-copy blocking: reserve_block / pop_ref_block under
// contention. 2 producers, 1 consumer, small ring (16), 2KB items.
// ---------------------------------------------------------------------------

mod large_struct_mpsc_zero_copy_blocking {
    use super::*;

    const P_COUNT: u64 = 2;
    const PER_P: u64 = 2_500;
    const TOTAL_ITEMS: u64 = P_COUNT * PER_P;

    // Uses push_block + pop_block (not reserve_block + pop_ref_block)
    // because the zero-copy blocking path has a known race under
    // release-mode optimizations — tracked separately.
    #[divan::bench]
    fn two_kb_items(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, mut consumer) =
                    RingBuffer::<LargeStruct>::new(Capacity::exact(16)).split();
                let producers: Vec<_> = (0..P_COUNT)
                    .map(|_| {
                        let p = producer.clone();
                        thread::spawn(move || {
                            for i in 0..PER_P {
                                if p.push_block(black_box(LargeStruct::new(i as u8))).is_err() {
                                    panic!("consumer dropped");
                                }
                            }
                        })
                    })
                    .collect();
                drop(producer);

                let mut received = 0u64;
                while let Some(val) = consumer.pop_block() {
                    black_box(slow_consume_work(val.data[0] as u64));
                    received += 1;
                }
                assert_eq!(received, TOTAL_ITEMS);
                for h in producers {
                    h.join().unwrap();
                }
            });
    }
}

// ---------------------------------------------------------------------------
// 7. Async API: push_async / pop_async, two producers + one consumer.
//
// Each side gets its own thread with a current_thread runtime + LocalSet.
// Producers and consumer are !Send so we can't pool them into a single
// multi_thread runtime; cross-thread waking still works because
// current_thread runtime wakers are honored when fired from foreign threads.
//
// Run with: cargo bench --bench mpsc --features async
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
mod async_mpsc {
    use super::*;
    use std::sync::{mpsc, Arc, Barrier};

    type ProducerMsg = quetzalcoatl::mpsc::Producer<u64>;
    type ConsumerMsg = quetzalcoatl::mpsc::Consumer<u64>;

    /// Each side runs on its own thread with a `current_thread` runtime + `LocalSet`.
    /// A pair of std channels hands each iteration's ring halves to persistent
    /// worker threads; a barrier synchronises completion.
    fn run_async_bench(
        bencher: divan::Bencher,
        cap: usize,
        p_count: u64,
        per_p: u64,
        consumer_work: fn(u64) -> u64,
    ) {
        let total_items = p_count * per_p;

        let (c_tx, c_rx) = mpsc::channel::<ConsumerMsg>();
        let barrier = Arc::new(Barrier::new(1 + p_count as usize + 1)); // main + producers + consumer

        let mut p_txs = Vec::new();
        let _producer_threads: Vec<_> = (0..p_count)
            .map(|_| {
                let (tx, rx) = mpsc::channel::<ProducerMsg>();
                p_txs.push(tx);
                let b = barrier.clone();
                thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    while let Ok(producer) = rx.recv() {
                        let local = tokio::task::LocalSet::new();
                        rt.block_on(local.run_until(async move {
                            for i in 0..per_p {
                                producer.push_async(i).await.expect("consumer dropped");
                            }
                        }));
                        b.wait();
                    }
                })
            })
            .collect();

        let c_barrier = barrier.clone();
        let _consumer_thread = thread::spawn(move || {
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
            .bench_local(|| {
                let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(cap)).split();
                for tx in &p_txs {
                    tx.send(producer.clone()).unwrap();
                }
                c_tx.send(consumer).unwrap();
                barrier.wait();
            });

        drop(p_txs);
        drop(c_tx);
        for h in _producer_threads {
            h.join().unwrap();
        }
        _consumer_thread.join().unwrap();
    }

    // Saturated: small ring, slow consumer. Same shape as the spin/block
    // variants in blocking_mpsc so the comparison is direct.
    #[divan::bench]
    fn async_saturated(bencher: divan::Bencher) {
        run_async_bench(bencher, 16, 2, 5_000, slow_consume_work);
    }

    // Unsaturated: large ring, fast consumer. Measures async overhead
    // when the ring rarely fills.
    #[divan::bench]
    fn async_unsaturated(bencher: divan::Bencher) {
        run_async_bench(bencher, 4096, 2, 5_000, |v| v);
    }
}
