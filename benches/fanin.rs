//! Fan-in benchmarks: N producers -> 1 consumer.
//!
//! Compares strategies for aggregating messages from many worker threads:
//!
//! 1. **MPSC ring buffer** -- single shared buffer, producers compete via FAA
//! 2. **N x SPSC fan-in** -- one SPSC ring per producer, consumer round-robin drains
//! 3. **tokio::sync::mpsc** -- async-native channels (bounded + unbounded)
//! 4. **crossbeam** -- bounded MPMC channel used as MPSC
//!
//! Buffer capacity is equalized: each SPSC ring gets `PER_RING_CAP` slots,
//! and the MPSC/crossbeam/tokio get `at_least(PER_RING_CAP * N)` so total
//! buffer memory is comparable across strategies.
//!
//! **FIFO note**: MPSC preserves a global FIFO order (FAA establishes a
//! total order on positions). N x SPSC fan-in only preserves per-producer
//! FIFO -- cross-producer ordering depends on the consumer's drain schedule.

use divan::black_box;
use std::thread;

fn main() {
    divan::main();
}

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
// 1. MPSC (FAA) vs N x SPSC fan-in vs tokio vs crossbeam
// =============================================================================

mod fanin {
    use super::*;

    #[divan::bench(args = [4u64, 16])]
    fn mpsc(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let mpsc_cap = total_cap(num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::at_least(mpsc_cap),
                )
                .split();

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

    #[divan::bench(args = [4u64, 16])]
    fn mpsc_drain(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let mpsc_cap = total_cap(num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::at_least(mpsc_cap),
                )
                .split();

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
            });
    }

    #[divan::bench(args = [4u64, 16])]
    fn spsc_fanin(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let mut consumers: Vec<quetzalcoatl::spsc::Consumer<u64>> = Vec::new();
                let mut producer_handles: Vec<thread::JoinHandle<()>> = Vec::new();

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
            });
    }

    #[divan::bench(args = [4u64, 16])]
    fn spsc_fanin_batch(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let mut consumers: Vec<quetzalcoatl::spsc::Consumer<u64>> = Vec::new();
                let mut producer_handles: Vec<thread::JoinHandle<()>> = Vec::new();

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
            });
    }

    #[divan::bench(args = [4u64, 16])]
    fn tokio_unbounded(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_producers as usize + 1)
            .build()
            .unwrap();

        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                rt.block_on(async {
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();

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
                });
            });
    }

    #[divan::bench(args = [4u64, 16])]
    fn tokio_bounded(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let mpsc_cap = total_cap(num_producers);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_producers as usize + 1)
            .build()
            .unwrap();

        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                rt.block_on(async {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(mpsc_cap);

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
                });
            });
    }

    #[divan::bench(args = [4u64, 16])]
    fn crossbeam(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let mpsc_cap = total_cap(num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(mpsc_cap);

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
            });
    }
}

// =============================================================================
// 2. Buffer capacity impact on MPSC at high producer counts
// =============================================================================

mod fanin_capacity {
    use super::*;

    const NUM_PRODUCERS: u64 = 12;
    const TOTAL_ITEMS: u64 = ITEMS_PER_PRODUCER * NUM_PRODUCERS;

    #[divan::bench(args = [256, 1024, 4096, 8192, 16384])]
    fn mpsc(bencher: divan::Bencher, cap: usize) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::exact(cap),
                )
                .split();

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
                while received < TOTAL_ITEMS {
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

// =============================================================================
// 3. Consumer batch drain: pop one-at-a-time vs drain (amortized head)
// =============================================================================

mod drain_strategy {
    use super::*;

    const NUM_PRODUCERS: u64 = 12;
    const TOTAL_ITEMS: u64 = ITEMS_PER_PRODUCER * NUM_PRODUCERS;

    #[divan::bench]
    fn mpsc_single_pop(bencher: divan::Bencher) {
        let cap = total_cap(NUM_PRODUCERS);
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::at_least(cap),
                )
                .split();

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
                while received < TOTAL_ITEMS {
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

    #[divan::bench]
    fn mpsc_batch_drain(bencher: divan::Bencher) {
        let cap = total_cap(NUM_PRODUCERS);
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::at_least(cap),
                )
                .split();

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
                while received < TOTAL_ITEMS {
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
            });
    }
}
