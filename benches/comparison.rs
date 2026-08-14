use divan::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

fn main() {
    divan::main();
}

// =============================================================================
// SPSC: quetzalcoatl vs crossbeam (1 producer, 1 consumer)
// =============================================================================

mod cmp_spsc {
    use super::*;

    #[divan::bench(args = [100_000u64, 1_000_000])]
    fn quetzalcoatl(bencher: divan::Bencher, total_items: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::spsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::exact(4096),
                )
                .split();

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
            });
    }

    #[divan::bench(args = [100_000u64, 1_000_000])]
    fn crossbeam(bencher: divan::Bencher, total_items: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(4096);

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
            });
    }
}

// =============================================================================
// MPSC: quetzalcoatl vs crossbeam vs tokio (N producers, 1 consumer)
// =============================================================================

mod cmp_mpsc {
    use super::*;

    const ITEMS_PER_PRODUCER: u64 = 5_000;

    #[divan::bench(args = [1u64, 2, 4, 8, 12, 16])]
    fn quetzalcoatl(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::exact(8192),
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

    #[divan::bench(args = [1u64, 2, 4, 8, 12, 16])]
    fn crossbeam(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(8192);

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

    #[divan::bench(args = [1u64, 2, 4, 8, 12, 16])]
    fn tokio(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_producers as usize + 1)
            .build()
            .unwrap();

        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                rt.block_on(async {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(8192);

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
}

// =============================================================================
// SPMC: quetzalcoatl vs crossbeam (1 producer, N consumers)
// =============================================================================

#[path = "comparison/spmc_split.rs"]
mod cmp_spmc_split;

#[path = "comparison/mpsc_split.rs"]
mod cmp_mpsc_split;

mod cmp_spmc {
    use super::*;

    const TOTAL_ITEMS: u64 = 20_000;

    #[divan::bench(args = [1u64, 2, 4, 8])]
    fn quetzalcoatl(bencher: divan::Bencher, num_consumers: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, consumer) = quetzalcoatl::spmc::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::exact(8192),
                )
                .split();

                let done = Arc::new(AtomicBool::new(false));

                let consumer_handles: Vec<_> = (0..num_consumers)
                    .map(|_| {
                        let c = consumer.clone();
                        let d = Arc::clone(&done);
                        thread::spawn(move || {
                            loop {
                                if c.pop().is_some() {
                                    // consumed
                                } else if d.load(Ordering::Relaxed) {
                                    while c.pop().is_some() {}
                                    break;
                                } else {
                                    std::hint::spin_loop();
                                }
                            }
                        })
                    })
                    .collect();

                for i in 0..TOTAL_ITEMS {
                    while producer.push(black_box(i)).is_err() {
                        std::hint::spin_loop();
                    }
                }

                done.store(true, Ordering::Relaxed);

                for h in consumer_handles {
                    h.join().unwrap();
                }
            });
    }

    #[divan::bench(args = [1u64, 2, 4, 8])]
    fn crossbeam(bencher: divan::Bencher, num_consumers: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(8192);

                let done = Arc::new(AtomicBool::new(false));

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

                for i in 0..TOTAL_ITEMS {
                    tx.send(black_box(i)).unwrap();
                }

                done.store(true, Ordering::Relaxed);

                for h in consumer_handles {
                    h.join().unwrap();
                }
            });
    }
}

// =============================================================================
// Broadcast: quetzalcoatl vs tokio::sync::broadcast
// (every consumer sees every message)
// =============================================================================

mod cmp_broadcast {
    use super::*;

    const TOTAL_ITEMS: u64 = 10_000;

    #[divan::bench(args = [1u64, 2, 4, 8])]
    fn quetzalcoatl(bencher: divan::Bencher, num_consumers: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, consumer) = quetzalcoatl::broadcast::RingBuffer::<u64>::new(
                    quetzalcoatl::capacity::Capacity::exact(4096),
                    num_consumers as usize + 1,
                )
                .split();
                let mut consumers: Vec<_> =
                    (0..num_consumers - 1).map(|_| consumer.clone()).collect();
                consumers.push(consumer);

                let ph = thread::spawn(move || {
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

                ph.join().unwrap();
                for h in handles {
                    h.join().unwrap();
                }
            });
    }

    #[divan::bench(args = [1u64, 2, 4, 8])]
    fn tokio(bencher: divan::Bencher, num_consumers: u64) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_consumers as usize + 1)
            .build()
            .unwrap();

        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                rt.block_on(async {
                    let (tx, _) = tokio::sync::broadcast::channel::<u64>(4096);

                    let mut receivers: Vec<_> =
                        (0..num_consumers).map(|_| tx.subscribe()).collect();

                    let ph = tokio::spawn(async move {
                        for i in 0..TOTAL_ITEMS {
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
                                while received < TOTAL_ITEMS {
                                    match rx.recv().await {
                                        Ok(_) => received += 1,
                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(
                                            n,
                                        )) => received += n,
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
                });
            });
    }
}

// =============================================================================
// Large struct (~2KB) -- the zero-copy differentiator
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

mod cmp_large_spsc {
    use super::*;

    const TOTAL_ITEMS: u64 = 5_000;

    #[divan::bench]
    fn quetzalcoatl_copy(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::spsc::RingBuffer::<LargeStruct>::new(
                    quetzalcoatl::capacity::Capacity::exact(256),
                )
                .split();

                let ph = thread::spawn(move || {
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

                ph.join().unwrap();
            });
    }

    #[divan::bench]
    fn quetzalcoatl_zerocopy(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (mut producer, mut consumer) =
                    quetzalcoatl::spsc::RingBuffer::<LargeStruct>::new(
                        quetzalcoatl::capacity::Capacity::exact(256),
                    )
                    .split();

                let ph = thread::spawn(move || {
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

                ph.join().unwrap();
            });
    }

    #[divan::bench]
    fn crossbeam(bencher: divan::Bencher) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (tx, rx) = crossbeam_channel::bounded::<LargeStruct>(256);

                let ph = thread::spawn(move || {
                    for i in 0..TOTAL_ITEMS {
                        tx.send(black_box(LargeStruct::new(i as u8))).unwrap();
                    }
                });

                let mut received = 0u64;
                while received < TOTAL_ITEMS {
                    if rx.recv().is_ok() {
                        received += 1;
                    }
                }

                ph.join().unwrap();
            });
    }
}

// ---------------------------------------------------------------------------
// Large struct MPSC: quetzalcoatl (copy + zerocopy) vs crossbeam vs tokio
// ---------------------------------------------------------------------------

mod cmp_large_mpsc {
    use super::*;

    const ITEMS_PER_PRODUCER: u64 = 1_000;

    #[divan::bench(args = [2u64, 4])]
    fn quetzalcoatl_copy(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<LargeStruct>::new(
                    quetzalcoatl::capacity::Capacity::exact(256),
                )
                .split();

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

    #[divan::bench(args = [2u64, 4])]
    fn quetzalcoatl_zerocopy(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (producer, mut consumer) = quetzalcoatl::mpsc::RingBuffer::<LargeStruct>::new(
                    quetzalcoatl::capacity::Capacity::exact(256),
                )
                .split();

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

    #[divan::bench(args = [2u64, 4])]
    fn crossbeam(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                let (tx, rx) = crossbeam_channel::bounded::<LargeStruct>(256);

                let handles: Vec<_> = (0..num_producers)
                    .map(|p| {
                        let t = tx.clone();
                        thread::spawn(move || {
                            for i in 0..ITEMS_PER_PRODUCER {
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
            });
    }

    #[divan::bench(args = [2u64, 4])]
    fn tokio(bencher: divan::Bencher, num_producers: u64) {
        let total_items = ITEMS_PER_PRODUCER * num_producers;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_producers as usize + 1)
            .build()
            .unwrap();

        bencher
            .counter(divan::counter::ItemsCount::new(total_items))
            .bench_local(|| {
                rt.block_on(async {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<LargeStruct>(256);

                    let handles: Vec<_> = (0..num_producers)
                        .map(|p| {
                            let t = tx.clone();
                            tokio::spawn(async move {
                                for i in 0..ITEMS_PER_PRODUCER {
                                    t.send(black_box(LargeStruct::new((p * 100 + i) as u8)))
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
                });
            });
    }
}

// ---------------------------------------------------------------------------
// Large struct SPMC: quetzalcoatl (copy) vs crossbeam
// ---------------------------------------------------------------------------

mod cmp_large_spmc {
    use super::*;

    const TOTAL_ITEMS: u64 = 2_000;

    #[divan::bench(args = [2u64, 4])]
    fn quetzalcoatl_copy(bencher: divan::Bencher, num_consumers: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (producer, consumer) = quetzalcoatl::spmc::RingBuffer::<LargeStruct>::new(
                    quetzalcoatl::capacity::Capacity::exact(256),
                )
                .split();

                let remaining = Arc::new(AtomicU64::new(TOTAL_ITEMS));

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

                for i in 0..TOTAL_ITEMS {
                    while producer.push(black_box(LargeStruct::new(i as u8))).is_err() {
                        std::hint::spin_loop();
                    }
                }

                for h in consumer_handles {
                    h.join().unwrap();
                }
            });
    }

    #[divan::bench(args = [2u64, 4])]
    fn crossbeam(bencher: divan::Bencher, num_consumers: u64) {
        bencher
            .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
            .bench_local(|| {
                let (tx, rx) = crossbeam_channel::bounded::<LargeStruct>(256);

                let remaining = Arc::new(AtomicU64::new(TOTAL_ITEMS));

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

                for i in 0..TOTAL_ITEMS {
                    tx.send(black_box(LargeStruct::new(i as u8))).unwrap();
                }

                for h in consumer_handles {
                    h.join().unwrap();
                }
            });
    }
}
