use super::*;
use std::sync::Barrier;

const CAPACITY: usize = 8192;
const ITEMS_PER_PRODUCER: u64 = 5_000;

struct QuetzalcoatlSteady {
    consumer: quetzalcoatl::mpsc::Consumer<u64>,
    total_items: u64,
    stop: Arc<AtomicBool>,
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl QuetzalcoatlSteady {
    fn new(num_producers: u64) -> Self {
        let (producer, consumer) = quetzalcoatl::mpsc::RingBuffer::<u64>::new(
            quetzalcoatl::capacity::Capacity::exact(CAPACITY),
        )
        .split();
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(num_producers as usize + 1));
        let finish = Arc::new(Barrier::new(num_producers as usize + 1));
        let workers = (0..num_producers)
            .map(|_| {
                let producer = producer.clone();
                let stop = Arc::clone(&stop);
                let start = Arc::clone(&start);
                let finish = Arc::clone(&finish);
                thread::spawn(move || loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for item in 0..ITEMS_PER_PRODUCER {
                        producer.push_block(black_box(item)).unwrap();
                    }
                    finish.wait();
                })
            })
            .collect();

        Self {
            consumer,
            total_items: ITEMS_PER_PRODUCER * num_producers,
            stop,
            start,
            finish,
            workers,
        }
    }

    fn transfer(&mut self) {
        self.start.wait();
        let mut received = 0u64;
        while received < self.total_items {
            if self.consumer.pop_block().is_some() {
                received += 1;
            }
        }
        self.finish.wait();
    }
}

impl Drop for QuetzalcoatlSteady {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

struct CrossbeamSteady {
    receiver: crossbeam_channel::Receiver<u64>,
    total_items: u64,
    stop: Arc<AtomicBool>,
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl CrossbeamSteady {
    fn new(num_producers: u64) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded::<u64>(CAPACITY);
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(num_producers as usize + 1));
        let finish = Arc::new(Barrier::new(num_producers as usize + 1));
        let workers = (0..num_producers)
            .map(|_| {
                let sender = sender.clone();
                let stop = Arc::clone(&stop);
                let start = Arc::clone(&start);
                let finish = Arc::clone(&finish);
                thread::spawn(move || loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for item in 0..ITEMS_PER_PRODUCER {
                        sender.send(black_box(item)).unwrap();
                    }
                    finish.wait();
                })
            })
            .collect();

        Self {
            receiver,
            total_items: ITEMS_PER_PRODUCER * num_producers,
            stop,
            start,
            finish,
            workers,
        }
    }

    fn transfer(&mut self) {
        self.start.wait();
        let mut received = 0u64;
        while received < self.total_items {
            if self.receiver.recv().is_ok() {
                received += 1;
            }
        }
        self.finish.wait();
    }
}

impl Drop for CrossbeamSteady {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}

#[divan::bench]
fn quetzalcoatl_setup(bencher: divan::Bencher) {
    bencher.bench_local(|| {
        black_box(
            quetzalcoatl::mpsc::RingBuffer::<u64>::new(quetzalcoatl::capacity::Capacity::exact(
                CAPACITY,
            ))
            .split(),
        )
    });
}

#[divan::bench]
fn crossbeam_setup(bencher: divan::Bencher) {
    bencher.bench_local(|| black_box(crossbeam_channel::bounded::<u64>(CAPACITY)));
}

#[divan::bench(args = [1u64, 2, 4, 8])]
fn quetzalcoatl_steady(bencher: divan::Bencher, num_producers: u64) {
    let mut harness = QuetzalcoatlSteady::new(num_producers);
    bencher
        .counter(divan::counter::ItemsCount::new(
            ITEMS_PER_PRODUCER * num_producers,
        ))
        .bench_local(|| harness.transfer());
}

#[divan::bench(args = [1u64, 2, 4, 8])]
fn crossbeam_steady(bencher: divan::Bencher, num_producers: u64) {
    let mut harness = CrossbeamSteady::new(num_producers);
    bencher
        .counter(divan::counter::ItemsCount::new(
            ITEMS_PER_PRODUCER * num_producers,
        ))
        .bench_local(|| harness.transfer());
}
