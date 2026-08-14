use super::*;
use std::sync::Barrier;

const CAPACITY: usize = 8192;
const TOTAL_ITEMS: u64 = 20_000;

struct QuetzalcoatlSteady {
    producer: quetzalcoatl::spmc::Producer<u64>,
    producer_done: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl QuetzalcoatlSteady {
    fn new(num_consumers: u64) -> Self {
        let (producer, consumer) = quetzalcoatl::spmc::RingBuffer::<u64>::new(
            quetzalcoatl::capacity::Capacity::exact(CAPACITY),
        )
        .split();
        let producer_done = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(num_consumers as usize + 1));
        let finish = Arc::new(Barrier::new(num_consumers as usize + 1));
        let workers = (0..num_consumers)
            .map(|_| {
                let consumer = consumer.clone();
                let producer_done = Arc::clone(&producer_done);
                let stop = Arc::clone(&stop);
                let start = Arc::clone(&start);
                let finish = Arc::clone(&finish);
                thread::spawn(move || loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    loop {
                        if consumer.pop().is_some() {
                            continue;
                        }
                        if producer_done.load(Ordering::Acquire) {
                            while consumer.pop().is_some() {}
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    finish.wait();
                })
            })
            .collect();

        Self {
            producer,
            producer_done,
            stop,
            start,
            finish,
            workers,
        }
    }

    fn transfer(&self) {
        self.producer_done.store(false, Ordering::Relaxed);
        self.start.wait();
        for item in 0..TOTAL_ITEMS {
            while self.producer.push(black_box(item)).is_err() {
                std::hint::spin_loop();
            }
        }
        self.producer_done.store(true, Ordering::Release);
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
    sender: crossbeam_channel::Sender<u64>,
    producer_done: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl CrossbeamSteady {
    fn new(num_consumers: u64) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded::<u64>(CAPACITY);
        let producer_done = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(num_consumers as usize + 1));
        let finish = Arc::new(Barrier::new(num_consumers as usize + 1));
        let workers = (0..num_consumers)
            .map(|_| {
                let receiver = receiver.clone();
                let producer_done = Arc::clone(&producer_done);
                let stop = Arc::clone(&stop);
                let start = Arc::clone(&start);
                let finish = Arc::clone(&finish);
                thread::spawn(move || loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    loop {
                        if receiver.try_recv().is_ok() {
                            continue;
                        }
                        if producer_done.load(Ordering::Acquire) {
                            while receiver.try_recv().is_ok() {}
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    finish.wait();
                })
            })
            .collect();

        Self {
            sender,
            producer_done,
            stop,
            start,
            finish,
            workers,
        }
    }

    fn transfer(&self) {
        self.producer_done.store(false, Ordering::Relaxed);
        self.start.wait();
        for item in 0..TOTAL_ITEMS {
            self.sender.send(black_box(item)).unwrap();
        }
        self.producer_done.store(true, Ordering::Release);
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
            quetzalcoatl::spmc::RingBuffer::<u64>::new(quetzalcoatl::capacity::Capacity::exact(
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
fn quetzalcoatl_steady(bencher: divan::Bencher, num_consumers: u64) {
    let harness = QuetzalcoatlSteady::new(num_consumers);
    bencher
        .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
        .bench_local(|| harness.transfer());
}

#[divan::bench(args = [1u64, 2, 4, 8])]
fn crossbeam_steady(bencher: divan::Bencher, num_consumers: u64) {
    let harness = CrossbeamSteady::new(num_consumers);
    bencher
        .counter(divan::counter::ItemsCount::new(TOTAL_ITEMS))
        .bench_local(|| harness.transfer());
}
