use divan::black_box;
use quetzalcoatl::capacity::Capacity;
use std::thread;

pub struct MpscWorkload {
    capacity: usize,
    items_per_producer: u64,
    num_producers: u64,
}

impl MpscWorkload {
    pub const fn new(capacity: usize, items_per_producer: u64, num_producers: u64) -> Self {
        Self {
            capacity,
            items_per_producer,
            num_producers,
        }
    }

    pub const fn total_items(&self) -> u64 {
        self.items_per_producer * self.num_producers
    }

    pub fn quetzalcoatl_spin(&self) {
        let per_producer = self.items_per_producer;
        let total = self.total_items();
        let (producer, mut consumer) =
            quetzalcoatl::mpsc::RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();

        let handles: Vec<_> = (0..self.num_producers)
            .map(|_| {
                let p = producer.clone();
                thread::spawn(move || {
                    for i in 0..per_producer {
                        while p.push(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        drop(producer);

        let mut received = 0u64;
        while received < total {
            if consumer.pop().is_some() {
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    pub fn quetzalcoatl_block(&self) {
        let per_producer = self.items_per_producer;
        let total = self.total_items();
        let (producer, mut consumer) =
            quetzalcoatl::mpsc::RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();

        let handles: Vec<_> = (0..self.num_producers)
            .map(|_| {
                let p = producer.clone();
                thread::spawn(move || {
                    for i in 0..per_producer {
                        p.push_block(black_box(i)).expect("consumer dropped");
                    }
                })
            })
            .collect();
        drop(producer);

        let mut received = 0u64;
        while received < total {
            match consumer.pop_block() {
                Some(_) => received += 1,
                None => break,
            }
        }
        assert_eq!(received, total);

        for h in handles {
            h.join().unwrap();
        }
    }

    pub fn crossbeam_spin(&self) {
        let per_producer = self.items_per_producer;
        let total = self.total_items();
        let (tx, rx) = crossbeam_channel::bounded::<u64>(self.capacity);

        let handles: Vec<_> = (0..self.num_producers)
            .map(|_| {
                let t = tx.clone();
                thread::spawn(move || {
                    for i in 0..per_producer {
                        while t.try_send(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        drop(tx);

        let mut received = 0u64;
        while received < total {
            if rx.try_recv().is_ok() {
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    pub fn crossbeam_block(&self) {
        let per_producer = self.items_per_producer;
        let total = self.total_items();
        let (tx, rx) = crossbeam_channel::bounded::<u64>(self.capacity);

        let handles: Vec<_> = (0..self.num_producers)
            .map(|_| {
                let t = tx.clone();
                thread::spawn(move || {
                    for i in 0..per_producer {
                        t.send(black_box(i)).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);

        let mut received = 0u64;
        while received < total {
            if rx.recv().is_ok() {
                received += 1;
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    pub fn std_block(&self) {
        let per_producer = self.items_per_producer;
        let total = self.total_items();
        let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(self.capacity);

        let handles: Vec<_> = (0..self.num_producers)
            .map(|_| {
                let t = tx.clone();
                thread::spawn(move || {
                    for i in 0..per_producer {
                        t.send(black_box(i)).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);

        let mut received = 0u64;
        while received < total {
            if rx.recv().is_ok() {
                received += 1;
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    pub fn std_spin(&self) {
        let per_producer = self.items_per_producer;
        let total = self.total_items();
        let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(self.capacity);

        let handles: Vec<_> = (0..self.num_producers)
            .map(|_| {
                let t = tx.clone();
                thread::spawn(move || {
                    for i in 0..per_producer {
                        while t.try_send(black_box(i)).is_err() {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        drop(tx);

        let mut received = 0u64;
        while received < total {
            if rx.try_recv().is_ok() {
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
