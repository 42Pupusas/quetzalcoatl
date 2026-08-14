use divan::black_box;
use quetzalcoatl::capacity::Capacity;
use std::thread;

pub struct SpscWorkload {
    capacity: usize,
    total_items: u64,
}

impl SpscWorkload {
    pub const fn new(capacity: usize, total_items: u64) -> Self {
        Self {
            capacity,
            total_items,
        }
    }

    pub fn quetzalcoatl_spin(&self) {
        let total = self.total_items;
        let (producer, mut consumer) =
            quetzalcoatl::spsc::RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();

        let ph = thread::spawn(move || {
            for i in 0..total {
                while producer.push(black_box(i)).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let mut received = 0u64;
        while received < total {
            if consumer.pop().is_some() {
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        ph.join().unwrap();
    }

    pub fn quetzalcoatl_block(&self) {
        let total = self.total_items;
        let (producer, mut consumer) =
            quetzalcoatl::spsc::RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();

        let ph = thread::spawn(move || {
            for i in 0..total {
                producer.push_block(black_box(i)).expect("consumer dropped");
            }
        });

        let mut received = 0u64;
        while received < total {
            match consumer.pop_block() {
                Some(_) => received += 1,
                None => break,
            }
        }
        assert_eq!(received, total);

        ph.join().unwrap();
    }

    pub fn crossbeam_spin(&self) {
        let total = self.total_items;
        let (tx, rx) = crossbeam_channel::bounded::<u64>(self.capacity);

        let ph = thread::spawn(move || {
            for i in 0..total {
                while tx.try_send(black_box(i)).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let mut received = 0u64;
        while received < total {
            if rx.try_recv().is_ok() {
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        ph.join().unwrap();
    }

    pub fn crossbeam_block(&self) {
        let total = self.total_items;
        let (tx, rx) = crossbeam_channel::bounded::<u64>(self.capacity);

        let ph = thread::spawn(move || {
            for i in 0..total {
                tx.send(black_box(i)).unwrap();
            }
        });

        let mut received = 0u64;
        while received < total {
            if rx.recv().is_ok() {
                received += 1;
            }
        }

        ph.join().unwrap();
    }

    pub fn std_block(&self) {
        let total = self.total_items;
        let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(self.capacity);

        let ph = thread::spawn(move || {
            for i in 0..total {
                tx.send(black_box(i)).unwrap();
            }
        });

        let mut received = 0u64;
        while received < total {
            if rx.recv().is_ok() {
                received += 1;
            }
        }

        ph.join().unwrap();
    }

    pub fn std_spin(&self) {
        let total = self.total_items;
        let (tx, rx) = std::sync::mpsc::sync_channel::<u64>(self.capacity);

        let ph = thread::spawn(move || {
            for i in 0..total {
                while tx.try_send(black_box(i)).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let mut received = 0u64;
        while received < total {
            if rx.try_recv().is_ok() {
                received += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        ph.join().unwrap();
    }
}
