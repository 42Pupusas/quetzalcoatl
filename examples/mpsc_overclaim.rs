use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::RingBuffer;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

struct OverclaimProbe {
    capacity: usize,
    producers: usize,
    deadline: Duration,
}

impl OverclaimProbe {
    const fn new(capacity: usize, producers: usize) -> Self {
        Self {
            capacity,
            producers,
            deadline: Duration::from_secs(2),
        }
    }

    fn run(&self) -> ProbeOutcome {
        let (producer, _consumer) = RingBuffer::<u64>::new(Capacity::exact(self.capacity)).split();
        let gate = Arc::new(Barrier::new(self.producers));
        let returned = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));

        for id in 0..self.producers {
            let handle = producer.clone();
            let gate = Arc::clone(&gate);
            let returned = Arc::clone(&returned);
            let accepted = Arc::clone(&accepted);
            std::thread::spawn(move || {
                gate.wait();
                if handle.push(id as u64).is_ok() {
                    accepted.fetch_add(1, Ordering::Relaxed);
                }
                returned.fetch_add(1, Ordering::Relaxed);
            });
        }

        let start = Instant::now();
        while start.elapsed() < self.deadline {
            if returned.load(Ordering::Relaxed) == self.producers {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        ProbeOutcome {
            producers: self.producers,
            capacity: self.capacity,
            returned: returned.load(Ordering::Relaxed),
            accepted: accepted.load(Ordering::Relaxed),
        }
    }
}

struct ProbeOutcome {
    producers: usize,
    capacity: usize,
    returned: usize,
    accepted: usize,
}

impl ProbeOutcome {
    fn stuck(&self) -> usize {
        self.producers - self.returned
    }

    fn report(&self) {
        println!(
            "cap={:<4} producers={:<3} returned={:<3} accepted={:<3} stuck={}",
            self.capacity,
            self.producers,
            self.returned,
            self.accepted,
            self.stuck()
        );
    }
}

struct ProbeSuite;

impl ProbeSuite {
    fn run(&self) {
        println!("push() must never block: every producer must return, capacity or not.\n");
        let mut stuck_total = 0usize;
        for attempt in 0..8 {
            println!("-- attempt {attempt} --");
            for (cap, producers) in [(1, 8), (1, 16), (2, 16), (4, 16), (16, 32)] {
                let outcome = OverclaimProbe::new(cap, producers).run();
                outcome.report();
                stuck_total += outcome.stuck();
            }
        }
        println!("\ntotal producers stuck inside push(): {stuck_total}");
    }
}

fn main() {
    ProbeSuite.run();
}
