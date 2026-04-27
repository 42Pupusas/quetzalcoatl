use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpmc_scan::RingBuffer;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

fn main() {
    let total: u64 = 1_700_000;
    let p = 2usize;
    let q = 2usize;
    let per_p = total / p as u64;
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();
    let received = Arc::new(AtomicUsize::new(0));
    let target = (per_p * p as u64) as usize;
    let done_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let consumers: Vec<_> = (0..q)
        .map(|cid| {
            let c = consumer.clone();
            let r = received.clone();
            let df = done_flag.clone();
            thread::spawn(move || {
                let mut local = 0u64;
                loop {
                    if let Some(v) = c.pop() {
                        std::hint::black_box(v);
                        local += 1;
                        r.fetch_add(1, Ordering::Relaxed);
                    } else if r.load(Ordering::Relaxed) >= target {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                eprintln!("consumer {cid} popped {local}");
            })
        })
        .collect();

    drop(consumer);

    let prods: Vec<_> = (0..p)
        .map(|tid| {
            let pp = producer.clone();
            thread::spawn(move || {
                for i in 0..per_p {
                    while pp.push((tid as u64) * per_p + i).is_err() {
                        std::hint::spin_loop();
                    }
                }
                eprintln!("producer {tid} done");
            })
        })
        .collect();
    drop(producer);

    let watchdog = {
        let r = received.clone();
        let df = done_flag.clone();
        thread::spawn(move || {
            let mut prev = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(2));
                let cur = r.load(Ordering::Relaxed);
                eprintln!("[watchdog] received={cur}/{target}");
                if cur >= target {
                    break;
                }
                if cur == prev {
                    eprintln!("[watchdog] STALL at received={cur}");
                    df.store(true, Ordering::Relaxed);
                    std::process::exit(2);
                }
                prev = cur;
            }
        })
    };

    for h in prods {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
    drop(watchdog);
    eprintln!("DONE");
}
