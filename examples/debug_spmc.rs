use quetzalcoatl::spmc::RingBuffer;
use quetzalcoatl::capacity::Capacity;

fn main() {
    let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
    let n = 16u64;

    let handle = std::thread::spawn(move || {
        for i in 0..n {
            let mut attempts = 0u64;
            while producer.push(i).is_err() {
                attempts += 1;
                if attempts > 10_000_000 {
                    eprintln!("Producer stuck at item {i}");
                    attempts = 0;
                }
                std::thread::yield_now();
            }
            eprintln!("Pushed {i}");
        }
        eprintln!("Producer done");
    });

    let mut received = 0u64;
    let mut attempts = 0u64;
    while received < n {
        if consumer.pop().is_some() {
            received += 1;
            eprintln!("Popped (received={received})");
            attempts = 0;
        } else {
            attempts += 1;
            if attempts > 10_000_000 {
                eprintln!("Consumer stuck at received={received}");
                attempts = 0;
            }
            std::thread::yield_now();
        }
    }

    handle.join().unwrap();
    assert_eq!(received, n);
    eprintln!("OK");
}
