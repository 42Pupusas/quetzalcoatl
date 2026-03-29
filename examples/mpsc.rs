//! MPSC (Multi-Producer, Single Consumer) example
//!
//! Demonstrates concurrent producers pushing to the same ring buffer.

use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::mpsc::RingBuffer;
use std::thread;
use std::time::Duration;

fn main() {
    println!("=== MPSC Concurrent Producers Example ===\n");

    // Create a ring buffer with capacity for at least 1000 items
    let (producer, mut consumer) = RingBuffer::new(Capacity::at_least(1000)).split();

    let num_producers = 4;
    let items_per_producer = 100;

    println!(
        "Spawning {} producer threads, each pushing {} items\n",
        num_producers, items_per_producer
    );

    // Spawn producer threads
    let handles: Vec<_> = (0..num_producers)
        .map(|producer_id| {
            let p = producer.clone(); // Clone producer for each thread
            thread::spawn(move || {
                println!("Producer {} started", producer_id);

                for i in 0..items_per_producer {
                    let value = producer_id * 1000 + i;

                    // Push with retry on full buffer
                    while p.push(value).is_err() {
                        thread::yield_now();
                    }

                    // Simulate some work
                    if i % 25 == 0 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }

                println!("Producer {} finished", producer_id);
            })
        })
        .collect();

    // Give producers a moment to start
    thread::sleep(Duration::from_millis(10));

    // Consumer thread pops items while producers are working
    let mut total_consumed = 0;
    let mut items_by_producer = vec![0usize; num_producers];

    println!("\nConsumer: Starting to consume items...\n");

    // Wait for all producers to finish
    for h in handles {
        h.join().unwrap();
    }

    // Consume all remaining items
    while let Some(value) = consumer.pop() {
        let producer_id = value / 1000;
        items_by_producer[producer_id] += 1;
        total_consumed += 1;
    }

    println!("Consumer finished consuming {} items\n", total_consumed);

    // Verify results
    println!("=== Results ===");
    println!("Total items consumed: {}", total_consumed);
    println!("Expected: {}", num_producers * items_per_producer);

    for (id, count) in items_by_producer.iter().enumerate() {
        println!("  Producer {}: {} items", id, count);
    }

    assert_eq!(total_consumed, num_producers * items_per_producer);
    println!("\n✓ All items received successfully!");
    println!("\n=== Example Complete ===");
}
