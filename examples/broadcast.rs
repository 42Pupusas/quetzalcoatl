//! Broadcast (Multi-Producer, Multi-Consumer) example
//!
//! Demonstrates a pub/sub pattern where every consumer receives
//! every item published after it subscribes.

use quetzalcoatl::broadcast::RingBuffer;
use quetzalcoatl::capacity::Capacity;
use std::thread;

fn main() {
    println!("=== Broadcast Example ===\n");

    // Create a broadcast ring buffer.
    // max_consumers = 8: up to 8 concurrent consumers.
    let (producer, mut c1) = RingBuffer::new(Capacity::exact(64), 8).split();

    // Clone to create a second consumer — it sees only future items.
    let mut c2 = c1.clone();

    println!("Publishing items 0-4 (both consumers subscribed)...\n");
    for i in 0u64..5 {
        producer.push(i).unwrap();
    }

    // Both consumers see the same items
    println!("Consumer 1:");
    while let Some(v) = c1.pop() {
        println!("  Got: {v}");
    }

    println!("\nConsumer 2:");
    while let Some(v) = c2.pop() {
        println!("  Got: {v}");
    }

    // Create a third consumer — it only sees items published from now on.
    let mut c3 = c1.clone();

    println!("\nPublishing items 100-104 (3 consumers subscribed)...\n");
    for i in 100u64..105 {
        producer.push(i).unwrap();
    }

    println!("Consumer 1:");
    while let Some(v) = c1.pop() {
        println!("  Got: {v}");
    }

    println!("\nConsumer 3 (joined late):");
    while let Some(v) = c3.pop() {
        println!("  Got: {v}");
    }

    // Multi-producer: clone the producer for another thread.
    let p2 = producer.clone();
    let handle = thread::spawn(move || {
        for i in 200u64..205 {
            while p2.push(i).is_err() {
                thread::yield_now();
            }
        }
    });

    for i in 300u64..305 {
        while producer.push(i).is_err() {
            thread::yield_now();
        }
    }

    handle.join().unwrap();

    println!("\nConsumer 2 (multi-producer items):");
    while let Some(v) = c2.pop() {
        println!("  Got: {v}");
    }

    println!("\n=== Example Complete ===");
}
