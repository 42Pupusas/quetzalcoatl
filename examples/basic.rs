//! Basic SPSC (Single Producer, Single Consumer) example
//!
//! Demonstrates simple usage of the ring buffer with one producer and one consumer.

use quetzalcoatl::{Capacity, RingBuffer};

fn main() {
    // Create a ring buffer with capacity for 16 items (power of two)
    let (producer, mut consumer) = RingBuffer::new(Capacity::exact(16)).split();

    println!("=== Basic SPSC Example ===\n");

    // Producer pushes some items
    println!("Producer: Pushing items 0-4");
    for i in 0..5 {
        match producer.push(i) {
            Ok(()) => println!("  Pushed: {}", i),
            Err(val) => println!("  Failed to push {} (buffer full)", val),
        }
    }

    println!("\nConsumer: Popping items");
    // Consumer pops items
    while let Some(item) = consumer.pop() {
        println!("  Popped: {}", item);
    }

    println!("\nConsumer: Trying to pop from empty buffer");
    match consumer.pop() {
        Some(item) => println!("  Popped: {}", item),
        None => println!("  Buffer is empty"),
    }

    // Push more items
    println!("\nProducer: Pushing more items 5-9");
    for i in 5..10 {
        producer.push(i).unwrap();
    }

    // Try to push when full
    println!("\nProducer: Trying to push when buffer is full (capacity=16)");
    match producer.push(999) {
        Ok(()) => println!("  Pushed: 999"),
        Err(val) => println!("  Failed to push {} (buffer full)", val),
    }

    // Pop a few to make space
    println!("\nConsumer: Popping 3 items to make space");
    for _ in 0..3 {
        if let Some(item) = consumer.pop() {
            println!("  Popped: {}", item);
        }
    }

    // Now we can push again
    println!("\nProducer: Now we can push again");
    match producer.push(999) {
        Ok(()) => println!("  Successfully pushed: 999"),
        Err(val) => println!("  Failed to push {}", val),
    }

    println!("\nConsumer: Popping all remaining items");
    while let Some(item) = consumer.pop() {
        println!("  Popped: {}", item);
    }

    println!("\n=== Example Complete ===");
}
