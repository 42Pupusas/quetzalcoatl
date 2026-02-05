//! Dynamic producer creation example
//!
//! Demonstrates creating producers dynamically as needed, simulating
//! scenarios like network servers where connections arrive over time.

use quetzalcoatl::{Capacity, RingBuffer};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
struct Message {
    connection_id: usize,
    data: String,
}

fn main() {
    println!("=== Dynamic Producer Creation Example ===\n");
    println!("Simulating a server with dynamic client connections\n");

    // Create a ring buffer for messages
    let (producer, mut consumer) = RingBuffer::new(Capacity::at_least(100)).split();

    // Simulate connections arriving over time
    let connection_handles: Vec<_> = (0..10)
        .map(|connection_id| {
            // Simulate staggered connection arrival
            thread::sleep(Duration::from_millis(connection_id as u64 * 10));

            let p = producer.clone(); // Each connection gets its own producer
            thread::spawn(move || {
                println!("Connection {} established", connection_id);

                // Each connection sends a few messages
                for msg_num in 0..5 {
                    let message = Message {
                        connection_id,
                        data: format!("Message {} from connection {}", msg_num, connection_id),
                    };

                    // Push with retry
                    let mut msg = Some(message);
                    while let Some(m) = msg {
                        match p.push(m) {
                            Ok(()) => msg = None,
                            Err(m) => {
                                msg = Some(m);
                                thread::yield_now();
                            }
                        }
                    }

                    // Simulate time between messages
                    thread::sleep(Duration::from_millis(5));
                }

                println!("Connection {} closed", connection_id);
            })
        })
        .collect();

    // Consumer processes messages as they arrive
    let consumer_handle = thread::spawn(move || {
        let mut messages_received = 0;
        let mut active = true;

        println!("\nServer: Started processing messages\n");

        while active || messages_received < 50 {
            match consumer.pop() {
                Some(msg) => {
                    println!(
                        "Server: Received from connection {}: {}",
                        msg.connection_id, msg.data
                    );
                    messages_received += 1;
                }
                None => {
                    // No messages available, continue polling
                    thread::sleep(Duration::from_millis(1));

                    // Check if we've received all expected messages
                    if messages_received >= 50 {
                        active = false;
                    }
                }
            }
        }

        println!(
            "\nServer: Processed {} messages total",
            messages_received
        );
        messages_received
    });

    // Wait for all connections to finish
    for h in connection_handles {
        h.join().unwrap();
    }

    // Wait for consumer to finish
    let total = consumer_handle.join().unwrap();

    println!("\n=== Results ===");
    println!("Total messages processed: {}", total);
    println!("Expected: 50 (10 connections × 5 messages)");
    assert_eq!(total, 50);
    println!("\n✓ All messages received successfully!");
    println!("\n=== Example Complete ===");
}
