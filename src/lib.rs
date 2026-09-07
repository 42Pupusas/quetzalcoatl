//! Lock-free ring buffers for high-performance concurrent communication.
//!
//! Quetzalcoatl provides four ring buffer variants, each optimized for a
//! different producer/consumer topology:
//!
//! | Module | Producers | Consumers | Best for |
//! |---|---|---|---|
//! | [`spsc`] | Single | Single | Pipelines, audio, networking |
//! | [`mpsc`] | Multiple | Single | Fan-in from worker threads |
//! | [`spmc`] | Single | Multiple | Work distribution / fan-out |
//! | [`broadcast`] | Multiple | Multiple | Pub/sub, event distribution |
//!
//! All variants are fully **lock-free** (no mutexes), use fixed-size
//! power-of-two buffers, and provide both a cloning `pop()` and a
//! zero-copy `pop_ref()` / `reserve()` → `write()` → `commit()` API.
//!
//! # Quick start
//!
//! Every buffer follows the same pattern: create a [`capacity::Capacity`],
//! construct the ring buffer, and `split()` into producer + consumer handles.
//!
//! ## MPSC (multi-producer, single-consumer)
//!
//! ```
//! use quetzalcoatl::mpsc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = RingBuffer::new(Capacity::exact(64)).split();
//!
//! // Clone the producer for multiple threads
//! let p2 = producer.clone();
//! let handle = std::thread::spawn(move || { p2.push(1u64).unwrap(); });
//!
//! producer.push(2).unwrap();
//! handle.join().unwrap();
//!
//! let mut values = vec![];
//! while let Some(v) = consumer.pop() { values.push(v); }
//! assert!(values.contains(&1));
//! assert!(values.contains(&2));
//! ```
//!
//! ## SPSC (single-producer, single-consumer)
//!
//! ```
//! use quetzalcoatl::spsc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, mut consumer) = RingBuffer::new(Capacity::exact(64)).split();
//!
//! producer.push(42u64).unwrap();
//! assert_eq!(consumer.pop(), Some(42));
//! ```
//!
//! ## SPMC (single-producer, multiple-consumer)
//!
//! ```
//! use quetzalcoatl::spmc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (producer, consumer) = RingBuffer::new(Capacity::exact(64)).split();
//!
//! // Clone the consumer for multiple threads
//! let c2 = consumer.clone();
//! std::thread::spawn(move || { assert!(c2.pop().is_some()); });
//!
//! producer.push(1u64).unwrap();
//! producer.push(2).unwrap();
//! # std::thread::sleep(std::time::Duration::from_millis(50));
//!
//! // Each item is consumed by exactly one consumer
//! ```
//!
//! ## Broadcast (multi-producer, multi-consumer)
//!
//! Every consumer sees every item published after it subscribes.
//!
//! ```
//! use quetzalcoatl::broadcast::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! // max_consumers = 4
//! let (producer, mut c1) = RingBuffer::new(Capacity::exact(64), 4).split();
//! let mut c2 = c1.clone(); // new consumer, sees future items
//!
//! producer.push(99u64).unwrap();
//!
//! assert_eq!(c1.pop(), Some(99));
//! assert_eq!(c2.pop(), Some(99)); // both see the same item
//! ```
//!
//! # Zero-copy API
//!
//! For large types, avoid copying with `reserve()` (producer) and
//! `pop_ref()` (consumer). The `reserve()` API uses a **typestate
//! pattern** — `write()` consumes the `SlotWriter` and returns a
//! `WrittenSlot` that can be safely committed:
//!
//! ```
//! use quetzalcoatl::spsc::RingBuffer;
//! use quetzalcoatl::capacity::Capacity;
//!
//! let (mut producer, mut consumer) = RingBuffer::<[u8; 4096]>::new(Capacity::exact(4)).split();
//!
//! // Write directly into the slot
//! let writer = producer.reserve().unwrap();
//! writer.write([0xAB; 4096]).commit();
//!
//! // Read without copying
//! let reader = consumer.pop_ref().unwrap();
//! assert_eq!(reader[0], 0xAB);
//! // slot is released when `reader` drops
//! ```
//!
//! # Choosing a buffer type
//!
//! - **[`spsc`]**: Lowest overhead. No atomics on the hot path beyond a
//!   single `Acquire`/`Release` pair. Use when you have exactly one
//!   producer and one consumer.
//!
//! - **[`mpsc`]**: Multiple producers claim slots via fetch-and-add (FAA),
//!   eliminating inter-producer contention on the tail pointer. Single
//!   consumer pops lock-free. Use for fan-in patterns (many writers, one
//!   reader). For high-throughput bulk consumption, use
//!   [`mpsc::Consumer::drain`] which amortizes the head pointer update
//!   across an entire batch.
//!
//! - **[`spmc`]**: Single producer writes lock-free. Multiple consumers
//!   share a CAS loop to claim items. Each item goes to exactly one
//!   consumer. Use for work-distribution / fan-out patterns.
//!
//! - **[`broadcast`]**: Multiple producers (CAS on the tail) and
//!   multiple consumers. Each consumer maintains its own read cursor.
//!   Items require `T: Clone` for `pop()`, or use `pop_ref()` for
//!   zero-copy reads. Use for pub/sub or event fan-out. With no
//!   consumers registered, pushes fail rather than being discarded.
//!   For large types with many consumers, see
//!   [`broadcast::arc::ArcRingBuffer`] which wraps values in `Arc<T>`
//!   for O(1) consumer clones.

#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::perf)]
#![allow(clippy::missing_errors_doc)]

pub mod broadcast;
pub mod capacity;
pub(crate) mod common;
pub mod mpmc;
pub mod mpsc;
pub mod spmc;
pub mod spsc;

// Re-export building blocks useful for external ring-buffer
// implementations (e.g. io_uring adapters).
pub use common::backoff::Backoff;
pub use common::park;
pub use common::CachePadded;
