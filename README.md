# Quetzalcoatl

A high-performance, lock-free **multi-producer, single-consumer (MPSC)** ring buffer for Rust.

## Features

- 🚀 **Lock-free**: No mutexes, uses atomic CAS operations for maximum throughput
- 🔄 **Multi-producer**: Multiple threads can push concurrently via simple `.clone()`
- 📦 **Zero dependencies**: Pure stdlib implementation
- 🛡️ **Memory safe**: Careful use of atomics and proper memory ordering
- ⚡ **Non-blocking**: Consumer returns immediately if data not ready
- 🎯 **Simple API**: Clean, idiomatic Rust interface

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
quetzalcoatl = "0.1"
```

## Usage

### Basic SPSC (Single Producer, Single Consumer)

```rust
use quetzalcoatl::RingBuffer;

let (producer, mut consumer) = RingBuffer::new(100).split();

// Producer pushes items
producer.push(42).unwrap();
producer.push(43).unwrap();

// Consumer pops items
assert_eq!(consumer.pop(), Some(42));
assert_eq!(consumer.pop(), Some(43));
assert_eq!(consumer.pop(), None); // Empty
```

### MPSC (Multi-Producer, Single Consumer)

```rust
use quetzalcoatl::RingBuffer;
use std::thread;

let (producer, mut consumer) = RingBuffer::new(1000).split();

// Spawn multiple producer threads
let handles: Vec<_> = (0..4)
    .map(|id| {
        let p = producer.clone(); // Clone producer for each thread
        thread::spawn(move || {
            for i in 0..100 {
                p.push(id * 100 + i).unwrap();
            }
        })
    })
    .collect();

// Wait for all producers
for h in handles {
    h.join().unwrap();
}

// Consumer receives all items (order may vary)
let mut items = Vec::new();
while let Some(item) = consumer.pop() {
    items.push(item);
}
assert_eq!(items.len(), 400);
```

### Dynamic Producer Creation

Perfect for network servers or event-driven systems:

```rust
use quetzalcoatl::RingBuffer;
use std::sync::Arc;
use std::thread;

let (producer, mut consumer) = RingBuffer::new(1000).split();

// Simulate new connections arriving dynamically
for connection_id in 0..10 {
    let p = producer.clone();
    thread::spawn(move || {
        // Each connection gets its own producer clone
        p.push(connection_id).unwrap();
    });
}
```

## How It Works

### Lock-Free Algorithm

- **Slot Reservation**: Producers use atomic compare-and-swap (CAS) to reserve slots
- **Ready Flags**: Each slot has an atomic ready flag to signal completion
- **Non-Blocking Consumer**: Returns `None` if slot claimed but not yet written
- **Memory Ordering**: Careful use of Acquire/Release semantics ensures proper synchronization

### Performance Characteristics

- **Time Complexity**: O(1) push and pop operations (with CAS retry on contention)
- **Space Complexity**: O(capacity) fixed-size buffer
- **Throughput**: Scales well with multiple producers (no lock contention)
- **Latency**: Low latency, fully non-blocking design

### When Consumer Returns None

The consumer may return `None` in two cases:

1. **Queue is empty**: No items available
2. **Slot not ready**: A producer claimed a slot but hasn't finished writing yet

This is expected behavior for a lock-free queue. The consumer can simply retry:

```rust
// Retry logic
loop {
    if let Some(item) = consumer.pop() {
        process(item);
        break;
    }
    // Optionally yield or continue with other work
}
```

## Safety

All unsafe code is carefully documented with `SAFETY` comments. The implementation:

- Uses `UnsafeCell<MaybeUninit<T>>` for uninitialized storage
- Employs atomic operations with proper memory ordering
- Ensures no data races through type system (`Send` + `Sync` bounds)
- Validates correctness with extensive testing

Run with Miri for additional validation:

```bash
cargo +nightly miri test
```

## API Documentation

Full API documentation is available at [docs.rs/quetzalcoatl](https://docs.rs/quetzalcoatl).

## Examples

See the [`examples/`](examples/) directory for complete working examples:

- `basic.rs` - Simple SPSC usage
- `mpsc.rs` - Multi-producer concurrent example
- `dynamic.rs` - Dynamic producer creation pattern

Run examples with:

```bash
cargo run --example basic
cargo run --example mpsc
```

## Limitations

- **Single consumer only**: Only one thread can call `pop()`
- **Fixed capacity**: Buffer size set at creation time
- **No blocking**: Consumer doesn't wait for data (use channels if you need blocking)

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Please ensure:

- All tests pass: `cargo test`
- Clippy is clean: `cargo clippy -- -D warnings`
- Code is formatted: `cargo fmt`

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.
