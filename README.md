# Quetzalcoatl

High-performance, lock-free ring buffers for Rust.

Four variants cover every producer/consumer topology:

| Module | Producers | Consumers | Use case |
|---|---|---|---|
| `mpsc` | Multiple | Single | Fan-in from worker threads |
| `spsc` | Single | Single | Pipelines, audio, networking |
| `spmc` | Single | Multiple | Work distribution, fan-out |
| `broadcast` | Multiple | Multiple | Pub/sub, event distribution |

## Features

- **Lock-free** — no mutexes, only atomic CAS / Acquire-Release
- **Zero dependencies** — pure `std` implementation
- **Zero-copy API** — `reserve()` + `commit()` on the producer side, `pop_ref()` on the consumer side
- **Miri-tested** — validated under Miri for undefined-behavior and data-race detection
- **Power-of-two capacity** — fast bitwise-AND indexing, no modulo

## Installation

```toml
[dependencies]
quetzalcoatl = "0.4"
```

## Quick start

### SPSC (single producer, single consumer)

```rust
use quetzalcoatl::spsc::RingBuffer;
use quetzalcoatl::capacity::Capacity;

let (producer, mut consumer) = RingBuffer::new(Capacity::exact(64)).split();

producer.push(42u64).unwrap();
producer.push(43).unwrap();

assert_eq!(consumer.pop(), Some(42));
assert_eq!(consumer.pop(), Some(43));
assert_eq!(consumer.pop(), None);
```

### MPSC (multiple producers, single consumer)

```rust
use quetzalcoatl::mpsc::RingBuffer;
use quetzalcoatl::capacity::Capacity;
use std::thread;

let (producer, mut consumer) = RingBuffer::new(Capacity::at_least(1000)).split();

let handles: Vec<_> = (0..4)
    .map(|id| {
        let p = producer.clone();
        thread::spawn(move || {
            for i in 0..100 {
                while p.push(id * 100 + i).is_err() {
                    thread::yield_now();
                }
            }
        })
    })
    .collect();

for h in handles {
    h.join().unwrap();
}

let mut items = Vec::new();
while let Some(item) = consumer.pop() {
    items.push(item);
}
assert_eq!(items.len(), 400);
```

### SPMC (single producer, multiple consumers)

One producer pushes items; multiple consumers compete to pop them.
Each item is consumed by exactly one consumer — ideal for work distribution.

```rust
use quetzalcoatl::spmc::RingBuffer;
use quetzalcoatl::capacity::Capacity;
use std::thread;

let (producer, consumer) = RingBuffer::new(Capacity::exact(64)).split();

let handles: Vec<_> = (0..4)
    .map(|_| {
        let c = consumer.clone();
        thread::spawn(move || {
            let mut count = 0;
            while let Some(_item) = c.pop() {
                count += 1;
            }
            count
        })
    })
    .collect();

for i in 0..100u64 {
    while producer.push(i).is_err() {
        thread::yield_now();
    }
}
drop(producer); // signal no more items

let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
assert_eq!(total, 100);
```

### Broadcast (multiple producers, multiple consumers)

Every consumer sees every item published after it subscribes.

```rust
use quetzalcoatl::broadcast::RingBuffer;
use quetzalcoatl::capacity::Capacity;

// Second argument is the maximum number of concurrent consumers.
let (producer, mut c1) = RingBuffer::new(Capacity::exact(64), 4).split();
let mut c2 = c1.clone(); // starts reading from current position

producer.push(10u64).unwrap();
producer.push(20).unwrap();

assert_eq!(c1.pop(), Some(10));
assert_eq!(c1.pop(), Some(20));
assert_eq!(c2.pop(), Some(10));
assert_eq!(c2.pop(), Some(20));
```

For large types with many consumers, `broadcast::arc::ArcRingBuffer` wraps
values in `Arc<T>` so each consumer pop is an O(1) refcount bump instead of
a full clone:

```rust
use quetzalcoatl::broadcast::arc::ArcRingBuffer;
use quetzalcoatl::capacity::Capacity;

let (producer, mut c1) = ArcRingBuffer::<[u8; 4096]>::new(Capacity::exact(64), 4).split();
let mut c2 = c1.clone();

producer.push([0xAB; 4096]).unwrap();

let arc1 = c1.pop().unwrap(); // Arc<[u8; 4096]> — cheap clone
let arc2 = c2.pop().unwrap();
assert_eq!(arc1[0], 0xAB);
assert_eq!(arc2[0], 0xAB);
```

## Zero-copy API

All four variants support a zero-copy path for large types:

```rust
use quetzalcoatl::spsc::RingBuffer;
use quetzalcoatl::capacity::Capacity;

let (producer, mut consumer) = RingBuffer::<[u8; 4096]>::new(Capacity::exact(4)).split();

// Producer: write directly into the slot
let mut writer = producer.reserve().unwrap();
writer.write([0xAB; 4096]);
writer.commit(); // makes the slot visible to the consumer

// Consumer: read without copying
let reader = consumer.pop_ref().unwrap();
assert_eq!(reader[0], 0xAB);
// slot is released when `reader` drops
```

## How it works

- **Slot reservation**: Producers use atomic compare-and-swap (CAS) to claim slots (MPSC/broadcast) or a simple local counter (SPSC/SPMC). Consumers use CAS on the head counter to claim items (SPMC).
- **Publication signaling**: Per-slot `AtomicBool` ready flags (MPSC/SPMC), tail advancement (SPSC), or per-slot `AtomicUsize` sequence numbers (broadcast).
- **Memory ordering**: Careful `Acquire`/`Release` pairs ensure data visibility without fences or mutexes.
- **Cache-line padding**: Head and tail counters are padded to avoid false sharing.

## Performance

- O(1) push and pop (with CAS retry under contention for MPSC/broadcast)
- Fixed-size buffer — no allocations on the hot path
- Scales well with multiple producers (exponential CAS backoff)

Run benchmarks:

```bash
cargo bench
```

## Safety

All `unsafe` code is documented with `SAFETY` comments. Run Miri for additional validation:

```bash
cargo +nightly miri test
```

## API documentation

Full API docs: [docs.rs/quetzalcoatl](https://docs.rs/quetzalcoatl)

## Examples

See the [`examples/`](examples/) directory:

```bash
cargo run --example basic       # SPSC basics
cargo run --example mpsc        # Multi-producer concurrent example
cargo run --example dynamic     # Dynamic producer creation pattern
cargo run --example broadcast   # Broadcast pub/sub
cargo run --example debug_spmc  # SPMC work distribution
```

## License

MIT — see [LICENSE](LICENSE) for details.
