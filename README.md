# Quetzalcoatl

High-performance, lock-free ring buffers for Rust.

Five variants cover every producer/consumer topology:

| Module | Producers | Consumers | Use case |
|---|---|---|---|
| `spsc` | Single | Single | Pipelines, audio, networking |
| `mpsc` | Multiple | Single | Fan-in from worker threads |
| `spmc` | Single | Multiple | Work distribution, fan-out |
| `mpmc` | Multiple | Multiple | High-throughput work queues |
| `broadcast` | Multiple | Multiple | Pub/sub, event distribution |

`mpmc` is the relaxed-FIFO scan-based design: producers reserve
batches via FAA, consumers scan privately and CAS-claim individual
slots. No shared head cursor, contention distributed across
per-slot atomics, futex-style park/unpark on backpressure. Use
`spmc` or one of the FIFO variants when ordering matters.

## Features

- **Lock-free** — no mutexes, only atomic FAA / Acquire-Release
- **Zero dependencies** — pure `std` implementation
- **Zero-copy API** — typestate `reserve()` → `write()` → `commit()` on the producer side, `pop_ref()` on the consumer side (all five variants)
- **Blocking variants** — `push_block` / `pop_block` / `reserve_block` / `pop_ref_block` across all rings (except broadcast) park on backpressure instead of forcing the caller to spin
- **Async variants** — `push_async` / `pop_async` across all five variants (behind the `async` feature flag) integrate with any `Waker`-based executor
- **Borrowed split** — `split_borrowed()` on SPSC/MPSC/SPMC lets the ring buffer own the storage while handing out `&`-tied producer/consumer handles, removing the `'static` bound on `T` for zero-copy APIs
- **Batch drain** — `drain()` / `drain_up_to()` / `drain_block()` amortize cache-line invalidations across the batch (O(1) per batch, not per item)
- **Compile-time tuning** — `mpmc::Config` trait + `Cfg<B, S, F>` helper let you customize batch / scan / flush behavior at the type level
- **Sound by construction** — `commit()` is only available on `WrittenSlot` (after `write()`), so safe code cannot cause UB
- **Miri-tested** — validated under Miri for undefined-behavior and data-race detection
- **Power-of-two capacity** — fast bitwise-AND indexing, no modulo

## Installation

```toml
[dependencies]
quetzalcoatl = "0.11"
```

To enable async support:

```toml
[dependencies]
quetzalcoatl = { version = "0.11", features = ["async"] }
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

// drain() amortizes the head-pointer update across the batch,
// reducing cache-line invalidations from O(n) to O(1).
let mut items = Vec::new();
consumer.drain(|item| items.push(item));
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

### MPMC (multiple producers, multiple consumers)

A relaxed-FIFO work queue. Each item goes to exactly one consumer,
chosen by the scan order — items are returned in publish order, not
push order, and there is no FIFO guarantee across producers. Trade
strict ordering for throughput when the workload allows.

```rust
use quetzalcoatl::mpmc::RingBuffer;
use quetzalcoatl::capacity::Capacity;
use std::thread;

let (producer, consumer) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();

// Cloneable producer + consumer handles for fan-in / fan-out.
let producers: Vec<_> = (0..4)
    .map(|tid| {
        let p = producer.clone();
        thread::spawn(move || {
            for i in 0..1000u64 {
                while p.push(tid * 1000 + i).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    })
    .collect();

let consumers: Vec<_> = (0..4)
    .map(|_| {
        let c = consumer.clone();
        thread::spawn(move || {
            // pop_block parks on empty, returns None only after the
            // last producer drops AND the ring drains.
            let mut count = 0u64;
            while c.pop_block().is_some() {
                count += 1;
            }
            count
        })
    })
    .collect();

drop(producer);
drop(consumer);

for h in producers { h.join().unwrap(); }
let total: u64 = consumers.into_iter().map(|h| h.join().unwrap()).sum();
assert_eq!(total, 4 * 1000);
```

Capacity must be `>= 4` (the per-slot tri-state encoding requires
it). For other sizes, use `spsc` / `spmc` / `mpsc`.

#### Tuning

Three knobs are exposed via [`mpmc::Config`]: `PRODUCER_BATCH`
(positions reserved per FAA on the claim cursor), `CAS_FAIL_SKIP`
(how far the consumer scan jumps after a lost CAS), and
`CONSUMED_FLUSH` (pops between watermark flushes). Defaults are
chosen for `cap=1024` and balanced (P, Q) workloads; tune via the
inline helper:

```rust
use quetzalcoatl::mpmc::{Cfg, RingBuffer};
use quetzalcoatl::capacity::Capacity;

// PRODUCER_BATCH=16, CAS_FAIL_SKIP=64, CONSUMED_FLUSH=32
let (p, c) = RingBuffer::<u64, Cfg<16, 64, 32>>::new(Capacity::exact(256)).split();
# drop((p, c));
```

#### Thread-count guidance

`mpmc` parks producers/consumers on a futex-style wake bitmap when
they can't make forward progress, but it still relies on
spin-then-park backoff in the common case. On an N-physical-core
SMT machine (2N logical CPUs) throughput becomes highly variable
once `P + Q` saturates the machine — peer threads sharing decode
bandwidth with their SMT siblings causes 5–10× run-to-run swings
near `P + Q ≈ 2N`.

Rules of thumb: `P + Q ≤ N` is tight and predictable; `P + Q < 2N`
is good with mild SMT-pairing variance; `P + Q ≈ 2N` is bimodal;
`P + Q > 2N` collapses on oversubscription. See the module docs
for details.

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

All five variants support a zero-copy path using a **typestate pattern**
that enforces correctness at compile time:

```rust
use quetzalcoatl::spsc::RingBuffer;
use quetzalcoatl::capacity::Capacity;

let (mut producer, mut consumer) = RingBuffer::<[u8; 4096]>::new(Capacity::exact(4)).split();

// Producer: reserve a slot, write into it, commit
let writer = producer.reserve().unwrap();   // SlotWriter (no data yet)
writer.write([0xAB; 4096]).commit();        // write() → WrittenSlot → commit()

// Consumer: read without copying
let reader = consumer.pop_ref().unwrap();
assert_eq!(reader[0], 0xAB);
// slot is released when `reader` drops
```

The type system guarantees soundness:

- `reserve()` returns a `SlotWriter` — you can inspect the slot via
  `slot_mut()` or initialize it via `write()`.
- `write()` consumes the `SlotWriter` and returns a `WrittenSlot`.
- `commit()` is only available on `WrittenSlot`, so you **cannot commit
  uninitialized data in safe Rust**.
- For advanced use (initializing via `slot_mut()`), an `unsafe fn
  commit_unchecked()` is available on `SlotWriter`.
- Dropping a `SlotWriter` rolls back the reservation (SPSC/SPMC) or
  tombstones the slot (MPSC/broadcast) — no panic, no abort.
- Dropping a `WrittenSlot` drops the value and then rolls back or
  tombstones — no leak, no panic.

## How it works

- **Slot reservation**: MPSC and broadcast producers use fetch-and-add (FAA)
  to claim slots contention-free on the first try. SPSC/SPMC use a simple
  local counter on the producer side. Consumers use CAS on the head counter
  to claim items (SPMC).
- **Publication signaling**: Per-slot `AtomicUsize` sequence numbers
  encode slot state (free / published / tombstoned). SPSC uses simple
  tail advancement since only one producer exists.
- **Memory ordering**: Careful `Acquire`/`Release` pairs ensure data
  visibility without fences or mutexes.
- **Cache-line padding**: Head and tail counters are padded to 64 bytes
  to avoid false sharing.
- **Tombstoning**: If an MPSC/broadcast `SlotWriter` is dropped without
  writing, the slot is marked with a tombstone sentinel. Consumers
  detect tombstones and silently skip past them.

## Performance

- O(1) push and pop — FAA for MPSC/broadcast producers (no retry loops),
  CAS for SPMC consumers
- Fixed-size buffer — no allocations on the hot path
- `drain()` / `drain_up_to()` / `drain_block()` across all rings amortize
  the head-pointer update for batch consumption (O(1) cache-line
  invalidations per batch instead of per item)
- Two-level `min_head` cache in broadcast avoids O(N) consumer scans

Run benchmarks:

```bash
cargo bench                    # all benchmarks
cargo bench --bench fanin      # MPSC vs N×SPSC fan-in vs tokio vs crossbeam
```

## Safety

All public APIs are safe (no `unsafe` in user-facing code except
`commit_unchecked()`). Internal `unsafe` blocks are documented with
`// SAFETY:` comments explaining the invariant that makes each operation
sound.

Run Miri for additional validation:

```bash
cargo +nightly miri test
```

## API documentation

Full API docs: [docs.rs/quetzalcoatl](https://docs.rs/quetzalcoatl)

## Examples

See the [`examples/`](examples/) directory:

```bash
cargo run --example basic        # SPSC basics
cargo run --example mpsc         # Multi-producer concurrent example
cargo run --example dynamic      # Dynamic producer creation pattern
cargo run --example broadcast    # Broadcast pub/sub
cargo run --example debug_spmc   # SPMC work distribution
cargo run --example mpmc_block   # MPMC push/pop × spin/block comparison
cargo run --example mpmc_perf    # MPMC single-shot throughput
cargo run --example mpmc_long    # MPMC per-iteration distribution
cargo run --example mpmc_pinned  # MPMC with CPU-affinity strategies
cargo run --example io_uring_recv # io_uring integration (Linux only)
```

## License

MIT — see [LICENSE](LICENSE) for details.
