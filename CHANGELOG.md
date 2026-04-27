# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-04-27

### Changed
- **SPMC layout**: data and per-slot synchronization markers now live
  in separate cache-padded arrays (struct-of-arrays). The producer's
  `ready[s]` write and the consumer's `done[s]` write target distinct
  cache lines, eliminating the symmetric producer↔consumer ping-pong
  on the readiness array.
- **SPMC consumer**: replaced per-pop `head.fetch_add(1)` with batched
  bounded-CAS claim. Consumers reserve up to `BATCH_SIZE = 32` positions
  in a single CAS bounded by the producer's `tail`, then drain locally
  without touching the shared `head`. Eliminates the head-line ping-pong
  that dominated multi-consumer workloads (~28% of cycles at 8c) and the
  post-claim spin loop. Throughput improves 1.5×–10× depending on
  consumer count, with the largest gains at 2–4 consumers.
- **SPMC `len()` semantics**: now reports positions not yet claimed by
  any consumer, which means it can transiently underestimate by up to
  `BATCH_SIZE` per consumer. Documentation updated; `len()` was already
  flagged as approximate.

### Added
- `Consumer::is_closed()` — returns `true` once the producer has been
  dropped, letting workers exit cleanly when the queue drains. Used in
  the example/bench harnesses in place of the prior shared-counter
  termination scheme.
- `bench_slow_work_scaling` and `bench_burst_producer_slow_work` in
  `benches/spmc.rs` — exercise the consumer claim path under
  ~50µs/item synthetic work modelling realistic consumer pools (e.g.
  signature verification). Confirms the bounded-CAS clamp `take =
  min(BATCH_SIZE, tail - head)` prevents monopoly windows from forming
  when the queue is shallow; SPMC tracks N×SPSC round-robin within
  ~5–15% across burst sizes 8, 32, 64 and consumer counts 1–8 under
  layout-fair conditions.

## [0.1.0] - 2024-02-05

### Added
- Initial release of Quetzalcoatl lock-free MPSC ring buffer
- `RingBuffer::new(capacity)` constructor
- `RingBuffer::split()` method to create producer/consumer pair
- `Producer` type with lock-free `push()` method
- `Consumer` type with non-blocking `pop()` method
- `Producer` implements `Clone` for easy multi-producer usage
- Helper methods: `len()`, `is_empty()`, `is_full()` on both Producer and Consumer
- Comprehensive test suite (14 tests covering SPSC and MPSC scenarios)
- Support for arbitrary capacity (power-of-two and non-power-of-two)
- Proper memory ordering with Acquire/Release/AcqRel semantics
- Zero external dependencies

### Features
- Lock-free multi-producer, single-consumer (MPSC) pattern
- Atomic CAS-based slot reservation
- Per-slot ready flags for synchronization
- Non-blocking consumer behavior
- Thread-safe with proper `Send` + `Sync` bounds
- Works with any `T: Send` type including zero-sized types

### Safety
- All unsafe code documented with SAFETY comments
- Proper use of `UnsafeCell<MaybeUninit<T>>` for uninitialized memory
- Validated with extensive testing
- Clippy clean with pedantic lints enabled

[Unreleased]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.6.0...v0.7.0
[0.1.0]: https://github.com/42Pupusas/quetzalcoatl/releases/tag/v0.1.0
