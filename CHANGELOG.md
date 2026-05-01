# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0] - 2026-05-01

### Added
- **`spsc::Producer::push_block` / `spsc::Consumer::pop_block`** —
  blocking variants for the SPSC ring. Symmetric single-side parking
  via `OnceLock<Thread>` + `AtomicBool` per side. `pop_block` returns
  `None` once the producer drops AND the queue drains; `push_block`
  returns `Err(val)` once the consumer drops.
- **`spsc::Consumer::is_closed`** — observe producer-drop terminally.
- **`spmc::Producer::push_block` / `spmc::Consumer::pop_block`** —
  blocking variants for the SPMC ring. Single-producer side uses
  `OnceLock<Thread>` + `AtomicBool`; multi-consumer side uses the
  shared `WakeSet` futex-style bitmap. Reuses the existing `closed`
  flag for producer-drop and adds `consumer_closed` for last-consumer
  drop.
- **`mpmc::Producer::reserve` + `SlotWriter` / `WrittenSlot`** —
  zero-copy producer API for the MPMC ring, mirroring the shape
  used by spsc/spmc/mpsc. `SlotWriter` dropped without commit
  restores the slot's bit to `batch_unused` so the same producer
  can reuse it without re-claiming a batch position; `WrittenSlot`
  dropped without commit drops the value in place and rolls back
  the bit. Producer-drop's existing tombstone loop covers any bit
  still uncommitted at handle-drop.
- **`mpmc::Consumer::pop_ref` + `SlotReader`** — zero-copy consumer
  API. Holds the slot in state "claimed but not released"
  (`ready[s] = round_pos + 2`) until the reader is dropped; on
  drop, drops the value, releases `done[s] = round_pos + cap`,
  and wakes one parked producer. Long-lived readers under
  contention will block the producer at the next-round position.
- **`reserve_block` / `pop_ref_block`** on spsc, spmc, mpsc, and
  mpmc — zero-copy blocking variants. Same wait protocol as
  `push_block` / `pop_block`; signature mirrors `reserve` /
  `pop_ref` (returns `Option<SlotWriter>` / `Option<SlotReader>`,
  with `None` meaning the peer has dropped). Each ring uses a
  non-mutating gate (`has_space` / `has_item`) inside the park
  loop so we don't FAA, CAS, or tombstone a slot we'd then have
  to roll back per iteration. Broadcast still has no blocking
  API by design.
- **`drain` / `drain_up_to` / `drain_block`** on spsc and mpmc;
  **`drain_block`** on mpsc (drain/`drain_up_to` already existed).
  drain_block combines drain's batched wake fan-out with park-on-
  empty, exiting cleanly when all producers have dropped.

### Fixed
- **`mpsc::Consumer::drain` woke only one parked producer per
  batch.** When N producers were parked on `push_block` waiting
  for space, a single drain freed N slots but only one producer
  resumed — the rest stayed parked until the next push or pop
  emitted another wake. With drain-only consumer patterns this
  could deadlock. Replaced `wake_one()` with a new
  `WakeSet::wake_n(count)` that releases up to `count` parkers
  per call. `mpmc::Consumer::drain` (newly added in this release)
  uses the same `wake_n(count)` shape, so the regression class
  is closed everywhere drains exist.

## [0.8.1] - 2026-05-01

### Fixed
- **Idle-thread CPU on `push_block` / `pop_block`** — replaced the
  200μs `park_timeout` backstop with plain `park()` across both
  `mpmc` and `mpsc` slow paths. The timeout caused parked threads
  to wake ~5,000×/sec on a fully idle ring, scan, and re-park,
  burning CPU and atomic traffic for no benefit. Wake correctness
  rests on the existing SeqCst `fetch_or` / `Relaxed` load pairing
  plus close-time `WakeSet::flush`; the timeout was redundant.

## [0.8.0] - 2026-04-27

### Added
- **`mpmc` module** — relaxed-FIFO multi-producer multi-consumer ring
  with no shared head cursor. Producers reserve batches via FAA on a
  shared `claim` cursor and publish out of order; consumers scan
  privately and CAS-claim the first published slot they find.
  Throughput in the scan-based design is 5–8× the prior strict-FIFO
  MPMC at low contention shapes (p2q2, p4q4) and ~2× at p8q8 median;
  the trade-off is loss of strict ordering — items are returned in
  publish order, not push order, and there is no FIFO across
  producers. Capacity must be `>= 4`.
- **`mpmc::Producer::push_block`** — blocks the calling thread on
  full ring instead of returning `Err`, parking via the same
  futex-style wake bitmap used internally. Returns `Err(val)` only
  when the last `Consumer` has dropped.
- **`mpmc::Consumer::pop_block`** — blocks on empty ring, returning
  `None` only after the last `Producer` drops AND the ring drains.
- **`mpmc::Config` trait** with `DefaultConfig` and `Cfg<B, S, F>`
  helper for compile-time tuning of `PRODUCER_BATCH`,
  `CAS_FAIL_SKIP`, `CONSUMED_FLUSH`. Bounds validated at
  monomorphization (`PRODUCER_BATCH` in `1..=32`, others `>= 1`).
- Diagnostic examples in `examples/`: `mpmc_perf` (single-shot
  throughput), `mpmc_long` (per-iteration distribution),
  `mpmc_pinned` (CPU-affinity strategies for variance investigation),
  `mpmc_block` (push/pop × spin/block comparison),
  `mpmc_vs_nspmc_dhat` (heap profile vs sharded N-SPMC under the
  optional `dhat-heap` feature).

### Changed
- **MPMC consolidation**: the prior strict-FIFO `mpmc` variant is
  removed. The scan-based variant (formerly `mpmc_scan`) is the only
  MPMC ring shipped, and it now occupies the `mpmc::` module path.
  Callers using the old MPMC must migrate; the new ring requires
  `cap >= 4` (the per-slot tri-state encoding aliases at smaller
  capacities) — workloads with `cap < 4` should switch to `spsc` /
  `spmc` / `mpsc`.
- Producer slow path uses futex-style park/unpark on a 64-bit wake
  bitmap (`std::thread::park_timeout` + `OnceLock<Thread>` parker
  table) when the spin/yield budget is exhausted. Mitigates the
  bimodal throughput collapse observed at thread counts saturating
  the machine — though SMT-pairing variance near `P + Q ≈ 2N`
  remains a fundamental property of spin-based MPMC; see the
  module docs for thread-count guidance.

### Removed
- `mpmc-instrument` cargo feature (the strict-FIFO MPMC it
  instrumented is gone).

## [0.7.1] - 2026-04-27

### Fixed
- Clippy: `Consumer::new` (SPMC) is now `const fn` (clippy::missing_const_for_fn).
- Clippy: replaced `|v| drop(v)` with `drop` in MPSC drain test
  (clippy::redundant_closure).

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

[Unreleased]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.8.0...HEAD
[0.8.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.6.0...v0.7.0
[0.1.0]: https://github.com/42Pupusas/quetzalcoatl/releases/tag/v0.1.0
