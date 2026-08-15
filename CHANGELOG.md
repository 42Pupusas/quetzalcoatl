# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.0] - 2026-08-14

### Fixed
- **Async wake path woke one waiter per progress event** — that is
  unsound, because a registered waiter cannot always use the position
  that was just freed. An mpmc producer publishes into a per-producer
  batch, so the freed position can belong to a different producer than
  the one the scan reaches first. The scanned producer re-registers and
  returns `Pending`, which consumes the wake, while the producer that
  owns the position stays parked. The consumers then find the ring
  empty and send no more wake events. A parked *thread* survives this
  (the `WakeSet` park sites keep a 1 ms `park_timeout` backstop), but
  an async waiter has no backstop: after `Poll::Pending`, only its
  waker can poll it again. `WakerSet::wake_one` becomes `wake_all`, and
  `wake_n` is now an alias for it. A waiter that cannot progress
  re-registers, which costs one extra poll. The round-robin cursor was
  a partial mitigation for the same failure and is removed.
- **`mpsc::Producer::push` could wait on a full ring** — `claim_slot`
  checked capacity and then advanced `tail` with a separate
  fetch-and-add, so a concurrent producer could move `tail` between the
  two steps. The producer then waited on a slot that no consumer would
  free, although `push` is documented non-blocking. A
  compare-and-exchange loop now checks capacity and advances `tail` as
  one operation. Measured overclaim rate on the old code: 0% at 1-2
  producers, 5.4% at 4, 42.9% at 8, 63.7% at 16.
- **Lost wakeups between a parked waiter and a departing peer** — the
  close handshake and the DATA/SPACE handshake have the same Dekker
  shape, and the arm-park re-check was not in the `SeqCst` total order.
  Both sides could sleep. The close flag is now `SeqCst` on both sides
  in spsc, mpsc, spmc, and mpmc, and every arm-park site carries the
  matching fence.
- **Data race on the async waker slot** — `WakerSlot` guarded an
  `UnsafeCell<Option<Waker>>` with a seqlock. A `Waker` is not
  trivially copyable, so the reader dereferenced a vtable pointer
  before it validated the sequence, and `store`'s drop of the previous
  waker raced that read. Miri reported the race on mpsc async
  push/pop. The waker now lives in an `AtomicPtr`: `store` and `wake`
  each swap the pointer once, so the thread that removes a pointer is
  its sole owner and is the only one that frees it.

### Changed
- **spmc slot completion metadata is compact** — the per-slot
  completion state moves into the existing sequence word.
- **Contended spmc claim batches are smaller** — this reduces the time
  a consumer holds positions that its peers wait for.

## [0.13.1] - 2026-08-13

### Fixed
- **`spsc::RingBuffer::split_borrowed` doc example** — the example
  borrowed the handles into `thread::scope` closures, but `spsc::Producer`
  holds a `Cell<usize>` and is therefore not `Sync`, so `&Producer` is not
  `Send`. The example failed to compile. The closures now take `move`,
  which is what the surrounding prose already described.

### Changed
- **Internal slot classification** — the `seq == pos * 2 + 1` /
  `TOMBSTONE` decode was repeated verbatim across `pop`, `pop_ref`,
  `drain`, and `drain_up_to` in both the mpsc and broadcast consumers.
  It now lives in one place, `common::SlotSnapshot::classify`, with
  `SeqSlot::classify` and `BroadcastSlot::classify` as the per-ring
  entry points. `common` is `pub(crate)`, so there is no public API
  change; the emitted work is the same single `Acquire` load plus the
  same comparisons.

## [0.12.0] - 2026-06-02

### Added
- **`broadcast::Producer::{push_block, reserve_block}`** — blocking
  producer API for the broadcast ring, mirroring the `push_block` /
  `reserve_block` already on spsc/spmc/mpsc/mpmc. Parks the calling
  thread (shared backoff schedule, then `thread::park`) while the ring
  is full — i.e. the slowest consumer hasn't advanced — and wakes when
  any consumer advances a head or drops. Returns `Err(val)` / `None`
  only when **all** consumers have been dropped (a push with no
  consumer would sit until overwritten, so that's treated as a closed
  channel). `ArcProducer` gains the matching `push_block` /
  `reserve_block` wrappers. Unlike `push`, the value is moved out and
  back on each retry, so `T` need not be `Clone`.
- **`spsc::RingBuffer::{producer_from_raw, consumer_from_raw}`** — `unsafe`
  constructors that reconstitute a `Producer` / `Consumer` from a
  `*const RingBuffer<T>`. This is the cross-address-space seam for memory
  shared between contexts that don't share a Rust allocator — e.g. a
  WebAssembly main thread and a Web Worker instantiated against the same
  `WebAssembly.Memory`, where neither `split` (`Arc`) nor `split_borrowed`
  (`&` lifetime) can bridge. The handle borrows the ring for `'static`; the
  caller must pin the ring for the program (e.g. `Box::leak`) and uphold the
  SPSC contract (exactly one producer and one consumer) across the boundary.
  See the safety docs on each method.

### Fixed
- **`mpmc::push_block` / `pop_block` saturated deadlock** —
  three independent fixes; all three are needed for the bench
  (`blocking_mpmc/block`: cap=16, 2P+2C, slow consumer) to run
  reliably:
  1. **Round-robin wake selection.** `WakeSet::wake_one` / `wake_n`
     used to always pick the lowest set bit, starving any
     higher-bit waiter when a low-bit waiter could be woken but
     not make progress. Selection now rotates through slots via
     a per-`WakeSet` cursor — every parked waiter gets a fair
     share of wake events. This was the dominant deadlock cause:
     producer at park slot 2 (empty batch, refill blocked) ate
     every consumer wake by re-parking, while producer at park
     slot 3 (holding the unpublished position the refill needed)
     stayed parked indefinitely.
  2. **Dekker SeqCst pairing.** `done.store` / `ready.store` are
     now SeqCst (not Release), and `WakeSet::wake_one` / `wake_n`
     start with a `SeqCst` fence — closes the classic Dekker race
     between publish-and-wake on one side and fetch_or-fence-recheck
     on the other.
  3. **`park_timeout(1ms)` backstop** in `push_block` / `pop_block`
     / `reserve_block` / `pop_ref_block`. Belt-and-suspenders:
     fast paths are unchanged (an unpark wakes immediately), the
     timeout caps any residual race at ~1ms.

  `mpmc::tests::async_push_pop_cross_thread_iters_saturated` is no
  longer `#[ignore]`.

## [0.10.0] - 2026-05-01

### Added
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
- **Bench coverage** for blocking, mpmc zero-copy, and zero-copy
  blocking APIs across all four rings.

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

### Packaging
- Tight `include` allowlist in Cargo.toml — `benches/` and
  `examples/` no longer ship in the published `.crate`. Package
  shrinks from 47 files / 111.4 KiB compressed to 27 files /
  86.7 KiB compressed.

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

[Unreleased]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/42Pupusas/quetzalcoatl/compare/v0.6.0...v0.7.0
[0.1.0]: https://github.com/42Pupusas/quetzalcoatl/releases/tag/v0.1.0
