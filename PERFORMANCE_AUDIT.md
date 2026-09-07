# Audit: v0.14.0 → 42af95c

## Status

Performance work is screening only, and is not a sign-off. Seven
unwind-safety bugs were found by reading the reservation, drain,
consumer-release and teardown paths, reproduced with failing tests, and
fixed; see the changelog. The remaining correctness gaps below are still
open.

Baseline: Git tag v0.14.0 (1b21acf), not the published tarball. The changelog reports missing source files in that tarball; the Git checkout builds. Baseline worktree: `/tmp/quetzalcoatl-audit-v014`. Separate target directories: `/tmp/quetzalcoatl-audit-target-v014` and `/tmp/quetzalcoatl-audit-target-head`.

Host: AMD Ryzen 7 7730U, 8 cores / 16 logical CPUs. Cargo 1.97.1. Default optimized bench profile, default features. Bench sources are unchanged between revisions. No CPU affinity, frequency control, or host-idleness guarantee. No builds/tests ran concurrently with timing runs.

## Matched benchmark screening

Ran baseline then HEAD with 40 samples per case, followed by HEAD then baseline with 100 samples per case. These are independent process runs, not confidence intervals. Times below are median elapsed workload times; positive changes mean slower HEAD.

| Case | Baseline, 100 samples | HEAD, 100 samples | Time change |
|---|---:|---:|---:|
| SPSC spin | 2.116 ms | 2.041 ms | -3.5% |
| SPSC blocking | 2.357 ms | 1.598 ms | -32.2% |
| MPSC spin, 1 producer | 145.6 µs | 145.9 µs | +0.2% |
| MPSC spin, 2 producers | 244.6 µs | 256.1 µs | +4.7% |
| MPSC spin, 4 producers | 354.0 µs | 444.2 µs | +25.5% |
| MPSC spin, 8 producers | 722.6 µs | 866.1 µs | +19.9% |
| MPSC spin, 12 producers | 1.205 ms | 1.365 ms | +13.3% |
| MPSC spin, 16 producers | 1.734 ms | 2.004 ms | +15.6% |

The first pair also showed MPSC spin slowdowns at 8, 12, and 16 producers: approximately +8%, +15%, and +11% elapsed time. The 4-producer result changed direction between pairs. Blocking MPSC varied substantially: at 16 producers the first pair showed +56% time, the second +2%. Do not treat the first blocking result as a confirmed regression.

`benches/harness/mpsc.rs` includes ring allocation, endpoint cloning, thread creation, joins, and teardown inside the timed operation. Each producer sends only 5,000 items; there is no synchronized start. These results do not isolate steady-state push/pop costs. At 16 producers plus the consumer, the workload exceeds the host's logical CPU count.

Reproduce each matched pass:

```text
cargo bench --manifest-path <revision>/Cargo.toml --target-dir <separate-target> --bench matched -- quetzalcoatl --sample-count 100 --max-time 1 --color never
```

## Other topology screening

One baseline→HEAD pair, 40 samples, default features. Requires repeated runs before conclusions.

| Case | Baseline median | HEAD median | Time change |
|---|---:|---:|---:|
| MPMC blocking | 793.1 µs | 851.4 µs | +7.3% |
| MPMC spin | 830.9 µs | 877.8 µs | +5.6% |
| SPMC blocking | 533.5 µs | 513.3 µs | -3.8% |
| SPMC spin | 670.7 µs | 589.7 µs | -12.1% |
| Broadcast consumer scaling, 1 | 94.45 µs | 115.1 µs | +21.9% |
| Broadcast consumer scaling, 4 | 136.8 µs | 195.5 µs | +42.9% |
| Broadcast consumer scaling, 8 | 360.1 µs | 490.8 µs | +36.3% |
| Broadcast producer scaling, 1 | 64.96 µs | 114.2 µs | +75.8% |
| Broadcast producer scaling, 8 | 2.700 ms | 1.056 ms | -60.9% |

Broadcast results have large variation and mixed direction. Prioritize its claim-validation and consumer-floor paths, but do not attribute these timings to a specific fix yet.

```text
cargo bench --manifest-path <revision>/Cargo.toml --target-dir <separate-target> --bench broadcast --bench mpmc --bench spmc -- consumer_scaling producer_scaling blocking_mpmc blocking_spmc --sample-count 40 --max-time 0.5 --color never
```

## Fixed during this audit

Both bugs live on the reservation paths, are invisible to Miri (nothing
is undefined — the ring stops making progress), and each was pinned by a
test confirmed to fail before the fix.

1. **A panicking `T::drop` skipped the slot release** in mpsc, broadcast
   and mpmc, because the release was gated on a value returned only
   after the destructor had run.
2. **A panicking waker in `commit_unchecked`** ran the writer's rollback
   over an already-published position, losing the committed value.
3. **A panicking drain callback** skipped the batched producer wake in
   spmc and mpmc while leaving the slots freed — a permanent hang for
   spmc's untimed `push_block` and for mpmc's `push_async`, which has no
   backstop timeout.
4. **A panicking `T::drop` under an spmc `SlotReader`** skipped the slot
   release, losing the position for the life of the ring.
5. **A panicking `T::clone` in `broadcast::Consumer::pop`** skipped the
   head advance, leaving the consumer re-reading the same position
   forever and pinning `min_head` against the producer.
6. **A panicking `T::drop` during spsc `Consumer::drop`** skipped the
   close, so the ring looked open for as long as it lived.
7. **The same during spmc `Consumer::drop`** skipped the park-slot
   release and the live-count decrement, so the ring never reported
   itself closed.

The endpoint destructors are now covered. `RingBuffer::drop` shares the
shape — one panicking value skips every later slot — but leaks rather
than stalls, which is the trade `std` makes for `Vec`; it is left as is.

Searching for the shape rather than the instance found all but the
first: a release, wake or cursor advance gated behind code that may
unwind. The user code that can unwind is not only `T::drop` — it is also
`T::clone`, the drain callback, and any `Waker`.

The fix each time is the same RAII shape the codebase already used in
`SlotRelease`, `BatchRelease` and `HeadPublisher`; the gaps were the
places that had not adopted it. No ring was uniformly ahead: spsc and
mpsc already guarded the reader paths, mpsc alone ordered its teardown
close before the drain, and spsc needed the same fix as spmc there.

Ordering is sometimes the whole fix. mpsc closes before it drains and
was correct for free; spsc and spmc drain first, which they must, since
a producer woken by the close has to find the freed positions already
published — so they need the guard instead.

Several first-draft tests passed while the bug was present — the mpmc
async one because `await` re-polls and finds the space regardless of any
wake, the spmc reader one because the neighbouring double-drop assertion
is satisfied by teardown skipping the slot. A test that cannot fail
proves nothing; each was made to fail first.

The benchmark tables above predate these fixes. Re-running `matched`
afterwards stayed within the run-to-run variation already documented,
which is expected: both fixes reorder reservation-drop paths and leave
push/pop untouched.

## The backstop measurement

Disabling `PARK_BACKSTOP` (raising it to 30s) and running
`block_stress_diagnostic` for 400 iterations produced no hang, which
looked like evidence the bound was unnecessary. It was not: the same
workload instrumented reported two backstop rescues in one iteration of
400, so wakes were still going missing — the runs simply did not lose
one while the bound was off.

That is the argument for measuring rather than sampling. A rare race
absent from 400 runs is not an absent race, and the cost of being wrong
here is a permanently hung process.

Rescues are an upper bound on lost wakes rather than an exact count:
work can legitimately arrive during the timeout window, and `wake_one`
serves one slot per event in round-robin order, so a waiter can be
passed over rather than lost. A zero count across the matrix would be
strong evidence the bound can go; a non-zero one is a starting point.

## Checks completed on HEAD

- Default workspace tests: 493 passed, 43 ignored; 15 doctests passed.
- Async workspace tests: 544 passed, 46 ignored; 15 doctests passed.
- Release all-feature workspace tests: 544 passed, 46 ignored; 15 doctests passed.
- Default and all-feature workspace/all-target Clippy: passed with `-D warnings`.
- Miri is clean on the whole library in both feature modes (492 and 538 tests). It is available on the nightly toolchain, not the default stable one.
- All four Loom models pass under `RUSTFLAGS="--cfg loom"`. They cover the leaf async-wake primitives only, as their own module docs state.
- Linux perf counters are available. A HEAD MPSC spin/8 screening run worked, but counters wrapped Cargo and the entire benchmark process; they are not per-item costs.
- `cargo graph --manifest-path ... --report` returned zero types. This is not a valid structural-health result; resolve its source-root invocation before relying on it. Doctor likewise checked the home directory rather than the repository; Cargo commands here used explicit manifest paths.

Ignored stress tests, package verification, and a complete feature/build matrix have not been run in this audit. Passing ordinary tests is not proof of concurrency correctness.

## Next steps, in priority order

1. Add a Rust steady-state harness with persistent workers, synchronized starts, separately measured lifecycle costs, configurable CPU affinity, verified item counts/checksums, and repeated alternating revision runs. Record compiler/flags, CPU placement, and host conditions.
2. Reproduce MPSC spin/8–16 and broadcast regressions with that harness. Bisect fixes versus subsequent owner-extraction refactors; inspect cross-crate generated code and cache-line placement before changing synchronization.
3. Cover all five topologies: push/pop, reserve/commit/pop_ref, drain, saturation, empty-to-nonempty wake latency, endpoint churn, and cancellation. Include async on/off and >64 live waiters for overflow paths.
4. Run the ignored stress tests with timeouts, package verification, and the remaining build/test/Clippy feature combinations.
5. Consider a debug-only assertion, or a test helper, that fails when a release/wake/close is reachable only through code that may unwind. Seven instances of one shape were found by reading; the eighth will not be.
6. **Find the cause of the mpmc missed wake.** The `backstop-metrics` feature now measures it: it counts parks that ended on the `PARK_BACKSTOP` timeout rather than a peer's wake, and among those, the ones whose next re-check found work already available. That second count is a *rescue* — the waiter was waiting for something already there. `block_stress_diagnostic` reported two rescues in one iteration of 400, so the bug is still live, roughly a thousand times rarer than the original 3% but not gone. `PARK_BACKSTOP` therefore stays: removing it turns each rescue into a permanent hang. Instrument the rescue path to capture ring state at the timeout, as `block_stress_diagnostic` already does for the deadlock.
7. Extend the Loom models past the leaf primitives to the ring publication and slot-reuse protocols, which no current model covers.
8. Agree a release budget for steady-state throughput and tail latency. Preserve correctness guarantees; optimize measured overhead rather than reverting required ordering or claim validation.
