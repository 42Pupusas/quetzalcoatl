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

## The lost wake, found

A set bit in the wake bitmap did not imply a wakeable waiter, and
`wake_one` treated the two as the same thing.

`arm` publishes the handle first and the bit second; a waker clears the
bit first and claims the handle second. The pair is not updated
atomically, so a bit can outlive the handle it advertises:

1. Waiter `w` arms bit 0 and parks.
2. Peer `p1` clears bit 0, taking ownership of that wake, and is
   descheduled before claiming the handle.
3. `w`'s `PARK_BACKSTOP` expires on its own. It re-arms — new handle,
   bit 0 set again.
4. `p1` resumes and claims that handle. Bit 0 is now set with nothing
   behind it.
5. Peer `p2` publishes work and calls `wake_one`. It picks bit 0,
   clears it, finds no handle, and returns having woken nobody. A
   waiter parked on another bit stays asleep on work that is ready.

`ThreadParker::wake` already returned `bool` for exactly this, and
`wake_one` discarded it. The fix is to keep searching when the return
is `false`: a retired stale bit is not a delivered wake. `wake_n` had
the same defect, plus a related one — it counted loop iterations rather
than unparks, so a stale bit consumed one of the `n` a drain owed to
real waiters.

The mechanism is self-reinforcing, which explains the rarity and why
the backstop masked it so well: step 3 *requires* a backstop timeout,
so the bug needs a previous near-miss to set up the next one.

Three unit tests reproduce it deterministically, single-threaded, with
no timing dependence (`a_wake_is_not_consumed_by_a_slot_whose_handle
_is_already_claimed` and neighbours in `common::park`). All three fail
on the previous code.

This is not yet grounds for removing `PARK_BACKSTOP`. It is one
confirmed defect on the path, and the stress matrix has since been
clean, but "no rescue observed" is the same evidence that was
misleading before. The bound comes out only after a long instrumented
campaign with a rescue count of zero.

## Futile wakes

Rescues were repeatedly dismissed as "round-robin explains it": the
wake reached another waiter rather than being lost. That excuse was
never tested, and it hides an assumption that is false here — that the
other waiter could use the wake.

mpmc producers are not interchangeable. Each parks holding a batch of
*specific* reserved positions from the `claim` cursor, and `DoneWord`
frees a slot for one exact position, so a producer woken for a position
outside its batch cannot use it and re-parks. `wake_one` delivers one
unpark per publish, so that publish is spent and the producer that was
waiting on the position gets nothing. Round-robin does not fix this; it
only changes who is passed over.

`futile_wakes` measures it: a park a peer's wake genuinely ended, whose
waiter then found no work and parked again. Two calibrations bracket
the counter — one stages a futile wake, one confirms a usable wake is
not counted.

Measured on `block_stress_diagnostic` (200 iterations, 2 producers x
5000 items, cap 16):

| | rescues | futile wakes | iterations with futile wakes |
|---|---|---|---|
| `wake_one` | 2 | ~45 | 30 / 200 |
| `flush` (experiment) | 0 | ~50 | 28 / 200 |

Two findings. Futile wakes are **common** — tens per run, not a rare
race — so wake routing is measurably lossy under saturation even when
nothing hangs. And both rescues in the `wake_one` run co-occurred with
a futile wake in the same iteration.

That co-occurrence is suggestive, not causal. Waking every producer
instead of one dropped rescues from 2 to 0, but the run had only 3
unwoken timeouts against 1, which is far too few events to separate the
effect from noise. The experiment was reverted. Establishing the link
needs thousands of iterations at both settings, comparing rescues per
unwoken timeout rather than per run.

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
6. **Decide whether `PARK_BACKSTOP` can go.** One lost-wake defect is found and fixed (see "The lost wake, found"): `wake_one` and `wake_n` consumed wakes on bits whose handles had already been claimed, waking nobody. Three deterministic unit tests cover it. What remains is to establish whether it was the *only* one — run `block_stress_diagnostic` under `backstop-metrics` for thousands of iterations across the matrix, and drop the bound only on a sustained zero rescue count. Note the defect needed a backstop timeout to arm itself, so its removal may change the rate of anything left rather than leaving it fixed. Loom cannot help here: see `common::park_handshake_model`.
7. **Settle whether futile wakes cause the residual rescues.** `futile_wakes` shows wake routing is lossy under saturation (see "Futile wakes"), and every rescue observed since co-occurred with one, but the event counts are far too small to call it causal. Run both wake policies for thousands of iterations and compare rescues *per unwoken timeout*. If the link holds, the fix is to route wakes by what a waiter is waiting for rather than by park slot — which means the wake bitmap needs to carry the awaited position, not just "someone is parked here." That is a protocol change, so measure first.
8. Extend the Loom models past the leaf primitives to the ring publication and slot-reuse protocols, which no current model covers. Blocked on loom 0.7.2 being unable to decide the park handshake — it reports deadlocks for a protocol containing no crate code at all, as `common::park_handshake_model` documents and calibrates. Reduce that to a minimal repro and file it upstream.
9. Agree a release budget for steady-state throughput and tail latency. Preserve correctness guarantees; optimize measured overhead rather than reverting required ordering or claim validation.
