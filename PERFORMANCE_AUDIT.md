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

### The pin, proven without threads

The stress numbers say misrouting happens; they do not show the
mechanism. Three deterministic tests do, with no timing to argue about:

`a_producer_holding_a_partial_batch_is_pinned_to_its_own_position`
builds a producer holding one reserved position, frees **three of the
ring's four slots**, and shows it still cannot proceed. Only its own
position releases it. A held `SlotReader` supplies the out-of-order
consumption that lets a batch span a blocked slot and a free one.

`a_pop_can_wake_the_one_producer_that_cannot_use_the_freed_slot` takes
the consequence: a `pop` frees one position, wakes one producer by park
slot, and the wake lands on the producer for which that position is
useless. Deleting just the `pop` makes the futile wake never appear, so
the counter fires from that wake and not incidentally.

`producers_blocked_by_a_full_ring_can_absorb_each_others_wakes` bounds
the claim. `refill_batch` returns `None` *before* touching the `claim`
cursor when the ring is full, so producers that park in that state hold
no reservation and are genuinely interchangeable. Misrouting is
harmless there. The pin needs a **partial batch**, which needs
out-of-order consumption to arise — which is why the residual rate is
low rather than constant.

What is proven: the pin is real, and a wake can be spent on a producer
that cannot use it. What is *not* proven: that this is what strands a
producer in the wild. The stress rescues remain too few to attribute.

### Sole-waiter rescues are intermittent and predate the recent changes

A `sole_waiter_rescue` — a waiter released by the timeout with no peer
parked to have taken its wake, the strongest available evidence of a
genuinely lost wake — appeared for the first time during the
position-targeted-wake experiment, which made that change the obvious
suspect. It is not: sole-waiter rescues have since occurred repeatedly
with the targeting reverted, roughly once every two or three
200-iteration runs, and did not reproduce on immediate re-runs of the
same build.

They are rare, they do not correlate with any change made so far, and
they are what `PARK_BACKSTOP` is currently catching. The backstop
cannot be removed while they occur.

#### Sharpening the instrument, and one thing it cannot do

"Was this waiter woken?" was answered by a single test: is its park
handle still armed. That test is too coarse, because
[`WakeSet::wake_one`](src/common/park.rs) clears a waiter's wake bit
**before** claiming its handle. A timeout firing between those two
steps finds an armed handle with the bit already gone.

[`WakeDelivery`](src/common/wake_delivery.rs) reads both and names the
states: `Delivered`, `InFlight` (bit gone, no unpark), `Untouched`, and
`Slotless` for a [`ParkSlot::Shared`] waiter, which publishes no bit,
arms no handle, and therefore times out by construction rather than by
defect. [`RescueEvidence`](src/mpmc/rescue_evidence.rs) accumulates
that over a park episode; observations never weaken, so a bit taken at
any park in the episode stays on record.

**The intended use of this split was wrong.** The plan was to subtract
the in-flight population from the lost-wake count as benign. It is not
benign: the crate's existing calibration test
`the_monitor_counts_a_wake_that_never_arrived` stages a *genuinely
lost* wake by clearing the waiter's bit so the publisher finds nobody
to unpark — which leaves exactly the armed-handle-with-cleared-bit
signature. A stolen wake and a late one are indistinguishable from the
waiter's side.

So `bit_taken_rescues` is documented as a weaker suspicion rather than
an exoneration, the stress assertion stays on the raw
`sole_waiter_rescues`, and the calibration test now asserts
`bit_taken_rescues == 1` so the overlap is pinned in place and cannot
be quietly reinterpreted as a filter later.

**No verdict on the rescues themselves.** Four consecutive
200-iteration runs after this work produced `rescues=0`, so the
sharpened instrument has not yet observed the event it was built for.
The rate quoted above (~1 per two or three runs) came from runs that
also hit the assertion and aborted early, so it is an estimate from a
small and biased sample, not a measured frequency. Slotless waiters are
ruled out for this harness specifically: it uses two producers and two
consumers against 64 park slots, so every waiter holds a lease.

### Position-targeted wakes: built, measured, rejected

The obvious fix follows from the pin: have a waiter publish the
position it is blocked on, and have a waker that frees one specific
position wake *that* waiter rather than picking one by rotation. It was
built — a table of awaited positions beside the wake bitmap, targeting
with a fallback to rotation when nobody is pinned, producers reporting
the lowest position in their batch, and both `Consumer::pop` and
`SlotRelease` routing by the position they free.

It is not in the tree. Measured against baseline on
`block_stress_diagnostic`:

| | futile wakes | of which producer | unwoken timeouts |
|---|---|---|---|
| baseline, run 1 | 46 | 9 | 5 |
| baseline, run 2 | 51 | 13 | 0 |
| targeted | 28 | 8 | 1 |

Producer futile wakes are the only figure targeting can move, and 8
against a baseline of 9 and 13 is noise. The drop in the total is
consumer-side variance, not an effect of the change.

Splitting the counter by side is what showed this, and it is the
finding worth keeping: **roughly 80% of futile wakes are consumers**,
which no amount of position routing can address, because a consumer
scans for any published slot and is genuinely interchangeable. The
misrouting story explains a real mechanism but a small share of the
observed events.

One run of the targeted build also produced a `sole_waiter_rescue`, the
first this campaign has ever seen, which did not reproduce. Whether the
targeting introduced it or merely perturbed the timing was not
established — another reason not to keep an unproven change on the park
path.

The negative result redirects the question: before routing wakes
better, find out what the *consumer* futile wakes are. A woken consumer
that finds nothing has usually lost the slot to a peer that claimed it
first, which is ordinary contention rather than a lost wake, but that
is a hypothesis and not yet a measurement.

### The scan budget a single lost CAS could exhaust

Asking what the consumer futile wakes are led to a real defect, found
by reading `claim_slot` rather than by the counter that prompted the
reading.

A lap of the ring was budgeted at `cap` iterations, but a lost CAS was
charged `CAS_FAIL_SKIP - 1` of them — 128 by default. On any ring
smaller than the skip, **one lost CAS ended the lap**, so the consumer
reported the ring empty while published items sat in slots it had never
examined, and `pop_block` parked on a ring with work in it. The stress
ring is `cap = 16`, so this was the governing case there, not an edge
one.

The two quantities were conflated: how far to jump clear of a contended
cache line, and how much of a bounded search that costs. A jump is only
meaningful modulo the ring — 128 positions on a ring of 16 lands back
on the same slot. [`ScanBudget`](src/mpmc/scan_budget.rs) now owns
both.

**The first fix was off by one and did not fix it.** It clamped the
skip to `cap - 1` and charged that against the lap, and the call site
then charged one more for the step every examined position pays. On
the stress ring that is 15 + 1 = 16: a single lost CAS still ended the
lap. This is why "it did not reduce blind futile wakes" was the result
— the fix had not engaged. The unit test that pinned the first version
passed because it checked the skip alone, not the skip followed by the
step the caller always makes.

The corrected accounting charges a lost CAS **one examination**,
whichever distance it jumps: the lap bounds the search in slots looked
at, and a lost CAS is one slot looked at. The jump is still clamped to
`cap - 1`, which is coprime with the power-of-two capacity, so a lap
of lost CASes walks every slot rather than the one it lost. The call
site no longer steps after a skip. Tests now cover the lap surviving
`cap - 1` lost CASes and ending on the `cap`th, and the clamped walk
visiting every slot.

With that in place the blind rate over 2000-iteration campaigns is
21–36 per 2000, against a futile total of ~750–830, and the
*sole-waiter* rescues split by timing (next section) show `blind=0` in
every campaign. The scan defect is no longer producing the residual.

### What the blind-futile counter can and cannot say

`blind_futile_wakes` counts a futile wake where work was still visible
when the waiter re-parked, to separate "nothing was there" from
"something was there and I missed it". Split by side, blind wakes are
**almost entirely consumer-side**: `producer_blind_futile_wakes` was 0
across three consecutive runs before a fourth produced exactly one.

Its limit was found by a test that failed. The sample is one more
unsynchronised look at a ring other threads are still working, so an
item published between the failed re-check and the sample reads as
blindness when nothing was missed — which is exactly what the staged
test hit. The counter is sound as a *rate compared across a policy
change* and unsound as a per-event defect count. The staged test was
deleted rather than weakened; the calibration that survives is in
`backstop_monitor`'s own tests, which drive the counter logic directly
with no ring and no threads.

The side split is lopsided rather than absolute. Blind wakes are
overwhelmingly consumer-side — three consecutive 200-iteration runs
gave `producer_blind` of 0, 0 and 0 — but a fourth produced a single
one, so "only consumers can be blind" is false and the mechanism is not
exclusive to the scan.

### The residual, bracketed and explained

The harness no longer aborts on a sole-waiter rescue; it accumulates
`BackstopStats` across iterations and prints totals, and
`QUETZALCOATL_STRESS_ITERS` scales it up. 2000 iterations is ~200 s in
release and is the campaign size below.

**Rate.** Before the changes in this section: 3, 5 and 7 sole-waiter
rescues per 2000 iterations, on 51–116 unwoken timeouts. Both sides
produce them (producer 0–3, consumer 2–5 per campaign).

**When the work arrived.** A rescue is "timeout, then work found". That
says nothing about *when* the work appeared, so two samples now bracket
the sleep — one just before the park, one the instant it returns — and
[`RescueTiming`](src/mpmc/rescue_evidence.rs) names the three answers:

- *Blind*: visible before the park. The pre-park re-check missed it; no
  wake was owed. A scan defect.
- *Late*: not visible when the park returned. The work arrived after the
  sleep ended; the timeout rescued nothing and merely preceded it.
- *Waiting*: absent before, present at return. Published during the
  sleep, and the waiter was not woken for it. This is the only timing a
  lost wake can produce.

The first bracketed campaign: `blind=0 late=4 waiting=3`. Blind is
zero, so the scan is not the residual. Late is the majority, and those
are not lost wakes. `waiting=3` is what remained to explain.

**Waiting, explained.** A publisher stores the item, then loads the
wake bitmap and unparks. A timeout that fires between those two steps
finds the item present and the bit untouched — the exact `Waiting`
signature — with the wake nanoseconds away. The instrument now holds a
park that matches the residual's signature for a
[`LATE_WAKE_GRACE`](src/mpmc/backstop_monitor.rs) of 5 ms, watching
for a waker to claim its handle. A claim within the grace proves the
wake was in flight; the clock beat it. Two calibration tests pin the
grace both ways: a 200 µs-late waker is caught, and a wake that never
comes is still counted as the residual.

Two 2000-iteration campaigns with the grace in place:

| | sole | blind | late | waiting | wakes after timeout | unwoken timeouts |
|---|---|---|---|---|---|---|
| run 1 | 3 | 0 | 3 | **0** | 9 | 141 |
| run 2 | 0 | 0 | 0 | **0** | 4 | 100 |

Every park that matched the residual's signature received its wake
inside the grace — 13 of 13. `waiting` is zero in both.

**That conclusion was wrong.** The paragraph that stood here read the
grace result as "every rescue was a late wake, the bound can go". The
bound was removed and the same campaign hung at iteration 241 with
all four threads parked. The instrument had measured exactly what it
said — a wake *did* arrive within 5 ms — but the wake it caught was
the *other* producer's timeout-and-futile-wake cycle bouncing a wake
back, not a publisher a few instructions behind the clock. With the
bound gone there is no cycle, and the routing defect below is a hang.
The lesson is the one already written under "Position-targeted
wakes": a benign explanation that fits the numbers is not the same as
the mechanism, and a timeout on the path being measured makes the two
indistinguishable.

### The deadlock, and the routing that fixes it

Removing `PARK_BACKSTOP` turned the residual into a reproducible hang:
2000-iteration campaigns stalled at iterations 241, 50 and 37. The
harness dumps the ring on a stall, and the decoded snapshots all
showed the same shape. From the third:

```text
claim = 4441
slot 9:  ready = Claimed(4409)   done = 4425   → free for position 4425
every other slot: consumed through 4440, free for its next round
producer_park = 0x6   consumer_park = 0x6
awaited = [(1, None), (2, Some((4425, 1)))]
```

The producer at park slot 2 holds position 4425 in its batch. Slot 9
is free for 4425. The producer is parked anyway, and the other
producer is parked on a refill that needs `done[claim & 15]`, which is
the same slot. Both consumers are parked because nothing is published.
Nobody will ever publish 4425, and no further release will ever
happen.

This is the mechanism the audit had already proved and set aside
("Position-targeted wakes: built, measured, rejected"). A release of
slot 9 issued one `wake_one`. Round-robin served the other producer,
which could not use position 4425, re-checked, and re-parked. The
producer that reserved 4425 was never woken, because nothing else ever
released. Under the 1 ms bound, that producer woke itself and found
its slot — a "rescue" — which is what the counters had been reporting
all along.

The fix is the routing that was built and rejected at `72b221f`, and
the rejection was the error: it was measured by futile-wake counts
under a bound that hid the very hang the routing prevents.
[`AwaitedBatch`](src/mpmc/awaited_batch.rs) publishes a parked
producer's `(batch_start, batch_unused)` beside its park slot, and
`WakeSet::wake_one_wanting` walks the parked bitmap for a slot whose
announcement contains the freed position before falling back to
round-robin. Every release site routes: `pop`, `SlotReader::drop`,
`drain` (now per item rather than `wake_n` at the end — a batch of
round-robin wakes could spend every one on producers that cannot use
them), and `BatchAbandon`.

The first version treated a producer waiting to *refill* as a taker
for every release. That hung at iteration 50 with the refilling
producer served first, unable to refill, and re-parked — the same
deadlock with the roles swapped. A refill needs the slot at the claim
cursor specifically, which a release elsewhere does nothing for, so a
refilling producer announces nothing and is served only by the
fallback.

Pinned deterministically by
`a_release_wakes_the_producer_that_reserved_the_position`: two
producers parked on different positions, the round-robin cursor
pointed at the wrong one, one release. Under round-robin the test
hangs; it polls rather than joins so a regression fails instead of
stalling.

**Result.** 2000 iterations of `block_stress_diagnostic` with no bound
on the park: complete, no stall. `unwoken_timeouts` fell from ~100 per
2000 to 23, all of them spurious unparks or `Shared` re-checks since
there is no timeout. The one sole-waiter rescue in the campaign was
`late` — work arrived after the park returned — which is not a lost
wake. Every ignored mpmc matrix test, the heavy and counter-free
suites, and both async stress shapes pass. `blocking_mpmc` median
moved from ~1.3 ms to ~0.77 ms on the same host, since a producer no
longer sleeps a full millisecond for a slot that freed microseconds
after it parked.

`PARK_BACKSTOP` is gone. A parked mpmc waiter sleeps until a peer
wakes it. The instrument stays: with no timeout, `unwoken_timeouts`
should stay near zero and a non-zero `waiting_sole_waiter_rescues` is
a lost wake, not a latency artefact.

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
7. **Consumer blind futile wakes: resolved as far as the residual goes.** The scan-budget fix was off by one on its first attempt and did nothing; the corrected accounting (one examination per lost CAS) engaged, and `blind` sole-waiter rescues read zero in every bracketed campaign since. The blind *futile* count is still non-zero (21–36 per 2000) and still has the documented false-positive mode; it is a rate for comparing policies, not a defect count, and is no longer on the path to the backstop decision.

8. **`PARK_BACKSTOP` is removed; the deadlock it hid is fixed.** See "The deadlock, and the routing that fixes it". Remaining: (a) run the unbounded campaign on another host, since one box proves one scheduler; (b) the `Shared` park slot still polls at 1 ms by construction — a ring with more than 64 live producers or consumers on one side has waiters no wake can reach, and that is now the only timed park in mpmc; (c) `wake_one_wanting` is a linear walk of the parked bitmap with an announcement load per set bit — fine at 2–8 parked producers, unmeasured at 64.

9. Extend the Loom models past the leaf primitives to the ring publication and slot-reuse protocols, which no current model covers. Blocked on loom 0.7.2 being unable to decide the park handshake — it reports deadlocks for a protocol containing no crate code at all, as `common::park_handshake_model` documents and calibrates. Reduce that to a minimal repro and file it upstream.
10. Agree a release budget for steady-state throughput and tail latency. Preserve correctness guarantees; optimize measured overhead rather than reverting required ordering or claim validation.
