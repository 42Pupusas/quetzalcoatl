# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **A panicking `T::drop` during consumer teardown left the ring
  looking open.** spsc's `Consumer::drop` drains the backlog before it
  closes, and spmc's drops its unread batch before it releases the park
  slot and decrements the live count. Those destructors are user code:
  unwinding past the close left `consumer_closed` unset (spsc) or the
  live count permanently high (spmc), so a producer never learned its
  peer was gone — `push_block` parks untimed, so it waited forever.

  The close now runs from a `ConsumerClose` guard armed before the
  drain, in both rings. The drain-then-close order is preserved, since
  a producer woken by the close must find the freed positions already
  published. mpsc was already correct: it closes *before* draining.

  `RingBuffer::drop` has the same shape — one panicking value skips
  every later slot — but the consequence is a leak, not a stall, and
  `std` makes the same trade for `Vec`. Left as is.

- **A panicking `T::drop` under an spmc `SlotReader` lost the slot.**
  `SlotReader::drop` ran the value's destructor and only then stored
  `done`, so an unwind in between never handed the position back. The
  producer could not reuse it for the life of the ring, and one blocked
  on it never woke, since `push_block` parks untimed. The release now
  runs from an `spmc::SlotRelease` guard armed before the destructor,
  matching mpsc and mpmc, which already had one. spsc was already
  correct through `HeadPublisher`.

  The neighbouring double-drop test passed throughout: `head` has
  already advanced by then, so teardown skips the slot and the value is
  not dropped twice. Only the producer's view was broken.

- **A panicking `T::clone` in `broadcast::Consumer::pop` stranded the
  consumer.** `pop` clones the value out of the slot and then advances
  the head. `Clone` is user code; unwinding past the advance left the
  head on the position just read, so the next `pop` re-read the same
  value and panicked again — forever. The consumer never reached a
  later value, and its stale head held `min_head` down so the producer
  could not reclaim the capacity either.

  The advance now runs from a `HeadAdvance` guard armed before the
  clone. This matches what `pop_ref` already did, whose `SlotReader`
  advances whether or not the borrower panicked, and it costs nothing:
  a broadcast consumer never owns the value, so there is no destructor
  to order against. broadcast is the only ring whose `pop` clones; the
  others move the value out.

- **A panicking drain callback left producers waiting on space it had
  already freed.** `drain` and `drain_up_to` release each slot inside
  their loop but wake the producers once at the end, which is what makes
  the batched `wake_n` worth having. Unwinding out of the user callback
  skipped that wake while leaving the slots freed, so a producer blocked
  on a full ring was never told the ring had space.

  In spmc the consequence was a permanent hang: `push_block` parks
  untimed, so nothing rescued the producer. In mpmc the blocking path
  was saved by the `PARK_BACKSTOP` timeout and merely stalled, but
  `push_async` has no backstop — its waker was never invoked and the
  task was never rescheduled. Both now own the wake in a `DrainWake`
  guard that fires from its destructor.

  spsc and mpsc were already correct here, through `HeadPublisher` and
  `BatchRelease`; they have tests now to keep them that way. The mpmc
  test asserts on the wake count rather than on a later `await`, which
  would re-poll, find the space, and pass either way.

- **A panicking payload destructor stranded its reservation.**
  `WrittenSlot::drop` asked `UncommittedSlot::drop_if_uncommitted()`
  whether it still owed the slot release — but that call runs `T::drop`
  first and only then returns. Unwinding out of the destructor skipped
  the release entirely, in all three rings that owe one: mpsc never
  tombstoned the position, broadcast never published its abandonment
  marker, and mpmc never returned the bit to the producer's batch.

  The consequence was lost progress, not lost memory. An mpsc consumer
  stopped at the unresolved position and every later value behind it
  became unreachable; the same for a broadcast consumer; an mpmc
  producer lost the position for the rest of the handle's life. Miri
  saw nothing, because nothing here is undefined — the ring simply
  stops.

  Each ring now arms its release *before* running the destructor, in
  `mpsc::ReservationRelease`, `broadcast::ReservationAbandon` and
  `mpmc::ReservationReturn` — the same RAII shape the consumer side
  already used for `SlotRelease`. `UncommittedSlot::is_armed` is what
  lets the decision be made ahead of the drop, and its docs now say why
  the returned flag cannot be used for this.

  The mpmc test needs the ring to wrap through the lost position before
  it fails: with spare batch positions available, a single reserve
  after the panic still succeeds and the bug hides.

- **A panicking waker in `commit_unchecked` destroyed a published
  value.** The method published the slot, woke the consumer, and only
  then called `mem::forget(self)`. A `Waker` is user code and may
  panic — an unwind between the publish and the forget ran
  `SlotWriter::drop`, which tombstoned (mpsc), abandoned (broadcast) or
  reclaimed (mpmc) a position the consumer could already see. In mpsc
  the committed value was silently lost: the consumer read `None`.

  The writer is now disarmed before publishing, which is the ordering
  the safe `commit` path already documented and enforced. spsc and
  spmc were unaffected — their `SlotWriter::drop` has nothing to undo.

- **The published crate could not be compiled.** `Cargo.toml`'s
  `include` listed `src/common` file by file, and nine modules declared
  in `common/mod.rs` were missing from it, so the 0.14.0 tarball failed
  with nine `file not found for module` errors. Every source directory
  is now matched by glob. `cargo package` reproduces the failure and
  verifies the fix, since it builds the tarball it produces.

### Changed
- **The wake bitmap is tested directly.**
  `WakeSet` is the park/unpark bitmap every topology waits on, and it
  had no test module — it was covered only through the blocking tests
  of the four rings, which exercise it incidentally and cannot isolate
  a wake-routing fault from a ring bug. Fourteen tests now pin it,
  including the starvation case its round-robin cursor exists to fix:
  a waiter at a low slot that re-parks immediately must not consume
  every wake and leave a higher slot parked forever. That test was
  confirmed to fail (and only it) with the cursor pinned to zero.

- **The Arc broadcast facade is tested where it lives.**
  `broadcast/arc.rs` had no test module; seven tests for it sat in
  `broadcast/mod.rs` and covered `push`, `pop`, `pop_ref` and
  `reserve`. The blocking and async methods, `slot_mut` /
  `commit_unchecked`, every `len` / `is_empty` / `is_full`,
  `ArcProducer::clone`, and the `Arc::try_unwrap` that recovers the
  value from a rejected push were all unreached. The seven move into
  the file they test and 24 now cover the facade, all of them passing
  under Miri.

- **mpmc's consumed watermark has an owner.**
  Consumers count their pops privately and publish the batch every
  `CONSUMED_FLUSH`; producers subtract the published total from the
  claim cursor to size their next reservation. The two halves sat in
  three files with the wrapping subtraction spelled out at the use
  site. `ConsumedWatermark` and `ConsumedTally` in
  `mpmc/consumed_watermark.rs` now own them, and the subtraction
  becomes `free()`, returning `None` for "the watermark accounts for
  the whole ring" — an estimate the producer rechecks per slot, since
  the watermark only ever understates progress. 15 tests, and the
  shared atomic becomes private.

- **The broadcast consumer-floor cache has an owner.**
  A producer answers "may I write this position?" from a private
  `Cell`, then a shared atomic, then a full registry scan. The three
  levels were spread across `mod.rs` (the atomic, cache-padded and
  `pub(crate)`) and `producer.rs` (the `Cell` and the escalation), with
  no test reaching them directly. `SharedFloor` and `FloorCache` in
  `broadcast/floor_cache.rs` now own them, including the `fetch_max`
  that keeps the shared value monotonic and the `any_subscribed` gate
  that stops a cache seeded at 0 from permitting the first `cap`
  positions before any consumer exists. 12 tests, `Producer` shrinks by
  a field and three methods, and the shared atomic becomes private.

- **mpmc's compile-time tuning moved out of `mpmc/mod.rs`.**
  `Config`, `DefaultConfig`, `Cfg` and `ConfigBounds` now live in
  `mpmc/config.rs`; the public paths are unchanged (`mpmc::Config`
  etc. are re-exported). `RingBuffer::scan_unused` — a producer's walk
  over its own batch bitmap, with the ring as the only thing it did
  not own — became a `Producer` method beside its single caller.
  `mpmc/mod.rs` loses ~120 lines and is left holding the ring: its
  fields, the slot accessors its four sibling modules share, and
  teardown.

- **`ConsumerRegistry` owns the broadcast consumer table.**
  `ConsumerSlot` was declared inline in `broadcast/mod.rs` with both
  fields `pub(crate)`, and its protocol was spread across three files:
  `mod.rs` CAS'd the active flag and scanned for the slowest head,
  `consumer.rs` loaded and stored heads through the raw slot array at
  six sites, and `producer.rs` answered "is anyone still listening?"
  from a free function walking the array.

  The registry now owns the seats and the questions asked of them:
  `subscribe_first` / `subscribe_at` / `unsubscribe` / `head_of` /
  `publish_head_past` / `floor` / `any_subscribed`. `ConsumerSlot`'s
  fields drop to private — nothing outside broadcast used them.

  The asm probe caught a real regression in the first cut: the
  registry's accessors carry no generics, so without `#[inline]` LLVM
  would not pull the single-load `head_of` or the single-store
  `publish_head_past` across the crate boundary, and the consumer hot
  path gained two real calls per lap. Generic code (like `SequenceWord`'s
  methods) inlines across crates by default; these needed the attribute
  spelled out. With it, push/pop/pop_ref/reserve+commit are again
  byte-identical to 598f63e (20/76/91/31 instructions).

- **`SequenceWord` owns the broadcast slot's sequence word.**
  `SlotState` could already decode the word, but the `AtomicUsize`
  itself was passed around raw — threaded through `SlotWriter` and
  `WrittenSlot` as a bare `&AtomicUsize`, with `store(published_word(pos),
  Release)` spelled out at three sites and `store(abandoned_word(pos),
  Release)` at two.

  This removed a genuine duplicate: `slot_reuse` declared its own
  `IN_PROGRESS = 0` for the value `slot_state` calls `VACANT`, the same
  constant defined twice in two modules with nothing keeping them
  equal. The claim check is now `is_claim_in_progress`, and the
  constant has one home.

- **`DoneWord` owns the SPMC slot's release word.** The consumer's
  `done[s] = pos + cap` was written out at five sites and the
  producer's matching `load(Acquire) == pos` at two, with the teardown
  check spelling the same comparison a third way. `SlotReader` no
  longer carries a loose `cap: usize` beside its raw pointer; it holds
  the `Capacity` and asks the word.

  This is the SPMC twin of the MPMC type, deliberately kept separate:
  MPMC stores `SeqCst` and SPMC stores `Release`, because each pairs
  with a different wake path. Merging them would mean picking one
  ordering for both.

- **`ReadyWord` and `DoneWord` own the MPMC slot's two words.** A slot
  is described by `ready[s]`, a round-tagged tri-state, and `done[s]`,
  the handshake from one round's consumer to the next round's producer.
  Both encodings were open-coded: the `done` release appeared at five
  sites and its matching read at three, and the `ready` word was decoded
  by hand in three places — the teardown path using a different formula
  from the consumer's, agreeing only because the state fits below the
  minimum capacity.

  `ReadyState` now names the three states, so `state == 1` is
  `is_published()` and the round arithmetic has one home. The words stay
  in separate arrays: `ready` is packed eight to a cache line for the
  consumer's scan, and merging them would cost that.

- **`SlotSequence` owns the MPSC slot's sequence word.** The word
  encodes free (`pos * 2`), published (`pos * 2 + 1`) and abandoned
  (`TOMBSTONE`), and each writer open-coded the arithmetic: the release
  transition `(head + capacity) * 2` appeared at seven call sites
  across three modules, each restating that consuming `head` frees the
  slot for `head + capacity`.

  Writers now go through `publish`, `tombstone`, `release` and
  `free_at`, and holders carry a `&SlotSequence` rather than a bare
  `&AtomicUsize` — which is what let the encoding leak in the first
  place. `TOMBSTONE` is no longer referenced outside the module that
  defines it.

- **`UncommittedSlot` owns the written-but-unpublished value.** Between
  `write` and `commit` a producer holds an initialized value no
  consumer can reach. All five rings tracked that with a `committed:
  bool` beside a raw pointer and an `if !self.committed { …
  drop_in_place() }` in `Drop`.

  The flag and the pointer are now one type. `WrittenSlot::commit`
  disarms it before publishing, and `Drop` asks
  `drop_if_uncommitted()`, whose return value says whether the ring
  still owes its own release — a tombstone in mpsc, an abandonment
  marker in broadcast, a returned batch bit in mpmc, and nothing in
  spsc/spmc, whose `Drop` impls are gone entirely.

  The ordering rule that makes a panicking waker safe (disarm first,
  publish second) was stated in five doc comments and enforced by
  none. It is now a property of the type each ring holds.

- **`Capacity` now owns ring indexing.** It already held `cap` and its
  `mask`, and enforced the power-of-two invariant that makes
  `pos & mask` a valid index. Every ring then unpacked it into two
  loose `usize` fields on construction and re-derived the indexing by
  hand, restating the bounds argument in a SAFETY comment at six
  sites.

  The rings now store the `Capacity` and call `index_of(pos)`. Both
  its fields are private, so the mask is no longer reachable outside
  the type that guarantees it.

  mpmc used the same mask for a second purpose — reducing a position
  *delta* modulo the capacity to decode a slot's tri-state — which is
  the same arithmetic carrying no bounds contract. That is now
  `wrap(delta)`, named apart from `index_of` so the two uses no longer
  read alike.

  Found on the way: spmc threaded a `mask` argument through three
  consumer functions that never used it (`bind_pos` took it as
  `_mask`). Removed.

- **`common` is split into one module per responsibility.** The module
  root held six unrelated things behind a single `mod.rs`: the slot
  sequence encoding, the ring's backing allocation, the cache-line
  padding, three blocking-loop traits, two test payload types, and a
  watchdog thread. They shared a file, not a subject.

  Now `seq_slot`, `aligned_buf`, `cache_padded`, `single_parker`,
  `drop_counter` and `progress_watchdog` each own one, and `mod.rs` is
  the module list plus the re-exports that keep every existing path
  working. Nothing moved in the public API.

  The pieces arrived with tests of their own: 24 for behaviour that
  was previously only exercised through the rings that used it, among
  them the first direct coverage of the slot encoding's central
  guarantee — that no two positions share a sequence value.

### Removed
- **Dead `consumer_count` field in the mpmc and spmc rings.** Both
  incremented it on every consumer clone and never read it: no load,
  no decrement, no use anywhere. Its doc comment said it assigned
  stable park-slot indices, which `ParkRegistry::lease` has done since
  the registry was introduced. Each clone paid an atomic RMW and a
  cache line for a number nothing consulted.

### Changed
- **Endpoint reference counts are now the `EndpointCount` type.** Five
  rings tracked how many handles remained on a side —
  `producer_count` in mpsc, mpmc and broadcast, `consumer_count_live`
  in spmc and mpmc — each as a bare `CachePadded<AtomicUsize>` with
  the protocol spelled out at every site: `fetch_add(1, Relaxed)` on
  clone, `fetch_sub(1, AcqRel) == 1` on drop to detect the last
  handle.

  The orderings are the reason this is a type rather than a
  convention. `Relaxed` is right for the increment and `AcqRel` is
  load-bearing for the decrement: the last endpoint closes the ring
  and wakes the peers, so the departing endpoints' writes have to be
  visible to it. Both facts now live in one documented place instead
  of being re-derived at ten call sites.

  `release` returns whether the caller was last and is `#[must_use]`,
  so decrementing the count while forgetting to close is no longer
  something the code can express.
- **BREAKING: `cas_backoff` is replaced by the `Backoff` type.** The
  free function took `&mut u32` and every caller kept that counter
  itself, which meant the schedule was only half of it. The other half
  was `if backoff < BACKOFF_PARK_THRESHOLD` before the call and
  `backoff = 0` after a park, written out at fourteen sites across
  five rings, with the threshold exported as a separate constant so
  the comparison could be spelled the same way each time.

  `Backoff` owns the counter and names the three operations: `spin`
  for a CAS retry that will never park, `spin_unless_exhausted` for
  the pre-park gate that spins and reports whether to continue, and
  `reset` after a park returns or a lap sees progress it cannot use.
  `BACKOFF_PARK_THRESHOLD` is gone; the threshold is now internal,
  since the comparison it existed for is no longer at the call sites.

  Code using it as a building block changes from
  `let mut b = 0u32; cas_backoff(&mut b)` to
  `let mut b = Backoff::new(); b.spin()`.
- **The head/tail cursor pair is now owned by `Cursors`.** spsc, mpsc
  and spmc each declared two `CachePadded<AtomicUsize>` cursors and
  then restated the same three derived operations against them:
  `len` as a pair of `Relaxed` loads and a wrapping subtraction,
  `is_empty`/`is_full` on top of it, and a drop-time non-atomic read
  of both cursors to find the positions still holding values. Those
  now live on the type, and `occupied()` returns the drop range as a
  `Range` behind `&mut self`, so the "every handle is gone, the
  cursors cannot move" precondition is the borrow checker's rather
  than a comment's.

  Publication stays with the rings, because they do not share one
  protocol: a cursor written by one thread is stored, one written by
  several is claimed by compare-exchange, and mpsc's tail and spmc's
  head are the multi-writer cases. The one publication that *is*
  uniform — the single-consumer head release, `SeqCst` in both spsc
  and mpsc for the same park-handshake reason — is
  `Cursors::publish_head`, replacing six hand-written stores. spsc's
  `mod.rs` no longer performs an atomic operation of its own.
- **Single-waiter park state is now owned by `SoleParker`.** The
  single-producer and single-consumer sides each carried a loose
  `ThreadParker` plus a `parked` flag, with the park protocol restated
  at every site that touched them, and a `SeqCst` fence that every
  caller had to remember to issue after arming. Arming now carries its
  own fence, so a site cannot forget it. The type takes the same
  `_park` field name as its multi-waiter twin `WakeSet`, so every side
  of every ring reads `producer_park` / `consumer_park` and the type
  says how many waiters the side admits. It also draws the same
  distinction `WakeSet` does between `wake` (fenced, for a caller that
  published with `Release` — MPSC, SPMC) and `wake_published`
  (fence-free, for a caller whose publish store is already `SeqCst` —
  SPSC), which was previously implicit in whether a given ring's
  hand-written `wake_*` happened to start with a fence.

- **BREAKING: `split_borrowed` now takes `&mut self`** on SPSC, MPSC and
  SPMC. It previously took `&self`, so safe code could call it twice on
  one ring and mint a second copy of a *single* endpoint — two SPSC
  producers *and* two SPSC consumers, two SPMC producers, or two MPSC
  consumers. Two producers each write from a private cursor, so both
  claim position 0 and `tail` advances past a slot neither initialized;
  the consumer then reads uninitialized memory as a `T`. With
  `T = String` this is confirmed undefined behavior under Miri, reachable
  with no `unsafe` in the calling code. `split(self)` already prevented
  this by consuming the ring; the exclusive borrow gives the borrowed
  split the same guarantee, checked at compile time. Callers add `mut` to
  the ring binding.
- **BREAKING: `RingBuffer::new_producer` (MPSC) and
  `RingBuffer::new_consumer` (SPMC) moved onto the handles** as
  [`Producer::new_producer`] and [`Consumer::new_consumer`]. The handles
  returned by `split_borrowed` borrow the ring for as long as they live,
  so with the `&mut self` split above the ring can no longer be borrowed
  again to create siblings. An existing handle already holds the shared
  reference and can hand out siblings with the same lifetime. Replace
  `ring.new_producer()` with `producer.new_producer()`, and
  `ring.new_consumer()` with `consumer.new_consumer()`. The many-side
  fan-out these enable is unchanged; only the single-writer/single-reader
  duplication is now rejected.
- **BREAKING: a broadcast buffer with no consumers now rejects pushes**
  instead of accepting and discarding them ("black hole" mode).
  `Producer::push` returns `Err(val)` and `Producer::reserve` returns
  `None` when no consumer is registered; the blocking and async variants
  already reported this as a closed channel. A consumer's progress is the
  only proof that a slot's previous occupant has been read, so with no
  consumer the previous behaviour let concurrent producers lap the ring
  and write the same slot — a data race, confirmed under Miri. Code that
  relied on pushing into a subscriber-less broadcast must now keep a
  consumer alive or handle the `Err`.
- **`RingBuffer::close` (SPSC, MPSC) documents what it actually does.**
  It claimed subsequent pushes are silently dropped; they succeed, and
  no push path ever consulted the flag. The method signals the consumer
  side. Callers who need pushes to fail should drop the consumer, which
  `push_block` reports through `Err`. Behaviour is unchanged — the
  documented contract was never the implemented one.

### Fixed
- **`spsc`'s blocking producer checked for a closed consumer with
  `Acquire` where the shared `push_block` loop requires `SeqCst`.**
  `SingleParkerProducer::consumer_gone` is called once before parking
  and again immediately after arming; the second call is the waiter's
  half of the same Dekker handshake described below, so an `Acquire`
  load leaves it unordered against the arming store. SPMC's identical
  impl already used `SeqCst`. A producer blocked in `push_block` on a
  full ring could therefore miss a concurrent consumer close and park
  until the ring was dropped.
- **`spsc::RingBuffer::close` published the close with `Release`,**
  where every other close in the crate — including MPSC's otherwise
  identical `close` — uses `SeqCst`. Closing and parking is a Dekker
  handshake over two locations: the closer stores "closed" then loads
  "is anyone parked?", while the waiter stores "I am parked" then
  loads "closed?". `Release`/`Acquire` orders a store against a later
  load of the *same* location and leaves that pair unordered, so both
  sides could read stale values — the closer seeing nobody parked and
  issuing no wake, the waiter seeing an open ring and parking with
  nothing left to wake it. A consumer already parked in `pop_block`
  when `close()` ran could sleep until the ring was dropped. The
  flag now lives in `CloseState`, whose `close` is `SeqCst` for every
  ring by construction.
- **A cancelled async waiter left its waker registered forever.**
  `push_async`/`pop_async` registered the waker from inside `poll_fn`
  and nothing ever removed it. On a full or empty ring a cancel/retry
  loop (a timeout, `select!`, a dropped task) left one live `Waker`
  behind per iteration: each new future's registration displaced the
  dead one into the overflow list, which no wake drained because the
  peer was gone. A thousand cancelled pushes on an idle ring left a
  thousand live wakers. The registration is now owned by the future —
  `ParkRegistration` for endpoints shared by several futures,
  `ParkedFuture` for the exclusive `&mut` consumers — and is withdrawn
  on drop, whether that drop is cancellation or completion. The
  displaced-waker rule is preserved: a peer's waker found in the slot
  at withdraw time moves to the overflow list, never dropped.
- **A waiter that moved threads was never woken.** Park handles lived in
  a `OnceLock<Thread>`, so the first thread to park on an endpoint owned
  that entry for the endpoint's life. Every endpoint is `Send`, so a
  handle that parks on one thread, moves, and parks on another was still
  recorded as the first: the wake went to a thread that was not parked,
  or had exited, and the thread actually sleeping was never signalled.
  Work-stealing executors and thread pools move handles as a matter of
  course. A `ThreadParker` now re-arms on every park, publishing the
  handle through an `AtomicPtr` whose every access is a `swap`, so the
  thread that takes the handle out is its sole owner and may drop it.
  A regression test that pops from two threads in turn hung past 120
  seconds before the fix.
- **More than 64 waiters on one ring could lose a wakeup.** Park slots
  were assigned by masking a monotonic counter, and the aliasing that
  follows was documented as benign — a false wake, a re-check, a
  re-park. It is not benign. Bit *i* of the wake bitmap is the only
  record that slot *i* has a waiter, so a wake clears that one bit and
  unparks one thread; a second waiter on the same slot is left parked
  with nothing marking it as waiting, and nothing wakes it again. Because
  the counter only ever increased, the same collision also arrived by
  churn: a ring that created 64 or more endpoints over its life aliased a
  long-lived waiter even with few alive at once. A `ParkRegistry` now
  leases slots and reclaims them on drop, so an index is reused only
  after its previous holder is gone. A waiter that finds every slot taken
  gets a shared slot with no bitmap bit and re-checks the ring on a
  timeout; async waiters, which cannot self-rescue because only a waker
  can poll a future again, go on an overflow list that every wake drains.
  Regression tests cover both the concurrent and the churn case.
- **The waker overflow list no longer uses a lock.** It was introduced
  as a `Mutex<Vec<Waker>>` — the only lock in the crate — on the
  argument that it sat off the lock-free paths. It did not: `WakerSet`
  never clears its `pending` flag, by design, so a ring that had parked
  a single async waiter routed *every* later wake through
  `WakerOverflow::wake_all` and took the mutex there. It is now a
  Treiber stack: a `compare_exchange` to register, one `swap` to take
  the whole chain, and a `Relaxed` null check that makes the common
  empty case free. The crate contains no locks again.
- **Two async operations on one handle stranded one of them.** The async
  methods take `&self`, so safe code can hold two `push_async` futures
  from a single producer (or two `pop_async` futures from a single
  consumer) and drive them from separate tasks with separate wakers. A
  park slot belongs to the *endpoint*, not to the future, so both
  registered in the same slot and the second store dropped the first
  waker — leaving a parked future that nothing could ever poll again. A
  displaced waker is now moved to the overflow list rather than dropped,
  and `WakerSlot::store` returns it so the invariant cannot be missed at
  a call site. A future re-registering its own waker, the common case,
  still displaces nothing.
- **A broadcast producer registered its async waker at a moving index.**
  The slot was derived from the ring tail, so it changed between polls of
  the same future, scattering registrations across slots other producers
  owned. The producer's leased slot is now used, as elsewhere.
- The `mpmc::Cfg` example was marked `ignore` and had never compiled — it
  referenced an undefined binding. It now runs with the rest of the
  doctests.
- **A broadcast consumer could loop forever on an abandoned
  reservation.** The tombstone was a single `usize::MAX` sentinel that
  named no position, but a broadcast consumer never clears a marker —
  every other consumer still has to see the same slot. Every later
  position aliasing that slot therefore also read as abandoned: at
  `cap == 1`, a consumer advanced past the same marker indefinitely
  instead of reporting an empty ring. The sequence word now encodes
  vacant / published(pos) / abandoned(pos), so a marker matches only the
  position it names. Abandoning a reservation also wakes consumers and
  producers: it releases the position for both, and a consumer parked on
  it was waiting for a publication that would never arrive.
- **A tombstone-only MPSC drain stranded ring capacity.** `drain` and
  `drain_up_to` advanced a local cursor across abandoned positions but
  published `head` only when a value came out, so a batch that found
  only tombstones cleared the slot metadata while leaving `head` on the
  tombstone. The space was never handed back and producers saw a ring
  that stayed full. Released positions are now counted separately from
  delivered values, and publication happens in a guard's destructor,
  which covers the unwind path as well. `pop` and `pop_ref` also wake a
  producer when they skip a tombstone, which they previously did not.
- **Blocking zero-copy reads could return `None` on an open channel.**
  `pop_ref_block` tested an observation and then returned whatever the
  claim produced, but an observation is not ownership: in MPSC the
  position could hold an abandoned reservation, and in SPMC/MPMC a peer
  consumer could take it first. Either turned into `None` from a
  blocking read while producers were live and publishing. The
  single-consumer gate now resolves tombstones itself so the claim after
  it cannot fail, and the multi-consumer paths treat a lost claim as
  contention and retry.
- **Dropping an MPMC producer could hang forever.** Handing back an
  unused batch position requires its previous round to be released, and
  only a consumer does that. The wait was unconditional, so with the
  consumers gone the destructor never returned — during cancellation or
  unwinding, exactly when the peers are disappearing. The wait now stops
  when the consumers close, leaving the slot in its previous round's
  state for `RingBuffer::drop` to reclaim.
- **An SPMC ring split with `split_borrowed` never closed.** The live
  consumer count was seeded at one and the split incremented it again
  while handing out a single consumer, so dropping that consumer left
  the tally at one: `consumer_closed` was never set and a producer
  blocked in `push_block` waited for a consumer that no longer existed.
- **An SPSC value pushed after the consumer left was leaked.** The
  consumer drains on drop, but the producer outlives it, and the ring
  had no cleanup of its own — the storage was freed with the value still
  in it. `RingBuffer::drop` now reclaims whatever remains between the
  cursors.
- **A broadcast slot with an outstanding reservation can no longer be
  reclaimed by a peer producer.** `reserve` claims a position and only
  `commit` or the guard's `Drop` resolves it, but reuse was gated solely
  on the consumer floor — consumer progress. A consumer subscribing
  *after* a reservation starts at `tail`, already past the reserved
  position, so the floor legitimately sat ahead of a slot still being
  written and a peer producer could claim the aliasing position. Two
  producers then held the same storage. `claim_slot` now also checks
  that the prior occupant of the target slot has resolved, so producer
  ownership is tracked independently of consumer heads. The
  no-consumers case was already safe; the regression test covers both.
- **`cargo +nightly miri test` now passes clean.** The README instructs
  users to run exactly that command, but it aborted with four
  leak-checker errors. Two SPSC tests exercising the raw-pointer seam
  (`raw_split_push_pop`, `raw_split_cross_thread`) used `Box::leak` to
  obtain the `'static` ring that `producer_from_raw` /
  `consumer_from_raw` require. The leak is correct in production — the
  ring really does outlive the program — but in a test it left an
  unreclaimed allocation on every run. A `PinnedRing` RAII owner now
  gives the same guarantee the `unsafe` contract asks for (a live ring at
  a fixed address, never moved) and frees it when the test ends.
- **Miri now checks two tests it had been skipping.** The
  `forgotten_written_slot_leaks_without_publishing` tests in SPSC and
  SPMC were `#[cfg_attr(miri, ignore)]` because the `DropCounter` they
  deliberately `mem::forget` holds an `Arc` whose allocation then leaks.
  The un-run *destructor* is the property under test; the leaked *heap
  block* was incidental. They now use a `BorrowedDropCounter` that owns
  no heap memory, so the assertions are unchanged and Miri covers them.
- Reconstructing the cross-thread handle in `raw_split_cross_thread` no
  longer round-trips the ring address through a `usize`. The integer cast
  erased the pointer's provenance, and Miri warned it "might miss pointer
  bugs" there; a provenance-preserving `SharedAddr` keeps those checks
  live.
- **Producers could overclaim slots and block in the non-blocking
  `push`** — a position was claimed with an unconditional
  `tail.fetch_add`, which always succeeds, so the fullness check before
  it was only a hint. With one free slot, every concurrent producer
  passed the check and claimed a position; the losers then waited inside
  `push` — documented never to block — for a consumer that might never
  advance. Observed as 3 of 8 producers wedged permanently. The claim is
  now a compare-and-swap that validates the position as part of taking
  it, so a producer only ever owns a slot it may write. This also
  removes an unbounded O(N) registry scan from the contended path:
  8-producer throughput rose from ~15 to ~55 Mitem/s, with
  single-producer throughput unchanged.
- **A broadcast producer could spin forever after the last consumer
  dropped** — the consumer floor was reported as `tail` when the
  registry was empty. A producer that had already claimed `pos` then
  compared it against a floor *ahead* of itself, and the backlog
  computation `pos - floor` wrapped to a huge value that was always
  `>= cap`, so the "is my slot free yet" loop never exited. Reachable
  from safe code whenever at least `cap` producers claimed positions
  between one producer's fullness check and its own claim. The floor is
  now a `ConsumerFloor` that distinguishes "no consumers" from a
  position, and the comparison tests the ordering before subtracting.
- **A panic could cause a double drop in SPSC** — three paths moved a
  value out of a slot before publishing the consumer's `head` cursor,
  and skipped that publication when the thread unwound between the two.
  `Consumer::drop` then drained from the stale cursor and dropped the
  same values a second time; with a heap-allocating `T` this aborted the
  process with heap corruption. Affected a panicking callback in
  `drain` and `drain_up_to`, and a panicking `T::drop` under
  `SlotReader`. Publication now happens in the destructor of a new
  `HeadPublisher` guard, so it runs on the unwind path too.
  Additionally, `WrittenSlot::commit` marked itself committed only
  *after* publishing `tail` and waking the consumer; a panic in that
  window let its destructor drop a value the consumer already owned.
  The guard is now disarmed first.
- **A forgotten SPSC/SPMC `SlotWriter` could publish an uninitialized
  slot** — `reserve` advanced the producer's private write cursor and
  relied on the guard's destructor to roll it back. `std::mem::forget`
  runs no destructor, so safe code could reserve a slot, forget the
  guard, and `push` a value into the *next* position; publishing `tail`
  then exposed the skipped, never-initialized slot to the consumer as a
  valid item. A safety argument may not depend on a destructor running.
  The cursor now advances on publication (`push`, `commit`,
  `commit_unchecked`) rather than on reservation, so an abandoned
  reservation — dropped *or* forgotten — leaves the cursor on the
  uninitialized slot and the next claim reuses it. Forgetting a
  `WrittenSlot` now leaks the value in place, which is safe, instead of
  publishing it. Capacity is no longer permanently consumed by a
  forgotten reservation.

### Added
- **Exhaustive-schedule models for the async wake machinery**, run
  under `RUSTFLAGS="--cfg loom" cargo test --lib --features async --
  release -- common::loom_models`. Where the release stress suite
  samples one platform's scheduler at scale and Miri checks one
  schedule for UB, `loom` enumerates every interleaving the C11
  memory model permits and asserts the wake-path invariants under all
  of them: a withdrawal never drops a peer's waker the wake is about
  to claim, a wake and a withdrawal claim a waker at most once, racing
  registrations and wakes deliver exactly one wake per waiter, and
  concurrent `ParkRegistry` leases never alias a slot. Four leaf
  primitives are covered — `WakerSet`/`WakerSlot`, `WakerOverflow`,
  `ParkRegistry`, and the `ExclusiveRegistration` withdraw path. The
  park/blocking machinery (which calls `std::thread::park`, unmocked
  by loom) and the ring slot machinery (far too large to enumerate)
  stay outside the model's reach; the stress suite covers those.
  Atomic types route through the new `common::atomics` aliases, which
  switch on the `cfg(loom)` flag cargo derives from the new
  `[target.'cfg(loom)'.dependencies]` entry — no cargo feature, no
  loom code compiled or fetched in default builds. The three leaf
  constructors keep `const` under std (loom's atomics are not
  const-constructible) and `WakerSlot`/`WakerOverflow`'s drop paths
  take their stored pointer with a `swap`, which is equivalent under
  `&mut self` and shared across both worlds.

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
