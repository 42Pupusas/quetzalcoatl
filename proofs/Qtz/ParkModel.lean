/-!
Model of the producer park/wake handshake of `push_block` and `reserve_block`, `WakeSet::arm`,
`WakeSet::disarm`, `WakeSet::stand_down`, `WakeSet::wake_one`, `WakeSet::wake_n`, `WakeSet::flush`
and `ParkOverflow`, as a step system whose steps are the atomic operations of the Rust.

State. `free` is the number of free slots (a producer's successful claim takes one, the consumer's
release gives them back). Each producer `i` is leased a park slot or not, fixed for the run. A
leased producer has its wake bit and its park handle (armed or claimed); an unleased one has no bit
and instead pushes its thread onto the overflow stack. `stack` is that stack, newest first, one
entry per push: a producer that pushes again before a drain has two entries, and is unparked twice.
`registered` marks a producer with at least one entry. Each producer has its park token (set by an
unpark that finds the thread not parked, as with `std::thread::park`) and a program counter. The
consumer releases its `left` slots in batches of any size, each followed by a wake loop owing one
wake per slot: a batch of one is `pop` and `wake_one`, a larger one is `drain` and `wake_n`. When
`closes` is set it then drops: it raises `closed`, drains the overflow stack, swaps the wake bitmap
to zero (`WakeSet::flush`) and claims the handle of every producer whose bit it took. It wakes
nobody after that: the values left in the ring are released without a wake.

Steps. Every step is one atomic access, interleaved arbitrarily between actors. A producer's loop
reads the closed flag and the ring in two steps, in the order its flavor does:

* mpsc and broadcast (`Variant.claimsFirst` false) check `closed` at the loop top (`top`) and then
  try the ring (`attempt`); after arming they check `closed` (`recheck`) and then the ring
  (`recheckRing`). Broadcast's ring access at the top only observes a free slot (`has_space`) and
  claims it in a step of its own (`take`), which a peer can beat.
* mpmc (`claimsFirst` true) tries the ring first (`top`) and then checks `closed` (`topClosed`);
  after arming the same (`recheck`, then `recheckClosed`).

A failed lap either spins back to `top` or arms. Arming is `handle` then `bit` for a leased producer
(`WakeSet::arm` publishes the handle before the bit), or one push onto the overflow stack for an
unleased one. Then the re-check, then `park`, which returns when its token is set or spuriously,
then `disarm` and back to `top`.

A waker loops through `drain` (the `swap` that takes the whole overflow stack), `unpark` of each
entry it took in stack order, then `scan` (load the bitmap and pick a set bit; any set bit may be
picked, which includes the round-robin choice of the Rust), `clear` (the `fetch_and` that retires
the bit; if the bit was already clear it rescans) and `claim` (take the handle and unpark; if the
handle was already claimed it rescans), until it has delivered its count or finds the bitmap empty.

The wake bit is not cleared by anything but a waker or the producer's own `disarm`, so it
outlives the producer's re-check. That is the window the theorem below uses.

A spurious return is a step of its own (`State.spurious`). It is in `Reach` but not in `Lost`: a
producer that only a spurious return would release is lost, because nothing makes one happen.

Loads that only decide whether to take a step are not steps: the overflow drain's empty check
(a drain that finds the stack empty takes nothing), the round-robin cursor, and the handle's null
check before its `swap` (a claim that finds no handle). Nor are the mpmc awaited-position
announcements, which only rank wakes this model does not route.

Not modelled: memory orderings (every step is sequentially consistent).

The `Variant` picks the protocol:

* `unrepaired` is `mpsc::Producer::push_block` as it was: the re-check claims the slot it sees,
  then plain `disarm`.
* `repaired` is mpsc with `WakeSet::stand_down`: a producer leaving after its re-check, whose
  `fetch_and` finds its bit already cleared, knows a waker spent a wake on it and runs the wake loop
  itself, handing the wake on (`wake_one`).
* `repairedWakingAll` is mpmc's `stand_down_waking_all`: the same, but the loop it runs is
  `wake_n(PARK_SLOTS)`.
* `observing` is `broadcast::Producer::push_block`: the re-check only observes that a slot is free
  (`has_space`) and claims nothing. The producer then plain `disarm`s and goes back to the top of
  its loop, where it tries to claim, and arms again if a peer got there first.
* `ignoresClose` is `observing` without the closed check in the re-check. It exists only so the
  closed-exit certificates have a protocol they must reject.

The closed exit after the re-check is `stand_down` where the variant stands down, and a plain
`disarm` otherwise.
-/

namespace Qtz

namespace Park

inductive Variant where
  | unrepaired
  | repaired
  | repairedWakingAll
  | observing
  | ignoresClose
  deriving DecidableEq, Repr

def Variant.standsDown : Variant → Bool
  | .repaired | .repairedWakingAll => true
  | _ => false

/-- The wakes a producer standing down forwards: `wake_one`, or `wake_n(PARK_SLOTS)`. -/
def Variant.forwardCount : Variant → Nat
  | .repairedWakingAll => 64
  | _ => 1

def Variant.seesClose : Variant → Bool
  | .ignoresClose => false
  | _ => true

/-- Whether the loop tries the ring before it checks `closed` (mpmc). -/
def Variant.claimsFirst : Variant → Bool
  | .repairedWakingAll => true
  | _ => false

/-- Whether a ring access only observes a free slot (broadcast's `has_space`). -/
def Variant.observes : Variant → Bool
  | .observing | .ignoresClose => true
  | _ => false

/-- A wake loop's phase. `count` is the number of wakes it has yet to deliver. -/
inductive Waker where
  | drain (count : Nat)
  | unpark (pending : List Nat) (count : Nat)
  | scan (count : Nat)
  | clear (i count : Nat)
  | claim (i count : Nat)
  deriving DecidableEq, Repr

inductive Pc where
  | top
  | attempt
  | topClosed
  | take
  | handle
  | bit
  | recheck
  | recheckRing
  | recheckClosed
  | park
  | disarmWon
  | disarmWoken
  | disarmClosed
  | forward (w : Waker)
  | done
  deriving DecidableEq, Repr

/-- The producer has made its last look at the ring and parks unless the channel is closed. -/
@[simp] def Pc.sleeps : Pc → Bool
  | .park | .recheckClosed => true
  | _ => false

/-- The producer has made its last check of `closed` and parks unless the ring has room. -/
@[simp] def Pc.mayPark : Pc → Bool
  | .park | .recheckRing => true
  | _ => false

structure Producer where
  pc : Pc
  leased : Bool
  bit : Bool
  handle : Bool
  token : Bool
  registered : Bool
  deriving DecidableEq, Repr

inductive Consumer where
  | release
  | waking (w : Waker)
  | close
  | drainOverflow
  | unparking (pending : List Nat)
  | flush
  | flushing (pending : List Nat)
  | finished
  deriving DecidableEq, Repr

structure State where
  free : Nat
  left : Nat
  closes : Bool
  closed : Bool
  consumer : Consumer
  producers : List Producer
  stack : List Nat
  deriving DecidableEq, Repr

/-- A leased producer that has not started. -/
def Producer.fresh : Producer := ⟨.top, true, false, false, false, false⟩

def Consumer.afterRelease (closes : Bool) (left : Nat) : Consumer :=
  if left = 0 then (if closes then .close else .finished) else .release

/-- One producer per entry of `leases`, leased a park slot where it is `true`. -/
def State.init (leases : List Bool) (releases : Nat) (closes : Bool := false) : State :=
  ⟨0, releases, closes, false, Consumer.afterRelease closes releases,
    leases.map (fun leased => { Producer.fresh with leased }), []⟩

def Producer.at (ps : List Producer) (i : Nat) : Producer := ps.getD i Producer.fresh

def Producer.update (ps : List Producer) (i : Nat) (f : Producer → Producer) : List Producer :=
  ps.set i (f (Producer.at ps i))

/-- The overflow stack after a `swap` takes all of it. -/
def Producer.unregister (ps : List Producer) : List Producer :=
  ps.map fun p => { p with registered := false }

/-- Where a wake loop goes after delivering one of `count` wakes. -/
def Waker.delivered (count : Nat) : Option Waker :=
  if count ≤ 1 then none else some (.scan (count - 1))

/-- One step of the wake loop on the producers' shared words, given the overflow stack. The result
is the new producers and the loop's next phase, `none` when the loop has finished. -/
def Waker.step (ps : List Producer) (stack : List Nat) : Waker → List (List Producer × Option Waker)
  | .drain count => [(Producer.unregister ps, some (.unpark stack count))]
  | .unpark [] count => [(ps, some (.scan count))]
  | .unpark (i :: rest) count =>
    [(Producer.update ps i (fun p => { p with token := true }), some (.unpark rest count))]
  | .scan count =>
    match (List.range ps.length).filter (fun i => (Producer.at ps i).bit) with
    | [] => [(ps, none)]
    | picks => picks.map fun i => (ps, some (.clear i count))
  | .clear i count =>
    if (Producer.at ps i).bit then
      [(Producer.update ps i (fun p => { p with bit := false }), some (.claim i count))]
    else [(ps, some (.scan count))]
  | .claim i count =>
    if (Producer.at ps i).handle then
      [(Producer.update ps i (fun p => { p with handle := false, token := true }),
        Waker.delivered count)]
    else [(ps, some (.scan count))]

/-- The overflow stack after a wake-loop step: a drain takes all of it. -/
def Waker.stackAfter : Waker → List Nat → List Nat
  | .drain _, _ => []
  | _, stack => stack

def State.setPc (s : State) (i : Nat) (pc : Pc) : State :=
  { s with producers := Producer.update s.producers i (fun p => { p with pc := pc }) }

def State.edit (s : State) (i : Nat) (f : Producer → Producer) : State :=
  { s with producers := Producer.update s.producers i f }

/-- The producer's claim succeeded or failed; a failed lap spins back to `top` or arms. -/
def State.claimOrArm (s : State) (i : Nat) : List State :=
  if 0 < s.free then [{ s.setPc i .done with free := s.free - 1 }]
  else [s.setPc i .handle, s.setPc i .top]

/-- Leaving after the re-check: `stand_down`, which runs a wake loop if a waker already took the
bit, or a plain `disarm`. -/
def State.leave (variant : Variant) (s : State) (i : Nat) : List State :=
  let p := Producer.at s.producers i
  if variant.standsDown && p.leased && !p.bit then
    [(s.edit i fun p => { p with pc := .forward (.drain variant.forwardCount) })]
  else [(s.edit i fun p => { p with bit := false, pc := .done })]

/-- Every successor of `s` by a step of producer `i`. -/
def State.producerSteps (variant : Variant) (s : State) (i : Nat) : List State :=
  let p := Producer.at s.producers i
  match p.pc with
  | .top =>
    if variant.claimsFirst then
      if 0 < s.free then [{ s.setPc i .done with free := s.free - 1 }]
      else [s.setPc i .topClosed]
    else if s.closed then [s.setPc i .done]
    else [s.setPc i .attempt]
  | .attempt =>
    if variant.observes then
      if 0 < s.free then [s.setPc i .take] else [s.setPc i .handle, s.setPc i .top]
    else s.claimOrArm i
  | .topClosed =>
    if s.closed then [s.setPc i .done] else [s.setPc i .handle, s.setPc i .top]
  | .take =>
    if 0 < s.free then [{ s.setPc i .done with free := s.free - 1 }] else [s.setPc i .top]
  | .handle =>
    if p.leased then [(s.edit i fun p => { p with handle := true, pc := .bit })]
    else
      [{ (s.edit i fun p => { p with registered := true, pc := .recheck }) with
          stack := i :: s.stack }]
  | .bit => [(s.edit i fun p => { p with bit := true, pc := .recheck })]
  | .recheck =>
    if variant.claimsFirst then
      if 0 < s.free then [{ s.setPc i .disarmWon with free := s.free - 1 }]
      else [s.setPc i .recheckClosed]
    else if s.closed && variant.seesClose then [s.setPc i .disarmClosed]
    else [s.setPc i .recheckRing]
  | .recheckRing =>
    if 0 < s.free then
      if variant.observes then [s.setPc i .disarmWoken]
      else [{ s.setPc i .disarmWon with free := s.free - 1 }]
    else [s.setPc i .park]
  | .recheckClosed =>
    if s.closed then [s.setPc i .disarmClosed] else [s.setPc i .park]
  | .park =>
    if p.token then [(s.edit i fun p => { p with token := false, pc := .disarmWoken })] else []
  | .disarmWon => s.leave variant i
  | .disarmWoken => [(s.edit i fun p => { p with bit := false, pc := .top })]
  | .disarmClosed => s.leave variant i
  | .forward w =>
    (Waker.step s.producers s.stack w).map fun (ps, next) =>
      match next with
      | some w' => { s with producers := ps, stack := w.stackAfter s.stack }.setPc i (.forward w')
      | none => { s with producers := ps, stack := w.stackAfter s.stack }.setPc i .done
  | .done => []

/-- The producers whose wake bit is set. -/
def State.bitIndices (s : State) : List Nat :=
  (List.range s.producers.length).filter fun i => (Producer.at s.producers i).bit

/-- A spurious return from `park`. -/
def State.spurious (s : State) (i : Nat) : List State :=
  if (Producer.at s.producers i).pc = .park then [s.setPc i .disarmWoken] else []

def State.consumerSteps (s : State) : List State :=
  match s.consumer with
  | .release =>
    (List.range s.left).map fun batch =>
      { s with free := s.free + (batch + 1), left := s.left - (batch + 1),
               consumer := .waking (.drain (batch + 1)) }
  | .waking w =>
    (Waker.step s.producers s.stack w).map fun (ps, next) =>
      match next with
      | some w' => { s with producers := ps, stack := w.stackAfter s.stack, consumer := .waking w' }
      | none =>
        { s with producers := ps, stack := w.stackAfter s.stack,
                 consumer := Consumer.afterRelease s.closes s.left }
  | .close => [{ s with closed := true, consumer := .drainOverflow }]
  | .drainOverflow =>
    [{ s with
        producers := Producer.unregister s.producers, stack := [],
        consumer := .unparking s.stack }]
  | .unparking [] => [{ s with consumer := .flush }]
  | .unparking (i :: rest) =>
    [{ s with
        producers := Producer.update s.producers i fun p => { p with token := true },
        consumer := .unparking rest }]
  | .flush =>
    [{ s with
        producers := s.producers.map fun p => { p with bit := false },
        consumer := .flushing s.bitIndices }]
  | .flushing [] => [{ s with consumer := .finished }]
  | .flushing (i :: rest) =>
    [{ s with
        producers := Producer.update s.producers i fun p =>
          if p.handle then { p with handle := false, token := true } else p,
        consumer := .flushing rest }]
  | .finished => []

/-- Every successor by a step that some actor takes for a reason. -/
def State.succs (variant : Variant) (s : State) : List State :=
  s.consumerSteps ++ (List.range s.producers.length).flatMap (s.producerSteps variant)

def State.spuriousSteps (s : State) : List State :=
  (List.range s.producers.length).flatMap s.spurious

/-- Every successor, spurious returns included. -/
def State.moves (variant : Variant) (s : State) : List State :=
  s.succs variant ++ s.spuriousSteps

inductive Reach (variant : Variant) (start : State) : State → Prop
  | init : Reach variant start start
  | step {s t : State} :
      Reach variant start s → t ∈ s.moves variant → Reach variant start t

/-- A producer is stuck for good with a reason to proceed: nothing can step, a producer has not
finished, and either a slot is free or the channel is closed. Nothing is left to wake it, because
only a waker or a flush unparks, and a spurious return is not something to wait for. -/
def State.Lost (variant : Variant) (s : State) : Prop :=
  s.succs variant = [] ∧ (0 < s.free ∨ s.closed = true) ∧ ∃ p ∈ s.producers, p.pc ≠ .done

/-- The steps actor `actor` can take: `0` is the consumer, `k + 1` producer `k`. -/
def State.actorSteps (variant : Variant) (s : State) (actor : Nat) : List State :=
  if actor = 0 then s.consumerSteps
  else if actor - 1 < s.producers.length then s.producerSteps variant (actor - 1)
  else []

/-- Runs a schedule: each entry names an actor and which of its enabled steps to take. `none` if
an entry names a step that does not exist. -/
def State.run (variant : Variant) : State → List (Nat × Nat) → Option State
  | s, [] => some s
  | s, (actor, choice) :: rest =>
    match (s.actorSteps variant actor)[choice]? with
    | none => none
    | some t => t.run variant rest

theorem State.mem_succs_of_actorSteps {variant : Variant} {s t : State} {actor : Nat}
    (mem : t ∈ s.actorSteps variant actor) : t ∈ s.succs variant := by
  unfold State.succs
  unfold State.actorSteps at mem
  by_cases zero : actor = 0
  · simp only [zero, ite_true] at mem
    exact List.mem_append_left _ mem
  · by_cases inside : actor - 1 < s.producers.length
    · simp only [zero, inside, ite_true, ite_false] at mem
      exact List.mem_append_right _ (List.mem_flatMap.mpr ⟨actor - 1, List.mem_range.mpr inside, mem⟩)
    · simp [zero, inside] at mem

theorem Reach.run {variant : Variant} {start s t : State} :
    ∀ {schedule : List (Nat × Nat)}, Reach variant start s →
      s.run variant schedule = some t → Reach variant start t
  | [], reach, h => by
    simp only [State.run, Option.some.injEq] at h
    exact h ▸ reach
  | (actor, choice) :: rest, reach, h => by
    simp only [State.run] at h
    split at h
    · exact absurd h (by simp)
    · rename_i next eq
      exact Reach.run (Reach.step reach
        (List.mem_append_left _ (State.mem_succs_of_actorSteps (List.mem_of_getElem? eq)))) h

/-- Two leased producers, a ring with room for one, and two releases of one slot by the consumer.
Producer 0 arms and its re-check takes the slot the first release freed. Producer 1 arms and
parks, finding the ring full. The second release's wake retires producer 0's bit, which is still
set, and unparks producer 0 instead of producer 1. Producer 0 then disarms and producer 1 sleeps
with a free slot and no one left to wake it. -/
def lostSchedule : List (Nat × Nat) :=
  [(1, 0), (1, 0), (0, 0), (0, 0), (0, 0), (0, 0), (1, 0), (1, 0), (1, 0), (1, 0),
   (2, 0), (2, 0), (2, 0), (2, 0), (2, 0), (2, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0),
   (0, 0), (1, 0)]

def lostState : State :=
  ⟨1, 0, false, false, .finished,
    [⟨.done, true, false, false, true, false⟩, ⟨.park, true, true, true, false, false⟩], []⟩

theorem lostSchedule_reaches_lostState :
    (State.init [true, true] 2).run .unrepaired lostSchedule = some lostState := by
  decide +kernel

theorem lostState_is_lost : lostState.Lost .unrepaired := by
  unfold State.Lost
  decide +kernel

/-- A wake is lost: a producer is left parked for good with a free slot. -/
theorem wake_is_lost :
    ∃ s, Reach .unrepaired (State.init [true, true] 2) s ∧ s.Lost .unrepaired :=
  ⟨lostState, Reach.run Reach.init lostSchedule_reaches_lostState, lostState_is_lost⟩

/-- Without the closed check in the re-check, a producer that arms after the flush parks for good:
it finds the ring full, the consumer gone and nobody left to wake it. -/
def closedSchedule : List (Nat × Nat) :=
  [(1, 0), (1, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (1, 0), (1, 0), (1, 0), (1, 0)]

def closedState : State :=
  ⟨0, 0, true, true, .finished, [⟨.park, true, true, true, false, false⟩], []⟩

theorem closedSchedule_reaches_closedState :
    (State.init [true] 0 true).run .ignoresClose closedSchedule = some closedState := by
  decide +kernel

theorem ignoring_close_is_lost :
    ∃ s, Reach .ignoresClose (State.init [true] 0 true) s ∧ s.Lost .ignoresClose :=
  ⟨closedState, Reach.run Reach.init closedSchedule_reaches_closedState,
    by unfold State.Lost; decide +kernel⟩

end Park

end Qtz
