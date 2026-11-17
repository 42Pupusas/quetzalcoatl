/-!
Model of the producer park/wake handshake of `mpsc::Producer::push_block`, `WakeSet::arm`,
`WakeSet::disarm`, `WakeSet::wake_one`, `WakeSet::wake_n` and `ParkOverflow`, as a step system
whose steps are the atomic operations of the Rust.

State. `free` is the number of free slots (a producer's successful `push` takes one, the
consumer's release gives them back). Each producer `i` is leased a park slot or not, fixed for the
run. A leased producer has its wake bit and its park handle (armed or claimed); an unleased one has
no bit and instead registers its thread on the overflow stack (`registered`). Each has its park
token (set by an unpark that finds the thread not parked, as with `std::thread::park`) and a
program counter. The consumer releases its `left` slots in batches of any size, each followed by a
wake loop owing one wake per slot: a batch of one is `pop` and `wake_one`, a larger one is `drain`
and `wake_n`. When `closes` is set it then drops: it raises `closed`, drains the overflow stack,
swaps the wake bitmap to zero (`WakeSet::flush`) and claims the handle of every producer whose bit
it took.

Steps. Every step is one atomic access, interleaved arbitrarily between actors:

* a producer runs `push` (`attempt`), then `arm`: a leased one in two steps (`handle`, then `bit`,
  as `WakeSet::arm` publishes the handle before the bit), an unleased one by pushing onto the
  overflow stack. Then the post-arm `push` (`recheck`), then `park`, which returns when its token
  is set or spuriously, then `disarm` and back to the loop top;
* a waker loops through `drain` (the `swap` that takes the whole overflow stack), `unpark` of each
  thread it took, then `scan` (load the bitmap and pick a set bit; any set bit may be picked, which
  includes the round-robin choice of the Rust), `clear` (the `fetch_and` that retires the bit; if
  the bit was already clear it rescans) and `claim` (take the handle and unpark; if the handle was
  already claimed it rescans), until it has delivered its count or finds the bitmap empty.

The wake bit is not cleared by anything but a waker or the producer's own `disarm`, so it
outlives the producer's re-check. That is the window the theorem below uses.

A spurious return is a step of its own (`State.spurious`). It is in `Reach` but not in `Lost`: a
producer that only a spurious return would release is lost, because nothing makes one happen.

The overflow drain's fast-path load is not a step of its own: a load that finds the stack empty
and skips the `swap` is a `drain` that takes nothing at that moment. Stale registrations, left by a
producer that did not park, cost an unpark that lands as a token; that is the same as a spurious
return.

The spin phase is not a step either: a `push` that fails and spins changes nothing the handshake
reads, so it is the producer not yet taking its `attempt` step.

Not modelled: memory orderings (every step is sequentially consistent). The loss shown below
needs none of the above.

The `Variant` picks the protocol:

* `unrepaired` is `mpsc::Producer::push_block` as it was: the re-check claims the slot it sees,
  then plain `disarm`.
* `repaired` is the same with `WakeSet::stand_down`: a producer whose re-check succeeded, and whose
  `disarm` finds its bit already cleared, knows a waker spent a wake on it and runs the wake loop
  itself, handing the wake on (`wake_one`).
* `repairedWakingAll` is `WakeSet::stand_down_waking_all`, used by mpmc producers: the same, but
  the loop it runs is `wake_n(PARK_SLOTS)`.
* `observing` is `broadcast::Producer::push_block`: the re-check only observes that a slot is free
  (`has_space`) and claims nothing. The producer then plain `disarm`s and goes back to the top of
  its loop, where it tries to claim, and arms again if a peer got there first.
* `ignoresClose` is `observing` without the closed check in the re-check. It exists only so the
  closed-exit certificates have a protocol they must reject.

The closed exit is the same in every variant: the loop top returns when `closed` is set, and so
does the re-check after `arm`, through a plain `disarm`. `reserve_block` has the same two exits,
so this model covers it as well.
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

/-- A wake loop's phase. `count` is the number of wakes it has yet to deliver. -/
inductive Waker where
  | drain (count : Nat)
  | unpark (pending : List Nat) (count : Nat)
  | scan (count : Nat)
  | clear (i count : Nat)
  | claim (i count : Nat)
  deriving DecidableEq, Repr

inductive Pc where
  | attempt
  | handle
  | bit
  | recheck
  | park
  | disarmWon
  | disarmWoken
  | disarmClosed
  | forward (w : Waker)
  | done
  deriving DecidableEq, Repr

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
  deriving DecidableEq, Repr

/-- A leased producer that has not started. -/
def Producer.fresh : Producer := ⟨.attempt, true, false, false, false, false⟩

def Consumer.afterRelease (closes : Bool) (left : Nat) : Consumer :=
  if left = 0 then (if closes then .close else .finished) else .release

/-- One producer per entry of `leases`, leased a park slot where it is `true`. -/
def State.init (leases : List Bool) (releases : Nat) (closes : Bool := false) : State :=
  ⟨0, releases, closes, false, Consumer.afterRelease closes releases,
    leases.map fun leased => { Producer.fresh with leased }⟩

def Producer.at (ps : List Producer) (i : Nat) : Producer := ps.getD i Producer.fresh

def Producer.update (ps : List Producer) (i : Nat) (f : Producer → Producer) : List Producer :=
  ps.set i (f (Producer.at ps i))

/-- The producers on the overflow stack. -/
def Producer.registeredIndices (ps : List Producer) : List Nat :=
  (List.range ps.length).filter fun i => (Producer.at ps i).registered

/-- The overflow stack after a `swap` takes all of it. -/
def Producer.unregister (ps : List Producer) : List Producer :=
  ps.map fun p => { p with registered := false }

/-- Where a wake loop goes after delivering one of `count` wakes. -/
def Waker.delivered (count : Nat) : Option Waker :=
  if count ≤ 1 then none else some (.scan (count - 1))

/-- One step of the wake loop on the producers' shared words. The result is the new producers
and the loop's next phase, `none` when the loop has finished. -/
def Waker.step (ps : List Producer) : Waker → List (List Producer × Option Waker)
  | .drain count =>
    [(Producer.unregister ps, some (.unpark (Producer.registeredIndices ps) count))]
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

def State.setPc (s : State) (i : Nat) (pc : Pc) : State :=
  { s with producers := Producer.update s.producers i (fun p => { p with pc := pc }) }

def State.edit (s : State) (i : Nat) (f : Producer → Producer) : State :=
  { s with producers := Producer.update s.producers i f }

/-- Every successor of `s` by a step of producer `i`. -/
def State.producerSteps (variant : Variant) (s : State) (i : Nat) : List State :=
  let p := Producer.at s.producers i
  match p.pc with
  | .attempt =>
    if s.closed then [s.setPc i .done]
    else if 0 < s.free then [{ s.setPc i .done with free := s.free - 1 }]
    else [s.setPc i .handle]
  | .handle =>
    if p.leased then [(s.edit i fun p => { p with handle := true, pc := .bit })]
    else [(s.edit i fun p => { p with registered := true, pc := .recheck })]
  | .bit => [(s.edit i fun p => { p with bit := true, pc := .recheck })]
  | .recheck =>
    if s.closed && variant.seesClose then [s.setPc i .disarmClosed]
    else if 0 < s.free then
      match variant with
      | .observing | .ignoresClose => [s.setPc i .disarmWoken]
      | _ => [{ s.setPc i .disarmWon with free := s.free - 1 }]
    else [s.setPc i .park]
  | .park =>
    if p.token then [(s.edit i fun p => { p with token := false, pc := .disarmWoken })] else []
  | .disarmWon =>
    if variant.standsDown && p.leased && !p.bit then
      [(s.edit i fun p => { p with pc := .forward (.drain variant.forwardCount) })]
    else [(s.edit i fun p => { p with bit := false, pc := .done })]
  | .disarmWoken => [(s.edit i fun p => { p with bit := false, pc := .attempt })]
  | .disarmClosed => [(s.edit i fun p => { p with bit := false, pc := .done })]
  | .forward w =>
    (Waker.step s.producers w).map fun (ps, next) =>
      match next with
      | some w' => { s with producers := ps }.setPc i (.forward w')
      | none => { s with producers := ps }.setPc i .done
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
    (Waker.step s.producers w).map fun (ps, next) =>
      match next with
      | some w' => { s with producers := ps, consumer := .waking w' }
      | none =>
        { s with producers := ps, consumer := Consumer.afterRelease s.closes s.left }
  | .close => [{ s with closed := true, consumer := .drainOverflow }]
  | .drainOverflow =>
    [{ s with
        producers := Producer.unregister s.producers,
        consumer := .unparking (Producer.registeredIndices s.producers) }]
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

/-- Runs a schedule: each entry names an actor (`0` the consumer, `k + 1` producer `k`) and
which of its enabled steps to take. `none` if an entry names a step that does not exist. -/
def State.run (variant : Variant) : State → List (Nat × Nat) → Option State
  | s, [] => some s
  | s, (actor, choice) :: rest =>
    let steps :=
      if actor = 0 then s.consumerSteps
      else if actor - 1 < s.producers.length then s.producerSteps variant (actor - 1)
      else []
    match steps[choice]? with
    | none => none
    | some t => t.run variant rest

theorem State.mem_succs_of_run_step {variant : Variant} {s t : State} {actor choice : Nat}
    (h : (if actor = 0 then s.consumerSteps
          else if actor - 1 < s.producers.length then s.producerSteps variant (actor - 1)
          else [])[choice]? = some t) :
    t ∈ s.succs variant := by
  have mem := List.mem_of_getElem? h
  unfold State.succs
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
        (List.mem_append_left _ (State.mem_succs_of_run_step eq))) h

/-- Two leased producers, a ring with room for one, and two releases of one slot by the consumer.
Producer 0 arms and its re-check takes the slot the first release freed. Producer 1 arms and
parks, finding the ring full. The second release's wake retires producer 0's bit, which is still
set, and unparks producer 0 instead of producer 1. Producer 0 then stands down and producer 1
sleeps with a free slot and no one left to wake it. -/
def lostSchedule : List (Nat × Nat) :=
  [(1, 0), (0, 0), (0, 0), (0, 0), (0, 0), (1, 0), (1, 0), (1, 0), (2, 0), (2, 0), (2, 0),
   (2, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (1, 0)]

def lostState : State :=
  ⟨1, 0, false, false, .finished,
    [⟨.done, true, false, false, true, false⟩, ⟨.park, true, true, true, false, false⟩]⟩

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
  [(1, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (1, 0), (1, 0), (1, 0)]

def closedState : State :=
  ⟨0, 0, true, true, .finished, [⟨.park, true, true, true, false, false⟩]⟩

theorem closedSchedule_reaches_closedState :
    (State.init [true] 0 true).run .ignoresClose closedSchedule = some closedState := by
  decide +kernel

theorem ignoring_close_is_lost :
    ∃ s, Reach .ignoresClose (State.init [true] 0 true) s ∧ s.Lost .ignoresClose :=
  ⟨closedState, Reach.run Reach.init closedSchedule_reaches_closedState,
    by unfold State.Lost; decide +kernel⟩

end Park

end Qtz
