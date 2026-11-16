/-!
Model of the producer park/wake handshake of `mpsc::Producer::push_block`, `WakeSet::arm`,
`WakeSet::disarm` and `WakeSet::wake_one`, as a step system whose steps are the atomic
operations of the Rust.

State. `free` is the number of free slots (a producer's successful `push` takes one, the
consumer's pop gives one back). Each producer `i` has its wake bit, its park handle (armed or
claimed), its park token (set by an unpark that finds the thread not parked, as with
`std::thread::park`) and a program counter. The consumer releases `left` slots, each followed by
a `wake_one`. When `closes` is set it then drops: it raises `closed`, swaps the wake bitmap to
zero (`WakeSet::flush`) and claims the handle of every producer whose bit it took.

Steps. Every step is one atomic access, interleaved arbitrarily between actors:

* a producer runs `push` (`attempt`), then `arm` as two steps (`handle`, then `bit`, as
  `WakeSet::arm` publishes the handle before the bit), then the post-arm `push` (`recheck`),
  then `park`, which blocks until its token is set, then `disarm` and back to the loop top;
* a waker (the consumer after each release) loops through `scan` (load the bitmap and pick a set
  bit; any set bit may be picked, which includes the round-robin choice of the Rust), `clear`
  (the `fetch_and` that retires the bit; if the bit was already clear it rescans) and `claim`
  (take the handle and unpark; if the handle was already claimed it rescans).

The wake bit is not cleared by anything but a waker or the producer's own `disarm`, so it
outlives the producer's re-check. That is the window the theorem below uses.

Not modelled: memory orderings (every step is sequentially consistent), spurious `park`
returns (none are assumed, which is what a correct protocol must not rely on), the spin phase,
and the overflow stack. The loss shown below needs none of them.

The `Variant` picks the protocol:

* `unrepaired` is `mpsc::Producer::push_block` as it was: the re-check claims the slot it sees,
  then plain `disarm`.
* `repaired` is the same with `WakeSet::stand_down`: a producer whose re-check succeeded, and whose
  `disarm` finds its bit already cleared, knows a waker spent a wake on it and runs the wake loop
  itself, handing the wake on.
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
  | observing
  | ignoresClose
  deriving DecidableEq, Repr

def Variant.standsDown : Variant → Bool
  | .repaired => true
  | _ => false

def Variant.seesClose : Variant → Bool
  | .ignoresClose => false
  | _ => true

inductive Waker where
  | scan
  | clear (i : Nat)
  | claim (i : Nat)
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
  bit : Bool
  handle : Bool
  token : Bool
  deriving DecidableEq, Repr

inductive Consumer where
  | release
  | waking (w : Waker)
  | close
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

def Producer.fresh : Producer := ⟨.attempt, false, false, false⟩

def Consumer.afterRelease (closes : Bool) (left : Nat) : Consumer :=
  if left = 0 then (if closes then .close else .finished) else .release

def State.init (n releases : Nat) (closes : Bool := false) : State :=
  ⟨0, releases, closes, false, Consumer.afterRelease closes releases,
    List.replicate n Producer.fresh⟩

def Producer.at (ps : List Producer) (i : Nat) : Producer := ps.getD i Producer.fresh

def Producer.update (ps : List Producer) (i : Nat) (f : Producer → Producer) : List Producer :=
  ps.set i (f (Producer.at ps i))

/-- One step of the wake loop on the producers' shared words. The result is the new producers
and the loop's next phase, `none` when the loop has finished. -/
def Waker.step (ps : List Producer) : Waker → List (List Producer × Option Waker)
  | .scan =>
    match (List.range ps.length).filter (fun i => (Producer.at ps i).bit) with
    | [] => [(ps, none)]
    | picks => picks.map fun i => (ps, some (.clear i))
  | .clear i =>
    if (Producer.at ps i).bit then
      [(Producer.update ps i (fun p => { p with bit := false }), some (.claim i))]
    else [(ps, some .scan)]
  | .claim i =>
    if (Producer.at ps i).handle then
      [(Producer.update ps i (fun p => { p with handle := false, token := true }), none)]
    else [(ps, some .scan)]

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
  | .handle => [(s.edit i fun p => { p with handle := true, pc := .bit })]
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
    if variant.standsDown && !p.bit then [(s.edit i fun p => { p with pc := .forward .scan })]
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

def State.consumerSteps (s : State) : List State :=
  match s.consumer with
  | .release =>
    [{ s with free := s.free + 1, left := s.left - 1, consumer := .waking .scan }]
  | .waking w =>
    (Waker.step s.producers w).map fun (ps, next) =>
      match next with
      | some w' => { s with producers := ps, consumer := .waking w' }
      | none =>
        { s with producers := ps, consumer := Consumer.afterRelease s.closes s.left }
  | .close => [{ s with closed := true, consumer := .flush }]
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

def State.succs (variant : Variant) (s : State) : List State :=
  s.consumerSteps ++ (List.range s.producers.length).flatMap (s.producerSteps variant)

inductive Reach (variant : Variant) (start : State) : State → Prop
  | init : Reach variant start start
  | step {s t : State} :
      Reach variant start s → t ∈ s.succs variant → Reach variant start t

/-- A producer is stuck for good with a reason to proceed: nothing can step, a producer has not
finished, and either a slot is free or the channel is closed. Nothing is left to wake it, because
only a waker or a flush unparks. -/
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
      exact Reach.run (Reach.step reach (State.mem_succs_of_run_step eq)) h

/-- Two producers, a ring with room for one, and two releases by the consumer. Producer 0 arms
and its re-check takes the slot the first release freed. Producer 1 arms and parks, finding the
ring full. The second release's wake retires producer 0's bit, which is still set, and unparks
producer 0 instead of producer 1. Producer 0 then stands down and producer 1 sleeps with a free
slot and no one left to wake it. -/
def lostSchedule : List (Nat × Nat) :=
  [(1, 0), (0, 0), (0, 0), (1, 0), (1, 0), (1, 0), (2, 0), (2, 0), (2, 0), (2, 0), (0, 0),
   (0, 0), (0, 0), (0, 0), (1, 0)]

def lostState : State :=
  ⟨1, 0, false, false, .finished, [⟨.done, false, false, true⟩, ⟨.park, true, true, false⟩]⟩

theorem lostSchedule_reaches_lostState :
    (State.init 2 2).run .unrepaired lostSchedule = some lostState := by decide +kernel

theorem lostState_is_lost : lostState.Lost .unrepaired := by
  unfold State.Lost
  decide +kernel

/-- A wake is lost: a producer is left parked for good with a free slot. -/
theorem wake_is_lost : ∃ s, Reach .unrepaired (State.init 2 2) s ∧ s.Lost .unrepaired :=
  ⟨lostState, Reach.run Reach.init lostSchedule_reaches_lostState, lostState_is_lost⟩

end Park

end Qtz
