/-!
Model of `broadcast::Producer::reserve_block`, whose `None` is documented to mean "every consumer
has dropped".

The loop checks for subscribers, observes space (`has_space`, a load that claims nothing) and then
claims a slot (`claim_slot`, a compare-exchange that returns `None` when the claim finds no room or
no subscriber). Between the observation and the claim a peer can take the slot, or the last
consumer can drop.

`Variant.returnsNone` is the code as it was: a failed claim returns `None`. `Variant.retries` is
`push_block`'s shape and the code now: a failed claim goes back to the top of the loop, which is
where the subscriber check reports a close.

The consumers drop at some point, or never. `closed` is the state of the subscriber check.

Not modelled: the park path (a producer that observes no space just stops here, since this model is
about what the claim reports), the claim's refusal on a peer's outstanding reservation on the
aliasing slot (it also reports `None`, and the retry covers it the same way), a consumer that
frees a slot, and memory orderings. `Park.Variant` covers the park path and its closed exit.
-/

namespace Qtz

namespace Reserve

inductive Variant where
  | returnsNone
  | retries
  deriving DecidableEq, Repr

inductive Pc where
  | top
  | observed
  | reserved
  | none
  | noRoom
  deriving DecidableEq, Repr

structure State where
  free : Nat
  closed : Bool
  producers : List Pc
  deriving DecidableEq, Repr

def State.init (free producers : Nat) : State := ⟨free, false, List.replicate producers .top⟩

def State.at (s : State) (i : Nat) : Pc := s.producers.getD i .top

def State.setPc (s : State) (i : Nat) (pc : Pc) : State :=
  { s with producers := s.producers.set i pc }

/-- Every successor of `s` by a step of producer `i`. -/
def State.steps (variant : Variant) (s : State) (i : Nat) : List State :=
  match s.at i with
  | .top =>
    if s.closed then [s.setPc i .none]
    else if 0 < s.free then [s.setPc i .observed]
    else [s.setPc i .noRoom]
  | .observed =>
    if 0 < s.free && !s.closed then [{ s.setPc i .reserved with free := s.free - 1 }]
    else
      match variant with
      | .returnsNone => [s.setPc i .none]
      | .retries => [s.setPc i .top]
  | _ => []

/-- The last consumer drops. -/
def State.drop (s : State) : List State :=
  if s.closed then [] else [{ s with closed := true }]

def State.succs (variant : Variant) (s : State) : List State :=
  s.drop ++ (List.range s.producers.length).flatMap (s.steps variant)

inductive Reach (variant : Variant) (free producers : Nat) : State → Prop
  | init : Reach variant free producers (State.init free producers)
  | step {s t : State} :
      Reach variant free producers s → t ∈ s.succs variant → Reach variant free producers t

/-- A producer reported a closed channel although no consumer dropped. -/
def State.FalseClose (s : State) : Prop := s.closed = false ∧ Pc.none ∈ s.producers

/-- One slot, two producers, consumers subscribed throughout. Both producers observe the slot
free. The first claims it. The second's claim finds no room and returns `None`. -/
theorem none_is_reported_on_an_open_channel :
    ∃ s, Reach .returnsNone 1 2 s ∧ s.FalseClose := by
  have a : Reach .returnsNone 1 2 ⟨1, false, [.observed, .top]⟩ :=
    .step .init (by decide +kernel)
  have b : Reach .returnsNone 1 2 ⟨1, false, [.observed, .observed]⟩ :=
    .step a (by decide +kernel)
  have c : Reach .returnsNone 1 2 ⟨0, false, [.reserved, .observed]⟩ :=
    .step b (by decide +kernel)
  have d : Reach .returnsNone 1 2 ⟨0, false, [.reserved, .none]⟩ :=
    .step c (by decide +kernel)
  exact ⟨_, d, by simp [State.FalseClose]⟩

/-- A producer is at `none` only after the consumers dropped. -/
def State.Honest (s : State) : Prop :=
  s.closed = false → ∀ pc ∈ s.producers, pc ≠ .none

theorem State.Honest.init (free producers : Nat) : (State.init free producers).Honest := by
  intro _ pc mem
  simp only [State.init, List.mem_replicate] at mem
  rw [mem.2]
  decide

theorem State.Honest.set {s : State} (honest : s.Honest) {i : Nat} {pc : Pc}
    (ok : s.closed = false → pc ≠ .none) : (s.setPc i pc).Honest := by
  intro open' other mem
  simp only [State.setPc] at open' mem
  rcases List.mem_or_eq_of_mem_set mem with old | new
  · exact honest open' other old
  · exact new ▸ ok open'

theorem State.Honest.step {s t : State} (honest : s.Honest) (mem : t ∈ s.succs .retries) :
    t.Honest := by
  simp only [State.succs, List.mem_append, List.mem_flatMap] at mem
  rcases mem with dropped | ⟨i, _, step⟩
  · unfold State.drop at dropped
    split at dropped
    · exact absurd dropped (by simp)
    · simp only [List.mem_singleton] at dropped
      subst dropped
      intro closed
      exact absurd closed (by simp)
  · unfold State.steps at step
    split at step <;> (try split at step) <;> (try split at step) <;>
      simp only [List.mem_singleton, List.not_mem_nil] at step <;> subst step <;>
      first
      | exact honest.set (fun _ => by decide)
      | exact honest.set (fun open' => by simp_all)

/-- With the retry, a producer reports a closed channel only after the consumers dropped, for any
number of producers and any number of free slots. A lost race goes back to the top of the loop,
where the subscriber check decides. -/
theorem retries_never_report_close {free producers : Nat} {s : State}
    (reach : Reach .retries free producers s) : ¬ s.FalseClose := by
  have honest : s.Honest := by
    induction reach with
    | init => exact State.Honest.init _ _
    | step _ mem ih => exact ih.step mem
  intro ⟨open', mem⟩
  exact honest open' _ mem rfl

/-- The report is not vacuous: after a drop a producer does report the close. -/
theorem retries_report_a_real_close :
    ∃ s, Reach .retries 1 1 s ∧ Pc.none ∈ s.producers := by
  have a : Reach .retries 1 1 ⟨1, true, [.top]⟩ := .step .init (by decide +kernel)
  have b : Reach .retries 1 1 ⟨1, true, [.none]⟩ := .step a (by decide +kernel)
  exact ⟨_, b, by simp⟩

/-- A retry gives no refuge to a producer that saw room: after a lost race the next step is the
loop top, which parks or claims, never a report. -/
theorem retry_returns_to_the_top {s : State} {i : Nat} (observed : s.at i = .observed)
    (lost : ¬ (0 < s.free ∧ s.closed = false)) :
    s.steps .retries i = [s.setPc i .top] := by
  simp only [State.steps, observed]
  have : (0 < s.free && !s.closed) = false := by
    cases hc : s.closed <;> simp_all
  simp [this]

end Reserve

end Qtz
