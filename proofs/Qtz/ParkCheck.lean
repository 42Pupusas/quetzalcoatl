import Qtz.ParkModel

/-!
Kernel-checked certificates for `ParkModel`: for a fixed number of producers and releases, a finite
set of states that contains the initial state, is closed under every step, and holds no `Lost` state.
Every reachable state is then in the set, so none is lost.

The set is found by a breadth-first search run inside the kernel, but the search is not trusted:
only the three facts above are, and they are checked on the set it returns. The set is a binary
trie indexed by a hash of the state, so an insert or a lookup costs `Visited.depth` steps whatever
the size of the set. The hash need not be injective; a lookup that finds a state in a leaf has found
it in the set.

These are theorems about the model at the stated sizes, not at every size. The model has no memory
orderings, and a producer does not push again after it finishes.
-/

namespace Qtz

namespace Park

def Pc.code : Pc → Nat
  | .attempt => 0
  | .handle => 1
  | .bit => 2
  | .recheck => 3
  | .park => 4
  | .disarmWon => 5
  | .disarmWoken => 6
  | .done => 7
  | .disarmClosed => 100
  | .forward .scan => 8
  | .forward (.clear i) => 9 + 2 * i
  | .forward (.claim i) => 10 + 2 * i

def Producer.code (p : Producer) : Nat :=
  p.pc.code * 8 + p.bit.toNat * 4 + p.handle.toNat * 2 + p.token.toNat

def Consumer.code : Consumer → Nat
  | .release => 0
  | .finished => 1
  | .waking .scan => 2
  | .waking (.clear i) => 3 + 2 * i
  | .waking (.claim i) => 4 + 2 * i
  | .close => 101
  | .flush => 102
  | .flushing pending => 103 + pending.foldl (fun acc i => acc * 3 + i + 1) 0

def State.key (s : State) : Nat :=
  s.producers.foldl (fun acc p => acc * 131 + p.code)
    (((s.free * 9 + s.left) * 17 + s.consumer.code) * 2 + s.closed.toNat)

/-- A set of states: a binary trie whose path is the low bits of a hash of the state. A leaf met
above the bottom stands for an empty subtrie, so the empty set is one node. -/
inductive Visited where
  | leaf (states : List State)
  | node (low high : Visited)

/-- Bits of the hash the trie indexes by. -/
def Visited.depth : Nat := 17

def Visited.slot (t : State) : Nat := t.key % 131071

def Visited.empty : Visited := .leaf []

def Visited.addAt (t : State) : Nat → Nat → Visited → Visited
  | 0, _, .leaf states => .leaf (t :: states)
  | 0, _, .node low high => .node low high
  | fuel + 1, path, .leaf _ =>
    if path % 2 = 0 then .node (Visited.addAt t fuel (path / 2) .empty) .empty
    else .node .empty (Visited.addAt t fuel (path / 2) .empty)
  | fuel + 1, path, .node low high =>
    if path % 2 = 0 then .node (Visited.addAt t fuel (path / 2) low) high
    else .node low (Visited.addAt t fuel (path / 2) high)

def Visited.add (set : Visited) (t : State) : Visited :=
  Visited.addAt t Visited.depth (Visited.slot t) set

def Visited.hasAt (t : State) : Nat → Nat → Visited → Bool
  | _, _, .leaf states => states.contains t
  | 0, _, .node _ _ => false
  | fuel + 1, path, .node low high =>
    if path % 2 = 0 then Visited.hasAt t fuel (path / 2) low
    else Visited.hasAt t fuel (path / 2) high

def Visited.has (set : Visited) (t : State) : Bool :=
  Visited.hasAt t Visited.depth (Visited.slot t) set

/-- Every state held satisfies `check`. -/
def Visited.all (check : State → Bool) : Visited → Bool
  | .leaf states => states.all check
  | .node low high => low.all check && high.all check

/-- Breadth-first search for at most `fuel` rounds, from `frontier`. -/
def Visited.explore (variant : Variant) : Nat → Visited → List State → Visited
  | 0, set, _ => set
  | fuel + 1, set, frontier =>
    let (set', next) := frontier.foldl
      (fun (acc : Visited × List State) s =>
        (s.succs variant).foldl
          (fun (acc : Visited × List State) t =>
            if acc.1.has t then acc else (acc.1.add t, t :: acc.2))
          acc)
      (set, [])
    if next.isEmpty then set' else Visited.explore variant fuel set' next

def Visited.reachable (variant : Variant) (start : State) (fuel : Nat) : Visited :=
  Visited.explore variant fuel (Visited.empty.add start) [start]

/-- Stuck for good with a slot free; the decidable form of `State.Lost`. -/
def State.stuck (variant : Variant) (s : State) : Bool :=
  s.succs variant == [] && decide (0 < s.free ∨ s.closed = true) &&
    s.producers.any (fun p => p.pc != .done)

theorem State.stuck_of_lost {variant : Variant} {s : State} (lost : s.Lost variant) :
    s.stuck variant = true := by
  obtain ⟨empty, free, p, mem, unfinished⟩ := lost
  simp only [State.stuck, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq, List.any_eq_true,
    bne_iff_ne, ne_eq]
  exact ⟨⟨empty, free⟩, p, mem, unfinished⟩

/-- What the kernel checks: every state filed is closed under steps and none is stuck. -/
def Visited.certifies (variant : Variant) (set : Visited) : Bool :=
  set.all fun s => (s.succs variant).all set.has && !s.stuck variant

theorem Visited.hasAt_check {t : State} {check : State → Bool} :
    ∀ {fuel path : Nat} {set : Visited},
      Visited.hasAt t fuel path set = true → set.all check = true → check t = true
  | _, _, .leaf states, has, ok => by
    simp only [Visited.hasAt, List.contains_iff_mem] at has
    simp only [Visited.all, List.all_eq_true] at ok
    exact ok t has
  | 0, _, .node _ _, has, _ => by simp [Visited.hasAt] at has
  | fuel + 1, path, .node low high, has, ok => by
    simp only [Visited.all, Bool.and_eq_true] at ok
    simp only [Visited.hasAt] at has
    split at has
    · exact Visited.hasAt_check has ok.1
    · exact Visited.hasAt_check has ok.2

theorem Visited.has_check {set : Visited} {t : State} {check : State → Bool}
    (has : set.has t = true) (ok : set.all check = true) : check t = true :=
  Visited.hasAt_check has ok

theorem Visited.reach_has {variant : Variant} {origin : State} {set : Visited}
    (certified : set.certifies variant = true) (start : set.has origin = true)
    {s : State} (reach : Reach variant origin s) : set.has s = true := by
  induction reach with
  | init => exact start
  | step prior mem ih =>
    have ok := Visited.has_check ih certified
    simp only [Bool.and_eq_true, List.all_eq_true] at ok
    exact ok.1 _ mem

theorem Visited.not_lost {variant : Variant} {origin : State} {set : Visited}
    (certified : set.certifies variant = true) (start : set.has origin = true)
    {s : State} (reach : Reach variant origin s) : ¬ s.Lost variant := by
  intro lost
  have ok := Visited.has_check (Visited.reach_has certified start reach) certified
  have := State.stuck_of_lost lost
  simp [this] at ok

/-- The repair is never lost with two producers and two releases. -/
theorem repaired_never_lost_2_2 {s : State} (reach : Reach .repaired (State.init 2 2) s) :
    ¬ s.Lost .repaired :=
  Visited.not_lost (set := Visited.reachable .repaired (State.init 2 2) 400)
    (by decide +kernel) (by decide +kernel) reach

/-- The same search on the unrepaired protocol does not certify: its closed set contains a stuck
state. This is what keeps the theorem above from being vacuous. -/
theorem unrepaired_is_not_certified_2_2 :
    (Visited.reachable .unrepaired (State.init 2 2) 400).certifies .unrepaired = false := by
  decide +kernel

theorem repaired_never_lost_2_3 {s : State} (reach : Reach .repaired (State.init 2 3) s) :
    ¬ s.Lost .repaired :=
  Visited.not_lost (set := Visited.reachable .repaired (State.init 2 3) 400)
    (by decide +kernel) (by decide +kernel) reach

/-- Broadcast's `push_block`, whose re-check claims nothing and whose exit is plain `disarm`, is
never lost with two producers and two releases. -/
theorem observing_never_lost_2_2 {s : State} (reach : Reach .observing (State.init 2 2) s) :
    ¬ s.Lost .observing :=
  Visited.not_lost (set := Visited.reachable .observing (State.init 2 2) 400)
    (by decide +kernel) (by decide +kernel) reach

theorem observing_never_lost_2_3 {s : State} (reach : Reach .observing (State.init 2 3) s) :
    ¬ s.Lost .observing :=
  Visited.not_lost (set := Visited.reachable .observing (State.init 2 3) 400)
    (by decide +kernel) (by decide +kernel) reach

/-! The closed exit. The consumer releases some slots, then drops: it raises `closed` and flushes
the bitmap. A producer must leave through the loop top or the post-arm re-check, whatever the
interleaving with its `arm`. `Lost` already counts a producer stuck beside a closed channel. -/

/-- Broadcast's exits are never stuck beside a closed channel, with two producers and one
release before the drop. -/
theorem observing_never_stuck_on_close_2_1 {s : State}
    (reach : Reach .observing (State.init 2 1 true) s) : ¬ s.Lost .observing :=
  Visited.not_lost (set := Visited.reachable .observing (State.init 2 1 true) 400)
    (by decide +kernel) (by decide +kernel) reach

theorem observing_never_stuck_on_close_2_0 {s : State}
    (reach : Reach .observing (State.init 2 0 true) s) : ¬ s.Lost .observing :=
  Visited.not_lost (set := Visited.reachable .observing (State.init 2 0 true) 400)
    (by decide +kernel) (by decide +kernel) reach

/-- Mpsc's `push_block` with `stand_down`, under the same drop. -/
theorem repaired_never_stuck_on_close_2_1 {s : State}
    (reach : Reach .repaired (State.init 2 1 true) s) : ¬ s.Lost .repaired :=
  Visited.not_lost (set := Visited.reachable .repaired (State.init 2 1 true) 400)
    (by decide +kernel) (by decide +kernel) reach

/-- Without the closed check in the re-check, a producer that arms after the flush parks for good.
This keeps the closed-exit certificates above from being vacuous. -/
theorem ignoring_close_is_not_certified_2_0 :
    (Visited.reachable .ignoresClose (State.init 2 0 true) 400).certifies .ignoresClose = false := by
  decide +kernel

end Park

end Qtz
