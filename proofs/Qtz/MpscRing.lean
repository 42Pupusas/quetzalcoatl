import Qtz.MpscSlots

/-!
The mpsc ring (`src/mpsc/{claim_ledger,producer,consumer,batch_release,slot_release}.rs`) as a
state machine over unwrapped positions.

Room is a count of credits, one per free slot. `credits` are the ones in the claim word,
`pending` the ones the consumer has added and no producer has moved in yet, and a producer in
the middle of a refill holds the ones it took out of `pending`. `published` is the head the
consumer has credited the producers up to. It trails `head`: the consumer releases slot words
first and `ClaimLedger::publish_head` credits them afterwards.

Each step is one atomic operation of the Rust.

* `claim`: the compare-exchange that takes a credit and advances the position.
* `mark`: the compare-exchange that sets the refill bit on a word with no credit.
* `collect`: the holder of the bit reads `pending`. A read of zero and a swap that takes
  everything are the same step, with zero or more credits taken.
* `reportFull`, `grant`: the holder's store that clears the bit. With nothing taken it leaves
  the position alone and the producer reports a full ring. Otherwise it claims the position
  with one credit and stores the rest.
* `finish`: the claim's owner publishes a value or tombstones the reservation.
* `consume`: the consumer releases the head position, whether it holds a value or a tombstone.
* `publish`: the consumer's `fetch_add` on `pending`.

No step has a hypothesis about how long ago a producer read the word, and the model has no
word width: credits are counted, never compared modulo anything.
-/

namespace Qtz

namespace Mpsc

inductive Refill where
  | idle
  | marked
  | holding (granted : Nat)

/-- The credits a refilling producer has taken out of `pending` and not yet stored. -/
def Refill.held : Refill → Nat
  | .holding granted => granted
  | _ => 0

/-- Whether the refill bit is set. -/
def Refill.busy : Refill → Bool
  | .idle => false
  | _ => true

structure Ring (capacity : Nat) where
  head : Nat
  published : Nat
  tail : Nat
  credits : Nat
  pending : Nat
  refill : Refill
  word : Nat → Nat × Nat
  claimed : Nat → Bool

variable {capacity : Nat}

def Ring.initial (capacity : Nat) : Ring capacity :=
  { head := 0, published := 0, tail := 0, credits := capacity, pending := 0, refill := .idle,
    word := fun index => (index, 0), claimed := fun _ => false }

inductive Step : Ring capacity → Ring capacity → Prop
  | claim {s : Ring capacity} (has_credit : 0 < s.credits) :
      Step s { s with tail := s.tail + 1, credits := s.credits - 1,
                      claimed := setClaimed s.claimed s.tail true }
  | mark {s : Ring capacity} (no_credit : s.credits = 0) (idle : s.refill = .idle) :
      Step s { s with refill := .marked }
  | collect {s : Ring capacity} (marked : s.refill = .marked) :
      Step s { s with refill := .holding s.pending, pending := 0 }
  | reportFull {s : Ring capacity} (nothing : s.refill = .holding 0) :
      Step s { s with refill := .idle }
  | grant {s : Ring capacity} (granted : Nat) (holding : s.refill = .holding (granted + 1)) :
      Step s { s with refill := .idle, tail := s.tail + 1, credits := granted,
                      claimed := setClaimed s.claimed s.tail true }
  | finish {s : Ring capacity} (position state : Nat) (owned : s.claimed position = true)
      (used : state = 1 ∨ state = 2) :
      Step s { s with word := setWord s.word (position % capacity) (position, state),
                      claimed := setClaimed s.claimed position false }
  | consume {s : Ring capacity} (state : Nat)
      (at_head : s.word (s.head % capacity) = (s.head, state)) (used : state = 1 ∨ state = 2) :
      Step s { s with word := setWord s.word (s.head % capacity) (s.head + capacity, 0),
                      head := s.head + 1 }
  | publish {s : Ring capacity} :
      Step s { s with published := s.head, pending := s.pending + (s.head - s.published) }

inductive Reachable (capacity : Nat) : Ring capacity → Prop
  | initial : Reachable capacity (Ring.initial capacity)
  | step {s t : Ring capacity} : Reachable capacity s → Step s t → Reachable capacity t

/-- `conserved` is the credit ledger: every slot the consumer has credited is either claimed
or is a credit in exactly one of the three places a credit can be. -/
structure Inv (s : Ring capacity) : Prop where
  slots : Slots capacity s.head s.tail s.word s.claimed
  published_le_head : s.published ≤ s.head
  conserved : s.tail + s.credits + s.pending + s.refill.held = s.published + capacity
  refilling_no_credit : s.refill ≠ .idle → s.credits = 0

theorem Inv.initial : Inv (Ring.initial capacity) :=
  ⟨Slots.initial, Nat.le_refl _, by simp [Ring.initial, Refill.held], fun busy => absurd rfl busy⟩

theorem Inv.credit_bounds {s : Ring capacity} (inv : Inv s) :
    s.credits ≤ capacity ∧ s.pending ≤ capacity ∧ s.refill.held ≤ capacity := by
  have := inv.conserved
  have := inv.published_le_head
  have := inv.slots.head_le_tail
  omega

theorem Inv.idle_of_credit {s : Ring capacity} (inv : Inv s) (has_credit : s.credits ≠ 0) :
    s.refill = .idle := by
  cases refill : s.refill with
  | idle => rfl
  | marked => exact absurd (inv.refilling_no_credit (by simp [refill])) has_credit
  | holding granted => exact absurd (inv.refilling_no_credit (by simp [refill])) has_credit

theorem Inv.step {s t : Ring capacity} (inv : Inv s) (capacity_positive : 0 < capacity)
    (step : Step s t) : Inv t := by
  have conserved := inv.conserved
  have published_le := inv.published_le_head
  cases step with
  | claim has_credit =>
    refine ⟨inv.slots.take (by omega), published_le, by simp only; omega, fun busy => ?_⟩
    have := inv.refilling_no_credit busy
    omega
  | mark no_credit idle =>
    rw [idle] at conserved
    exact ⟨inv.slots, published_le, conserved, fun _ => no_credit⟩
  | collect marked =>
    rw [marked] at conserved
    simp only [Refill.held] at conserved
    refine ⟨inv.slots, published_le, by simp only [Refill.held]; omega, fun _ => ?_⟩
    exact inv.refilling_no_credit (by simp [marked])
  | reportFull nothing =>
    rw [nothing] at conserved
    exact ⟨inv.slots, published_le, conserved, fun busy => absurd rfl busy⟩
  | grant granted holding =>
    rw [holding] at conserved
    simp only [Refill.held] at conserved
    have := inv.refilling_no_credit (by simp [holding])
    exact ⟨inv.slots.take (by omega), published_le, by simp only [Refill.held]; omega,
      fun busy => absurd rfl busy⟩
  | finish position state owned used =>
    exact ⟨inv.slots.finish position state owned used, published_le, conserved,
      inv.refilling_no_credit⟩
  | consume state at_head used =>
    exact ⟨inv.slots.consume capacity_positive state at_head used, by simp only; omega,
      conserved, inv.refilling_no_credit⟩
  | publish =>
    exact ⟨inv.slots, Nat.le_refl _, by simp only; omega, inv.refilling_no_credit⟩

theorem Reachable.inv {s : Ring capacity} (capacity_positive : 0 < capacity)
    (reachable : Reachable capacity s) : Inv s := by
  induction reachable with
  | initial => exact Inv.initial
  | step _ step inv => exact inv.step capacity_positive step

/-- The slot of a claimed position is free for exactly that position, so a producer's write
never lands on a value the consumer has not yet released. -/
theorem Inv.claimed_slot_free {s : Ring capacity} (inv : Inv s) {position : Nat}
    (owned : s.claimed position = true) : s.word (position % capacity) = (position, 0) :=
  inv.slots.claimed_slot_free owned

/-- A producer reports a full ring only after reading `pending` as zero while it holds the
refill bit. At that read every slot the consumer has credited is claimed. -/
theorem Inv.full_of_nothing_pending {s : Ring capacity} (inv : Inv s)
    (marked : s.refill = .marked) (nothing : s.pending = 0) :
    s.tail = s.published + capacity := by
  have conserved := inv.conserved
  have := inv.refilling_no_credit (by simp [marked])
  rw [marked] at conserved
  simp only [Refill.held] at conserved
  omega

theorem Step.tail_of_refilling {s t : Ring capacity} (inv : Inv s) (step : Step s t)
    (busy : s.refill ≠ .idle) (still_busy : t.refill ≠ .idle) : t.tail = s.tail := by
  have no_credit := inv.refilling_no_credit busy
  cases step with
  | claim has_credit => omega
  | mark => rfl
  | collect => rfl
  | reportFull => exact absurd rfl still_busy
  | grant => exact absurd rfl still_busy
  | finish => rfl
  | consume => rfl
  | publish => rfl

/-- A run of steps, by any thread, through which the refill bit stays set. -/
inductive Refilling : Ring capacity → Ring capacity → Prop
  | start {s : Ring capacity} (busy : s.refill ≠ .idle) : Refilling s s
  | step {s t u : Ring capacity} :
      Refilling s t → Step t u → u.refill ≠ .idle → Refilling s u

/-- While the refill bit stays set nobody claims, so the position its holder read when it set
the bit is still the next one to claim when it stores the word. -/
theorem Refilling.tail {s t : Ring capacity} (capacity_positive : 0 < capacity) (inv : Inv s)
    (window : Refilling s t) : t.tail = s.tail ∧ Inv t ∧ t.refill ≠ .idle := by
  induction window with
  | start busy => exact ⟨rfl, inv, busy⟩
  | step _ step still_busy before =>
    obtain ⟨same, inv_before, busy⟩ := before
    exact ⟨(step.tail_of_refilling inv_before busy still_busy).trans same,
      inv_before.step capacity_positive step, still_busy⟩

end Mpsc

end Qtz
