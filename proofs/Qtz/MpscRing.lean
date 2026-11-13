import Qtz.MpscClaim
import Qtz.MpscSlotWord

/-!
The mpsc ring (`src/mpsc/{producer,consumer,batch_release,reservation_release}.rs`) as a
state machine over unwrapped positions.

`word index` is the `(position, state)` pair behind the slot's sequence word, with state 0
free, 1 published, 2 abandoned. `claimed p` holds from the moment a producer wins position `p`
until it publishes or tombstones it. The window `[head, head + capacity)` names one position
per slot.

`published` is the head cursor producers read (`Cursors::publish_head`). It trails `head`: the
consumer releases slots first and `BatchRelease::drop` publishes afterwards. A producer's claim
is bounded by `published`, not by `head`.

Steps: a producer claims the tail position; the claim's owner finishes it, by publishing a
value (`WrittenSlot::commit`) or tombstoning it (`SlotWriter::drop`,
`ReservationRelease::drop`); the consumer releases the head position, whether it holds a value
(`pop`, `BatchRelease::take`) or a tombstone (`BatchRelease::settle`); the consumer publishes
its head.
-/

namespace Qtz

namespace Mpsc

structure Ring (capacity : Nat) where
  head : Nat
  published : Nat
  tail : Nat
  word : Nat → Nat × Nat
  claimed : Nat → Bool

variable {capacity : Nat}

def Ring.initial (capacity : Nat) : Ring capacity :=
  { head := 0, published := 0, tail := 0, word := fun index => (index, 0), claimed := fun _ => false }

def Ring.setWord (s : Ring capacity) (index : Nat) (value : Nat × Nat) : Ring capacity :=
  { s with word := fun other => if other = index then value else s.word other }

inductive Step : Ring capacity → Ring capacity → Prop
  | claim {s : Ring capacity} (room : s.tail < s.published + capacity) :
      Step s { s with tail := s.tail + 1,
                      claimed := fun p => if p = s.tail then true else s.claimed p }
  | finish {s : Ring capacity} (position state : Nat) (owned : s.claimed position = true)
      (used : state = 1 ∨ state = 2) :
      Step s { s.setWord (position % capacity) (position, state) with
                claimed := fun p => if p = position then false else s.claimed p }
  | consume {s : Ring capacity} (state : Nat) (pending : s.head < s.tail)
      (at_head : s.word (s.head % capacity) = (s.head, state)) (used : state = 1 ∨ state = 2) :
      Step s { s.setWord (s.head % capacity) (s.head + capacity, 0) with head := s.head + 1 }
  | publish {s : Ring capacity} : Step s { s with published := s.head }

inductive Reachable (capacity : Nat) : Ring capacity → Prop
  | initial : Reachable capacity (Ring.initial capacity)
  | step {s t : Ring capacity} : Reachable capacity s → Step s t → Reachable capacity t

structure Inv (s : Ring capacity) : Prop where
  head_le_tail : s.head ≤ s.tail
  published_le_head : s.published ≤ s.head
  tail_le_published : s.tail ≤ s.published + capacity
  position : ∀ p, s.head ≤ p → p < s.head + capacity → (s.word (p % capacity)).1 = p
  state_below : ∀ p, s.head ≤ p → p < s.head + capacity → (s.word (p % capacity)).2 < 3
  free_iff : ∀ p, s.head ≤ p → p < s.head + capacity →
    ((s.word (p % capacity)).2 = 0 ↔ (s.tail ≤ p ∨ s.claimed p = true))
  claimed_in : ∀ p, s.claimed p = true → s.head ≤ p ∧ p < s.tail

theorem window_unique {head p q : Nat} (first_low : head ≤ p) (first_high : p < head + capacity)
    (second_low : head ≤ q) (second_high : q < head + capacity)
    (same_slot : p % capacity = q % capacity) : p = q := by
  by_cases ordered : p ≤ q
  · exact (eq_of_mod_eq_of_close ordered (by omega) same_slot).symm
  · exact eq_of_mod_eq_of_close (by omega) (by omega) same_slot.symm

theorem next_lap_of_same_slot {head p : Nat} (capacity_positive : 0 < capacity)
    (low : head + 1 ≤ p) (high : p < head + 1 + capacity)
    (same : p % capacity = head % capacity) : p = head + capacity := by
  have next : (head + capacity) % capacity = head % capacity := Nat.add_mod_right _ _
  exact window_unique (head := head + 1) (by omega) (by omega) (by omega) (by omega)
    (same.trans next.symm)

theorem Inv.tail_le_full {s : Ring capacity} (inv : Inv s) : s.tail ≤ s.head + capacity := by
  have := inv.published_le_head
  have := inv.tail_le_published
  omega

theorem Inv.initial : Inv (Ring.initial capacity) := by
  refine ⟨Nat.le_refl _, Nat.le_refl _, by simp [Ring.initial], ?_, ?_, ?_, ?_⟩
  · intro p _ below
    simp only [Ring.initial] at below ⊢
    rw [Nat.mod_eq_of_lt (by omega)]
  · intro p _ _
    simp [Ring.initial]
  · intro p _ _
    simp [Ring.initial]
  · intro p claimed
    simp [Ring.initial] at claimed

theorem Inv.claim {s : Ring capacity} (inv : Inv s) (room : s.tail < s.published + capacity) :
    Inv (capacity := capacity)
      { s with
        tail := s.tail + 1
        claimed := fun p => if p = s.tail then true else s.claimed p } := by
  have published_le := inv.published_le_head
  refine ⟨by have := inv.head_le_tail; simp only; omega, inv.published_le_head,
    by simp only; omega, ?_, ?_, ?_, ?_⟩
  · intro p low high
    exact inv.position p low high
  · intro p low high
    exact inv.state_below p low high
  · intro p low high
    simp only
    have old := inv.free_iff p low high
    by_cases is_tail : p = s.tail
    · simp only [is_tail, ite_true] at old ⊢
      constructor
      · intro; exact Or.inr trivial
      · intro; exact old.2 (Or.inl (Nat.le_refl _))
    · simp only [is_tail, ite_false]
      constructor
      · intro zero
        rcases old.1 zero with le | owned
        · exact Or.inl (by omega)
        · exact Or.inr owned
      · intro h
        apply old.2
        rcases h with le | owned
        · exact Or.inl (by omega)
        · exact Or.inr owned
  · intro p owned
    have := inv.head_le_tail
    simp only at owned ⊢
    by_cases is_tail : p = s.tail
    · omega
    · simp only [is_tail, ite_false] at owned
      have := inv.claimed_in p owned
      omega

theorem Inv.finish {s : Ring capacity} (inv : Inv s)
    (position state : Nat) (owned : s.claimed position = true) (used : state = 1 ∨ state = 2) :
    Inv (capacity := capacity)
      { s.setWord (position % capacity) (position, state) with
        claimed := fun p => if p = position then false else s.claimed p } := by
  obtain ⟨low, below_tail⟩ := inv.claimed_in position owned
  have tail_le := inv.tail_le_full
  have in_window : position < s.head + capacity := by omega
  refine ⟨inv.head_le_tail, inv.published_le_head, inv.tail_le_published, ?_, ?_, ?_, ?_⟩
  · intro p p_low p_high
    simp only [Ring.setWord]
    by_cases same : p % capacity = position % capacity
    · have := window_unique p_low p_high low in_window same
      simp [this]
    · simp [same, inv.position p p_low p_high]
  · intro p p_low p_high
    simp only [Ring.setWord]
    by_cases same : p % capacity = position % capacity
    · simp [same]; omega
    · simp [same, inv.state_below p p_low p_high]
  · intro p p_low p_high
    simp only [Ring.setWord]
    have old := inv.free_iff p p_low p_high
    by_cases same : p % capacity = position % capacity
    · have is_position := window_unique p_low p_high low in_window same
      subst is_position
      simp
      omega
    · have different : p ≠ position := fun h => same (by rw [h])
      simp [same, different, old]
  · intro p claimed_p
    simp only at claimed_p
    by_cases same : p = position
    · simp [same] at claimed_p
    · simp [same] at claimed_p
      exact inv.claimed_in p claimed_p

theorem Inv.consume {s : Ring capacity} (inv : Inv s) (capacity_positive : 0 < capacity)
    (state : Nat) (pending : s.head < s.tail)
    (at_head : s.word (s.head % capacity) = (s.head, state)) (used : state = 1 ∨ state = 2) :
    Inv (capacity := capacity)
      { s.setWord (s.head % capacity) (s.head + capacity, 0) with head := s.head + 1 } := by
  have tail_le := inv.tail_le_full
  have head_not_free : (s.word (s.head % capacity)).2 ≠ 0 := by
    rw [at_head]; simp only; omega
  have head_not_claimed : s.claimed s.head = false := by
    have old := inv.free_iff s.head (Nat.le_refl _) (by omega)
    cases claimed : s.claimed s.head
    · rfl
    · exact absurd (old.2 (Or.inr claimed)) head_not_free
  have published_le := inv.published_le_head
  have bounded := inv.tail_le_published
  refine ⟨by simp only [Ring.setWord]; omega, by simp only [Ring.setWord]; omega,
    by simp only [Ring.setWord]; omega, ?_, ?_, ?_, ?_⟩
  · intro p low high
    simp only [Ring.setWord] at low high ⊢
    by_cases same : p % capacity = s.head % capacity
    · have is_next := next_lap_of_same_slot capacity_positive low high same
      simp [is_next]
    · have not_head : p ≠ s.head + capacity := fun h => same (by rw [h]; exact Nat.add_mod_right _ _)
      simp [same, inv.position p (by omega) (by omega)]
  · intro p low high
    simp only [Ring.setWord] at low high ⊢
    by_cases same : p % capacity = s.head % capacity
    · simp [same]
    · have not_head : p ≠ s.head + capacity := fun h => same (by rw [h]; exact Nat.add_mod_right _ _)
      simp [same, inv.state_below p (by omega) (by omega)]
  · intro p low high
    simp only [Ring.setWord] at low high ⊢
    by_cases same : p % capacity = s.head % capacity
    · have is_next := next_lap_of_same_slot capacity_positive low high same
      subst is_next
      simp [same]
      omega
    · have not_head : p ≠ s.head + capacity := fun h => same (by rw [h]; exact Nat.add_mod_right _ _)
      simp only [same, ite_false]
      exact inv.free_iff p (by omega) (by omega)
  · intro p owned
    simp only [Ring.setWord] at owned ⊢
    have old := inv.claimed_in p owned
    have : p ≠ s.head := fun h => by rw [h] at owned; rw [head_not_claimed] at owned; cases owned
    omega

/-- The slot of a claimed position is free for exactly that position, so a producer's write
never lands on a value the consumer has not yet released. -/
theorem Inv.claimed_slot_free {s : Ring capacity} (inv : Inv s) {position : Nat}
    (owned : s.claimed position = true) :
    s.word (position % capacity) = (position, 0) := by
  obtain ⟨low, below_tail⟩ := inv.claimed_in position owned
  have high : position < s.head + capacity := by have := inv.tail_le_full; omega
  exact Prod.ext (inv.position position low high)
    ((inv.free_iff position low high).2 (Or.inr owned))

theorem Inv.publish {s : Ring capacity} (inv : Inv s) : Inv (capacity := capacity) { s with published := s.head } := by
  have := inv.tail_le_full
  exact ⟨inv.head_le_tail, Nat.le_refl _, by simp only; omega, inv.position, inv.state_below,
    inv.free_iff, inv.claimed_in⟩

theorem Inv.step {s t : Ring capacity} (inv : Inv s) (capacity_positive : 0 < capacity)
    (step : Step s t) : Inv t := by
  cases step with
  | claim room => exact inv.claim room
  | finish position state owned used => exact inv.finish position state owned used
  | consume state pending at_head used => exact inv.consume capacity_positive state pending at_head used
  | publish => exact inv.publish

theorem Reachable.inv {s : Ring capacity} (capacity_positive : 0 < capacity)
    (reachable : Reachable capacity s) : Inv s := by
  induction reachable with
  | initial => exact Inv.initial
  | step _ step inv => exact inv.step capacity_positive step

end Mpsc

end Qtz
