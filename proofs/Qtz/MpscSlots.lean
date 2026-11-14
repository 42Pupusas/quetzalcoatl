import Qtz.MpmcSlot

/-!
The slot words of an mpsc ring over unwrapped positions, apart from how a claim is decided.

`word index` is the `(position, state)` pair behind the slot's sequence word, with state 0
free, 1 published, 2 abandoned. `claimed p` holds from the moment a producer wins position `p`
until it publishes or tombstones it. `head` is the next position the consumer reads and `tail`
the next one a producer claims. The window `[head, head + capacity)` names one position per
slot.

`Slots` is what the words satisfy. It is kept by a claim that has room (`take`), by the
claim's owner publishing or tombstoning it (`finish`), and by the consumer releasing the head
position (`consume`). The consumer does not compare `head` with `tail`: a published or
abandoned word at the head is enough, and `consume` derives `head < tail` from it.
-/

namespace Qtz

namespace Mpsc

def setWord (word : Nat → Nat × Nat) (index : Nat) (value : Nat × Nat) : Nat → Nat × Nat :=
  fun other => if other = index then value else word other

def setClaimed (claimed : Nat → Bool) (position : Nat) (value : Bool) : Nat → Bool :=
  fun other => if other = position then value else claimed other

structure Slots (capacity head tail : Nat) (word : Nat → Nat × Nat) (claimed : Nat → Bool) :
    Prop where
  head_le_tail : head ≤ tail
  tail_le_full : tail ≤ head + capacity
  position : ∀ p, head ≤ p → p < head + capacity → (word (p % capacity)).1 = p
  state_below : ∀ p, head ≤ p → p < head + capacity → (word (p % capacity)).2 < 3
  free_iff : ∀ p, head ≤ p → p < head + capacity →
    ((word (p % capacity)).2 = 0 ↔ (tail ≤ p ∨ claimed p = true))
  claimed_in : ∀ p, claimed p = true → head ≤ p ∧ p < tail

variable {capacity head tail : Nat} {word : Nat → Nat × Nat} {claimed : Nat → Bool}

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

theorem Slots.initial : Slots capacity 0 0 (fun index => (index, 0)) (fun _ => false) := by
  refine ⟨Nat.le_refl _, by omega, ?_, ?_, ?_, ?_⟩
  · intro p _ below
    simp only
    rw [Nat.mod_eq_of_lt (by omega)]
  · intro p _ _
    simp
  · intro p _ _
    simp
  · intro p owned
    simp at owned

theorem Slots.take (slots : Slots capacity head tail word claimed)
    (room : tail < head + capacity) :
    Slots capacity head (tail + 1) word (setClaimed claimed tail true) := by
  have head_le_tail := slots.head_le_tail
  refine ⟨by omega, by omega, slots.position, slots.state_below, ?_, ?_⟩
  · intro p low high
    have old := slots.free_iff p low high
    simp only [setClaimed]
    by_cases is_tail : p = tail
    · subst is_tail
      simp only [ite_true, or_true, iff_true]
      exact old.2 (Or.inl (Nat.le_refl _))
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
    simp only [setClaimed] at owned
    by_cases is_tail : p = tail
    · omega
    · simp only [is_tail, ite_false] at owned
      have := slots.claimed_in p owned
      omega

theorem Slots.finish (slots : Slots capacity head tail word claimed) (position state : Nat)
    (owned : claimed position = true) (used : state = 1 ∨ state = 2) :
    Slots capacity head tail (setWord word (position % capacity) (position, state))
      (setClaimed claimed position false) := by
  obtain ⟨low, below_tail⟩ := slots.claimed_in position owned
  have tail_le := slots.tail_le_full
  have in_window : position < head + capacity := by omega
  refine ⟨slots.head_le_tail, slots.tail_le_full, ?_, ?_, ?_, ?_⟩
  · intro p p_low p_high
    simp only [setWord]
    by_cases same : p % capacity = position % capacity
    · have := window_unique p_low p_high low in_window same
      simp [this]
    · simp [same, slots.position p p_low p_high]
  · intro p p_low p_high
    simp only [setWord]
    by_cases same : p % capacity = position % capacity
    · simp [same]; omega
    · simp [same, slots.state_below p p_low p_high]
  · intro p p_low p_high
    simp only [setWord, setClaimed]
    have old := slots.free_iff p p_low p_high
    by_cases same : p % capacity = position % capacity
    · have is_position := window_unique p_low p_high low in_window same
      subst is_position
      simp
      omega
    · have different : p ≠ position := fun h => same (by rw [h])
      simp [same, different, old]
  · intro p claimed_p
    simp only [setClaimed] at claimed_p
    by_cases same : p = position
    · simp [same] at claimed_p
    · simp [same] at claimed_p
      exact slots.claimed_in p claimed_p

theorem Slots.consume (slots : Slots capacity head tail word claimed)
    (capacity_positive : 0 < capacity) (state : Nat)
    (at_head : word (head % capacity) = (head, state)) (used : state = 1 ∨ state = 2) :
    Slots capacity (head + 1) tail (setWord word (head % capacity) (head + capacity, 0))
      claimed := by
  have tail_le := slots.tail_le_full
  have head_not_free : (word (head % capacity)).2 ≠ 0 := by
    rw [at_head]; simp only; omega
  have head_free_iff := slots.free_iff head (Nat.le_refl _) (by omega)
  have pending : head < tail :=
    (Nat.lt_or_ge head tail).resolve_right
      (fun reached => head_not_free (head_free_iff.2 (Or.inl reached)))
  have head_not_claimed : claimed head = false := by
    cases owned : claimed head
    · rfl
    · exact absurd (head_free_iff.2 (Or.inr owned)) head_not_free
  have other_slot : ∀ p, ¬ p % capacity = head % capacity → p ≠ head + capacity :=
    fun p different same => different (by rw [same]; exact Nat.add_mod_right _ _)
  refine ⟨by omega, by omega, ?_, ?_, ?_, ?_⟩
  · intro p low high
    simp only [setWord]
    by_cases same : p % capacity = head % capacity
    · have is_next := next_lap_of_same_slot capacity_positive low high same
      simp [is_next]
    · have := other_slot p same
      simp [same, slots.position p (by omega) (by omega)]
  · intro p low high
    simp only [setWord]
    by_cases same : p % capacity = head % capacity
    · simp [same]
    · have := other_slot p same
      simp [same, slots.state_below p (by omega) (by omega)]
  · intro p low high
    simp only [setWord]
    by_cases same : p % capacity = head % capacity
    · have is_next := next_lap_of_same_slot capacity_positive low high same
      subst is_next
      simp [same]
      omega
    · have := other_slot p same
      simp only [same, ite_false]
      exact slots.free_iff p (by omega) (by omega)
  · intro p owned
    have old := slots.claimed_in p owned
    have : p ≠ head := fun h => by rw [h] at owned; rw [head_not_claimed] at owned; cases owned
    omega

/-- The slot of a claimed position is free for exactly that position, so a producer's write
never lands on a value the consumer has not yet released. -/
theorem Slots.claimed_slot_free (slots : Slots capacity head tail word claimed) {position : Nat}
    (owned : claimed position = true) : word (position % capacity) = (position, 0) := by
  obtain ⟨low, below_tail⟩ := slots.claimed_in position owned
  have high : position < head + capacity := by have := slots.tail_le_full; omega
  exact Prod.ext (slots.position position low high)
    ((slots.free_iff position low high).2 (Or.inr owned))

end Mpsc

end Qtz
