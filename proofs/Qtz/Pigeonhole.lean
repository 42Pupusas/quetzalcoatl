/-!
Counting facts the ring model needs, without a finite-set library.
-/

namespace Qtz

/-- A function that takes `count` different arguments to values below `limit`
cannot have more arguments than the limit allows. -/
theorem pigeonhole : ∀ (count limit : Nat) (value : Nat → Nat),
    (∀ index, index < count → value index < limit) →
    (∀ first second, first < count → second < count → value first = value second → first = second) →
    count ≤ limit
  | 0, limit, _, _, _ => Nat.zero_le limit
  | count + 1, limit, value, below, injective => by
    have last_below : value count < limit := below count (by omega)
    have squeezed := pigeonhole count (limit - 1)
      (fun index => if value count < value index then value index - 1 else value index)
      (by
        intro index index_below
        have bounded := below index (by omega)
        have different : value index ≠ value count := fun same =>
          absurd (injective index count (by omega) (by omega) same) (by omega)
        split <;> omega)
      (by
        intro first second first_below second_below same
        have first_different : value first ≠ value count := fun same_value =>
          absurd (injective first count (by omega) (by omega) same_value) (by omega)
        have second_different : value second ≠ value count := fun same_value =>
          absurd (injective second count (by omega) (by omega) same_value) (by omega)
        apply injective first second (by omega) (by omega)
        split at same <;> split at same <;> omega)
    omega

/-- Two positions in the same slot of a ring of `capacity` slots are a whole number of
rounds apart. -/
theorem position_split (capacity position : Nat) :
    position = (position / capacity) * capacity + position % capacity := by
  have := Nat.div_add_mod position capacity
  rw [Nat.mul_comm] at this
  omega

theorem same_slot_same_round {capacity first second : Nat}
    (same_slot : first % capacity = second % capacity)
    (same_round : first / capacity = second / capacity) : first = second := by
  rw [position_split capacity first, position_split capacity second, same_slot, same_round]

theorem same_slot_round_lt {capacity first second : Nat}
    (same_slot : first % capacity = second % capacity) (earlier : first < second) :
    first / capacity < second / capacity := by
  by_cases ordered : first / capacity < second / capacity
  · exact ordered
  · have not_after : second / capacity * capacity ≤ first / capacity * capacity :=
      Nat.mul_le_mul_right _ (by omega)
    have := position_split capacity first
    have := position_split capacity second
    omega

theorem same_slot_lap_apart {capacity first second : Nat}
    (same_slot : first % capacity = second % capacity) (earlier : first < second) :
    first + capacity ≤ second := by
  have rounds := same_slot_round_lt same_slot earlier
  have lap : (first / capacity + 1) * capacity ≤ second / capacity * capacity :=
    Nat.mul_le_mul_right _ rounds
  rw [Nat.add_mul, Nat.one_mul] at lap
  have := position_split capacity first
  have := position_split capacity second
  omega

end Qtz
