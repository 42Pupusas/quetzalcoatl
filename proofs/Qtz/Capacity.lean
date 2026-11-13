/-! Model of `Capacity::MAX` in `src/capacity.rs`. -/

namespace Qtz

def maxCapacity (width : Nat) : Nat := 2 ^ (width - 3)

theorem capacity_in_lower_half_of_position_space {width capacity : Nat}
    (width_at_least_three : 3 ≤ width) (capacity_at_most_max : capacity ≤ maxCapacity width) :
    2 * capacity < 2 ^ width := by
  obtain ⟨extra_bits, width_split⟩ := Nat.exists_eq_add_of_le width_at_least_three
  subst width_split
  have modulus_split : 2 ^ (3 + extra_bits) = 2 ^ extra_bits * 8 := by
    rw [Nat.pow_add, Nat.mul_comm]
  have max_positive : 0 < 2 ^ extra_bits := Nat.two_pow_pos extra_bits
  unfold maxCapacity at capacity_at_most_max
  rw [Nat.add_sub_cancel_left] at capacity_at_most_max
  omega

end Qtz
