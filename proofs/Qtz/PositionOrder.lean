/-! Model of `src/common/position_order.rs`, for any word width. -/

namespace Qtz

def isAfter {width : Nat} (position reference : BitVec width) : Bool :=
  (0#width).slt (position - reference)

theorem isAfter_iff_distance_in_lower_half {width : Nat} (position reference : BitVec width) :
    isAfter position reference = true ↔
      0 < (position - reference).toNat ∧ 2 * (position - reference).toNat < 2 ^ width := by
  have distance_below_modulus := (position - reference).isLt
  have modulus_positive : 0 < 2 ^ width := Nat.two_pow_pos width
  have zero_as_int : (0#width).toInt = 0 := by
    simp [BitVec.toInt_eq_toNat_cond, modulus_positive]
  unfold isAfter
  rw [BitVec.slt_iff_toInt_lt, zero_as_int, BitVec.toInt_eq_toNat_cond (position - reference)]
  split <;> omega

theorem sub_swap_eq_neg {width : Nat} (position reference : BitVec width) :
    reference - position = -(position - reference) := by
  rw [BitVec.neg_sub, BitVec.sub_eq_add_neg, BitVec.add_comm]

theorem sub_swap_toNat {width : Nat} (position reference : BitVec width)
    (distance_nonzero : 0 < (position - reference).toNat) :
    (reference - position).toNat = 2 ^ width - (position - reference).toNat := by
  have distance_below_modulus := (position - reference).isLt
  rw [sub_swap_eq_neg, BitVec.toNat_neg]
  exact Nat.mod_eq_of_lt (by omega)

theorem isAfter_irrefl {width : Nat} (position : BitVec width) :
    isAfter position position = false := by
  cases is_after : isAfter position position
  · rfl
  · have distance_facts := (isAfter_iff_distance_in_lower_half position position).1 is_after
    rw [BitVec.sub_self, BitVec.toNat_zero] at distance_facts
    omega

theorem isAfter_asymm {width : Nat} (position reference : BitVec width)
    (position_after_reference : isAfter position reference = true) :
    isAfter reference position = false := by
  have forward_facts :=
    (isAfter_iff_distance_in_lower_half position reference).1 position_after_reference
  have backward_distance := sub_swap_toNat position reference forward_facts.1
  cases reference_after_position : isAfter reference position
  · rfl
  · have backward_facts :=
      (isAfter_iff_distance_in_lower_half reference position).1 reference_after_position
    omega

theorem isAfter_add_positive_step {width : Nat} (start step : BitVec width)
    (step_positive : (0#width).slt step = true) :
    isAfter (start + step) start = true := by
  have step_recovered : start + step - start = step := by
    rw [BitVec.add_comm]; exact BitVec.add_sub_cancel step start
  unfold isAfter
  rw [step_recovered]; exact step_positive

end Qtz
