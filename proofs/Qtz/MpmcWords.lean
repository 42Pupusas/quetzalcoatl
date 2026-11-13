import Qtz.Capacity
import Qtz.ConsumerFloor

/-!
Model of the mpmc slot words: `ReadyWord` (`src/mpmc/ready_word.rs`) and
`DoneWord` (`src/mpmc/done_word.rs`).

A capacity is `2 ^ log`. `RingBuffer::new` asserts `capacity >= 4`, which
is `2 ≤ log` here, so the three ready states (offsets 0, 1, 2) stay below
the capacity.
-/

namespace Qtz

def wrap {width : Nat} (capacity delta : BitVec width) : BitVec width :=
  delta &&& (capacity - 1#width)

theorem wrap_toNat {width log : Nat} (capacity delta : BitVec width)
    (capacity_is_power : capacity.toNat = 2 ^ log) (log_below_width : log < width) :
    (wrap capacity delta).toNat = delta.toNat % 2 ^ log := by
  have one_toNat : (1#width).toNat = 1 := by
    have : 2 ≤ 2 ^ width := by
      calc 2 = 2 ^ 1 := rfl
        _ ≤ 2 ^ width := Nat.pow_le_pow_right (by omega) (by omega)
    simp only [BitVec.toNat_ofNat]; exact Nat.mod_eq_of_lt (by omega)
  have power_positive := Nat.two_pow_pos log
  have mask_toNat : (capacity - 1#width).toNat = 2 ^ log - 1 := by
    rw [wrapped_sub_toNat, one_toNat, capacity_is_power]
    split <;> omega
  unfold wrap
  rw [BitVec.toNat_and, mask_toNat, Nat.and_two_pow_sub_one_eq_mod]

theorem sub_aligned_mod {width log : Nat} (round_position slot : BitVec width)
    (log_below_width : log < width)
    (aligned : round_position.toNat % 2 ^ log = slot.toNat) :
    (round_position - slot).toNat % 2 ^ log = 0 := by
  have round_bound := round_position.isLt
  have slot_bound := slot.isLt
  have modulus_split : 2 ^ width = 2 ^ log * 2 ^ (width - log) := by
    rw [← Nat.pow_add]; congr 1; omega
  have round_split := Nat.div_add_mod round_position.toNat (2 ^ log)
  have slot_below := Nat.mod_lt round_position.toNat (Nat.two_pow_pos log)
  rw [wrapped_sub_toNat]
  generalize 2 ^ (width - log) = laps at *
  generalize 2 ^ log = period at *
  generalize round_position.toNat / period = round_lap at *
  rw [aligned] at round_split
  generalize round_position.toNat = round_value at *
  generalize slot.toNat = slot_value at *
  split
  · rw [← round_split, Nat.add_sub_cancel]; exact Nat.mul_mod_right _ _
  · omega

theorem wrap_state_of_aligned {width log : Nat} (capacity round_position slot : BitVec width)
    (state : Nat) (capacity_is_power : capacity.toNat = 2 ^ log) (log_below_width : log < width)
    (state_below_capacity : state < 2 ^ log)
    (aligned : round_position.toNat % 2 ^ log = slot.toNat) :
    wrap capacity (round_position + BitVec.ofNat width state - slot) = BitVec.ofNat width state := by
  have modulus_split : 2 ^ width = 2 ^ log * 2 ^ (width - log) := by
    rw [← Nat.pow_add]; congr 1; omega
  have power_positive := Nat.two_pow_pos (width - log)
  have state_below_modulus : state < 2 ^ width := by
    have := Nat.two_pow_pos log
    rw [modulus_split]
    exact Nat.lt_of_lt_of_le state_below_capacity (Nat.le_mul_of_pos_right _ power_positive)
  have regrouped : round_position + BitVec.ofNat width state - slot
      = (round_position - slot) + BitVec.ofNat width state := BitVec.add_sub_comm
  apply BitVec.eq_of_toNat_eq
  rw [wrap_toNat _ _ capacity_is_power log_below_width, regrouped, BitVec.toNat_add,
    BitVec.toNat_ofNat, Nat.mod_eq_of_lt state_below_modulus, modulus_split,
    Nat.mod_mul_right_mod, Nat.add_mod, sub_aligned_mod _ _ log_below_width aligned,
    Nat.zero_add, Nat.mod_mod, Nat.mod_eq_of_lt state_below_capacity]

inductive ReadyState (width : Nat) where
  | free (round_position : BitVec width)
  | published (round_position : BitVec width)
  | claimed (round_position : BitVec width)
  deriving DecidableEq

def decodeReady {width : Nat} (capacity word slot : BitVec width) : ReadyState width :=
  let state := wrap capacity (word - slot)
  let round_position := word - state
  if state = 0#width then .free round_position
  else if state = 1#width then .published round_position
  else .claimed round_position

theorem decodeReady_of_aligned_state {width log : Nat}
    (capacity round_position slot : BitVec width) (state : Nat)
    (capacity_is_power : capacity.toNat = 2 ^ log) (log_below_width : log < width)
    (state_below_capacity : state < 2 ^ log)
    (aligned : round_position.toNat % 2 ^ log = slot.toNat) :
    decodeReady capacity (round_position + BitVec.ofNat width state) slot =
      if state = 0 then .free round_position
      else if state = 1 then .published round_position
      else .claimed round_position := by
  have modulus_split : 2 ^ width = 2 ^ log * 2 ^ (width - log) := by
    rw [← Nat.pow_add]; congr 1; omega
  have power_positive := Nat.two_pow_pos (width - log)
  have state_below_modulus : state < 2 ^ width := by
    have := Nat.two_pow_pos log
    rw [modulus_split]
    exact Nat.lt_of_lt_of_le state_below_capacity (Nat.le_mul_of_pos_right _ power_positive)
  have state_toNat : (BitVec.ofNat width state).toNat = state := by
    rw [BitVec.toNat_ofNat]; exact Nat.mod_eq_of_lt state_below_modulus
  have zero_toNat : (0#width).toNat = 0 := by simp
  have one_toNat : (1#width).toNat = 1 := by
    have : 2 ≤ 2 ^ width := by
      calc 2 = 2 ^ 1 := rfl
        _ ≤ 2 ^ width := Nat.pow_le_pow_right (by omega) (by omega)
    simp only [BitVec.toNat_ofNat]; exact Nat.mod_eq_of_lt (by omega)
  unfold decodeReady
  simp only [wrap_state_of_aligned capacity round_position slot state capacity_is_power
    log_below_width state_below_capacity aligned, BitVec.add_sub_cancel]
  have zero_case : (BitVec.ofNat width state = 0#width) ↔ state = 0 := by
    constructor
    · intro words_equal
      have := congrArg BitVec.toNat words_equal
      omega
    · intro state_zero; subst state_zero; rfl
  have one_case : (BitVec.ofNat width state = 1#width) ↔ state = 1 := by
    constructor
    · intro words_equal
      have := congrArg BitVec.toNat words_equal
      omega
    · intro state_one; subst state_one; rfl
  simp only [zero_case, one_case]

variable {width log : Nat} (capacity round_position slot : BitVec width)

theorem decodeReady_free (capacity_is_power : capacity.toNat = 2 ^ log)
    (log_below_width : log < width)
    (aligned : round_position.toNat % 2 ^ log = slot.toNat) :
    decodeReady capacity round_position slot = .free round_position := by
  have := decodeReady_of_aligned_state capacity round_position slot 0 capacity_is_power
    log_below_width (Nat.two_pow_pos log) aligned
  have zero_word : round_position + BitVec.ofNat width 0 = round_position := by simp
  rw [zero_word] at this
  simpa using this

theorem decodeReady_published (capacity_is_power : capacity.toNat = 2 ^ log)
    (log_below_width : log < width) (at_least_four : 2 ≤ log)
    (aligned : round_position.toNat % 2 ^ log = slot.toNat) :
    decodeReady capacity (round_position + 1#width) slot = .published round_position := by
  have four_le : 4 ≤ 2 ^ log := by
    calc 4 = 2 ^ 2 := rfl
      _ ≤ 2 ^ log := Nat.pow_le_pow_right (by omega) at_least_four
  simpa using decodeReady_of_aligned_state capacity round_position slot 1 capacity_is_power
    log_below_width (by omega) aligned

theorem decodeReady_claimed (capacity_is_power : capacity.toNat = 2 ^ log)
    (log_below_width : log < width) (at_least_four : 2 ≤ log)
    (aligned : round_position.toNat % 2 ^ log = slot.toNat) :
    decodeReady capacity (round_position + 2#width) slot = .claimed round_position := by
  have four_le : 4 ≤ 2 ^ log := by
    calc 4 = 2 ^ 2 := rfl
      _ ≤ 2 ^ log := Nat.pow_le_pow_right (by omega) at_least_four
  simpa using decodeReady_of_aligned_state capacity round_position slot 2 capacity_is_power
    log_below_width (by omega) aligned

def doneIsFreeFor {width : Nat} (word position : BitVec width) : Bool :=
  word == position

def doneRelease {width : Nat} (capacity round_position : BitVec width) : BitVec width :=
  round_position + capacity

theorem done_free_for_next_lap {width : Nat} (capacity round_position : BitVec width) :
    doneIsFreeFor (doneRelease capacity round_position) (round_position + capacity) = true := by
  simp [doneIsFreeFor, doneRelease]

theorem done_not_free_for_ended_round {width : Nat} (capacity round_position : BitVec width)
    (capacity_nonzero : capacity ≠ 0#width) :
    doneIsFreeFor (doneRelease capacity round_position) round_position = false := by
  simp only [doneIsFreeFor, doneRelease, beq_eq_false_iff_ne, ne_eq]
  intro words_equal
  apply capacity_nonzero
  have := congrArg (fun word => word - round_position) words_equal
  simp only [BitVec.sub_self] at this
  rw [BitVec.add_comm, BitVec.add_sub_cancel] at this
  exact this

theorem done_not_free_for_nearby_round {width : Nat} (capacity round_position other : BitVec width)
    (other_differs : other ≠ round_position + capacity) :
    doneIsFreeFor (doneRelease capacity round_position) other = false := by
  simp only [doneIsFreeFor, doneRelease, beq_eq_false_iff_ne, ne_eq]
  exact fun words_equal => other_differs words_equal.symm

end Qtz
