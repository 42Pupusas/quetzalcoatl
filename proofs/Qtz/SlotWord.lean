import Qtz.Capacity
import Qtz.ConsumerFloor

/-!
Model of `SlotState` (`src/broadcast/slot_state.rs`): a slot word is
`position * 4 + state`, with wrapping arithmetic.

The word width is `surviving_bits + 2`: the two low bits hold the state,
so only the low `surviving_bits` bits of a position survive in the word.
-/

namespace Qtz

def vacantWord {width : Nat} : BitVec width := 0#width
def publishedState {width : Nat} : BitVec width := 1#width
def abandonedState {width : Nat} : BitVec width := 2#width
def unwrittenWord {width : Nat} : BitVec width := 3#width

def encode {width : Nat} (position state : BitVec width) : BitVec width :=
  position * 4#width + state

def publishedWord {width : Nat} (position : BitVec width) : BitVec width :=
  encode position publishedState

def abandonedWord {width : Nat} (position : BitVec width) : BitVec width :=
  encode position abandonedState

theorem small_constant_toNat {surviving_bits : Nat} (value : Nat) (value_small : value < 4) :
    (BitVec.ofNat (surviving_bits + 2) value).toNat = value := by
  have modulus_split : 2 ^ (surviving_bits + 2) = 2 ^ surviving_bits * 4 := by rw [Nat.pow_add]
  have modulus_positive := Nat.two_pow_pos surviving_bits
  rw [BitVec.toNat_ofNat]
  exact Nat.mod_eq_of_lt (by omega)

theorem encode_toNat {surviving_bits : Nat} (position state : BitVec (surviving_bits + 2))
    (state_small : state.toNat < 4) :
    (encode position state).toNat = (position.toNat % 2 ^ surviving_bits) * 4 + state.toNat := by
  have modulus_split : 2 ^ (surviving_bits + 2) = 2 ^ surviving_bits * 4 := by rw [Nat.pow_add]
  have remainder_small : position.toNat % 2 ^ surviving_bits < 2 ^ surviving_bits :=
    Nat.mod_lt _ (Nat.two_pow_pos surviving_bits)
  unfold encode
  rw [BitVec.toNat_add, BitVec.toNat_mul, BitVec.toNat_ofNat, Nat.mul_mod_mod, modulus_split,
    Nat.mul_mod_mul_right]
  exact Nat.mod_eq_of_lt (by omega)

theorem publishedWord_toNat {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    (publishedWord position).toNat = (position.toNat % 2 ^ surviving_bits) * 4 + 1 := by
  have one_toNat : (publishedState : BitVec (surviving_bits + 2)).toNat = 1 :=
    small_constant_toNat 1 (by omega)
  rw [publishedWord, encode_toNat _ _ (by omega), one_toNat]

theorem abandonedWord_toNat {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    (abandonedWord position).toNat = (position.toNat % 2 ^ surviving_bits) * 4 + 2 := by
  have two_toNat : (abandonedState : BitVec (surviving_bits + 2)).toNat = 2 :=
    small_constant_toNat 2 (by omega)
  rw [abandonedWord, encode_toNat _ _ (by omega), two_toNat]

theorem vacantWord_toNat {width : Nat} : (vacantWord : BitVec width).toNat = 0 := by
  simp [vacantWord]

theorem unwrittenWord_toNat {surviving_bits : Nat} :
    (unwrittenWord : BitVec (surviving_bits + 2)).toNat = 3 :=
  small_constant_toNat 3 (by omega)

theorem publishedWord_ne_vacant {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    publishedWord position ≠ vacantWord := by
  intro words_equal
  have := congrArg BitVec.toNat words_equal
  rw [publishedWord_toNat, vacantWord_toNat] at this
  omega

theorem abandonedWord_ne_vacant {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    abandonedWord position ≠ vacantWord := by
  intro words_equal
  have := congrArg BitVec.toNat words_equal
  rw [abandonedWord_toNat, vacantWord_toNat] at this
  omega

theorem publishedWord_ne_unwritten {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    publishedWord position ≠ unwrittenWord := by
  intro words_equal
  have := congrArg BitVec.toNat words_equal
  rw [publishedWord_toNat, unwrittenWord_toNat] at this
  omega

theorem abandonedWord_ne_unwritten {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    abandonedWord position ≠ unwrittenWord := by
  intro words_equal
  have := congrArg BitVec.toNat words_equal
  rw [abandonedWord_toNat, unwrittenWord_toNat] at this
  omega

theorem publishedWord_ne_abandonedWord {surviving_bits : Nat}
    (published_position abandoned_position : BitVec (surviving_bits + 2)) :
    publishedWord published_position ≠ abandonedWord abandoned_position := by
  intro words_equal
  have := congrArg BitVec.toNat words_equal
  rw [publishedWord_toNat, abandonedWord_toNat] at this
  omega

theorem publishedWord_eq_iff {surviving_bits : Nat} (left right : BitVec (surviving_bits + 2)) :
    publishedWord left = publishedWord right ↔
      left.toNat % 2 ^ surviving_bits = right.toNat % 2 ^ surviving_bits := by
  constructor
  · intro words_equal
    have := congrArg BitVec.toNat words_equal
    rw [publishedWord_toNat, publishedWord_toNat] at this
    omega
  · intro residues_equal
    apply BitVec.eq_of_toNat_eq
    rw [publishedWord_toNat, publishedWord_toNat, residues_equal]

theorem abandonedWord_eq_iff {surviving_bits : Nat} (left right : BitVec (surviving_bits + 2)) :
    abandonedWord left = abandonedWord right ↔
      left.toNat % 2 ^ surviving_bits = right.toNat % 2 ^ surviving_bits := by
  constructor
  · intro words_equal
    have := congrArg BitVec.toNat words_equal
    rw [abandonedWord_toNat, abandonedWord_toNat] at this
    omega
  · intro residues_equal
    apply BitVec.eq_of_toNat_eq
    rw [abandonedWord_toNat, abandonedWord_toNat, residues_equal]

theorem residues_differ_of_distance {surviving_bits : Nat}
    (position reference : BitVec (surviving_bits + 2))
    (distance_positive : 0 < (position - reference).toNat)
    (distance_below_period : (position - reference).toNat < 2 ^ surviving_bits) :
    position.toNat % 2 ^ surviving_bits ≠ reference.toNat % 2 ^ surviving_bits := by
  intro residues_equal
  have period_positive := Nat.two_pow_pos surviving_bits
  have modulus_split : 2 ^ (surviving_bits + 2) = 2 ^ surviving_bits * 4 := by rw [Nat.pow_add]
  have position_bound := position.isLt
  have reference_bound := reference.isLt
  have distance_split := wrapped_sub_toNat position reference
  have position_split := Nat.div_add_mod position.toNat (2 ^ surviving_bits)
  have reference_split := Nat.div_add_mod reference.toNat (2 ^ surviving_bits)
  have residue_below_period := Nat.mod_lt position.toNat period_positive
  have position_lap_small : position.toNat / 2 ^ surviving_bits < 4 :=
    Nat.div_lt_of_lt_mul (by omega)
  have reference_lap_small : reference.toNat / 2 ^ surviving_bits < 4 :=
    Nat.div_lt_of_lt_mul (by omega)
  rw [modulus_split] at distance_split
  generalize 2 ^ surviving_bits = period at *
  generalize position.toNat / period = position_lap at *
  generalize reference.toNat / period = reference_lap at *
  generalize position.toNat % period = residue at *
  generalize (position - reference).toNat = distance at *
  rw [← residues_equal] at reference_split
  generalize position.toNat = position_value at *
  generalize reference.toNat = reference_value at *
  have position_lap_cases :
      position_lap = 0 ∨ position_lap = 1 ∨ position_lap = 2 ∨ position_lap = 3 := by omega
  have reference_lap_cases :
      reference_lap = 0 ∨ reference_lap = 1 ∨ reference_lap = 2 ∨ reference_lap = 3 := by omega
  split at distance_split <;>
  rcases position_lap_cases with rfl | rfl | rfl | rfl <;>
  rcases reference_lap_cases with rfl | rfl | rfl | rfl <;> omega

theorem next_occupant_word_differs {surviving_bits : Nat}
    (position capacity : BitVec (surviving_bits + 2))
    (capacity_positive : 0 < capacity.toNat)
    (capacity_within_max : capacity.toNat ≤ maxCapacity (surviving_bits + 2))
    (at_least_one_surviving_bit : 1 ≤ surviving_bits) :
    publishedWord (position + capacity) ≠ publishedWord position ∧
    abandonedWord (position + capacity) ≠ abandonedWord position := by
  have max_is_half_period : maxCapacity (surviving_bits + 2) * 2 = 2 ^ surviving_bits := by
    unfold maxCapacity
    obtain ⟨lower_bits, rfl⟩ := Nat.exists_eq_add_of_le at_least_one_surviving_bit
    rw [show 1 + lower_bits + 2 - 3 = lower_bits by omega, Nat.add_comm 1 lower_bits,
      Nat.pow_succ]
  have max_positive : 0 < maxCapacity (surviving_bits + 2) := Nat.two_pow_pos _
  have distance_is_capacity : (position + capacity - position).toNat = capacity.toNat := by
    rw [BitVec.add_comm]
    exact congrArg BitVec.toNat (BitVec.add_sub_cancel capacity position)
  have residues_differ := residues_differ_of_distance (position + capacity) position
    (by omega) (by omega)
  exact ⟨fun words_equal => residues_differ ((publishedWord_eq_iff _ _).1 words_equal),
    fun words_equal => residues_differ ((abandonedWord_eq_iff _ _).1 words_equal)⟩

end Qtz
