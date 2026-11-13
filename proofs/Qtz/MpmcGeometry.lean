import Qtz.MpmcWords

/-!
The words of one mpmc slot, indexed by round.

A slot at index `slot` in a ring of `2 ^ log` slots stands for the
logical positions `slot + round * 2 ^ log`. Its `ready` and `done` words
are those positions plus a small state, reduced to `width` bits. Two
rounds give the same word exactly when they differ by a multiple of
`wraps = 2 ^ (width - log)`.
-/

namespace Qtz

theorem reduce_lap (slot period wraps lap : Nat) (slot_below : slot < period)
    (wraps_pos : 0 < wraps) :
    (slot + lap * period) % (period * wraps) = slot + (lap % wraps) * period := by
  have lap_split := Nat.mod_add_div lap wraps
  have remainder_below : lap % wraps < wraps := Nat.mod_lt lap wraps_pos
  generalize lap % wraps = remainder at *
  generalize lap / wraps = quotient at *
  subst lap_split
  have rewritten : slot + (remainder + wraps * quotient) * period
      = (slot + remainder * period) + (period * wraps) * quotient := by
    rw [Nat.add_mul, Nat.mul_assoc, Nat.mul_comm wraps, Nat.mul_assoc, Nat.mul_comm period wraps,
      Nat.mul_comm quotient, Nat.add_assoc]
  rw [rewritten, Nat.add_mul_mod_self_left]
  apply Nat.mod_eq_of_lt
  have : (remainder + 1) * period ≤ wraps * period := Nat.mul_le_mul_right _ remainder_below
  rw [Nat.add_mul, Nat.one_mul, Nat.mul_comm wraps] at this
  omega

structure Geometry where
  width : Nat
  log : Nat
  slot : Nat
  log_at_least_two : 2 ≤ log
  log_below_width : log < width
  slot_below_period : slot < 2 ^ log

namespace Geometry

variable (g : Geometry)

def period : Nat := 2 ^ g.log

def wraps : Nat := 2 ^ (g.width - g.log)

def capacityWord : BitVec g.width := BitVec.ofNat g.width g.period

def slotWord : BitVec g.width := BitVec.ofNat g.width g.slot

def roundPosition (round : Nat) : BitVec g.width :=
  BitVec.ofNat g.width (g.slot + round * g.period)

def word (round state : Nat) : BitVec g.width :=
  BitVec.ofNat g.width (g.slot + round * g.period + state)

theorem period_positive : 0 < g.period := Nat.two_pow_pos g.log

theorem wraps_positive : 0 < g.wraps := Nat.two_pow_pos _

theorem period_mul_wraps : g.period * g.wraps = 2 ^ g.width := by
  unfold period wraps
  rw [← Nat.pow_add]
  congr 1
  have := g.log_below_width
  omega

theorem four_le_period : 4 ≤ g.period := by
  unfold period
  calc 4 = 2 ^ 2 := rfl
    _ ≤ 2 ^ g.log := Nat.pow_le_pow_right (by omega) g.log_at_least_two

theorem period_below_modulus : g.period < 2 ^ g.width := by
  have := g.period_mul_wraps
  have := g.wraps_positive
  have : g.period * 1 ≤ g.period * g.wraps := Nat.mul_le_mul_left _ this
  have : g.wraps ≠ 1 := fun wraps_one => by
    have := g.log_below_width
    unfold wraps at wraps_one
    have : 2 ^ (g.width - g.log) ≥ 2 ^ 1 :=
      Nat.pow_le_pow_right (by omega) (by omega)
    omega
  have : 2 ≤ g.wraps := by
    have := g.wraps_positive
    omega
  have : g.period * 2 ≤ g.period * g.wraps := Nat.mul_le_mul_left _ this
  have := g.period_positive
  omega

theorem capacityWord_toNat : g.capacityWord.toNat = g.period := by
  unfold capacityWord
  rw [BitVec.toNat_ofNat]
  exact Nat.mod_eq_of_lt g.period_below_modulus

theorem slotWord_toNat : g.slotWord.toNat = g.slot := by
  have := g.slot_below_period
  have := g.period_below_modulus
  unfold slotWord
  rw [BitVec.toNat_ofNat]
  exact Nat.mod_eq_of_lt (by unfold period at *; omega)

theorem word_eq_roundPosition_add (round state : Nat) :
    g.word round state = g.roundPosition round + BitVec.ofNat g.width state := by
  unfold word roundPosition
  rw [BitVec.ofNat_add]

theorem roundPosition_eq_word (round : Nat) : g.roundPosition round = g.word round 0 := by
  rw [word_eq_roundPosition_add]
  simp

theorem roundPosition_succ (round : Nat) :
    g.roundPosition (round + 1) = g.roundPosition round + g.capacityWord := by
  unfold roundPosition capacityWord
  rw [← BitVec.ofNat_add, Nat.succ_mul, Nat.add_assoc]

theorem word_state_succ_round (round state : Nat) :
    g.word (round + 1) state = g.word round state + g.capacityWord := by
  unfold word capacityWord
  rw [← BitVec.ofNat_add, Nat.succ_mul]
  congr 1
  omega

theorem roundPosition_aligned (round : Nat) :
    (g.roundPosition round).toNat % g.period = g.slot := by
  unfold roundPosition
  rw [BitVec.toNat_ofNat]
  have divides : g.period ∣ 2 ^ g.width := ⟨g.wraps, g.period_mul_wraps.symm⟩
  rw [Nat.mod_mod_of_dvd _ divides, Nat.add_mul_mod_self_right]
  exact Nat.mod_eq_of_lt g.slot_below_period

theorem decode_word (round state : Nat) (state_below_three : state < 3) :
    decodeReady g.capacityWord (g.word round state) g.slotWord =
      if state = 0 then .free (g.roundPosition round)
      else if state = 1 then .published (g.roundPosition round)
      else .claimed (g.roundPosition round) := by
  have period_power : g.capacityWord.toNat = 2 ^ g.log := by
    rw [capacityWord_toNat]; rfl
  have aligned : (g.roundPosition round).toNat % 2 ^ g.log = g.slotWord.toNat := by
    rw [slotWord_toNat]; exact g.roundPosition_aligned round
  have four := g.four_le_period
  unfold period at four
  rw [word_eq_roundPosition_add]
  exact decodeReady_of_aligned_state g.capacityWord (g.roundPosition round) g.slotWord state
    period_power g.log_below_width (by omega) aligned

theorem roundPosition_cancel_wraps (round other : Nat)
    (same_position : g.roundPosition round = g.roundPosition other) :
    round % g.wraps = other % g.wraps := by
  have slot_below := g.slot_below_period
  have period_pos := g.period_positive
  have wraps_pos := g.wraps_positive
  have reduce : ∀ lap : Nat,
      (g.slot + lap * g.period) % 2 ^ g.width = g.slot + (lap % g.wraps) * g.period := by
    intro lap
    rw [← g.period_mul_wraps]
    exact reduce_lap g.slot g.period g.wraps lap slot_below wraps_pos
  have toNat_equal := congrArg BitVec.toNat same_position
  unfold roundPosition at toNat_equal
  rw [BitVec.toNat_ofNat, BitVec.toNat_ofNat, reduce, reduce] at toNat_equal
  have laps_equal : (round % g.wraps) * g.period = (other % g.wraps) * g.period := by omega
  exact Nat.eq_of_mul_eq_mul_right period_pos laps_equal

end Geometry

end Qtz
