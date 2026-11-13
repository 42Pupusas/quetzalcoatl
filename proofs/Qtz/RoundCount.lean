import Qtz.Pigeonhole

/-!
How many rounds of one slot lie below the `claim` cursor.

With `period` slots, slot `index` holds the positions `index + round * period`. Every
position below `claim` is reserved, so `roundsBelow period claim index` is the number of
rounds of that slot that have been reserved.
-/

namespace Qtz

def roundsBelow (period claim index : Nat) : Nat := (claim + period - 1 - index) / period

theorem lt_roundsBelow_iff {period claim index : Nat} (below : index < period) (round : Nat) :
    round < roundsBelow period claim index ↔ index + round * period < claim := by
  unfold roundsBelow
  have positive : 0 < period := by omega
  rw [Nat.lt_iff_add_one_le, Nat.le_div_iff_mul_le positive, Nat.succ_mul]
  omega

theorem roundsBelow_add_le {period claim index count : Nat} (below : index < period)
    (batch : count ≤ period) :
    roundsBelow period (claim + count) index ≤ roundsBelow period claim index + 1 := by
  apply Nat.le_of_not_lt
  intro excess
  have beyond := (lt_roundsBelow_iff below (roundsBelow period claim index + 1)).1 (by omega)
  have not_below : ¬ roundsBelow period claim index < roundsBelow period claim index := by omega
  rw [lt_roundsBelow_iff below] at not_below
  rw [Nat.succ_mul] at beyond
  omega

theorem roundsBelow_le_add {period claim index count : Nat} (below : index < period) :
    roundsBelow period claim index ≤ roundsBelow period (claim + count) index := by
  apply Nat.le_of_not_lt
  intro excess
  have := (lt_roundsBelow_iff below _).1 excess
  have not_below : ¬ roundsBelow period (claim + count) index < roundsBelow period (claim + count) index := by omega
  rw [lt_roundsBelow_iff below] at not_below
  omega

/-- A round of a slot is the next one to be reserved exactly when the batch taken at
`claim` reaches it. -/
theorem reserved_in_batch_iff {period claim index count round : Nat} (below : index < period)
    (batch : count ≤ period) :
    (claim ≤ index + round * period ∧ index + round * period < claim + count) ↔
      (round = roundsBelow period claim index ∧
        index + round * period < claim + count) := by
  have first_unreached := lt_roundsBelow_iff (claim := claim) below
    (roundsBelow period claim index)
  have this_round := lt_roundsBelow_iff (claim := claim) below round
  have not_before : ¬ roundsBelow period claim index < roundsBelow period claim index := by omega
  constructor
  · rintro ⟨at_least, within⟩
    refine ⟨?_, within⟩
    apply Nat.le_antisymm
    · apply Nat.le_of_not_lt
      intro after
      have lap : (roundsBelow period claim index + 1) * period ≤ round * period :=
        Nat.mul_le_mul_right _ after
      rw [Nat.succ_mul] at lap
      omega
    · omega
  · rintro ⟨rfl, within⟩
    exact ⟨by omega, within⟩

theorem roundsBelow_position {period index round : Nat} (below : index < period) :
    roundsBelow period (index + round * period) index = round := by
  have positive : 0 < period := by omega
  unfold roundsBelow
  have rewritten : index + round * period + period - 1 - index = (period - 1) + round * period := by
    omega
  rw [rewritten, Nat.add_mul_div_right _ _ positive, Nat.div_eq_of_lt (by omega)]
  omega

/-- Every position below `claim` is the same slot and round as one of its rounds. -/
theorem roundsBelow_le_round_of_lt {period claim position : Nat} (below : position < claim)
    (positive : 0 < period) :
    position / period < roundsBelow period claim (position % period) := by
  have slot_below : position % period < period := Nat.mod_lt _ positive
  rw [lt_roundsBelow_iff slot_below]
  have := position_split period position
  omega

end Qtz
