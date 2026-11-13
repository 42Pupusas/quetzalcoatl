import Qtz.MpmcRing
import Qtz.RoundCount

/-!
What each kind of ring step does to the phase of a position.

A slot step writes one phase at one position (`Effect`); taking a batch reserves every
position in `[claim, claim + count)`.
-/

namespace Qtz

namespace Ring

variable {g : Geometry}

theorem phaseAt_eq (s : Ring g) (p : Nat) :
    s.phaseAt p = (s.slots (p % g.period)).phase (p / g.period) := rfl

/-- A slot update that sets the phase of position `q` to `phase` and nothing else visible
in `phaseAt`. -/
theorem phaseAt_updateSlot (s : Ring g) (q index : Nat) (at_index : q % g.period = index)
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index)) (phase : Phase)
    (sets : (change (s.view index)).phase = (s.view index).setPhase (q / g.period) phase)
    (p : Nat) :
    (s.updateSlot index change).phaseAt p = if p = q then phase else s.phaseAt p := by
  subst at_index
  by_cases same_slot : p % g.period = q % g.period
  · have slot_eq : (s.updateSlot (q % g.period) change).slots (p % g.period) =
        (change (s.view (q % g.period))).retag g := by
      simp only [updateSlot, same_slot, ite_true]
    rw [phaseAt_eq, slot_eq]
    show (change (s.view (q % g.period))).phase (p / g.period) = _
    rw [sets]
    by_cases same_round : p / g.period = q / g.period
    · have same : p = q := same_slot_same_round same_slot same_round
      subst same
      simp [SlotModel.setPhase]
    · have different : p ≠ q := fun same => same_round (by rw [same])
      simp only [SlotModel.setPhase, same_round, different, ite_false]
      rw [phaseAt_eq, same_slot]
      rfl
  · have different : p ≠ q := fun same => same_slot (by rw [same])
    have slot_eq : (s.updateSlot (q % g.period) change).slots (p % g.period) =
        s.slots (p % g.period) := by
      simp only [updateSlot, same_slot, ite_false]
    rw [phaseAt_eq, slot_eq]
    simp only [different, ite_false]
    rfl

theorem phaseAt_reserveBatch (s : Ring g) (k count p : Nat) :
    (s.reserveBatch k count).phaseAt p =
      if s.claim ≤ p ∧ p < s.claim + count then .reserved else s.phaseAt p := by
  have split := position_split g.period p
  have slot_idem : p % g.period % g.period = p % g.period := Nat.mod_mod _ _
  have position : p % g.period + p / g.period * g.period = p := by omega
  rw [phaseAt_eq]
  show ((s.view (p % g.period)).reserveRange s.claim count).phase (p / g.period) = _
  simp only [SlotModel.reserveRange, Geometry.slotGeom_slot, Geometry.slotGeom_period,
    slot_idem, position]
  rfl

theorem phaseAt_position (s : Ring g) {index : Nat} (below : index < g.period) (round : Nat) :
    s.phaseAt (index + round * g.period) = (s.view index).phase round := by
  unfold phaseAt
  rw [position_mod round below, position_div round below]

end Ring

end Qtz
