import Qtz.MpmcRingInv

/-!
Facts about what each ring step leaves unchanged, used to show the invariant survives it.
-/

namespace Qtz

namespace Ring

variable {g : Geometry}

theorem view_updateSlot_self (s : Ring g) (index : Nat)
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index)) :
    (s.updateSlot index change).view index = change (s.view index) := by
  simp only [view, updateSlot, ite_true]
  rfl

theorem view_updateSlot_other (s : Ring g) {index other : Nat} (different : other ≠ index)
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index)) :
    (s.updateSlot index change).view other = s.view other := by
  simp only [view, updateSlot, different, ite_false]

theorem slot_inv_updateSlot {s : Ring g}
    (slot_inv : ∀ index, index < g.period → SlotModel.Inv (s.view index)) {index : Nat}
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index))
    (step : SlotModel.Step (s.view index) (change (s.view index))) :
    ∀ other, other < g.period → SlotModel.Inv ((s.updateSlot index change).view other) := by
  intro other below
  by_cases same : other = index
  · subst same
    rw [view_updateSlot_self]
    exact (slot_inv other below).step step
  · rw [view_updateSlot_other s same]
    exact slot_inv other below

theorem phaseAt_updateProducer (s : Ring g) (k : Nat) (producer : Producer) (p : Nat) :
    (s.updateProducer k producer).phaseAt p = s.phaseAt p := rfl

theorem producers_updateProducer (s : Ring g) (k j : Nat) (producer : Producer) :
    (s.updateProducer k producer).producers j = if j = k then producer else s.producers j := rfl

theorem phaseAt_afterAcquire (s : Ring g) (k bit p : Nat) :
    (s.afterAcquire k bit).phaseAt p =
      if p = (s.producers k).start + bit then .writing else s.phaseAt p := by
  unfold afterAcquire
  rw [phaseAt_updateProducer]
  exact phaseAt_updateSlot s ((s.producers k).start + bit) _ rfl _ .writing rfl p

theorem phaseAt_afterPublish (s : Ring g) (k bit p : Nat) :
    (s.afterPublish k bit).phaseAt p =
      if p = (s.producers k).start + bit then .published else s.phaseAt p := by
  unfold afterPublish
  rw [phaseAt_updateProducer]
  exact phaseAt_updateSlot s ((s.producers k).start + bit) _ rfl _ .published rfl p

theorem phaseAt_afterUnwrite (s : Ring g) (k bit p : Nat) :
    (s.afterUnwrite k bit).phaseAt p =
      if p = (s.producers k).start + bit then .reserved else s.phaseAt p := by
  unfold afterUnwrite
  rw [phaseAt_updateProducer]
  exact phaseAt_updateSlot s ((s.producers k).start + bit) _ rfl _ .reserved rfl p

theorem phaseAt_afterAbandon (s : Ring g) (k bit p : Nat) :
    (s.afterAbandon k bit).phaseAt p =
      if p = (s.producers k).start + bit then .released else s.phaseAt p := by
  unfold afterAbandon
  rw [phaseAt_updateProducer]
  exact phaseAt_updateSlot s ((s.producers k).start + bit) _ rfl _ .released rfl p

theorem phaseAt_afterClaim (s : Ring g) {index : Nat} (below : index < g.period) (p : Nat) :
    (s.afterClaim index).phaseAt p =
      if p = index + (s.view index).readyRound * g.period then .claimed else s.phaseAt p :=
  phaseAt_updateSlot s (index + (s.view index).readyRound * g.period) index
    (position_mod _ below) _ .claimed (by rw [position_div _ below]; rfl) p

theorem phaseAt_afterRelease (s : Ring g) {index : Nat} (below : index < g.period) (p : Nat) :
    (s.afterRelease index).phaseAt p =
      if p = index + (s.view index).doneRound * g.period then .released else s.phaseAt p :=
  phaseAt_updateSlot s (index + (s.view index).doneRound * g.period) index
    (position_mod _ below) _ .released (by rw [position_div _ below]; rfl) p

end Ring

end Qtz
