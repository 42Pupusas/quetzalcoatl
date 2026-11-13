import Qtz.MpmcRingConsume

/-!
The producer steps keep the ring invariant: acquiring a position to write, publishing or
unwriting it, abandoning a reserved position, and orphaning one after the consumers left.
-/

namespace Qtz

theorem Producer.withoutBit_start (producer : Producer) (bit : Nat) :
    (producer.withoutBit bit).start = producer.start := rfl

theorem Producer.withBit_start (producer : Producer) (bit : Nat) :
    (producer.withBit bit).start = producer.start := rfl

namespace Ring

variable {g : Geometry} {limit : Nat} {s : Ring g}

theorem doneRoundAt_updateSlot_producer {g : Geometry} (s : Ring g) (index : Nat)
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index))
    (same : ∀ slot, (change slot).doneRound = slot.doneRound) (k : Nat) (producer : Producer)
    (p : Nat) :
    ((s.updateSlot index change).updateProducer k producer).doneRoundAt p = s.doneRoundAt p :=
  doneRoundAt_updateSlot_same s index change same p

/-- A step that moves a slot's `done` round up past a position still reserved at the old
round cannot disturb a fallback: the fallback's slot would have a `done` round beyond its
own round. -/
theorem Inv.done_same_of_release (inv : Inv limit s) {t : Ring g} (q : Nat)
    (old_ne : s.phaseAt q ≠ .unreserved) (at_done : s.doneRoundAt q = q / g.period)
    (elsewhere : ∀ p, p % g.period ≠ q % g.period → t.doneRoundAt p = s.doneRoundAt p) :
    ∀ j seen, (s.producers j).pending = .fallback seen → seen = s.claim →
      t.doneRoundAt seen = s.doneRoundAt seen := by
  intro j seen is_fallback at_claim
  by_cases same_slot : seen % g.period = q % g.period
  · exfalso
    have fallback := inv.pending_fallback j seen is_fallback at_claim
    have q_below := inv.below_claim old_ne
    have earlier := same_slot_round_lt (capacity := g.period) same_slot.symm (by omega : q < seen)
    have slot_eq : s.doneRoundAt seen = s.doneRoundAt q := by
      unfold doneRoundAt; rw [same_slot]
    omega
  · exact elsewhere seen same_slot

theorem Inv.acquire (inv : Inv limit s) {k bit : Nat}
    (unused : (s.producers k).unused bit = true) (not_writing : (s.producers k).writing = none)
    (done_matches : s.doneMatches ((s.producers k).start + bit)) :
    Inv limit (s.afterAcquire k bit) := by
  obtain ⟨bit_below, reserved, owner⟩ := inv.held_reserved k bit unused
  have pending_none := inv.pending_none_of_unused unused
  have step : SlotModel.Step (s.view (((s.producers k).start + bit) % g.period))
      ((s.view (((s.producers k).start + bit) % g.period)).beginWriteAt
        (((s.producers k).start + bit) / g.period)) :=
    SlotModel.Step.beginWrite _ reserved done_matches
  refine inv.move (t := s.afterAcquire k bit) k ((s.producers k).start + bit) .writing
    { (s.producers k).withoutBit bit with writing := some bit } rfl rfl (fun _ => rfl)
    (phaseAt_afterAcquire s k bit) (fun _ => rfl)
    (slot_inv_updateSlot inv.slot_inv
      (fun slot => slot.beginWriteAt (((s.producers k).start + bit) / g.period)) step)
    owner (by rw [reserved]; decide) (by decide) (fun _ => by rw [reserved]; rfl) rfl
    (fun h => (h pending_none).elim)
    (fun _ seen _ _ => doneRoundAt_updateSlot_producer s _
      (fun slot => slot.beginWriteAt (((s.producers k).start + bit) / g.period))
      (fun _ => rfl) k _ seen)
    ?_ ?_ ?_ ?_
  · intro b h
    have h' : (if b = bit then false else (s.producers k).unused b) = true := h
    have not_bit : b ≠ bit := fun same => by simp [same] at h'
    have old : (s.producers k).unused b = true := by simpa [not_bit] using h'
    obtain ⟨below, res, own⟩ := inv.held_reserved k b old
    refine ⟨below, ?_, own⟩
    have different : (s.producers k).start + b ≠ (s.producers k).start + bit := by omega
    rw [phaseAt_afterAcquire]
    simp only [Producer.withoutBit_start, different, ite_false]
    exact res
  · intro b h
    obtain rfl : bit = b := Option.some.inj h
    refine ⟨bit_below, ?_, owner⟩
    rw [phaseAt_afterAcquire]
    simp [Producer.withoutBit_start]
  · intro open_ring p res_p who_p
    have hp : p ≠ (s.producers k).start + bit := fun same => by
      rw [same, phaseAt_afterAcquire] at res_p; simp at res_p
    rw [phaseAt_afterAcquire] at res_p
    simp only [hp, ite_false] at res_p
    obtain ⟨b, ub, placed⟩ := inv.reserved_held open_ring p res_p
    rw [who_p] at ub placed
    have not_bit : b ≠ bit := fun same => by rw [same] at placed; omega
    refine ⟨b, ?_, placed⟩
    show (if b = bit then false else (s.producers k).unused b) = true
    simp [not_bit, ub]
  · intro p wr who_p
    by_cases hp : p = (s.producers k).start + bit
    · exact ⟨bit, rfl, hp.symm⟩
    · rw [phaseAt_afterAcquire] at wr
      simp only [hp, ite_false] at wr
      obtain ⟨b, hb, _⟩ := inv.writing_held p wr
      rw [who_p, not_writing] at hb
      simp at hb

theorem Inv.publish (inv : Inv limit s) {k bit : Nat}
    (writing : (s.producers k).writing = some bit) : Inv limit (s.afterPublish k bit) := by
  obtain ⟨bit_below, wr, owner⟩ := inv.held_writing k bit writing
  have pending_none := inv.pending_none_of_writing writing
  have step : SlotModel.Step (s.view (((s.producers k).start + bit) % g.period))
      ((s.view (((s.producers k).start + bit) % g.period)).publishAt
        (((s.producers k).start + bit) / g.period)) :=
    SlotModel.Step.publish _ wr
  refine inv.move (t := s.afterPublish k bit) k ((s.producers k).start + bit) .published
    { s.producers k with writing := none } rfl rfl (fun _ => rfl)
    (phaseAt_afterPublish s k bit) (fun _ => rfl)
    (slot_inv_updateSlot inv.slot_inv
      (fun slot => slot.publishAt (((s.producers k).start + bit) / g.period)) step)
    owner (by rw [wr]; decide) (by decide) (fun h => absurd h (by decide)) rfl
    (fun h => (h pending_none).elim)
    (fun _ seen _ _ => doneRoundAt_updateSlot_producer s _
      (fun slot => slot.publishAt (((s.producers k).start + bit) / g.period))
      (fun _ => rfl) k _ seen)
    ?_ ?_ ?_ ?_
  · intro b ub
    obtain ⟨below, res, own⟩ := inv.held_reserved k b ub
    refine ⟨below, ?_, own⟩
    have different : (s.producers k).start + b ≠ (s.producers k).start + bit := fun same => by
      rw [same, wr] at res; exact absurd res (by decide)
    rw [phaseAt_afterPublish]
    simp only [different, ite_false]
    exact res
  · intro b h
    simp at h
  · intro open_ring p res_p who_p
    have hp : p ≠ (s.producers k).start + bit := fun same => by
      rw [same, phaseAt_afterPublish] at res_p; simp at res_p
    rw [phaseAt_afterPublish] at res_p
    simp only [hp, ite_false] at res_p
    obtain ⟨b, ub, placed⟩ := inv.reserved_held open_ring p res_p
    rw [who_p] at ub placed
    exact ⟨b, ub, placed⟩
  · intro p wr' who_p
    have hp : p ≠ (s.producers k).start + bit := fun same => by
      rw [same, phaseAt_afterPublish] at wr'; simp at wr'
    rw [phaseAt_afterPublish] at wr'
    simp only [hp, ite_false] at wr'
    obtain ⟨b, hb, placed⟩ := inv.writing_held p wr'
    rw [who_p] at hb placed
    rw [writing] at hb
    have same : bit = b := Option.some.inj hb
    exact absurd (by omega : p = (s.producers k).start + bit) hp

theorem Inv.unwrite (inv : Inv limit s) {k bit : Nat}
    (writing : (s.producers k).writing = some bit) : Inv limit (s.afterUnwrite k bit) := by
  obtain ⟨bit_below, wr, owner⟩ := inv.held_writing k bit writing
  have pending_none := inv.pending_none_of_writing writing
  have step : SlotModel.Step (s.view (((s.producers k).start + bit) % g.period))
      ((s.view (((s.producers k).start + bit) % g.period)).unwriteAt
        (((s.producers k).start + bit) / g.period)) :=
    SlotModel.Step.unwrite _ wr
  refine inv.move (t := s.afterUnwrite k bit) k ((s.producers k).start + bit) .reserved
    { (s.producers k).withBit bit with writing := none } rfl rfl (fun _ => rfl)
    (phaseAt_afterUnwrite s k bit) (fun _ => rfl)
    (slot_inv_updateSlot inv.slot_inv
      (fun slot => slot.unwriteAt (((s.producers k).start + bit) / g.period)) step)
    owner (by rw [wr]; decide) (by decide) (fun _ => by rw [wr]; rfl) rfl
    (fun h => (h pending_none).elim)
    (fun _ seen _ _ => doneRoundAt_updateSlot_producer s _
      (fun slot => slot.unwriteAt (((s.producers k).start + bit) / g.period))
      (fun _ => rfl) k _ seen)
    ?_ ?_ ?_ ?_
  · intro b h
    have h' : (if b = bit then true else (s.producers k).unused b) = true := h
    by_cases same : b = bit
    · subst same
      refine ⟨bit_below, ?_, owner⟩
      rw [phaseAt_afterUnwrite]
      simp [Producer.withBit_start]
    · have old : (s.producers k).unused b = true := by simpa [same] using h'
      obtain ⟨below, res, own⟩ := inv.held_reserved k b old
      refine ⟨below, ?_, own⟩
      have different : (s.producers k).start + b ≠ (s.producers k).start + bit := by omega
      rw [phaseAt_afterUnwrite]
      simp only [Producer.withBit_start, different, ite_false]
      exact res
  · intro b h
    simp at h
  · intro open_ring p res_p who_p
    by_cases hp : p = (s.producers k).start + bit
    · exact ⟨bit, by show (if bit = bit then true else _) = true; simp, hp.symm⟩
    · rw [phaseAt_afterUnwrite] at res_p
      simp only [hp, ite_false] at res_p
      obtain ⟨b, ub, placed⟩ := inv.reserved_held open_ring p res_p
      rw [who_p] at ub placed
      refine ⟨b, ?_, placed⟩
      show (if b = bit then true else (s.producers k).unused b) = true
      by_cases same : b = bit <;> simp [same, ub]
  · intro p wr' who_p
    have hp : p ≠ (s.producers k).start + bit := fun same => by
      rw [same, phaseAt_afterUnwrite] at wr'; simp at wr'
    rw [phaseAt_afterUnwrite] at wr'
    simp only [hp, ite_false] at wr'
    obtain ⟨b, hb, placed⟩ := inv.writing_held p wr'
    rw [who_p] at hb placed
    rw [writing] at hb
    have same : bit = b := Option.some.inj hb
    exact absurd (by omega : p = (s.producers k).start + bit) hp

theorem Inv.abandonOne (inv : Inv limit s) {k bit : Nat}
    (unused : (s.producers k).unused bit = true)
    (done_matches : s.doneMatches ((s.producers k).start + bit)) :
    Inv limit (s.afterAbandon k bit) := by
  obtain ⟨bit_below, reserved, owner⟩ := inv.held_reserved k bit unused
  have pending_none := inv.pending_none_of_unused unused
  have slot_below : ((s.producers k).start + bit) % g.period < g.period :=
    Nat.mod_lt _ g.period_positive
  have step : SlotModel.Step (s.view (((s.producers k).start + bit) % g.period))
      ((s.view (((s.producers k).start + bit) % g.period)).abandonAt
        (((s.producers k).start + bit) / g.period)) :=
    SlotModel.Step.abandon _ reserved done_matches
  have at_done : s.doneRoundAt ((s.producers k).start + bit) =
      ((s.producers k).start + bit) / g.period :=
    ((inv.slot_inv _ slot_below).done_word_round reserved done_matches).symm
  refine inv.move (t := s.afterAbandon k bit) k ((s.producers k).start + bit) .released
    ((s.producers k).withoutBit bit) rfl rfl (fun _ => rfl)
    (phaseAt_afterAbandon s k bit) (fun _ => rfl)
    (slot_inv_updateSlot inv.slot_inv
      (fun slot => slot.abandonAt (((s.producers k).start + bit) / g.period)) step)
    owner (by rw [reserved]; decide) (by decide) (fun h => absurd h (by decide)) rfl
    (fun h => (h pending_none).elim)
    (inv.done_same_of_release _ (by rw [reserved]; decide) at_done
      (fun p different => doneRoundAt_updateSlot_other s different
        (fun slot => slot.abandonAt (((s.producers k).start + bit) / g.period))))
    ?_ ?_ ?_ ?_
  · intro b h
    have h' : (if b = bit then false else (s.producers k).unused b) = true := h
    have not_bit : b ≠ bit := fun same => by simp [same] at h'
    have old : (s.producers k).unused b = true := by simpa [not_bit] using h'
    obtain ⟨below, res, own⟩ := inv.held_reserved k b old
    refine ⟨below, ?_, own⟩
    have different : (s.producers k).start + b ≠ (s.producers k).start + bit := by omega
    rw [phaseAt_afterAbandon]
    simp only [Producer.withoutBit_start, different, ite_false]
    exact res
  · intro b h
    obtain ⟨below, wr, own⟩ := inv.held_writing k b h
    refine ⟨below, ?_, own⟩
    have different : (s.producers k).start + b ≠ (s.producers k).start + bit := fun same => by
      rw [same, reserved] at wr; exact absurd wr (by decide)
    rw [phaseAt_afterAbandon]
    simp only [Producer.withoutBit_start, different, ite_false]
    exact wr
  · intro open_ring p res_p who_p
    have hp : p ≠ (s.producers k).start + bit := fun same => by
      rw [same, phaseAt_afterAbandon] at res_p; simp at res_p
    rw [phaseAt_afterAbandon] at res_p
    simp only [hp, ite_false] at res_p
    obtain ⟨b, ub, placed⟩ := inv.reserved_held open_ring p res_p
    rw [who_p] at ub placed
    have not_bit : b ≠ bit := fun same => by rw [same] at placed; omega
    refine ⟨b, ?_, placed⟩
    show (if b = bit then false else (s.producers k).unused b) = true
    simp [not_bit, ub]
  · intro p wr' who_p
    have hp : p ≠ (s.producers k).start + bit := fun same => by
      rw [same, phaseAt_afterAbandon] at wr'; simp at wr'
    rw [phaseAt_afterAbandon] at wr'
    simp only [hp, ite_false] at wr'
    obtain ⟨b, hb, placed⟩ := inv.writing_held p wr'
    rw [who_p] at hb placed
    exact ⟨b, hb, placed⟩

theorem Inv.orphanOne (inv : Inv limit s) {k bit : Nat}
    (unused : (s.producers k).unused bit = true) (closed : s.closed = true) :
    Inv limit (s.afterOrphan k bit) := by
  obtain ⟨bit_below, reserved, owner⟩ := inv.held_reserved k bit unused
  have pending_none := inv.pending_none_of_unused unused
  refine inv.move (t := s.afterOrphan k bit) k ((s.producers k).start + bit) .reserved
    ((s.producers k).withoutBit bit) rfl rfl (fun _ => rfl)
    (fun p => by
      show s.phaseAt p = _
      by_cases hp : p = (s.producers k).start + bit
      · simp [hp, reserved]
      · simp [hp])
    (fun _ => rfl) inv.slot_inv owner (by rw [reserved]; decide) (by decide)
    (fun _ => by rw [reserved]; rfl) rfl (fun h => (h pending_none).elim)
    (fun _ _ _ _ => rfl) ?_ ?_ ?_ ?_
  · intro b h
    have h' : (if b = bit then false else (s.producers k).unused b) = true := h
    have not_bit : b ≠ bit := fun same => by simp [same] at h'
    have old : (s.producers k).unused b = true := by simpa [not_bit] using h'
    exact inv.held_reserved k b old
  · intro b h
    exact inv.held_writing k b h
  · intro open_ring _ _ _
    have h : s.closed = false := open_ring
    rw [closed] at h
    cases h
  · intro p wr' who_p
    obtain ⟨b, hb, placed⟩ := inv.writing_held p wr'
    rw [who_p] at hb placed
    exact ⟨b, hb, placed⟩

end Ring

end Qtz
