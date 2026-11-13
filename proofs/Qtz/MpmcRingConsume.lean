import Qtz.MpmcRingMove

/-!
The consumer steps and the two refill decisions keep the ring invariant.
-/

namespace Qtz

namespace Ring

variable {g : Geometry} {limit : Nat} {s : Ring g}

theorem Inv.idle_owns_nothing (inv : Inv limit s) (open_ring : s.closed = false) {k : Nat}
    (idle : (s.producers k).idle) : ∀ p, s.who p = k → (s.phaseAt p).owned = false := by
  intro p who_p
  by_cases reserved : s.phaseAt p = .reserved
  · obtain ⟨bit, unused, _⟩ := inv.reserved_held open_ring p reserved
    rw [who_p, idle.1 bit] at unused
    exact absurd unused (by decide)
  · by_cases writing : s.phaseAt p = .writing
    · obtain ⟨bit, held, _⟩ := inv.writing_held p writing
      rw [who_p, idle.2] at held
      exact absurd held (by simp)
    · exact Phase.owned_false_of_ne reserved writing

theorem Inv.doneRound_of_matches (inv : Inv limit s) (wraps_fit : limit + 2 ≤ g.wraps)
    (done_matches : s.doneMatches s.claim) :
    s.doneRoundAt s.claim = s.claim / g.period := by
  have below : s.claim % g.period < g.period := Nat.mod_lt _ g.period_positive
  have lower := inv.done_le_rounds_below below
  have upper := inv.lead_le below
  have placed : s.claim % g.period + s.claim / g.period * g.period = s.claim := by
    have := position_split g.period s.claim
    omega
  have rounds := roundsBelow_position (period := g.period) (index := s.claim % g.period)
    (round := s.claim / g.period) below
  rw [placed] at rounds
  have same : (s.view (s.claim % g.period)).doneRound % g.wraps =
      s.claim / g.period % g.wraps :=
    (g.slotGeom (s.claim % g.period)).roundPosition_cancel_wraps
      (s.view (s.claim % g.period)).doneRound (s.claim / g.period) done_matches
  exact (eq_of_mod_eq_of_close (by omega) (by omega) same).symm

theorem Inv.decideFreeKnown (inv : Inv limit s) {k batch : Nat} (open_ring : s.closed = false)
    (idle : (s.producers k).idle) (one : 1 ≤ batch) (fits : batch ≤ g.period) :
    Inv limit (s.setPending k (.freeKnown batch)) :=
  inv.setPending k _ (fun _ => idle)
    (fun other is_free => by
      cases is_free
      exact ⟨one, fits, inv.idle_owns_nothing open_ring idle⟩)
    (fun _ is_fallback => by cases is_fallback) (fun _ is_fallback => by cases is_fallback)

theorem Inv.decideFallback (inv : Inv limit s) (wraps_fit : limit + 2 ≤ g.wraps) {k : Nat}
    (idle : (s.producers k).idle) (done_matches : s.doneMatches s.claim) :
    Inv limit (s.setPending k (.fallback s.claim)) :=
  inv.setPending k _ (fun _ => idle) (fun _ is_free => by cases is_free)
    (fun seen is_fallback => by cases is_fallback; exact Nat.le_refl _)
    (fun seen is_fallback _ => by
      cases is_fallback
      exact inv.doneRound_of_matches wraps_fit done_matches)

theorem Inv.consumerClaim (inv : Inv limit s) {index : Nat} (below : index < g.period)
    {position : BitVec g.width}
    (decoded : decodeReady g.capacityWord (s.view index).readyWord
      (g.slotGeom index).slotWord = .published position) :
    Inv limit (s.afterClaim index) := by
  have slot := inv.slot_inv index below
  obtain ⟨ready_current, published⟩ :=
    slot.published_iff_ready.1 (slot.claim_reads_published decoded)
  have at_q : s.phaseAt (index + (s.view index).readyRound * g.period) = .published := by
    rw [phaseAt_position s below, ready_current]
    exact published
  have step : SlotModel.Step (s.view index) (SlotModel.claimCurrent (s.view index)) :=
    SlotModel.Step.claim position decoded
  exact inv.retire (t := s.afterClaim index) (index + (s.view index).readyRound * g.period)
    .claimed rfl rfl (fun _ => rfl) (phaseAt_afterClaim s below) (fun _ => rfl)
    (slot_inv_updateSlot inv.slot_inv SlotModel.claimCurrent step)
    (by rw [at_q]; decide) (by rw [at_q]; decide) (by rw [at_q]; decide)
    (by decide) (by decide) (by decide)
    (fun _ seen _ _ => doneRoundAt_updateSlot_same s index SlotModel.claimCurrent (fun _ => rfl) seen)

theorem Inv.consumerRelease (inv : Inv limit s) {index : Nat} (below : index < g.period)
    (claimed : (s.view index).phase (s.view index).doneRound = .claimed) :
    Inv limit (s.afterRelease index) := by
  have at_q : s.phaseAt (index + (s.view index).doneRound * g.period) = .claimed := by
    rw [phaseAt_position s below]
    exact claimed
  have step : SlotModel.Step (s.view index) (SlotModel.releaseCurrent (s.view index)) :=
    SlotModel.Step.release claimed
  exact inv.retire (t := s.afterRelease index) (index + (s.view index).doneRound * g.period)
    .released rfl rfl (fun _ => rfl) (phaseAt_afterRelease s below) (fun _ => rfl)
    (slot_inv_updateSlot inv.slot_inv SlotModel.releaseCurrent step)
    (by rw [at_q]; decide) (by rw [at_q]; decide) (by rw [at_q]; decide)
    (by decide) (by decide) (by decide)
    (fun j seen is_fallback at_claim => by
      by_cases at_index : seen % g.period = index
      · exfalso
        have fallback := inv.pending_fallback j seen is_fallback at_claim
        have q_below := inv.below_claim (by rw [at_q]; decide :
          s.phaseAt (index + (s.view index).doneRound * g.period) ≠ .unreserved)
        unfold doneRoundAt at fallback
        rw [at_index] at fallback
        have split := position_split g.period seen
        rw [at_index, ← fallback] at split
        omega
      · exact doneRoundAt_updateSlot_other s at_index _)

end Ring

end Qtz
