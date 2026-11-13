import Qtz.MpmcRingReserve

/-!
A step that changes the phase of one position `q` and the record of one producer `k`, and
nothing else the invariant looks at, keeps the invariant. `Inv.move` takes the facts about
`k` that differ from step to step as hypotheses; everything about the other producers, the
`pending` fields and the ordering of positions is proved once here.

`Inv.retire` is the special case of a consumer step: the position leaves `published` or
`claimed` and no producer changes.
-/

namespace Qtz

theorem doneRoundAt_updateSlot_same {g : Geometry} (s : Ring g) (index : Nat)
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index))
    (same : ∀ slot, (change slot).doneRound = slot.doneRound) (p : Nat) :
    (s.updateSlot index change).doneRoundAt p = s.doneRoundAt p := by
  unfold Ring.doneRoundAt
  by_cases at_index : p % g.period = index
  · rw [at_index, Ring.view_updateSlot_self, same]
  · rw [Ring.view_updateSlot_other s at_index]

theorem doneRoundAt_updateSlot_other {g : Geometry} (s : Ring g) {index p : Nat}
    (different : p % g.period ≠ index)
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index)) :
    (s.updateSlot index change).doneRoundAt p = s.doneRoundAt p := by
  unfold Ring.doneRoundAt
  rw [Ring.view_updateSlot_other s different]

theorem Phase.owned_false_of_ne {phase : Phase} (not_reserved : phase ≠ .reserved)
    (not_writing : phase ≠ .writing) : phase.owned = false := by
  cases phase <;> simp_all [Phase.owned]

namespace Ring

variable {g : Geometry} {limit : Nat} {s : Ring g}

theorem Inv.pending_none_of_unused (inv : Inv limit s) {k bit : Nat}
    (unused : (s.producers k).unused bit = true) : (s.producers k).pending = .none := by
  by_cases none_pending : (s.producers k).pending = .none
  · exact none_pending
  · exact absurd ((inv.pending_idle k none_pending).1 bit) (by simp [unused])

theorem Inv.pending_none_of_writing (inv : Inv limit s) {k bit : Nat}
    (writing : (s.producers k).writing = some bit) : (s.producers k).pending = .none := by
  by_cases none_pending : (s.producers k).pending = .none
  · exact none_pending
  · exact absurd (inv.pending_idle k none_pending).2 (by simp [writing])

theorem Inv.move (inv : Inv limit s) {t : Ring g} (k q : Nat) (new : Phase) (next : Producer)
    (claim_eq : t.claim = s.claim) (closed_eq : t.closed = s.closed)
    (who_eq : ∀ p, t.who p = s.who p)
    (phase_eq : ∀ p, t.phaseAt p = if p = q then new else s.phaseAt p)
    (producers_eq : ∀ j, t.producers j = if j = k then next else s.producers j)
    (slot : ∀ index, index < g.period → SlotModel.Inv (t.view index))
    (who_q : s.who q = k) (old_ne : s.phaseAt q ≠ .unreserved) (new_ne : new ≠ .unreserved)
    (owned_mono : new.owned = true → (s.phaseAt q).owned = true)
    (pending_same : next.pending = (s.producers k).pending)
    (pending_idle : next.pending ≠ .none → next.idle)
    (done_same : ∀ j seen, (s.producers j).pending = .fallback seen → seen = s.claim →
      t.doneRoundAt seen = s.doneRoundAt seen)
    (held_reserved : ∀ bit, next.unused bit = true →
      bit < g.period ∧ t.phaseAt (next.start + bit) = .reserved ∧ s.who (next.start + bit) = k)
    (held_writing : ∀ bit, next.writing = some bit →
      bit < g.period ∧ t.phaseAt (next.start + bit) = .writing ∧ s.who (next.start + bit) = k)
    (reserved_held : t.closed = false → ∀ p, t.phaseAt p = .reserved → s.who p = k →
      ∃ bit, next.unused bit = true ∧ next.start + bit = p)
    (writing_held : ∀ p, t.phaseAt p = .writing → s.who p = k →
      ∃ bit, next.writing = some bit ∧ next.start + bit = p) :
    Inv limit t := by
  have q_below : q < s.claim := inv.below_claim old_ne
  have at_q : t.phaseAt q = new := by rw [phase_eq]; simp
  have off_q : ∀ p, p ≠ q → t.phaseAt p = s.phaseAt p := fun p different => by
    rw [phase_eq]; simp [different]
  have at_k : t.producers k = next := by rw [producers_eq]; simp
  have off_k : ∀ j, j ≠ k → t.producers j = s.producers j := fun j different => by
    rw [producers_eq]; simp [different]
  have not_q : ∀ p j, s.who p = j → j ≠ k → p ≠ q := fun p j owner different same_position =>
    different (by rw [← owner, same_position]; exact who_q)
  have owned_le : ∀ p, (t.phaseAt p).owned = true → (s.phaseAt p).owned = true := by
    intro p owned
    by_cases same : p = q
    · rw [same, at_q] at owned; rw [same]; exact owned_mono owned
    · rw [off_q p same] at owned; exact owned
  have pending_t : ∀ j, (t.producers j).pending = (s.producers j).pending := by
    intro j
    by_cases same : j = k
    · rw [same, at_k]; exact pending_same
    · rw [off_k j same]
  refine ⟨slot, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro p
    rw [claim_eq]
    by_cases same : p = q
    · rw [same, at_q]
      constructor
      · intro h; exact absurd h new_ne
      · intro h; omega
    · rw [off_q p same]; exact inv.unreserved_iff p
  · intro p below
    rw [who_eq]
    rw [claim_eq] at below
    exact inv.who_below p below
  · intro first second same_slot same_who first_owned second_owned
    rw [who_eq, who_eq] at same_who
    exact inv.one_owned first second same_slot same_who (owned_le _ first_owned)
      (owned_le _ second_owned)
  · intro j bit unused
    by_cases same : j = k
    · rw [same, at_k] at unused ⊢
      obtain ⟨below, reserved, owner⟩ := held_reserved bit unused
      exact ⟨below, reserved, (who_eq _).trans owner⟩
    · rw [off_k j same] at unused ⊢
      obtain ⟨below, reserved, owner⟩ := inv.held_reserved j bit unused
      have different := not_q _ j owner same
      exact ⟨below, by rw [off_q _ different]; exact reserved, by rw [who_eq]; exact owner⟩
  · intro j bit writing
    by_cases same : j = k
    · rw [same, at_k] at writing ⊢
      obtain ⟨below, held, owner⟩ := held_writing bit writing
      exact ⟨below, held, (who_eq _).trans owner⟩
    · rw [off_k j same] at writing ⊢
      obtain ⟨below, held, owner⟩ := inv.held_writing j bit writing
      have different := not_q _ j owner same
      exact ⟨below, by rw [off_q _ different]; exact held, by rw [who_eq]; exact owner⟩
  · intro open_ring p reserved
    rw [who_eq]
    by_cases same : s.who p = k
    · rw [same, at_k]
      exact reserved_held open_ring p reserved same
    · have different := not_q p _ rfl same
      rw [off_q p different] at reserved
      rw [off_k _ same]
      exact inv.reserved_held (by rw [← closed_eq]; exact open_ring) p reserved
  · intro p writing
    rw [who_eq]
    by_cases same : s.who p = k
    · rw [same, at_k]
      exact writing_held p writing same
    · have different := not_q p _ rfl same
      rw [off_q p different] at writing
      rw [off_k _ same]
      exact inv.writing_held p writing
  · intro j not_none
    by_cases same : j = k
    · rw [same, at_k] at not_none ⊢
      exact pending_idle not_none
    · rw [off_k j same] at not_none ⊢
      exact inv.pending_idle j not_none
  · intro j batch is_free
    rw [pending_t] at is_free
    obtain ⟨one, fits, owns⟩ := inv.pending_free j batch is_free
    refine ⟨one, fits, fun p who_p => Bool.eq_false_iff.2 fun owned => ?_⟩
    exact Bool.eq_false_iff.1 (owns p ((who_eq p).symm.trans who_p)) (owned_le p owned)
  · intro j seen is_fallback
    rw [pending_t] at is_fallback
    rw [claim_eq]
    exact inv.fallback_seen_le j seen is_fallback
  · intro j seen is_fallback at_claim
    rw [pending_t] at is_fallback
    rw [claim_eq] at at_claim
    rw [done_same j seen is_fallback at_claim]
    exact inv.pending_fallback j seen is_fallback at_claim

/-- A consumer step: the position leaves `published` or `claimed` for `claimed` or
`released`, and no producer changes. -/
theorem Inv.retire (inv : Inv limit s) {t : Ring g} (q : Nat) (new : Phase)
    (claim_eq : t.claim = s.claim) (closed_eq : t.closed = s.closed)
    (who_eq : ∀ p, t.who p = s.who p)
    (phase_eq : ∀ p, t.phaseAt p = if p = q then new else s.phaseAt p)
    (producers_eq : ∀ j, t.producers j = s.producers j)
    (slot : ∀ index, index < g.period → SlotModel.Inv (t.view index))
    (old_ne : s.phaseAt q ≠ .unreserved) (old_reserved : s.phaseAt q ≠ .reserved)
    (old_writing : s.phaseAt q ≠ .writing) (new_ne : new ≠ .unreserved)
    (new_reserved : new ≠ .reserved) (new_writing : new ≠ .writing)
    (done_same : ∀ j seen, (s.producers j).pending = .fallback seen → seen = s.claim →
      t.doneRoundAt seen = s.doneRoundAt seen) :
    Inv limit t := by
  have back : ∀ p phase, phase ≠ new → t.phaseAt p = phase → s.phaseAt p = phase := by
    intro p phase different at_p
    rw [phase_eq] at at_p
    by_cases same : p = q
    · simp [same] at at_p; exact absurd at_p.symm different
    · simp [same] at at_p; exact at_p
  have differs : ∀ p phase, s.phaseAt p = phase → s.phaseAt q ≠ phase → p ≠ q :=
    fun p phase at_p different same => different (by rw [← same]; exact at_p)
  have off_q : ∀ p, p ≠ q → t.phaseAt p = s.phaseAt p := fun p different => by
    rw [phase_eq]; simp [different]
  exact inv.move (t := t) (s.who q) q new (s.producers (s.who q)) claim_eq closed_eq who_eq
    phase_eq (fun j => by rw [producers_eq j]; by_cases h : j = s.who q <;> simp [h]) slot rfl
    old_ne new_ne (fun owned => by
      rw [Phase.owned_false_of_ne new_reserved new_writing] at owned
      exact absurd owned (by decide))
    rfl (inv.pending_idle _) done_same
    (fun bit unused => by
      obtain ⟨below, reserved, owner⟩ := inv.held_reserved _ bit unused
      exact ⟨below, by rw [off_q _ (differs _ _ reserved old_reserved)]; exact reserved, owner⟩)
    (fun bit writing => by
      obtain ⟨below, held, owner⟩ := inv.held_writing _ bit writing
      exact ⟨below, by rw [off_q _ (differs _ _ held old_writing)]; exact held, owner⟩)
    (fun open_ring p reserved who_p => by
      have at_p := back p .reserved (Ne.symm new_reserved) reserved
      have := inv.reserved_held (by rw [← closed_eq]; exact open_ring) p at_p
      rw [who_p] at this
      exact this)
    (fun p writing who_p => by
      have at_p := back p .writing (Ne.symm new_writing) writing
      have := inv.writing_held p at_p
      rw [who_p] at this
      exact this)

end Ring

end Qtz
