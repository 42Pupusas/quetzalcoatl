import Qtz.MpmcRingEffects

/-!
The invariant of the mpmc ring model, and the counting argument behind it.

A position is *owned* while it is reserved or being written: its producer has not yet
published it or given it up. The ghost `who` names the producer that reserved each position.

How far reservations run ahead of a slot's `done` word rests on one fact, `one_owned`: a
producer owns at most one position of any slot, orphans included. A batch is at most one lap,
and a producer takes a free-known batch only when it owns nothing (`pending_free`). In a
closed ring a producer may own orphans, so a single-position fallback instead needs its slot to
have nothing outstanding (`pending_fallback`).
-/

namespace Qtz

namespace Ring

variable {g : Geometry}

structure Inv (limit : Nat) (s : Ring g) : Prop where
  slot_inv : ∀ index, index < g.period → SlotModel.Inv (s.view index)
  unreserved_iff : ∀ position, s.phaseAt position = .unreserved ↔ s.claim ≤ position
  who_below : ∀ position, position < s.claim → s.who position < limit
  one_owned : ∀ first second, first % g.period = second % g.period →
    s.who first = s.who second → (s.phaseAt first).owned = true →
    (s.phaseAt second).owned = true → first = second
  held_reserved : ∀ k bit, (s.producers k).unused bit = true →
    bit < g.period ∧ s.phaseAt ((s.producers k).start + bit) = .reserved ∧
      s.who ((s.producers k).start + bit) = k
  held_writing : ∀ k bit, (s.producers k).writing = some bit →
    bit < g.period ∧ s.phaseAt ((s.producers k).start + bit) = .writing ∧
      s.who ((s.producers k).start + bit) = k
  reserved_held : s.closed = false → ∀ position, s.phaseAt position = .reserved →
    ∃ bit, (s.producers (s.who position)).unused bit = true ∧
      (s.producers (s.who position)).start + bit = position
  writing_held : ∀ position, s.phaseAt position = .writing →
    ∃ bit, (s.producers (s.who position)).writing = some bit ∧
      (s.producers (s.who position)).start + bit = position
  pending_free : ∀ k seen batch, (s.producers k).pending = .freeKnown seen batch →
    1 ≤ batch ∧ batch ≤ g.period ∧
      ∀ position, s.who position = k → (s.phaseAt position).owned = false
  pending_fallback : ∀ k seen, (s.producers k).pending = .fallback seen → seen = s.claim →
    s.doneRoundAt seen = seen / g.period

variable {limit : Nat} {s : Ring g}

theorem Inv.below_claim (inv : Inv limit s) {position : Nat}
    (reserved : s.phaseAt position ≠ .unreserved) : position < s.claim := by
  apply Nat.lt_of_not_le
  intro not_below
  exact reserved ((inv.unreserved_iff position).2 not_below)

theorem Inv.phase_of_below_claim (inv : Inv limit s) {position : Nat}
    (below : position < s.claim) : s.phaseAt position ≠ .unreserved := by
  intro unreserved
  have := (inv.unreserved_iff position).1 unreserved
  omega

theorem Inv.done_le_rounds_below (inv : Inv limit s) {index : Nat} (below : index < g.period) :
    (s.view index).doneRound ≤ roundsBelow g.period s.claim index := by
  by_cases zero : (s.view index).doneRound = 0
  · omega
  · have previous : (s.view index).doneRound - 1 < (s.view index).doneRound := by omega
    have released := (inv.slot_inv index below).below_done_released _ previous
    have reserved : s.phaseAt (index + ((s.view index).doneRound - 1) * g.period) ≠ .unreserved := by
      rw [phaseAt_position s below, released]; decide
    have := (lt_roundsBelow_iff below _).2 (inv.below_claim reserved)
    omega

/-- A round strictly between the slot's `done` round and the next unreserved one is
reserved: no producer has started on it, and it is not yet released. -/
theorem Inv.reserved_between (inv : Inv limit s) {index round : Nat} (below : index < g.period)
    (after_done : (s.view index).doneRound < round)
    (before_claim : round < roundsBelow g.period s.claim index) :
    (s.view index).phase round = .reserved := by
  have slot := inv.slot_inv index below
  have placed : index + round * g.period < s.claim := (lt_roundsBelow_iff below round).1 before_claim
  have not_unreserved := inv.phase_of_below_claim placed
  rw [phaseAt_position s below] at not_unreserved
  have not_released : (s.view index).phase round ≠ .released := fun released => by
    have := slot.released_below_done round released
    omega
  have not_in_progress : (s.view index).phase round ≠ .writing ∧
      (s.view index).phase round ≠ .published ∧ (s.view index).phase round ≠ .claimed := by
    refine ⟨?_, ?_, ?_⟩ <;> intro phase <;>
      have := slot.in_progress_is_current round (by rw [phase]; rfl) <;> omega
  generalize (s.view index).phase round = phase at *
  cases phase <;> simp_all

/-- The core count. Rounds `done + 1` up to the last reserved one are each reserved by a
different producer, and none by `skipped` if that producer owns nothing. Dropping the
`skipped` producer leaves `limit - 1` owners. -/
theorem Inv.lead_le (inv : Inv limit s) {index : Nat} (below : index < g.period)
    (skipped : Nat) (skipped_below : skipped < limit) (owns_nothing : ∀ position, s.who position = skipped →
      (s.phaseAt position).owned = false) :
    roundsBelow g.period s.claim index ≤ (s.view index).doneRound + limit := by
  have done_le := inv.done_le_rounds_below below
  by_cases few : roundsBelow g.period s.claim index ≤ (s.view index).doneRound + 1
  · omega
  · let done := (s.view index).doneRound
    let owner : Nat → Nat := fun offset =>
      s.who (index + (done + 1 + offset) * g.period)
    let squeezed : Nat → Nat := fun offset =>
      if skipped < owner offset then owner offset - 1 else owner offset
    have reserved : ∀ offset, offset < roundsBelow g.period s.claim index - done - 1 →
        s.phaseAt (index + (done + 1 + offset) * g.period) = .reserved := fun offset small => by
      rw [phaseAt_position s below]
      exact inv.reserved_between below (by omega) (by omega)
    have different_owner : ∀ offset, offset < roundsBelow g.period s.claim index - done - 1 →
        owner offset ≠ skipped := fun offset small same => by
      have := owns_nothing _ same
      rw [reserved offset small] at this
      exact absurd this (by decide)
    have fits := pigeonhole (roundsBelow g.period s.claim index - done - 1) (limit - 1) squeezed
      (fun offset small => by
        have owner_below : owner offset < limit :=
          inv.who_below _ ((lt_roundsBelow_iff below _).1 (by omega))
        have owner_different : owner offset ≠ skipped := different_owner offset small
        show (if skipped < owner offset then owner offset - 1 else owner offset) < limit - 1
        split <;> omega)
      (fun first second first_small second_small same => by
        have first_different := different_owner first first_small
        have second_different := different_owner second second_small
        have same_owner : owner first = owner second := by
          change (if skipped < owner first then owner first - 1 else owner first) =
            (if skipped < owner second then owner second - 1 else owner second) at same
          split at same <;> split at same <;> omega
        have equal := inv.one_owned _ _
          (by rw [position_mod _ below, position_mod _ below]) same_owner
          (by rw [reserved first first_small]; rfl) (by rw [reserved second second_small]; rfl)
        have rounds : (done + 1 + first) * g.period = (done + 1 + second) * g.period := by omega
        have := Nat.eq_of_mul_eq_mul_right (Nat.pos_of_ne_zero fun zero => by
          have := g.period_positive; omega) rounds
        omega)
    omega

end Ring

end Qtz
