import Qtz.MpmcRingStep

namespace Qtz

namespace SlotModel

variable {g : Geometry}

/-- Reserving rounds that are unreserved and inside the `done` window keeps the slot's
invariant. -/
theorem Inv.reserveRange {s : SlotModel g} (inv : Inv s) (start count : Nat)
    (fresh : ∀ round, start ≤ g.slot + round * g.period →
      g.slot + round * g.period < start + count →
      s.phase round = .unreserved ∧ round < s.doneRound + g.wraps) :
    Inv (s.reserveRange start count) := by
  have phase_eq : ∀ round, (s.reserveRange start count).phase round =
      if start ≤ g.slot + round * g.period ∧ g.slot + round * g.period < start + count
      then .reserved else s.phase round := fun _ => rfl
  refine ⟨inv.state_valid, ?_, ?_, ?_, ?_, ?_⟩
  · intro round below
    rw [phase_eq]
    split
    · rename_i inside
      have unreserved := (fresh round inside.1 inside.2).1
      have released := inv.below_done_released round below
      exact absurd (unreserved.symm.trans released) (by decide)
    · exact inv.below_done_released round below
  · intro round released
    rw [phase_eq] at released
    split at released
    · exact absurd released (by decide)
    · exact inv.released_below_done round released
  · intro round progress
    rw [phase_eq] at progress
    split at progress
    · exact absurd progress (by decide)
    · exact inv.in_progress_is_current round progress
  · have same : (s.reserveRange start count).phase s.doneRound = .published ↔
        s.phase s.doneRound = .published := by
      rw [phase_eq]
      split
      · rename_i inside
        have unreserved := (fresh _ inside.1 inside.2).1
        rw [unreserved]
        constructor <;> intro h <;> exact absurd h (by decide)
      · exact Iff.rfl
    show s.readyState = 1 ↔ (s.readyRound = s.doneRound ∧
      (s.reserveRange start count).phase s.doneRound = .published)
    rw [same]
    exact inv.published_iff_ready
  · intro round not_unreserved not_released
    rw [phase_eq] at not_unreserved not_released
    split at not_unreserved
    · rename_i inside
      exact (fresh round inside.1 inside.2).2
    · rename_i outside
      simp only [outside, ite_false] at not_released
      exact inv.within_window round not_unreserved not_released

end SlotModel

namespace Ring

variable {g : Geometry} {limit : Nat} {s t : Ring g}

theorem phaseAt_initial (g : Geometry) (position : Nat) :
    (Ring.initial g).phaseAt position = .unreserved := rfl

theorem Inv.initial (limit : Nat) : Inv limit (Ring.initial g) where
  slot_inv index _ := SlotModel.Inv.initial (g.slotGeom index)
  unreserved_iff position := ⟨fun _ => Nat.zero_le _, fun _ => rfl⟩
  who_below position below := absurd below (Nat.not_lt_zero _)
  one_owned first second _ _ owned _ := by
    rw [phaseAt_initial] at owned; exact absurd owned (by decide)
  held_reserved k bit unused := by simp [Ring.initial, Producer.fresh] at unused
  held_writing k bit writing := by simp [Ring.initial, Producer.fresh] at writing
  reserved_held _ position reserved := by
    rw [phaseAt_initial] at reserved; exact absurd reserved (by decide)
  writing_held position writing := by
    rw [phaseAt_initial] at writing; exact absurd writing (by decide)
  pending_idle k pending := absurd rfl pending
  pending_free k batch pending := by simp [Ring.initial, Producer.fresh] at pending
  fallback_seen_le k seen pending := by simp [Ring.initial, Producer.fresh] at pending
  pending_fallback k seen pending := by simp [Ring.initial, Producer.fresh] at pending

theorem producers_setPending_self (s : Ring g) (k : Nat) (pending : Pending) :
    (s.setPending k pending).producers k = { s.producers k with pending := pending } := by
  simp [setPending, updateProducer]

theorem producers_setPending_other (s : Ring g) {k j : Nat} (pending : Pending) (different : j ≠ k) :
    (s.setPending k pending).producers j = s.producers j := by
  simp [setPending, updateProducer, different]

theorem unused_setPending (s : Ring g) (k j : Nat) (pending : Pending) :
    ((s.setPending k pending).producers j).unused = (s.producers j).unused := by
  by_cases same : j = k
  · subst same; rw [producers_setPending_self]
  · rw [producers_setPending_other s pending same]

theorem start_setPending (s : Ring g) (k j : Nat) (pending : Pending) :
    ((s.setPending k pending).producers j).start = (s.producers j).start := by
  by_cases same : j = k
  · subst same; rw [producers_setPending_self]
  · rw [producers_setPending_other s pending same]

theorem writing_setPending (s : Ring g) (k j : Nat) (pending : Pending) :
    ((s.setPending k pending).producers j).writing = (s.producers j).writing := by
  by_cases same : j = k
  · subst same; rw [producers_setPending_self]
  · rw [producers_setPending_other s pending same]

theorem who_setPending (s : Ring g) (k position : Nat) (pending : Pending) :
    (s.setPending k pending).who position = s.who position := rfl

theorem phaseAt_setPending (s : Ring g) (k position : Nat) (pending : Pending) :
    (s.setPending k pending).phaseAt position = s.phaseAt position := rfl

/-- Setting a producer's pending refill leaves everything but the four `pending` fields as
they were. -/
theorem Inv.setPending (inv : Inv limit s) (k : Nat) (pending : Pending)
    (idle : pending ≠ .none → (s.producers k).idle)
    (free : ∀ batch, pending = .freeKnown batch → 1 ≤ batch ∧ batch ≤ g.period ∧
      ∀ position, s.who position = k → (s.phaseAt position).owned = false)
    (seen_le : ∀ seen, pending = .fallback seen → seen ≤ s.claim)
    (fallback : ∀ seen, pending = .fallback seen → seen = s.claim →
      s.doneRoundAt seen = seen / g.period) :
    Inv limit (s.setPending k pending) := by
  refine ⟨inv.slot_inv, inv.unreserved_iff, inv.who_below, inv.one_owned, ?_, ?_, ?_, ?_, ?_, ?_,
    ?_, ?_⟩
  · intro j bit unused
    simp only [unused_setPending, start_setPending, who_setPending, phaseAt_setPending] at unused ⊢
    exact inv.held_reserved j bit unused
  · intro j bit writing
    simp only [writing_setPending, start_setPending, who_setPending, phaseAt_setPending] at writing ⊢
    exact inv.held_writing j bit writing
  · intro open_ring position reserved
    simp only [phaseAt_setPending] at reserved
    simp only [unused_setPending, start_setPending, who_setPending]
    exact inv.reserved_held open_ring position reserved
  · intro position writing
    simp only [phaseAt_setPending] at writing
    simp only [writing_setPending, start_setPending, who_setPending]
    exact inv.writing_held position writing
  · intro j not_none
    by_cases same : j = k
    · subst same
      rw [producers_setPending_self] at not_none ⊢
      exact idle not_none
    · rw [producers_setPending_other s pending same] at not_none ⊢
      exact inv.pending_idle j not_none
  · intro j batch is_free
    by_cases same : j = k
    · subst same
      rw [producers_setPending_self] at is_free
      exact free batch is_free
    · rw [producers_setPending_other s pending same] at is_free
      exact inv.pending_free j batch is_free
  · intro j seen is_fallback
    by_cases same : j = k
    · subst same
      rw [producers_setPending_self] at is_fallback
      exact seen_le seen is_fallback
    · rw [producers_setPending_other s pending same] at is_fallback
      exact inv.fallback_seen_le j seen is_fallback
  · intro j seen is_fallback at_claim
    by_cases same : j = k
    · subst same
      rw [producers_setPending_self] at is_fallback
      exact fallback seen is_fallback at_claim
    · rw [producers_setPending_other s pending same] at is_fallback
      exact inv.pending_fallback j seen is_fallback at_claim

theorem Inv.retry (inv : Inv limit s) (k : Nat) : Inv limit (s.setPending k .none) :=
  inv.setPending k .none (fun not_none => absurd rfl not_none)
    (fun _ is_free => by simp at is_free) (fun _ is_fallback => by simp at is_fallback)
    (fun _ is_fallback => by simp at is_fallback)

theorem Inv.close (inv : Inv limit s) : Inv limit { s with closed := true } :=
  { inv with reserved_held := fun open_ring => by simp at open_ring }

end Ring

end Qtz
