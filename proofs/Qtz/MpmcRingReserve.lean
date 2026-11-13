import Qtz.MpmcRingPreserve

/-!
Taking a batch keeps the ring invariant.

Both refills end in `reserveBatch`. A free-known batch needs its producer to own nothing; a
fallback's single position needs only that its producer owns nothing in that position's slot,
which holds because the slot's `done` round is the fallback's own round. The guard on
`reserve` (the round lies less than `wraps` above the slot's `done` round) comes from
`lead_le` and `limit + 2 ≤ wraps`.
-/

namespace Qtz

theorem Producer.batch_start (start count : Nat) : (Producer.batch start count).start = start := rfl

theorem Producer.batch_unused (start count bit : Nat) :
    (Producer.batch start count).unused bit = decide (bit < count) := rfl

theorem Producer.batch_writing (start count : Nat) : (Producer.batch start count).writing = none :=
  rfl

theorem Producer.batch_pending (start count : Nat) :
    (Producer.batch start count).pending = .none := rfl

namespace Ring

variable {g : Geometry} {limit : Nat} {s : Ring g}

theorem phaseAt_reserveBatch_inside (s : Ring g) (k count : Nat) {p : Nat}
    (inside : s.claim ≤ p ∧ p < s.claim + count) :
    (s.reserveBatch k count).phaseAt p = .reserved := by
  rw [phaseAt_reserveBatch]
  simp only [inside.1, inside.2, and_self, ite_true]

theorem phaseAt_reserveBatch_outside (s : Ring g) (k count : Nat) {p : Nat}
    (outside : ¬(s.claim ≤ p ∧ p < s.claim + count)) :
    (s.reserveBatch k count).phaseAt p = s.phaseAt p := by
  rw [phaseAt_reserveBatch]
  simp only [outside, ite_false]

theorem who_reserveBatch_inside (s : Ring g) (k count : Nat) {p : Nat}
    (inside : s.claim ≤ p ∧ p < s.claim + count) : (s.reserveBatch k count).who p = k := by
  show (if s.claim ≤ p ∧ p < s.claim + count then k else s.who p) = k
  simp only [inside.1, inside.2, and_self, ite_true]

theorem who_reserveBatch_outside (s : Ring g) (k count : Nat) {p : Nat}
    (outside : ¬(s.claim ≤ p ∧ p < s.claim + count)) :
    (s.reserveBatch k count).who p = s.who p := by
  show (if s.claim ≤ p ∧ p < s.claim + count then k else s.who p) = s.who p
  simp only [outside, ite_false]

theorem producers_reserveBatch_self (s : Ring g) (k count : Nat) :
    (s.reserveBatch k count).producers k = Producer.batch s.claim count := by
  simp [reserveBatch]

theorem producers_reserveBatch_other (s : Ring g) {k j : Nat} (count : Nat) (different : j ≠ k) :
    (s.reserveBatch k count).producers j = s.producers j := by
  simp [reserveBatch, different]

theorem view_reserveBatch (s : Ring g) (k count index : Nat) :
    (s.reserveBatch k count).view index = (s.view index).reserveRange s.claim count := rfl

/-- An owned position is at or above its slot's `done` round. -/
theorem Inv.done_le_owned (inv : Inv limit s) {p : Nat} (owned : (s.phaseAt p).owned = true) :
    (s.view (p % g.period)).doneRound ≤ p / g.period := by
  apply Nat.le_of_not_lt
  intro below
  have released := (inv.slot_inv _ (Nat.mod_lt _ g.period_positive)).below_done_released _ below
  unfold phaseAt at owned
  rw [released] at owned
  exact absurd owned (by decide)

/-- A fallback's position is the next round its slot's `done` word is waiting for, so the
producer owns nothing in that slot: its older positions there lie below `done`. -/
theorem Inv.fallback_owns_nothing (inv : Inv limit s) {k seen : Nat}
    (pending : (s.producers k).pending = .fallback seen) (at_claim : seen = s.claim) :
    ∀ p, p % g.period = s.claim % g.period → (s.phaseAt p).owned = false := by
  intro p same_slot
  by_cases owned : (s.phaseAt p).owned = true
  · have below : p < s.claim :=
      inv.below_claim (Phase.ne_unreserved_of_owned owned)
    have earlier := same_slot_round_lt same_slot below
    have at_done := inv.pending_fallback k seen pending at_claim
    have floor := inv.done_le_owned owned
    rw [same_slot] at floor
    have done_eq : (s.view (s.claim % g.period)).doneRound = s.claim / g.period := by
      rw [at_claim] at at_done
      exact at_done
    omega
  · exact Bool.eq_false_iff.2 owned

theorem Inv.reserveBatch (inv : Inv limit s) (wraps_fit : limit + 2 ≤ g.wraps) {k count : Nat}
    (k_below : k < limit) (positive : 1 ≤ count) (batch : count ≤ g.period)
    (idle : (s.producers k).idle)
    (owns_nothing : ∀ q p, s.claim ≤ q → q < s.claim + count → p % g.period = q % g.period →
      s.who p = k → (s.phaseAt p).owned = false) :
    Inv limit (s.reserveBatch k count) := by
  have trichotomy : ∀ p, (s.claim ≤ p ∧ p < s.claim + count) ∨ p < s.claim ∨
      s.claim + count ≤ p := fun p => by omega
  have old : ∀ p, p < s.claim → ¬(s.claim ≤ p ∧ p < s.claim + count) := fun p _ => by omega
  have owned_below : ∀ p, ((s.reserveBatch k count).phaseAt p).owned = true →
      p < s.claim + count := by
    intro p owned
    rcases trichotomy p with inside | below | beyond
    · exact inside.2
    · omega
    · have outside : ¬(s.claim ≤ p ∧ p < s.claim + count) := fun h => by omega
      rw [phaseAt_reserveBatch_outside s k count outside,
        (inv.unreserved_iff p).2 (by omega)] at owned
      exact absurd owned (by decide)
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro index below
    rw [view_reserveBatch]
    apply (inv.slot_inv index below).reserveRange
    intro round at_least within
    simp only [Geometry.slotGeom_slot, Geometry.slotGeom_period, Geometry.slotGeom_wraps,
      Nat.mod_eq_of_lt below] at at_least within ⊢
    constructor
    · have := (inv.unreserved_iff (index + round * g.period)).2 at_least
      rwa [phaseAt_position s below] at this
    · have round_eq := ((reserved_in_batch_iff below batch).1 ⟨at_least, within⟩).1
      have := inv.lead_le below
      omega
  · intro p
    show _ ↔ s.claim + count ≤ p
    rcases trichotomy p with inside | below | beyond
    · rw [phaseAt_reserveBatch_inside s k count inside]
      exact ⟨fun h => absurd h (by decide), fun h => by omega⟩
    · rw [phaseAt_reserveBatch_outside s k count (old p below), inv.unreserved_iff]
      omega
    · have outside : ¬(s.claim ≤ p ∧ p < s.claim + count) := fun h => by omega
      rw [phaseAt_reserveBatch_outside s k count outside, inv.unreserved_iff]
      omega
  · intro p below
    rcases trichotomy p with inside | below_claim | beyond
    · rw [who_reserveBatch_inside s k count inside]; exact k_below
    · rw [who_reserveBatch_outside s k count (old p below_claim)]
      exact inv.who_below p below_claim
    · exact absurd below (by show ¬ p < s.claim + count; omega)
  · intro first second same_slot same_who first_owned second_owned
    have first_below := owned_below first first_owned
    have second_below := owned_below second second_owned
    rcases trichotomy first with first_in | first_old | first_beyond <;>
      rcases trichotomy second with second_in | second_old | second_beyond
    · rcases Nat.lt_trichotomy first second with before | same | after
      · have := same_slot_lap_apart same_slot before
        omega
      · exact same
      · have := same_slot_lap_apart same_slot.symm after
        omega
    · rw [who_reserveBatch_inside s k count first_in,
        who_reserveBatch_outside s k count (old second second_old)] at same_who
      rw [phaseAt_reserveBatch_outside s k count (old second second_old)] at second_owned
      have := owns_nothing first second first_in.1 first_in.2 same_slot.symm same_who.symm
      rw [this] at second_owned
      exact absurd second_owned (by decide)
    · omega
    · rw [who_reserveBatch_inside s k count second_in,
        who_reserveBatch_outside s k count (old first first_old)] at same_who
      rw [phaseAt_reserveBatch_outside s k count (old first first_old)] at first_owned
      have := owns_nothing second first second_in.1 second_in.2 same_slot same_who
      rw [this] at first_owned
      exact absurd first_owned (by decide)
    · rw [who_reserveBatch_outside s k count (old first first_old),
        who_reserveBatch_outside s k count (old second second_old)] at same_who
      rw [phaseAt_reserveBatch_outside s k count (old first first_old)] at first_owned
      rw [phaseAt_reserveBatch_outside s k count (old second second_old)] at second_owned
      exact inv.one_owned first second same_slot same_who first_owned second_owned
    · omega
    · omega
    · omega
    · omega
  · intro j bit unused
    by_cases same : j = k
    · rw [same, producers_reserveBatch_self] at unused ⊢
      simp only [Producer.batch_unused, Producer.batch_start, decide_eq_true_eq] at unused ⊢
      refine ⟨by omega, ?_, ?_⟩
      · exact phaseAt_reserveBatch_inside s k count ⟨by omega, by omega⟩
      · exact who_reserveBatch_inside s k count ⟨by omega, by omega⟩
    · rw [producers_reserveBatch_other s count same] at unused ⊢
      obtain ⟨bit_below, reserved, owner⟩ := inv.held_reserved j bit unused
      have below := inv.below_claim (by rw [reserved]; decide)
      rw [phaseAt_reserveBatch_outside s k count (old _ below),
        who_reserveBatch_outside s k count (old _ below)]
      exact ⟨bit_below, reserved, owner⟩
  · intro j bit writing
    by_cases same : j = k
    · rw [same, producers_reserveBatch_self, Producer.batch_writing] at writing
      exact absurd writing (by simp)
    · rw [producers_reserveBatch_other s count same] at writing ⊢
      obtain ⟨bit_below, held, owner⟩ := inv.held_writing j bit writing
      have below := inv.below_claim (by rw [held]; decide)
      rw [phaseAt_reserveBatch_outside s k count (old _ below),
        who_reserveBatch_outside s k count (old _ below)]
      exact ⟨bit_below, held, owner⟩
  · intro open_ring p reserved
    have open_old : s.closed = false := open_ring
    rcases trichotomy p with inside | below | beyond
    · rw [who_reserveBatch_inside s k count inside, producers_reserveBatch_self]
      refine ⟨p - s.claim, ?_, ?_⟩
      · simp only [Producer.batch_unused, decide_eq_true_eq]; omega
      · simp only [Producer.batch_start]; omega
    · rw [phaseAt_reserveBatch_outside s k count (old p below)] at reserved
      obtain ⟨bit, unused, placed⟩ := inv.reserved_held open_old p reserved
      have different : s.who p ≠ k := fun same => by
        rw [same, idle.1 bit] at unused
        exact absurd unused (by decide)
      rw [who_reserveBatch_outside s k count (old p below),
        producers_reserveBatch_other s count different]
      exact ⟨bit, unused, placed⟩
    · have outside : ¬(s.claim ≤ p ∧ p < s.claim + count) := fun h => by omega
      rw [phaseAt_reserveBatch_outside s k count outside,
        (inv.unreserved_iff p).2 (by omega)] at reserved
      exact absurd reserved (by decide)
  · intro p writing
    rcases trichotomy p with inside | below | beyond
    · rw [phaseAt_reserveBatch_inside s k count inside] at writing
      exact absurd writing (by decide)
    · rw [phaseAt_reserveBatch_outside s k count (old p below)] at writing
      obtain ⟨bit, held, placed⟩ := inv.writing_held p writing
      have different : s.who p ≠ k := fun same => by
        rw [same, idle.2] at held
        exact absurd held (by simp)
      rw [who_reserveBatch_outside s k count (old p below),
        producers_reserveBatch_other s count different]
      exact ⟨bit, held, placed⟩
    · have outside : ¬(s.claim ≤ p ∧ p < s.claim + count) := fun h => by omega
      rw [phaseAt_reserveBatch_outside s k count outside,
        (inv.unreserved_iff p).2 (by omega)] at writing
      exact absurd writing (by decide)
  · intro j not_none
    by_cases same : j = k
    · rw [same, producers_reserveBatch_self, Producer.batch_pending] at not_none
      exact absurd rfl not_none
    · rw [producers_reserveBatch_other s count same] at not_none ⊢
      exact inv.pending_idle j not_none
  · intro j batch_size is_free
    by_cases same : j = k
    · rw [same, producers_reserveBatch_self, Producer.batch_pending] at is_free
      exact absurd is_free (by simp)
    · rw [producers_reserveBatch_other s count same] at is_free
      obtain ⟨at_least_one, fits, owns⟩ := inv.pending_free j batch_size is_free
      refine ⟨at_least_one, fits, ?_⟩
      intro p who_p
      by_cases inside : s.claim ≤ p ∧ p < s.claim + count
      · rw [who_reserveBatch_inside s k count inside] at who_p
        exact absurd who_p.symm same
      · rw [who_reserveBatch_outside s k count inside] at who_p
        rw [phaseAt_reserveBatch_outside s k count inside]
        exact owns p who_p
  · intro j seen is_fallback
    by_cases same : j = k
    · rw [same, producers_reserveBatch_self, Producer.batch_pending] at is_fallback
      exact absurd is_fallback (by simp)
    · rw [producers_reserveBatch_other s count same] at is_fallback
      have := inv.fallback_seen_le j seen is_fallback
      show seen ≤ s.claim + count
      omega
  · intro j seen is_fallback at_claim
    by_cases same : j = k
    · rw [same, producers_reserveBatch_self, Producer.batch_pending] at is_fallback
      exact absurd is_fallback (by simp)
    · rw [producers_reserveBatch_other s count same] at is_fallback
      have := inv.fallback_seen_le j seen is_fallback
      have claim_moved : seen = s.claim + count := at_claim
      omega

theorem Inv.commitFreeKnown (inv : Inv limit s) (wraps_fit : limit + 2 ≤ g.wraps) {k batch : Nat}
    (k_below : k < limit) (pending : (s.producers k).pending = .freeKnown batch) :
    Inv limit (s.reserveBatch k batch) := by
  obtain ⟨positive, fits, owns⟩ := inv.pending_free k batch pending
  exact inv.reserveBatch wraps_fit k_below positive fits
    (inv.pending_idle k (by rw [pending]; simp))
    (fun _ p _ _ _ who_p => owns p who_p)

theorem Inv.commitFallback (inv : Inv limit s) (wraps_fit : limit + 2 ≤ g.wraps) {k seen : Nat}
    (k_below : k < limit) (pending : (s.producers k).pending = .fallback seen)
    (at_claim : seen = s.claim) : Inv limit (s.reserveBatch k 1) :=
  inv.reserveBatch wraps_fit k_below (Nat.le_refl 1)
    (by have := g.four_le_period; omega)
    (inv.pending_idle k (by rw [pending]; simp))
    (fun q p at_least within same_slot _ => by
      have is_claim : q = s.claim := by omega
      rw [is_claim] at same_slot
      exact inv.fallback_owns_nothing pending at_claim p same_slot)

end Ring

end Qtz
