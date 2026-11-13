import Qtz.SlotWord

/-!
Model of `SlotState::classify` (`src/broadcast/slot_state.rs`) for the
word a consumer loaded, reading `position`.
-/

namespace Qtz

inductive SlotSnapshot where
  | ready
  | tombstoned
  | notReady
  deriving DecidableEq

def classify {width : Nat} (word position : BitVec width) : SlotSnapshot :=
  if word = publishedWord position then .ready
  else if word = abandonedWord position then .tombstoned
  else .notReady

theorem classify_ready_iff {width : Nat} (word position : BitVec width) :
    classify word position = .ready ↔ word = publishedWord position := by
  unfold classify
  split <;> rename_i published_case
  · simp [published_case]
  · split <;> simp [published_case]

theorem classify_tombstoned_iff {surviving_bits : Nat} (word position : BitVec (surviving_bits + 2)) :
    classify word position = .tombstoned ↔ word = abandonedWord position := by
  unfold classify
  split <;> rename_i published_case
  · simp only [reduceCtorEq, false_iff]
    intro abandoned_case
    exact publishedWord_ne_abandonedWord position position (published_case.symm.trans abandoned_case)
  · split <;> simp [*]

theorem classify_own_published {width : Nat} (position : BitVec width) :
    classify (publishedWord position) position = .ready :=
  (classify_ready_iff _ _).2 rfl

theorem classify_own_abandoned {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    classify (abandonedWord position) position = .tombstoned :=
  (classify_tombstoned_iff _ _).2 rfl

theorem classify_not_ready {width : Nat} (word position : BitVec width)
    (not_published : word ≠ publishedWord position) (not_abandoned : word ≠ abandonedWord position) :
    classify word position = .notReady := by
  simp [classify, not_published, not_abandoned]

theorem classify_vacant {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    classify vacantWord position = .notReady :=
  classify_not_ready _ _ (publishedWord_ne_vacant position).symm
    (abandonedWord_ne_vacant position).symm

theorem classify_unwritten {surviving_bits : Nat} (position : BitVec (surviving_bits + 2)) :
    classify unwrittenWord position = .notReady :=
  classify_not_ready _ _ (publishedWord_ne_unwritten position).symm
    (abandonedWord_ne_unwritten position).symm

theorem residues_differ_of_nearby {surviving_bits : Nat} (left right : BitVec (surviving_bits + 2))
    (nearby : (0 < (left - right).toNat ∧ (left - right).toNat < 2 ^ surviving_bits) ∨
      (0 < (right - left).toNat ∧ (right - left).toNat < 2 ^ surviving_bits)) :
    left.toNat % 2 ^ surviving_bits ≠ right.toNat % 2 ^ surviving_bits := by
  rcases nearby with ⟨positive, below⟩ | ⟨positive, below⟩
  · exact residues_differ_of_distance left right positive below
  · exact (residues_differ_of_distance right left positive below).symm

/-- A marker left by another position within one period of the reader's
own, ahead or behind, is never mistaken for the reader's own. -/
theorem classify_nearby_published {surviving_bits : Nat} (marker_position position : BitVec (surviving_bits + 2))
    (nearby : (0 < (marker_position - position).toNat ∧
        (marker_position - position).toNat < 2 ^ surviving_bits) ∨
      (0 < (position - marker_position).toNat ∧
        (position - marker_position).toNat < 2 ^ surviving_bits)) :
    classify (publishedWord marker_position) position = .notReady := by
  have residues_differ := residues_differ_of_nearby marker_position position nearby
  exact classify_not_ready _ _
    (fun words_equal => residues_differ ((publishedWord_eq_iff _ _).1 words_equal))
    (publishedWord_ne_abandonedWord marker_position position)

theorem classify_nearby_abandoned {surviving_bits : Nat} (marker_position position : BitVec (surviving_bits + 2))
    (nearby : (0 < (marker_position - position).toNat ∧
        (marker_position - position).toNat < 2 ^ surviving_bits) ∨
      (0 < (position - marker_position).toNat ∧
        (position - marker_position).toNat < 2 ^ surviving_bits)) :
    classify (abandonedWord marker_position) position = .notReady := by
  have residues_differ := residues_differ_of_nearby marker_position position nearby
  exact classify_not_ready _ _
    (fun words_equal => publishedWord_ne_abandonedWord position marker_position words_equal.symm)
    (fun words_equal => residues_differ ((abandonedWord_eq_iff _ _).1 words_equal))

end Qtz
