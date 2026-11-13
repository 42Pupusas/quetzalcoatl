import Qtz.MpscRing

/-!
Joins the mpsc pieces. `MpscRing` runs on unwrapped positions and abstract `(position, state)`
words; the Rust runs on `width`-bit words. Here the Rust tests are shown to decide what the
ring's steps assume.

* A producer's full-ring test, on a cached or a fresh `published` head, implies the claim
  guard `tail < published + capacity`.
* The consumer's `classify` of the word stored at `head`, which is the encoding of the
  abstract word, reports exactly the abstract state. So a published word is consumed as a
  value, a tombstone is stepped over, and a free word, whether unclaimed or claimed but not
  finished, is waited on.

Producers never read slot words in the mpsc ring, so word distinctness across positions
(`encode_eq_of_close`) is not needed: the consumer reads only the slot at `head`, whose
abstract position is `head`.
-/

namespace Qtz

namespace Mpsc

variable {capacity : Nat}

inductive Steps : Ring capacity → Ring capacity → Prop
  | refl (s : Ring capacity) : Steps s s
  | step {s t u : Ring capacity} : Step s t → Steps t u → Steps s u

theorem Step.published_mono {s t : Ring capacity} (inv : Inv s) (step : Step s t) :
    s.published ≤ t.published := by
  cases step with
  | claim => exact Nat.le_refl _
  | finish => exact Nat.le_refl _
  | consume => exact Nat.le_refl _
  | publish => exact inv.published_le_head

theorem Steps.published_mono {capacity : Nat} (capacity_positive : 0 < capacity)
    {s t : Ring capacity} (inv : Inv s) (steps : Steps s t) : s.published ≤ t.published := by
  induction steps with
  | refl => exact Nat.le_refl _
  | step first _ rest =>
    exact Nat.le_trans (Step.published_mono inv first)
      (rest (inv.step capacity_positive first))

/-- A producer's full-ring test on a cached head is sound. The cache was read at `before`, an
earlier state, and must be less than a word lap behind the tail. -/
theorem claim_guard_of_cached {width : Nat} (capacity_positive : 0 < capacity)
    {before s : Ring capacity} (inv_before : Inv before) (earlier : Steps before s)
    (inv : Inv s) (capacity_fits : capacity < 2 ^ width)
    (cache_within_lap : s.tail - before.published < 2 ^ width)
    (room : wrappedRoom (width := width) s.tail before.published capacity = true) :
    s.tail < s.published + capacity := by
  have cached_le := Steps.published_mono capacity_positive inv_before earlier
  have published_le_head := inv.published_le_head
  have head_le_tail := inv.head_le_tail
  have := claim_room_of_cached (width := width) (tail := s.tail) (head := s.published)
    (cached := before.published) cached_le (by omega) capacity_fits cache_within_lap room
  omega

/-- The same test on a head read just now needs no lap bound. -/
theorem claim_guard_of_fresh {width : Nat} {s : Ring capacity} (inv : Inv s)
    (capacity_fits : capacity < 2 ^ width)
    (room : wrappedRoom (width := width) s.tail s.published capacity = true) :
    s.tail < s.published + capacity := by
  have published_le_head := inv.published_le_head
  have head_le_tail := inv.head_le_tail
  have bounded := inv.tail_le_published
  have := claim_room_of_fresh (width := width) (tail := s.tail) (head := s.published)
    (capacity := capacity) (by omega) capacity_fits (by omega) room
  omega

def Ring.concreteWord (width : Nat) (s : Ring capacity) (index : Nat) : Nat :=
  MpscWord.encode width (s.word index).1 (s.word index).2

def Ring.headClass (width : Nat) (s : Ring capacity) : MpscWord.Class :=
  MpscWord.classify width (s.concreteWord width (s.head % capacity)) s.head

theorem Inv.head_word {s : Ring capacity} (inv : Inv s) (capacity_positive : 0 < capacity)
    (state : Nat) (state_is : (s.word (s.head % capacity)).2 = state) :
    s.word (s.head % capacity) = (s.head, state) :=
  Prod.ext (inv.position s.head (Nat.le_refl _) (by omega)) state_is

theorem Inv.headClass_free {width : Nat} (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (state_is : (s.word (s.head % capacity)).2 = 0) : s.headClass width = .notReady := by
  unfold Ring.headClass Ring.concreteWord
  rw [inv.head_word capacity_positive 0 state_is]
  exact MpscWord.classify_free width_two s.head

theorem Inv.headClass_published {width : Nat} {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (state_is : (s.word (s.head % capacity)).2 = 1) : s.headClass width = .ready := by
  unfold Ring.headClass Ring.concreteWord
  rw [inv.head_word capacity_positive 1 state_is]
  exact MpscWord.classify_published s.head

theorem Inv.headClass_abandoned {width : Nat} (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (state_is : (s.word (s.head % capacity)).2 = 2) : s.headClass width = .tombstoned := by
  unfold Ring.headClass Ring.concreteWord
  rw [inv.head_word capacity_positive 2 state_is]
  exact MpscWord.classify_tombstoned width_two s.head

/-- What the consumer decides from the real word is what the ring's `consume` step needs: a
`ready` or `tombstoned` reading comes with the abstract word the step requires. -/
theorem Inv.consume_of_headClass {width : Nat} (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity) (pending : s.head < s.tail)
    (read : s.headClass width = .ready ∨ s.headClass width = .tombstoned) :
    ∃ t, Step s t := by
  have bounded := inv.state_below s.head (Nat.le_refl _) (by omega)
  have cases : (s.word (s.head % capacity)).2 = 0 ∨ (s.word (s.head % capacity)).2 = 1 ∨
      (s.word (s.head % capacity)).2 = 2 := by omega
  rcases cases with zero | one | two
  · rw [inv.headClass_free width_two capacity_positive zero] at read
    cases read <;> contradiction
  · exact ⟨_, Step.consume 1 pending (inv.head_word capacity_positive 1 one) (Or.inl rfl)⟩
  · exact ⟨_, Step.consume 2 pending (inv.head_word capacity_positive 2 two) (Or.inr rfl)⟩

/-- A `notReady` reading means the head position has not been finished: either nobody
claimed it yet, or its owner still holds it. The consumer never skips it. -/
theorem Inv.unfinished_of_notReady {width : Nat} (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (waiting : s.headClass width = .notReady) :
    s.tail ≤ s.head ∨ s.claimed s.head = true := by
  have bounded := inv.state_below s.head (Nat.le_refl _) (by omega)
  have free_iff := inv.free_iff s.head (Nat.le_refl _) (by omega)
  have cases : (s.word (s.head % capacity)).2 = 0 ∨ (s.word (s.head % capacity)).2 = 1 ∨
      (s.word (s.head % capacity)).2 = 2 := by omega
  rcases cases with zero | one | two
  · exact free_iff.1 zero
  · rw [inv.headClass_published capacity_positive one] at waiting
    cases waiting
  · rw [inv.headClass_abandoned width_two capacity_positive two] at waiting
    cases waiting

end Mpsc

end Qtz
