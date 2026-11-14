import Qtz.MpscRing
import Qtz.MpscSlotWord
import Qtz.MpscClaimWord

/-!
Joins the mpsc pieces. `MpscRing` runs on unwrapped positions, counted credits and abstract
`(position, state)` slot words; the Rust runs on a packed claim word and `width`-bit slot
words. Here the Rust's tests and updates on those words are shown to decide and perform the
ring's steps.

* The claim word of a ring state is `ClaimWord.layout` of its tail, refill bit and credits. A
  compare-exchange that succeeds on a word with a credit is a `claim` step, one that succeeds
  on a word with no credit and no refill bit is a `mark` step, and both stores in
  `ClaimLedger::refill` write the word of the state after `reportFull` and `grant`. The
  expected word may have been loaded at any earlier time: the only fact used about it is that
  the exchange found the word equal to it.
* A step that moves neither the position nor the refill bit leaves the claim word alone, so
  those four writes are the only ones.
* The position a producer takes from the claim word is the claimed position reduced to
  `positionBits` bits. The slot it indexes with that, and the slot word it writes from it, are
  those of the full position.
* The counts the consumer side computes from words, in `publish_head`, `len` and `occupied`,
  are the counts of the ring.
* The consumer's `classify` of the word stored at `head` reports exactly the abstract state.
  So a published word is consumed as a value, a tombstone is stepped over, and a free word,
  whether unclaimed or claimed but not finished, is waited on.

Producers never read slot words in the mpsc ring, so word distinctness across positions
(`encode_eq_of_close`) is not needed: the consumer reads only the slot at `head`, whose
abstract position is `head`.
-/

namespace Qtz

namespace Mpsc

variable {capacity positionBits creditBits : Nat}

def Ring.claimWord (positionBits creditBits : Nat) (s : Ring capacity) : Nat :=
  ClaimWord.layout positionBits creditBits s.tail s.refill.busy s.credits

theorem Inv.credits_fit {s : Ring capacity} (inv : Inv s) (capacity_fits : capacity < 2 ^ creditBits) :
    s.credits < 2 ^ creditBits := by
  have := inv.credit_bounds.1
  omega

/-- The compare-exchange in `ClaimLedger::claim` that succeeds on a word with a credit. The
producer gets the claimed position reduced to the position field, the ring had room for it,
and the word it stored is the word of the ring after the claim. -/
theorem Inv.claim_of_matching_word {s : Ring capacity} (inv : Inv s)
    (capacity_fits : capacity < 2 ^ creditBits) {expected : Nat}
    (exchange_succeeds : expected = s.claimWord positionBits creditBits)
    (has_credit : ClaimWord.creditsOf creditBits expected ≠ 0) :
    ∃ t, Step s t ∧ s.tail < s.head + capacity ∧ t.claimed s.tail = true ∧
      ClaimWord.positionOf creditBits expected = s.tail % 2 ^ positionBits ∧
      ClaimWord.claim positionBits creditBits expected = t.claimWord positionBits creditBits := by
  subst exchange_succeeds
  have credits_fit := inv.credits_fit capacity_fits
  unfold Ring.claimWord at has_credit ⊢
  rw [ClaimWord.creditsOf_layout credits_fit] at has_credit
  have idle := inv.idle_of_credit has_credit
  have conserved := inv.conserved
  have published_le := inv.published_le_head
  refine ⟨_, Step.claim (Nat.pos_of_ne_zero has_credit), by omega, by simp [setClaimed],
    ClaimWord.positionOf_layout credits_fit, ?_⟩
  simp only [idle, Refill.busy]
  exact ClaimWord.claim_layout credits_fit (Nat.pos_of_ne_zero has_credit)

/-- The compare-exchange that succeeds on a word with no credit and no refill bit sets the
bit, and its holder reads the position to refill at. -/
theorem Inv.mark_of_matching_word {s : Ring capacity} (inv : Inv s)
    (capacity_fits : capacity < 2 ^ creditBits) {expected : Nat}
    (exchange_succeeds : expected = s.claimWord positionBits creditBits)
    (no_credit : ClaimWord.creditsOf creditBits expected = 0)
    (not_refilling : ClaimWord.refillBit creditBits expected = 0) :
    ∃ t, Step s t ∧ t.refill = .marked ∧ t.tail = s.tail ∧
      ClaimWord.positionOf creditBits expected = s.tail % 2 ^ positionBits ∧
      ClaimWord.mark creditBits expected = t.claimWord positionBits creditBits := by
  subst exchange_succeeds
  have credits_fit := inv.credits_fit capacity_fits
  unfold Ring.claimWord at no_credit not_refilling ⊢
  rw [ClaimWord.creditsOf_layout credits_fit] at no_credit
  rw [ClaimWord.refillBit_layout credits_fit] at not_refilling
  have idle : s.refill = .idle := by
    cases refill : s.refill <;> simp [refill, Refill.busy] at not_refilling ⊢
  refine ⟨_, Step.mark no_credit idle, rfl, rfl, ClaimWord.positionOf_layout credits_fit, ?_⟩
  simp only [idle, no_credit, Refill.busy]
  exact ClaimWord.mark_layout

/-- The refill bit of the word is clear exactly when no producer is refilling. The Rust reads
it twice: a producer that loads it set waits, and after a successful exchange the bit of the
word it stored tells a claim from a refill. -/
theorem Inv.refillBit_claimWord {s : Ring capacity} (inv : Inv s)
    (capacity_fits : capacity < 2 ^ creditBits) :
    ClaimWord.refillBit creditBits (s.claimWord positionBits creditBits) = 0
      ↔ s.refill = .idle := by
  unfold Ring.claimWord
  rw [ClaimWord.refillBit_layout (inv.credits_fit capacity_fits)]
  cases s.refill <;> simp [Refill.busy]

/-- `ClaimLedger::refill` with nothing granted stores `pack(position, 0)`, where `position`
is what the holder read when it set the bit. `Refilling.tail` shows the tail has not moved
since, which is `position_read`. The stored word is the word of the ring after
`reportFull`. -/
theorem Inv.reportFull_word {s : Ring capacity} (inv : Inv s) {position : Nat}
    (position_read : position = s.tail % 2 ^ positionBits) (nothing : s.refill = .holding 0) :
    ∃ t, Step s t ∧
      ClaimWord.pack positionBits creditBits position 0 = t.claimWord positionBits creditBits := by
  have no_credit := inv.refilling_no_credit (by simp [nothing])
  refine ⟨_, Step.reportFull nothing, ?_⟩
  simp only [Ring.claimWord, Refill.busy, no_credit]
  rw [ClaimWord.pack_layout (Nat.two_pow_pos _)]
  exact ClaimWord.layout_congr (by rw [position_read, Nat.mod_mod])

/-- `ClaimLedger::refill` with credits granted claims `position` with one of them and stores
`pack(position.wrapping_add(1), granted - 1)`, the addition wrapping at the `width` of a
`usize`. The ring had room for the position, and the stored word is the word of the ring
after `grant`. -/
theorem Inv.grant_word {width : Nat} {s : Ring capacity} (inv : Inv s)
    (capacity_fits : capacity < 2 ^ creditBits) (positionBits_le : positionBits ≤ width)
    {position granted : Nat} (position_read : position = s.tail % 2 ^ positionBits)
    (holding : s.refill = .holding (granted + 1)) :
    ∃ t, Step s t ∧ s.tail < s.head + capacity ∧ t.claimed s.tail = true ∧
      ClaimWord.pack positionBits creditBits ((position + 1) % 2 ^ width) granted
        = t.claimWord positionBits creditBits := by
  have held_le := inv.credit_bounds.2.2
  have conserved := inv.conserved
  have published_le := inv.published_le_head
  rw [holding] at held_le conserved
  simp only [Refill.held] at held_le conserved
  refine ⟨_, Step.grant granted holding, by omega, by simp [setClaimed], ?_⟩
  simp only [Ring.claimWord, Refill.busy]
  rw [ClaimWord.pack_layout (by omega)]
  exact ClaimWord.layout_congr (by
    rw [Nat.mod_mod_of_dvd _ (Nat.pow_dvd_pow 2 positionBits_le), position_read,
      Nat.mod_add_mod])

/-- The whole refill, from the state `marked` in which the bit was set to the store that
clears it, whatever the other threads did in between: the position read at `marked` is the one
the store claims or leaves. -/
theorem Refilling.store_word {width : Nat} {marked s : Ring capacity}
    (capacity_positive : 0 < capacity) (inv : Inv marked) (window : Refilling marked s)
    (capacity_fits : capacity < 2 ^ creditBits) (positionBits_le : positionBits ≤ width)
    {position : Nat} (position_read : position = marked.tail % 2 ^ positionBits) :
    (s.refill = .holding 0 → ∃ t, Step s t ∧
      ClaimWord.pack positionBits creditBits position 0
        = t.claimWord positionBits creditBits) ∧
    (∀ granted, s.refill = .holding (granted + 1) →
      ∃ t, Step s t ∧ s.tail < s.head + capacity ∧ t.claimed s.tail = true ∧
        ClaimWord.pack positionBits creditBits ((position + 1) % 2 ^ width) granted
          = t.claimWord positionBits creditBits) := by
  obtain ⟨same, inv_now, _⟩ := window.tail capacity_positive inv
  have read_now : position = s.tail % 2 ^ positionBits := by rw [same]; exact position_read
  exact ⟨fun nothing => inv_now.reportFull_word read_now nothing,
    fun _ holding => inv_now.grant_word capacity_fits positionBits_le read_now holding⟩

/-- The claim word changes only when the position or the refill bit does. The consumer's
addition to `pending`, the holder's read of it and every slot-word store leave it alone. -/
theorem Step.claimWord_unchanged {s t : Ring capacity} (step : Step s t)
    (same_position : t.tail = s.tail) (same_bit : t.refill.busy = s.refill.busy) :
    t.claimWord positionBits creditBits = s.claimWord positionBits creditBits := by
  cases step with
  | claim => simp only at same_position; omega
  | mark _ idle => simp [idle, Refill.busy] at same_bit
  | collect => simp only [Ring.claimWord, same_bit]
  | reportFull nothing => simp [nothing, Refill.busy] at same_bit
  | grant => simp only at same_position; omega
  | finish => rfl
  | consume => rfl
  | publish => rfl

/-- `publish_head` computes the credits to add as `head.wrapping_sub(published)`. The head is
never a lap ahead of what it has credited, so that is the true count. -/
theorem Inv.released_count {width : Nat} {s : Ring capacity} (inv : Inv s)
    (capacity_fits : capacity < 2 ^ width) :
    (BitVec.ofNat width s.head - BitVec.ofNat width s.published).toNat
      = s.head - s.published := by
  have conserved := inv.conserved
  have head_le_tail := inv.slots.head_le_tail
  obtain ⟨released, head_eq⟩ := Nat.exists_eq_add_of_le inv.published_le_head
  rw [head_eq, BitVec.ofNat_add, BitVec.add_comm, BitVec.add_sub_cancel, BitVec.toNat_ofNat,
    Nat.mod_eq_of_lt (by omega)]
  omega

/-- `len` and `occupied` compute `capacity - (credits + pending)`. With no refill under way
that is the number of positions claimed and not yet credited back. -/
theorem Inv.unreleased_count {s : Ring capacity} (inv : Inv s) (idle : s.refill = .idle) :
    capacity - (s.credits + s.pending) = s.tail - s.published := by
  have conserved := inv.conserved
  rw [idle] at conserved
  simp only [Refill.held] at conserved
  omega

/-- A producer indexes its slot with the reduced position. For a power-of-two capacity no
larger than the position field that is the slot of the full position. -/
theorem slot_of_reduced_position {log : Nat} (log_le : log ≤ positionBits) (position : Nat) :
    position % 2 ^ positionBits % 2 ^ log = position % 2 ^ log :=
  Nat.mod_mod_of_dvd _ (Nat.pow_dvd_pow 2 log_le)

variable {width : Nat}

def Ring.concreteWord (positionBits width : Nat) (s : Ring capacity) (index : Nat) : Nat :=
  MpscWord.encode positionBits width (s.word index).1 (s.word index).2

def Ring.headClass (positionBits width : Nat) (s : Ring capacity) : MpscWord.Class :=
  MpscWord.classify positionBits width (s.concreteWord positionBits width (s.head % capacity))
    s.head

/-- The slot word a producer writes when it finishes a claim. It holds only the reduced
position the claim word gave it, and writes the word of the full position. -/
theorem finish_word (position state : Nat) :
    MpscWord.encode positionBits width (position % 2 ^ positionBits) state
      = MpscWord.encode positionBits width position state :=
  MpscWord.encode_reduced position state

theorem Inv.head_word {s : Ring capacity} (inv : Inv s) (capacity_positive : 0 < capacity)
    (state : Nat) (state_is : (s.word (s.head % capacity)).2 = state) :
    s.word (s.head % capacity) = (s.head, state) :=
  Prod.ext (inv.slots.position s.head (Nat.le_refl _) (by omega)) state_is

theorem Inv.headClass_free (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (state_is : (s.word (s.head % capacity)).2 = 0) :
    s.headClass positionBits width = .notReady := by
  unfold Ring.headClass Ring.concreteWord
  rw [inv.head_word capacity_positive 0 state_is]
  exact MpscWord.classify_free width_two s.head

theorem Inv.headClass_published {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (state_is : (s.word (s.head % capacity)).2 = 1) :
    s.headClass positionBits width = .ready := by
  unfold Ring.headClass Ring.concreteWord
  rw [inv.head_word capacity_positive 1 state_is]
  exact MpscWord.classify_published s.head

theorem Inv.headClass_abandoned (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (state_is : (s.word (s.head % capacity)).2 = 2) :
    s.headClass positionBits width = .tombstoned := by
  unfold Ring.headClass Ring.concreteWord
  rw [inv.head_word capacity_positive 2 state_is]
  exact MpscWord.classify_tombstoned width_two s.head

/-- What the consumer decides from the real word is what the ring's `consume` step needs: a
`ready` or `tombstoned` reading comes with the abstract word the step requires. -/
theorem Inv.consume_of_headClass (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (read : s.headClass positionBits width = .ready
      ∨ s.headClass positionBits width = .tombstoned) :
    ∃ t, Step s t ∧ t.head = s.head + 1 := by
  have bounded := inv.slots.state_below s.head (Nat.le_refl _) (by omega)
  have cases : (s.word (s.head % capacity)).2 = 0 ∨ (s.word (s.head % capacity)).2 = 1 ∨
      (s.word (s.head % capacity)).2 = 2 := by omega
  rcases cases with zero | one | two
  · rw [inv.headClass_free width_two capacity_positive zero] at read
    cases read <;> contradiction
  · exact ⟨_, Step.consume 1 (inv.head_word capacity_positive 1 one) (Or.inl rfl), rfl⟩
  · exact ⟨_, Step.consume 2 (inv.head_word capacity_positive 2 two) (Or.inr rfl), rfl⟩

/-- A `notReady` reading means the head position has not been finished: either nobody
claimed it yet, or its owner still holds it. The consumer never skips it. -/
theorem Inv.unfinished_of_notReady (width_two : 2 ≤ width) {s : Ring capacity}
    (inv : Inv s) (capacity_positive : 0 < capacity)
    (waiting : s.headClass positionBits width = .notReady) :
    s.tail ≤ s.head ∨ s.claimed s.head = true := by
  have bounded := inv.slots.state_below s.head (Nat.le_refl _) (by omega)
  have free_iff := inv.slots.free_iff s.head (Nat.le_refl _) (by omega)
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
