import Qtz.MpmcSlot

/-!
Model of `SlotSequence` (`src/common/slot_sequence.rs`), the word of one mpsc slot.

The word for position `pos` in state `state` (free 0, published 1, abandoned 2) is
`(pos mod 2 ^ positionBits) * 4 + state` reduced to `width` bits. The Rust has
`positionBits = 32`, because a producer learns only the low 32 bits of the position it claimed,
and `width = usize::BITS`. `classify` reads a word against the position the consumer expects.
Two words are equal only when both state and position agree, for positions fewer than
`2 ^ min positionBits (width - 2)` apart.
-/

namespace Qtz

/-- Reducing to one power of two and then another reduces to the smaller. -/
theorem mod_two_pow_mod_two_pow (value first second : Nat) :
    value % 2 ^ first % 2 ^ second = value % 2 ^ min first second := by
  by_cases ordered : first ≤ second
  · rw [Nat.min_eq_left ordered]
    exact Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le (Nat.mod_lt _ (Nat.two_pow_pos _))
      (Nat.pow_le_pow_right (by omega) ordered))
  · rw [Nat.min_eq_right (by omega)]
    exact Nat.mod_mod_of_dvd _ (Nat.pow_dvd_pow 2 (by omega))

namespace MpscWord

def encode (positionBits width pos state : Nat) : Nat :=
  (pos % 2 ^ positionBits * 4 + state) % 2 ^ width

inductive Class where
  | ready
  | tombstoned
  | notReady
  deriving DecidableEq

def classify (positionBits width word pos : Nat) : Class :=
  if word = encode positionBits width pos 1 then .ready
  else if word = encode positionBits width pos 2 then .tombstoned
  else .notReady

/-- The word after `release(head, capacity)`: free for the next lap. -/
def released (positionBits width head capacity : Nat) : Nat :=
  encode positionBits width (head + capacity) 0

theorem encode_split (quarter pos state : Nat) (quarter_pos : 0 < quarter)
    (state_below : state < 4) :
    (pos * 4 + state) % (4 * quarter) = 4 * (pos % quarter) + state := by
  have remainder_below := Nat.mod_lt pos quarter_pos
  have split := Nat.mod_add_div pos quarter
  generalize pos % quarter = remainder at *
  generalize pos / quarter = lap at *
  subst split
  have rewritten : (remainder + quarter * lap) * 4 + state
      = (4 * remainder + state) + (4 * quarter) * lap := by
    have : quarter * lap * 4 = 4 * quarter * lap := by
      rw [Nat.mul_comm 4 quarter, Nat.mul_right_comm]
    rw [Nat.add_mul, this]
    omega
  rw [rewritten, Nat.add_mul_mod_self_left]
  exact Nat.mod_eq_of_lt (by omega)

variable {positionBits width : Nat}

/-- A producer that knows only the low `positionBits` bits of its position writes the same
word as one that knows all of it. -/
theorem encode_reduced (pos state : Nat) :
    encode positionBits width (pos % 2 ^ positionBits) state
      = encode positionBits width pos state := by
  unfold encode
  rw [Nat.mod_mod]

/-- The consumer's head is a `width`-bit word. It writes and expects the same slot word as the
unwrapped head, as long as the position field is no wider than the word. -/
theorem encode_wrapped (positionBits_le : positionBits ≤ width) (pos state : Nat) :
    encode positionBits width (pos % 2 ^ width) state = encode positionBits width pos state := by
  unfold encode
  rw [Nat.mod_mod_of_dvd _ (Nat.pow_dvd_pow 2 positionBits_le)]

theorem encode_eq_iff (width_two : 2 ≤ width) {pos other state other_state : Nat}
    (state_below : state < 4) (other_below : other_state < 4) :
    encode positionBits width pos state = encode positionBits width other other_state ↔
      state = other_state ∧
        pos % 2 ^ min positionBits (width - 2) = other % 2 ^ min positionBits (width - 2) := by
  obtain ⟨rest, rfl⟩ := Nat.exists_eq_add_of_le width_two
  have split : 2 ^ (2 + rest) = 4 * 2 ^ rest := by
    rw [Nat.pow_add]
  have quarter_pos := Nat.two_pow_pos rest
  unfold encode
  rw [Nat.add_sub_cancel_left, split, encode_split _ _ _ quarter_pos state_below,
    encode_split _ _ _ quarter_pos other_below, mod_two_pow_mod_two_pow, mod_two_pow_mod_two_pow]
  omega

/-- Positions close enough together have the same word only when they are the same position
and state. -/
theorem encode_eq_of_close (width_two : 2 ≤ width)
    {behind ahead state other_state : Nat} (state_below : state < 4) (other_below : other_state < 4)
    (ordered : behind ≤ ahead) (close : ahead < behind + 2 ^ min positionBits (width - 2))
    (same : encode positionBits width behind state = encode positionBits width ahead other_state) :
    state = other_state ∧ behind = ahead := by
  obtain ⟨same_state, same_position⟩ := (encode_eq_iff width_two state_below other_below).1 same
  exact ⟨same_state, (eq_of_mod_eq_of_close ordered close same_position).symm⟩

/-- The consumer classifies against its `width`-bit head and gets the reading of the unwrapped
one. -/
theorem classify_wrapped (positionBits_le : positionBits ≤ width) (word pos : Nat) :
    classify positionBits width word (pos % 2 ^ width) = classify positionBits width word pos := by
  unfold classify
  rw [encode_wrapped positionBits_le, encode_wrapped positionBits_le]

theorem classify_published (pos : Nat) :
    classify positionBits width (encode positionBits width pos 1) pos = .ready := by
  simp [classify]

theorem classify_tombstoned (width_two : 2 ≤ width) (pos : Nat) :
    classify positionBits width (encode positionBits width pos 2) pos = .tombstoned := by
  have : encode positionBits width pos 2 ≠ encode positionBits width pos 1 := fun same =>
    absurd ((encode_eq_iff width_two (by omega) (by omega)).1 same).1 (by omega)
  simp [classify, this]

theorem classify_free (width_two : 2 ≤ width) (pos : Nat) :
    classify positionBits width (encode positionBits width pos 0) pos = .notReady := by
  have published : encode positionBits width pos 0 ≠ encode positionBits width pos 1 :=
    fun same => absurd ((encode_eq_iff width_two (by omega) (by omega)).1 same).1 (by omega)
  have abandoned : encode positionBits width pos 0 ≠ encode positionBits width pos 2 :=
    fun same => absurd ((encode_eq_iff width_two (by omega) (by omega)).1 same).1 (by omega)
  simp [classify, published, abandoned]

/-- A word written for another position close enough never reads as `ready` or `tombstoned`
at the wanted position. -/
theorem classify_other_position (width_two : 2 ≤ width)
    {written wanted state : Nat} (state_below : state < 4)
    (apart : written < wanted + 2 ^ min positionBits (width - 2) ∧ wanted ≤ written ∨
      wanted < written + 2 ^ min positionBits (width - 2) ∧ written ≤ wanted)
    (different : written ≠ wanted) :
    classify positionBits width (encode positionBits width written state) wanted = .notReady := by
  have mismatch : ∀ other_state, other_state < 4 →
      encode positionBits width written state ≠ encode positionBits width wanted other_state := by
    intro other_state other_below same
    rcases apart with ⟨near, le⟩ | ⟨near, le⟩
    · exact different (encode_eq_of_close width_two other_below state_below le near same.symm
        |>.2.symm)
    · exact different (encode_eq_of_close width_two state_below other_below le near same).2
  simp [classify, mismatch 1 (by omega), mismatch 2 (by omega)]

end MpscWord

end Qtz
