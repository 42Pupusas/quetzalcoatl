import Qtz.MpmcSlot

/-!
Model of `SlotSequence` (`src/common/slot_sequence.rs`), the word of one mpsc slot.

The word for position `pos` in state `state` (free 0, published 1, abandoned 2) is
`pos * 4 + state` reduced to `width` bits. `classify` reads a word against the position the
consumer expects. Two words are equal only when both state and position agree, for positions
fewer than `2 ^ (width - 2)` apart.
-/

namespace Qtz

namespace MpscWord

def encode (width pos state : Nat) : Nat := (pos * 4 + state) % 2 ^ width

inductive Class where
  | ready
  | tombstoned
  | notReady
  deriving DecidableEq

def classify (width word pos : Nat) : Class :=
  if word = encode width pos 1 then .ready
  else if word = encode width pos 2 then .tombstoned
  else .notReady

/-- The word after `release(head, capacity)`: free for the next lap. -/
def released (width head capacity : Nat) : Nat := encode width (head + capacity) 0

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

theorem encode_eq_iff {width : Nat} (width_two : 2 ≤ width) {pos other state other_state : Nat}
    (state_below : state < 4) (other_below : other_state < 4) :
    encode width pos state = encode width other other_state ↔
      state = other_state ∧ pos % 2 ^ (width - 2) = other % 2 ^ (width - 2) := by
  obtain ⟨rest, rfl⟩ := Nat.exists_eq_add_of_le width_two
  have split : 2 ^ (2 + rest) = 4 * 2 ^ rest := by
    rw [Nat.pow_add]
  have quarter_pos := Nat.two_pow_pos rest
  unfold encode
  rw [Nat.add_sub_cancel_left, split, encode_split _ _ _ quarter_pos state_below,
    encode_split _ _ _ quarter_pos other_below]
  omega

/-- Positions fewer than a quarter of the word space apart have the same word only when they
are the same position and state. -/
theorem encode_eq_of_close {width : Nat} (width_two : 2 ≤ width)
    {behind ahead state other_state : Nat} (state_below : state < 4) (other_below : other_state < 4)
    (ordered : behind ≤ ahead) (close : ahead < behind + 2 ^ (width - 2))
    (same : encode width behind state = encode width ahead other_state) :
    state = other_state ∧ behind = ahead := by
  obtain ⟨same_state, same_position⟩ := (encode_eq_iff width_two state_below other_below).1 same
  exact ⟨same_state, (eq_of_mod_eq_of_close ordered close same_position).symm⟩

theorem classify_published {width : Nat} (pos : Nat) :
    classify width (encode width pos 1) pos = .ready := by
  simp [classify]

theorem classify_tombstoned {width : Nat} (width_two : 2 ≤ width) (pos : Nat) :
    classify width (encode width pos 2) pos = .tombstoned := by
  have : encode width pos 2 ≠ encode width pos 1 := fun same =>
    absurd ((encode_eq_iff width_two (by omega) (by omega)).1 same).1 (by omega)
  simp [classify, this]

theorem classify_free {width : Nat} (width_two : 2 ≤ width) (pos : Nat) :
    classify width (encode width pos 0) pos = .notReady := by
  have published : encode width pos 0 ≠ encode width pos 1 := fun same =>
    absurd ((encode_eq_iff width_two (by omega) (by omega)).1 same).1 (by omega)
  have abandoned : encode width pos 0 ≠ encode width pos 2 := fun same =>
    absurd ((encode_eq_iff width_two (by omega) (by omega)).1 same).1 (by omega)
  simp [classify, published, abandoned]

/-- A word written for another position within a quarter of the word space never reads as
`ready` or `tombstoned` at the wanted position. -/
theorem classify_other_position {width : Nat} (width_two : 2 ≤ width)
    {written wanted state : Nat} (state_below : state < 4)
    (apart : written < wanted + 2 ^ (width - 2) ∧ wanted ≤ written ∨
      wanted < written + 2 ^ (width - 2) ∧ written ≤ wanted)
    (different : written ≠ wanted) :
    classify width (encode width written state) wanted = .notReady := by
  have mismatch : ∀ other_state, other_state < 4 →
      encode width written state ≠ encode width wanted other_state := by
    intro other_state other_below same
    rcases apart with ⟨near, le⟩ | ⟨near, le⟩
    · exact different (encode_eq_of_close width_two other_below state_below le near same.symm
        |>.2.symm)
    · exact different (encode_eq_of_close width_two state_below other_below le near same).2
  simp [classify, mismatch 1 (by omega), mismatch 2 (by omega)]

end MpscWord

end Qtz
