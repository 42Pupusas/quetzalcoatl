/-!
Model of the claim word in `src/mpsc/claim_ledger.rs`.

The word packs three fields. From the top: the next position to claim, reduced to
`positionBits` bits; one bit that is set while a producer moves credits in; and the credits
ready to claim, in `creditBits` bits. The Rust has `positionBits = 32` and `creditBits = 31`.

`layout` is the word for given field values. Each operation the Rust performs on the word is
defined here on the raw number, as the Rust computes it, and shown to act on the fields.
-/

namespace Qtz

/-- A field above `lowSpan` wraps on its own: reducing the whole word reduces only it. -/
theorem field_wrap {fieldSpan lowSpan high low : Nat} (low_lt : low < lowSpan)
    (fieldSpan_pos : 0 < fieldSpan) :
    (high * lowSpan + low) % (fieldSpan * lowSpan) = high % fieldSpan * lowSpan + low := by
  have split : high * lowSpan + low
      = high % fieldSpan * lowSpan + low + fieldSpan * lowSpan * (high / fieldSpan) := by
    have parts : (high % fieldSpan + fieldSpan * (high / fieldSpan)) * lowSpan
        = high % fieldSpan * lowSpan + fieldSpan * lowSpan * (high / fieldSpan) := by
      rw [Nat.add_mul, Nat.mul_right_comm]
    rw [Nat.mod_add_div] at parts
    omega
  rw [split, Nat.add_mul_mod_self_left]
  apply Nat.mod_eq_of_lt
  have high_lt : high % fieldSpan + 1 ≤ fieldSpan := Nat.mod_lt _ fieldSpan_pos
  have bound := Nat.mul_le_mul_right lowSpan high_lt
  rw [Nat.add_mul, Nat.one_mul] at bound
  omega

/-- Masking one bit tests it. -/
theorem and_two_pow_eq_zero_iff (word bit : Nat) :
    word &&& 2 ^ bit = 0 ↔ word / 2 ^ bit % 2 = 0 := by
  have tested : word.testBit bit = decide (word / 2 ^ bit % 2 = 1) :=
    Nat.testBit_eq_decide_div_mod_eq
  constructor
  · intro masked
    have := congrArg (fun value => value.testBit bit) masked
    simp [Nat.testBit_and, tested] at this
    omega
  · intro clear
    apply Nat.eq_of_testBit_eq
    intro other
    simp only [Nat.testBit_and, Nat.testBit_two_pow, Nat.zero_testBit]
    by_cases same : bit = other
    · subst same
      simp [tested]
      omega
    · simp [same]

namespace Mpsc

namespace ClaimWord

def layout (positionBits creditBits position : Nat) (refilling : Bool) (credits : Nat) : Nat :=
  position % 2 ^ positionBits * 2 ^ (creditBits + 1) + refilling.toNat * 2 ^ creditBits + credits

/-- `credits_of`: `word & CREDIT_MASK`. -/
def creditsOf (creditBits word : Nat) : Nat := word &&& (2 ^ creditBits - 1)

/-- `position_of`: `word >> POSITION_SHIFT`. -/
def positionOf (creditBits word : Nat) : Nat := word >>> (creditBits + 1)

/-- `word & REFILLING`. -/
def refillBit (creditBits word : Nat) : Nat := word &&& 2 ^ creditBits

/-- `word.wrapping_add(CLAIM)`: one position on, one credit fewer. -/
def claim (positionBits creditBits word : Nat) : Nat :=
  (word + (2 ^ (creditBits + 1) - 1)) % 2 ^ (positionBits + (creditBits + 1))

/-- `word | REFILLING`. -/
def mark (creditBits word : Nat) : Nat := word ||| 2 ^ creditBits

/-- `pack`: the shift drops the bits of `position` above the position field. -/
def pack (positionBits creditBits position credits : Nat) : Nat :=
  position <<< (creditBits + 1) % 2 ^ (positionBits + (creditBits + 1)) ||| credits

variable {positionBits creditBits position credits : Nat} {refilling : Bool}

theorem layout_above_credits :
    layout positionBits creditBits position refilling credits
      = (position % 2 ^ positionBits * 2 + refilling.toNat) * 2 ^ creditBits + credits := by
  unfold layout
  rw [Nat.pow_succ, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm 2 (2 ^ creditBits)]

theorem creditsOf_layout (credits_fit : credits < 2 ^ creditBits) :
    creditsOf creditBits (layout positionBits creditBits position refilling credits) = credits := by
  unfold creditsOf
  rw [Nat.and_two_pow_sub_one_eq_mod, layout_above_credits, Nat.add_comm,
    Nat.add_mul_mod_self_right]
  exact Nat.mod_eq_of_lt credits_fit

theorem refillBit_layout (credits_fit : credits < 2 ^ creditBits) :
    refillBit creditBits (layout positionBits creditBits position refilling credits) = 0
      ↔ refilling = false := by
  unfold refillBit
  rw [and_two_pow_eq_zero_iff, layout_above_credits, Nat.add_comm,
    Nat.add_mul_div_right _ _ (Nat.two_pow_pos _), Nat.div_eq_of_lt credits_fit, Nat.zero_add]
  cases refilling <;> simp <;> omega

theorem positionOf_layout (credits_fit : credits < 2 ^ creditBits) :
    positionOf creditBits (layout positionBits creditBits position refilling credits)
      = position % 2 ^ positionBits := by
  have low_fits : refilling.toNat * 2 ^ creditBits + credits < 2 ^ (creditBits + 1) := by
    rw [Nat.pow_succ]
    cases refilling <;> simp <;> omega
  unfold positionOf layout
  rw [Nat.shiftRight_eq_div_pow, Nat.add_assoc, Nat.add_comm,
    Nat.add_mul_div_right _ _ (Nat.two_pow_pos _), Nat.div_eq_of_lt low_fits, Nat.zero_add]

/-- A claim moves the position on by one and takes one credit. The position field wraps on its
own, and the credit field does not borrow from the bit above it because it held a credit. -/
theorem claim_layout (credits_fit : credits < 2 ^ creditBits) (has_credit : 0 < credits) :
    claim positionBits creditBits (layout positionBits creditBits position false credits)
      = layout positionBits creditBits (position + 1) false (credits - 1) := by
  have span_le : 2 ^ creditBits ≤ 2 ^ (creditBits + 1) := by rw [Nat.pow_succ]; omega
  have moved : layout positionBits creditBits position false credits + (2 ^ (creditBits + 1) - 1)
      = (position % 2 ^ positionBits + 1) * 2 ^ (creditBits + 1) + (credits - 1) := by
    unfold layout
    rw [Nat.add_mul, Nat.one_mul]
    simp only [Bool.toNat_false, Nat.zero_mul, Nat.add_zero]
    omega
  unfold claim
  rw [moved, Nat.pow_add 2 positionBits, field_wrap (by omega) (Nat.two_pow_pos _),
    Nat.mod_add_mod]
  simp [layout]

theorem mark_layout :
    mark creditBits (layout positionBits creditBits position false 0)
      = layout positionBits creditBits position true 0 := by
  have bit_fits : 2 ^ creditBits < 2 ^ (creditBits + 1) := by
    have := Nat.two_pow_pos creditBits
    rw [Nat.pow_succ]; omega
  unfold mark layout
  simp only [Bool.toNat_false, Bool.toNat_true, Nat.zero_mul, Nat.one_mul, Nat.add_zero]
  rw [Nat.mul_comm, Nat.two_pow_add_eq_or_of_lt bit_fits]

theorem pack_layout (credits_fit : credits < 2 ^ creditBits) :
    pack positionBits creditBits position credits
      = layout positionBits creditBits position false credits := by
  have low_fits : credits < 2 ^ (creditBits + 1) := by rw [Nat.pow_succ]; omega
  unfold pack layout
  simp only [Bool.toNat_false, Nat.zero_mul, Nat.add_zero]
  rw [Nat.shiftLeft_eq, Nat.pow_add 2 positionBits, Nat.mul_mod_mul_right, Nat.mul_comm,
    Nat.two_pow_add_eq_or_of_lt low_fits]

/-- The word depends on the position only through its low `positionBits` bits. -/
theorem layout_congr {other : Nat}
    (same : position % 2 ^ positionBits = other % 2 ^ positionBits) :
    layout positionBits creditBits position refilling credits
      = layout positionBits creditBits other refilling credits := by
  unfold layout
  rw [same]

end ClaimWord

end Mpsc

end Qtz
