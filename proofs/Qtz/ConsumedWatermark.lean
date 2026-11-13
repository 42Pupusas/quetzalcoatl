import Qtz.ConsumerFloor

/-!
Model of `ConsumedWatermark::free` and `ConsumedTally`
(`src/mpmc/consumed_watermark.rs`).

Positions are words that wrap. The claim cursor is written as the
watermark plus the distance `in_flight_distance` it has advanced past
it, an unwrapped number that must fit in the word.
-/

namespace Qtz

def freeSlots {width : Nat} (claim watermark capacity : BitVec width) : Option (BitVec width) :=
  if (claim - watermark).ult capacity then some (capacity - (claim - watermark)) else none

theorem claim_minus_watermark_toNat {width : Nat} (watermark : BitVec width)
    (in_flight_distance : Nat) (distance_fits : in_flight_distance < 2 ^ width) :
    (watermark + BitVec.ofNat width in_flight_distance - watermark).toNat = in_flight_distance := by
  rw [BitVec.add_comm]
  rw [BitVec.add_sub_cancel, BitVec.toNat_ofNat]
  exact Nat.mod_eq_of_lt distance_fits

theorem freeSlots_none_of_full {width : Nat} (watermark capacity : BitVec width)
    (in_flight_distance : Nat) (distance_fits : in_flight_distance < 2 ^ width)
    (ring_accounted_for : capacity.toNat ≤ in_flight_distance) :
    freeSlots (watermark + BitVec.ofNat width in_flight_distance) watermark capacity = none := by
  have distance := claim_minus_watermark_toNat watermark in_flight_distance distance_fits
  unfold freeSlots
  have not_below : ¬ (watermark + BitVec.ofNat width in_flight_distance - watermark).ult capacity =
      true := by
    simp only [BitVec.ult, decide_eq_true_eq, distance]
    omega
  simp [not_below]

theorem freeSlots_some_of_room {width : Nat} (watermark capacity : BitVec width)
    (in_flight_distance : Nat) (distance_fits : in_flight_distance < 2 ^ width)
    (room_left : in_flight_distance < capacity.toNat) :
    ∃ slots, freeSlots (watermark + BitVec.ofNat width in_flight_distance) watermark capacity =
      some slots := by
  have distance := claim_minus_watermark_toNat watermark in_flight_distance distance_fits
  unfold freeSlots
  have below : (watermark + BitVec.ofNat width in_flight_distance - watermark).ult capacity =
      true := by
    simp only [BitVec.ult, decide_eq_true_eq, distance]
    exact room_left
  simp [below]

/-- The claim cursor plus the granted `slots` reaches exactly
`watermark + capacity`, and at least one slot is granted. -/
theorem freeSlots_reserves_to_watermark_plus_capacity {width : Nat}
    (watermark capacity : BitVec width) (in_flight_distance : Nat)
    (distance_fits : in_flight_distance < 2 ^ width) (slots : BitVec width)
    (granted : freeSlots (watermark + BitVec.ofNat width in_flight_distance) watermark capacity =
      some slots) :
    in_flight_distance + slots.toNat = capacity.toNat ∧ 0 < slots.toNat := by
  have distance := claim_minus_watermark_toNat watermark in_flight_distance distance_fits
  have capacity_bound := capacity.isLt
  unfold freeSlots at granted
  split at granted
  · rename_i below
    simp only [BitVec.ult, decide_eq_true_eq, distance] at below
    have slots_value : slots = capacity - (watermark + BitVec.ofNat width in_flight_distance
        - watermark) := (Option.some.inj granted).symm
    rw [slots_value, wrapped_sub_toNat, distance]
    split <;> omega
  · exact absurd granted (by simp)

/-- The tally step for one pop: the pending count reaches `flush_at` and is
published to the watermark, or stays pending. -/
def recordPop (flush_at watermark pending : Nat) : Nat × Nat :=
  if flush_at ≤ pending + 1 then (watermark + (pending + 1), 0) else (watermark, pending + 1)

def flushPending (watermark pending : Nat) : Nat × Nat :=
  if 0 < pending then (watermark + pending, 0) else (watermark, pending)

theorem recordPop_counts_one_pop (flush_at watermark pending : Nat) :
    (recordPop flush_at watermark pending).1 + (recordPop flush_at watermark pending).2 =
      watermark + pending + 1 := by
  unfold recordPop
  split <;> simp <;> omega

theorem recordPop_pending_below_flush (flush_at watermark pending : Nat)
    (flush_positive : 0 < flush_at) :
    (recordPop flush_at watermark pending).2 < flush_at := by
  unfold recordPop
  split <;> simp <;> omega

theorem recordPop_watermark_monotone (flush_at watermark pending : Nat) :
    watermark ≤ (recordPop flush_at watermark pending).1 := by
  unfold recordPop
  split <;> simp

theorem flushPending_preserves_total (watermark pending : Nat) :
    (flushPending watermark pending).1 + (flushPending watermark pending).2 =
      watermark + pending := by
  unfold flushPending
  split <;> simp

theorem flushPending_leaves_nothing_pending (watermark pending : Nat) :
    (flushPending watermark pending).2 = 0 := by
  unfold flushPending
  split <;> simp <;> omega

end Qtz
