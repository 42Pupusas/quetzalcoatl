import Qtz.ConsumedWatermark

/-!
Model of the full-ring test in `mpsc::Producer::claim_slot` (`src/mpsc/producer.rs`).

A producer claims the tail position when `tail.wrapping_sub(cached_head) < capacity`, where
`cached_head` is a head it read earlier. Positions here are unwrapped numbers: `tail` is the
claim cursor, `head` the consumer's current head, `cached` the producer's cached head, with
`cached ≤ head ≤ tail` and a ring that holds at most `capacity` positions, `tail - head ≤
capacity`. The Rust compares the words reduced to `width` bits.
-/

namespace Qtz

def wrappedRoom {width : Nat} (tail cached capacity : Nat) : Bool :=
  (BitVec.ofNat width tail - BitVec.ofNat width cached).ult (BitVec.ofNat width capacity)

theorem wrappedRoom_iff {width tail cached capacity : Nat} (cached_le_tail : cached ≤ tail)
    (capacity_fits : capacity < 2 ^ width) :
    wrappedRoom (width := width) tail cached capacity = true ↔
      (tail - cached) % 2 ^ width < capacity := by
  obtain ⟨distance, rfl⟩ := Nat.exists_eq_add_of_le cached_le_tail
  unfold wrappedRoom
  rw [BitVec.ofNat_add, BitVec.add_comm, BitVec.add_sub_cancel]
  simp only [BitVec.ult, decide_eq_true_eq, BitVec.toNat_ofNat]
  rw [Nat.mod_eq_of_lt capacity_fits]
  have : cached + distance - cached = distance := by omega
  rw [this]

/-- A claim made on a cached head is sound while the cache is less than a full word lap
behind the tail: the head, which is at least as far on, then leaves room too. -/
theorem claim_room_of_cached {width tail head cached capacity : Nat} (cached_le_head : cached ≤ head)
    (head_le_tail : head ≤ tail) (capacity_fits : capacity < 2 ^ width)
    (cache_within_lap : tail - cached < 2 ^ width)
    (room : wrappedRoom (width := width) tail cached capacity = true) :
    tail - head < capacity := by
  rw [wrappedRoom_iff (by omega) capacity_fits, Nat.mod_eq_of_lt cache_within_lap] at room
  omega

/-- A claim made on a head read just now is sound whenever the ring holds at most
`capacity` positions. -/
theorem claim_room_of_fresh {width tail head capacity : Nat} (head_le_tail : head ≤ tail)
    (capacity_fits : capacity < 2 ^ width) (ring_holds_capacity : tail - head ≤ capacity)
    (room : wrappedRoom (width := width) tail head capacity = true) :
    tail - head < capacity :=
  claim_room_of_cached (Nat.le_refl _) head_le_tail capacity_fits (by omega) room

/-- Without that bound the test is unsound: a producer whose cache is a word lap and two
positions behind sees room in a full ring. Four-bit words, capacity four, cached head 0,
true head 14, tail 18. -/
theorem stale_cache_sees_room_in_a_full_ring :
    wrappedRoom (width := 4) 18 0 4 = true ∧ 18 - 14 = 4 := by
  decide

end Qtz
