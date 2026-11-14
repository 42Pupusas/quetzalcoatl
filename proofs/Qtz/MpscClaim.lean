import Qtz.ConsumedWatermark

/-!
Model of the full-ring test `mpsc::Producer::claim_slot` used before the claim ledger
(`src/mpsc/claim_ledger.rs`), kept for the two theorems that show why it was replaced:
`stale_cache_sees_room_in_a_full_ring` and `no_local_test_is_sound`. Nothing in the ring model
depends on this file.

A producer claimed the tail position when `tail.wrapping_sub(cached_head) < capacity`, where
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

/-- The test is unsound at every width: a producer whose cached head is a
full word lap behind the tail sees room, however full the ring is. The ring here is exactly
full (`tail - head = capacity`) and the cache is no later than the head. -/
theorem stale_cache_sees_room_in_a_full_ring {width capacity : Nat} (capacity_positive : 0 < capacity)
    (capacity_fits : capacity < 2 ^ width) (cached : Nat) :
    wrappedRoom (width := width) (cached + 2 ^ width) cached capacity = true ∧
      cached ≤ cached + 2 ^ width - capacity := by
  refine ⟨?_, by omega⟩
  rw [wrappedRoom_iff (by omega) capacity_fits]
  have : cached + 2 ^ width - cached = 2 ^ width := by omega
  rw [this, Nat.mod_self]
  exact capacity_positive

/-- No claim test that reads only the tail word and a producer-local snapshot can be sound.
`test` receives the tail reduced to `width` bits and whatever the producer stored when it last
looked (a head, a lap count, any other value). If it ever says room, it says room again once the
tail is a whole lap further on and the ring is exactly full, because the producer saw nothing
in between. A lap counter kept in the producer is part of `snapshot`, so it changes nothing.
Lap bits stored in the tail word just make `width` larger, and the same lag defeats them one
lap later. Only a wider tail, or a read of other shared state on every claim, can be sound. -/
theorem no_local_test_is_sound {width capacity : Nat} {Snapshot : Type}
    (capacity_fits : capacity < 2 ^ width) (test : Nat → Snapshot → Bool)
    {tail cached : Nat} {snapshot : Snapshot} (cached_le_tail : cached ≤ tail)
    (says_room : test (tail % 2 ^ width) snapshot = true) :
    ∃ later_tail head, head ≤ later_tail ∧ later_tail - head = capacity ∧ cached ≤ head ∧
      test (later_tail % 2 ^ width) snapshot = true := by
  refine ⟨tail + 2 ^ width, tail + 2 ^ width - capacity, by omega, by omega, by omega, ?_⟩
  rw [Nat.add_mod_right]
  exact says_room

end Qtz
