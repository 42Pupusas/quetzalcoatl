/-!
Model of `ConsumerFloor::permits` and `permits_from_stale`
(`src/broadcast/consumer_floor.rs`) for a floor at `consumer_head`.

`Capacity` does not rule out `2 ^ (width - 1)`, so the capacity bound
`2 * capacity < 2 ^ width` is a hypothesis, not a fact.
-/

namespace Qtz

def permits {width : Nat} (consumer_head position capacity : BitVec width) : Bool :=
  (position - consumer_head).slt capacity

def permitsStale {width : Nat} (consumer_head position capacity : BitVec width) : Bool :=
  (position - consumer_head).ult capacity

theorem wrapped_sub_toNat {width : Nat} (minuend subtrahend : BitVec width) :
    (minuend - subtrahend).toNat =
      if subtrahend.toNat ≤ minuend.toNat then minuend.toNat - subtrahend.toNat
      else minuend.toNat + 2 ^ width - subtrahend.toNat := by
  have minuend_bound := minuend.isLt
  have subtrahend_bound := subtrahend.isLt
  rw [BitVec.toNat_sub]
  split
  · rw [show 2 ^ width - subtrahend.toNat + minuend.toNat
        = (minuend.toNat - subtrahend.toNat) + 2 ^ width by omega, Nat.add_mod_right]
    exact Nat.mod_eq_of_lt (by omega)
  · exact (Nat.mod_eq_of_lt (by omega)).trans (by omega)

theorem slt_iff_toNat_lt {width : Nat} (left right : BitVec width)
    (left_in_lower_half : 2 * left.toNat < 2 ^ width)
    (right_in_lower_half : 2 * right.toNat < 2 ^ width) :
    left.slt right = true ↔ left.toNat < right.toNat := by
  rw [BitVec.slt_iff_toInt_lt, BitVec.toInt_eq_toNat_cond left, BitVec.toInt_eq_toNat_cond right]
  simp only [left_in_lower_half, right_in_lower_half, ite_true]
  omega

theorem permits_head_behind_position {width : Nat}
    (consumer_head distance_behind capacity : BitVec width)
    (distance_in_lower_half : 2 * distance_behind.toNat < 2 ^ width)
    (capacity_in_lower_half : 2 * capacity.toNat < 2 ^ width) :
    permits consumer_head (consumer_head + distance_behind) capacity
      = decide (distance_behind.toNat < capacity.toNat) := by
  have distance_recovered : consumer_head + distance_behind - consumer_head = distance_behind := by
    rw [BitVec.add_comm]; exact BitVec.add_sub_cancel distance_behind consumer_head
  unfold permits
  rw [distance_recovered, Bool.eq_iff_iff,
    slt_iff_toNat_lt distance_behind capacity distance_in_lower_half capacity_in_lower_half]
  simp

theorem permits_head_ahead_of_position {width : Nat}
    (consumer_head distance_ahead capacity : BitVec width)
    (distance_nonzero : 0 < distance_ahead.toNat)
    (distance_in_lower_half : 2 * distance_ahead.toNat < 2 ^ width)
    (capacity_nonzero : 0 < capacity.toNat)
    (capacity_in_lower_half : 2 * capacity.toNat < 2 ^ width) :
    permits consumer_head (consumer_head - distance_ahead) capacity = true := by
  have distance_below_modulus := distance_ahead.isLt
  have position_minus_head : consumer_head - distance_ahead - consumer_head = -distance_ahead := by
    rw [BitVec.sub_eq_add_neg consumer_head distance_ahead,
      BitVec.add_comm consumer_head (-distance_ahead), BitVec.add_sub_cancel]
  have negated_toNat : (-distance_ahead).toNat = 2 ^ width - distance_ahead.toNat := by
    rw [BitVec.toNat_neg]; exact Nat.mod_eq_of_lt (by omega)
  have negated_in_upper_half : ¬ 2 * (2 ^ width - distance_ahead.toNat) < 2 ^ width := by omega
  unfold permits
  rw [position_minus_head, BitVec.slt_iff_toInt_lt, BitVec.toInt_eq_toNat_cond capacity,
    BitVec.toInt_eq_toNat_cond (-distance_ahead), negated_toNat]
  simp only [negated_in_upper_half, capacity_in_lower_half, ite_true, ite_false]
  omega

theorem stale_head_permission_is_sound {width : Nat}
    (stale_head lag position capacity : BitVec width)
    (capacity_in_lower_half : 2 * capacity.toNat < 2 ^ width)
    (lag_within_distance : lag.toNat ≤ (position - stale_head).toNat)
    (stale_permits : permitsStale stale_head position capacity = true) :
    permits (stale_head + lag) position capacity = true := by
  have distance_below_modulus := (position - stale_head).isLt
  have distance_from_true_head :
      position - (stale_head + lag) = (position - stale_head) - lag :=
    (BitVec.sub_sub position stale_head lag).symm
  have distance_under_capacity : (position - stale_head).toNat < capacity.toNat := by
    simpa [permitsStale, BitVec.ult] using stale_permits
  have reduced_distance :
      ((position - stale_head) - lag).toNat = (position - stale_head).toNat - lag.toNat := by
    simp only [wrapped_sub_toNat (position - stale_head) lag, lag_within_distance, ite_true]
  unfold permits
  rw [distance_from_true_head, slt_iff_toNat_lt _ _ (by omega) capacity_in_lower_half]
  omega

end Qtz
