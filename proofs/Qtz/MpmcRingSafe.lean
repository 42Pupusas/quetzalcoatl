import Qtz.MpmcRingProduce

/-!
The ring is safe: every reachable state satisfies `Inv`, so every slot keeps the slot
invariant (no `reserve` ever steps outside the `done` window) and a producer whose `done`
check passes writes at the round its slot was released for.

The only condition is `limit + 2 ≤ wraps`: the number of producers plus two fits in the
number of rounds a `done` word tells apart.
-/

namespace Qtz

namespace Ring

variable {g : Geometry} {limit : Nat} {s t : Ring g}

theorem Inv.step (inv : Inv limit s) (wraps_fit : limit + 2 ≤ g.wraps)
    (step : Step g limit s t) : Inv limit t := by
  cases step with
  | close => exact inv.close
  | consumerClaim index position below decoded => exact inv.consumerClaim below decoded
  | consumerRelease index below claimed => exact inv.consumerRelease below claimed
  | decideFreeKnown k batch k_below open_ring idle _ one fits =>
    exact inv.decideFreeKnown open_ring idle one fits
  | decideFallback k k_below idle _ done_matches =>
    exact inv.decideFallback wraps_fit idle done_matches
  | retry k _ => exact inv.retry k
  | commitFreeKnown k batch k_below pending => exact inv.commitFreeKnown wraps_fit k_below pending
  | commitFallback k seen k_below pending at_claim =>
    exact inv.commitFallback wraps_fit k_below pending at_claim
  | acquire k bit _ unused not_writing done_matches =>
    exact inv.acquire unused not_writing done_matches
  | publish k bit _ writing => exact inv.publish writing
  | unwrite k bit _ writing => exact inv.unwrite writing
  | abandonOne k bit _ unused done_matches => exact inv.abandonOne unused done_matches
  | orphanOne k bit _ unused closed => exact inv.orphanOne unused closed

theorem Reachable.inv (wraps_fit : limit + 2 ≤ g.wraps) (reachable : Reachable limit s) :
    Inv limit s := by
  induction reachable with
  | initial => exact Inv.initial limit
  | step _ step inv => exact inv.step wraps_fit step

/-- Every slot of a reachable ring satisfies the slot invariant, in particular
`within_window`: no reserved round is `wraps` or more above the slot's `done` round. -/
theorem Reachable.slot_inv (wraps_fit : limit + 2 ≤ g.wraps) (reachable : Reachable limit s)
    {index : Nat} (below : index < g.period) : SlotModel.Inv (s.view index) :=
  (reachable.inv wraps_fit).slot_inv index below

/-- A producer whose `done` check passes for a position it holds writes at that position's
round, which is the round the slot was released for. -/
theorem Reachable.write_round (wraps_fit : limit + 2 ≤ g.wraps) (reachable : Reachable limit s)
    {position : Nat} (reserved : s.phaseAt position = .reserved)
    (done_matches : s.doneMatches position) :
    s.doneRoundAt position = position / g.period :=
  ((reachable.slot_inv wraps_fit (Nat.mod_lt _ g.period_positive)).done_word_round reserved
    done_matches).symm

end Ring

end Qtz
