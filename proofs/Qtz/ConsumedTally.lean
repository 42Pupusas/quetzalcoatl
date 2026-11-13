import Qtz.ConsumedWatermark

/-!
The shared watermark and the consumers' private tallies (`src/mpmc/consumed_watermark.rs`).

Every pop is counted once, either in the watermark or in exactly one consumer's pending
count, so the watermark never exceeds the number of pops. The numbers here are unwrapped;
the real watermark is the same number reduced to the word width.
-/

namespace Qtz

structure Tallies where
  watermark : Nat
  pending : Nat → Nat

namespace Tallies

/-- Pending pops of the consumers numbered below `count`. -/
def pendingBelow (tallies : Tallies) : Nat → Nat
  | 0 => 0
  | count + 1 => tallies.pendingBelow count + tallies.pending count

/-- Pops accounted for: published, plus pending in the first `count` consumers. -/
def accounted (tallies : Tallies) (count : Nat) : Nat :=
  tallies.watermark + tallies.pendingBelow count

def initial : Tallies := { watermark := 0, pending := fun _ => 0 }

theorem pendingBelow_initial : ∀ count, initial.pendingBelow count = 0
  | 0 => rfl
  | count + 1 => by
    unfold pendingBelow
    rw [pendingBelow_initial count]
    rfl

/-- One consumer's `(watermark, pending)` pair replaced. -/
def withConsumer (tallies : Tallies) (consumer : Nat) (state : Nat × Nat) : Tallies :=
  { watermark := state.1
    pending := fun other => if other = consumer then state.2 else tallies.pending other }

/-- `ConsumedTally::record` by consumer number `consumer`. -/
def record (tallies : Tallies) (flush_at consumer : Nat) : Tallies :=
  tallies.withConsumer consumer (recordPop flush_at tallies.watermark (tallies.pending consumer))

/-- `ConsumedTally::flush` by consumer number `consumer`. -/
def flush (tallies : Tallies) (consumer : Nat) : Tallies :=
  tallies.withConsumer consumer (flushPending tallies.watermark (tallies.pending consumer))

theorem pendingBelow_withConsumer (tallies : Tallies) (consumer : Nat) (state : Nat × Nat) :
    ∀ count, consumer < count →
      (tallies.withConsumer consumer state).pendingBelow count + tallies.pending consumer =
        tallies.pendingBelow count + state.2
  | 0, below => absurd below (Nat.not_lt_zero _)
  | count + 1, below => by
    unfold pendingBelow
    by_cases same : consumer = count
    · subst same
      have rest : ∀ n, n ≤ consumer →
          (tallies.withConsumer consumer state).pendingBelow n = tallies.pendingBelow n := by
        intro n
        induction n with
        | zero => intro _; rfl
        | succ n ih =>
          intro fits
          unfold pendingBelow
          rw [ih (by omega)]
          have : n ≠ consumer := by omega
          simp [withConsumer, this]
      rw [rest consumer (Nat.le_refl _)]
      simp [withConsumer]
      omega
    · have inner := pendingBelow_withConsumer tallies consumer state count (by omega)
      have other : (tallies.withConsumer consumer state).pending count =
          tallies.pending count := by
        simp [withConsumer, Ne.symm same]
      rw [other]
      omega

theorem accounted_record (tallies : Tallies) (flush_at consumer count : Nat)
    (member : consumer < count) :
    (tallies.record flush_at consumer).accounted count = tallies.accounted count + 1 := by
  have moved := pendingBelow_withConsumer tallies consumer
    (recordPop flush_at tallies.watermark (tallies.pending consumer)) count member
  have counted := recordPop_counts_one_pop flush_at tallies.watermark (tallies.pending consumer)
  unfold accounted record
  simp only [withConsumer] at moved ⊢
  omega

theorem accounted_flush (tallies : Tallies) (consumer count : Nat) (member : consumer < count) :
    (tallies.flush consumer).accounted count = tallies.accounted count := by
  have moved := pendingBelow_withConsumer tallies consumer
    (flushPending tallies.watermark (tallies.pending consumer)) count member
  have counted := flushPending_preserves_total tallies.watermark (tallies.pending consumer)
  unfold accounted flush
  simp only [withConsumer] at moved ⊢
  omega

theorem watermark_le_accounted (tallies : Tallies) (count : Nat) :
    tallies.watermark ≤ tallies.accounted count := by
  unfold accounted
  omega

/-- A tally run: `pops` recorded pops and any number of flushes leave the watermark at or
below `pops`, because the pops are the accounted total. -/
inductive Run (count : Nat) (flush_at : Nat) : Tallies → Nat → Prop
  | initial : Run count flush_at initial 0
  | record {tallies pops} (consumer : Nat) (member : consumer < count)
      (run : Run count flush_at tallies pops) :
      Run count flush_at (tallies.record flush_at consumer) (pops + 1)
  | flush {tallies pops} (consumer : Nat) (member : consumer < count)
      (run : Run count flush_at tallies pops) :
      Run count flush_at (tallies.flush consumer) pops

theorem Run.accounted_eq {count flush_at : Nat} {tallies : Tallies} {pops : Nat}
    (run : Run count flush_at tallies pops) : tallies.accounted count = pops := by
  induction run with
  | initial =>
    unfold accounted
    rw [pendingBelow_initial]
    rfl
  | record consumer member _ ih => rw [accounted_record _ _ _ _ member, ih]
  | flush consumer member _ ih => rw [accounted_flush _ _ _ member, ih]

theorem Run.watermark_le_pops {count flush_at : Nat} {tallies : Tallies} {pops : Nat}
    (run : Run count flush_at tallies pops) : tallies.watermark ≤ pops :=
  run.accounted_eq ▸ tallies.watermark_le_accounted count

/-- With every consumer flushed, the watermark is exactly the number of pops. -/
theorem Run.watermark_eq_pops_of_flushed {count flush_at : Nat} {tallies : Tallies}
    {pops : Nat} (run : Run count flush_at tallies pops)
    (flushed : ∀ consumer, consumer < count → tallies.pending consumer = 0) :
    tallies.watermark = pops := by
  have : tallies.pendingBelow count = 0 := by
    have all : ∀ n, n ≤ count → tallies.pendingBelow n = 0 := by
      intro n
      induction n with
      | zero => intro _; rfl
      | succ n ih =>
        intro fits
        unfold pendingBelow
        rw [ih (by omega), flushed n (by omega)]
    exact all count (Nat.le_refl _)
  have := run.accounted_eq
  unfold accounted at this
  omega

end Tallies

end Qtz
