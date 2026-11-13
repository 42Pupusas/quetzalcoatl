import Qtz.MpmcSlot
import Qtz.Pigeonhole

/-!
The mpmc ring as a state machine over unwrapped positions
(`src/mpmc/{producer,consumer,batch_abandon}.rs`), for the `compare_exchange` refill
(`Config::REFILL_CAS = true`).

Each slot is a `SlotModel`. This file adds what sits between them: the shared `claim`
cursor, the producers' batches and the consumers' `closed` flag. Every step is atomic and
sequentially consistent. Positions are unwrapped: the model does not cover `claim` passing
`usize::MAX`.

What the model abstracts:

* The consumed watermark. `refill_batch` grants `min(PRODUCER_BATCH, free)` positions and
  `free` is at most the capacity (`freeSlots_reserves_to_watermark_plus_capacity`). The model
  lets a refill take any batch of at most `batchMax` positions that fits in one lap of the
  ring, so nothing proved about it depends on what the watermark says.
* The batch bitmap's width. A batch is at most one lap of the ring, which is all the proof
  uses.
* A refill is a decision and a commit, because `claim` can move in between. A
  `compare_exchange` commits only if `claim` is still what the decision saw; a failed one
  starts over (`retry`, which may also stand for a spurious `compare_exchange_weak` failure).
* `who` is a ghost: the producer that reserved each position.
-/

namespace Qtz

namespace SlotModel

variable {g : Geometry}

def reserveAt (s : SlotModel g) (round : Nat) : SlotModel g :=
  { s with phase := s.setPhase round .reserved }

def beginWriteAt (s : SlotModel g) (round : Nat) : SlotModel g :=
  { s with phase := s.setPhase round .writing }

def publishAt (s : SlotModel g) (round : Nat) : SlotModel g :=
  { s with phase := s.setPhase round .published, readyRound := round, readyState := 1 }

def unwriteAt (s : SlotModel g) (round : Nat) : SlotModel g :=
  { s with phase := s.setPhase round .reserved }

def claimCurrent (s : SlotModel g) : SlotModel g :=
  { s with phase := s.setPhase s.readyRound .claimed, readyState := 2 }

def releaseCurrent (s : SlotModel g) : SlotModel g :=
  { s with phase := s.setPhase s.doneRound .released, doneRound := s.doneRound + 1 }

def abandonAt (s : SlotModel g) (round : Nat) : SlotModel g :=
  ⟨round + 1, round, 2, s.setPhase round .released⟩

/-- Reserves every round of this slot whose position lies in `[start, start + count)`. -/
def reserveRange (s : SlotModel g) (start count : Nat) : SlotModel g :=
  { s with phase := fun round =>
      if start ≤ g.slot + round * g.period ∧ g.slot + round * g.period < start + count
      then .reserved else s.phase round }

end SlotModel

namespace Geometry

/-- The geometry of the slot at `index`, defined for every index so that ring state
needs no proof arguments. Only indices below `period` are ever used. -/
def slotGeom (g : Geometry) (index : Nat) : Geometry :=
  { g with slot := index % g.period, slot_below_period := Nat.mod_lt _ g.period_positive }

theorem slotGeom_slot (g : Geometry) (index : Nat) : (g.slotGeom index).slot = index % g.period :=
  rfl

theorem slotGeom_period (g : Geometry) (index : Nat) : (g.slotGeom index).period = g.period :=
  rfl

theorem slotGeom_wraps (g : Geometry) (index : Nat) : (g.slotGeom index).wraps = g.wraps :=
  rfl

end Geometry

/-- A refill a producer has decided on but not yet committed. `seen` is the `claim` the
decision read, which the `compare_exchange` compares against. -/
inductive Pending where
  | none
  | freeKnown (seen batch : Nat)
  | fallback (seen : Nat)
  deriving DecidableEq

/-- One producer: its batch (`start` plus the bits still unused), the bit it is writing,
and its pending refill. -/
structure Producer where
  start : Nat
  unused : Nat → Bool
  writing : Option Nat
  pending : Pending

def Producer.idle (producer : Producer) : Prop :=
  (∀ bit, producer.unused bit = false) ∧ producer.writing = none

def Producer.fresh : Producer := ⟨0, fun _ => false, none, .none⟩

def Producer.withoutBit (producer : Producer) (bit : Nat) : Producer :=
  { producer with unused := fun other => if other = bit then false else producer.unused other }

def Producer.withBit (producer : Producer) (bit : Nat) : Producer :=
  { producer with unused := fun other => if other = bit then true else producer.unused other }

def Producer.batch (start count : Nat) : Producer :=
  ⟨start, fun bit => decide (bit < count), none, .none⟩

structure Ring (g : Geometry) where
  slots : Nat → SlotModel g
  claim : Nat
  closed : Bool
  producers : Nat → Producer
  who : Nat → Nat

namespace Ring

variable {g : Geometry}

def initial (g : Geometry) : Ring g :=
  ⟨fun _ => SlotModel.initial g, 0, false, fun _ => Producer.fresh, fun _ => 0⟩

/-- The slot at `index`, read under its own geometry. -/
def view (s : Ring g) (index : Nat) : SlotModel (g.slotGeom index) :=
  (s.slots index).retag _

def updateSlot (s : Ring g) (index : Nat)
    (change : SlotModel (g.slotGeom index) → SlotModel (g.slotGeom index)) : Ring g :=
  { s with slots := fun other =>
      if other = index then (change (s.view index)).retag g else s.slots other }

def updateProducer (s : Ring g) (index : Nat) (producer : Producer) : Ring g :=
  { s with producers := fun other => if other = index then producer else s.producers other }

def phaseAt (s : Ring g) (position : Nat) : Phase :=
  (s.view (position % g.period)).phase (position / g.period)

def doneRoundAt (s : Ring g) (position : Nat) : Nat :=
  (s.view (position % g.period)).doneRound

/-- The slot's `done` word is the one `DoneWord::is_free_for(position)` looks for. -/
def doneMatches (s : Ring g) (position : Nat) : Prop :=
  (s.view (position % g.period)).doneWord =
    (g.slotGeom (position % g.period)).roundPosition (position / g.period)

/-- Takes `[claim, claim + count)` for producer `k`. -/
def reserveBatch (s : Ring g) (k count : Nat) : Ring g :=
  { s with
    slots := fun index => ((s.view index).reserveRange s.claim count).retag g
    claim := s.claim + count
    producers := fun other =>
      if other = k then Producer.batch s.claim count else s.producers other
    who := fun position =>
      if s.claim ≤ position ∧ position < s.claim + count then k else s.who position }

def afterClaim (s : Ring g) (index : Nat) : Ring g := s.updateSlot index SlotModel.claimCurrent

def afterRelease (s : Ring g) (index : Nat) : Ring g := s.updateSlot index SlotModel.releaseCurrent

def afterAcquire (s : Ring g) (k bit : Nat) : Ring g :=
  (s.updateSlot (((s.producers k).start + bit) % g.period)
      (fun slot => slot.beginWriteAt (((s.producers k).start + bit) / g.period))).updateProducer
    k { (s.producers k).withoutBit bit with writing := some bit }

def afterPublish (s : Ring g) (k bit : Nat) : Ring g :=
  (s.updateSlot (((s.producers k).start + bit) % g.period)
      (fun slot => slot.publishAt (((s.producers k).start + bit) / g.period))).updateProducer
    k { s.producers k with writing := none }

def afterUnwrite (s : Ring g) (k bit : Nat) : Ring g :=
  (s.updateSlot (((s.producers k).start + bit) % g.period)
      (fun slot => slot.unwriteAt (((s.producers k).start + bit) / g.period))).updateProducer
    k { (s.producers k).withBit bit with writing := none }

def afterAbandon (s : Ring g) (k bit : Nat) : Ring g :=
  (s.updateSlot (((s.producers k).start + bit) % g.period)
      (fun slot => slot.abandonAt (((s.producers k).start + bit) / g.period))).updateProducer
    k ((s.producers k).withoutBit bit)

def afterOrphan (s : Ring g) (k bit : Nat) : Ring g :=
  s.updateProducer k ((s.producers k).withoutBit bit)

/-- One step of the ring with at most `limit` producers and batches of at most `batchMax`. -/
inductive Step (g : Geometry) (limit batchMax : Nat) : Ring g → Ring g → Prop
  /-- The last consumer is dropped. -/
  | close {s : Ring g} : Step g limit batchMax s { s with closed := true }
  /-- `ReadyWord::claim`, by a consumer. -/
  | consumerClaim {s : Ring g} (index : Nat) (position : BitVec g.width) :
      index < g.period →
      decodeReady g.capacityWord (s.view index).readyWord (g.slotGeom index).slotWord =
        .published position →
      Step g limit batchMax s (s.afterClaim index)
  /-- `DoneWord::release`, by the consumer that claimed. -/
  | consumerRelease {s : Ring g} (index : Nat) :
      index < g.period →
      (s.view index).phase (s.view index).doneRound = .claimed →
      Step g limit batchMax s (s.afterRelease index)
  /-- `refill_batch` with the consumers still there: `consumed.free(claim)` is `Some`, and
  the batch is `min(PRODUCER_BATCH, free)`, which is at most one lap. -/
  | decideFreeKnown {s : Ring g} (k batch : Nat) :
      k < limit → s.closed = false → (s.producers k).idle → (s.producers k).pending = .none →
      1 ≤ batch → batch ≤ batchMax → batch ≤ g.period →
      Step g limit batchMax s
        (s.updateProducer k { s.producers k with pending := .freeKnown s.claim batch })
  /-- `refill_batch`: no estimate, and `claim`'s slot is free for it. -/
  | decideFallback {s : Ring g} (k : Nat) :
      k < limit → (s.producers k).idle → (s.producers k).pending = .none →
      s.doneMatches s.claim →
      Step g limit batchMax s
        (s.updateProducer k { s.producers k with pending := .fallback s.claim })
  /-- A lost `compare_exchange`: the refill starts over. -/
  | retry {s : Ring g} (k : Nat) :
      k < limit →
      Step g limit batchMax s (s.updateProducer k { s.producers k with pending := .none })
  | commitFreeKnown {s : Ring g} (k seen batch : Nat) :
      k < limit → (s.producers k).pending = .freeKnown seen batch → seen = s.claim →
      Step g limit batchMax s (s.reserveBatch k batch)
  | commitFallback {s : Ring g} (k seen : Nat) :
      k < limit → (s.producers k).pending = .fallback seen → seen = s.claim →
      Step g limit batchMax s (s.reserveBatch k 1)
  /-- `acquire_slot` finds a batch position whose `done` is free for it. -/
  | acquire {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).unused bit = true → (s.producers k).writing = none →
      s.doneMatches ((s.producers k).start + bit) →
      Step g limit batchMax s (s.afterAcquire k bit)
  | publish {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).writing = some bit →
      Step g limit batchMax s (s.afterPublish k bit)
  /-- A `SlotWriter` or `WrittenSlot` dropped without committing returns its bit. -/
  | unwrite {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).writing = some bit →
      Step g limit batchMax s (s.afterUnwrite k bit)
  /-- `BatchAbandon::release` finds `done` free for the position and gives it up. -/
  | abandonOne {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).unused bit = true →
      s.doneMatches ((s.producers k).start + bit) →
      Step g limit batchMax s (s.afterAbandon k bit)
  /-- `BatchAbandon::release` finds the consumers gone and returns with the position
  still reserved. -/
  | orphanOne {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).unused bit = true → s.closed = true →
      Step g limit batchMax s (s.afterOrphan k bit)

inductive Reachable (limit batchMax : Nat) : Ring g → Prop
  | initial : Reachable limit batchMax (Ring.initial g)
  | step {s t : Ring g} : Reachable limit batchMax s → Step g limit batchMax s t →
      Reachable limit batchMax t

end Ring

end Qtz
