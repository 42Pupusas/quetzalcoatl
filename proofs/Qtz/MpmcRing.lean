import Qtz.MpmcSlot
import Qtz.Pigeonhole

/-!
The mpmc ring as a state machine over unwrapped positions
(`src/mpmc/{producer,consumer,batch_abandon,consumed_watermark}.rs`), for the
`compare_exchange` refill (`Config::REFILL_CAS = true`).

Each slot is a `SlotModel`. This file adds what sits between them: the shared `claim`
cursor, the consumed watermark, the producers' batches and the consumers' `closed`
flag. Every step is atomic and sequentially consistent. Positions are unwrapped: the
model does not cover `claim` or the watermark passing `usize::MAX` (`freeSlots` in
`ConsumedWatermark.lean` covers the wrapping subtraction).

What the model abstracts:

* The watermark is a number that only grows and never passes `consumed`, the count of
  consumer claims. Each consumer's private tally, and when it flushes, are left open:
  `flush` moves the watermark to any value in that range.
* The batch bitmap's width. A batch is at most `capacity` positions, which is all the
  proof uses.
* A refill is a decision and a commit, because `claim` can move in between. A
  `compare_exchange` commits only if `claim` is still what the decision saw; a failed
  one starts over (`retry`, which may also stand for a spurious `compare_exchange_weak`
  failure).
* `who` and `claimLog` are ghosts: the producer that reserved each position, and the
  position of each consumer claim in order.
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
  watermark : Nat
  consumed : Nat
  closed : Bool
  producers : Nat → Producer
  who : Nat → Nat
  claimLog : Nat → Nat

namespace Ring

variable {g : Geometry}

def initial (g : Geometry) : Ring g :=
  ⟨fun _ => SlotModel.initial g, 0, 0, 0, false, fun _ => Producer.fresh, fun _ => 0, fun _ => 0⟩

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

/-- `producer` has `position` among its unused batch positions. -/
def Holds (producer : Producer) (position : Nat) : Prop :=
  ∃ bit, producer.unused bit = true ∧ producer.start + bit = position

/-- Takes `[claim, claim + count)` for producer `k`. -/
def reserveBatch (s : Ring g) (k count : Nat) : Ring g :=
  { s with
    slots := fun index => ((s.view index).reserveRange s.claim count).retag g
    claim := s.claim + count
    producers := fun other =>
      if other = k then Producer.batch s.claim count else s.producers other
    who := fun position =>
      if s.claim ≤ position ∧ position < s.claim + count then k else s.who position }

/-- One step of the ring with at most `limit` producers and batches of at most `batchMax`. -/
inductive Step (g : Geometry) (limit batchMax : Nat) : Ring g → Ring g → Prop
  /-- The last consumer is dropped. -/
  | close {s : Ring g} : Step g limit batchMax s { s with closed := true }
  /-- A consumer publishes its tally. -/
  | flush {s : Ring g} (watermark : Nat) :
      s.watermark ≤ watermark → watermark ≤ s.consumed →
      Step g limit batchMax s { s with watermark := watermark }
  /-- `ReadyWord::claim`, by a consumer. -/
  | consumerClaim {s : Ring g} (index : Nat) (position : BitVec g.width) :
      index < g.period →
      decodeReady g.capacityWord (s.view index).readyWord (g.slotGeom index).slotWord =
        .published position →
      Step g limit batchMax s
        { s.updateSlot index SlotModel.claimCurrent with
          consumed := s.consumed + 1
          claimLog := fun n =>
            if n = s.consumed then (s.view index).readyRound * g.period + index
            else s.claimLog n }
  /-- `DoneWord::release`, by the consumer that claimed. -/
  | consumerRelease {s : Ring g} (index : Nat) :
      index < g.period →
      (s.view index).phase (s.view index).doneRound = .claimed →
      Step g limit batchMax s (s.updateSlot index SlotModel.releaseCurrent)
  /-- `refill_batch`: `consumed.free(claim)` is `Some`, and the batch is
  `min(PRODUCER_BATCH, free)`. -/
  | decideFreeKnown {s : Ring g} (k batch : Nat) :
      k < limit → (s.producers k).idle → (s.producers k).pending = .none →
      s.claim < s.watermark + g.period →
      1 ≤ batch → batch ≤ batchMax → s.claim + batch ≤ s.watermark + g.period →
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
      Step g limit batchMax s
        ((s.updateSlot (((s.producers k).start + bit) % g.period)
            (fun slot => slot.beginWriteAt (((s.producers k).start + bit) / g.period))).updateProducer
          k { (s.producers k).withoutBit bit with writing := some bit })
  | publish {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).writing = some bit →
      Step g limit batchMax s
        ((s.updateSlot (((s.producers k).start + bit) % g.period)
            (fun slot => slot.publishAt (((s.producers k).start + bit) / g.period))).updateProducer
          k { s.producers k with writing := none })
  /-- A `SlotWriter` or `WrittenSlot` dropped without committing returns its bit. -/
  | unwrite {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).writing = some bit →
      Step g limit batchMax s
        ((s.updateSlot (((s.producers k).start + bit) % g.period)
            (fun slot => slot.unwriteAt (((s.producers k).start + bit) / g.period))).updateProducer
          k { (s.producers k).withBit bit with writing := none })
  /-- `BatchAbandon::release` finds `done` free for the position and gives it up. -/
  | abandonOne {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).unused bit = true →
      s.doneMatches ((s.producers k).start + bit) →
      Step g limit batchMax s
        ((s.updateSlot (((s.producers k).start + bit) % g.period)
            (fun slot => slot.abandonAt (((s.producers k).start + bit) / g.period))).updateProducer
          k ((s.producers k).withoutBit bit))
  /-- `BatchAbandon::release` finds the consumers gone and returns with the position
  still reserved. -/
  | orphanOne {s : Ring g} (k bit : Nat) :
      k < limit → (s.producers k).unused bit = true → s.closed = true →
      Step g limit batchMax s (s.updateProducer k ((s.producers k).withoutBit bit))

inductive Reachable (limit batchMax : Nat) : Ring g → Prop
  | initial : Reachable limit batchMax (Ring.initial g)
  | step {s t : Ring g} : Reachable limit batchMax s → Step g limit batchMax s t →
      Reachable limit batchMax t

end Ring

end Qtz
