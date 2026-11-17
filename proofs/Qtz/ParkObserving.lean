import Qtz.ParkModel

/-!
The `observing` protocol (`broadcast::Producer::push_block`) never loses a wake, for any number of
producers and any number of releases, when the channel is not closed.

The invariant counts what is owed to the ring. A producer owes one look at the ring when it is about
to `attempt`, or when a waker has retired its bit while it waits (its re-check, parked, or about to
disarm): that producer will look at the ring again before it can arm again. A waker between a release
and the `fetch_and` that retires a bit owes one pick. While some producer sleeps with its bit set, the
free slots never outnumber what is owed: a release adds a slot and an owed pick, the pick turns into
an owed look (it finds a set bit, the sleeper's, so it cannot come up empty), and a look takes a slot
or finds none free.

The protocols that lose a wake break exactly this: their re-check takes the slot itself, so a pick
that retires the stale bit lands on a producer that owes nothing.
-/

namespace Qtz

namespace Park

/-- 1 while a waker has yet to retire a bit. -/
def Waker.owed : Waker → Nat
  | .scan | .clear _ => 1
  | .claim _ => 0

/-- 1 when the producer will look at the ring again before it can arm: about to `attempt`, or
waiting with its bit retired by a waker. Under `repaired`, also 1 when its re-check won and its bit
was retired, until the wake it forwards retires another bit. -/
def Producer.owed (p : Producer) : Nat :=
  match p.pc with
  | .attempt => 1
  | .recheck | .park | .disarmWoken | .disarmWon => if p.bit then 0 else 1
  | .forward w => w.owed
  | _ => 0

/-- What `observing` keeps true of each producer. `claimed` says the consumer is about to claim this
producer's handle. A set bit only stands while the producer waits; a waiting producer without a token
still has its handle; and one whose bit is retired without a token is about to be claimed. -/
def Producer.sound (p : Producer) (claimed : Bool) : Bool :=
  match p.pc with
  | .attempt | .handle | .done => !p.bit
  | .bit => !p.bit && (p.token || p.handle)
  | .recheck | .park => (p.token || p.handle) && (p.bit || p.token || claimed)
  | .disarmWoken => true
  | _ => false

def Producer.owedSum (ps : List Producer) : Nat := (ps.map Producer.owed).sum

/-- 1 while a waker has released a slot and not yet retired a bit. -/
def Consumer.pending : Consumer → Nat
  | .waking .scan => 1
  | .waking (.clear _) => 1
  | _ => 0

def State.claimed (s : State) (i : Nat) : Bool := decide (s.consumer = .waking (.claim i))

/-- Some producer is parked with its bit set. -/
def State.Sleeper (s : State) : Prop :=
  ∃ i, (Producer.at s.producers i).pc = .park ∧ (Producer.at s.producers i).bit = true

structure State.Inv (s : State) : Prop where
  never_closes : s.closes = false
  open_ring : s.closed = false
  consumer_open : s.consumer = .release ∨ s.consumer = .finished ∨ ∃ w, s.consumer = .waking w
  sound : ∀ i, (Producer.at s.producers i).sound (s.claimed i) = true
  owed : s.Sleeper → s.free ≤ Producer.owedSum s.producers + s.consumer.pending

theorem Producer.at_update (ps : List Producer) (i j : Nat) (f : Producer → Producer) :
    Producer.at (Producer.update ps i f) j =
      if j = i ∧ i < ps.length then f (Producer.at ps i) else Producer.at ps j := by
  unfold Producer.at Producer.update
  by_cases eq : j = i
  · subst eq
    by_cases inside : j < ps.length
    · simp [inside, List.getD_eq_getElem?_getD, Producer.at]
    · simp [inside, List.getD_eq_getElem?_getD]
  · simp [eq, List.getD_eq_getElem?_getD, Ne.symm eq]

theorem Producer.owedSum_update (ps : List Producer) (i : Nat) (f : Producer → Producer)
    (inside : i < ps.length) :
    Producer.owedSum (Producer.update ps i f) + (Producer.at ps i).owed =
      Producer.owedSum ps + (f (Producer.at ps i)).owed := by
  unfold Producer.update
  generalize f (Producer.at ps i) = x
  induction ps generalizing i with
  | nil => simp at inside
  | cons head tail ih =>
    cases i with
    | zero => simp [Producer.owedSum, Producer.at]; omega
    | succ i =>
      have step := ih i (by simpa using inside)
      simp [Producer.owedSum, Producer.at] at step ⊢
      omega

theorem Producer.owedSum_update_same (ps : List Producer) (i : Nat) (f : Producer → Producer)
    (inside : i < ps.length) (same : (f (Producer.at ps i)).owed = (Producer.at ps i).owed) :
    Producer.owedSum (Producer.update ps i f) = Producer.owedSum ps := by
  have total := Producer.owedSum_update ps i f inside
  omega

theorem Consumer.afterRelease_open (left : Nat) :
    Consumer.afterRelease false left = .release ∨ Consumer.afterRelease false left = .finished ∨
      ∃ w, Consumer.afterRelease false left = .waking w := by
  simp only [Consumer.afterRelease]
  split <;> simp

theorem Producer.at_beyond {ps : List Producer} {i : Nat} (beyond : ¬ i < ps.length) :
    Producer.at ps i = Producer.fresh := by
  simp [Producer.at, List.getD_eq_getElem?_getD, List.getElem?_eq_none (Nat.le_of_not_lt beyond)]

theorem Producer.at_of_lt {ps : List Producer} {i : Nat} (inside : i < ps.length) :
    Producer.at ps i = ps[i] := by
  simp [Producer.at, List.getD_eq_getElem?_getD, inside]

theorem State.Sleeper.inside {s : State} {i : Nat}
    (bit : (Producer.at s.producers i).bit = true) : i < s.producers.length := by
  by_cases inside : i < s.producers.length
  · exact inside
  · rw [Producer.at_beyond inside] at bit
    cases bit

theorem Producer.retire {p : Producer} (bit : p.bit = true) (sound : p.sound false = true) :
    p.owed = 0 ∧ ({ p with bit := false } : Producer).owed = 1 ∧
      ({ p with bit := false } : Producer).sound true = true := by
  rcases p with ⟨pc, bitField, handle, token⟩
  cases pc <;> simp_all [Producer.sound, Producer.owed]

theorem Producer.claim_sound {p : Producer} (sound : p.sound true = true) :
    ({ p with handle := false, token := true } : Producer).sound false = true := by
  rcases p with ⟨pc, bit, handle, token⟩
  cases pc <;> simp_all [Producer.sound]

theorem Producer.unclaimed_sound {p : Producer} (handle : p.handle = false)
    (sound : p.sound true = true) : p.sound false = true := by
  rcases p with ⟨pc, bit, handleField, token⟩
  cases pc <;> simp_all [Producer.sound]

theorem Producer.waiting_bit {p : Producer} (park : p.pc = .park) (token : p.token = false)
    (sound : p.sound false = true) : p.bit = true ∧ p.owed = 0 := by
  rcases p with ⟨pc, bit, handle, tokenField⟩
  subst park
  cases bit <;> simp_all [Producer.sound, Producer.owed]

theorem Producer.live_of_sound {p : Producer} {claimed : Bool} (sound : p.sound claimed = true) :
    p.pc = .attempt ∨ p.pc = .handle ∨ p.pc = .bit ∨ p.pc = .recheck ∨ p.pc = .park ∨
      p.pc = .disarmWoken ∨ p.pc = .done := by
  rcases p with ⟨pc, bit, handle, token⟩
  cases pc <;> simp_all [Producer.sound]

theorem Waker.step_ne_nil (ps : List Producer) (w : Waker) : Waker.step ps w ≠ [] := by
  cases w with
  | scan =>
    simp only [Waker.step]
    split
    · simp
    · rename_i picks notEmpty
      simpa using notEmpty
  | clear i => simp only [Waker.step]; split <;> simp
  | claim i => simp only [Waker.step]; split <;> simp

theorem Producer.owedSum_eq_zero {ps : List Producer} (none : ∀ p ∈ ps, p.owed = 0) :
    Producer.owedSum ps = 0 := by
  induction ps with
  | nil => rfl
  | cons head tail ih =>
    simp only [Producer.owedSum, List.map_cons, List.sum_cons] at ih ⊢
    rw [none head (by simp), ih (fun p mem => none p (by simp [mem]))]

/-- A producer step: producer `i` changes by `f`, and the free count may change. -/
theorem State.Inv.move {s t : State} (inv : s.Inv) {i : Nat} (inside : i < s.producers.length)
    {f : Producer → Producer}
    (producers : t.producers = Producer.update s.producers i f)
    (consumer : t.consumer = s.consumer) (closes : t.closes = s.closes)
    (closed : t.closed = s.closed)
    (sound : (f (Producer.at s.producers i)).sound (s.claimed i) = true)
    (bound : t.free = 0 ∨
      (t.free + (Producer.at s.producers i).owed ≤ s.free + (f (Producer.at s.producers i)).owed ∧
        ((f (Producer.at s.producers i)).pc = .park → (f (Producer.at s.producers i)).bit = false))) :
    t.Inv where
  never_closes := closes ▸ inv.never_closes
  open_ring := closed ▸ inv.open_ring
  consumer_open := consumer ▸ inv.consumer_open
  sound j := by
    have claimed : t.claimed j = s.claimed j := by simp [State.claimed, consumer]
    rw [claimed, producers, Producer.at_update]
    split
    · rename_i same
      rw [same.1]
      exact sound
    · exact inv.sound j
  owed sleeper := by
    rcases bound with zero | ⟨le, notPark⟩
    · omega
    obtain ⟨j, park, bit⟩ := sleeper
    rw [producers, Producer.at_update] at park bit
    have before : s.Sleeper := by
      split at park
      · rename_i same
        rw [ite_eq_left_of_eq_true _ _ (eq_true same)] at bit
        exact absurd (notPark park) (by simp [bit])
      · rename_i other
        rw [ite_eq_right_of_eq_false _ _ (eq_false other)] at bit
        exact ⟨j, park, bit⟩
    have total := Producer.owedSum_update s.producers i f inside
    have previous := inv.owed before
    rw [producers, consumer]
    omega

theorem State.Inv.producer_step {s t : State} (inv : s.Inv) {i : Nat}
    (inside : i < s.producers.length) (mem : t ∈ s.producerSteps .observing i) : t.Inv := by
  have sound := inv.sound i
  have live := Producer.live_of_sound sound
  unfold State.producerSteps at mem
  simp only [inv.open_ring, Bool.false_and, Bool.false_eq_true, ite_false] at mem
  rcases live with pc | pc | pc | pc | pc | pc | pc <;> simp only [pc] at mem
  · split at mem
    · rename_i free
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .done }) inside rfl rfl rfl rfl
        (by simp_all [Producer.sound])
        (Or.inr ⟨by simp [Producer.owed, pc]; omega, by simp⟩)
    · rename_i full
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .handle }) inside rfl rfl rfl rfl
        (by simp_all [Producer.sound]) (Or.inl (by simp [State.setPc]; omega))
  · simp only [List.mem_singleton] at mem
    subst mem
    exact inv.move (f := fun p => { p with handle := true, pc := .bit }) inside rfl rfl rfl rfl
      (by simp_all [Producer.sound])
      (Or.inr ⟨by simp [Producer.owed, pc, State.edit], by simp⟩)
  · simp only [List.mem_singleton] at mem
    subst mem
    exact inv.move (f := fun p => { p with bit := true, pc := .recheck }) inside rfl rfl rfl rfl
      (by simp_all [Producer.sound])
      (Or.inr ⟨by simp [Producer.owed, pc, State.edit], by simp⟩)
  · split at mem
    · simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .disarmWoken }) inside rfl rfl rfl rfl
        (by simp_all [Producer.sound])
        (Or.inr ⟨by simp [Producer.owed, pc, State.setPc], by simp⟩)
    · rename_i full
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .park }) inside rfl rfl rfl rfl
        (by simp_all [Producer.sound]) (Or.inl (by simp [State.setPc]; omega))
  · split at mem
    · simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with token := false, pc := .disarmWoken }) inside
        rfl rfl rfl rfl (by simp_all [Producer.sound])
        (Or.inr ⟨by simp [Producer.owed, pc, State.edit], by simp⟩)
    · simp at mem
  · simp only [List.mem_singleton] at mem
    subst mem
    exact inv.move (f := fun p => { p with bit := false, pc := .attempt }) inside rfl rfl rfl rfl
      (by simp_all [Producer.sound])
      (Or.inr ⟨by simp only [Producer.owed, pc, State.edit]; split <;> omega, by simp⟩)
  · simp at mem

theorem State.Inv.sound_unclaimed {s : State} (inv : s.Inv) {consumer : Consumer}
    (before : ∀ k, s.consumer ≠ .waking (.claim k)) (after : ∀ k, consumer ≠ .waking (.claim k))
    (j : Nat) : (Producer.at s.producers j).sound (decide (consumer = .waking (.claim j))) = true := by
  have sound := inv.sound j
  simp only [State.claimed, decide_eq_false (before j)] at sound
  rw [decide_eq_false (after j)]
  exact sound

theorem State.Inv.consumer_step {s t : State} (inv : s.Inv) (mem : t ∈ s.consumerSteps) :
    t.Inv := by
  have afterRelease : ∀ k, Consumer.afterRelease s.closes s.left ≠ .waking (.claim k) := by
    intro k
    unfold Consumer.afterRelease
    split <;> (try split) <;> simp
  rcases inv.consumer_open with consumer | consumer | ⟨w, consumer⟩
  · simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
    subst mem
    exact {
      never_closes := inv.never_closes
      open_ring := inv.open_ring
      consumer_open := Or.inr (Or.inr ⟨.scan, rfl⟩)
      sound := inv.sound_unclaimed (by simp [consumer]) (by simp)
      owed := fun sleeper => by
        have := inv.owed sleeper
        simp only [consumer, Consumer.pending] at this ⊢
        omega }
  · simp [State.consumerSteps, consumer] at mem
  · simp only [State.consumerSteps, consumer, List.mem_map] at mem
    obtain ⟨⟨ps, next⟩, step, rfl⟩ := mem
    cases w with
    | scan =>
      simp only [Waker.step] at step
      split at step
      · rename_i none
        simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        exact {
          never_closes := inv.never_closes
          open_ring := inv.open_ring
          consumer_open := inv.never_closes ▸ Consumer.afterRelease_open s.left
          sound := inv.sound_unclaimed (by simp [consumer]) afterRelease
          owed := fun ⟨j, _, bit⟩ => by
            have mem : j ∈ (List.range s.producers.length).filter
                (fun i => (Producer.at s.producers i).bit) :=
              List.mem_filter.mpr ⟨List.mem_range.mpr (State.Sleeper.inside bit), bit⟩
            simp [none] at mem }
      · simp only [List.mem_map] at step
        obtain ⟨k, _, step⟩ := step
        simp only [Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        exact {
          never_closes := inv.never_closes
          open_ring := inv.open_ring
          consumer_open := Or.inr (Or.inr ⟨.clear k, rfl⟩)
          sound := inv.sound_unclaimed (by simp [consumer]) (by simp)
          owed := fun sleeper => by
            have := inv.owed sleeper
            simp only [consumer, Consumer.pending] at this ⊢
            omega }
    | clear k =>
      simp only [Waker.step] at step
      split at step
      · rename_i bit
        simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        have inside := State.Sleeper.inside bit
        have unclaimed := inv.sound k
        simp only [State.claimed, consumer] at unclaimed
        obtain ⟨owedBefore, owedAfter, soundAfter⟩ := Producer.retire bit unclaimed
        exact {
          never_closes := inv.never_closes
          open_ring := inv.open_ring
          consumer_open := Or.inr (Or.inr ⟨.claim k, rfl⟩)
          sound := fun j => by
            simp only [State.claimed]
            rw [Producer.at_update]
            split
            · rename_i same
              rw [same.1]
              simpa using soundAfter
            · rename_i other
              have different : j ≠ k := fun eq => other ⟨eq, inside⟩
              have sound := inv.sound j
              simp only [State.claimed, consumer] at sound
              simpa [Ne.symm different] using sound
          owed := fun ⟨j, park, bitAfter⟩ => by
            rw [Producer.at_update] at park bitAfter
            have before : s.Sleeper := by
              split at park
              · rename_i same
                rw [ite_eq_left_of_eq_true _ _ (eq_true same)] at bitAfter
                simp at bitAfter
              · rename_i other
                rw [ite_eq_right_of_eq_false _ _ (eq_false other)] at bitAfter
                exact ⟨j, park, bitAfter⟩
            have previous := inv.owed before
            have total := Producer.owedSum_update s.producers k
              (fun p => { p with bit := false }) inside
            simp only [consumer, Consumer.pending] at previous ⊢
            omega }
      · simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        exact {
          never_closes := inv.never_closes
          open_ring := inv.open_ring
          consumer_open := Or.inr (Or.inr ⟨.scan, rfl⟩)
          sound := inv.sound_unclaimed (by simp [consumer]) (by simp)
          owed := fun sleeper => by
            have := inv.owed sleeper
            simp only [consumer, Consumer.pending] at this ⊢
            omega }
    | claim k =>
      simp only [Waker.step] at step
      have claimedSound := inv.sound k
      simp only [State.claimed, consumer, decide_true] at claimedSound
      split at step
      · rename_i handle
        simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        have inside : k < s.producers.length := by
          by_cases inside : k < s.producers.length
          · exact inside
          · rw [Producer.at_beyond inside] at handle
            cases handle
        exact {
          never_closes := inv.never_closes
          open_ring := inv.open_ring
          consumer_open := inv.never_closes ▸ Consumer.afterRelease_open s.left
          sound := fun j => by
            simp only [State.claimed]
            rw [Producer.at_update, decide_eq_false (afterRelease j)]
            split
            · exact Producer.claim_sound claimedSound
            · rename_i other
              by_cases eq : j = k
              · exact absurd ⟨eq, inside⟩ other
              · have sound := inv.sound j
                simp only [State.claimed, consumer] at sound
                simpa [Ne.symm eq] using sound
          owed := fun ⟨j, park, bit⟩ => by
            rw [Producer.at_update] at park bit
            have before : s.Sleeper := by
              split at park
              · rename_i same
                rw [ite_eq_left_of_eq_true _ _ (eq_true same)] at bit
                exact ⟨k, park, bit⟩
              · rename_i other
                rw [ite_eq_right_of_eq_false _ _ (eq_false other)] at bit
                exact ⟨j, park, bit⟩
            have previous := inv.owed before
            have total := Producer.owedSum_update_same s.producers k
              (fun p => { p with handle := false, token := true }) inside rfl
            have pendingAfter : (Consumer.afterRelease s.closes s.left).pending = 0 := by
              unfold Consumer.afterRelease
              split <;> (try split) <;> rfl
            simp only [consumer, Consumer.pending] at previous
            simp only [pendingAfter]
            omega }
      · rename_i handle
        simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        exact {
          never_closes := inv.never_closes
          open_ring := inv.open_ring
          consumer_open := Or.inr (Or.inr ⟨.scan, rfl⟩)
          sound := fun j => by
            simp only [State.claimed]
            by_cases eq : j = k
            · subst eq
              exact Producer.unclaimed_sound (by simpa using handle) claimedSound
            · have sound := inv.sound j
              simp only [State.claimed, consumer] at sound
              simpa [Ne.symm eq] using sound
          owed := fun sleeper => by
            have := inv.owed sleeper
            simp only [consumer, Consumer.pending] at this ⊢
            omega }

theorem State.Inv.step {s t : State} (inv : s.Inv) (mem : t ∈ s.succs .observing) : t.Inv := by
  unfold State.succs at mem
  rcases List.mem_append.mp mem with consumer | producer
  · exact inv.consumer_step consumer
  · obtain ⟨i, inside, step⟩ := List.mem_flatMap.mp producer
    exact inv.producer_step (List.mem_range.mp inside) step

theorem State.Inv.init (n releases : Nat) : (State.init n releases).Inv where
  never_closes := rfl
  open_ring := rfl
  consumer_open := by
    simp only [State.init, Consumer.afterRelease]
    split <;> simp
  sound i := by
    simp only [State.init, Producer.at, List.getD_eq_getElem?_getD, List.getElem?_replicate]
    split <;> rfl
  owed _ := by simp [State.init]

theorem Reach.observing_inv {n releases : Nat} {s : State}
    (reach : Reach .observing (State.init n releases) s) : s.Inv := by
  induction reach with
  | init => exact State.Inv.init n releases
  | step _ mem inv => exact inv.step mem

/-- No state the `observing` protocol reaches is lost, for every number of producers and
releases. -/
theorem observing_never_lost {n releases : Nat} {s : State}
    (reach : Reach .observing (State.init n releases) s) : ¬ s.Lost .observing := by
  intro ⟨empty, freeOrClosed, ⟨p, member, unfinished⟩⟩
  have inv := reach.observing_inv
  unfold State.succs at empty
  obtain ⟨consumerIdle, producersIdle⟩ := List.append_eq_nil_iff.mp empty
  have finished : s.consumer = .finished := by
    rcases inv.consumer_open with consumer | consumer | ⟨w, consumer⟩
    · simp [State.consumerSteps, consumer] at consumerIdle
    · exact consumer
    · simp only [State.consumerSteps, consumer, List.map_eq_nil_iff] at consumerIdle
      exact absurd consumerIdle (Waker.step_ne_nil _ _)
  have idle : ∀ i, i < s.producers.length →
      (Producer.at s.producers i).pc = .done ∨
        ((Producer.at s.producers i).pc = .park ∧ (Producer.at s.producers i).token = false) := by
    intro i inside
    have stuck := (List.flatMap_eq_nil_iff.mp producersIdle) i (List.mem_range.mpr inside)
    rcases Producer.live_of_sound (inv.sound i) with pc | pc | pc | pc | pc | pc | pc <;>
      simp only [State.producerSteps, pc, inv.open_ring, Bool.false_and, Bool.false_eq_true,
        ite_false] at stuck
    · split at stuck <;> simp at stuck
    · simp at stuck
    · simp at stuck
    · split at stuck <;> simp at stuck
    · split at stuck
      · simp at stuck
      · rename_i token
        exact Or.inr ⟨pc, by simpa using token⟩
    · simp at stuck
    · exact Or.inl pc
  have unclaimed : ∀ i, (Producer.at s.producers i).sound false = true := by
    intro i
    have sound := inv.sound i
    simpa [State.claimed, finished] using sound
  have nothingOwed : Producer.owedSum s.producers = 0 := by
    refine Producer.owedSum_eq_zero fun q qMember => ?_
    obtain ⟨i, inside, rfl⟩ := List.getElem_of_mem qMember
    rw [← Producer.at_of_lt inside]
    rcases idle i inside with done | ⟨park, token⟩
    · simp [Producer.owed, done]
    · exact (Producer.waiting_bit park token (unclaimed i)).2
  obtain ⟨i, inside, rfl⟩ := List.getElem_of_mem member
  rw [← Producer.at_of_lt inside] at unfinished
  rcases idle i inside with done | ⟨park, token⟩
  · exact unfinished done
  have bit := (Producer.waiting_bit park token (unclaimed i)).1
  have bound := inv.owed ⟨i, park, bit⟩
  simp only [nothingOwed, finished, Consumer.pending] at bound
  rcases freeOrClosed with free | closed
  · omega
  · simp [inv.open_ring] at closed

end Park

end Qtz
