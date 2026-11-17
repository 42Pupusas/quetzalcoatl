import Qtz.ParkRepaired

/-!
Closing the channel loses no producer. With `ParkObserving` and `ParkRepaired`, this gives that the
`observing` and `repaired` protocols never lose a wake, for any number of producers and releases,
whether or not the consumer closes.

Once `closed` is set no producer can start to park: the re-check after `arm` sees `closed` and takes
the closed exit. So the parked producers are the ones parked at the `close` step, and each of them is
answered: it holds a token, or it still has its handle and either the flush will take its bit or a
waker forwarded by a `stand_down` is about to claim it. The flush swaps every bit to zero and claims
the handle of each producer whose bit it took, so when it has finished, a producer still parked
without a token must have a forwarded waker on the way, and a forwarding producer is not stuck.
-/

namespace Qtz

namespace Park

/-- Some producer's forwarded wake loop is about to claim `i`'s handle. -/
def State.ForwardClaims (s : State) (i : Nat) : Prop :=
  ∃ k, (Producer.at s.producers k).pc = .forward (.claim i)

/-- The flush will claim `i`'s handle: it has yet to swap the bits and `i`'s is set, or it took
`i`'s bit and has yet to claim. -/
def State.Pending (s : State) (i : Nat) : Prop :=
  match s.consumer with
  | .flush => (Producer.at s.producers i).bit = true
  | .flushing pending => i ∈ pending
  | _ => False

/-- A parked producer `i` will be woken. -/
def State.WillWake (s : State) (i : Nat) : Prop :=
  (Producer.at s.producers i).token = true ∨
    ((Producer.at s.producers i).handle = true ∧ (s.Pending i ∨ s.ForwardClaims i))

structure State.Closing (s : State) : Prop where
  closed : s.closed = true
  consumer_closing :
    s.consumer = .flush ∨ (∃ pending, s.consumer = .flushing pending) ∨ s.consumer = .finished
  parked : ∀ j, (Producer.at s.producers j).pc = .park → s.WillWake j

theorem Producer.inside_of_pc {ps : List Producer} {i : Nat} {pc : Pc}
    (eq : (Producer.at ps i).pc = pc) (moved : pc ≠ .attempt) : i < ps.length :=
  Producer.inside fun fresh => moved (by rw [← eq, fresh]; rfl)

theorem Producer.at_clear_bits (ps : List Producer) (k : Nat) :
    Producer.at (ps.map fun p => { p with bit := false }) k =
      { Producer.at ps k with bit := false } := by
  unfold Producer.at
  rw [List.getD_eq_getElem?_getD, List.getD_eq_getElem?_getD, List.getElem?_map]
  cases ps[k]? <;> rfl

theorem State.WillWake.transfer {s t : State} {j : Nat} (will : s.WillWake j)
    (consumer : t.consumer = s.consumer)
    (same : Producer.at t.producers j = Producer.at s.producers j)
    (claims : ∀ k, (Producer.at s.producers k).pc = .forward (.claim j) →
      (Producer.at t.producers k).pc = .forward (.claim j)) :
    t.WillWake j := by
  unfold State.WillWake at *
  rw [same]
  rcases will with token | ⟨handle, pending | ⟨k, claim⟩⟩
  · exact Or.inl token
  · refine Or.inr ⟨handle, Or.inl ?_⟩
    revert pending
    unfold State.Pending
    rw [consumer, same]
    exact id
  · exact Or.inr ⟨handle, Or.inr ⟨k, claims k claim⟩⟩

/-- No producer step can be stuck but a finished producer or one parked without a token. -/
theorem State.producerSteps_idle {variant : Variant} {s : State} {i : Nat}
    (stuck : s.producerSteps variant i = []) :
    (Producer.at s.producers i).pc = .done ∨
      ((Producer.at s.producers i).pc = .park ∧ (Producer.at s.producers i).token = false) := by
  unfold State.producerSteps at stuck
  cases pcEq : (Producer.at s.producers i).pc with
  | park =>
    simp only [pcEq] at stuck
    split at stuck
    · simp at stuck
    · rename_i token
      exact Or.inr ⟨rfl, by simpa using token⟩
  | forward w =>
    simp only [pcEq, List.map_eq_nil_iff] at stuck
    exact absurd stuck (Waker.step_ne_nil _ _)
  | done => exact Or.inl rfl
  | recheck =>
    simp only [pcEq] at stuck
    split at stuck
    · simp at stuck
    · split at stuck
      · cases variant <;> simp at stuck
      · simp at stuck
  | _ => simp only [pcEq] at stuck; (repeat' split at stuck) <;> simp at stuck

/-- With the channel closed, a producer step either edits that producer alone, leaving it not
parked and not forwarding, or is one step of its forwarded wake loop. -/
theorem State.closed_producer_step {variant : Variant} (sees : variant.seesClose = true)
    {s t : State} (closed : s.closed = true) {i : Nat} (mem : t ∈ s.producerSteps variant i) :
    t.consumer = s.consumer ∧ t.closed = s.closed ∧
      ((∃ f : Producer → Producer, t.producers = Producer.update s.producers i f ∧
          (f (Producer.at s.producers i)).pc ≠ .park ∧
          ∀ w, (Producer.at s.producers i).pc ≠ .forward w) ∨
        ∃ w ps' next, (Producer.at s.producers i).pc = .forward w ∧
          (ps', next) ∈ Waker.step s.producers w ∧
          t.producers = Producer.update ps' i fun p => { p with pc := Waker.resume next }) := by
  have edited : ∀ f : Producer → Producer, (f (Producer.at s.producers i)).pc ≠ .park →
      (∀ w, (Producer.at s.producers i).pc ≠ .forward w) → t = s.edit i f →
      t.consumer = s.consumer ∧ t.closed = s.closed ∧
        ((∃ f : Producer → Producer, t.producers = Producer.update s.producers i f ∧
            (f (Producer.at s.producers i)).pc ≠ .park ∧
            ∀ w, (Producer.at s.producers i).pc ≠ .forward w) ∨
          ∃ w ps' next, (Producer.at s.producers i).pc = .forward w ∧
            (ps', next) ∈ Waker.step s.producers w ∧
            t.producers = Producer.update ps' i fun p => { p with pc := Waker.resume next }) := by
    rintro f notPark notForward rfl
    exact ⟨rfl, rfl, Or.inl ⟨f, rfl, notPark, notForward⟩⟩
  unfold State.producerSteps at mem
  obtain ⟨pc, pcEq⟩ : ∃ pc, (Producer.at s.producers i).pc = pc := ⟨_, rfl⟩
  cases pc with
  | attempt =>
    simp only [pcEq, closed, ite_true, List.mem_singleton] at mem
    exact edited _ (by simp) (by simp [pcEq]) mem
  | handle =>
    simp only [pcEq, List.mem_singleton] at mem
    exact edited _ (by simp) (by simp [pcEq]) mem
  | bit =>
    simp only [pcEq, List.mem_singleton] at mem
    exact edited _ (by simp) (by simp [pcEq]) mem
  | recheck =>
    simp only [pcEq, closed, sees, Bool.and_self, ite_true, List.mem_singleton] at mem
    exact edited _ (by simp) (by simp [pcEq]) mem
  | park =>
    simp only [pcEq] at mem
    split at mem
    · simp only [List.mem_singleton] at mem
      exact edited _ (by simp) (by simp [pcEq]) mem
    · simp at mem
  | disarmWon =>
    simp only [pcEq] at mem
    split at mem <;> simp only [List.mem_singleton] at mem <;>
      exact edited _ (by simp) (by simp [pcEq]) mem
  | disarmWoken =>
    simp only [pcEq, List.mem_singleton] at mem
    exact edited _ (by simp) (by simp [pcEq]) mem
  | disarmClosed =>
    simp only [pcEq, List.mem_singleton] at mem
    exact edited _ (by simp) (by simp [pcEq]) mem
  | forward w =>
    simp only [pcEq, List.mem_map] at mem
    obtain ⟨⟨ps', next⟩, step, rfl⟩ := mem
    cases next <;> exact ⟨rfl, rfl, Or.inr ⟨w, ps', _, pcEq, step, rfl⟩⟩
  | done => simp [pcEq] at mem

theorem State.Closing.edit {s t : State} (inv : s.Closing) {i : Nat} {f : Producer → Producer}
    (consumer : t.consumer = s.consumer) (closed : t.closed = s.closed)
    (producers : t.producers = Producer.update s.producers i f)
    (notPark : (f (Producer.at s.producers i)).pc ≠ .park)
    (notForward : ∀ w, (Producer.at s.producers i).pc ≠ .forward w) : t.Closing where
  closed := closed ▸ inv.closed
  consumer_closing := consumer ▸ inv.consumer_closing
  parked j park := by
    have unchanged : ∀ k, k ≠ i → Producer.at t.producers k = Producer.at s.producers k :=
      fun k different => by
        rw [producers, Producer.at_update,
          ite_eq_right_of_eq_false _ _ (eq_false fun same => different same.1)]
    have notI : j ≠ i := by
      intro same
      subst same
      rw [producers, Producer.at_update] at park
      split at park
      · exact notPark park
      · rename_i outside
        exact outside ⟨rfl, Producer.inside_of_pc park (by simp)⟩
    rw [unchanged j notI] at park
    refine (inv.parked j park).transfer consumer (unchanged j notI) fun k claim => ?_
    have notK : k ≠ i := fun same => notForward _ (same ▸ claim)
    rw [unchanged k notK]
    exact claim

theorem State.Closing.forward {s t : State} (inv : s.Closing) {i : Nat} {w : Waker}
    (consumer : t.consumer = s.consumer) (closed : t.closed = s.closed)
    (pc : (Producer.at s.producers i).pc = .forward w) {ps' : List Producer}
    {next : Option Waker} (step : (ps', next) ∈ Waker.step s.producers w)
    (producers : t.producers = Producer.update ps' i fun p => { p with pc := Waker.resume next }) :
    t.Closing where
  closed := closed ▸ inv.closed
  consumer_closing := consumer ▸ inv.consumer_closing
  parked j park := by
    obtain ⟨length, frame⟩ := Waker.step_frame step
    have insideI : i < s.producers.length := Producer.inside_of_pc pc (by simp)
    have others : ∀ k, k ≠ i → Producer.at t.producers k = Producer.at ps' k :=
      fun k different => by
        rw [producers, Producer.at_update,
          ite_eq_right_of_eq_false _ _ (eq_false fun same => different same.1)]
    have self : (Producer.at t.producers i).pc = Waker.resume next := by
      rw [producers, Producer.at_update, ite_eq_left_of_eq_true _ _ (eq_true ⟨rfl, by omega⟩)]
    have notI : j ≠ i := by
      intro same
      subst same
      rw [self] at park
      cases next <;> simp [Waker.resume] at park
    rw [others j notI] at park
    have parkBefore : (Producer.at s.producers j).pc = .park := (frame j).1.symm.trans park
    have insideJ : j < s.producers.length := Producer.inside_of_pc parkBefore (by simp)
    have will := inv.parked j parkBefore
    have keep : w ≠ .claim j → ∀ k, (Producer.at s.producers k).pc = .forward (.claim j) →
        (Producer.at t.producers k).pc = .forward (.claim j) := by
      intro notMine k claim
      by_cases same : k = i
      · subst same
        rw [pc] at claim
        exact absurd (Pc.forward.inj claim) notMine
      · rw [others k same, (frame k).1]
        exact claim
    have unmoved : ps' = s.producers → w ≠ .claim j → t.WillWake j := fun same notMine =>
      will.transfer consumer (by rw [others j notI, same]) (keep notMine)
    cases w with
    | scan =>
      simp only [Waker.step] at step
      split at step
      · simp only [List.mem_singleton, Prod.mk.injEq] at step
        exact unmoved step.1 (by simp)
      · simp only [List.mem_map, Prod.mk.injEq] at step
        obtain ⟨_, _, rfl, _⟩ := step
        exact unmoved rfl (by simp)
    | clear k =>
      simp only [Waker.step] at step
      split at step
      · simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        by_cases same : j = k
        · subst same
          unfold State.WillWake
          rw [others j notI, Producer.at_update,
            ite_eq_left_of_eq_true _ _ (eq_true ⟨rfl, insideJ⟩)]
          rcases will with token | ⟨handle, _⟩
          · exact Or.inl token
          · exact Or.inr ⟨handle, Or.inr ⟨i, by rw [self]; rfl⟩⟩
        · refine will.transfer consumer ?_ (keep (by simp))
          rw [others j notI, Producer.at_update,
            ite_eq_right_of_eq_false _ _ (eq_false fun eq => same eq.1)]
      · simp only [List.mem_singleton, Prod.mk.injEq] at step
        exact will.transfer consumer (by rw [others j notI, step.1]) (keep (by simp))
    | claim k =>
      simp only [Waker.step] at step
      split at step
      · simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        by_cases same : j = k
        · subst same
          unfold State.WillWake
          rw [others j notI, Producer.at_update,
            ite_eq_left_of_eq_true _ _ (eq_true ⟨rfl, insideJ⟩)]
          exact Or.inl rfl
        · refine will.transfer consumer ?_ (keep fun eq => same (Waker.claim.inj eq).symm)
          rw [others j notI, Producer.at_update,
            ite_eq_right_of_eq_false _ _ (eq_false fun eq => same eq.1)]
      · rename_i handle
        simp only [List.mem_singleton, Prod.mk.injEq] at step
        obtain ⟨rfl, rfl⟩ := step
        by_cases same : j = k
        · subst same
          unfold State.WillWake
          rw [others j notI]
          rcases will with token | ⟨handleSet, _⟩
          · exact Or.inl token
          · exact absurd handleSet (by simpa using handle)
        · exact will.transfer consumer (by rw [others j notI])
            (keep fun eq => same (Waker.claim.inj eq).symm)

theorem State.Closing.consumer_step {s t : State} (inv : s.Closing)
    (mem : t ∈ s.consumerSteps) : t.Closing := by
  rcases inv.consumer_closing with consumer | ⟨pending, consumer⟩ | consumer
  · simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
    subst mem
    exact {
      closed := inv.closed
      consumer_closing := Or.inr (Or.inl ⟨_, rfl⟩)
      parked := fun j park => by
        rw [Producer.at_clear_bits] at park
        have insideJ : j < s.producers.length := Producer.inside_of_pc park (by simp)
        unfold State.WillWake State.Pending State.ForwardClaims
        simp only [Producer.at_clear_bits]
        rcases inv.parked j park with token | ⟨handle, pending | ⟨k, claim⟩⟩
        · exact Or.inl token
        · simp only [State.Pending, consumer] at pending
          exact Or.inr ⟨handle, Or.inl
            (List.mem_filter.mpr ⟨List.mem_range.mpr insideJ, pending⟩)⟩
        · exact Or.inr ⟨handle, Or.inr ⟨k, claim⟩⟩ }
  · cases pending with
    | nil =>
      simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
      subst mem
      exact {
        closed := inv.closed
        consumer_closing := Or.inr (Or.inr rfl)
        parked := fun j park => by
          rcases inv.parked j park with token | ⟨handle, pending | forward⟩
          · exact Or.inl token
          · simp [State.Pending, consumer] at pending
          · exact Or.inr ⟨handle, Or.inr forward⟩ }
    | cons i rest =>
      simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
      subst mem
      have pcSame : ∀ k, (Producer.at (Producer.update s.producers i fun p =>
          if p.handle then { p with handle := false, token := true } else p) k).pc =
            (Producer.at s.producers k).pc := by
        intro k
        rw [Producer.at_update]
        split
        · rename_i same
          rw [same.1]
          split <;> rfl
        · rfl
      exact {
        closed := inv.closed
        consumer_closing := Or.inr (Or.inl ⟨rest, rfl⟩)
        parked := fun j park => by
          rw [pcSame] at park
          have insideJ : j < s.producers.length := Producer.inside_of_pc park (by simp)
          have will := inv.parked j park
          unfold State.WillWake State.Pending State.ForwardClaims
          simp only [pcSame]
          by_cases same : j = i
          · subst same
            rw [Producer.at_update, ite_eq_left_of_eq_true _ _ (eq_true ⟨rfl, insideJ⟩)]
            cases handle : (Producer.at s.producers j).handle
            · rcases will with token | ⟨handleSet, _⟩
              · exact Or.inl (by simp [token])
              · rw [handle] at handleSet
                cases handleSet
            · exact Or.inl (by simp)
          · rw [Producer.at_update, ite_eq_right_of_eq_false _ _ (eq_false fun eq => same eq.1)]
            rcases will with token | ⟨handle, pending | forward⟩
            · exact Or.inl token
            · simp only [State.Pending, consumer, List.mem_cons] at pending
              exact Or.inr ⟨handle, Or.inl (pending.resolve_left same)⟩
            · exact Or.inr ⟨handle, Or.inr forward⟩ }
  · simp [State.consumerSteps, consumer] at mem

theorem State.Closing.step {variant : Variant} (sees : variant.seesClose = true) {s t : State}
    (inv : s.Closing) (mem : t ∈ s.succs variant) : t.Closing := by
  unfold State.succs at mem
  rcases List.mem_append.mp mem with consumer | producer
  · exact inv.consumer_step consumer
  · obtain ⟨i, _, step⟩ := List.mem_flatMap.mp producer
    obtain ⟨consumer, closed, edit | ⟨w, ps', next, pc, wake, producers⟩⟩ :=
      State.closed_producer_step sees inv.closed step
    · obtain ⟨f, producers, notPark, notForward⟩ := edit
      exact inv.edit consumer closed producers notPark notForward
    · exact inv.forward consumer closed pc wake producers

/-- The consumer's `close` step, from a state whose parked producers are each answered. -/
theorem State.Closing.enter {s t : State} (close : s.consumer = .close)
    (parked : ∀ j, (Producer.at s.producers j).pc = .park →
      (Producer.at s.producers j).token = true ∨
        ((Producer.at s.producers j).handle = true ∧
          ((Producer.at s.producers j).bit = true ∨ s.ForwardClaims j)))
    (mem : t ∈ s.consumerSteps) : t.Closing := by
  simp only [State.consumerSteps, close, List.mem_singleton] at mem
  subst mem
  exact {
    closed := rfl
    consumer_closing := Or.inl rfl
    parked := fun j park => (parked j park).imp_right fun ⟨handle, rest⟩ => ⟨handle, rest⟩ }

theorem State.Closing.not_lost {variant : Variant} {s : State} (inv : s.Closing) :
    ¬ s.Lost variant := by
  intro ⟨empty, _, ⟨p, member, unfinished⟩⟩
  unfold State.succs at empty
  obtain ⟨consumerIdle, producersIdle⟩ := List.append_eq_nil_iff.mp empty
  have finished : s.consumer = .finished := by
    rcases inv.consumer_closing with consumer | ⟨pending, consumer⟩ | consumer
    · simp [State.consumerSteps, consumer] at consumerIdle
    · cases pending <;> simp [State.consumerSteps, consumer] at consumerIdle
    · exact consumer
  have idle : ∀ i, i < s.producers.length →
      (Producer.at s.producers i).pc = .done ∨
        ((Producer.at s.producers i).pc = .park ∧ (Producer.at s.producers i).token = false) :=
    fun i inside => State.producerSteps_idle
      ((List.flatMap_eq_nil_iff.mp producersIdle) i (List.mem_range.mpr inside))
  obtain ⟨i, inside, rfl⟩ := List.getElem_of_mem member
  rw [← Producer.at_of_lt inside] at unfinished
  rcases idle i inside with done | ⟨park, token⟩
  · exact unfinished done
  rcases inv.parked i park with woken | ⟨_, pending | ⟨k, claim⟩⟩
  · rw [token] at woken
    cases woken
  · simp [State.Pending, finished] at pending
  · rcases idle k (Producer.inside_of_pc claim (by simp)) with done | ⟨parked, _⟩
    · rw [done] at claim
      cases claim
    · rw [parked] at claim
      cases claim

theorem State.Inv.parked_at_close {s : State} (inv : s.Inv) (close : s.consumer = .close)
    (j : Nat) (park : (Producer.at s.producers j).pc = .park) :
    (Producer.at s.producers j).token = true ∨
      ((Producer.at s.producers j).handle = true ∧
        ((Producer.at s.producers j).bit = true ∨ s.ForwardClaims j)) := by
  have sound := inv.sound j
  simp only [State.claimed, close, reduceCtorEq, decide_false] at sound
  revert sound park
  generalize Producer.at s.producers j = p
  rcases p with ⟨pc, bit, handle, token⟩
  intro park sound
  subst park
  cases bit <;> cases handle <;> cases token <;> simp_all [Producer.sound]

theorem State.Repaired.parked_at_close {s : State} (inv : s.Repaired)
    (close : s.consumer = .close) (j : Nat) (park : (Producer.at s.producers j).pc = .park) :
    (Producer.at s.producers j).token = true ∨
      ((Producer.at s.producers j).handle = true ∧
        ((Producer.at s.producers j).bit = true ∨ s.ForwardClaims j)) := by
  have claims : s.Claims j → s.ForwardClaims j := fun claims =>
    claims.elim (fun waking => by rw [close] at waking; cases waking) id
  have fit := inv.fit j
  revert fit park claims
  generalize s.Claims j = claimed
  generalize Producer.at s.producers j = p
  rcases p with ⟨pc, bit, handle, token⟩
  intro park claims fit
  subst park
  simp only [Producer.Fit] at fit
  cases bit <;> cases handle <;> cases token <;> simp_all

theorem Reach.observing_phase {n releases : Nat} {closes : Bool} {s : State}
    (reach : Reach .observing (State.init n releases closes) s) : s.Inv ∨ s.Closing := by
  induction reach with
  | init => exact Or.inl (State.Inv.init n releases closes)
  | @step s t _ mem phase =>
    rcases phase with inv | closing
    · unfold State.succs at mem
      rcases List.mem_append.mp mem with consumer | producer
      · by_cases close : s.consumer = .close
        · exact Or.inr (State.Closing.enter close (inv.parked_at_close close) consumer)
        · exact Or.inl (inv.consumer_step close consumer)
      · obtain ⟨i, inside, step⟩ := List.mem_flatMap.mp producer
        exact Or.inl (inv.producer_step (List.mem_range.mp inside) step)
    · exact Or.inr (closing.step rfl mem)

theorem Reach.repaired_phase {n releases : Nat} {closes : Bool} {s : State}
    (reach : Reach .repaired (State.init n releases closes) s) : s.Repaired ∨ s.Closing := by
  induction reach with
  | init => exact Or.inl (State.Repaired.init n releases closes)
  | @step s t _ mem phase =>
    rcases phase with inv | closing
    · unfold State.succs at mem
      rcases List.mem_append.mp mem with consumer | producer
      · by_cases close : s.consumer = .close
        · exact Or.inr (State.Closing.enter close (inv.parked_at_close close) consumer)
        · exact Or.inl (inv.consumer_step close consumer)
      · obtain ⟨i, inside, step⟩ := List.mem_flatMap.mp producer
        exact Or.inl (inv.producer_step (List.mem_range.mp inside) step)
    · exact Or.inr (closing.step rfl mem)

/-- The `observing` protocol never loses a wake: for every number of producers and releases,
whether or not the consumer closes, no reachable state is lost. -/
theorem observing_never_lost {n releases : Nat} {closes : Bool} {s : State}
    (reach : Reach .observing (State.init n releases closes) s) : ¬ s.Lost .observing :=
  reach.observing_phase.elim State.Inv.not_lost State.Closing.not_lost

/-- The `repaired` protocol never loses a wake: for every number of producers and releases,
whether or not the consumer closes, no reachable state is lost. -/
theorem repaired_never_lost {n releases : Nat} {closes : Bool} {s : State}
    (reach : Reach .repaired (State.init n releases closes) s) : ¬ s.Lost .repaired :=
  reach.repaired_phase.elim State.Repaired.not_lost State.Closing.not_lost

end Park

end Qtz
