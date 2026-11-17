import Qtz.ParkObserving

/-!
The `repaired` protocol (`WakeSet::stand_down`, used by the mpsc, spmc and mpmc producers) never
loses a wake while the channel is open, for any number of producers and any number of releases.
`ParkClose` covers the rest of the run.

The count is the one `ParkObserving` keeps, with one more debt. A producer whose re-check took a
slot, and whose bit a waker then retired, owes the wake it was given: it pays it by running the wake
loop itself in `stand_down`. That forwarded loop owes a pick exactly as the consumer's loop does, so
the count is unchanged by where a waker runs.

Wakers now run in the consumer and in any number of producers, so "a waker is about to claim this
producer's handle" is `State.Claims`, which looks at all of them.
-/

namespace Qtz

namespace Park

/-- Some waker, the consumer's or one a producer forwards, is about to claim `i`'s handle. -/
def State.Claims (s : State) (i : Nat) : Prop :=
  s.consumer = .waking (.claim i) ∨ ∃ k, (Producer.at s.producers k).pc = .forward (.claim i)

/-- What `repaired` keeps true of each producer, given whether a waker is about to claim its
handle. A set bit only stands while the producer waits or disarms; a waiting producer without a
token still has its handle; and one whose bit is retired without a token is about to be claimed. -/
def Producer.Fit (p : Producer) (claimed : Prop) : Prop :=
  match p.pc with
  | .attempt | .handle | .done | .forward _ => p.bit = false
  | .bit => p.bit = false ∧ (p.token = true ∨ p.handle = true)
  | .recheck | .park =>
    (p.token = true ∨ p.handle = true) ∧ (p.bit = true ∨ p.token = true ∨ claimed)
  | .disarmWoken | .disarmWon => True
  | .disarmClosed => False

structure State.Repaired (s : State) : Prop where
  open_ring : s.closed = false
  consumer_open : s.consumer = .release ∨ s.consumer = .finished ∨ s.consumer = .close ∨
    ∃ w, s.consumer = .waking w
  fit : ∀ i, (Producer.at s.producers i).Fit (s.Claims i)
  owed : s.Sleeper → s.free ≤ Producer.owedSum s.producers + s.consumer.pending

/-- Where a forwarding producer goes once its wake loop takes a step. -/
def Waker.resume : Option Waker → Pc
  | some w => .forward w
  | none => .done

def Waker.owedAfter : Option Waker → Nat
  | some w => w.owed
  | none => 0

theorem Consumer.pending_waking (w : Waker) : (Consumer.waking w).pending = w.owed := by
  cases w <;> rfl

theorem Consumer.afterRelease_pending (closes : Bool) (left : Nat) :
    (Consumer.afterRelease closes left).pending = 0 := by
  unfold Consumer.afterRelease
  split <;> (try split) <;> rfl

theorem Producer.inside {ps : List Producer} {i : Nat} (moved : Producer.at ps i ≠ Producer.fresh) :
    i < ps.length := by
  by_cases inside : i < ps.length
  · exact inside
  · exact absurd (Producer.at_beyond inside) moved

theorem Producer.Fit.mono {p : Producer} {claimed wider : Prop} (fit : p.Fit claimed)
    (widen : claimed → wider) : p.Fit wider := by
  rcases p with ⟨pc, bit, handle, token⟩
  cases pc <;> simp only [Producer.Fit] at fit ⊢ <;>
    first | exact fit | exact ⟨fit.1, fit.2.imp_right (Or.imp_right widen)⟩

theorem Producer.Fit.retire {p : Producer} {claimed : Prop} (fit : p.Fit claimed)
    (bit : p.bit = true) :
    p.owed = 0 ∧ ({ p with bit := false } : Producer).owed = 1 ∧
      ({ p with bit := false } : Producer).Fit True := by
  rcases p with ⟨pc, bitField, handle, token⟩
  cases pc <;> simp_all [Producer.Fit, Producer.owed]

theorem Producer.Fit.claim {p : Producer} {claimed : Prop} (fit : p.Fit claimed) :
    ({ p with handle := false, token := true } : Producer).Fit False := by
  rcases p with ⟨pc, bit, handle, token⟩
  cases pc <;> simp_all [Producer.Fit]

theorem Producer.Fit.unclaimed {p : Producer} {claimed : Prop} (fit : p.Fit claimed)
    (handle : p.handle = false) : p.Fit False := by
  rcases p with ⟨pc, bit, handleField, token⟩
  cases pc <;> simp_all [Producer.Fit]

theorem Producer.Fit.waiting {p : Producer} (fit : p.Fit False) (park : p.pc = .park)
    (token : p.token = false) : p.bit = true ∧ p.owed = 0 := by
  rcases p with ⟨pc, bit, handle, tokenField⟩
  subst park
  cases bit <;> simp_all [Producer.Fit, Producer.owed]

/-- A wake-loop step keeps every program counter and only clears bits. -/
theorem Waker.step_frame {ps ps' : List Producer} {w : Waker} {next : Option Waker}
    (mem : (ps', next) ∈ Waker.step ps w) :
    ps'.length = ps.length ∧
      ∀ j, (Producer.at ps' j).pc = (Producer.at ps j).pc ∧
        ((Producer.at ps' j).bit = true → (Producer.at ps j).bit = true) := by
  cases w with
  | scan =>
    simp only [Waker.step] at mem
    split at mem
    · simp only [List.mem_singleton, Prod.mk.injEq] at mem
      obtain ⟨rfl, rfl⟩ := mem
      exact ⟨rfl, fun _ => ⟨rfl, id⟩⟩
    · simp only [List.mem_map, Prod.mk.injEq] at mem
      obtain ⟨_, _, rfl, rfl⟩ := mem
      exact ⟨rfl, fun _ => ⟨rfl, id⟩⟩
  | clear k =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · refine ⟨by simp [Producer.update], fun j => ?_⟩
      rw [Producer.at_update]
      split
      · rename_i same
        rw [same.1]
        simp
      · exact ⟨rfl, id⟩
    · exact ⟨rfl, fun _ => ⟨rfl, id⟩⟩
  | claim k =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · refine ⟨by simp [Producer.update], fun j => ?_⟩
      rw [Producer.at_update]
      split
      · rename_i same
        rw [same.1]
        simp
      · exact ⟨rfl, id⟩
    · exact ⟨rfl, fun _ => ⟨rfl, id⟩⟩

/-- A wake-loop step never lowers what is owed, counting the loop's own pick, unless it is a scan
that found no bit set. -/
theorem Waker.step_owed {ps ps' : List Producer} {w : Waker} {next : Option Waker}
    {claimed : Nat → Prop} (fits : ∀ j, (Producer.at ps j).Fit (claimed j))
    (mem : (ps', next) ∈ Waker.step ps w) :
    Producer.owedSum ps + w.owed ≤ Producer.owedSum ps' + Waker.owedAfter next ∨
      ∀ j, (Producer.at ps j).bit = false := by
  cases w with
  | scan =>
    simp only [Waker.step] at mem
    split at mem
    · rename_i none
      refine Or.inr fun j => ?_
      cases bit : (Producer.at ps j).bit
      · rfl
      · have inside : j < ps.length :=
          Producer.inside fun fresh => by simp [fresh, Producer.fresh] at bit
        have picked : j ∈ (List.range ps.length).filter (fun i => (Producer.at ps i).bit) :=
          List.mem_filter.mpr ⟨List.mem_range.mpr inside, bit⟩
        simp [none] at picked
    · simp only [List.mem_map, Prod.mk.injEq] at mem
      obtain ⟨_, _, rfl, rfl⟩ := mem
      exact Or.inl (by simp [Waker.owed, Waker.owedAfter])
  | clear k =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rename_i bit
      have inside : k < ps.length :=
        Producer.inside fun fresh => by simp [fresh, Producer.fresh] at bit
      obtain ⟨before, after, _⟩ := (fits k).retire bit
      have total := Producer.owedSum_update ps k (fun p => { p with bit := false }) inside
      refine Or.inl ?_
      simp only [Waker.owed, Waker.owedAfter]
      omega
    · exact Or.inl (by simp [Waker.owed, Waker.owedAfter])
  | claim k =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rename_i handle
      have inside : k < ps.length :=
        Producer.inside fun fresh => by simp [fresh, Producer.fresh] at handle
      have total := Producer.owedSum_update_same ps k
        (fun p => { p with handle := false, token := true }) inside rfl
      exact Or.inl (by simp [Waker.owed, Waker.owedAfter, total])
    · exact Or.inl (by simp [Waker.owed, Waker.owedAfter])

/-- A wake-loop step keeps every producer fit, moving the claim it holds from `w` to `next`.
`other` is what every other waker claims. -/
theorem Waker.step_fit {ps ps' : List Producer} {w : Waker} {next : Option Waker}
    {other : Nat → Prop} (fits : ∀ j, (Producer.at ps j).Fit (other j ∨ w = .claim j))
    (mem : (ps', next) ∈ Waker.step ps w) (j : Nat) :
    (Producer.at ps' j).Fit (other j ∨ next = some (.claim j)) := by
  have unclaimed : ∀ {k}, w ≠ .claim k →
      (Producer.at ps k).Fit (other k ∨ next = some (.claim k)) :=
    fun different => (fits _).mono (Or.imp_right fun same => absurd same different)
  cases w with
  | scan =>
    simp only [Waker.step] at mem
    split at mem
    · simp only [List.mem_singleton, Prod.mk.injEq] at mem
      obtain ⟨rfl, rfl⟩ := mem
      exact unclaimed (by simp)
    · simp only [List.mem_map, Prod.mk.injEq] at mem
      obtain ⟨_, _, rfl, rfl⟩ := mem
      exact unclaimed (by simp)
  | clear k =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rename_i bit
      rw [Producer.at_update]
      split
      · rename_i same
        rw [same.1]
        exact ((fits k).retire bit).2.2.mono fun _ => Or.inr rfl
      · exact unclaimed (by simp)
    · exact unclaimed (by simp)
  | claim k =>
    have away : j ≠ k → (Producer.at ps j).Fit (other j ∨ next = some (.claim j)) :=
      fun different => (fits j).mono
        (Or.imp_right fun same => absurd (Waker.claim.inj same).symm different)
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rename_i handle
      rw [Producer.at_update]
      split
      · rename_i same
        rw [same.1]
        exact (fits k).claim.mono False.elim
      · rename_i outside
        by_cases same : j = k
        · have inside : k < ps.length :=
            Producer.inside fun fresh => by simp [fresh, Producer.fresh] at handle
          exact absurd ⟨same, inside⟩ outside
        · exact away same
    · rename_i handle
      by_cases same : j = k
      · subst same
        exact ((fits j).unclaimed (by simpa using handle)).mono False.elim
      · exact away same

/-- A producer step: producer `i` changes by `f`, which neither starts nor ends a claim. -/
theorem State.Repaired.move {s t : State} (inv : s.Repaired) {i : Nat}
    (inside : i < s.producers.length) {f : Producer → Producer}
    (producers : t.producers = Producer.update s.producers i f)
    (consumer : t.consumer = s.consumer) (closed : t.closed = s.closed)
    (keeps : ∀ k, (Producer.at s.producers i).pc ≠ .forward (.claim k))
    (fit : (f (Producer.at s.producers i)).Fit (s.Claims i))
    (bound : t.free = 0 ∨
      (t.free + (Producer.at s.producers i).owed ≤ s.free + (f (Producer.at s.producers i)).owed ∧
        ((f (Producer.at s.producers i)).pc = .park → (f (Producer.at s.producers i)).bit = false))) :
    t.Repaired where
  open_ring := closed ▸ inv.open_ring
  consumer_open := consumer ▸ inv.consumer_open
  fit j := by
    have grows : s.Claims j → t.Claims j := by
      rintro (waking | ⟨k, claim⟩)
      · exact Or.inl (consumer ▸ waking)
      · refine Or.inr ⟨k, ?_⟩
        rw [producers, Producer.at_update, ite_eq_right_of_eq_false _ _
          (eq_false fun same => keeps j (by rw [← same.1]; exact claim))]
        exact claim
    rw [producers, Producer.at_update]
    split
    · rename_i same
      obtain ⟨rfl, _⟩ := same
      exact fit.mono grows
    · exact (inv.fit j).mono grows
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

/-- A step of the consumer's wake loop. -/
theorem State.Repaired.consumer_waker {s : State} (inv : s.Repaired) {w : Waker}
    (consumer : s.consumer = .waking w) {ps' : List Producer} {next : Option Waker}
    (step : (ps', next) ∈ Waker.step s.producers w) :
    (match next with
      | some w' => { s with producers := ps', consumer := .waking w' }
      | none => { s with producers := ps', consumer := Consumer.afterRelease s.closes s.left } :
        State).Repaired := by
  obtain ⟨_, frame⟩ := Waker.step_frame step
  have after := Waker.step_fit
    (other := fun j => ∃ k, (Producer.at s.producers k).pc = .forward (.claim j))
    (fun j => (inv.fit j).mono fun claims => claims.elim
      (fun waking => Or.inr (Consumer.waking.inj (consumer.symm.trans waking))) Or.inl) step
  have moved : ∀ j, (∃ k, (Producer.at s.producers k).pc = .forward (.claim j)) →
      ∃ k, (Producer.at ps' k).pc = .forward (.claim j) :=
    fun _ ⟨k, claim⟩ => ⟨k, (frame k).1.trans claim⟩
  have bound : (∃ j, (Producer.at ps' j).pc = .park ∧ (Producer.at ps' j).bit = true) →
      s.free ≤ Producer.owedSum ps' + Waker.owedAfter next := fun ⟨j, park, bit⟩ => by
    have previous := inv.owed ⟨j, (frame j).1.symm.trans park, (frame j).2 bit⟩
    rw [consumer, Consumer.pending_waking] at previous
    rcases Waker.step_owed inv.fit step with grows | quiet
    · omega
    · exact absurd ((frame j).2 bit) (by simp [quiet j])
  cases next with
  | none =>
    exact {
      open_ring := inv.open_ring
      consumer_open := Consumer.afterRelease_open s.closes s.left
      fit := fun j => (after j).mono fun claims =>
        claims.elim (fun other => Or.inr (moved j other)) (fun resumes => by cases resumes)
      owed := fun sleeper => by
        have := bound sleeper
        show s.free ≤ _ + (Consumer.afterRelease s.closes s.left).pending
        rw [Consumer.afterRelease_pending]
        exact this }
  | some w' =>
    exact {
      open_ring := inv.open_ring
      consumer_open := Or.inr (Or.inr (Or.inr ⟨w', rfl⟩))
      fit := fun j => (after j).mono fun claims =>
        claims.elim (fun other => Or.inr (moved j other))
          (fun resumes => Or.inl (congrArg Consumer.waking (Option.some.inj resumes)))
      owed := fun sleeper => by
        have := bound sleeper
        show s.free ≤ _ + (Consumer.waking w').pending
        rw [Consumer.pending_waking]
        exact this }

/-- A step of the wake loop producer `i` forwards from `stand_down`. -/
theorem State.Repaired.forward_step {s : State} (inv : s.Repaired) {i : Nat}
    (inside : i < s.producers.length) {w : Waker}
    (pc : (Producer.at s.producers i).pc = .forward w) {ps' : List Producer}
    {next : Option Waker} (step : (ps', next) ∈ Waker.step s.producers w) :
    ({ s with producers := Producer.update ps' i fun p => { p with pc := Waker.resume next } } :
      State).Repaired := by
  obtain ⟨length, frame⟩ := Waker.step_frame step
  have insideAfter : i < ps'.length := by omega
  have quietSelf : (Producer.at s.producers i).bit = false := by
    have fit := inv.fit i
    simpa [Producer.Fit, pc] using fit
  have after := Waker.step_fit (other := fun j => s.consumer = .waking (.claim j) ∨
      ∃ k, k ≠ i ∧ (Producer.at s.producers k).pc = .forward (.claim j))
    (fun j => (inv.fit j).mono fun claims => by
      rcases claims with waking | ⟨k, claim⟩
      · exact Or.inl (Or.inl waking)
      · by_cases same : k = i
        · subst same
          exact Or.inr (Pc.forward.inj (pc.symm.trans claim))
        · exact Or.inl (Or.inr ⟨k, same, claim⟩)) step
  exact {
    open_ring := inv.open_ring
    consumer_open := inv.consumer_open
    fit := fun j => by
      show (Producer.at (Producer.update ps' i fun p => { p with pc := Waker.resume next }) j).Fit _
      rw [Producer.at_update]
      split
      · have quiet : (Producer.at ps' i).bit = false := by
          cases bit : (Producer.at ps' i).bit
          · rfl
          · rw [(frame i).2 bit] at quietSelf
            cases quietSelf
        cases next <;> simp [Producer.Fit, Waker.resume, quiet]
      · refine (after j).mono fun claims => ?_
        rcases claims with (waking | ⟨k, different, claim⟩) | resumes
        · exact Or.inl waking
        · refine Or.inr ⟨k, ?_⟩
          show (Producer.at (Producer.update ps' i fun p => { p with pc := Waker.resume next })
            k).pc = _
          rw [Producer.at_update, ite_eq_right_of_eq_false _ _ (eq_false fun same => different same.1)]
          exact (frame k).1.trans claim
        · refine Or.inr ⟨i, ?_⟩
          show (Producer.at (Producer.update ps' i fun p => { p with pc := Waker.resume next })
            i).pc = _
          rw [Producer.at_update, ite_eq_left_of_eq_true _ _ (eq_true ⟨rfl, insideAfter⟩), resumes]
          rfl
    owed := fun ⟨j, park, bit⟩ => by
      change (Producer.at (Producer.update ps' i fun p => { p with pc := Waker.resume next })
        j).pc = .park at park
      change (Producer.at (Producer.update ps' i fun p => { p with pc := Waker.resume next })
        j).bit = true at bit
      rw [Producer.at_update] at park bit
      have before : s.Sleeper := by
        split at park
        · cases next <;> simp [Waker.resume] at park
        · rename_i other
          rw [ite_eq_right_of_eq_false _ _ (eq_false other)] at bit
          exact ⟨j, (frame j).1.symm.trans park, (frame j).2 bit⟩
      have previous := inv.owed before
      have total := Producer.owedSum_update ps' i
        (fun p => { p with pc := Waker.resume next }) insideAfter
      have selfOwed : (Producer.at ps' i).owed = w.owed := by
        simp [Producer.owed, (frame i).1, pc]
      have resumed : ∀ p : Producer, ({ p with pc := Waker.resume next } : Producer).owed =
          Waker.owedAfter next := by
        intro p
        cases next <;> simp [Producer.owed, Waker.resume, Waker.owedAfter]
      rw [resumed] at total
      show s.free ≤ Producer.owedSum (Producer.update ps' i fun p =>
        { p with pc := Waker.resume next }) + s.consumer.pending
      rcases Waker.step_owed inv.fit step with grows | quiet
      · omega
      · obtain ⟨k, _, bitBefore⟩ := before
        simp [quiet k] at bitBefore }

theorem State.Repaired.producer_step {s t : State} (inv : s.Repaired) {i : Nat}
    (inside : i < s.producers.length) (mem : t ∈ s.producerSteps .repaired i) : t.Repaired := by
  have fit := inv.fit i
  unfold State.producerSteps at mem
  have forwarded := mem
  simp only [inv.open_ring, Bool.false_and, Bool.false_eq_true, ite_false, Variant.standsDown,
    Bool.true_and] at mem
  cases pcEq : (Producer.at s.producers i).pc with
  | attempt =>
    simp only [pcEq] at mem
    split at mem
    · rename_i free
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .done }) inside rfl rfl rfl
        (by simp [pcEq]) (by simp_all [Producer.Fit])
        (Or.inr ⟨by simp [Producer.owed, pcEq]; omega, by simp⟩)
    · rename_i full
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .handle }) inside rfl rfl rfl
        (by simp [pcEq]) (by simp_all [Producer.Fit]) (Or.inl (by simp [State.setPc]; omega))
  | handle =>
    simp only [pcEq, List.mem_singleton] at mem
    subst mem
    exact inv.move (f := fun p => { p with handle := true, pc := .bit }) inside rfl rfl rfl
      (by simp [pcEq]) (by simp_all [Producer.Fit])
      (Or.inr ⟨by simp [Producer.owed, pcEq, State.edit], by simp⟩)
  | bit =>
    simp only [pcEq, List.mem_singleton] at mem
    subst mem
    exact inv.move (f := fun p => { p with bit := true, pc := .recheck }) inside rfl rfl rfl
      (by simp [pcEq]) (by simp_all [Producer.Fit])
      (Or.inr ⟨by simp [Producer.owed, pcEq, State.edit], by simp⟩)
  | recheck =>
    simp only [pcEq] at mem
    split at mem
    · rename_i free
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .disarmWon }) inside rfl rfl rfl
        (by simp [pcEq]) (by simp_all [Producer.Fit])
        (Or.inr ⟨by simp only [Producer.owed, pcEq]; split <;> omega, by simp⟩)
    · rename_i full
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .park }) inside rfl rfl rfl
        (by simp [pcEq]) (by simp_all [Producer.Fit]) (Or.inl (by simp [State.setPc]; omega))
  | park =>
    simp only [pcEq] at mem
    split at mem
    · simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with token := false, pc := .disarmWoken }) inside
        rfl rfl rfl (by simp [pcEq]) (by simp [Producer.Fit])
        (Or.inr ⟨by simp [Producer.owed, pcEq, State.edit], by simp⟩)
    · simp at mem
  | disarmWon =>
    simp only [pcEq] at mem
    split at mem
    · rename_i retired
      have quiet : (Producer.at s.producers i).bit = false := by simpa using retired
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with pc := .forward .scan }) inside rfl rfl rfl
        (by simp [pcEq]) (by simp [Producer.Fit, quiet])
        (Or.inr ⟨by simp [Producer.owed, pcEq, quiet, State.edit, Waker.owed], by simp⟩)
    · rename_i standing
      have set : (Producer.at s.producers i).bit = true := by simpa using standing
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move (f := fun p => { p with bit := false, pc := .done }) inside rfl rfl rfl
        (by simp [pcEq]) (by simp [Producer.Fit])
        (Or.inr ⟨by simp [Producer.owed, pcEq, set, State.edit], by simp⟩)
  | disarmWoken =>
    simp only [pcEq, List.mem_singleton] at mem
    subst mem
    exact inv.move (f := fun p => { p with bit := false, pc := .attempt }) inside rfl rfl rfl
      (by simp [pcEq]) (by simp [Producer.Fit])
      (Or.inr ⟨by simp only [Producer.owed, pcEq, State.edit]; split <;> omega, by simp⟩)
  | disarmClosed => simp [Producer.Fit, pcEq] at fit
  | forward w =>
    simp only [pcEq, List.mem_map] at forwarded
    obtain ⟨⟨ps', next⟩, step, rfl⟩ := forwarded
    cases next <;> exact inv.forward_step inside pcEq step
  | done => simp [pcEq] at mem

theorem State.Repaired.consumer_step {s t : State} (inv : s.Repaired)
    (notClose : s.consumer ≠ .close) (mem : t ∈ s.consumerSteps) : t.Repaired := by
  rcases inv.consumer_open with consumer | consumer | consumer | ⟨w, consumer⟩
  · simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
    subst mem
    exact {
      open_ring := inv.open_ring
      consumer_open := Or.inr (Or.inr (Or.inr ⟨.scan, rfl⟩))
      fit := fun j => (inv.fit j).mono fun claims => claims.elim
        (fun waking => by rw [consumer] at waking; cases waking) Or.inr
      owed := fun sleeper => by
        have := inv.owed sleeper
        simp only [consumer, Consumer.pending] at this ⊢
        omega }
  · simp [State.consumerSteps, consumer] at mem
  · exact absurd consumer notClose
  · simp only [State.consumerSteps, consumer, List.mem_map] at mem
    obtain ⟨⟨ps', next⟩, step, rfl⟩ := mem
    cases next <;> exact inv.consumer_waker consumer step

/-- Every step but the consumer's `close` keeps the invariant; `ParkClose` takes over from there. -/
theorem State.Repaired.step {s t : State} (inv : s.Repaired) (notClose : s.consumer ≠ .close)
    (mem : t ∈ s.succs .repaired) : t.Repaired := by
  unfold State.succs at mem
  rcases List.mem_append.mp mem with consumer | producer
  · exact inv.consumer_step notClose consumer
  · obtain ⟨i, inside, step⟩ := List.mem_flatMap.mp producer
    exact inv.producer_step (List.mem_range.mp inside) step

theorem State.Repaired.init (n releases : Nat) (closes : Bool) :
    (State.init n releases closes).Repaired where
  open_ring := rfl
  consumer_open := Consumer.afterRelease_open closes releases
  fit i := by
    simp only [State.init, Producer.at, List.getD_eq_getElem?_getD, List.getElem?_replicate]
    split <;> simp [Producer.Fit, Producer.fresh]
  owed _ := by simp [State.init]

/-- A state that keeps the invariant is not lost. -/
theorem State.Repaired.not_lost {s : State} (inv : s.Repaired) : ¬ s.Lost .repaired := by
  intro ⟨empty, freeOrClosed, ⟨p, member, unfinished⟩⟩
  unfold State.succs at empty
  obtain ⟨consumerIdle, producersIdle⟩ := List.append_eq_nil_iff.mp empty
  have finished : s.consumer = .finished := by
    rcases inv.consumer_open with consumer | consumer | consumer | ⟨w, consumer⟩
    · simp [State.consumerSteps, consumer] at consumerIdle
    · exact consumer
    · simp [State.consumerSteps, consumer] at consumerIdle
    · simp only [State.consumerSteps, consumer, List.map_eq_nil_iff] at consumerIdle
      exact absurd consumerIdle (Waker.step_ne_nil _ _)
  have idle : ∀ i, i < s.producers.length →
      (Producer.at s.producers i).pc = .done ∨
        ((Producer.at s.producers i).pc = .park ∧ (Producer.at s.producers i).token = false) := by
    intro i inside
    have stuck := (List.flatMap_eq_nil_iff.mp producersIdle) i (List.mem_range.mpr inside)
    have fit := inv.fit i
    unfold State.producerSteps at stuck
    simp only [inv.open_ring, Bool.false_and, Bool.false_eq_true, ite_false, Variant.standsDown,
      Bool.true_and] at stuck
    cases pcEq : (Producer.at s.producers i).pc with
    | park =>
      simp only [pcEq] at stuck
      split at stuck
      · simp at stuck
      · rename_i token
        exact Or.inr ⟨rfl, by simpa using token⟩
    | disarmClosed => simp [Producer.Fit, pcEq] at fit
    | forward w =>
      simp only [pcEq, List.map_eq_nil_iff] at stuck
      exact absurd stuck (Waker.step_ne_nil _ _)
    | done => exact Or.inl rfl
    | _ => simp only [pcEq] at stuck; (try split at stuck) <;> simp at stuck
  have unclaimed : ∀ i, ¬ s.Claims i := by
    rintro i (waking | ⟨k, claim⟩)
    · rw [finished] at waking
      cases waking
    · have inside : k < s.producers.length :=
        Producer.inside fun fresh => by simp [fresh, Producer.fresh] at claim
      rcases idle k inside with done | ⟨park, _⟩
      · rw [done] at claim
        cases claim
      · rw [park] at claim
        cases claim
  have fits : ∀ i, (Producer.at s.producers i).Fit False :=
    fun i => (inv.fit i).mono (unclaimed i)
  have nothingOwed : Producer.owedSum s.producers = 0 := by
    refine Producer.owedSum_eq_zero fun q qMember => ?_
    obtain ⟨i, inside, rfl⟩ := List.getElem_of_mem qMember
    rw [← Producer.at_of_lt inside]
    rcases idle i inside with done | ⟨park, token⟩
    · simp [Producer.owed, done]
    · exact ((fits i).waiting park token).2
  obtain ⟨i, inside, rfl⟩ := List.getElem_of_mem member
  rw [← Producer.at_of_lt inside] at unfinished
  rcases idle i inside with done | ⟨park, token⟩
  · exact unfinished done
  have bit := ((fits i).waiting park token).1
  have bound := inv.owed ⟨i, park, bit⟩
  simp only [nothingOwed, finished, Consumer.pending] at bound
  rcases freeOrClosed with free | closed
  · omega
  · simp [inv.open_ring] at closed

end Park

end Qtz
