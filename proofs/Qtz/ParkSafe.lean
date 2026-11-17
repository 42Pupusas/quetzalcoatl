import Qtz.ParkLedger

/-!
The park/wake handshake never loses a wake. This holds for broadcast's `push_block` (`observing`),
for the mpsc and spmc `stand_down` (`repaired`) and for mpmc's `stand_down_waking_all`
(`repairedWakingAll`). It holds for any number of producers, each leased a park slot or not, for any
number of releases in batches of any size, whether or not the consumer closes, and with spurious
returns from `park`.

The invariant has an open half and a closing half.

While the channel is open, `ParkLedger`'s count bounds the free slots. On each route, if some
producer sleeps where a peer can find it by that route, the free slots never outnumber what is owed
on that route.

Once the channel closes, no producer can start to park, because its re-check sees `closed`. The
consumer's drain of the overflow stack and its flush of the bitmap take every producer already
parked, unless it holds a token. A forwarded wake loop can still be running, but it can always take
a step, so it never leaves the run stuck.

What every producer keeps, open or closed, is `Producer.Fit`: a parked producer will be woken.
-/

namespace Qtz

namespace Park

/-- The protocols proved here. `unrepaired` and `ignoresClose` lose a wake (`ParkModel`). -/
def Variant.Proved : Variant → Prop
  | .observing | .repaired | .repairedWakingAll => True
  | _ => False

def Consumer.Open : Consumer → Prop
  | .release | .waking _ | .close | .finished => True
  | _ => False

def Consumer.Closing : Consumer → Prop
  | .drainOverflow | .unparking _ | .flush | .flushing _ | .finished => True
  | _ => False

/-- Past the drain of the overflow stack. -/
def Consumer.Drained : Consumer → Prop
  | .unparking _ | .flush | .flushing _ | .finished => True
  | _ => False

/-- Past the flush of the bitmap. -/
def Consumer.Flushed : Consumer → Prop
  | .flushing _ | .finished => True
  | _ => False

/-- The consumer is about to deliver a wake to producer `j` by `route`. -/
def Consumer.Delivers : Consumer → Route → Nat → Prop
  | .waking w, route, j => w.Delivers route j
  | .unparking pending, .overflow, j => j ∈ pending
  | .flushing pending, .bitmap, j => j ∈ pending
  | _, _, _ => False

def Consumer.pending (route : Route) : Consumer → Nat
  | .waking w => w.owed route
  | _ => 0

/-- A producer's forwarded wake loop is about to deliver a wake to `j` by `route`. -/
def Producer.Forwards (ps : List Producer) (j : Nat) (route : Route) : Prop :=
  ∃ k w, (Producer.at ps k).pc = .forward w ∧ w.Delivers route j

def State.Delivers (s : State) (j : Nat) (route : Route) : Prop :=
  s.consumer.Delivers route j ∨ Producer.Forwards s.producers j route

/-- Some producer is parked where a peer can find it by `route`. -/
def State.Sleeper (s : State) (route : Route) : Prop :=
  ∃ j, (Producer.at s.producers j).pc = .park ∧ (Producer.at s.producers j).visible route = true

structure State.Safe (variant : Variant) (s : State) : Prop where
  opened : s.closed = false → s.consumer.Open
  closing : s.closed = true → s.consumer.Closing
  releasing : s.consumer = .release → 0 < s.left
  won : ∀ j, (Producer.at s.producers j).pc = .disarmWon → variant.standsDown = true
  exits : ∀ j, (Producer.at s.producers j).pc = .disarmClosed → s.closed = true
  fit : ∀ j, (Producer.at s.producers j).Fit (s.Delivers j)
  owed : ∀ route, s.closed = false → s.Sleeper route →
    s.free ≤ Producer.owedSum route s.producers + s.consumer.pending route
  drained : s.closed = true → s.consumer.Drained → ∀ j,
    (Producer.at s.producers j).pc = .park → (Producer.at s.producers j).registered = false
  flushed : s.closed = true → s.consumer.Flushed → ∀ j,
    (Producer.at s.producers j).pc = .park → (Producer.at s.producers j).bit = false

theorem Variant.Proved.sees {variant : Variant} (proved : variant.Proved) :
    variant.seesClose = true := by
  cases variant <;> first | rfl | exact nomatch proved

theorem Variant.forwardCount_pos (variant : Variant) : 1 ≤ variant.forwardCount := by
  cases variant <;> simp [Variant.forwardCount]

theorem Consumer.afterRelease_open (closes : Bool) (left : Nat) :
    (Consumer.afterRelease closes left).Open := by
  unfold Consumer.afterRelease
  split <;> (try split) <;> trivial

theorem Consumer.afterRelease_pending (route : Route) (closes : Bool) (left : Nat) :
    (Consumer.afterRelease closes left).pending route = 0 := by
  unfold Consumer.afterRelease
  split <;> (try split) <;> rfl

theorem Consumer.afterRelease_releasing {closes : Bool} {left : Nat}
    (release : Consumer.afterRelease closes left = .release) : 0 < left := by
  unfold Consumer.afterRelease at release
  by_cases zero : left = 0
  · cases closes <;> simp [zero] at release
  · omega

theorem State.Safe.open_of {variant : Variant} {s : State} (inv : s.Safe variant)
    (notClosing : ¬ s.consumer.Closing) : s.closed = false := by
  cases closed : s.closed
  · rfl
  · exact absurd (inv.closing closed) notClosing

theorem State.Safe.closed_of {variant : Variant} {s : State} (inv : s.Safe variant)
    (notOpen : ¬ s.consumer.Open) : s.closed = true := by
  cases closed : s.closed
  · exact absurd (inv.opened closed) notOpen
  · rfl

theorem Producer.Forwards.frame {ps ps' : List Producer} (frame : Producer.Frame ps ps')
    {j : Nat} {route : Route} : Producer.Forwards ps j route → Producer.Forwards ps' j route :=
  fun ⟨k, w, pc, delivers⟩ => ⟨k, w, (frame.2 k).1.trans pc, delivers⟩

theorem Producer.Frame.back {ps ps' : List Producer} (frame : Producer.Frame ps ps') (j : Nat)
    (park : (Producer.at ps' j).pc = .park) :
    (Producer.at ps j).pc = .park ∧ ∀ route,
      (Producer.at ps' j).visible route = true → (Producer.at ps j).visible route = true :=
  ⟨(frame.2 j).1.symm.trans park, (frame.2 j).2⟩

/-- Every producer is fit after `update`, given that the updated one is. -/
theorem Producer.Fit.update {ps : List Producer} {i : Nat} {f : Producer → Producer}
    {delivered : Nat → Route → Prop} (j : Nat)
    (self : i < ps.length → (f (Producer.at ps i)).Fit (delivered i))
    (other : j ≠ i → (Producer.at ps j).Fit (delivered j)) :
    (Producer.at (Producer.update ps i f) j).Fit (delivered j) := by
  rw [Producer.at_update]
  split
  · rename_i same
    rw [same.1]
    exact self same.2
  · rename_i outside
    by_cases same : j = i
    · subst same
      rw [Producer.at_beyond fun inside => outside ⟨rfl, inside⟩]
      exact Producer.fresh_fit _
    · exact other same

/-- The closing half survives a step that parks nobody new once closed and makes no parked
producer visible, with the consumer no earlier in its closing sequence. -/
theorem State.Safe.closing_keep {variant : Variant} {s t : State} (inv : s.Safe variant)
    (closed : t.closed = s.closed)
    (back : t.closed = true → ∀ j, (Producer.at t.producers j).pc = .park →
      (Producer.at s.producers j).pc = .park ∧ ∀ route,
        (Producer.at t.producers j).visible route = true →
          (Producer.at s.producers j).visible route = true)
    (drained : t.consumer.Drained →
      s.consumer.Drained ∨ ∀ j, (Producer.at t.producers j).registered = false)
    (flushed : t.consumer.Flushed →
      s.consumer.Flushed ∨ ∀ j, (Producer.at t.producers j).bit = false) :
    (t.closed = true → t.consumer.Drained → ∀ j,
      (Producer.at t.producers j).pc = .park → (Producer.at t.producers j).registered = false) ∧
    (t.closed = true → t.consumer.Flushed → ∀ j,
      (Producer.at t.producers j).pc = .park → (Producer.at t.producers j).bit = false) := by
  refine ⟨fun c d j park => ?_, fun c f j park => ?_⟩
  · rcases drained d with before | none
    · cases registered : (Producer.at t.producers j).registered
      · rfl
      · obtain ⟨parked, visible⟩ := back c j park
        have := inv.drained (closed ▸ c) before j parked
        have seen : (Producer.at s.producers j).registered = true := visible .overflow registered
        rw [seen] at this
        cases this
    · exact none j
  · rcases flushed f with before | none
    · cases bit : (Producer.at t.producers j).bit
      · rfl
      · obtain ⟨parked, visible⟩ := back c j park
        have := inv.flushed (closed ▸ c) before j parked
        have seen : (Producer.at s.producers j).bit = true := visible .bitmap bit
        rw [seen] at this
        cases this
    · exact none j

/-- A consumer step of the closing sequence: the channel is closed, and the step only takes bits
and registrations away. -/
theorem State.Safe.closing_step {variant : Variant} {s t : State} (inv : s.Safe variant)
    (frame : Producer.Frame s.producers t.producers) (sClosed : s.closed = true)
    (closed : t.closed = true) (consumer : t.consumer.Closing)
    (fit : ∀ j, (Producer.at t.producers j).Fit (t.Delivers j))
    (drained : t.consumer.Drained →
      s.consumer.Drained ∨ ∀ j, (Producer.at t.producers j).registered = false)
    (flushed : t.consumer.Flushed →
      s.consumer.Flushed ∨ ∀ j, (Producer.at t.producers j).bit = false) :
    t.Safe variant :=
  have keep := inv.closing_keep (closed.trans sClosed.symm)
    (fun _ j park => frame.back j park) drained flushed
  { opened := fun c => nomatch closed.symm.trans c
    closing := fun _ => consumer
    releasing := fun release => by rw [release] at consumer; exact nomatch consumer
    won := fun j disarm => inv.won j ((frame.2 j).1.symm.trans disarm)
    exits := fun _ _ => closed
    fit
    owed := fun _ c => nomatch closed.symm.trans c
    drained := keep.1
    flushed := keep.2 }

/-- A producer step: producer `i` changes from `p` by `f`, which neither starts nor runs a wake
loop. -/
theorem State.Safe.move {variant : Variant} {s t : State} (inv : s.Safe variant) {i : Nat}
    (inside : i < s.producers.length) {p : Producer} (current : Producer.at s.producers i = p)
    {f : Producer → Producer}
    (producers : t.producers = Producer.update s.producers i f)
    (consumer : t.consumer = s.consumer) (closed : t.closed = s.closed) (left : t.left = s.left)
    (notForward : ∀ w, p.pc ≠ .forward w)
    (fitAfter : (f p).Fit (s.Delivers i))
    (wonAfter : (f p).pc = .disarmWon → variant.standsDown = true)
    (exitAfter : (f p).pc = .disarmClosed → s.closed = true)
    (parkAfter : (f p).pc = .park → s.closed = false)
    (bound : ∀ route, s.closed = false → t.free = 0 ∨
      (t.free + p.owed route ≤ s.free + (f p).owed route ∧
        ((f p).pc = .park → (f p).visible route = false))) :
    t.Safe variant := by
  subst current
  have others : ∀ j, j ≠ i → Producer.at t.producers j = Producer.at s.producers j :=
    fun j different => by
      rw [producers, Producer.at_update,
        ite_eq_right_of_eq_false _ _ (eq_false fun same => different same.1)]
  have self : Producer.at t.producers i = f (Producer.at s.producers i) := by
    rw [producers, Producer.at_update, ite_eq_left_of_eq_true _ _ (eq_true ⟨rfl, inside⟩)]
  have grows : ∀ j route, s.Delivers j route → t.Delivers j route := by
    rintro j route (delivers | ⟨k, w, pc, delivers⟩)
    · exact Or.inl (consumer ▸ delivers)
    · have different : k ≠ i := fun same => notForward w (same ▸ pc)
      exact Or.inr ⟨k, w, by rw [others k different]; exact pc, delivers⟩
  have keep := inv.closing_keep closed
    (fun c j park => by
      by_cases same : j = i
      · rw [same, self] at park
        have opened := parkAfter park
        rw [closed, opened] at c
        cases c
      · rw [others j same] at park ⊢
        exact ⟨park, fun _ => id⟩)
    (fun d => Or.inl (consumer ▸ d)) (fun f' => Or.inl (consumer ▸ f'))
  exact {
    opened := fun c => by
      rw [consumer]
      exact inv.opened (closed ▸ c)
    closing := fun c => by
      rw [consumer]
      exact inv.closing (closed ▸ c)
    releasing := fun release => by
      rw [left]
      exact inv.releasing (consumer ▸ release)
    won := fun j disarm => by
      by_cases same : j = i
      · rw [same, self] at disarm
        exact wonAfter disarm
      · rw [others j same] at disarm
        exact inv.won j disarm
    exits := fun j disarm => by
      rw [closed]
      by_cases same : j = i
      · rw [same, self] at disarm
        exact exitAfter disarm
      · rw [others j same] at disarm
        exact inv.exits j disarm
    fit := fun j => by
      by_cases same : j = i
      · rw [same, self]
        exact fitAfter.mono (grows i)
      · rw [others j same]
        exact (inv.fit j).mono (grows j)
    owed := fun route c ⟨j, park, visible⟩ => by
      have opened : s.closed = false := closed ▸ c
      rcases bound route opened with zero | ⟨le, notPark⟩
      · rw [zero]
        exact Nat.zero_le _
      have before : s.Sleeper route := by
        by_cases same : j = i
        · rw [same, self] at park visible
          rw [notPark park] at visible
          cases visible
        · rw [others j same] at park visible
          exact ⟨j, park, visible⟩
      have total := Producer.owedSum_update route s.producers i f inside
      have previous := inv.owed route opened before
      rw [producers, consumer]
      omega
    drained := keep.1
    flushed := keep.2 }

/-- A step of the wake loop producer `i` forwards from `stand_down`. -/
theorem State.Safe.forward_step {variant : Variant} {s t : State} (inv : s.Safe variant)
    {i : Nat} (inside : i < s.producers.length) {w : Waker}
    (pc : (Producer.at s.producers i).pc = .forward w) {ps' : List Producer}
    {next : Option Waker} (step : (ps', next) ∈ Waker.step s.producers w)
    (producers : t.producers = Producer.update ps' i fun p => { p with pc := Waker.resume next })
    (consumer : t.consumer = s.consumer) (closed : t.closed = s.closed) (left : t.left = s.left)
    (free : t.free = s.free) : t.Safe variant := by
  have frame := Waker.step_frame step
  have insideAfter : i < ps'.length := by rw [frame.1]; exact inside
  have after := Waker.step_fit (other := fun j route => s.consumer.Delivers route j ∨
      ∃ k w', k ≠ i ∧ (Producer.at s.producers k).pc = .forward w' ∧ w'.Delivers route j)
    (fun j => (inv.fit j).mono fun route delivers => by
      rcases delivers with delivers | ⟨k, w', claim, delivers⟩
      · exact Or.inl (Or.inl delivers)
      · by_cases same : k = i
        · rw [same, pc] at claim
          cases claim
          exact Or.inr delivers
        · exact Or.inl (Or.inr ⟨k, w', same, claim, delivers⟩)) step
  have others : ∀ j, j ≠ i → Producer.at t.producers j = Producer.at ps' j :=
    fun j different => by
      rw [producers, Producer.at_update,
        ite_eq_right_of_eq_false _ _ (eq_false fun same => different same.1)]
  have self : Producer.at t.producers i = { Producer.at ps' i with pc := Waker.resume next } := by
    rw [producers, Producer.at_update, ite_eq_left_of_eq_true _ _ (eq_true ⟨rfl, insideAfter⟩)]
  have selfPc : (Producer.at ps' i).pc = .forward w := (frame.2 i).1.trans pc
  have parked : ∀ j, (Producer.at t.producers j).pc = .park →
      j ≠ i ∧ (Producer.at ps' j).pc = .park := by
    intro j park
    by_cases same : j = i
    · rw [same, self] at park
      cases next <;> simp [Waker.resume] at park
    · rw [others j same] at park
      exact ⟨same, park⟩
  have resumedPc : ∀ target, (Producer.at t.producers i).pc = target →
      target ≠ .disarmWon ∧ target ≠ .disarmClosed := by
    intro target eq
    rw [self] at eq
    subst eq
    cases next <;> simp [Waker.resume]
  have keep := inv.closing_keep closed
    (fun _ j park => by
      obtain ⟨same, park⟩ := parked j park
      rw [others j same]
      exact frame.back j park)
    (fun d => Or.inl (consumer ▸ d)) (fun f => Or.inl (consumer ▸ f))
  exact {
    opened := fun c => by
      rw [consumer]
      exact inv.opened (closed ▸ c)
    closing := fun c => by
      rw [consumer]
      exact inv.closing (closed ▸ c)
    releasing := fun release => by
      rw [left]
      exact inv.releasing (consumer ▸ release)
    won := fun j disarm => by
      by_cases same : j = i
      · rw [same] at disarm
        exact absurd rfl (resumedPc _ disarm).1
      · rw [others j same] at disarm
        exact inv.won j ((frame.2 j).1.symm.trans disarm)
    exits := fun j disarm => by
      by_cases same : j = i
      · rw [same] at disarm
        exact absurd rfl (resumedPc _ disarm).2
      · rw [others j same] at disarm
        rw [closed]
        exact inv.exits j ((frame.2 j).1.symm.trans disarm)
    fit := fun j => by
      have widen : ∀ route, (s.consumer.Delivers route j ∨
          ∃ k w', k ≠ i ∧ (Producer.at s.producers k).pc = .forward w' ∧ w'.Delivers route j) ∨
            Waker.DeliversNext next route j → t.Delivers j route := by
        rintro route ((delivers | ⟨k, w', different, claim, delivers⟩) | resumes)
        · exact Or.inl (consumer ▸ delivers)
        · refine Or.inr ⟨k, w', ?_, delivers⟩
          rw [others k different]
          exact (frame.2 k).1.trans claim
        · cases next with
          | none => exact resumes.elim
          | some w'' =>
            refine Or.inr ⟨i, w'', ?_, resumes⟩
            rw [self]
            rfl
      by_cases same : j = i
      · have own := (after j).mono widen
        rw [same] at own ⊢
        rw [self]
        have quiet : (Producer.at ps' i).bit = false := by
          have shape := own.2
          simp only [selfPc] at shape
          exact shape
        cases next <;> exact ⟨own.1, quiet⟩
      · rw [others j same]
        exact (after j).mono widen
    owed := fun route c ⟨j, park, visible⟩ => by
      obtain ⟨same, park⟩ := parked j park
      rw [others j same] at visible
      have previous := inv.owed route (closed ▸ c)
        ⟨j, (frame.2 j).1.symm.trans park, (frame.2 j).2 route visible⟩
      have total := Producer.owedSum_update route ps' i
        (fun p => { p with pc := Waker.resume next }) insideAfter
      have selfOwed : (Producer.at ps' i).owed route = w.owed route := by
        simp [Producer.owed, selfPc]
      have resumed : ({ Producer.at ps' i with pc := Waker.resume next } : Producer).owed route =
          Waker.owedAfter route next := by
        cases next <;> simp [Producer.owed, Waker.resume, Waker.owedAfter]
      rw [selfOwed, resumed] at total
      rw [producers, consumer, free]
      rcases Waker.step_owed route inv.fit step with grows | quiet
      · omega
      · rw [quiet j] at visible
        cases visible
    drained := keep.1
    flushed := keep.2 }

theorem State.Safe.producer_step {variant : Variant} (proved : variant.Proved) {s t : State}
    (inv : s.Safe variant) {i : Nat} (inside : i < s.producers.length)
    (mem : t ∈ s.producerSteps variant i) : t.Safe variant := by
  have fit := inv.fit i
  have wonAt := inv.won i
  have exitsAt := inv.exits i
  have sees := proved.sees
  unfold State.producerSteps at mem
  generalize current : Producer.at s.producers i = p at fit wonAt exitsAt mem
  obtain ⟨pc, leased, bit, handle, token, registered⟩ := p
  obtain ⟨kept, shape⟩ := fit
  cases pc with
  | attempt =>
    dsimp only at mem shape
    split at mem
    · rename_i closed
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move inside current (f := fun p => { p with pc := .done }) rfl rfl rfl rfl
        (by simp) ⟨kept, shape⟩ (by simp) (by simp) (by simp)
        (fun _ c => nomatch closed.symm.trans c)
    · split at mem
      · rename_i free
        simp only [List.mem_singleton] at mem
        subst mem
        exact inv.move inside current (f := fun p => { p with pc := .done }) rfl rfl rfl rfl
          (by simp) ⟨kept, shape⟩ (by simp) (by simp) (by simp)
          (fun _ _ => Or.inr ⟨by simp only [Producer.owed]; omega, by simp⟩)
      · rename_i full
        simp only [List.mem_singleton] at mem
        subst mem
        exact inv.move inside current (f := fun p => { p with pc := .handle }) rfl rfl rfl rfl
          (by simp) ⟨kept, shape⟩ (by simp) (by simp) (by simp)
          (fun _ _ => Or.inl (by simp only [State.setPc]; omega))
  | handle =>
    dsimp only at mem shape
    split at mem
    · rename_i isLeased
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move inside current (f := fun p => { p with handle := true, pc := .bit })
        rfl rfl rfl rfl (by simp) ⟨kept, isLeased, shape, Or.inr rfl⟩ (by simp) (by simp)
        (by simp) (fun _ _ => Or.inr ⟨by simp [Producer.owed, State.edit], by simp⟩)
    · rename_i unleased
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move inside current
        (f := fun p => { p with registered := true, pc := .recheck }) rfl rfl rfl rfl (by simp)
        ⟨⟨fun _ => by simpa using unleased, fun set => nomatch shape.symm.trans set⟩,
          Or.inr (Or.inr ⟨by simpa using unleased, Or.inl rfl⟩)⟩
        (by simp) (by simp) (by simp)
        (fun _ _ => Or.inr ⟨by simp [Producer.owed, State.edit], by simp⟩)
  | bit =>
    dsimp only at mem shape
    obtain ⟨isLeased, _, armed⟩ := shape
    simp only [List.mem_singleton] at mem
    subst mem
    exact inv.move inside current (f := fun p => { p with bit := true, pc := .recheck })
      rfl rfl rfl rfl (by simp)
      ⟨⟨kept.1, fun _ => isLeased⟩,
        armed.elim Or.inl fun armedHandle => Or.inr (Or.inl ⟨isLeased, armedHandle, Or.inl rfl⟩)⟩
      (by simp) (by simp) (by simp)
      (fun _ _ => Or.inr ⟨by simp [Producer.owed, State.edit], by simp⟩)
  | recheck =>
    dsimp only at mem shape
    simp only [sees, Bool.and_true] at mem
    split at mem
    · rename_i closed
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move inside current (f := fun p => { p with pc := .disarmClosed }) rfl rfl rfl rfl
        (by simp) ⟨kept, trivial⟩ (by simp) (fun _ => closed) (by simp)
        (fun _ c => nomatch closed.symm.trans c)
    · rename_i opened
      split at mem
      · rename_i free
        have won : ∀ {t : State}, t = { s.setPc i .disarmWon with free := s.free - 1 } →
            variant.standsDown = true → t.Safe variant := by
          rintro t rfl stands
          exact inv.move inside current (f := fun p => { p with pc := .disarmWon })
            rfl rfl rfl rfl (by simp) ⟨kept, trivial⟩ (fun _ => stands) (by simp) (by simp)
            (fun _ _ => Or.inr ⟨by
              simp only [Producer.owed]
              cases leased <;> cases bit <;> cases registered <;> simp <;> omega,
              by simp⟩)
        cases variant with
        | unrepaired => exact (proved : False).elim
        | ignoresClose => exact (proved : False).elim
        | observing =>
          simp only [List.mem_singleton] at mem
          subst mem
          exact inv.move inside current (f := fun p => { p with pc := .disarmWoken })
            rfl rfl rfl rfl (by simp) ⟨kept, trivial⟩ (by simp) (by simp) (by simp)
            (fun _ _ => Or.inr ⟨by simp [Producer.owed, State.setPc], by simp⟩)
        | repaired => exact won (List.mem_singleton.mp mem) rfl
        | repairedWakingAll => exact won (List.mem_singleton.mp mem) rfl
      · rename_i full
        simp only [List.mem_singleton] at mem
        subst mem
        exact inv.move inside current (f := fun p => { p with pc := .park }) rfl rfl rfl rfl
          (by simp) ⟨kept, shape⟩ (by simp) (by simp) (fun _ => by simpa using opened)
          (fun _ _ => Or.inl (by simp only [State.setPc]; omega))
  | park =>
    dsimp only at mem shape
    split at mem
    · simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move inside current (f := fun p => { p with token := false, pc := .disarmWoken })
        rfl rfl rfl rfl (by simp) ⟨kept, trivial⟩ (by simp) (by simp) (by simp)
        (fun _ _ => Or.inr ⟨by simp [Producer.owed, State.edit], by simp⟩)
    · simp at mem
  | disarmWon =>
    dsimp only at mem
    have stands : variant.standsDown = true := wonAt rfl
    simp only [stands, Bool.true_and] at mem
    split at mem
    · rename_i owes
      simp only [List.mem_singleton] at mem
      subst mem
      have quiet : bit = false := by
        simp at owes
        exact owes.2
      exact inv.move inside current
        (f := fun p => { p with pc := .forward (.drain variant.forwardCount) })
        rfl rfl rfl rfl (by simp) ⟨kept, quiet⟩ (by simp) (by simp) (by simp)
        (fun _ _ => Or.inr ⟨by
          have := variant.forwardCount_pos
          simp [Producer.owed, Waker.owed, State.edit, owes]
          omega, by simp⟩)
    · rename_i keeps
      simp only [List.mem_singleton] at mem
      subst mem
      exact inv.move inside current (f := fun p => { p with bit := false, pc := .done })
        rfl rfl rfl rfl (by simp) ⟨⟨kept.1, nofun⟩, rfl⟩ (by simp) (by simp) (by simp)
        (fun _ _ => Or.inr ⟨by simp [Producer.owed, State.edit, keeps], by simp⟩)
  | disarmWoken =>
    dsimp only at mem
    simp only [List.mem_singleton] at mem
    subst mem
    exact inv.move inside current (f := fun p => { p with bit := false, pc := .attempt })
      rfl rfl rfl rfl (by simp) ⟨⟨kept.1, nofun⟩, rfl⟩ (by simp) (by simp) (by simp)
      (fun _ _ => Or.inr ⟨by simp only [Producer.owed, State.edit]; split <;> omega, by simp⟩)
  | disarmClosed =>
    dsimp only at mem
    simp only [List.mem_singleton] at mem
    subst mem
    exact inv.move inside current (f := fun p => { p with bit := false, pc := .done })
      rfl rfl rfl rfl (by simp) ⟨⟨kept.1, nofun⟩, rfl⟩ (by simp) (by simp) (by simp)
      (fun _ c => nomatch (exitsAt rfl).symm.trans c)
  | forward w =>
    dsimp only at mem
    obtain ⟨⟨ps', next⟩, step, rfl⟩ := List.mem_map.mp mem
    have pc : (Producer.at s.producers i).pc = .forward w := by rw [current]
    cases next <;> exact inv.forward_step inside pc step rfl rfl rfl rfl rfl
  | done =>
    dsimp only at mem
    simp at mem

theorem State.Safe.spurious_step {variant : Variant} {s t : State} (inv : s.Safe variant)
    {i : Nat} (inside : i < s.producers.length) (mem : t ∈ s.spurious i) : t.Safe variant := by
  unfold State.spurious at mem
  split at mem
  · rename_i park
    simp only [List.mem_singleton] at mem
    subst mem
    have fit := inv.fit i
    generalize current : Producer.at s.producers i = p at fit park
    obtain ⟨pc, leased, bit, handle, token, registered⟩ := p
    dsimp only at park
    subst park
    exact inv.move inside current (f := fun p => { p with pc := .disarmWoken }) rfl rfl rfl rfl
      (by simp) ⟨fit.1, trivial⟩ (by simp) (by simp) (by simp)
      (fun _ _ => Or.inr ⟨by simp [Producer.owed, State.setPc], by simp⟩)
  · simp at mem

/-- A step of the consumer's wake loop. -/
theorem State.Safe.consumer_waker {variant : Variant} {s : State} (inv : s.Safe variant)
    {w : Waker} (consumer : s.consumer = .waking w) {ps' : List Producer} {next : Option Waker}
    (step : (ps', next) ∈ Waker.step s.producers w) :
    (match next with
      | some w' => { s with producers := ps', consumer := .waking w' }
      | none => { s with producers := ps', consumer := Consumer.afterRelease s.closes s.left } :
        State).Safe variant := by
  have frame := Waker.step_frame step
  have opened : s.closed = false := inv.open_of (by rw [consumer]; exact id)
  have after := Waker.step_fit (other := fun j route => Producer.Forwards s.producers j route)
    (fun j => (inv.fit j).mono fun route delivers => by
      rcases delivers with delivers | forwards
      · rw [consumer] at delivers
        exact Or.inr delivers
      · exact Or.inl forwards) step
  have bound : ∀ route, (∃ j, (Producer.at ps' j).pc = .park ∧
      (Producer.at ps' j).visible route = true) →
      s.free ≤ Producer.owedSum route ps' + Waker.owedAfter route next :=
    fun route ⟨j, park, visible⟩ => by
      have previous := inv.owed route opened
        ⟨j, (frame.2 j).1.symm.trans park, (frame.2 j).2 route visible⟩
      rw [consumer] at previous
      rcases Waker.step_owed route inv.fit step with grows | quiet
      · simp only [Consumer.pending] at previous
        omega
      · rw [quiet j] at visible
        cases visible
  have won : ∀ j, (Producer.at ps' j).pc = .disarmWon → variant.standsDown = true :=
    fun j disarm => inv.won j ((frame.2 j).1.symm.trans disarm)
  have exits : ∀ j, (Producer.at ps' j).pc = .disarmClosed → s.closed = true :=
    fun j disarm => inv.exits j ((frame.2 j).1.symm.trans disarm)
  cases next with
  | none =>
    exact {
      opened := fun _ => Consumer.afterRelease_open s.closes s.left
      closing := fun c => nomatch opened.symm.trans c
      releasing := Consumer.afterRelease_releasing
      won
      exits
      fit := fun j => (after j).mono fun route delivers => by
        rcases delivers with forwards | resumes
        · exact Or.inr (Producer.Forwards.frame frame forwards)
        · exact resumes.elim
      owed := fun route _ sleeper => by
        have := bound route sleeper
        show s.free ≤ _ + (Consumer.afterRelease s.closes s.left).pending route
        rw [Consumer.afterRelease_pending]
        simpa [Waker.owedAfter] using this
      drained := fun c => nomatch opened.symm.trans c
      flushed := fun c => nomatch opened.symm.trans c }
  | some w' =>
    exact {
      opened := fun _ => trivial
      closing := fun c => nomatch opened.symm.trans c
      releasing := fun release => nomatch release
      won
      exits
      fit := fun j => (after j).mono fun route delivers => by
        rcases delivers with forwards | resumes
        · exact Or.inr (Producer.Forwards.frame frame forwards)
        · exact Or.inl resumes
      owed := fun route _ sleeper => bound route sleeper
      drained := fun c => nomatch opened.symm.trans c
      flushed := fun c => nomatch opened.symm.trans c }

theorem State.Safe.consumer_step {variant : Variant} {s t : State} (inv : s.Safe variant)
    (mem : t ∈ s.consumerSteps) : t.Safe variant := by
  cases consumer : s.consumer with
  | release =>
    simp only [State.consumerSteps, consumer, List.mem_map, List.mem_range] at mem
    obtain ⟨batch, _, rfl⟩ := mem
    have opened : s.closed = false := inv.open_of (by rw [consumer]; exact id)
    exact {
      opened := fun _ => trivial
      closing := fun c => nomatch opened.symm.trans c
      releasing := fun release => nomatch release
      won := inv.won
      exits := inv.exits
      fit := fun j => (inv.fit j).mono fun route delivers => by
        rcases delivers with delivers | forwards
        · rw [consumer] at delivers
          cases route <;> exact nomatch delivers
        · exact Or.inr forwards
      owed := fun route _ sleeper => by
        have := inv.owed route opened sleeper
        rw [consumer] at this
        simp only [Consumer.pending, Waker.owed] at this ⊢
        omega
      drained := fun c => nomatch opened.symm.trans c
      flushed := fun c => nomatch opened.symm.trans c }
  | waking w =>
    simp only [State.consumerSteps, consumer, List.mem_map] at mem
    obtain ⟨⟨ps', next⟩, step, rfl⟩ := mem
    cases next <;> exact inv.consumer_waker consumer step
  | close =>
    simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
    subst mem
    exact {
      opened := fun c => nomatch c
      closing := fun _ => trivial
      releasing := fun release => nomatch release
      won := inv.won
      exits := fun _ _ => rfl
      fit := fun j => (inv.fit j).mono fun route delivers => by
        rcases delivers with delivers | forwards
        · rw [consumer] at delivers
          cases route <;> exact nomatch delivers
        · exact Or.inr forwards
      owed := fun _ c => nomatch c
      drained := fun _ drained => nomatch drained
      flushed := fun _ flushed => nomatch flushed }
  | drainOverflow =>
    have closed := inv.closed_of (by rw [consumer]; exact id)
    simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
    subst mem
    have frame := Producer.Frame.unregister s.producers
    exact inv.closing_step frame closed closed trivial
      (fun j => by
        show (Producer.at (Producer.unregister s.producers) j).Fit _
        rw [Producer.at_unregister]
        exact (inv.fit j).unregister
          (fun registered => Or.inl (Producer.mem_registeredIndices registered))
          fun route delivers => by
            rcases delivers with delivers | forwards
            · rw [consumer] at delivers
              cases route <;> exact nomatch delivers
            · exact Or.inr (Producer.Forwards.frame frame forwards))
      (fun _ => Or.inr fun j => by
        show (Producer.at (Producer.unregister s.producers) j).registered = false
        rw [Producer.at_unregister])
      (fun flushed => nomatch flushed)
  | unparking pending =>
    have closed := inv.closed_of (by rw [consumer]; exact id)
    have drained : s.consumer.Drained := by rw [consumer]; trivial
    cases pending with
    | nil =>
      simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
      subst mem
      exact inv.closing_step (Producer.Frame.refl _) closed closed trivial
        (fun j => (inv.fit j).mono fun route delivers => by
          rcases delivers with delivers | forwards
          · rw [consumer] at delivers
            cases route <;> simp [Consumer.Delivers] at delivers
          · exact Or.inr forwards)
        (fun _ => Or.inl drained) (fun flushed => nomatch flushed)
    | cons i rest =>
      simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
      subst mem
      have frame : Producer.Frame s.producers
          (Producer.update s.producers i fun p => { p with token := true }) :=
        Producer.Frame.update (fun _ => rfl) fun _ route => by cases route <;> exact id
      exact inv.closing_step frame closed closed trivial
        (fun j => Producer.Fit.update (f := fun p => { p with token := true })
          (delivered := fun j route =>
            Consumer.Delivers (.unparking rest) route j ∨
              Producer.Forwards (Producer.update s.producers i fun p => { p with token := true })
                j route)
          j (fun _ => (inv.fit i).unparked _)
          fun same => (inv.fit j).mono fun route delivers => by
            rcases delivers with delivers | forwards
            · rw [consumer] at delivers
              cases route
              · exact nomatch delivers
              · exact Or.inl ((List.mem_cons.mp delivers).resolve_left same)
            · exact Or.inr (Producer.Forwards.frame frame forwards))
        (fun _ => Or.inl drained) (fun flushed => nomatch flushed)
  | flush =>
    have closed := inv.closed_of (by rw [consumer]; exact id)
    simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
    subst mem
    have frame := Producer.Frame.clear_bits s.producers
    exact inv.closing_step frame closed closed trivial
      (fun j => by
        show (Producer.at (s.producers.map fun p => { p with bit := false }) j).Fit _
        rw [Producer.at_clear_bits]
        exact (inv.fit j).flushed
          (fun bit => Or.inl (List.mem_filter.mpr
            ⟨List.mem_range.mpr (Producer.inside_of_bit bit), bit⟩))
          fun route delivers => by
            rcases delivers with delivers | forwards
            · rw [consumer] at delivers
              cases route <;> exact nomatch delivers
            · exact Or.inr (Producer.Forwards.frame frame forwards))
      (fun _ => Or.inl (by rw [consumer]; trivial))
      (fun _ => Or.inr fun j => by
        show (Producer.at (s.producers.map fun p => { p with bit := false }) j).bit = false
        rw [Producer.at_clear_bits])
  | flushing pending =>
    have closed := inv.closed_of (by rw [consumer]; exact id)
    have drained : s.consumer.Drained := by rw [consumer]; trivial
    have flushed : s.consumer.Flushed := by rw [consumer]; trivial
    cases pending with
    | nil =>
      simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
      subst mem
      exact inv.closing_step (Producer.Frame.refl _) closed closed trivial
        (fun j => (inv.fit j).mono fun route delivers => by
          rcases delivers with delivers | forwards
          · rw [consumer] at delivers
            cases route <;> simp [Consumer.Delivers] at delivers
          · exact Or.inr forwards)
        (fun _ => Or.inl drained) (fun _ => Or.inl flushed)
    | cons i rest =>
      simp only [State.consumerSteps, consumer, List.mem_singleton] at mem
      subst mem
      have frame : Producer.Frame s.producers (Producer.update s.producers i fun p =>
          if p.handle then { p with handle := false, token := true } else p) :=
        Producer.Frame.update (fun p => by split <;> rfl) fun p route visible => by
          revert visible
          split <;> (cases route <;> exact id)
      exact inv.closing_step frame closed closed trivial
        (fun j => Producer.Fit.update (delivered := fun j route =>
            Consumer.Delivers (.flushing rest) route j ∨
              Producer.Forwards (Producer.update s.producers i fun p =>
                if p.handle then { p with handle := false, token := true } else p) j route)
          j
          (fun _ => by
            split
            · exact (inv.fit i).claimed _
            · rename_i handle
              exact (inv.fit i).unclaimed (by simpa using handle) fun delivers => by
                rcases delivers with delivers | forwards
                · rw [consumer] at delivers
                  exact nomatch delivers
                · exact Or.inr (Producer.Forwards.frame frame forwards))
          fun same => (inv.fit j).mono fun route delivers => by
            rcases delivers with delivers | forwards
            · rw [consumer] at delivers
              cases route
              · exact Or.inl ((List.mem_cons.mp delivers).resolve_left same)
              · exact nomatch delivers
            · exact Or.inr (Producer.Forwards.frame frame forwards))
        (fun _ => Or.inl drained) (fun _ => Or.inl flushed)
  | finished => simp [State.consumerSteps, consumer] at mem

theorem State.init_at (leases : List Bool) (releases : Nat) (closes : Bool) (j : Nat) :
    (Producer.at (State.init leases releases closes).producers j).pc = .attempt ∧
      (Producer.at (State.init leases releases closes).producers j).bit = false ∧
      (Producer.at (State.init leases releases closes).producers j).registered = false := by
  simp only [State.init, Producer.at, List.getD_eq_getElem?_getD, List.getElem?_map]
  cases leases[j]? <;> simp [Producer.fresh]

theorem State.Safe.init (variant : Variant) (leases : List Bool) (releases : Nat)
    (closes : Bool) : (State.init leases releases closes).Safe variant where
  opened _ := Consumer.afterRelease_open closes releases
  closing c := nomatch c
  releasing := Consumer.afterRelease_releasing
  won j disarm := by
    rw [(State.init_at leases releases closes j).1] at disarm
    cases disarm
  exits j disarm := by
    rw [(State.init_at leases releases closes j).1] at disarm
    cases disarm
  fit j := by
    obtain ⟨pc, bit, registered⟩ := State.init_at leases releases closes j
    revert pc bit registered
    generalize Producer.at (State.init leases releases closes).producers j = q
    rcases q with ⟨pc, leased, bit, handle, token, registered⟩
    intro pcStart bitStart registeredStart
    dsimp only at pcStart bitStart registeredStart
    subst pcStart bitStart registeredStart
    exact ⟨⟨nofun, nofun⟩, rfl⟩
  owed _ _ := fun ⟨j, park, _⟩ => by
    rw [(State.init_at leases releases closes j).1] at park
    cases park
  drained c := nomatch c
  flushed c := nomatch c

theorem Reach.safe {variant : Variant} (proved : variant.Proved) {leases : List Bool}
    {releases : Nat} {closes : Bool} {s : State}
    (reach : Reach variant (State.init leases releases closes) s) : s.Safe variant := by
  induction reach with
  | init => exact State.Safe.init variant leases releases closes
  | step _ mem inv =>
    unfold State.moves at mem
    rcases List.mem_append.mp mem with moved | spurious
    · unfold State.succs at moved
      rcases List.mem_append.mp moved with consumer | producer
      · exact inv.consumer_step consumer
      · obtain ⟨i, inside, step⟩ := List.mem_flatMap.mp producer
        exact inv.producer_step proved (List.mem_range.mp inside) step
    · obtain ⟨i, inside, step⟩ := List.mem_flatMap.mp spurious
      exact inv.spurious_step (List.mem_range.mp inside) step

/-- A state that keeps the invariant is not lost. -/
theorem State.Safe.not_lost {variant : Variant} {s : State} (inv : s.Safe variant) :
    ¬ s.Lost variant := by
  intro ⟨empty, freeOrClosed, ⟨p, member, unfinished⟩⟩
  unfold State.succs at empty
  obtain ⟨consumerIdle, producersIdle⟩ := List.append_eq_nil_iff.mp empty
  have finished : s.consumer = .finished := by
    cases consumer : s.consumer with
    | finished => rfl
    | release =>
      have := inv.releasing consumer
      simp only [State.consumerSteps, consumer, List.map_eq_nil_iff,
        List.range_eq_nil] at consumerIdle
      omega
    | waking w =>
      simp only [State.consumerSteps, consumer, List.map_eq_nil_iff] at consumerIdle
      exact absurd consumerIdle (Waker.step_ne_nil _ _)
    | unparking pending =>
      cases pending <;> simp [State.consumerSteps, consumer] at consumerIdle
    | flushing pending =>
      cases pending <;> simp [State.consumerSteps, consumer] at consumerIdle
    | _ => simp [State.consumerSteps, consumer] at consumerIdle
  have idle : ∀ i, i < s.producers.length →
      (Producer.at s.producers i).pc = .done ∨
        ((Producer.at s.producers i).pc = .park ∧ (Producer.at s.producers i).token = false) := by
    intro i inside
    have stuck := (List.flatMap_eq_nil_iff.mp producersIdle) i (List.mem_range.mpr inside)
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
  have quiet : ∀ j route, ¬ s.Delivers j route := by
    rintro j route (delivers | ⟨k, w, pc, _⟩)
    · rw [finished] at delivers
      cases route <;> exact delivers
    · have inside : k < s.producers.length :=
        Producer.inside fun fresh => by rw [fresh] at pc; simp [Producer.fresh] at pc
      rcases idle k inside with done | ⟨park, _⟩
      · rw [done] at pc
        cases pc
      · rw [park] at pc
        cases pc
  have fits : ∀ j, (Producer.at s.producers j).Fit fun _ => False :=
    fun j => (inv.fit j).mono fun route delivers => quiet j route delivers
  have nothing : ∀ route, Producer.owedSum route s.producers = 0 := fun route =>
    Producer.owedSum_eq_zero fun q qMember => by
      obtain ⟨i, inside, rfl⟩ := List.getElem_of_mem qMember
      rw [← Producer.at_of_lt inside]
      rcases idle i inside with done | ⟨park, token⟩
      · simp [Producer.owed, done]
      · exact ((fits i).waiting park token).2 route
  obtain ⟨i, inside, rfl⟩ := List.getElem_of_mem member
  rw [← Producer.at_of_lt inside] at unfinished
  rcases idle i inside with done | ⟨park, token⟩
  · exact unfinished done
  obtain ⟨visible, _⟩ := (fits i).waiting park token
  cases closed : s.closed
  · have free : 0 < s.free := by
      rcases freeOrClosed with free | shut
      · exact free
      · rw [closed] at shut
        cases shut
    have bound : ∀ route, (Producer.at s.producers i).visible route = true → s.free ≤ 0 :=
      fun route seen => by
        have := inv.owed route closed ⟨i, park, seen⟩
        rw [nothing route, finished] at this
        simpa [Consumer.pending] using this
    rcases visible with bit | registered
    · have := bound .bitmap bit
      omega
    · have := bound .overflow registered
      omega
  · rcases visible with bit | registered
    · have := inv.flushed closed (by rw [finished]; trivial) i park
      rw [bit] at this
      cases this
    · have := inv.drained closed (by rw [finished]; trivial) i park
      rw [registered] at this
      cases this

/-- The park/wake handshake never loses a wake. This holds for every protocol in `Variant.Proved`,
every assignment of park slots, any number of releases in batches of any size, whether or not the
consumer closes, and with spurious returns from `park`. -/
theorem never_lost {variant : Variant} (proved : variant.Proved) {leases : List Bool}
    {releases : Nat} {closes : Bool} {s : State}
    (reach : Reach variant (State.init leases releases closes) s) : ¬ s.Lost variant :=
  (reach.safe proved).not_lost

end Park

end Qtz
