import Qtz.ParkModel

/-!
The bookkeeping behind `ParkSafe`: what each producer and each wake loop owes, what the invariant
keeps true of a single producer, and how one step of a wake loop moves both.

A wake reaches a producer by one of two routes. On the bitmap, a waker retires the producer's bit and
claims its park handle. On the overflow stack, a drain takes every registration at once and unparks
each thread it took.

A producer owes one look at the ring when it is about to look at it at the top of its loop, or when
it waits (re-checking, parked, or about to disarm) and no peer can find it any more, because its bit
or its registration was taken. A producer whose re-check took a slot and whose bit was then retired owes the wake it was
given, which `stand_down` passes on. A wake loop owes the wakes it has yet to deliver. On the
overflow route a loop stops owing once it has drained the stack: the drain reaches every registered
producer, and one that registers later re-checks the ring after the release that started the loop.
-/

namespace Qtz

namespace Park

/-- How a wake reaches a producer: through its wake bit and park handle, or through the overflow
stack. -/
inductive Route where
  | bitmap
  | overflow
  deriving DecidableEq

/-- Whether a peer can find the producer by `route`. -/
def Producer.visible (p : Producer) : Route → Bool
  | .bitmap => p.bit
  | .overflow => p.registered

/-- The wakes a loop still owes on `route`. -/
def Waker.owed : Route → Waker → Nat
  | _, .drain count => count
  | .bitmap, .unpark _ count => count
  | .bitmap, .scan count => count
  | .bitmap, .clear _ count => count
  | .bitmap, .claim _ count => count - 1
  | .overflow, _ => 0

/-- Where a forwarding producer goes once its wake loop takes a step. -/
def Waker.resume : Option Waker → Pc
  | some w => .forward w
  | none => .done

def Waker.owedAfter (route : Route) : Option Waker → Nat
  | some w => w.owed route
  | none => 0

/-- The loop is about to deliver a wake to producer `j` by `route`. -/
def Waker.Delivers : Waker → Route → Nat → Prop
  | .claim i _, .bitmap, j => j = i
  | .unpark pending _, .overflow, j => j ∈ pending
  | _, _, _ => False

def Waker.DeliversNext : Option Waker → Route → Nat → Prop
  | some w, route, j => w.Delivers route j
  | none, _, _ => False

def Producer.owed (route : Route) (p : Producer) : Nat :=
  match p.pc with
  | .top | .attempt | .take => 1
  | .recheck | .recheckRing | .recheckClosed | .park | .disarmWoken | .disarmClosed =>
    if p.bit || p.registered then 0 else 1
  | .disarmWon => if p.leased && !p.bit then 1 else 0
  | .forward w => w.owed route
  | _ => 0

def Producer.owedSum (route : Route) (ps : List Producer) : Nat :=
  (ps.map (Producer.owed route)).sum

/-- A leased producer never registers on the overflow stack; a slotless one never sets a bit. -/
def Producer.Kept (p : Producer) : Prop :=
  (p.registered = true → p.leased = false) ∧ (p.bit = true → p.leased = true)

/-- A waiting producer will be woken: it holds a token, or a peer can still find it, or a wake is
on its way to it by its route. -/
def Producer.Awaited (p : Producer) (delivered : Route → Prop) : Prop :=
  p.token = true ∨ (p.leased = true ∧ p.handle = true ∧ (p.bit = true ∨ delivered .bitmap)) ∨
    (p.leased = false ∧ (p.registered = true ∨ delivered .overflow))

/-- What the invariant keeps true of each producer, given which wakes are on their way to it. A set
bit only stands while the producer waits or disarms, and a waiting producer will be woken. -/
def Producer.Fit (p : Producer) (delivered : Route → Prop) : Prop :=
  p.Kept ∧
    match p.pc with
    | .top | .attempt | .topClosed | .take | .handle | .done | .forward _ => p.bit = false
    | .bit => p.leased = true ∧ p.bit = false ∧ (p.token = true ∨ p.handle = true)
    | .recheck | .recheckRing | .recheckClosed | .park => p.Awaited delivered
    | .disarmWon | .disarmWoken | .disarmClosed => True

theorem Producer.fresh_fit (delivered : Route → Prop) : Producer.fresh.Fit delivered :=
  ⟨⟨nofun, fun _ => rfl⟩, rfl⟩

theorem Producer.Fit.mono {p : Producer} {delivered wider : Route → Prop}
    (fit : p.Fit delivered) (widen : ∀ route, delivered route → wider route) : p.Fit wider := by
  rcases p with ⟨pc, leased, bit, handle, token, registered⟩
  obtain ⟨kept, fit⟩ := fit
  refine ⟨kept, ?_⟩
  cases pc <;> simp only [Producer.Awaited] at fit ⊢ <;> try exact fit
  all_goals
    exact fit.imp_right (Or.imp (fun ⟨l, h, b⟩ => ⟨l, h, b.imp_right (widen _)⟩)
      (fun ⟨l, r⟩ => ⟨l, r.imp_right (widen _)⟩))

theorem Producer.Fit.flushed {p : Producer} {delivered wider : Route → Prop}
    (fit : p.Fit delivered) (taken : p.bit = true → wider .bitmap)
    (widen : ∀ route, delivered route → wider route) :
    ({ p with bit := false } : Producer).Fit wider := by
  rcases p with ⟨pc, leased, bit, handle, token, registered⟩
  have := widen .bitmap
  have := widen .overflow
  cases leased <;> cases bit <;> cases registered <;> cases pc <;>
    simp_all [Producer.Fit, Producer.Kept, Producer.Awaited] <;> grind

theorem Producer.Fit.unregister {p : Producer} {delivered wider : Route → Prop}
    (fit : p.Fit delivered) (taken : p.registered = true → wider .overflow)
    (widen : ∀ route, delivered route → wider route) :
    ({ p with registered := false } : Producer).Fit wider := by
  rcases p with ⟨pc, leased, bit, handle, token, registered⟩
  have := widen .bitmap
  have := widen .overflow
  cases leased <;> cases bit <;> cases registered <;> cases pc <;>
    simp_all [Producer.Fit, Producer.Kept, Producer.Awaited] <;> grind

theorem Producer.Fit.claimed {p : Producer} {delivered : Route → Prop} (fit : p.Fit delivered)
    (wider : Route → Prop) : ({ p with handle := false, token := true } : Producer).Fit wider := by
  rcases p with ⟨pc, leased, bit, handle, token, registered⟩
  obtain ⟨kept, fit⟩ := fit
  refine ⟨kept, ?_⟩
  cases pc <;> simp_all [Producer.Awaited]

theorem Producer.Fit.unparked {p : Producer} {delivered : Route → Prop} (fit : p.Fit delivered)
    (wider : Route → Prop) : ({ p with token := true } : Producer).Fit wider := by
  rcases p with ⟨pc, leased, bit, handle, token, registered⟩
  obtain ⟨kept, fit⟩ := fit
  refine ⟨kept, ?_⟩
  cases pc <;> simp_all [Producer.Awaited]

/-- A producer whose handle is gone can only be woken by a token or the overflow stack. -/
theorem Producer.Fit.unclaimed {p : Producer} {delivered wider : Route → Prop}
    (fit : p.Fit delivered) (handle : p.handle = false)
    (widen : delivered .overflow → wider .overflow) : p.Fit wider := by
  rcases p with ⟨pc, leased, bit, handleField, token, registered⟩
  obtain ⟨kept, fit⟩ := fit
  refine ⟨kept, ?_⟩
  subst handle
  cases pc <;> simp only [Producer.Awaited] at fit ⊢ <;> try exact fit
  all_goals exact fit.imp_right (Or.imp (fun ⟨_, h, _⟩ => absurd h (by simp))
    (fun ⟨l, r⟩ => ⟨l, r.imp_right widen⟩))

/-- Retiring a set bit turns what the producer owes from nothing to one look. -/
theorem Producer.Fit.retire {p : Producer} {delivered : Route → Prop} (fit : p.Fit delivered)
    (bit : p.bit = true) (route : Route) :
    p.owed route = 0 ∧ ({ p with bit := false } : Producer).owed route = 1 := by
  rcases p with ⟨pc, leased, bitField, handle, token, registered⟩
  obtain ⟨kept, fit⟩ := fit
  subst bit
  cases leased <;> cases registered <;> cases pc <;> simp_all [Producer.Kept, Producer.owed]

/-- A producer parked without a token, with no wake on its way, can still be found and owes
nothing. -/
theorem Producer.Fit.waiting {p : Producer} (fit : p.Fit fun _ => False) (park : p.pc = .park)
    (token : p.token = false) :
    (p.bit = true ∨ p.registered = true) ∧ ∀ route, p.owed route = 0 := by
  rcases p with ⟨pc, leased, bit, handle, tokenField, registered⟩
  obtain ⟨_, fit⟩ := fit
  subst park token
  simp only [Producer.Awaited] at fit
  have visible : bit = true ∨ registered = true := by
    rcases fit with token | ⟨_, _, set | never⟩ | ⟨_, set | never⟩
    · cases token
    · exact Or.inl set
    · exact never.elim
    · exact Or.inr set
    · exact never.elim
  refine ⟨visible, fun route => ?_⟩
  rcases visible with set | set <;> simp [Producer.owed, set]

theorem Producer.owed_unregister (route : Route) (p : Producer) :
    p.owed route ≤ ({ p with registered := false } : Producer).owed route := by
  rcases p with ⟨pc, leased, bit, handle, token, registered⟩
  cases pc <;> simp only [Producer.owed] <;> (try split) <;> (try split) <;> simp_all

theorem Producer.owed_clear (route : Route) (p : Producer) :
    p.owed route ≤ ({ p with bit := false } : Producer).owed route := by
  rcases p with ⟨pc, leased, bit, handle, token, registered⟩
  cases pc <;> simp only [Producer.owed] <;> (try split) <;> (try split) <;> simp_all

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

theorem Producer.at_beyond {ps : List Producer} {i : Nat} (beyond : ¬ i < ps.length) :
    Producer.at ps i = Producer.fresh := by
  simp [Producer.at, List.getD_eq_getElem?_getD, List.getElem?_eq_none (Nat.le_of_not_lt beyond)]

theorem Producer.at_of_lt {ps : List Producer} {i : Nat} (inside : i < ps.length) :
    Producer.at ps i = ps[i] := by
  simp [Producer.at, List.getD_eq_getElem?_getD, inside]

theorem Producer.inside {ps : List Producer} {i : Nat} (moved : Producer.at ps i ≠ Producer.fresh) :
    i < ps.length := by
  by_cases inside : i < ps.length
  · exact inside
  · exact absurd (Producer.at_beyond inside) moved

theorem Producer.inside_of_bit {ps : List Producer} {i : Nat}
    (bit : (Producer.at ps i).bit = true) : i < ps.length :=
  Producer.inside fun fresh => by simp [fresh, Producer.fresh] at bit

theorem Producer.at_map {ps : List Producer} {g : Producer → Producer}
    (fresh : g Producer.fresh = Producer.fresh) (j : Nat) :
    Producer.at (ps.map g) j = g (Producer.at ps j) := by
  unfold Producer.at
  rw [List.getD_eq_getElem?_getD, List.getD_eq_getElem?_getD, List.getElem?_map]
  cases ps[j]? <;> simp [fresh]

theorem Producer.at_unregister (ps : List Producer) (j : Nat) :
    Producer.at (Producer.unregister ps) j =
      { Producer.at ps j with registered := false } :=
  Producer.at_map rfl j

theorem Producer.at_clear_bits (ps : List Producer) (j : Nat) :
    Producer.at (ps.map fun p => { p with bit := false }) j =
      { Producer.at ps j with bit := false } :=
  Producer.at_map rfl j

theorem Producer.owedSum_update (route : Route) (ps : List Producer) (i : Nat)
    (f : Producer → Producer) (inside : i < ps.length) :
    Producer.owedSum route (Producer.update ps i f) + (Producer.at ps i).owed route =
      Producer.owedSum route ps + (f (Producer.at ps i)).owed route := by
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

theorem Producer.owedSum_update_same (route : Route) (ps : List Producer) (i : Nat)
    (f : Producer → Producer) (same : ∀ p, (f p).owed route = p.owed route) :
    Producer.owedSum route (Producer.update ps i f) = Producer.owedSum route ps := by
  by_cases inside : i < ps.length
  · have total := Producer.owedSum_update route ps i f inside
    rw [same] at total
    omega
  · simp [Producer.update, List.set_eq_of_length_le (Nat.le_of_not_lt inside)]

theorem Producer.owedSum_map_le (route : Route) {g : Producer → Producer}
    (grows : ∀ p, p.owed route ≤ (g p).owed route) (ps : List Producer) :
    Producer.owedSum route ps ≤ Producer.owedSum route (ps.map g) := by
  induction ps with
  | nil => simp [Producer.owedSum]
  | cons head tail ih =>
    have := grows head
    simp only [Producer.owedSum, List.map_cons, List.sum_cons, List.map_map] at ih ⊢
    exact Nat.add_le_add this ih

theorem Producer.owedSum_eq_zero {route : Route} {ps : List Producer}
    (none : ∀ p ∈ ps, p.owed route = 0) : Producer.owedSum route ps = 0 := by
  induction ps with
  | nil => rfl
  | cons head tail ih =>
    simp only [Producer.owedSum, List.map_cons, List.sum_cons] at ih ⊢
    rw [none head (by simp), ih (fun p mem => none p (by simp [mem]))]

theorem Waker.step_ne_nil (ps : List Producer) (stack : List Nat) (w : Waker) :
    Waker.step ps stack w ≠ [] := by
  cases w with
  | drain => simp [Waker.step]
  | unpark pending count => cases pending <;> simp [Waker.step]
  | scan =>
    simp only [Waker.step]
    split
    · simp
    · rename_i picks notEmpty
      simpa using notEmpty
  | clear => simp only [Waker.step]; split <;> simp
  | claim => simp only [Waker.step]; split <;> simp

/-- What a wake-loop step may do to the producers: keep every program counter, and only take bits
and registrations away. -/
def Producer.Frame (ps ps' : List Producer) : Prop :=
  ps'.length = ps.length ∧
    ∀ j, (Producer.at ps' j).pc = (Producer.at ps j).pc ∧
      ∀ route, (Producer.at ps' j).visible route = true → (Producer.at ps j).visible route = true

theorem Producer.Frame.refl (ps : List Producer) : Producer.Frame ps ps :=
  ⟨rfl, fun _ => ⟨rfl, fun _ => id⟩⟩

theorem Producer.Frame.update {ps : List Producer} {i : Nat} {f : Producer → Producer}
    (pc : ∀ p, (f p).pc = p.pc)
    (visible : ∀ p route, (f p).visible route = true → p.visible route = true) :
    Producer.Frame ps (Producer.update ps i f) := by
  refine ⟨by simp [Producer.update], fun j => ?_⟩
  rw [Producer.at_update]
  split
  · rename_i same
    rw [same.1]
    exact ⟨pc _, visible _⟩
  · exact ⟨rfl, fun _ => id⟩

theorem Producer.Frame.map {ps : List Producer} {g : Producer → Producer}
    (fresh : g Producer.fresh = Producer.fresh) (pc : ∀ p, (g p).pc = p.pc)
    (visible : ∀ p route, (g p).visible route = true → p.visible route = true) :
    Producer.Frame ps (ps.map g) :=
  ⟨by simp, fun j => by rw [Producer.at_map fresh]; exact ⟨pc _, visible _⟩⟩

theorem Producer.Frame.unregister (ps : List Producer) :
    Producer.Frame ps (Producer.unregister ps) :=
  Producer.Frame.map rfl (fun _ => rfl) fun _ route => by
    cases route <;> simp [Producer.visible]

theorem Producer.Frame.clear_bits (ps : List Producer) :
    Producer.Frame ps (ps.map fun p => { p with bit := false }) :=
  Producer.Frame.map rfl (fun _ => rfl) fun _ route => by
    cases route <;> simp [Producer.visible]

theorem Waker.step_frame {ps ps' : List Producer} {stack : List Nat} {w : Waker}
    {next : Option Waker} (mem : (ps', next) ∈ Waker.step ps stack w) :
    Producer.Frame ps ps' := by
  cases w with
  | drain =>
    simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem
    obtain ⟨rfl, _⟩ := mem
    exact Producer.Frame.unregister ps
  | unpark pending count =>
    cases pending <;> simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, _⟩ := mem
    · exact Producer.Frame.refl _
    · exact Producer.Frame.update (fun _ => rfl) fun _ route => by cases route <;> exact id
  | scan =>
    simp only [Waker.step] at mem
    split at mem
    · simp only [List.mem_singleton, Prod.mk.injEq] at mem
      obtain ⟨rfl, _⟩ := mem
      exact Producer.Frame.refl _
    · simp only [List.mem_map, Prod.mk.injEq] at mem
      obtain ⟨_, _, rfl, _⟩ := mem
      exact Producer.Frame.refl _
  | clear =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, _⟩ := mem
    · exact Producer.Frame.update (fun _ => rfl) fun _ route => by
        cases route <;> simp [Producer.visible]
    · exact Producer.Frame.refl _
  | claim =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, _⟩ := mem
    · exact Producer.Frame.update (fun _ => rfl) fun _ route => by cases route <;> exact id
    · exact Producer.Frame.refl _

/-- After a wake-loop step every registered producer is still on the stack that step leaves. -/
theorem Waker.step_stacked {ps ps' : List Producer} {stack : List Nat} {w : Waker}
    {next : Option Waker} (mem : (ps', next) ∈ Waker.step ps stack w)
    (stacked : ∀ j, (Producer.at ps j).registered = true → j ∈ stack) (j : Nat)
    (registered : (Producer.at ps' j).registered = true) : j ∈ w.stackAfter stack := by
  cases w with
  | drain =>
    simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem
    obtain ⟨rfl, _⟩ := mem
    rw [Producer.at_unregister] at registered
    cases registered
  | _ => exact stacked j ((Waker.step_frame mem).2 j |>.2 .overflow registered)

theorem Waker.owedAfter_delivered (route : Route) (count : Nat) :
    Waker.owedAfter route (Waker.delivered count) =
      match route with
      | .bitmap => count - 1
      | .overflow => 0 := by
  unfold Waker.delivered
  split <;> cases route <;> simp [Waker.owedAfter, Waker.owed] <;> omega

/-- A wake-loop step never lowers what is owed on `route`, counting the loop's own debt, unless
afterwards no peer can find any producer by that route. -/
theorem Waker.step_owed (route : Route) {ps ps' : List Producer} {stack : List Nat} {w : Waker}
    {next : Option Waker} {delivered : Nat → Route → Prop}
    (fits : ∀ j, (Producer.at ps j).Fit (delivered j))
    (mem : (ps', next) ∈ Waker.step ps stack w) :
    Producer.owedSum route ps + w.owed route ≤
        Producer.owedSum route ps' + Waker.owedAfter route next ∨
      ∀ j, (Producer.at ps' j).visible route = false := by
  cases w with
  | drain count =>
    simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem
    obtain ⟨rfl, rfl⟩ := mem
    cases route
    · have grows := Producer.owedSum_map_le .bitmap (Producer.owed_unregister .bitmap) ps
      exact Or.inl (by simp only [Waker.owed, Waker.owedAfter, Producer.unregister]; omega)
    · exact Or.inr fun j => by simp [Producer.at_unregister, Producer.visible]
  | unpark pending count =>
    cases pending <;> simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · cases route <;> exact Or.inl (by simp [Waker.owed, Waker.owedAfter])
    · rw [Producer.owedSum_update_same route _ _ _ fun p => by
        rcases p with ⟨pc, _, _, _, _, _⟩; cases pc <;> rfl]
      cases route <;> exact Or.inl (by simp [Waker.owed, Waker.owedAfter])
  | scan count =>
    simp only [Waker.step] at mem
    split at mem
    · rename_i none
      simp only [List.mem_singleton, Prod.mk.injEq] at mem
      obtain ⟨rfl, rfl⟩ := mem
      cases route
      · refine Or.inr fun j => ?_
        cases bit : (Producer.at ps' j).bit
        · exact bit
        · have picked : j ∈ (List.range ps'.length).filter (fun i => (Producer.at ps' i).bit) :=
            List.mem_filter.mpr ⟨List.mem_range.mpr (Producer.inside_of_bit bit), bit⟩
          simp [none] at picked
      · exact Or.inl (by simp [Waker.owed, Waker.owedAfter])
    · simp only [List.mem_map, Prod.mk.injEq] at mem
      obtain ⟨_, _, rfl, rfl⟩ := mem
      cases route <;> exact Or.inl (by simp [Waker.owed, Waker.owedAfter])
  | clear k count =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rename_i bit
      obtain ⟨before, after⟩ := (fits k).retire bit route
      have total := Producer.owedSum_update route ps k (fun p => { p with bit := false })
        (Producer.inside_of_bit bit)
      rw [before, after] at total
      cases route <;> exact Or.inl (by simp only [Waker.owed, Waker.owedAfter]; omega)
    · cases route <;> exact Or.inl (by simp [Waker.owed, Waker.owedAfter])
  | claim k count =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rw [Producer.owedSum_update_same route _ _ _ fun p => by
        rcases p with ⟨pc, _, _, _, _, _⟩; cases pc <;> rfl, Waker.owedAfter_delivered]
      cases route <;> exact Or.inl (by simp [Waker.owed])
    · cases route <;> exact Or.inl (by simp [Waker.owed, Waker.owedAfter])

/-- A wake-loop step keeps every producer fit, moving the wakes it is about to deliver from `w`
to `next`. `other` is what every other waker delivers. Every registered producer is on `stack`. -/
theorem Waker.step_fit {ps ps' : List Producer} {stack : List Nat} {w : Waker}
    {next : Option Waker} {other : Nat → Route → Prop}
    (stacked : ∀ j, (Producer.at ps j).registered = true → j ∈ stack)
    (fits : ∀ j, (Producer.at ps j).Fit fun route => other j route ∨ w.Delivers route j)
    (mem : (ps', next) ∈ Waker.step ps stack w) (j : Nat) :
    (Producer.at ps' j).Fit fun route => other j route ∨ Waker.DeliversNext next route j := by
  have quiet : (∀ route, ¬ w.Delivers route j) →
      (Producer.at ps j).Fit fun route => other j route ∨ Waker.DeliversNext next route j :=
    fun none => (fits j).mono fun route => Or.imp_right fun delivers => absurd delivers (none route)
  cases w with
  | drain count =>
    simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem
    obtain ⟨rfl, rfl⟩ := mem
    rw [Producer.at_unregister]
    exact (fits j).unregister
      (fun registered => Or.inr (stacked j registered))
      fun route => Or.imp_right fun delivers => by cases route <;> exact delivers.elim
  | unpark pending count =>
    cases pending with
    | nil =>
      simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem
      obtain ⟨rfl, rfl⟩ := mem
      exact quiet fun route delivers => by cases route <;> simp [Waker.Delivers] at delivers
    | cons i rest =>
      simp only [Waker.step, List.mem_singleton, Prod.mk.injEq] at mem
      obtain ⟨rfl, rfl⟩ := mem
      rw [Producer.at_update]
      split
      · rename_i same
        rw [same.1]
        exact (fits i).unparked _
      · rename_i outside
        by_cases same : j = i
        · subst same
          rw [Producer.at_beyond fun inside => outside ⟨rfl, inside⟩]
          exact Producer.fresh_fit _
        · refine (fits j).mono fun route => Or.imp_right fun delivers => ?_
          cases route <;> simp only [Waker.Delivers, List.mem_cons] at delivers
          exact delivers.resolve_left same
  | scan count =>
    simp only [Waker.step] at mem
    split at mem
    · simp only [List.mem_singleton, Prod.mk.injEq] at mem
      obtain ⟨rfl, rfl⟩ := mem
      exact quiet fun route delivers => by cases route <;> exact delivers
    · simp only [List.mem_map, Prod.mk.injEq] at mem
      obtain ⟨_, _, rfl, rfl⟩ := mem
      exact quiet fun route delivers => by cases route <;> exact delivers
  | clear k count =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rw [Producer.at_update]
      split
      · rename_i same
        rw [same.1]
        exact (fits k).flushed (fun _ => Or.inr rfl)
          fun route => Or.imp_right fun delivers => by cases route <;> exact delivers.elim
      · exact quiet fun route delivers => by cases route <;> exact delivers
    · exact quiet fun route delivers => by cases route <;> exact delivers
  | claim k count =>
    simp only [Waker.step] at mem
    split at mem <;> simp only [List.mem_singleton, Prod.mk.injEq] at mem <;>
      obtain ⟨rfl, rfl⟩ := mem
    · rename_i handle
      rw [Producer.at_update]
      split
      · rename_i same
        rw [same.1]
        exact (fits k).claimed _
      · rename_i outside
        have inside : k < ps.length :=
          Producer.inside fun fresh => by simp [fresh, Producer.fresh] at handle
        exact quiet fun route delivers => by
          cases route <;> simp only [Waker.Delivers] at delivers
          exact outside ⟨delivers, inside⟩
    · rename_i handle
      by_cases same : j = k
      · subst same
        exact (fits j).unclaimed (by simpa using handle)
          (Or.imp_right fun delivers => delivers.elim)
      · exact quiet fun route delivers => by
          cases route <;> simp only [Waker.Delivers] at delivers
          exact same delivers

end Park

end Qtz
