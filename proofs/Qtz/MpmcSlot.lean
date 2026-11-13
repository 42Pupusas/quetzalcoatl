import Qtz.MpmcGeometry

/-!
One mpmc slot as a state machine over unwrapped rounds, for
`ReadyWord`, `DoneWord` and the producer, consumer and abandon paths
(`src/mpmc/{ready_word,done_word,producer,consumer,batch_abandon}.rs`).

Each step is atomic and sequentially consistent: memory orderings are
not modeled. The batch bitmap, the shared claim cursor and the other
slots are not modeled either. A position reserved at this slot is
a `round`; the `done` word is derived from `doneRound`, the number of
rounds that have been released.

`reserve` is guarded by `round < doneRound + wraps`. That window is an
assumption about the rest of the ring, not something this file proves:
a `done` word names a round only modulo `wraps`, so a producer can only
tell its round from the current one while they are less than `wraps`
apart.
-/

namespace Qtz

theorem eq_of_mod_eq_of_close {behind ahead window : Nat} (ordered : behind ≤ ahead)
    (close : ahead < behind + window) (same : behind % window = ahead % window) :
    ahead = behind := by
  obtain ⟨gap, rfl⟩ := Nat.exists_eq_add_of_le ordered
  have gap_multiple := Nat.sub_mod_eq_zero_of_mod_eq same.symm
  rw [Nat.add_sub_cancel_left] at gap_multiple
  have gap_below : gap < window := by omega
  rw [Nat.mod_eq_of_lt gap_below] at gap_multiple
  omega

inductive Phase where
  | unreserved
  | reserved
  | writing
  | published
  | claimed
  | released
  deriving DecidableEq

def Phase.inProgress : Phase → Bool
  | .writing | .published | .claimed => true
  | _ => false

structure SlotModel (g : Geometry) where
  doneRound : Nat
  readyRound : Nat
  readyState : Nat
  phase : Nat → Phase

namespace SlotModel

variable {g : Geometry}

def initial (g : Geometry) : SlotModel g := ⟨0, 0, 0, fun _ => .unreserved⟩

/-- The same slot data read under another geometry. The fields do not mention the
geometry, so a ring can store its slots under one geometry and view each under its own. -/
def retag (s : SlotModel g) (g' : Geometry) : SlotModel g' :=
  ⟨s.doneRound, s.readyRound, s.readyState, s.phase⟩

def setPhase (s : SlotModel g) (round : Nat) (phase : Phase) : Nat → Phase :=
  fun other => if other = round then phase else s.phase other

def readyWord (s : SlotModel g) : BitVec g.width := g.word s.readyRound s.readyState

def doneWord (s : SlotModel g) : BitVec g.width := g.roundPosition s.doneRound

inductive Step : SlotModel g → SlotModel g → Prop
  | reserve {s : SlotModel g} (round : Nat) :
      s.phase round = .unreserved → round < s.doneRound + g.wraps →
      Step s { s with phase := s.setPhase round .reserved }
  | beginWrite {s : SlotModel g} (round : Nat) :
      s.phase round = .reserved → s.doneWord = g.roundPosition round →
      Step s { s with phase := s.setPhase round .writing }
  | publish {s : SlotModel g} (round : Nat) :
      s.phase round = .writing →
      Step s { s with phase := s.setPhase round .published, readyRound := round, readyState := 1 }
  | unwrite {s : SlotModel g} (round : Nat) :
      s.phase round = .writing →
      Step s { s with phase := s.setPhase round .reserved }
  | claim {s : SlotModel g} (position : BitVec g.width) :
      decodeReady g.capacityWord s.readyWord g.slotWord = .published position →
      Step s { s with phase := s.setPhase s.readyRound .claimed, readyState := 2 }
  | release {s : SlotModel g} :
      s.phase s.doneRound = .claimed →
      Step s { s with phase := s.setPhase s.doneRound .released, doneRound := s.doneRound + 1 }
  | abandon {s : SlotModel g} (round : Nat) :
      s.phase round = .reserved → s.doneWord = g.roundPosition round →
      Step s ⟨round + 1, round, 2, s.setPhase round .released⟩

inductive Reachable : SlotModel g → Prop
  | initial : Reachable (initial g)
  | step {s t : SlotModel g} : Reachable s → Step s t → Reachable t

structure Inv (s : SlotModel g) : Prop where
  state_valid : s.readyState < 3
  below_done_released : ∀ round, round < s.doneRound → s.phase round = .released
  released_below_done : ∀ round, s.phase round = .released → round < s.doneRound
  in_progress_is_current : ∀ round, (s.phase round).inProgress = true → round = s.doneRound
  published_iff_ready :
    s.readyState = 1 ↔ (s.readyRound = s.doneRound ∧ s.phase s.doneRound = .published)
  within_window : ∀ round, s.phase round ≠ .unreserved → s.phase round ≠ .released →
    round < s.doneRound + g.wraps

theorem setPhase_self (s : SlotModel g) (round : Nat) (phase : Phase) :
    s.setPhase round phase round = phase := by
  simp [setPhase]

theorem setPhase_other (s : SlotModel g) {round other : Nat} (phase : Phase) (different : other ≠ round) :
    s.setPhase round phase other = s.phase other := by
  simp [setPhase, different]

theorem Inv.initial (g : Geometry) : Inv (initial g) where
  state_valid := by show 0 < 3; omega
  below_done_released round below := absurd below (Nat.not_lt_zero _)
  released_below_done round released := by simp [SlotModel.initial] at released
  in_progress_is_current round progress := by simp [SlotModel.initial, Phase.inProgress] at progress
  published_iff_ready := by simp [SlotModel.initial]
  within_window round unreserved _ := (unreserved rfl).elim

theorem Inv.done_word_round {s : SlotModel g} (inv : Inv s) {round : Nat}
    (reserved : s.phase round = .reserved) (done_matches : s.doneWord = g.roundPosition round) :
    round = s.doneRound := by
  have same_residue := g.roundPosition_cancel_wraps s.doneRound round done_matches
  have not_below : s.doneRound ≤ round := Nat.le_of_not_lt fun below => by
    have released := inv.below_done_released round below
    rw [reserved] at released
    exact absurd released (by decide)
  have within := inv.within_window round (by rw [reserved]; decide) (by rw [reserved]; decide)
  exact eq_of_mod_eq_of_close not_below within same_residue

theorem Inv.claim_reads_published {s : SlotModel g} (inv : Inv s) {position : BitVec g.width}
    (decoded : decodeReady g.capacityWord s.readyWord g.slotWord = .published position) :
    s.readyState = 1 := by
  have valid := inv.state_valid
  unfold SlotModel.readyWord at decoded
  rw [g.decode_word _ _ valid] at decoded
  by_cases zero : s.readyState = 0
  · simp [zero] at decoded
  · by_cases one : s.readyState = 1
    · exact one
    · simp [zero, one] at decoded

theorem Inv.step {s t : SlotModel g} (inv : Inv s) (step : Step s t) : Inv t := by
  have set_at : ∀ (round other : Nat) (phase : Phase),
      s.setPhase round phase other = if other = round then phase else s.phase other :=
    fun _ _ _ => rfl
  cases step with
  | reserve round unreserved guard =>
    refine ⟨inv.state_valid, ?_, ?_, ?_, ?_, ?_⟩
    all_goals try dsimp only
    · intro r below
      by_cases same : r = round
      · subst same
        have released := inv.below_done_released r below
        rw [unreserved] at released
        exact absurd released (by decide)
      · simp only [set_at, same, ite_false]; exact inv.below_done_released r below
    · intro r released
      by_cases same : r = round
      · subst same; simp [set_at] at released
      · simp only [set_at, same, ite_false] at released; exact inv.released_below_done r released
    · intro r progress
      by_cases same : r = round
      · subst same; simp [set_at, Phase.inProgress] at progress
      · simp only [set_at, same, ite_false] at progress; exact inv.in_progress_is_current r progress
    · have published_unchanged : (s.setPhase round .reserved s.doneRound = .published) ↔
          s.phase s.doneRound = .published := by
        by_cases same : s.doneRound = round
        · rw [same]; simp [set_at, unreserved]
        · simp [set_at, same]
      simp only [published_unchanged]
      exact inv.published_iff_ready
    · intro r not_unreserved not_released
      by_cases same : r = round
      · subst same; exact guard
      · simp only [set_at, same, ite_false] at not_unreserved not_released
        exact inv.within_window r not_unreserved not_released
  | beginWrite round reserved done_matches =>
    have at_done := inv.done_word_round reserved done_matches
    refine ⟨inv.state_valid, ?_, ?_, ?_, ?_, ?_⟩
    all_goals try dsimp only
    · intro r below
      by_cases same : r = round
      · omega
      · simp only [set_at, same, ite_false]; exact inv.below_done_released r below
    · intro r released
      by_cases same : r = round
      · subst same; simp [set_at] at released
      · simp only [set_at, same, ite_false] at released; exact inv.released_below_done r released
    · intro r progress
      by_cases same : r = round
      · omega
      · simp only [set_at, same, ite_false] at progress; exact inv.in_progress_is_current r progress
    · constructor
      · intro one
        have published := (inv.published_iff_ready.1 one).2
        rw [← at_done, reserved] at published
        exact absurd published (by decide)
      · rintro ⟨_, published⟩
        rw [← at_done] at published
        simp [set_at] at published
    · intro r not_unreserved not_released
      by_cases same : r = round
      · subst same
        exact inv.within_window r (by rw [reserved]; decide) (by rw [reserved]; decide)
      · simp only [set_at, same, ite_false] at not_unreserved not_released
        exact inv.within_window r not_unreserved not_released
  | publish round writing =>
    have at_done := inv.in_progress_is_current round (by rw [writing]; rfl)
    refine ⟨by show 1 < 3; omega, ?_, ?_, ?_, ?_, ?_⟩
    all_goals try dsimp only
    · intro r below
      by_cases same : r = round
      · omega
      · simp only [set_at, same, ite_false]; exact inv.below_done_released r below
    · intro r released
      by_cases same : r = round
      · subst same; simp [set_at] at released
      · simp only [set_at, same, ite_false] at released; exact inv.released_below_done r released
    · intro r progress
      by_cases same : r = round
      · exact same.trans at_done
      · simp only [set_at, same, ite_false] at progress; exact inv.in_progress_is_current r progress
    · constructor
      · intro _
        refine ⟨at_done, ?_⟩
        rw [← at_done]; simp [set_at]
      · intro _; rfl
    · intro r not_unreserved not_released
      by_cases same : r = round
      · subst same
        exact inv.within_window r (by rw [writing]; decide) (by rw [writing]; decide)
      · simp only [set_at, same, ite_false] at not_unreserved not_released
        exact inv.within_window r not_unreserved not_released
  | unwrite round writing =>
    have at_done := inv.in_progress_is_current round (by rw [writing]; rfl)
    refine ⟨inv.state_valid, ?_, ?_, ?_, ?_, ?_⟩
    all_goals try dsimp only
    · intro r below
      by_cases same : r = round
      · omega
      · simp only [set_at, same, ite_false]; exact inv.below_done_released r below
    · intro r released
      by_cases same : r = round
      · subst same; simp [set_at] at released
      · simp only [set_at, same, ite_false] at released; exact inv.released_below_done r released
    · intro r progress
      by_cases same : r = round
      · subst same; simp [set_at, Phase.inProgress] at progress
      · simp only [set_at, same, ite_false] at progress; exact inv.in_progress_is_current r progress
    · have published_unchanged : (s.setPhase round .reserved s.doneRound = .published) ↔
          s.phase s.doneRound = .published := by
        by_cases same : s.doneRound = round
        · rw [same]; simp [set_at, writing]
        · simp [set_at, same]
      simp only [published_unchanged]
      exact inv.published_iff_ready
    · intro r not_unreserved not_released
      by_cases same : r = round
      · subst same
        exact inv.within_window r (by rw [writing]; decide) (by rw [writing]; decide)
      · simp only [set_at, same, ite_false] at not_unreserved not_released
        exact inv.within_window r not_unreserved not_released
  | claim position decoded =>
    have state_one := inv.claim_reads_published decoded
    obtain ⟨ready_current, published⟩ := inv.published_iff_ready.1 state_one
    refine ⟨by show 2 < 3; omega, ?_, ?_, ?_, ?_, ?_⟩
    all_goals try dsimp only
    · intro r below
      by_cases same : r = s.readyRound
      · omega
      · simp only [set_at, same, ite_false]; exact inv.below_done_released r below
    · intro r released
      by_cases same : r = s.readyRound
      · subst same; simp [set_at] at released
      · simp only [set_at, same, ite_false] at released; exact inv.released_below_done r released
    · intro r progress
      by_cases same : r = s.readyRound
      · exact same.trans ready_current
      · simp only [set_at, same, ite_false] at progress; exact inv.in_progress_is_current r progress
    · constructor
      · intro two; exact absurd two (by show ¬ (2 = 1); omega)
      · rintro ⟨_, claimed⟩
        rw [← ready_current] at claimed
        simp [set_at] at claimed
    · intro r not_unreserved not_released
      by_cases same : r = s.readyRound
      · have := g.wraps_positive; omega
      · simp only [set_at, same, ite_false] at not_unreserved not_released
        exact inv.within_window r not_unreserved not_released
  | release claimed =>
    have not_published : s.readyState ≠ 1 := by
      intro one
      have published := (inv.published_iff_ready.1 one).2
      rw [claimed] at published
      exact absurd published (by decide)
    refine ⟨inv.state_valid, ?_, ?_, ?_, ?_, ?_⟩
    all_goals try dsimp only
    · intro r below
      by_cases same : r = s.doneRound
      · subst same; simp [set_at]
      · simp only [set_at, same, ite_false]
        exact inv.below_done_released r (by omega)
    · intro r released
      by_cases same : r = s.doneRound
      · omega
      · simp only [set_at, same, ite_false] at released
        have := inv.released_below_done r released
        omega
    · intro r progress
      by_cases same : r = s.doneRound
      · subst same; simp [set_at, Phase.inProgress] at progress
      · simp only [set_at, same, ite_false] at progress
        exact absurd (inv.in_progress_is_current r progress) same
    · constructor
      · intro one; exact absurd one not_published
      · rintro ⟨_, published⟩
        have different : s.doneRound + 1 ≠ s.doneRound := by omega
        simp only [set_at, different, ite_false] at published
        have := inv.in_progress_is_current _ (by rw [published]; rfl)
        omega
    · intro r not_unreserved not_released
      by_cases same : r = s.doneRound
      · subst same; simp [set_at] at not_released
      · simp only [set_at, same, ite_false] at not_unreserved not_released
        have := inv.within_window r not_unreserved not_released
        omega
  | abandon round reserved done_matches =>
    have at_done := inv.done_word_round reserved done_matches
    refine ⟨by show 2 < 3; omega, ?_, ?_, ?_, ?_, ?_⟩
    all_goals try dsimp only
    · intro r below
      by_cases same : r = round
      · subst same; simp [set_at]
      · simp only [set_at, same, ite_false]
        exact inv.below_done_released r (by omega)
    · intro r released
      by_cases same : r = round
      · omega
      · simp only [set_at, same, ite_false] at released
        have := inv.released_below_done r released
        omega
    · intro r progress
      by_cases same : r = round
      · subst same; simp [set_at, Phase.inProgress] at progress
      · simp only [set_at, same, ite_false] at progress
        exact absurd ((inv.in_progress_is_current r progress).trans at_done.symm) same
    · constructor
      · intro two; exact absurd two (by show ¬ (2 = 1); omega)
      · rintro ⟨ready_at_done, _⟩
        omega
    · intro r not_unreserved not_released
      by_cases same : r = round
      · subst same; simp [set_at] at not_released
      · simp only [set_at, same, ite_false] at not_unreserved not_released
        have := inv.within_window r not_unreserved not_released
        omega

theorem Reachable.inv {s : SlotModel g} (reachable : Reachable s) : Inv s := by
  induction reachable with
  | initial => exact Inv.initial g
  | step _ step inv => exact inv.step step

/-- At most one round at a time owns the slot's storage. -/
theorem Reachable.storage_has_one_owner {s : SlotModel g} (reachable : Reachable s)
    {first second : Nat} (first_owns : (s.phase first).inProgress = true)
    (second_owns : (s.phase second).inProgress = true) : first = second :=
  (reachable.inv.in_progress_is_current first first_owns).trans
    (reachable.inv.in_progress_is_current second second_owns).symm

/-- A producer that passes the `done` check writes at the round the slot
was released for, and never over a value that is still unread. -/
theorem Reachable.write_starts_at_released_round {s : SlotModel g} (reachable : Reachable s)
    {round : Nat} (reserved : s.phase round = .reserved)
    (done_matches : s.doneWord = g.roundPosition round) :
    round = s.doneRound ∧ s.readyState ≠ 1 := by
  have at_done := reachable.inv.done_word_round reserved done_matches
  refine ⟨at_done, fun one => ?_⟩
  have published := (reachable.inv.published_iff_ready.1 one).2
  rw [← at_done, reserved] at published
  exact absurd published (by decide)

/-- A claim succeeds only on a value published at the round the slot is
on, so a consumer never reads a value from a different round. -/
theorem Reachable.claim_takes_current_round {s : SlotModel g} (reachable : Reachable s)
    {position : BitVec g.width}
    (decoded : decodeReady g.capacityWord s.readyWord g.slotWord = .published position) :
    s.readyRound = s.doneRound ∧ s.phase s.doneRound = .published := by
  exact reachable.inv.published_iff_ready.1 (reachable.inv.claim_reads_published decoded)

end SlotModel

end Qtz
