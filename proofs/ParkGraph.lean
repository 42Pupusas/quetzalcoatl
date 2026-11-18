import Qtz.ParkModel
import Std.Data.HashMap

/-!
Exports the reachable step graph of the park model for the Rust schedule replay
(`src/common/park_replay`). `lake exe park_graph <name>` prints the graph of one configuration: the line
`c <variant> <leases> <releases> <closes> <depth>`, then one line per state, `s <id> <encoding>`,
then one line per edge, `e <from> <to> <actor>`. The actor is `c` for the consumer, `p<k>` for a
step of producer `k` and `z<k>` for its spurious return from `park`. The Rust side builds its run
from the `c` line. A state whose overflow stack is deeper than the depth is written `x` instead of
`s` and not expanded, because unleased producers that return spuriously can push without bound.

`lake exe park_graph` with no name lists the configurations and their sizes. The Rust test runs the exe itself, so
the graph it checks against is always the current model's.
-/

namespace Qtz

namespace Park

def Nat.encodeList (xs : List Nat) : String :=
  "[" ++ ",".intercalate (xs.map toString) ++ "]"

def Waker.encode : Waker → String
  | .drain c => s!"drain:{c}"
  | .unpark p c => s!"unpark:{Nat.encodeList p}:{c}"
  | .scan c => s!"scan:{c}"
  | .clear i c => s!"clear:{i}:{c}"
  | .claim i c => s!"claim:{i}:{c}"

def Pc.encode : Pc → String
  | .top => "top"
  | .attempt => "attempt"
  | .topClosed => "topClosed"
  | .take => "take"
  | .handle => "handle"
  | .bit => "bit"
  | .recheck => "recheck"
  | .recheckRing => "recheckRing"
  | .recheckClosed => "recheckClosed"
  | .park => "park"
  | .disarmWon => "disarmWon"
  | .disarmWoken => "disarmWoken"
  | .disarmClosed => "disarmClosed"
  | .forward w => s!"forward({w.encode})"
  | .done => "done"

def Producer.flag (set : Bool) (c : Char) : String :=
  if set then c.toString else "-"

def Producer.encode (p : Producer) : String :=
  p.pc.encode ++ ":" ++ Producer.flag p.leased 'L' ++ Producer.flag p.bit 'b' ++
    Producer.flag p.handle 'h' ++ Producer.flag p.token 't' ++ Producer.flag p.registered 'r'

def Consumer.encode : Consumer → String
  | .release => "release"
  | .waking w => s!"waking({w.encode})"
  | .close => "close"
  | .drainOverflow => "drainOverflow"
  | .unparking p => s!"unparking:{Nat.encodeList p}"
  | .flush => "flush"
  | .flushing p => s!"flushing:{Nat.encodeList p}"
  | .finished => "finished"

/-- `closes` and `capacity` are fixed for a run, so they are left out. -/
def State.encode (s : State) : String :=
  s!"{s.free}/{s.left}/{if s.closed then 1 else 0}/{s.consumer.encode}/" ++
    " ".intercalate (s.producers.map Producer.encode) ++ "/" ++ Nat.encodeList s.stack

def Variant.encode : Variant → String
  | .unrepaired => "unrepaired"
  | .repaired => "repaired"
  | .repairedWakingAll => "repairedWakingAll"
  | .observing => "observing"
  | .ignoresClose => "ignoresClose"

structure Config where
  name : String
  variant : Variant
  leases : List Bool
  releases : Nat
  closes : Bool
  depth : Nat

structure Graph where
  ids : Std.HashMap String Nat := {}
  states : Array State := #[]
  edges : Array (Nat × Nat × String) := #[]

/-- Every successor of `s` with the actor that steps to it; together these are `s.moves`. -/
def State.labelled (variant : Variant) (s : State) : List (State × String) :=
  (s.consumerSteps variant).map (·, "c") ++
    (List.range s.producers.length).flatMap (fun i =>
      (s.producerSteps variant i).map (·, s!"p{i}") ++ (s.spurious i).map (·, s!"z{i}"))

def Graph.intern (g : Graph) (s : State) : Graph × Nat :=
  let key := s.encode
  match g.ids[key]? with
  | some id => (g, id)
  | none => ({ g with ids := g.ids.insert key g.states.size, states := g.states.push s },
      g.states.size)

partial def Graph.expand (config : Config) (g : Graph) (next : Nat) : Graph :=
  if h : next < g.states.size then
    let s := g.states[next]
    if s.stack.length > config.depth then g.expand config (next + 1)
    else
      let g := (s.labelled config.variant).foldl (fun g (t, actor) =>
        let (g, id) := g.intern t
        { g with edges := g.edges.push (next, id, actor) }) g
      g.expand config (next + 1)
  else g

def Config.graph (config : Config) : Graph :=
  let (g, _) := ({} : Graph).intern
    (State.init config.leases config.releases config.closes)
  g.expand config 0

def Config.header (c : Config) : String :=
  let leases := String.join (c.leases.map fun leased => if leased then "L" else "S")
  s!"c {c.variant.encode} {leases} {c.releases} {if c.closes then 1 else 0} {c.depth}\n"

def Graph.render (g : Graph) (config : Config) : String := Id.run do
  let mut out := config.header
  for h : i in [0:g.states.size] do
    let s := g.states[i]'h.upper
    let mark := if s.stack.length > config.depth then "x" else "s"
    out := out ++ s!"{mark} {i} {s.encode}\n"
  for (a, b, actor) in g.edges do
    out := out ++ s!"e {a} {b} {actor}\n"
  return out

def Config.all : List Config :=
  [ ⟨"mpsc_two_leased", .repaired, [true, true], 1, true, 2⟩,
    ⟨"mpsc_leased_and_shared", .repaired, [true, false], 1, true, 2⟩,
    ⟨"mpsc_two_leased_two_releases", .repaired, [true, true], 2, false, 2⟩,
    ⟨"broadcast_two_leased", .observing, [true, true], 1, true, 2⟩,
    ⟨"broadcast_leased_and_shared", .observing, [true, false], 1, true, 2⟩,
    ⟨"broadcast_two_leased_two_releases", .observing, [true, true], 2, false, 2⟩,
    ⟨"mpmc_two_leased", .repairedWakingAll, [true, true], 1, true, 2⟩,
    ⟨"mpmc_two_leased_two_releases", .repairedWakingAll, [true, true], 2, false, 2⟩,
    ⟨"mpmc_two_leased_open", .repairedWakingAll, [true, true], 1, false, 2⟩,
    ⟨"mpmc_one_leased", .repairedWakingAll, [true], 1, true, 2⟩ ]

end Park

end Qtz

open Qtz.Park in
def main (args : List String) : IO UInt32 := do
  match args with
  | [name] =>
    match Config.all.find? (·.name == name) with
    | some config =>
      IO.print ((config.graph).render config)
      return 0
    | none =>
      IO.eprintln s!"no configuration {name}"
      return 1
  | _ =>
    for config in Config.all do
      let g := config.graph
      IO.println s!"{config.name}: {g.states.size} states, {g.edges.size} edges"
    return 0
