//! Exhaustive exploration of a replay, checked edge by edge against the
//! model graph.

use std::any::Any;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;

use super::fixture::{Edge, ModelGraph, RunConfig};
use super::session::{Grant, Session};
use super::snapshot::{ReplayTarget, Snapshot};
use crate::common::park_registry::ParkSlot;
use crate::common::replay_point::ReplayPoint;

/// One running replay: its actors stopped at their points.
pub struct Live {
    pub session: Arc<Session>,
    pub target: Box<dyn ReplayTarget>,
    /// Each producer's park slot.
    pub slots: Vec<ParkSlot>,
    pub threads: Vec<JoinHandle<Box<dyn Any + Send>>>,
    pub leases: Vec<bool>,
    pub left: usize,
    /// Whether one release can free several slots behind a single wake
    /// loop (mpsc `drain`); otherwise each release frees one.
    pub batched: bool,
}

impl Live {
    /// The model's encoding of the current state, and the key that
    /// also holds what the model abstracts away.
    fn state(&self) -> (String, String) {
        let views = self.session.view();
        let snapshot = Snapshot {
            target: self.target.as_ref(),
            slots: &self.slots,
            leases: &self.leases,
            views: &views,
            left: self.left,
        };
        (snapshot.model(), snapshot.key())
    }

    /// The starting cursors that put each leased producer first in the
    /// round-robin. The model's waker may pick any set bit; the Rust
    /// picks the first one after the cursor, so only varying where the
    /// cursor starts reaches every pick.
    fn turns(&self) -> Vec<u64> {
        let mut turns: Vec<u64> = self
            .slots
            .iter()
            .filter_map(|slot| slot.index())
            .map(|index| index as u64)
            .collect();
        if turns.is_empty() {
            turns.push(0);
        }
        turns
    }

    /// Points the round-robin cursor at `cursor` before any actor has
    /// read it: every actor's first stop comes before its wake loop.
    fn turn_to(&self, cursor: u64) {
        self.target
            .wake_set()
            .cursor
            .store(cursor, Ordering::Relaxed);
    }

    /// Whether a producer stopped at `point` fails its lap and then
    /// chooses between spinning and arming.
    fn fails_lap(&self, point: ReplayPoint) -> bool {
        match point {
            ReplayPoint::Attempt => self.target.free() == 0,
            ReplayPoint::TopClosed => !self.target.closed(),
            _ => false,
        }
    }

    /// Every step some actor can take now.
    fn enabled(&self) -> Vec<(usize, Grant)> {
        let mut grants = Vec::new();
        for (actor, view) in self.session.view().into_iter().enumerate() {
            match view.point {
                None => {}
                Some(ReplayPoint::Release) => {
                    let most = if self.batched { self.left } else { 1 };
                    grants.extend((1..=most).map(|batch| (actor, Grant::Batch(batch))));
                }
                Some(ReplayPoint::Park) => {
                    if view.token {
                        grants.push((actor, Grant::Step));
                    }
                    grants.push((actor, Grant::Spurious));
                }
                Some(point) if actor > 0 && self.fails_lap(point) => {
                    grants.push((actor, Grant::Spin(true)));
                    grants.push((actor, Grant::Spin(false)));
                }
                Some(_) => grants.push((actor, Grant::Step)),
            }
        }
        grants
    }

    fn apply(&mut self, actor: usize, grant: Grant) {
        if let Grant::Batch(batch) = grant {
            self.left -= batch;
        }
        self.session.grant(actor, grant);
    }

    /// Ends the run and re-raises any actor's own panic.
    fn close(self) {
        self.session.abort();
        for thread in self.threads {
            if let Err(payload) = thread.join() {
                Session::rethrow(payload);
            }
        }
    }
}

/// A run's starting cursor and the steps granted since.
#[derive(Clone, Debug)]
struct Schedule {
    cursor: u64,
    steps: Vec<(usize, Grant)>,
}

impl Schedule {
    fn then(&self, actor: usize, grant: Grant) -> Self {
        let mut next = self.clone();
        next.steps.push((actor, grant));
        next
    }
}

/// The model's actor label for a step.
struct Label;

impl Label {
    fn of(actor: usize, grant: Grant) -> String {
        match (actor, grant) {
            (0, _) => "c".to_owned(),
            (k, Grant::Spurious) => format!("z{}", k - 1),
            (k, _) => format!("p{}", k - 1),
        }
    }
}

pub struct Explorer<'a> {
    graph: &'a ModelGraph,
    start: &'a dyn Fn(&RunConfig) -> Live,
    seen: HashSet<String>,
    used: HashSet<Edge>,
    runs: usize,
}

impl<'a> Explorer<'a> {
    pub fn new(graph: &'a ModelGraph, start: &'a dyn Fn(&RunConfig) -> Live) -> Self {
        Self {
            graph,
            start,
            seen: HashSet::new(),
            used: HashSet::new(),
            runs: 0,
        }
    }

    /// Explores every schedule and reports coverage on stderr.
    ///
    /// # Panics
    ///
    /// Panics on a Rust state or step the model does not have.
    pub fn run(mut self) {
        let probe = (self.start)(&self.graph.config);
        let turns = probe.turns();
        probe.close();
        let mut queue = VecDeque::new();
        for cursor in turns {
            let root = Schedule {
                cursor,
                steps: Vec::new(),
            };
            let start = self.replay(&root);
            let (_, _, key) = self.locate(&start, &root);
            start.close();
            if self.seen.insert(key) {
                queue.push_back(root);
            }
        }
        while let Some(schedule) = queue.pop_front() {
            self.expand(&schedule, &mut queue);
        }
        let unused = self.graph.unused(&self.used);
        eprintln!(
            "{}: {} Rust states in {} runs; {} of {} model edges taken, {} unused",
            self.graph.name,
            self.seen.len(),
            self.runs,
            self.used.len(),
            self.graph.edge_count(),
            unused.len(),
        );
        for (from, to, actor) in unused.iter().take(5) {
            eprintln!(
                "  unused {actor}: {} -> {}",
                self.graph.encoding(*from),
                self.graph.encoding(*to)
            );
        }
        assert!(self.graph.state_count() > 0);
    }

    fn replay(&mut self, schedule: &Schedule) -> Live {
        self.runs += 1;
        let mut live = (self.start)(&self.graph.config);
        live.session.settle();
        live.turn_to(schedule.cursor);
        for &(actor, grant) in &schedule.steps {
            live.apply(actor, grant);
        }
        live
    }

    /// The model state the run is in, whether the export expanded it,
    /// and the run's dedup key.
    fn locate(&self, live: &Live, schedule: &Schedule) -> (usize, bool, String) {
        let (model, key) = live.state();
        let (id, expanded) = self.graph.state(&model).unwrap_or_else(|| {
            panic!(
                "{}: the Rust reached {model}, which the model does not, by {schedule:?}",
                self.graph.name
            )
        });
        (id, expanded, key)
    }

    /// Takes every step enabled at the end of `schedule`, checks each
    /// against the model, and queues the schedules that reach a state
    /// not seen before. Breadth first, so every schedule is a shortest
    /// one and a replay stays short.
    fn expand(&mut self, schedule: &Schedule, queue: &mut VecDeque<Schedule>) {
        let live = self.replay(schedule);
        let (from, expanded, _) = self.locate(&live, schedule);
        if !expanded {
            live.close();
            return;
        }
        let grants = live.enabled();
        let mut current = Some(live);
        for (actor, grant) in grants {
            let mut run = current.take().unwrap_or_else(|| self.replay(schedule));
            run.apply(actor, grant);
            let next = schedule.then(actor, grant);
            let (to, _, key) = self.locate(&run, &next);
            run.close();
            let edge = (from, to, Label::of(actor, grant));
            assert!(
                self.graph.has_edge(&edge),
                "{}: the Rust stepped {} -> {} as {}, an edge the model does not have, by {next:?}",
                self.graph.name,
                self.graph.encoding(from),
                self.graph.encoding(to),
                edge.2,
            );
            self.used.insert(edge);
            if self.seen.insert(key) {
                queue.push_back(next);
            }
        }
    }
}
