//! The cooperative scheduler of a replay.
//!
//! Each actor runs on its own thread and stops at every
//! [`ReplayPoint`]. Only the actor holding a grant runs, from the point
//! it stopped at to the next one, so each grant is one model step. In a
//! replay a park never blocks: the session keeps the park tokens and
//! grants a park only when its token is set, or as a spurious return.

use std::any::Any;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{JoinHandle, Thread, ThreadId};

use crate::common::replay_point::{ReplayActor, ReplayPoint, Scheduler};

/// How an actor is to take its next step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Grant {
    Step,
    /// A claim that fails then spins (`true`) or arms (`false`).
    Spin(bool),
    /// A return from `park` that nothing caused.
    Spurious,
    /// A release of this many slots.
    Batch(usize),
}

/// What the controller sees of one actor between steps.
#[derive(Clone, Debug)]
pub struct ActorView {
    pub point: Option<ReplayPoint>,
    pub finished: bool,
    pub token: bool,
    pub thread: Option<ThreadId>,
    /// At [`ReplayPoint::Unpark`], the threads still to unpark.
    pub pending: Vec<ThreadId>,
    /// The count of the wake loop the actor last entered.
    pub count: usize,
}

struct Actor {
    view: ActorView,
    grant: Option<Grant>,
    last: Option<Grant>,
}

struct Inner {
    actors: Vec<Actor>,
    running: Option<usize>,
    aborted: bool,
}

/// The unwind payload that ends an actor of an abandoned run.
struct Aborted;

pub struct Session {
    inner: Mutex<Inner>,
    turn: Condvar,
}

impl Scheduler for Session {
    fn stop(&self, me: usize, point: ReplayPoint, pending: Vec<ThreadId>) -> bool {
        self.wait_turn(me, point, pending)
    }

    fn spins(&self, me: usize) -> bool {
        self.lock().actors[me].last == Some(Grant::Spin(true))
    }

    fn unparked(&self, thread: &Thread) {
        let mut inner = self.lock();
        if let Some(actor) = inner
            .actors
            .iter_mut()
            .find(|actor| actor.view.thread == Some(thread.id()))
        {
            actor.view.token = true;
        }
    }

    fn finish(&self, me: usize) {
        {
            let mut inner = self.lock();
            let actor = &mut inner.actors[me];
            actor.view.finished = true;
            actor.view.point = None;
            if inner.running == Some(me) {
                inner.running = None;
            }
        }
        self.turn.notify_all();
    }
}

impl Session {
    pub fn new(actors: usize) -> Arc<Self> {
        let actors = (0..actors)
            .map(|_| Actor {
                view: ActorView {
                    point: None,
                    finished: false,
                    token: false,
                    thread: None,
                    pending: Vec::new(),
                    count: 0,
                },
                grant: None,
                last: None,
            })
            .collect();
        Arc::new(Self {
            inner: Mutex::new(Inner {
                actors,
                running: None,
                aborted: false,
            }),
            turn: Condvar::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `body` as actor `index` on a thread of its own.
    pub fn spawn<R: Send + 'static>(
        self: &Arc<Self>,
        index: usize,
        body: impl FnOnce() -> R + Send + 'static,
    ) -> JoinHandle<R> {
        let session = Arc::clone(self);
        std::thread::spawn(move || {
            session.lock().actors[index].view.thread = Some(std::thread::current().id());
            let _actor = ReplayActor::enter(session, index);
            body()
        })
    }

    /// Stops actor `me` at `point` until it is granted a step. `false`
    /// when the run was abandoned while the actor unwinds.
    fn wait_turn(&self, me: usize, point: ReplayPoint, pending: Vec<ThreadId>) -> bool {
        let mut inner = self.lock();
        if inner.aborted && std::thread::panicking() {
            return false;
        }
        let view = &mut inner.actors[me].view;
        view.point = Some(point);
        view.pending = pending;
        if let ReplayPoint::Drain { count } = point {
            view.count = count;
        }
        if inner.running == Some(me) {
            inner.running = None;
        }
        self.turn.notify_all();
        loop {
            if inner.aborted {
                drop(inner);
                std::panic::resume_unwind(Box::new(Aborted));
            }
            let actor = &mut inner.actors[me];
            if let Some(grant) = actor.grant.take() {
                actor.view.point = None;
                actor.last = Some(grant);
                return true;
            }
            inner = self
                .turn
                .wait(inner)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// The batch actor `me` was last granted.
    pub fn granted_batch(&self, me: usize) -> usize {
        let last = self.lock().actors[me].last;
        match last {
            Some(Grant::Batch(batch)) => batch,
            other => panic!("a release was granted {other:?}"),
        }
    }

    /// Waits until every actor has stopped at a point or finished.
    pub fn settle(&self) {
        let mut inner = self.lock();
        while inner.running.is_some()
            || inner
                .actors
                .iter()
                .any(|actor| !actor.view.finished && actor.view.point.is_none())
        {
            inner = self
                .turn
                .wait(inner)
                .unwrap_or_else(PoisonError::into_inner);
        }
        drop(inner);
    }

    /// Runs one step of actor `index` and waits for it to stop again.
    pub fn grant(&self, index: usize, grant: Grant) {
        {
            let mut inner = self.lock();
            let actor = &mut inner.actors[index];
            assert!(
                !actor.view.finished && actor.view.point.is_some(),
                "actor {index} cannot step"
            );
            if grant == Grant::Step && actor.view.point == Some(ReplayPoint::Park) {
                assert!(actor.view.token, "actor {index} parks without a token");
                actor.view.token = false;
            }
            actor.grant = Some(grant);
            inner.running = Some(index);
            drop(inner);
            self.turn.notify_all();
        }
        self.settle();
    }

    pub fn view(&self) -> Vec<ActorView> {
        self.lock()
            .actors
            .iter()
            .map(|actor| actor.view.clone())
            .collect()
    }

    /// Ends the run: every actor still waiting unwinds.
    pub fn abort(&self) {
        self.lock().aborted = true;
        self.turn.notify_all();
    }

    /// Re-raises an actor's panic unless it is the unwind of
    /// [`abort`](Self::abort).
    pub fn rethrow(payload: Box<dyn Any + Send>) {
        if !payload.is::<Aborted>() {
            std::panic::resume_unwind(payload);
        }
    }
}
