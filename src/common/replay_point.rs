//! Stops at the step boundaries of the park model, for the schedule
//! replay.
//!
//! `proofs/Qtz/ParkModel.lean` takes each atomic access of the park
//! handshake as one step. The replay ([`park_replay`]) runs the real
//! blocking code one step at a time and checks every step against the
//! graph the model exports, so each stop names the model step about to
//! run.
//!
//! The stops inside [`WakeSet`](super::park::WakeSet) go through its
//! gate, so only the set a replay follows stops there. A thread
//! outside a replay passes straight through. Outside tests every call
//! compiles to nothing.
//!
//! [`park_replay`]: super::park_replay

use super::atomics::thread::Thread;

/// The model step a thread is about to take.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(not(all(test, not(loom))), allow(dead_code))]
pub enum ReplayPoint {
    /// Consumer: the next release.
    Release,
    /// Consumer: the drop that closes the ring.
    Close,
    /// Producer loop: the first access at the top, the closed check
    /// (mpsc, broadcast) or the claim (mpmc).
    Top,
    /// Producer loop: the claim (mpsc), or the look for a free slot
    /// (broadcast).
    Attempt,
    /// mpmc producer loop: the closed check after a failed claim.
    TopClosed,
    /// Broadcast producer loop: the claim after seeing a free slot.
    Take,
    /// Publishing the park handle, or pushing onto the overflow stack.
    Handle,
    /// Publishing the wake bit.
    Bit,
    /// The first access after arming: the closed check (mpsc,
    /// broadcast) or the claim (mpmc).
    Recheck,
    /// The ring access after the closed check: the claim (mpsc), or
    /// the look for a free slot (broadcast).
    RecheckRing,
    /// mpmc: the closed check after the claim after arming.
    RecheckClosed,
    Park,
    /// Clearing the bit after a park.
    DisarmWoken,
    /// Standing down after the re-check claimed a slot.
    DisarmWon,
    /// Standing down after the re-check saw the close.
    DisarmClosed,
    /// Taking the overflow stack, for a wake loop owing `count` wakes,
    /// or for a close when `count` is 0.
    Drain {
        count: usize,
    },
    /// Unparking the first of the entries taken from the stack.
    Unpark,
    /// Done with the entries taken from the stack.
    UnparkDone,
    /// Loading the bitmap. `cursor` is the round-robin cursor the pick
    /// will use.
    Scan {
        cursor: u64,
        count: usize,
    },
    /// Clearing the bit of `slot`.
    Clear {
        slot: u32,
        count: usize,
        cursor: u64,
    },
    /// Claiming the handle of `slot`.
    Claim {
        slot: u32,
        count: usize,
        cursor: u64,
    },
    /// Close: swapping the bitmap to zero.
    Flush,
    /// Close: claiming the handle of the lowest slot in `bits`, the
    /// slots the swap took that are still to be visited.
    Flushing {
        bits: u64,
    },
    /// Close: done with the slots the swap took.
    FlushingDone,
}

/// What a stop hands control to: the replay's scheduler.
#[cfg(all(test, not(loom)))]
pub trait Scheduler: Send + Sync {
    /// Stops actor `me` at `point` until it is granted a step, naming
    /// the threads a stack drain still has to unpark. `false` when the
    /// stop was skipped.
    fn stop(&self, me: usize, point: ReplayPoint, pending: Vec<std::thread::ThreadId>) -> bool;
    /// Actor `me`'s choice at a backoff gate: `true` to spin again.
    fn spins(&self, me: usize) -> bool;
    /// Records an unpark of `thread` as its park token.
    fn unparked(&self, thread: &Thread);
    /// Marks actor `me` finished.
    fn finish(&self, me: usize);
}

/// Makes its thread an actor of a replay until dropped, then marks the
/// actor finished however its body ended.
#[cfg(all(test, not(loom)))]
pub struct ReplayActor {
    scheduler: std::sync::Arc<dyn Scheduler>,
    index: usize,
}

#[cfg(all(test, not(loom)))]
thread_local! {
    static ACTOR: std::cell::RefCell<Option<(std::sync::Arc<dyn Scheduler>, usize)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, not(loom)))]
impl ReplayActor {
    /// Makes the calling thread actor `index` of `scheduler`.
    #[must_use]
    pub fn enter(scheduler: std::sync::Arc<dyn Scheduler>, index: usize) -> Self {
        let entry = (std::sync::Arc::clone(&scheduler), index);
        ACTOR.with(|actor| *actor.borrow_mut() = Some(entry));
        Self { scheduler, index }
    }

    fn current() -> Option<(std::sync::Arc<dyn Scheduler>, usize)> {
        ACTOR.with(|actor| actor.borrow().clone())
    }
}

#[cfg(all(test, not(loom)))]
impl Drop for ReplayActor {
    fn drop(&mut self) {
        ACTOR.with(|actor| actor.borrow_mut().take());
        self.scheduler.finish(self.index);
    }
}

#[cfg(all(test, not(loom)))]
impl ReplayPoint {
    /// Waits here until the replay schedules this thread.
    pub fn reach(self) {
        self.reach_unparking(Vec::new());
    }

    /// [`reach`](Self::reach), naming the threads a stack drain still
    /// has to unpark, the next one first.
    pub fn reach_unparking(self, pending: Vec<std::thread::ThreadId>) {
        if let Some((scheduler, me)) = ReplayActor::current() {
            scheduler.stop(me, self, pending);
        }
    }

    /// Stops at [`Park`](Self::Park). `true` when a replay is running,
    /// which then stands in for the park itself.
    pub fn park_in_replay() -> bool {
        ReplayActor::current()
            .is_some_and(|(scheduler, me)| scheduler.stop(me, Self::Park, Vec::new()))
    }

    /// The replay's choice at a backoff gate: `Some(true)` to spin
    /// again, `Some(false)` to park. `None` outside a replay.
    pub fn spin_verdict() -> Option<bool> {
        ReplayActor::current().map(|(scheduler, me)| scheduler.spins(me))
    }

    /// Records an unpark of `thread` as its park token.
    pub fn unparked(thread: &Thread) {
        if let Some((scheduler, _)) = ReplayActor::current() {
            scheduler.unparked(thread);
        }
    }
}

#[cfg(not(all(test, not(loom))))]
impl ReplayPoint {
    #[inline]
    #[allow(clippy::unused_self)]
    pub const fn reach(self) {}

    #[inline]
    pub const fn park_in_replay() -> bool {
        false
    }

    #[inline]
    pub const fn spin_verdict() -> Option<bool> {
        None
    }

    #[inline]
    pub const fn unparked(_thread: &Thread) {}
}
