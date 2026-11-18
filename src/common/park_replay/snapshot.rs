//! Maps a stopped Rust run to the model's state encoding.

use std::thread::ThreadId;

use super::session::ActorView;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkSlot;
use crate::common::replay_point::ReplayPoint;

/// The ring a replay runs on, as far as the model sees it.
pub trait ReplayTarget: Send {
    /// The producer side's park state.
    fn wake_set(&self) -> &WakeSet;
    /// Whether the consumer has closed the ring.
    fn closed(&self) -> bool;
    /// Free slots: credits a claim can take.
    fn free(&self) -> usize;
}

/// One stopped moment of a run. Actor 0 is the consumer, actor `k + 1`
/// producer `k`.
pub struct Snapshot<'a> {
    pub target: &'a dyn ReplayTarget,
    pub slots: &'a [ParkSlot],
    pub leases: &'a [bool],
    pub views: &'a [ActorView],
    pub left: usize,
}

impl Snapshot<'_> {
    /// The model's encoding of this state (`State.encode` in
    /// `proofs/ParkGraph.lean`).
    pub fn model(&self) -> String {
        let set = self.target.wake_set();
        let stack = self.thread_list(&set.overflow.threads());
        let producers: Vec<String> = (0..self.slots.len()).map(|k| self.producer(k)).collect();
        format!(
            "{}/{}/{}/{}/{}/{}",
            self.target.free(),
            self.left,
            u8::from(self.target.closed()),
            self.consumer(&self.views[0]),
            producers.join(" "),
            stack
        )
    }

    /// The model encoding plus the Rust state the model abstracts away,
    /// so two runs that agree on it continue alike.
    pub fn key(&self) -> String {
        let set = self.target.wake_set();
        let points: Vec<String> = self
            .views
            .iter()
            .map(|view| format!("{:?}:{}", view.point, view.count))
            .collect();
        format!(
            "{} | {} | {}",
            self.model(),
            set.cursor.load(std::sync::atomic::Ordering::Relaxed),
            points.join(",")
        )
    }

    fn producer_of_slot(&self, slot: u32) -> usize {
        self.slots
            .iter()
            .position(|s| *s == ParkSlot::Leased(slot))
            .unwrap_or_else(|| panic!("slot {slot} belongs to no producer"))
    }

    fn producer_of_thread(&self, thread: ThreadId) -> usize {
        self.views[1..]
            .iter()
            .position(|view| view.thread == Some(thread))
            .unwrap_or_else(|| panic!("{thread:?} is no producer"))
    }

    fn list(items: impl IntoIterator<Item = usize>) -> String {
        let items: Vec<String> = items.into_iter().map(|i| i.to_string()).collect();
        format!("[{}]", items.join(","))
    }

    fn thread_list(&self, threads: &[ThreadId]) -> String {
        Self::list(threads.iter().map(|t| self.producer_of_thread(*t)))
    }

    fn bits_list(&self, mut bits: u64) -> String {
        let mut producers = Vec::new();
        while bits != 0 {
            producers.push(self.producer_of_slot(bits.trailing_zeros()));
            bits &= bits - 1;
        }
        Self::list(producers)
    }

    fn waker(&self, view: &ActorView) -> String {
        match view.point {
            Some(ReplayPoint::Drain { count }) => format!("drain:{count}"),
            Some(ReplayPoint::Unpark) => {
                format!("unpark:{}:{}", self.thread_list(&view.pending), view.count)
            }
            Some(ReplayPoint::UnparkDone) => format!("unpark:[]:{}", view.count),
            Some(ReplayPoint::Scan { count, .. }) => format!("scan:{count}"),
            Some(ReplayPoint::Clear { slot, count, .. }) => {
                format!("clear:{}:{count}", self.producer_of_slot(slot))
            }
            Some(ReplayPoint::Claim { slot, count, .. }) => {
                format!("claim:{}:{count}", self.producer_of_slot(slot))
            }
            other => panic!("{other:?} is not a wake loop step"),
        }
    }

    fn consumer(&self, view: &ActorView) -> String {
        if view.finished {
            return "finished".to_owned();
        }
        let closing = view.count == 0;
        match view.point {
            Some(ReplayPoint::Release) => "release".to_owned(),
            Some(ReplayPoint::Close) => "close".to_owned(),
            Some(ReplayPoint::Drain { count: 0 }) => "drainOverflow".to_owned(),
            Some(ReplayPoint::Unpark) if closing => {
                format!("unparking:{}", self.thread_list(&view.pending))
            }
            Some(ReplayPoint::UnparkDone) if closing => "unparking:[]".to_owned(),
            Some(ReplayPoint::Flush) => "flush".to_owned(),
            Some(ReplayPoint::Flushing { bits }) => format!("flushing:{}", self.bits_list(bits)),
            Some(ReplayPoint::FlushingDone) => "flushing:[]".to_owned(),
            _ => format!("waking({})", self.waker(view)),
        }
    }

    fn pc(&self, view: &ActorView) -> String {
        if view.finished {
            return "done".to_owned();
        }
        let name = match view.point {
            Some(ReplayPoint::Top) => "top",
            Some(ReplayPoint::Attempt) => "attempt",
            Some(ReplayPoint::TopClosed) => "topClosed",
            Some(ReplayPoint::Take) => "take",
            Some(ReplayPoint::Handle) => "handle",
            Some(ReplayPoint::Bit) => "bit",
            Some(ReplayPoint::Recheck) => "recheck",
            Some(ReplayPoint::RecheckRing) => "recheckRing",
            Some(ReplayPoint::RecheckClosed) => "recheckClosed",
            Some(ReplayPoint::Park) => "park",
            Some(ReplayPoint::DisarmWoken) => "disarmWoken",
            Some(ReplayPoint::DisarmWon) => "disarmWon",
            Some(ReplayPoint::DisarmClosed) => "disarmClosed",
            _ => return format!("forward({})", self.waker(view)),
        };
        name.to_owned()
    }

    fn producer(&self, k: usize) -> String {
        let set = self.target.wake_set();
        let view = &self.views[k + 1];
        let slot = self.slots[k];
        let bit = set.wake.load(std::sync::atomic::Ordering::Relaxed) & slot.mask() != 0;
        let registered = view
            .thread
            .is_some_and(|thread| set.overflow.threads().contains(&thread));
        let flag = |on: bool, c: char| if on { c } else { '-' };
        format!(
            "{}:{}{}{}{}{}",
            self.pc(view),
            flag(self.leases[k], 'L'),
            flag(bit, 'b'),
            flag(set.is_armed(slot), 'h'),
            flag(view.token, 't'),
            flag(registered, 'r'),
        )
    }
}
