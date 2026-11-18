//! Registration for blocking waiters that hold no park slot.
//!
//! The wake bitmap is one `u64`, so past [`PARK_SLOTS`] waiters on one
//! side a lease is refused and the waiter is [`ParkSlot::Shared`]. Such
//! a waiter publishes no bit and arms no handle, so no peer can find
//! it through the bitmap.
//!
//! Its previous rescue was a timeout: park for a millisecond, re-check
//! the ring, park again. That is correct but it is polling, and it is
//! the shape this crate has spent an audit removing — a lost wake and
//! a slow one become indistinguishable, and every slotless waiter
//! costs a thousand wakeups a second whether or not the ring moves.
//!
//! [`ParkOverflow`] gives those waiters somewhere to be found instead.
//! A slotless waiter pushes its [`Thread`] handle before its final
//! re-check, and every wake on the side drains the stack. The park
//! itself is then untimed, exactly like a leased waiter's.
//!
//! # Structure
//!
//! A Treiber stack: an [`AtomicPtr`] head, a `compare_exchange` to
//! push, one `swap` to take the whole chain. This mirrors
//! [`WakerOverflow`](super::waker_overflow::WakerOverflow), which
//! solves the same problem for async waiters, and the reasoning about
//! ABA carries over unchanged: pushing never dereferences the head it
//! read, and taking the entire chain at once avoids a general
//! lock-free pop.
//!
//! # Ordering
//!
//! The emptiness check is `SeqCst`, and pairs with the `SeqCst` CAS in
//! [`register`](ParkOverflow::register) the same way the bitmap's
//! `fetch_or` pairs with the peer's load: the waiter registers before
//! its final re-check, the peer drains after its release, so either
//! the waiter sees the release or the peer sees the registration.
//! Callers reach this through [`WakeSet`](super::park::WakeSet), whose
//! wake paths already issue the leading `SeqCst` fence that drains the
//! publishing store out of the store buffer.
//!
//! # Stale registrations
//!
//! A waiter whose re-check succeeds returns without parking and leaves
//! its node behind; removing one node from a Treiber stack is the
//! operation this structure deliberately does not offer. The next
//! drain takes it and unparks a thread that is not parked, which
//! leaves a token that makes one later park return early. Every
//! blocking loop here re-checks after parking, so a spurious return
//! costs one lap and never loses a wakeup. Stale nodes do not
//! accumulate: their presence makes the stack non-empty, so the next
//! wake drains them.
//!
//! [`PARK_SLOTS`]: super::park::PARK_SLOTS
//! [`ParkSlot::Shared`]: super::park_registry::ParkSlot::Shared

use std::ptr;

use super::atomics::{thread, AtomicPtr};
use super::replay_point::ReplayPoint;
use std::sync::atomic::Ordering;
use thread::Thread;

/// One registration, owned by the stack until a drainer takes it.
struct Node {
    thread: Thread,
    next: *mut Self,
}

/// Park handles for waiters that could not lease a park slot.
///
/// `Send`/`Sync` are the automatic ones: the only field is an
/// [`AtomicPtr`], and the [`Thread`] handles it owns are themselves
/// `Send` and `Sync`.
pub struct ParkOverflow {
    head: AtomicPtr<Node>,
    #[cfg(test)]
    takes: super::atomics::AtomicUsize,
}

impl ParkOverflow {
    // Loom's `AtomicPtr::new` is not const, so the loom twin of this
    // constructor takes the weaker form. Std callers keep const use.
    #[cfg(not(loom))]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            #[cfg(test)]
            takes: super::atomics::AtomicUsize::new(0),
        }
    }

    #[cfg(loom)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            #[cfg(test)]
            takes: super::atomics::AtomicUsize::new(0),
        }
    }

    /// Registers the calling thread to be unparked by the next
    /// [`wake_all`](Self::wake_all).
    ///
    /// Call before the final re-check of the ring, so a peer that
    /// releases after this point cannot miss the registration.
    pub fn register(&self) {
        self.push(thread::current());
    }

    fn push(&self, handle: Thread) {
        let node = Box::into_raw(Box::new(Node {
            thread: handle,
            next: ptr::null_mut(),
        }));
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // SAFETY: `node` was just allocated here and is not yet
            // published, so this thread is its only accessor.
            unsafe { (*node).next = head };
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    /// Unparks and clears every registered waiter.
    ///
    /// Each waiter re-registers if it still cannot progress, which
    /// costs one lap of its loop and never loses a wakeup.
    ///
    /// The `SeqCst` fast-path load is the ordering half of the
    /// handshake; see the module docs.
    #[inline]
    pub fn wake_all(&self) {
        if self.head.load(Ordering::SeqCst).is_null() {
            return;
        }
        self.note_take();
        let mut node = self.head.swap(ptr::null_mut(), Ordering::AcqRel);
        while !node.is_null() {
            Self::reach_unpark(node);
            // SAFETY: the swap detached the whole chain, so this thread
            // is its sole owner and no peer can reach these nodes.
            let owned = unsafe { Box::from_raw(node) };
            node = owned.next;
            ReplayPoint::unparked(&owned.thread);
            owned.thread.unpark();
        }
    }

    /// Stops the replay before unparking the chain from `node` on.
    #[cfg(all(test, not(loom)))]
    fn reach_unpark(node: *mut Node) {
        ReplayPoint::Unpark.reach_unparking(Self::chain_threads(node));
    }

    #[cfg(not(all(test, not(loom))))]
    #[inline]
    const fn reach_unpark(_node: *mut Node) {}

    /// The threads of the chain from `node` on, newest first.
    #[cfg(all(test, not(loom)))]
    fn chain_threads(mut node: *mut Node) -> Vec<std::thread::ThreadId> {
        let mut threads = Vec::new();
        while !node.is_null() {
            // SAFETY: test-only; the caller owns the chain, or holds
            // `&self` with no concurrent drainer.
            let current = unsafe { &*node };
            threads.push(current.thread.id());
            node = current.next;
        }
        threads
    }

    /// The registered threads, newest first.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn threads(&self) -> Vec<std::thread::ThreadId> {
        Self::chain_threads(self.head.load(Ordering::Acquire))
    }

    /// Whether any waiter is registered.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire).is_null()
    }

    /// How many times the chain has been detached — that is, how often
    /// a wake went past the empty fast path.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn take_count(&self) -> usize {
        self.takes.load(Ordering::Relaxed)
    }

    /// How many registrations are currently on the stack.
    #[cfg(test)]
    pub(crate) fn depth(&self) -> usize {
        let mut node = self.head.load(Ordering::Acquire);
        let mut depth = 0;
        while !node.is_null() {
            depth += 1;
            // SAFETY: test-only, and callers hold `&self` with no
            // concurrent drainer, so no node here can be freed.
            node = unsafe { (*node).next };
        }
        depth
    }

    #[cfg(test)]
    fn note_take(&self) {
        self.takes.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    const fn note_take(&self) {}
}

impl Default for ParkOverflow {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ParkOverflow {
    fn drop(&mut self) {
        // `&mut self` proves no peer holds a reference, so a plain swap
        // takes the pointer with the uniqueness `get_mut` would give
        // (loom's `AtomicPtr` exposes no `get_mut`).
        let mut node = self.head.swap(ptr::null_mut(), Ordering::Relaxed);
        while !node.is_null() {
            // SAFETY: the swap detached the whole chain, so this thread
            // is its sole owner and no peer can reach these nodes.
            let owned = unsafe { Box::from_raw(node) };
            node = owned.next;
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
