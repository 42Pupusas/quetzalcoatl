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
            // SAFETY: the swap detached the whole chain, so this thread
            // is its sole owner and no peer can reach these nodes.
            let owned = unsafe { Box::from_raw(node) };
            node = owned.next;
            owned.thread.unpark();
        }
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
mod tests {
    use super::ParkOverflow;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    #[test]
    fn an_empty_overflow_wakes_nothing() {
        let overflow = ParkOverflow::new();
        assert!(overflow.is_empty());
        overflow.wake_all();
        assert_eq!(overflow.take_count(), 0);
    }

    #[test]
    fn registering_leaves_the_handle_on_the_stack() {
        let overflow = ParkOverflow::new();
        overflow.register();
        assert!(!overflow.is_empty());
        assert_eq!(overflow.depth(), 1);
    }

    #[test]
    fn waking_clears_the_registrations() {
        let overflow = ParkOverflow::new();
        overflow.register();
        overflow.register();
        assert_eq!(overflow.depth(), 2);

        overflow.wake_all();
        assert!(overflow.is_empty());
        assert_eq!(overflow.take_count(), 1);
    }

    /// The property a single shared handle slot cannot offer: a later
    /// registration must not displace an earlier one.
    #[test]
    fn a_later_registration_does_not_displace_an_earlier_one() {
        let overflow = Arc::new(ParkOverflow::new());
        let woken = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(Barrier::new(3));

        let parked: Vec<_> = (0..2)
            .map(|_| {
                let overflow = Arc::clone(&overflow);
                let woken = Arc::clone(&woken);
                let ready = Arc::clone(&ready);
                std::thread::spawn(move || {
                    overflow.register();
                    ready.wait();
                    std::thread::park();
                    woken.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();

        ready.wait();
        while overflow.depth() < 2 {
            std::thread::yield_now();
        }
        overflow.wake_all();
        for handle in parked {
            handle.join().unwrap();
        }
        assert_eq!(woken.load(Ordering::Relaxed), 2);
    }

    /// The whole point of the type: a registered waiter parks untimed
    /// and is released by a peer's wake, not by a clock.
    #[test]
    fn a_registered_waiter_is_released_by_a_peer_wake() {
        let overflow = Arc::new(ParkOverflow::new());
        let released = Arc::new(AtomicBool::new(false));

        let waiter = {
            let overflow = Arc::clone(&overflow);
            let released = Arc::clone(&released);
            std::thread::spawn(move || {
                overflow.register();
                std::thread::park();
                released.store(true, Ordering::Release);
            })
        };

        while overflow.is_empty() {
            std::thread::yield_now();
        }
        assert!(!released.load(Ordering::Acquire));
        overflow.wake_all();
        waiter.join().unwrap();
        assert!(released.load(Ordering::Acquire));
    }

    #[test]
    fn many_registrations_are_all_woken() {
        let overflow = ParkOverflow::new();
        for _ in 0..256 {
            overflow.register();
        }
        assert_eq!(overflow.depth(), 256);
        overflow.wake_all();
        assert!(overflow.is_empty());
    }

    /// Concurrent pushes must not lose a registration to each other's
    /// CAS, and a drain must take a whole consistent chain.
    #[test]
    fn concurrent_registrations_are_all_woken() {
        let overflow = Arc::new(ParkOverflow::new());
        let woken = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Barrier::new(8));

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let overflow = Arc::clone(&overflow);
                let woken = Arc::clone(&woken);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.register();
                    std::thread::park();
                    woken.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();

        while overflow.depth() < 8 {
            std::thread::yield_now();
        }
        overflow.wake_all();
        for handle in threads {
            handle.join().unwrap();
        }
        assert_eq!(woken.load(Ordering::Relaxed), 8);
    }

    /// A registration racing a drain is either taken by that drain or
    /// left on the stack for the next one — never dropped between the
    /// two.
    ///
    /// The final drain is retried rather than issued once: the racing
    /// drain may run before the registrar has pushed at all, so a
    /// single trailing `wake_all` can precede the registration and
    /// prove nothing. Retrying until the stack is observed empty *and*
    /// the waiter has resumed tests the property without depending on
    /// which side won.
    #[test]
    fn a_concurrent_drain_never_strands_a_registration() {
        for round in 0..500 {
            let overflow = Arc::new(ParkOverflow::new());
            let woken = Arc::new(AtomicBool::new(false));
            let gate = Arc::new(Barrier::new(2));

            let registrar = {
                let overflow = Arc::clone(&overflow);
                let woken = Arc::clone(&woken);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.register();
                    std::thread::park();
                    woken.store(true, Ordering::Release);
                })
            };
            {
                let overflow = Arc::clone(&overflow);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.wake_all();
                })
                .join()
                .unwrap();
            }

            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !woken.load(Ordering::Acquire) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "round {round}: a registration was stranded by a concurrent drain"
                );
                overflow.wake_all();
                std::thread::yield_now();
            }
            registrar.join().unwrap();
        }
    }

    /// Dropping a stack that still holds registrations must free the
    /// nodes rather than leak the chain.
    #[test]
    fn dropping_a_populated_overflow_releases_its_handles() {
        let overflow = ParkOverflow::new();
        overflow.register();
        overflow.register();
        drop(overflow);
    }
}
