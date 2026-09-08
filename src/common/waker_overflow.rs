//! Registration for async waiters that hold no park slot, and for
//! those displaced from one.
//!
//! A blocking waiter without a slot is reached through the park
//! overflow stack. An async waiter has no equivalent standing presence
//! — once it returns `Poll::Pending`, only its waker can poll it again
//! — so a slotless async waiter must still be registered somewhere, or
//! it is lost.
//!
//! It cannot simply share a [`super::wake_async::WakerSlot`]: storing a
//! waker there replaces the previous one, and the displaced future is
//! still parked. Overflow registrations therefore accumulate in a list,
//! and every wake drains it.
//!
//! # Structure
//!
//! The list is a Treiber stack: an [`AtomicPtr`] head, a `compare_
//! exchange` to push, and a single `swap` to take the whole chain. The
//! operations this type needs are the ones a Treiber stack does without
//! a lock, and taking the entire chain at once sidesteps the ABA hazard
//! that makes a general lock-free *pop* difficult.
//!
//! Pushing never dereferences the head it read — it only writes that
//! pointer into its own fresh node — so a drainer freeing the chain
//! cannot be observed through a dangling pointer. A push whose CAS
//! succeeds against a recycled address is still correct: the CAS
//! succeeds only when the recycled node is the current head, which is
//! the link the pusher wanted.
//!
//! Every wake on a ring reaches [`WakerOverflow::wake_all`], because
//! [`WakerSet`](super::wake_async::WakerSet) deliberately never clears
//! its `pending` flag. An empty stack is therefore the common case and
//! costs one `Relaxed` load — no atomic read-modify-write, no
//! allocation, and nothing that can contend.
//!
//! # Withdrawal
//!
//! A cancelled future must be able to take its registration back out.
//! Without that, a cancel/retry loop on an idle ring pushes a fresh
//! entry per iteration and nothing removes them until some peer
//! happens to wake: the stack grows with the number of cancellations
//! rather than the number of waiters.
//!
//! [`withdraw`](WakerOverflow::withdraw) detaches the whole chain with
//! the same `swap` a drain uses, drops the node whose
//! [`RegistrationId`] matches, and pushes the survivors back. Detaching
//! makes the walk safe — the walker owns every node it inspects — at
//! the cost of briefly hiding peers from a concurrent wake. That is
//! benign: a waiter re-pushed here has not been woken, and the waiter
//! doing the withdrawing is on its way out. A peer that pushes during
//! the window lands on a fresh chain and is untouched.
//!
//! # Duplicates
//!
//! Identity is the [`RegistrationId`], not the waker, so two
//! registrations from one task are distinct entries and withdrawing one
//! leaves the other. Registering the same *id* twice is not something
//! the callers do: an [`ExclusiveRegistration`] holds at most one live
//! registration at a time.
//!
//! [`ExclusiveRegistration`]: super::park_registration::ExclusiveRegistration

use std::ptr;

use super::atomics::AtomicPtr;
use super::registration_id::{Registration, RegistrationId};
use std::sync::atomic::Ordering;

/// One registration, owned by the stack until a drainer takes it.
struct Node {
    registration: Registration,
    next: *mut Self,
}

/// Registrations for waiters that could not lease a park slot, or that
/// were displaced from one.
///
/// `Send`/`Sync` are the automatic ones: the only shared field is an
/// [`AtomicPtr`], and the [`Waker`](std::task::Waker)s it owns are
/// themselves `Send` and `Sync`.
pub struct WakerOverflow {
    head: AtomicPtr<Node>,
    #[cfg(test)]
    takes: super::atomics::AtomicUsize,
}

impl WakerOverflow {
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

    /// Registers a waiter to be woken by the next [`wake_all`].
    ///
    /// [`wake_all`]: Self::wake_all
    pub fn register(&self, registration: Registration) {
        let node = Box::into_raw(Box::new(Node {
            registration,
            next: ptr::null_mut(),
        }));
        // SAFETY: `node` was just allocated here and is not yet
        // published, so this thread is its only accessor.
        unsafe { self.push_node(node) };
    }

    /// Links an owned node onto the stack.
    ///
    /// # Safety
    ///
    /// `node` must be a live, uniquely-owned allocation this call takes
    /// ownership of, and must not already be reachable from the stack.
    unsafe fn push_node(&self, node: *mut Node) {
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // SAFETY: the caller owns `node` uniquely and it is not yet
            // published, so this thread is its only accessor.
            unsafe { (*node).next = head };
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => head = actual,
            }
        }
    }

    /// Removes the registration `id` names, if it is still here.
    ///
    /// Returns whether it was found. See the module docs for why this
    /// detaches the chain rather than unlinking in place.
    pub fn withdraw(&self, id: RegistrationId) -> bool {
        if self.head.load(Ordering::Relaxed).is_null() {
            return false;
        }
        let mut node = self.head.swap(ptr::null_mut(), Ordering::Acquire);
        let mut found = false;
        while !node.is_null() {
            // SAFETY: the swap detached the whole chain, so this thread
            // is its sole owner and no peer can reach these nodes.
            let owned = unsafe { Box::from_raw(node) };
            node = owned.next;
            if !found && owned.registration.is(id) {
                found = true;
                drop(owned);
                continue;
            }
            let retained = Box::into_raw(owned);
            // SAFETY: `retained` came from a Box this thread owns and
            // was just unlinked from the detached chain.
            unsafe { self.push_node(retained) };
        }
        found
    }

    /// Wakes and clears every registered waker.
    ///
    /// Each waiter re-registers if it still cannot progress, which
    /// costs a poll and never loses a wakeup.
    pub fn wake_all(&self) {
        if self.head.load(Ordering::Relaxed).is_null() {
            return;
        }
        self.note_take();
        let mut node = self.head.swap(ptr::null_mut(), Ordering::Acquire);
        while !node.is_null() {
            // SAFETY: the swap detached the whole chain, so this thread
            // is its sole owner and no peer can reach these nodes.
            let owned = unsafe { Box::from_raw(node) };
            node = owned.next;
            owned.registration.wake();
        }
    }

    /// Whether any registration is present.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire).is_null()
    }

    /// How many times the chain has been detached — that is, how often
    /// a wake went past the empty fast path.
    #[cfg(test)]
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

impl Default for WakerOverflow {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WakerOverflow {
    fn drop(&mut self) {
        // `&mut self` proves no peer holds a reference, so a plain swap
        // takes the pointer with the same uniqueness the old `get_mut`
        // path had (and is the shared form: loom's `AtomicPtr` exposes
        // no `get_mut`).
        let mut node = self.head.swap(ptr::null_mut(), Ordering::Relaxed);
        while !node.is_null() {
            // SAFETY: the swap detached the whole chain, so this thread
            // is its sole owner and no peer can reach these nodes.
            let owned = unsafe { Box::from_raw(node) };
            node = owned.next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WakerOverflow;
    use crate::common::registration_id::{Registration, RegistrationIds};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct CountingWaker {
        count: AtomicUsize,
    }

    impl CountingWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                count: AtomicUsize::new(0),
            })
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::Relaxed)
        }
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn an_empty_overflow_wakes_nothing() {
        let overflow = WakerOverflow::new();
        assert!(overflow.is_empty());
        overflow.wake_all();
        assert_eq!(overflow.take_count(), 0);
    }

    #[test]
    fn every_registered_waker_is_woken() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let first = CountingWaker::new();
        let second = CountingWaker::new();
        overflow.register(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&first)),
        ));
        overflow.register(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&second)),
        ));

        overflow.wake_all();
        assert_eq!(first.count(), 1);
        assert_eq!(second.count(), 1);
    }

    /// Unlike a single shared slot, an overflow registration never
    /// displaces an earlier one.
    #[test]
    fn a_later_registration_does_not_displace_an_earlier_one() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let displaced = CountingWaker::new();
        overflow.register(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&displaced)),
        ));
        overflow.register(Registration::new(
            ids.mint(),
            Waker::from(CountingWaker::new()),
        ));

        overflow.wake_all();
        assert_eq!(displaced.count(), 1);
    }

    #[test]
    fn waking_clears_the_registrations() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let waker = CountingWaker::new();
        overflow.register(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&waker)),
        ));

        overflow.wake_all();
        assert!(overflow.is_empty());
        overflow.wake_all();
        assert_eq!(waker.count(), 1);
    }

    /// The leak this type's `withdraw` exists to close: a cancelled
    /// waiter must be able to take its own registration back out.
    #[test]
    fn a_withdrawn_registration_is_removed_and_never_woken() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let waker = CountingWaker::new();
        let id = ids.mint();
        overflow.register(Registration::new(id, Waker::from(Arc::clone(&waker))));

        assert!(overflow.withdraw(id), "the registration was not found");
        assert!(overflow.is_empty());

        overflow.wake_all();
        assert_eq!(waker.count(), 0, "a withdrawn waiter was woken");
    }

    /// Withdrawal must remove one registration, not the chain.
    #[test]
    fn withdrawing_one_registration_leaves_its_peers() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let kept = CountingWaker::new();
        let other = CountingWaker::new();

        let kept_id = ids.mint();
        overflow.register(Registration::new(kept_id, Waker::from(Arc::clone(&kept))));
        let cancelled_id = ids.mint();
        overflow.register(Registration::new(
            cancelled_id,
            Waker::from(CountingWaker::new()),
        ));
        let other_id = ids.mint();
        overflow.register(Registration::new(other_id, Waker::from(Arc::clone(&other))));

        assert!(overflow.withdraw(cancelled_id));
        assert_eq!(overflow.depth(), 2);

        overflow.wake_all();
        assert_eq!(kept.count(), 1, "a peer lost its wake");
        assert_eq!(other.count(), 1, "a peer lost its wake");
    }

    /// Two registrations from one task share a waker, so only the id
    /// tells them apart. Withdrawing one must leave the sibling
    /// reachable — the lost-wakeup defect this identity exists to fix.
    #[test]
    fn withdrawing_one_of_two_registrations_from_one_task_leaves_the_sibling() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let task = CountingWaker::new();

        let cancelled = ids.mint();
        overflow.register(Registration::new(cancelled, Waker::from(Arc::clone(&task))));
        let sibling = ids.mint();
        overflow.register(Registration::new(sibling, Waker::from(Arc::clone(&task))));

        assert!(overflow.withdraw(cancelled));

        overflow.wake_all();
        assert_eq!(
            task.count(),
            1,
            "the surviving sibling was not woken exactly once"
        );
    }

    #[test]
    fn withdrawing_an_absent_registration_reports_nothing_and_keeps_the_rest() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let waker = CountingWaker::new();
        overflow.register(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&waker)),
        ));

        let absent = ids.mint();
        assert!(!overflow.withdraw(absent));
        assert_eq!(overflow.depth(), 1);

        overflow.wake_all();
        assert_eq!(waker.count(), 1);
    }

    #[test]
    fn withdrawing_from_an_empty_overflow_reports_nothing() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        assert!(!overflow.withdraw(ids.mint()));
    }

    /// A withdrawn registration is gone for good: withdrawing it again
    /// must not remove a peer in its place.
    #[test]
    fn withdrawing_twice_removes_only_the_first_time() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let peer = CountingWaker::new();
        let id = ids.mint();
        overflow.register(Registration::new(
            id,
            Waker::from(CountingWaker::new()),
        ));
        overflow.register(Registration::new(
            ids.mint(),
            Waker::from(Arc::clone(&peer)),
        ));

        assert!(overflow.withdraw(id));
        assert!(!overflow.withdraw(id));
        assert_eq!(overflow.depth(), 1);

        overflow.wake_all();
        assert_eq!(peer.count(), 1);
    }

    /// The accumulation this closes: a cancel/retry loop on an idle
    /// ring must not grow the stack.
    #[test]
    fn a_cancel_retry_loop_leaves_nothing_behind() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let task = CountingWaker::new();

        for _ in 0..1_000 {
            let id = ids.mint();
            overflow.register(Registration::new(id, Waker::from(Arc::clone(&task))));
            assert!(overflow.withdraw(id));
        }

        assert_eq!(overflow.depth(), 0);
        assert_eq!(task.count(), 0);
    }

    /// Dropping a stack that still holds registrations must free the
    /// nodes rather than leak the chain.
    #[test]
    fn dropping_a_populated_overflow_releases_its_wakers() {
        let waker = CountingWaker::new();
        {
            let overflow = WakerOverflow::new();
            let ids = RegistrationIds::new();
            overflow.register(Registration::new(
                ids.mint(),
                Waker::from(Arc::clone(&waker)),
            ));
            overflow.register(Registration::new(
                ids.mint(),
                Waker::from(Arc::clone(&waker)),
            ));
        }
        assert_eq!(Arc::strong_count(&waker), 1);
        assert_eq!(waker.count(), 0);
    }

    #[test]
    fn many_registrations_are_all_woken() {
        let overflow = WakerOverflow::new();
        let ids = RegistrationIds::new();
        let wakers: Vec<_> = (0..256).map(|_| CountingWaker::new()).collect();
        for waker in &wakers {
            overflow.register(Registration::new(
                ids.mint(),
                Waker::from(Arc::clone(waker)),
            ));
        }

        overflow.wake_all();
        assert!(overflow.is_empty());
        for waker in &wakers {
            assert_eq!(waker.count(), 1);
        }
    }

    /// Concurrent pushes must not lose a registration to each other's
    /// CAS, and a drain must take a whole consistent chain.
    #[test]
    fn concurrent_registrations_are_all_woken() {
        let overflow = Arc::new(WakerOverflow::new());
        let ids = Arc::new(RegistrationIds::new());
        let wakers: Vec<_> = (0..512).map(|_| CountingWaker::new()).collect();

        let gate = Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = wakers
            .chunks(64)
            .map(|chunk| {
                let overflow = Arc::clone(&overflow);
                let ids = Arc::clone(&ids);
                let gate = Arc::clone(&gate);
                let chunk: Vec<_> = chunk.iter().map(Arc::clone).collect();
                std::thread::spawn(move || {
                    gate.wait();
                    for waker in chunk {
                        overflow.register(Registration::new(ids.mint(), Waker::from(waker)));
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        overflow.wake_all();
        for (i, waker) in wakers.iter().enumerate() {
            assert_eq!(waker.count(), 1, "waker {i} was lost by a concurrent push");
        }
    }

    /// A withdrawal detaches the chain, so a peer registering during
    /// that window must survive it rather than be dropped or stranded.
    #[test]
    fn a_withdrawal_never_strands_a_concurrent_registration() {
        for round in 0..2_000 {
            let overflow = Arc::new(WakerOverflow::new());
            let ids = Arc::new(RegistrationIds::new());
            let peer = CountingWaker::new();

            let cancelled = ids.mint();
            overflow.register(Registration::new(
                cancelled,
                Waker::from(CountingWaker::new()),
            ));

            let gate = Arc::new(std::sync::Barrier::new(2));
            let registrar = {
                let overflow = Arc::clone(&overflow);
                let ids = Arc::clone(&ids);
                let peer = Arc::clone(&peer);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.register(Registration::new(ids.mint(), Waker::from(peer)));
                })
            };
            let withdrawer = {
                let overflow = Arc::clone(&overflow);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.withdraw(cancelled)
                })
            };
            registrar.join().unwrap();
            let withdrew = withdrawer.join().unwrap();
            assert!(withdrew, "round {round}: the withdrawal lost its own entry");

            overflow.wake_all();
            assert_eq!(
                peer.count(),
                1,
                "round {round}: a registration was stranded by a concurrent withdrawal"
            );
        }
    }

    /// A registration racing a drain is either taken by that drain or
    /// left on the stack for the next one — never dropped between the
    /// two.
    #[test]
    fn a_concurrent_drain_never_strands_a_registration() {
        for round in 0..2_000 {
            let overflow = Arc::new(WakerOverflow::new());
            let ids = RegistrationIds::new();
            let waker = CountingWaker::new();

            let gate = Arc::new(std::sync::Barrier::new(2));
            let registrar = {
                let overflow = Arc::clone(&overflow);
                let waker = Arc::clone(&waker);
                let gate = Arc::clone(&gate);
                let id = ids.mint();
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.register(Registration::new(id, Waker::from(waker)));
                })
            };
            let drainer = {
                let overflow = Arc::clone(&overflow);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.wake_all();
                })
            };
            registrar.join().unwrap();
            drainer.join().unwrap();

            overflow.wake_all();
            assert_eq!(
                waker.count(),
                1,
                "round {round}: a registration was stranded by a concurrent drain"
            );
        }
    }
}
