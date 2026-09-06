//! Registration for async waiters that hold no park slot.
//!
//! A blocking waiter without a slot rescues itself: it parks with a
//! timeout and re-checks the ring. An async waiter has no such
//! backstop — once it returns `Poll::Pending`, only its waker can poll
//! it again — so a slotless async waiter must still be registered
//! somewhere, or it is lost.
//!
//! It cannot simply share a [`super::wake_async::WakerSlot`]: storing a
//! waker there replaces and drops the previous one, so the displaced
//! future is never polled again. Overflow registrations therefore
//! accumulate in a list, and every wake drains it.
//!
//! # Structure
//!
//! The list is a Treiber stack: an [`AtomicPtr`] head, a `compare_
//! exchange` to push, and a single `swap` to take the whole chain. The
//! two operations this type needs are exactly the two a Treiber stack
//! does without a lock, and taking the entire chain at once sidesteps
//! the ABA hazard that makes a general lock-free *pop* difficult.
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
//! # Duplicates
//!
//! A waker equal to one already on the stack is pushed again rather
//! than deduplicated: scanning the chain would mean dereferencing nodes
//! a concurrent drainer may be freeing, which needs a reclamation
//! scheme this does not warrant. A duplicate costs one extra poll of a
//! future that was going to be polled anyway, and every wake empties
//! the stack, so entries do not accumulate across wakes.

use std::ptr;
use std::task::Waker;

use super::atomics::AtomicPtr;
use std::sync::atomic::Ordering;

/// One registration, owned by the stack until a drainer takes it.
struct Node {
    waker: Waker,
    next: *mut Self,
}

/// Wakers for waiters that could not lease a park slot.
///
/// `Send`/`Sync` are the automatic ones: the only field is an
/// [`AtomicPtr`], and the [`Waker`]s it owns are themselves `Send` and
/// `Sync`.
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

    /// Registers a waker to be woken by the next [`wake_all`].
    ///
    /// [`wake_all`]: Self::wake_all
    pub fn register(&self, waker: &Waker) {
        self.push(waker.clone());
    }

    /// Takes ownership of a waker displaced from a [`WakerSlot`].
    ///
    /// [`WakerSlot`]: super::wake_async::WakerSlot
    pub fn register_owned(&self, waker: Waker) {
        self.push(waker);
    }

    fn push(&self, waker: Waker) {
        let node = Box::into_raw(Box::new(Node {
            waker,
            next: ptr::null_mut(),
        }));
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // SAFETY: `node` was just allocated here and is not yet
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
            owned.waker.wake();
        }
    }

    /// Whether any waker is registered.
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
        let first = CountingWaker::new();
        let second = CountingWaker::new();
        overflow.register(&Waker::from(Arc::clone(&first)));
        overflow.register(&Waker::from(Arc::clone(&second)));

        overflow.wake_all();
        assert_eq!(first.count(), 1);
        assert_eq!(second.count(), 1);
    }

    /// Unlike a single shared slot, an overflow registration never
    /// displaces an earlier one.
    #[test]
    fn a_later_registration_does_not_displace_an_earlier_one() {
        let overflow = WakerOverflow::new();
        let displaced = CountingWaker::new();
        overflow.register(&Waker::from(Arc::clone(&displaced)));
        overflow.register(&Waker::from(CountingWaker::new()));

        overflow.wake_all();
        assert_eq!(displaced.count(), 1);
    }

    #[test]
    fn waking_clears_the_registrations() {
        let overflow = WakerOverflow::new();
        let waker = CountingWaker::new();
        overflow.register(&Waker::from(Arc::clone(&waker)));

        overflow.wake_all();
        assert!(overflow.is_empty());
        overflow.wake_all();
        assert_eq!(waker.count(), 1);
    }

    /// Dropping a stack that still holds registrations must free the
    /// nodes rather than leak the chain.
    #[test]
    fn dropping_a_populated_overflow_releases_its_wakers() {
        let waker = CountingWaker::new();
        {
            let overflow = WakerOverflow::new();
            overflow.register(&Waker::from(Arc::clone(&waker)));
            overflow.register(&Waker::from(Arc::clone(&waker)));
        }
        assert_eq!(Arc::strong_count(&waker), 1);
        assert_eq!(waker.count(), 0);
    }

    #[test]
    fn many_registrations_are_all_woken() {
        let overflow = WakerOverflow::new();
        let wakers: Vec<_> = (0..256).map(|_| CountingWaker::new()).collect();
        for waker in &wakers {
            overflow.register(&Waker::from(Arc::clone(waker)));
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
        let wakers: Vec<_> = (0..512).map(|_| CountingWaker::new()).collect();

        let gate = Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = wakers
            .chunks(64)
            .map(|chunk| {
                let overflow = Arc::clone(&overflow);
                let gate = Arc::clone(&gate);
                let chunk: Vec<_> = chunk.iter().map(Arc::clone).collect();
                std::thread::spawn(move || {
                    gate.wait();
                    for waker in chunk {
                        overflow.register(&Waker::from(waker));
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

    /// A registration racing a drain is either taken by that drain or
    /// left on the stack for the next one — never dropped between the
    /// two.
    #[test]
    fn a_concurrent_drain_never_strands_a_registration() {
        for round in 0..2_000 {
            let overflow = Arc::new(WakerOverflow::new());
            let waker = CountingWaker::new();

            let gate = Arc::new(std::sync::Barrier::new(2));
            let registrar = {
                let overflow = Arc::clone(&overflow);
                let waker = Arc::clone(&waker);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    overflow.register(&Waker::from(waker));
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
