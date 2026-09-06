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
//! The list is behind a `Mutex`, which the rest of this crate avoids.
//! It is reached only once more than `PARK_SLOTS` endpoints of one kind
//! are alive at once, never on the lock-free paths, and the critical
//! section is a `Vec` push or take.
//!
//! Keeping it off those paths takes an explicit guard. Every wake on a
//! ring reaches [`WakerOverflow::wake_all`], because
//! [`WakerSet`](super::wake_async::WakerSet) deliberately never clears
//! its `pending` flag. An `occupied` flag, loaded `Relaxed`, is
//! therefore checked before the lock: the list is empty in every run
//! that stays under `PARK_SLOTS` endpoints, so the mutex is never
//! acquired at all. Without it a single parked async waiter would put
//! a lock acquisition on every subsequent wake for the ring's lifetime.
//!
//! A stale `occupied` costs one needless lock of an empty list. It
//! cannot lose a wake: `occupied` is set under the lock before the
//! waker is visible to a drainer, and cleared under the lock once the
//! list is taken.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::task::Waker;

/// Wakers for waiters that could not lease a park slot.
pub struct WakerOverflow {
    /// Whether `waiting` is non-empty. Read `Relaxed` to keep the lock
    /// off the wake path; see the module docs.
    occupied: AtomicBool,
    waiting: Mutex<Vec<Waker>>,
    #[cfg(test)]
    locks: std::sync::atomic::AtomicUsize,
}

impl WakerOverflow {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            occupied: AtomicBool::new(false),
            waiting: Mutex::new(Vec::new()),
            #[cfg(test)]
            locks: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Registers a waker to be woken by the next [`wake_all`].
    ///
    /// A waker equivalent to one already registered is not added twice,
    /// so a future that re-registers on every poll does not grow the
    /// list without bound.
    ///
    /// [`wake_all`]: Self::wake_all
    // The lint would drop the guard before the `occupied` store. The
    // store belongs inside the critical section: it is what makes the
    // waker reachable to a drainer, and publishing it after the unlock
    // lets a drain land between the two.
    #[allow(clippy::significant_drop_tightening)]
    pub fn register(&self, waker: &Waker) {
        let mut waiting = self.lock();
        if waiting.iter().any(|w| w.will_wake(waker)) {
            return;
        }
        waiting.push(waker.clone());
        self.occupied.store(true, Ordering::Release);
    }

    /// Takes ownership of a waker displaced from a [`WakerSlot`].
    ///
    /// [`WakerSlot`]: super::wake_async::WakerSlot
    #[allow(clippy::significant_drop_tightening)]
    pub fn register_owned(&self, waker: Waker) {
        let mut waiting = self.lock();
        if waiting.iter().any(|w| w.will_wake(&waker)) {
            return;
        }
        waiting.push(waker);
        self.occupied.store(true, Ordering::Release);
    }

    /// Wakes and clears every registered waker.
    ///
    /// Each waiter re-registers if it still cannot progress, which
    /// costs a poll and never loses a wakeup.
    pub fn wake_all(&self) {
        if !self.occupied.load(Ordering::Acquire) {
            return;
        }
        // Cleared under the lock, with the take. Clearing it after the
        // guard drops opens a window where a registration pushed by a
        // peer is left with `occupied` false, and every later wake
        // takes the fast path over a waiter that is still parked.
        let woken = {
            let mut waiting = self.lock();
            self.occupied.store(false, Ordering::Release);
            std::mem::take(&mut *waiting)
        };
        for waker in woken {
            waker.wake();
        }
    }

    /// Whether any waker is registered.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Whether `occupied` still agrees with the list.
    ///
    /// A registered waker with `occupied` clear is a stranded waiter:
    /// every later [`wake_all`](Self::wake_all) takes the fast path and
    /// skips it.
    #[cfg(test)]
    fn is_consistent(&self) -> bool {
        let waiting = self.lock();
        waiting.is_empty() || self.occupied.load(Ordering::Acquire)
    }

    /// How many times the mutex has been acquired.
    #[cfg(test)]
    pub(crate) fn lock_count(&self) -> usize {
        self.locks.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    fn note_lock(&self) {
        self.locks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    const fn note_lock(&self) {}

    /// The lock is only ever held for a `Vec` push or take, neither of
    /// which can panic while holding it, so poisoning would mean a
    /// panic elsewhere in this type. Recovering the guard keeps one
    /// unrelated panic from poisoning every later wake into a hang.
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Waker>> {
        self.note_lock();
        self.waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for WakerOverflow {
    fn default() -> Self {
        Self::new()
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

    /// The `occupied` fast path must not lose a registration that
    /// lands while a drain is in flight.
    ///
    /// This is a smoke test, not a proof. The losing interleaving needs
    /// the drainer to clear `occupied` between a peer's push and its
    /// own take, which is a few instructions wide; this test has not
    /// been observed to fail even against an implementation that
    /// clears the flag outside the lock. It guards the invariant
    /// (`occupied` set whenever the list is non-empty) rather than
    /// relying on hitting the window. Model-checking the handshake is
    /// the job for `loom`.
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

            // The drainer either took the registration or missed it,
            // but it must never clear `occupied` over a list it did not
            // drain: every later wake would then skip that waiter.
            assert!(
                overflow.is_consistent(),
                "round {round}: a registration outlived the flag that makes it reachable"
            );
            overflow.wake_all();
            assert_eq!(
                waker.count(),
                1,
                "round {round}: a registration was stranded by a concurrent drain"
            );
        }
    }

    #[test]
    fn re_registering_the_same_waker_does_not_grow_the_list() {
        let overflow = WakerOverflow::new();
        let waker = Waker::from(CountingWaker::new());
        for _ in 0..16 {
            overflow.register(&waker);
        }
        assert_eq!(overflow.lock().len(), 1);
    }
}
