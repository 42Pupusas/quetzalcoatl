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

use std::sync::Mutex;
use std::task::Waker;

/// Wakers for waiters that could not lease a park slot.
pub struct WakerOverflow {
    waiting: Mutex<Vec<Waker>>,
}

impl WakerOverflow {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            waiting: Mutex::new(Vec::new()),
        }
    }

    /// Registers a waker to be woken by the next [`wake_all`].
    ///
    /// A waker equivalent to one already registered is not added twice,
    /// so a future that re-registers on every poll does not grow the
    /// list without bound.
    ///
    /// [`wake_all`]: Self::wake_all
    pub fn register(&self, waker: &Waker) {
        let mut waiting = self.lock();
        if waiting.iter().any(|w| w.will_wake(waker)) {
            return;
        }
        waiting.push(waker.clone());
    }

    /// Takes ownership of a waker displaced from a [`WakerSlot`].
    ///
    /// [`WakerSlot`]: super::wake_async::WakerSlot
    pub fn register_owned(&self, waker: Waker) {
        let mut waiting = self.lock();
        if waiting.iter().any(|w| w.will_wake(&waker)) {
            return;
        }
        waiting.push(waker);
    }

    /// Wakes and clears every registered waker.
    ///
    /// Each waiter re-registers if it still cannot progress, which
    /// costs a poll and never loses a wakeup.
    pub fn wake_all(&self) {
        let woken = std::mem::take(&mut *self.lock());
        for waker in woken {
            waker.wake();
        }
    }

    /// Whether any waker is registered.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// The lock is only ever held for a `Vec` push or take, neither of
    /// which can panic while holding it, so poisoning would mean a
    /// panic elsewhere in this type. Recovering the guard keeps one
    /// unrelated panic from poisoning every later wake into a hang.
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Waker>> {
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
