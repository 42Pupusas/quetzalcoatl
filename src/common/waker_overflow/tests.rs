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
    overflow.register(Registration::new(id, Waker::from(CountingWaker::new())));
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
