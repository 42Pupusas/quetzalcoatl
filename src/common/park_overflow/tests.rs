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
