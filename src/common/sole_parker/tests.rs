use super::SoleParker;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[test]
fn a_fresh_parker_has_nobody_parked() {
    let park = SoleParker::new();
    assert!(!park.is_parked());
}

#[test]
fn arming_publishes_the_parked_flag() {
    let park = SoleParker::new();
    park.arm();
    assert!(park.is_parked());
}

#[test]
fn disarming_withdraws_the_commitment() {
    let park = SoleParker::new();
    park.arm();
    park.disarm();
    assert!(!park.is_parked());
}

#[test]
fn waking_nobody_is_a_no_op() {
    let park = SoleParker::new();
    park.wake();
    assert!(!park.is_parked());
}

#[test]
fn a_wake_clears_the_flag_so_the_next_peer_skips_it() {
    let park = SoleParker::new();
    park.arm();
    park.wake();
    assert!(
        !park.is_parked(),
        "a second peer must not pay for an unpark already delivered"
    );
}

#[test]
fn a_wake_releases_a_parked_thread() {
    let park = Arc::new(SoleParker::new());
    let peer = Arc::clone(&park);
    let woke = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&woke);

    let handle = std::thread::spawn(move || {
        peer.arm();
        std::thread::park();
        flag.store(true, Ordering::SeqCst);
    });

    while !park.is_parked() {
        std::thread::yield_now();
    }
    park.wake();
    handle.join().unwrap();
    assert!(woke.load(Ordering::SeqCst));
}

/// The migration case. One logical waiter parks on a succession of
/// threads; each `arm` must redirect the wake to the thread parked
/// *now*. A write-once handle passes the first round and then sends
/// every later wake to a thread that has already exited, hanging
/// the second. Sequential by construction, because a side with one
/// waiter never has two threads arming at once.
#[test]
fn each_park_redirects_the_wake_to_the_thread_parked_now() {
    let park = Arc::new(SoleParker::new());

    for _ in 0..2 {
        let peer = Arc::clone(&park);
        let handle = std::thread::spawn(move || {
            peer.arm();
            std::thread::park();
            peer.disarm();
        });

        while !park.is_parked() {
            std::thread::yield_now();
        }
        park.wake();
        handle.join().unwrap();
    }
}

#[test]
fn the_flag_occupies_its_own_cache_line() {
    assert_eq!(std::mem::align_of::<SoleParker>(), 64);
}
