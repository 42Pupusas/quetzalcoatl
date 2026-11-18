use super::ThreadParker;

#[test]
fn an_unarmed_parker_has_nothing_to_wake() {
    let parker = ThreadParker::new();
    assert!(!parker.is_armed());
    assert!(!parker.wake());
}

#[test]
fn arming_publishes_a_handle_and_waking_claims_it() {
    let parker = ThreadParker::new();
    parker.arm();
    assert!(parker.is_armed());
    assert!(parker.wake());
    assert!(!parker.is_armed());
}

#[test]
fn only_one_of_two_wakers_claims_the_handle() {
    let parker = ThreadParker::new();
    parker.arm();
    assert!(parker.wake());
    assert!(!parker.wake());
}

/// A second arm with no intervening wake must displace the first.
/// This is the migration case: a handle parks, is woken by
/// something other than its peer, and parks again elsewhere. A
/// write-once cell keeps naming the first thread.
#[cfg_attr(loom, ignore = "spawns real threads; the loom twin models this")]
#[cfg(not(loom))]
#[test]
fn rearming_without_an_intervening_wake_replaces_the_thread() {
    let parker = std::sync::Arc::new(ThreadParker::new());
    parker.arm();
    let first = parker.armed_id().unwrap();
    assert_eq!(first, std::thread::current().id());

    let moved = std::sync::Arc::clone(&parker);
    let second = std::thread::spawn(move || {
        moved.arm();
        std::thread::current().id()
    })
    .join()
    .unwrap();

    assert_ne!(first, second);
    assert_eq!(
        parker.armed_id(),
        Some(second),
        "a wake would reach the thread that armed first, not the one parked now"
    );
}

#[cfg(not(loom))]
#[test]
fn a_rearmed_parker_wakes_the_current_thread() {
    let parker = std::sync::Arc::new(ThreadParker::new());
    let peer = std::sync::Arc::clone(&parker);

    let handle = std::thread::spawn(move || {
        peer.arm();
        std::thread::park();
    });

    while !parker.is_armed() {
        std::thread::yield_now();
    }
    assert!(parker.wake());
    handle.join().unwrap();
}
