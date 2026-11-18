use super::{Registration, RegistrationIds};
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
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn every_minted_id_is_distinct() {
    let ids = RegistrationIds::new();
    let minted: Vec<_> = (0..1_000).map(|_| ids.mint()).collect();

    for (i, id) in minted.iter().enumerate() {
        assert!(
            !minted[..i].iter().any(|earlier| earlier == id),
            "id {id:?} was minted twice"
        );
    }
}

/// The defect this type exists to fix: one task's waker cannot
/// distinguish two of its own registrations, so identity has to
/// come from somewhere else.
#[test]
fn two_registrations_sharing_a_waker_are_still_distinct() {
    let ids = RegistrationIds::new();
    let waker = Waker::from(CountingWaker::new());

    let first = Registration::new(ids.mint(), waker.clone());
    let second = Registration::new(ids.mint(), waker);

    // Miri has no codegen-level vtable dedup, so `will_wake` on a
    // clone is unreliable there; the identity claim below is the
    // point of the test either way.
    #[cfg(not(miri))]
    assert!(
        first.will_wake_same_task_as(&second),
        "the two registrations must share a task waker"
    );
    assert!(!first.is(second.id()));
    assert!(!second.is(first.id()));
}

#[test]
fn a_registration_recognises_its_own_id() {
    let ids = RegistrationIds::new();
    let id = ids.mint();
    let registration = Registration::new(id, Waker::from(CountingWaker::new()));

    assert!(registration.is(id));
}

#[test]
fn waking_fires_the_registered_waker() {
    let ids = RegistrationIds::new();
    let waker = CountingWaker::new();
    let registration = Registration::new(ids.mint(), Waker::from(Arc::clone(&waker)));

    registration.wake();
    assert_eq!(waker.count.load(Ordering::Relaxed), 1);
}

#[test]
fn ids_from_one_set_are_minted_distinctly_across_threads() {
    let ids = Arc::new(RegistrationIds::new());
    let gate = Arc::new(std::sync::Barrier::new(8));

    // The threads are spawned into a `Vec` before any is joined, so
    // they mint concurrently; joining inside the first iterator
    // would serialise them and test nothing.
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let ids = Arc::clone(&ids);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                (0..256).map(|_| ids.mint()).collect::<Vec<_>>()
            })
        })
        .collect();

    let mut all = Vec::new();
    for thread in threads {
        all.extend(thread.join().unwrap());
    }
    let total = all.len();
    all.sort_unstable();
    all.dedup();

    assert_eq!(all.len(), total, "two threads were handed one id");
}
