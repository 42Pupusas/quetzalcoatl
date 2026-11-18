use super::*;
use std::sync::atomic::AtomicUsize as StdAtomicUsize;
use std::sync::Arc;

#[test]
fn a_fresh_count_holds_the_single_split_endpoint() {
    assert_eq!(EndpointCount::new().live(), 1);
}

#[test]
fn registering_stops_at_the_limit() {
    let count = EndpointCount::new();
    assert!(count.try_register(3));
    assert!(count.try_register(3));
    assert!(!count.try_register(3));
    assert_eq!(count.live(), 3);
}

#[test]
fn a_limit_of_one_admits_no_clone() {
    let count = EndpointCount::new();
    assert!(!count.try_register(1));
    assert!(count.release());
}

#[test]
fn a_released_handle_frees_room_under_the_limit() {
    let count = EndpointCount::new();
    assert!(count.try_register(2));
    assert!(!count.try_register(2));
    assert!(!count.release());
    assert!(count.try_register(2));
}

#[test]
fn concurrent_registrations_never_pass_the_limit() {
    let count = Arc::new(EndpointCount::new());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let count = Arc::clone(&count);
        handles.push(std::thread::spawn(move || {
            (0..64).filter(|_| count.try_register(100)).count()
        }));
    }
    let admitted: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(admitted, 99);
    assert_eq!(count.live(), 100);
}

#[test]
fn the_sole_endpoint_releasing_is_the_last() {
    let count = EndpointCount::new();
    assert!(count.release());
}

#[test]
fn a_registered_clone_makes_the_first_release_not_the_last() {
    let count = EndpointCount::new();
    count.register();
    assert!(!count.release());
    assert!(count.release());
}

#[test]
fn every_registered_handle_is_counted() {
    let count = EndpointCount::new();
    for _ in 0..7 {
        count.register();
    }
    assert_eq!(count.live(), 8);
}

#[test]
fn exactly_one_of_many_concurrent_releases_reports_last() {
    let count = Arc::new(EndpointCount::new());
    for _ in 0..15 {
        count.register();
    }
    let lasts = Arc::new(StdAtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let count = Arc::clone(&count);
        let lasts = Arc::clone(&lasts);
        handles.push(std::thread::spawn(move || {
            if count.release() {
                lasts.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(lasts.load(Ordering::Acquire), 1);
    assert_eq!(count.live(), 0);
}

#[test]
fn registering_from_many_threads_counts_every_clone() {
    let count = Arc::new(EndpointCount::new());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let count = Arc::clone(&count);
        handles.push(std::thread::spawn(move || {
            for _ in 0..64 {
                count.register();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(count.live(), 1 + 8 * 64);
}

#[test]
fn the_count_occupies_its_own_cache_line() {
    assert_eq!(std::mem::align_of::<EndpointCount>(), 64);
}
