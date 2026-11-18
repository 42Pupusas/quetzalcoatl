use super::*;
use crate::common::drop_counter::BorrowedDropCounter;
use std::ptr::addr_of_mut;
use std::sync::atomic::{AtomicUsize, Ordering};

fn count(counter: &AtomicUsize) -> usize {
    counter.load(Ordering::Relaxed)
}

#[test]
fn dropping_an_armed_guard_drops_the_value() {
    let counter = AtomicUsize::new(0);
    let mut storage = MaybeUninit::new(BorrowedDropCounter::new(&counter));
    // SAFETY: storage holds an initialized value reachable only here.
    let guard = unsafe { UncommittedSlot::armed(addr_of_mut!(storage)) };
    assert_eq!(count(&counter), 0);
    drop(guard);
    assert_eq!(count(&counter), 1);
}

#[test]
fn a_committed_guard_leaves_the_value_to_the_consumer() {
    let counter = AtomicUsize::new(0);
    let mut storage = MaybeUninit::new(BorrowedDropCounter::new(&counter));
    // SAFETY: as above.
    let mut guard = unsafe { UncommittedSlot::armed(addr_of_mut!(storage)) };
    guard.commit();
    drop(guard);
    assert_eq!(count(&counter), 0);
    // SAFETY: the guard disowned it, so this test owns it now.
    unsafe { storage.assume_init_drop() };
    assert_eq!(count(&counter), 1);
}

#[test]
fn an_uncommitted_drop_reports_that_the_release_is_owed() {
    let counter = AtomicUsize::new(0);
    let mut storage = MaybeUninit::new(BorrowedDropCounter::new(&counter));
    // SAFETY: as above.
    let mut guard = unsafe { UncommittedSlot::armed(addr_of_mut!(storage)) };
    assert!(guard.drop_if_uncommitted());
    assert_eq!(count(&counter), 1);
}

#[test]
fn a_committed_guard_owes_no_release() {
    let counter = AtomicUsize::new(0);
    let mut storage = MaybeUninit::new(BorrowedDropCounter::new(&counter));
    // SAFETY: as above.
    let mut guard = unsafe { UncommittedSlot::armed(addr_of_mut!(storage)) };
    guard.commit();
    assert!(!guard.drop_if_uncommitted());
    assert_eq!(count(&counter), 0);
    // SAFETY: the guard disowned it.
    unsafe { storage.assume_init_drop() };
}

#[test]
fn the_value_is_dropped_exactly_once_however_often_it_is_asked() {
    let counter = AtomicUsize::new(0);
    let mut storage = MaybeUninit::new(BorrowedDropCounter::new(&counter));
    // SAFETY: as above.
    let mut guard = unsafe { UncommittedSlot::armed(addr_of_mut!(storage)) };
    assert!(guard.drop_if_uncommitted());
    assert!(!guard.drop_if_uncommitted());
    assert!(!guard.drop_if_uncommitted());
    drop(guard);
    assert_eq!(count(&counter), 1);
}

#[test]
fn committing_after_the_value_was_dropped_changes_nothing() {
    let counter = AtomicUsize::new(0);
    let mut storage = MaybeUninit::new(BorrowedDropCounter::new(&counter));
    // SAFETY: as above.
    let mut guard = unsafe { UncommittedSlot::armed(addr_of_mut!(storage)) };
    assert!(guard.drop_if_uncommitted());
    guard.commit();
    drop(guard);
    assert_eq!(count(&counter), 1);
}

#[test]
fn a_forgotten_guard_leaks_the_value_rather_than_publishing_it() {
    let counter = AtomicUsize::new(0);
    let mut storage = MaybeUninit::new(BorrowedDropCounter::new(&counter));
    // SAFETY: as above.
    let guard = unsafe { UncommittedSlot::armed(addr_of_mut!(storage)) };
    std::mem::forget(guard);
    assert_eq!(count(&counter), 0);
    // SAFETY: nothing else can reach it; this test reclaims it so
    // the intended leak stays observable without leaking memory.
    unsafe { storage.assume_init_drop() };
    assert_eq!(count(&counter), 1);
}
