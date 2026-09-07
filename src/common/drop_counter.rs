//! Payload types whose destructors are observable, for tests that need
//! to prove a value was dropped exactly once.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A payload that counts its own drops through a shared counter.
#[derive(Clone, Debug)]
pub struct DropCounter {
    pub counter: Arc<AtomicUsize>,
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// A [`DropCounter`] that borrows its counter rather than sharing an
/// `Arc`.
///
/// For tests that deliberately `mem::forget` a value. Forgetting a
/// `DropCounter` also leaks its `Arc` allocation, which Miri's leak
/// checker reports and which forces the test to be skipped there. This
/// type owns no heap memory, so the intended leak — the destructor
/// never running — stays observable while Miri still checks the test.
#[derive(Clone, Debug)]
pub struct BorrowedDropCounter<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> BorrowedDropCounter<'a> {
    pub const fn new(counter: &'a AtomicUsize) -> Self {
        Self { counter }
    }
}

impl Drop for BorrowedDropCounter<'_> {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_the_payload_bumps_the_shared_counter() {
        let counter = Arc::new(AtomicUsize::new(0));
        drop(DropCounter {
            counter: counter.clone(),
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn each_clone_counts_its_own_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let original = DropCounter {
            counter: counter.clone(),
        };
        drop(original.clone());
        drop(original);
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn a_forgotten_borrowed_counter_records_nothing() {
        let counter = AtomicUsize::new(0);
        std::mem::forget(BorrowedDropCounter::new(&counter));
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_dropped_borrowed_counter_records_one() {
        let counter = AtomicUsize::new(0);
        drop(BorrowedDropCounter::new(&counter));
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_borrowed_counter_owns_no_heap_allocation() {
        assert_eq!(
            std::mem::size_of::<BorrowedDropCounter<'_>>(),
            std::mem::size_of::<&AtomicUsize>()
        );
    }
}
