//! Ownership of a slot value that has been written but not published.
//!
//! Between `write` and `commit` the producer owns an initialized value
//! that no consumer can reach. Whoever holds it must drop it if the
//! commit never happens, and must *stop* owning it the instant the
//! commit publishes — a wake path can panic, and a guard still armed
//! at that point would drop a value a consumer already owns.

use std::mem::MaybeUninit;

/// A written-but-unpublished slot value.
///
/// Armed on construction: dropping it drops the value in place. Call
/// [`commit`](Self::commit) once the value is published to hand
/// ownership to the consumer.
///
/// Each ring pairs the value with its own way of releasing the slot
/// (a tombstone, an abandonment marker, a returned batch bit, or
/// nothing at all). [`drop_if_uncommitted`](Self::drop_if_uncommitted)
/// reports whether the release is owed.
pub struct UncommittedSlot<T> {
    data: *mut MaybeUninit<T>,
    armed: bool,
}

// SAFETY: the guard owns the value it points at — the constructor's
// contract is that no other thread can observe it — so moving the
// guard moves a `T`, and nothing more.
unsafe impl<T: Send> Send for UncommittedSlot<T> {}

impl<T> UncommittedSlot<T> {
    /// # Safety
    ///
    /// `data` must point to an initialized `T` that no other thread can
    /// observe, and must stay valid for the lifetime of this guard.
    pub const unsafe fn armed(data: *mut MaybeUninit<T>) -> Self {
        Self { data, armed: true }
    }

    /// Releases ownership of the value.
    ///
    /// Call this *before* publishing, not after: a panic between the
    /// publish and this call would drop a value the consumer owns.
    pub const fn commit(&mut self) {
        self.armed = false;
    }

    /// Whether the guard still owns a value, and so still owes both the
    /// destructor and the slot release.
    ///
    /// Ask this *before* [`drop_if_uncommitted`](Self::drop_if_uncommitted)
    /// when the release must survive a panicking `T::drop`: the return
    /// value of that call is only observable once the destructor has
    /// already run, so an unwind never reaches it.
    pub const fn is_armed(&self) -> bool {
        self.armed
    }

    /// Drops the value if it was never committed, and reports whether
    /// it did — a `true` means the caller still owes the slot release.
    ///
    /// The report is unreachable when `T::drop` panics; a caller whose
    /// release must run on the unwind path should arm a guard from
    /// [`is_armed`](Self::is_armed) instead of branching on this.
    ///
    /// Idempotent: a second call reports `false` and drops nothing.
    pub fn drop_if_uncommitted(&mut self) -> bool {
        if !self.armed {
            return false;
        }
        self.armed = false;
        // SAFETY: `armed` held, so the constructor's guarantee stands
        // and no commit has transferred the value away.
        unsafe {
            self.data.cast::<T>().drop_in_place();
        }
        true
    }
}

impl<T> Drop for UncommittedSlot<T> {
    fn drop(&mut self) {
        self.drop_if_uncommitted();
    }
}

#[cfg(test)]
mod tests {
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

}
