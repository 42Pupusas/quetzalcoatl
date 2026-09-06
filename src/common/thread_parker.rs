//! Migration-safe park handle.
//!
//! A waiter's [`Thread`] handle names the thread that is parked *right
//! now*, not the thread that happened to park first. Every blocking
//! endpoint in this crate is `Send`, so a handle may park on one thread,
//! move, and park again on another. A write-once cell records the first
//! thread forever and sends every later wake to a thread that is no
//! longer parked — and, once that thread has exited, to nobody at all.
//!
//! [`ThreadParker`] is therefore re-armable: each `arm` publishes the
//! calling thread, replacing whatever was there.
//!
//! # Ownership
//!
//! The handle is owned through an [`AtomicPtr`] and every access is a
//! `swap`, so the thread that swaps a pointer *out* is its unique owner
//! and the only one that may drop it. This is what makes replacement
//! safe: a peer reading the slot cannot be looking at a box the armer is
//! concurrently freeing, because only one of them can receive it.
//!
//! # Why a claimed-out handle cannot lose a wake
//!
//! [`wake`](ThreadParker::wake) takes the handle rather than borrowing
//! it, so a second concurrent waker finds the slot empty and reports
//! `false`. That is not a dropped wakeup: the waker that *did* claim the
//! handle unparks the same waiter, so exactly one unpark is delivered
//! for the pair. Callers gate on their own wake bitmap or `parked` flag
//! and treat `false` as "someone else is delivering it."
//!
//! A stale handle cannot be observed within a single blocking call: a
//! thread cannot migrate while it is executing the loop that armed it,
//! so the handle a waker claims mid-call names the thread that armed it.
//! Staleness only spans calls, which is exactly what re-arming fixes.

use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread::Thread;

/// A re-armable park handle for one waiter.
pub struct ThreadParker {
    handle: AtomicPtr<Thread>,
}

impl ThreadParker {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            handle: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Publishes the calling thread as this slot's parked thread,
    /// replacing any previously armed handle.
    ///
    /// Call before publishing the "parked" signal (the wake bit or the
    /// `parked` flag) so a peer that observes that signal always finds
    /// a handle here.
    #[inline]
    pub fn arm(&self) {
        let next = Box::into_raw(Box::new(std::thread::current()));
        let prev = self.handle.swap(next, Ordering::AcqRel);
        if !prev.is_null() {
            // SAFETY: the swap removed `prev` from the slot, so this
            // thread is its sole owner and no peer can observe it again.
            drop(unsafe { Box::from_raw(prev) });
        }
    }

    /// Claims the armed handle and unparks it. Returns `true` only when
    /// this call claimed a handle and issued the unpark.
    #[inline]
    pub fn wake(&self) -> bool {
        if self.handle.load(Ordering::Relaxed).is_null() {
            return false;
        }
        let claimed = self.handle.swap(ptr::null_mut(), Ordering::AcqRel);
        if claimed.is_null() {
            return false;
        }
        // SAFETY: the swap removed `claimed` from the slot, so this
        // thread is its sole owner and no peer can observe it again.
        let handle = unsafe { Box::from_raw(claimed) };
        handle.unpark();
        true
    }

    /// Whether a handle is currently armed.
    ///
    /// For tests that need to observe that a waiter has committed to
    /// parking. Note this is *not* monotonic — [`wake`](Self::wake)
    /// clears it — so a probe must latch on the transition it sees.
    #[must_use]
    #[inline]
    pub fn is_armed(&self) -> bool {
        !self.handle.load(Ordering::Acquire).is_null()
    }

    /// The thread id of the armed handle, if any.
    ///
    /// Test-only: identifies *which* thread a wake would reach, which
    /// is the property that separates a re-armable parker from a
    /// write-once one.
    #[cfg(test)]
    fn armed_id(&self) -> Option<std::thread::ThreadId> {
        let armed = self.handle.load(Ordering::Acquire);
        if armed.is_null() {
            return None;
        }
        // SAFETY: the pointer is non-null and only replaced by a swap,
        // and no peer can free it while this single-threaded test holds
        // the only reference.
        Some(unsafe { &*armed }.id())
    }
}

impl Default for ThreadParker {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ThreadParker {
    fn drop(&mut self) {
        let stored = *self.handle.get_mut();
        if !stored.is_null() {
            // SAFETY: `&mut self` proves no peer holds a reference, and
            // the pointer was last published by `arm`.
            drop(unsafe { Box::from_raw(stored) });
        }
    }
}

#[cfg(test)]
mod tests {
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
}
