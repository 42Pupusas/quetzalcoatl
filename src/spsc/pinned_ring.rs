use super::RingBuffer;
use crate::capacity::Capacity;

/// A heap-pinned ring buffer with a stable address, for exercising the
/// raw-pointer seam ([`RingBuffer::producer_from_raw`] /
/// [`RingBuffer::consumer_from_raw`]) in tests.
///
/// Those constructors hand out handles borrowing the ring for `'static`,
/// which production callers satisfy with `Box::leak`. A leak is correct
/// there — the ring really does live for the whole program — but in a
/// test it makes every run end with an unreclaimed allocation, which
/// Miri reports as a leak. `PinnedRing` provides the same guarantee the
/// `unsafe` contract asks for (a live ring at a fixed address that is
/// never moved) while still freeing the allocation, so the test suite
/// stays clean under `cargo +nightly miri test`.
///
/// Use [`PinnedRing::with`], which drops the ring only after the closure
/// returns, so handles cannot outlive the allocation.
pub(super) struct PinnedRing<T> {
    ptr: *mut RingBuffer<T>,
}

impl<T> PinnedRing<T> {
    /// Runs `body` against a pinned ring, then frees it.
    ///
    /// `body` receives the ring's address. Handles reconstructed from it
    /// must not escape the closure: they claim `'static`, but the
    /// allocation ends here.
    pub(super) fn with<R>(capacity: Capacity, body: impl FnOnce(&Self) -> R) -> R {
        let pinned = Self {
            ptr: Box::into_raw(Box::new(RingBuffer::new(capacity))),
        };
        body(&pinned)
    }

    pub(super) const fn as_ptr(&self) -> *const RingBuffer<T> {
        self.ptr.cast_const()
    }

    /// A `Send` handle to the ring's address, for the cross-thread test.
    pub(super) const fn shared(&self) -> SharedAddr<T> {
        SharedAddr(self.ptr.cast_const())
    }
}

/// A ring address that can cross a thread boundary.
///
/// The production seam this models passes the ring's address between
/// execution contexts as raw bits. Carrying a real pointer instead of a
/// `usize` keeps its provenance intact, so Miri can still validate every
/// access through it — an integer round-trip erases provenance and makes
/// Miri silently skip those checks.
#[derive(Clone, Copy)]
pub(super) struct SharedAddr<T>(*const RingBuffer<T>);

// SAFETY: the address is only used to reconstruct handles for a ring
// that `PinnedRing::with` keeps alive and unmoved for the whole closure,
// and `RingBuffer<T>` is `Sync` when `T: Send`.
unsafe impl<T: Send> Send for SharedAddr<T> {}

impl<T> SharedAddr<T> {
    pub(super) const fn get(self) -> *const RingBuffer<T> {
        self.0
    }
}

impl<T> Drop for PinnedRing<T> {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from `Box::into_raw` in `with` and is freed
        // exactly once, here. `with` returns only after `body` has run,
        // and every handle `body` created is dropped by then, so no
        // reference to the ring outlives this deallocation.
        drop(unsafe { Box::from_raw(self.ptr) });
    }
}
