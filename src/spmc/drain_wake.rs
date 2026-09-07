//! Unwind-safe producer wake for SPMC drains.

use super::RingBuffer;

/// Owns the obligation to wake the producer after a drain has freed
/// slots.
///
/// A drain releases each slot inside its loop, but the producer only
/// needs telling once, so the wake is batched to the end. That leaves a
/// window in which the ring has space and the producer does not know:
/// unwinding through it — a panicking drain callback — skips the wake
/// and leaves a producer parked on a ring that is no longer full.
///
/// `push_block` parks untimed, so nothing rescues it; the producer
/// sleeps until the process ends. Waking from the destructor closes the
/// window, since it runs on the unwind path as well as the normal one.
pub(super) struct DrainWake<'a, T> {
    ring: &'a RingBuffer<T>,
    released: bool,
}

impl<'a, T> DrainWake<'a, T> {
    pub(super) const fn new(ring: &'a RingBuffer<T>) -> Self {
        Self {
            ring,
            released: false,
        }
    }

    /// Records that a slot has been handed back to the producer.
    ///
    /// Call *after* the slot is released but *before* the user
    /// callback, so an unwind through the callback still wakes.
    pub(super) const fn released(&mut self) {
        self.released = true;
    }
}

impl<T> Drop for DrainWake<'_, T> {
    fn drop(&mut self) {
        if !self.released {
            return;
        }
        self.ring.wake_producer();
        #[cfg(feature = "async")]
        self.ring.wake_producer_async();
    }
}
