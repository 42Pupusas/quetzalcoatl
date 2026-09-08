//! Unwind-safe producer wakes for MPMC drains.

use super::RingBuffer;
use super::Config;

/// Owns the obligation to wake producers after a drain has freed slots.
///
/// A drain releases each slot inside its loop. The blocking-side wake
/// is routed at release time, because each freed position belongs to
/// one specific reserved batch and a batch of wakes handed out
/// round-robin at the end could spend every one of them on producers
/// that cannot use them. The async notification has no position to
/// route by and is issued once for the whole drain.
///
/// That leaves a window in which the ring has space and async
/// producers do not know, and unwinding through it — a panicking drain
/// callback — skips the notification entirely, so the task is never
/// rescheduled. Notifying from the destructor closes the window, since
/// it runs on the unwind path as well as the normal one.
pub(super) struct DrainWake<'a, T, C: Config> {
    ring: &'a RingBuffer<T, C>,
    released: usize,
}

impl<'a, T, C: Config> DrainWake<'a, T, C> {
    pub(super) const fn new(ring: &'a RingBuffer<T, C>) -> Self {
        Self { ring, released: 0 }
    }

    /// Records that the slot for `pos` has been handed back to the
    /// producers, and wakes the one that reserved it.
    ///
    /// Call *after* the slot is released but *before* the user
    /// callback, so an unwind through the callback still wakes.
    pub(super) fn released(&mut self, pos: usize) {
        self.ring.wake_producer_for(pos);
        self.released += 1;
    }
}

impl<T, C: Config> Drop for DrainWake<'_, T, C> {
    fn drop(&mut self) {
        if self.released == 0 {
            return;
        }
        self.ring.notify_producers_n(self.released);
    }
}
