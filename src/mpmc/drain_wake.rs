//! Unwind-safe producer wakes for MPMC drains.

use super::RingBuffer;
use super::Config;

/// Owns the obligation to wake producers after a drain has freed slots.
///
/// A drain releases each slot inside its loop, but wakes once at the
/// end: `wake_n(count)` releases as many parked producers as there are
/// freed slots, where a `wake_one` per item would cost a syscall each.
/// That leaves a window in which the ring has space and the producers do
/// not know, and unwinding through it — a panicking drain callback —
/// skips the wake entirely.
///
/// Blocking producers survive that on the `PARK_BACKSTOP` timeout in
/// `push_block`, which merely turns the bug into a stall. An async
/// producer has no such backstop: its waker is never invoked and the
/// task is never rescheduled. Waking from the destructor closes the
/// window, since it runs on the unwind path as well as the normal one.
pub(super) struct DrainWake<'a, T, C: Config> {
    ring: &'a RingBuffer<T, C>,
    released: usize,
}

impl<'a, T, C: Config> DrainWake<'a, T, C> {
    pub(super) const fn new(ring: &'a RingBuffer<T, C>) -> Self {
        Self { ring, released: 0 }
    }

    /// Records that a slot has been handed back to the producers.
    ///
    /// Call *after* the slot is released but *before* the user
    /// callback, so an unwind through the callback still wakes.
    pub(super) const fn released(&mut self) {
        self.released += 1;
    }
}

impl<T, C: Config> Drop for DrainWake<'_, T, C> {
    fn drop(&mut self) {
        if self.released == 0 {
            return;
        }
        self.ring.producer_park.wake_n(self.released);
        self.ring.notify_producers_n(self.released);
    }
}
