//! Unwind-safe release of an abandoned MPSC reservation.

use super::RingBuffer;
use crate::common::SlotSequence;

/// Owns the obligation to tombstone a reservation that was never
/// committed.
///
/// A producer that gives up a claimed position must mark it so the
/// consumer skips it and the space returns to the ring. When the
/// reservation already holds a value, that release has to happen after
/// the value's destructor — which may panic.
///
/// Unwinding out of `T::drop` with the release still ahead of it leaves
/// the position claimed forever: the consumer stops there waiting for a
/// publication that never comes, and the slot never returns to the
/// producers.
///
/// Holding one of these across the destructor closes that window, since
/// the tombstone is published from this type's own `Drop`.
pub(super) struct ReservationRelease<'a, T> {
    seq: &'a SlotSequence,
    queue: &'a RingBuffer<T>,
}

impl<'a, T> ReservationRelease<'a, T> {
    /// Arms the tombstone for a claimed position.
    ///
    /// Construct this *before* any code that might panic — the value's
    /// destructor, in particular.
    pub(super) const fn new(seq: &'a SlotSequence, queue: &'a RingBuffer<T>) -> Self {
        Self { seq, queue }
    }
}

impl<T> Drop for ReservationRelease<'_, T> {
    fn drop(&mut self) {
        self.seq.tombstone();
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
    }
}
