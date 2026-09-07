//! Unwind-safe abandonment of a broadcast reservation.

use super::slot_state::SequenceWord;
use super::RingBuffer;

/// Owns the obligation to mark a reservation that was never committed
/// as abandoned.
///
/// The marker is what resolves the claim: a consumer sitting at the
/// position can advance past it, and a producer waiting on the aliasing
/// slot can reclaim it. When the reservation already holds a value, that
/// marker has to be published after the value's destructor — which may
/// panic.
///
/// Unwinding out of `T::drop` with the marker still ahead of it leaves
/// the claim unresolved for good: consumers stop at the position and
/// never reach anything published behind it.
///
/// Holding one of these across the destructor closes that window, since
/// the marker is published from this type's own `Drop`.
pub(super) struct ReservationAbandon<'a, T> {
    sequence: &'a SequenceWord,
    pos: usize,
    queue: &'a RingBuffer<T>,
}

impl<'a, T> ReservationAbandon<'a, T> {
    /// Arms the abandonment marker for a claimed position.
    ///
    /// Construct this *before* any code that might panic — the value's
    /// destructor, in particular.
    pub(super) const fn new(
        sequence: &'a SequenceWord,
        pos: usize,
        queue: &'a RingBuffer<T>,
    ) -> Self {
        Self {
            sequence,
            pos,
            queue,
        }
    }
}

impl<T> Drop for ReservationAbandon<'_, T> {
    fn drop(&mut self) {
        self.sequence.abandon(self.pos);
        self.queue.notify_consumers();
        self.queue.wake_producer();
        self.queue.notify_producers();
    }
}
