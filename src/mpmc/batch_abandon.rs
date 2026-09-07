//! Giving up a producer's unused batch positions.

use std::sync::atomic::Ordering;

use super::{Config, RingBuffer};

/// Hands back the positions a departing producer reserved but never
/// used.
///
/// A reserved position cannot simply be dropped: the next-round producer
/// for the same slot waits on `done[s]`, so an unreleased reservation
/// stalls that slot for good. Releasing it, though, means writing state
/// that the *previous* round's consumer may still be reading, so the
/// release has to wait for `done[s]` to reach this position first.
///
/// That wait used to be unconditional, which made dropping a producer
/// depend on a consumer still running. With the consumers gone it never
/// finished, so a handle could not be dropped — during cancellation or
/// unwinding, at the exact moment the peers are disappearing.
///
/// Once the last consumer is gone the wait is also pointless: nothing
/// will read the slot again, no producer is waiting for it (a blocked
/// producer returns `Err` on the same signal), and the ring is being
/// torn down. So the abandonment stops there and leaves the slot in the
/// previous round's state, which is what `RingBuffer::drop` needs in
/// order to drop a value the consumers never took.
pub(super) struct BatchAbandon<'a, T, C: Config> {
    queue: &'a RingBuffer<T, C>,
}

impl<'a, T, C: Config> BatchAbandon<'a, T, C> {
    pub(super) const fn new(queue: &'a RingBuffer<T, C>) -> Self {
        Self { queue }
    }

    /// Releases every position still set in `unused`, starting at
    /// `start`.
    pub(super) fn release_all(&self, start: usize, unused: u32) {
        let mut bits = unused;
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            self.release(start + bit);
            bits &= bits - 1;
        }
    }

    /// Releases one reserved position, or gives up if the consumers
    /// leave while waiting for the previous round.
    fn release(&self, pos: usize) {
        let q = self.queue;
        let done = q.done_slot(pos);
        let mut backoff = crate::common::backoff::Backoff::new();
        while done.load(Ordering::Acquire) != pos {
            // SeqCst: the post-arm half of the close handshake, as in
            // the blocking paths.
            if q.consumer_closed.is_closed_for_parking() {
                return;
            }
            // A consumer can be parked waiting for a slot this batch
            // already published, while we wait on the `done` that only
            // that consumer can advance. Waking it each lap breaks the
            // cycle.
            q.consumer_park.wake_one();
            q.notify_consumers();
            backoff.spin();
        }
        q.ready_slot(pos).store(pos + 2, Ordering::Release);
        // SeqCst: drains the store buffer so the wake below cannot miss
        // a producer that just parked on this slot.
        q.done_slot(pos).store(pos + q.cap, Ordering::SeqCst);
        // The slot is now free for its next-round producer, which may
        // be parked waiting for exactly this release.
        q.producer_park.wake_one();
        q.notify_producers();
    }
}
