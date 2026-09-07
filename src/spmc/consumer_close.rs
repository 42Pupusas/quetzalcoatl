//! Unwind-safe close of an spmc consumer endpoint.

use super::RingBuffer;

/// Owns the obligation to retire one consumer, closing the ring when it
/// is the last.
///
/// [`Consumer::drop`](super::Consumer) drops the values still held in
/// its claimed batch before it releases its park slot and decrements the
/// live count. Those destructors are user code: unwinding past the
/// release leaves the count permanently high, so the ring never reports
/// itself closed, and the park slot is never returned to the pool. A
/// producer in `push_block` \u2014 which parks untimed \u2014 waits forever for a
/// consumer that no longer exists.
///
/// The retire runs from this guard's destructor, so it happens on the
/// unwind path as well as the normal one. The batch release stays where
/// it is, ahead of the retire: a producer woken by the close must find
/// the freed positions already published.
pub(super) struct ConsumerClose<T> {
    ring: *const RingBuffer<T>,
    park_slot: crate::common::park_registry::ParkSlot,
}

impl<T> ConsumerClose<T> {
    /// Arms the retire for the consumer holding `park_slot`.
    ///
    /// Construct this *before* dropping the batch, whose `T::drop` may
    /// panic.
    ///
    /// # Safety
    ///
    /// `ring` must stay live until this guard is dropped, and
    /// `park_slot` must be this consumer's, not yet released.
    pub(super) const unsafe fn new(
        ring: *const RingBuffer<T>,
        park_slot: crate::common::park_registry::ParkSlot,
    ) -> Self {
        Self { ring, park_slot }
    }
}

impl<T> Drop for ConsumerClose<T> {
    fn drop(&mut self) {
        // SAFETY: `new` requires the ring to outlive this guard; the
        // consumer's handle owns it for the whole of `Consumer::drop`.
        let ring = unsafe { &*self.ring };
        ring.consumer_slots.release(self.park_slot);
        if ring.consumer_count_live.release() {
            // SeqCst store + wake_producer's SeqCst load of the parked
            // flag are the two halves of the close handshake. Reading
            // the parker handle directly skips the load and lets the
            // producer park after we decide not to wake it.
            ring.consumer_closed.close();
            ring.wake_producer();
            #[cfg(feature = "async")]
            ring.producer_waker.flush();
        }
    }
}
