//! Unwind-safe close of the spsc consumer endpoint.

use super::RingBuffer;

/// Owns the obligation to close the consumer end and release the
/// producer.
///
/// [`Consumer::drop`](super::Consumer) drains the backlog before it
/// closes, so the values are gone before the producer is told the peer
/// is leaving. The drain runs `T::drop`, which is user code: unwinding
/// past the close leaves `consumer_closed` unset, and the ring looks
/// open for as long as it lives. A producer then never returns `Err`
/// from `push_block`, which parks untimed, and never observes
/// `is_closed`.
///
/// Holding one of these across the drain closes that window, keeping
/// the drain-then-close order that a parked producer relies on to claim
/// space the drain just freed.
///
/// The ring is held as a raw pointer because the drain needs `&mut
/// Consumer` for the whole loop this guard spans.
pub(super) struct ConsumerClose<T> {
    ring: *const RingBuffer<T>,
}

impl<T> ConsumerClose<T> {
    /// Arms the close.
    ///
    /// Construct this *before* the drain, whose `T::drop` may panic.
    ///
    /// # Safety
    ///
    /// `ring` must stay live until this guard is dropped.
    pub(super) const unsafe fn new(ring: *const RingBuffer<T>) -> Self {
        Self { ring }
    }
}

impl<T> Drop for ConsumerClose<T> {
    fn drop(&mut self) {
        // SAFETY: `new` requires the ring to outlive this guard; the
        // consumer's handle owns it for the whole of `Consumer::drop`.
        let ring = unsafe { &*self.ring };
        // Close then wake: the SeqCst store inside `close` and the
        // SeqCst load inside `wake_producer` are the two halves of the
        // handshake, so the producer cannot park after we decide not to
        // wake it.
        ring.consumer_closed.close();
        ring.wake_producer();
        #[cfg(feature = "async")]
        ring.producer_waker.flush();
    }
}
