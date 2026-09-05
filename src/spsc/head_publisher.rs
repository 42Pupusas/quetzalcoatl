//! Unwind-safe publication of the SPSC consumer head.

use std::sync::atomic::Ordering;

use super::RingBuffer;

/// Owns the obligation to publish the consumer's `head` cursor.
///
/// Consuming a slot happens in two steps: the value is moved (or
/// dropped) out of the slot, and only then may `head` advance — the
/// reverse order would let the producer refill a slot whose old value
/// is still being read.
///
/// That leaves a window in which the slot is empty but `head` still
/// points at it. If the thread unwinds through that window — a
/// panicking `drain` callback, a panicking `T::drop` — the stale `head`
/// tells [`Consumer::drop`](super::Consumer) that those slots are still
/// live, and it reads them a second time. For a `T` with a destructor
/// that is a double free.
///
/// Holding one of these across the window closes it: the cursor is
/// published by the destructor, so it runs on the unwind path as well
/// as the normal one.
pub(super) struct HeadPublisher<'a, T> {
    ring: &'a RingBuffer<T>,
    head: usize,
    advanced: bool,
}

impl<'a, T> HeadPublisher<'a, T> {
    pub(super) const fn new(ring: &'a RingBuffer<T>, head: usize) -> Self {
        Self {
            ring,
            head,
            advanced: false,
        }
    }

    pub(super) const fn position(&self) -> usize {
        self.head
    }

    /// Records that the slot at the current position has been consumed.
    ///
    /// Call this *after* the value has been moved out but *before* any
    /// code that might panic.
    pub(super) const fn advance(&mut self) {
        self.head += 1;
        self.advanced = true;
    }
}

impl<T> Drop for HeadPublisher<'_, T> {
    fn drop(&mut self) {
        if !self.advanced {
            return;
        }
        // SeqCst: see Consumer::pop. Pairs with the producer's SeqCst
        // `parked.store(true)` to close the missed-wakeup race, and
        // subsumes the Release needed to hand these slots back.
        self.ring.head.store(self.head, Ordering::SeqCst);
        self.ring.wake_producer();
        #[cfg(feature = "async")]
        self.ring.wake_producer_async();
    }
}
