//! Unwind-safe release of a consumed MPMC slot.

use std::sync::atomic::Ordering;

use super::{Config, RingBuffer};

/// Owns the obligation to hand one consumed slot back to the producers.
///
/// Reading a slot happens in two steps: the value is moved (or dropped)
/// out of the slot, and only then may `done` advance — the reverse order
/// would let the producer for the next round refill a slot whose old
/// value is still being read.
///
/// That leaves a window in which the slot is empty but still reads as
/// "claimed but not released". If the thread unwinds through that window
/// — a panicking `T::drop` under a [`SlotReader`](super::SlotReader) —
/// [`RingBuffer::drop`] explicitly treats that state as still holding
/// live data, and drops the same value a second time. For a
/// heap-allocating `T` that is a double free.
///
/// Holding one of these across the window closes it: the release runs in
/// the destructor, so it happens on the unwind path as well as the
/// normal one.
pub(super) struct SlotRelease<'a, T, C: Config> {
    queue: &'a RingBuffer<T, C>,
    pos: usize,
    round_pos: usize,
}

impl<'a, T, C: Config> SlotRelease<'a, T, C> {
    /// Arms the release for the slot at `pos` in round `round_pos`.
    ///
    /// Construct this *before* any code that might panic — the value's
    /// destructor, in particular.
    pub(super) const fn new(queue: &'a RingBuffer<T, C>, pos: usize, round_pos: usize) -> Self {
        Self {
            queue,
            pos,
            round_pos,
        }
    }
}

impl<T, C: Config> Drop for SlotRelease<'_, T, C> {
    fn drop(&mut self) {
        // SeqCst — see Consumer::pop. The store-buffer drain is
        // necessary so the upcoming wake_one's wake.load doesn't miss a
        // parked producer's bit.
        self.queue
            .done_slot(self.pos)
            .store(
                self.round_pos + self.queue.capacity.get(),
                Ordering::SeqCst,
            );
        self.queue.producer_park.wake_one();
        self.queue.notify_producers();
    }
}
