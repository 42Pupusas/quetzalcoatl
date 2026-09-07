//! Unwind-safe head advance for a broadcast consumer.

use super::RingBuffer;

/// Owns the obligation to advance one consumer past a position it has
/// finished with.
///
/// [`Consumer::pop`](super::Consumer::pop) clones the value out of the
/// slot and only then advances. `Clone` is user code and may panic, and
/// unwinding past the advance leaves the consumer's head on the position
/// it just tried to read: the next `pop` re-reads the same value and
/// panics again, forever. The consumer never reaches a later value, and
/// its stale head holds `min_head` down so the producer cannot reclaim
/// the capacity either.
///
/// Advancing anyway is what [`SlotReader`](super::SlotReader) already
/// does — its `Drop` advances whether or not the borrower panicked —
/// and it costs nothing here, because a broadcast consumer never owns
/// the value: the producer drops it when the slot is overwritten.
pub(super) struct HeadAdvance<'a, T> {
    queue: &'a RingBuffer<T>,
    slot_index: usize,
    head: usize,
}

impl<'a, T> HeadAdvance<'a, T> {
    /// Arms the advance past `head` for the consumer at `slot_index`.
    ///
    /// Construct this *before* any code that might panic — the value's
    /// `Clone`, in particular.
    pub(super) const fn new(queue: &'a RingBuffer<T>, slot_index: usize, head: usize) -> Self {
        Self {
            queue,
            slot_index,
            head,
        }
    }
}

impl<T> Drop for HeadAdvance<'_, T> {
    fn drop(&mut self) {
        self.queue
            .consumers
            .publish_head_past(self.slot_index, self.head);
        self.queue.wake_producer();
        self.queue.notify_producers();
    }
}
