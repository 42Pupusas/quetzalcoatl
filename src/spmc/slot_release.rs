//! Unwind-safe release of a consumed SPMC slot.

use super::done_word::DoneWord;
use super::RingBuffer;
use crate::capacity::Capacity;

/// Owns the obligation to hand one consumed slot back to the producer.
///
/// Reading a slot happens in two steps: the value is dropped out of the
/// slot, and only then may `done` advance — the reverse order would let
/// the producer refill a slot whose old value is still being read.
///
/// That leaves a window in which the value is gone but the slot still
/// reads as unreleased. Unwinding through it — a panicking `T::drop`
/// under a [`SlotReader`](super::SlotReader) — skips the release, and
/// the producer never gets the position back: it is lost for the life of
/// the ring, and a producer blocked on it never wakes, since
/// `push_block` parks untimed.
///
/// Holding one of these across the window closes it: the release runs in
/// the destructor, so it happens on the unwind path as well as the
/// normal one.
pub(super) struct SlotRelease<'a, T> {
    ring: &'a RingBuffer<T>,
    done: *const DoneWord,
    head: usize,
    capacity: Capacity,
}

impl<'a, T> SlotRelease<'a, T> {
    /// Arms the release for the slot at `head`.
    ///
    /// Construct this *before* any code that might panic — the value's
    /// destructor, in particular.
    ///
    /// # Safety
    ///
    /// `done` must point to the `DoneWord` of the slot at `head`, in a
    /// ring that outlives this guard.
    pub(super) const unsafe fn new(
        ring: &'a RingBuffer<T>,
        done: *const DoneWord,
        head: usize,
        capacity: Capacity,
    ) -> Self {
        Self {
            ring,
            done,
            head,
            capacity,
        }
    }
}

impl<T> Drop for SlotRelease<'_, T> {
    fn drop(&mut self) {
        // SAFETY: `new` requires `done` to name the slot at `head` in a
        // ring that outlives this guard.
        unsafe {
            (*self.done).release(self.head, self.capacity);
        }
        self.ring.wake_producer();
        #[cfg(feature = "async")]
        self.ring.wake_producer_async();
    }
}
