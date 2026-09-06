//! Unwind-safe release of a consumed MPSC slot.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::RingBuffer;

/// Owns the obligation to hand one consumed slot back to the producers.
///
/// Reading a slot happens in two steps: the value is moved (or dropped)
/// out of the slot, and only then may the slot be released — the reverse
/// order would let a producer refill a slot whose old value is still
/// being read.
///
/// That leaves a window in which the slot is empty but its sequence
/// still marks it published and `head` still points at it. If the thread
/// unwinds through that window — a panicking `T::drop` under a
/// [`SlotReader`](super::SlotReader) — the stale state tells
/// [`RingBuffer::drop`] that the slot is still live, and it drops the
/// same value a second time. For a heap-allocating `T` that is a double
/// free.
///
/// Holding one of these across the window closes it: the release runs in
/// the destructor, so it happens on the unwind path as well as the
/// normal one.
pub(super) struct SlotRelease<'a, T> {
    ring: &'a RingBuffer<T>,
    seq: &'a AtomicUsize,
    head: usize,
}

impl<'a, T> SlotRelease<'a, T> {
    /// Arms the release for the slot at `head`.
    ///
    /// Construct this *before* any code that might panic — the value's
    /// destructor, in particular.
    pub(super) const fn new(ring: &'a RingBuffer<T>, seq: &'a AtomicUsize, head: usize) -> Self {
        Self { ring, seq, head }
    }
}

impl<T> Drop for SlotRelease<'_, T> {
    fn drop(&mut self) {
        self.seq
            .store((self.head + self.ring.cap) * 2, Ordering::Release);
        // SeqCst: see Consumer::pop. The `xchg` drains the store buffer,
        // publishing the sequence store above before the bitmap load
        // inside `wake_one_published`.
        self.ring.head.store(self.head + 1, Ordering::SeqCst);
        self.ring.producer_park.wake_one_published();
        self.ring.notify_producers();
    }
}
