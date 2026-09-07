//! Batched release of consumed MPSC positions.


use super::RingBuffer;

/// Owns the obligation to publish a drain's head cursor and to wake the
/// producers it freed.
///
/// A drain releases two kinds of position: one that delivered a value,
/// and one that only carried an abandoned reservation. Both hand a slot
/// back to the producers, so both must be counted — publishing only when
/// a value came out leaves a tombstone-only batch having cleared the
/// slot metadata while `head` still names the tombstone, so the space is
/// never returned and the producers see a ring that stays full.
///
/// The count of released positions is therefore kept separately from the
/// count of delivered values: the former decides what to publish and how
/// many producers to wake, the latter is what the caller asked for.
///
/// Publication happens in the destructor, which also makes it unwind-
/// safe: a panicking drain callback leaves the positions already taken
/// out of the ring published as consumed rather than stranded.
pub(super) struct BatchRelease<'a, T> {
    ring: &'a RingBuffer<T>,
    head: usize,
    released: usize,
    delivered: usize,
}

impl<'a, T> BatchRelease<'a, T> {
    pub(super) const fn new(ring: &'a RingBuffer<T>, head: usize) -> Self {
        Self {
            ring,
            head,
            released: 0,
            delivered: 0,
        }
    }

    pub(super) const fn position(&self) -> usize {
        self.head
    }

    pub(super) const fn delivered(&self) -> usize {
        self.delivered
    }

    /// Records a position released without delivering a value: the slot
    /// held an abandoned reservation.
    ///
    /// Call after the slot's sequence has been handed to the next round.
    pub(super) const fn skip(&mut self) {
        self.head += 1;
        self.released += 1;
    }

    /// Records a position whose value has been moved out.
    ///
    /// Call *after* the move but *before* the user callback, so an
    /// unwind through the callback still publishes it.
    pub(super) const fn take(&mut self) {
        self.head += 1;
        self.released += 1;
        self.delivered += 1;
    }
}

impl<T> Drop for BatchRelease<'_, T> {
    fn drop(&mut self) {
        if self.released == 0 {
            return;
        }
        // SeqCst: see Consumer::pop. Drains the store buffer so the
        // sequence stores above are visible before the wake path loads
        // the park bitmap.
        self.ring.cursors.publish_head(self.head);
        self.ring.producer_park.wake_n(self.released);
        self.ring.notify_producers_n(self.released);
    }
}
