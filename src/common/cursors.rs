//! The head/tail cursor pair that defines a ring's occupancy.
//!
//! Three rings — spsc, mpsc and spmc — track their contents as two
//! monotonically increasing positions: `tail`, the next position a
//! producer will write, and `head`, the next position a consumer will
//! read. Neither wraps; the mask is applied only when indexing storage,
//! so `tail - head` is the number of items in the ring and stays
//! correct across the `usize` rollover that the subtraction wraps
//! through.
//!
//! What this type owns is that subtraction and the invariant behind it,
//! which was written out identically in all three rings. What it
//! deliberately does not own is *publication*, because the three rings
//! do not share one protocol:
//!
//! | ring | head | tail |
//! |------|------|------|
//! | spsc | one consumer, store  | one producer, store  |
//! | mpsc | one consumer, store  | many producers, CAS  |
//! | spmc | many consumers, CAS  | one producer, store  |
//!
//! A cursor written by exactly one thread can be stored; one written by
//! several must be claimed with a compare-exchange. That is a property
//! of the ring, not of the cursor, and the orderings follow from it
//! along with each ring's park handshake. Those stores stay at the
//! sites that can justify them, reached through [`Cursors::head`] and
//! [`Cursors::tail`].
//!
//! The one publication that *is* uniform is the single-consumer head
//! store: spsc and mpsc both release consumed slots with `SeqCst`, for
//! the same reason in both, so it lives here as
//! [`publish_head`](Cursors::publish_head) rather than being restated
//! at each of the six sites that do it.

use super::CachePadded;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A ring's producer and consumer positions, each on its own cache line.
///
/// `repr(C)` because the rings embedding this rely on field order to
/// keep the two cursors from sharing a line with each other or with the
/// immutable header fields ahead of them.
#[repr(C)]
pub struct Cursors {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

impl Cursors {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }

    /// The consumer cursor, for the ring-specific protocols that write
    /// it: a multi-consumer compare-exchange, or a borrow handed to a
    /// type that publishes later.
    #[inline]
    pub const fn head(&self) -> &AtomicUsize {
        &self.head.0
    }

    /// The producer cursor. See [`head`](Self::head); every ring
    /// publishes this one differently.
    #[inline]
    pub const fn tail(&self) -> &AtomicUsize {
        &self.tail.0
    }

    /// Publishes a single consumer's new head, releasing every slot
    /// below it back to the producer.
    ///
    /// `SeqCst` rather than `Release` on two counts. The slots must be
    /// visible as free, which `Release` alone would give; but this
    /// store is also the consumer's half of the park handshake, and has
    /// to be ordered against the producer's `SeqCst` arm in the
    /// opposite direction. Either the consumer's following wake sees
    /// the producer parked, or the producer's post-arm re-read sees
    /// this head — a `Release` store and an `Acquire` load are on
    /// different locations here and order neither way, leaving the
    /// interleaving where each misses the other and the producer sleeps
    /// on a ring that has space.
    ///
    /// Only correct where a single consumer owns the cursor. A ring
    /// whose consumers contend for positions claims them through
    /// [`head`](Self::head) instead.
    #[inline]
    pub fn publish_head(&self, head: usize) {
        self.head.0.store(head, Ordering::SeqCst);
    }

    /// The number of items in the ring.
    ///
    /// Both loads are `Relaxed` and independent, so a caller that is
    /// neither the producer nor the consumer can observe them from
    /// different moments and read a length that was never simultaneously
    /// true — including one above the capacity. It answers "is there
    /// work?", not "exactly how much".
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    #[must_use]
    pub fn is_full(&self, cap: usize) -> bool {
        self.len() == cap
    }

    /// The positions still holding values, for a ring being dropped.
    ///
    /// Takes `&mut self`, which is the whole point: every handle is
    /// gone, so the cursors are read without atomics and cannot move
    /// while the caller walks the range.
    #[inline]
    pub fn occupied(&mut self) -> Range<usize> {
        *self.head.0.get_mut()..*self.tail.0.get_mut()
    }
}

impl Default for Cursors {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::Cursors;
    use std::sync::atomic::Ordering;

    #[test]
    fn a_fresh_pair_is_empty() {
        let cursors = Cursors::new();
        assert_eq!(cursors.len(), 0);
        assert!(cursors.is_empty());
    }

    #[test]
    fn length_is_the_gap_between_the_cursors() {
        let cursors = Cursors::new();
        cursors.tail().store(7, Ordering::Relaxed);
        assert_eq!(cursors.len(), 7);
        cursors.publish_head(5);
        assert_eq!(cursors.len(), 2);
    }

    #[test]
    fn a_ring_is_full_when_the_gap_reaches_capacity() {
        let cursors = Cursors::new();
        cursors.tail().store(4, Ordering::Relaxed);
        assert!(cursors.is_full(4));
        assert!(!cursors.is_full(8));
        cursors.publish_head(1);
        assert!(!cursors.is_full(4));
    }

    #[test]
    fn length_survives_the_position_rollover() {
        let cursors = Cursors::new();
        cursors.publish_head(usize::MAX - 1);
        cursors.tail().store(usize::MAX.wrapping_add(3), Ordering::Relaxed);
        assert_eq!(cursors.len(), 4);
    }

    #[test]
    fn the_occupied_range_spans_the_unconsumed_positions() {
        let mut cursors = Cursors::new();
        cursors.publish_head(2);
        cursors.tail().store(5, Ordering::Relaxed);
        assert_eq!(cursors.occupied().collect::<Vec<_>>(), vec![2, 3, 4]);
    }

    #[test]
    fn a_drained_ring_has_nothing_to_drop() {
        let mut cursors = Cursors::new();
        cursors.publish_head(5);
        cursors.tail().store(5, Ordering::Relaxed);
        assert!(cursors.occupied().next().is_none());
    }

    #[test]
    fn a_head_that_overtook_the_tail_yields_no_positions() {
        let mut cursors = Cursors::new();
        cursors.publish_head(9);
        cursors.tail().store(4, Ordering::Relaxed);
        assert!(cursors.occupied().next().is_none());
    }

    #[test]
    fn a_published_head_is_visible_to_another_thread() {
        let cursors = std::sync::Arc::new(Cursors::new());
        let peer = std::sync::Arc::clone(&cursors);
        let handle = std::thread::spawn(move || {
            while peer.head().load(Ordering::SeqCst) != 3 {
                std::hint::spin_loop();
            }
        });
        cursors.publish_head(3);
        handle.join().unwrap();
    }

    #[test]
    fn each_cursor_occupies_its_own_cache_line() {
        assert_eq!(std::mem::align_of::<Cursors>(), 64);
        assert_eq!(std::mem::size_of::<Cursors>(), 128);
    }
}
