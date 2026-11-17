//! Minting of the MPSC producer and consumer handles.

use std::sync::Arc;

use super::{Consumer, Producer, RingBuffer};
use crate::capacity::Capacity;

/// An MPSC ring that has not been split into its handles yet.
///
/// [`RingBuffer`] is the state the endpoints share. This type owns one
/// until it is split, and is the only safe way to obtain the
/// [`Producer`] / [`Consumer`] pair working on it.
pub struct SplitRing<T>(pub(super) RingBuffer<T>);

impl<T> SplitRing<T> {
    /// Creates a new MPSC ring buffer with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if the capacity exceeds 2^30.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        Self(RingBuffer::new(capacity))
    }

    /// Splits the ring buffer into a [`Producer`] and [`Consumer`] pair.
    ///
    /// Both handles hold an `Arc` to the ring buffer, so `T` must be
    /// `'static`. For borrowed data (non-`'static` `T`), use
    /// [`split_borrowed`](Self::split_borrowed).
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self.0);
        let park_slot = arc.producer_slots.lease();
        let producer = Producer::new_with(arc.clone(), park_slot);
        let consumer = Consumer::new_with(arc);
        (producer, consumer)
    }

    /// Splits a *borrowed* ring buffer into a producer/consumer pair
    /// whose lifetimes are tied to the ring.
    ///
    /// Unlike [`split`](Self::split), the handles hold `&RingBuffer<T>`
    /// instead of `Arc<RingBuffer<T>>`. This lets `T` carry lifetimes
    /// shorter than `'static`.
    ///
    /// The returned producer is not `Clone`. Create additional producers
    /// via [`Producer::new_producer`], which assigns each one its own
    /// park slot.
    ///
    /// Takes `&mut self` so the ring can be split only once. A second
    /// split would mint a second *consumer* — breaking the single-
    /// consumer contract this queue's `head` protocol relies on — and a
    /// second producer sharing park slot 0 with the first. The exclusive
    /// borrow makes that a borrow-check error rather than undefined
    /// behavior.
    ///
    /// ```compile_fail
    /// use quetzalcoatl::mpsc::SplitRing;
    /// use quetzalcoatl::capacity::Capacity;
    ///
    /// let mut ring = SplitRing::<String>::new(Capacity::exact(4));
    /// let (p1, mut c1) = ring.split_borrowed();
    /// let (p2, mut c2) = ring.split_borrowed();
    /// p1.push("a".to_string()).unwrap();
    /// ```
    #[must_use]
    #[allow(
        clippy::type_complexity,
        reason = "a pair of handles, each naming the ring it borrows"
    )]
    pub fn split_borrowed(&mut self) -> (Producer<T, &RingBuffer<T>>, Consumer<T, &RingBuffer<T>>) {
        let shared: &RingBuffer<T> = &self.0;
        let park_slot = shared.producer_slots.lease();
        let producer = Producer::new_with(shared, park_slot);
        (producer, Consumer::new_with(shared))
    }
}
