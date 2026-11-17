//! Minting of the SPSC producer and consumer handles.

use std::sync::Arc;

use super::{Consumer, Producer, RingBuffer};
use crate::capacity::Capacity;

/// An SPSC ring that has not been split into its handles yet.
///
/// [`RingBuffer`] is the state the two endpoints share. This type owns
/// one until it is split, and is the only safe way to obtain the
/// [`Producer`] / [`Consumer`] pair working on it.
pub struct SplitRing<T>(RingBuffer<T>);

impl<T> SplitRing<T> {
    /// Creates a new SPSC ring buffer with the given capacity.
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
        let producer = Producer::new(arc.clone());
        let consumer = Consumer::new_with(arc);
        (producer, consumer)
    }

    /// Splits a *borrowed* ring buffer into a producer/consumer pair
    /// whose lifetimes are tied to the ring.
    ///
    /// Unlike [`split`](Self::split), the handles hold `&RingBuffer<T>`
    /// instead of `Arc<RingBuffer<T>>`. This lets `T` carry lifetimes
    /// shorter than `'static` — the borrow checker proves the ring
    /// outlives both handles, so pushing `T<'a>` is safe as long as
    /// `'a` outlives the ring.
    ///
    /// Takes `&mut self` so the ring can be split only once: a second
    /// split would mint a second producer *and* a second consumer, and
    /// two producers each writing from their own private cursor publish
    /// a `tail` past a slot neither initialized — the consumer then
    /// reads uninitialized memory as a `T`. The exclusive borrow makes
    /// that a borrow-check error rather than undefined behavior.
    ///
    /// Use with [`std::thread::scope`] to move the handles into scoped
    /// threads:
    ///
    /// ```
    /// use quetzalcoatl::spsc::SplitRing;
    /// use quetzalcoatl::capacity::Capacity;
    ///
    /// let mut ring = SplitRing::<&str>::new(Capacity::exact(4));
    /// let (producer, mut consumer) = ring.split_borrowed();
    ///
    /// std::thread::scope(|s| {
    ///     s.spawn(move || {
    ///         producer.push("hello").unwrap();
    ///     });
    ///     s.spawn(move || {
    ///         loop {
    ///             if let Some(v) = consumer.pop() {
    ///                 assert_eq!(v, "hello");
    ///                 break;
    ///             }
    ///             std::thread::yield_now();
    ///         }
    ///     });
    /// });
    /// ```
    ///
    /// Splitting the same ring twice is rejected at compile time:
    ///
    /// ```compile_fail
    /// use quetzalcoatl::spsc::SplitRing;
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
    pub const fn split_borrowed(
        &mut self,
    ) -> (Producer<T, &RingBuffer<T>>, Consumer<T, &RingBuffer<T>>) {
        let shared: &RingBuffer<T> = &self.0;
        (Producer::new_with(shared), Consumer::new_with(shared))
    }
}
