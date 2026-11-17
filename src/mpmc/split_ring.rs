//! Minting of the MPMC producer and consumer handles.

use std::sync::Arc;

use super::{Config, Consumer, DefaultConfig, Producer, RingBuffer};
use crate::capacity::Capacity;

/// An MPMC ring that has not been split into its handles yet.
///
/// [`RingBuffer`] is the state the endpoints share. This type owns one
/// until it is split, and is the only safe way to obtain the
/// [`Producer`] / [`Consumer`] pair working on it.
pub struct SplitRing<T, C: Config = DefaultConfig>(pub(super) RingBuffer<T, C>);

impl<T, C: Config> SplitRing<T, C> {
    /// Creates a new MPMC ring buffer.
    ///
    /// # Panics
    ///
    /// Panics if `capacity.get() < 4`. The per-slot tri-state
    /// encoding aliases at smaller capacities.
    ///
    /// Also panics if the capacity leaves no room for a producer:
    /// `usize::MAX / capacity / 32` must be at least 1.
    ///
    /// `C`'s tunables are validated at monomorphization via
    /// `ConfigBounds`: `PRODUCER_BATCH` must be in `1..=32`,
    /// the others must be `>= 1`.
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        Self(RingBuffer::new(capacity))
    }

    /// Splits the ring into a [`Producer`] and a [`Consumer`].
    /// Both handles are cloneable for additional producer/consumer
    /// threads.
    #[must_use]
    pub fn split(self) -> (Producer<T, C>, Consumer<T, C>) {
        let arc = Arc::new(self.0);
        let producer = Producer::new(arc.clone());
        let consumer = Consumer::new(arc);
        (producer, consumer)
    }
}
