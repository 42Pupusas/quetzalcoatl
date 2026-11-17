//! Minting of the broadcast producer and first consumer.

use std::sync::Arc;

use super::floor_cache::FloorCache;
use super::{Consumer, Producer, RingBuffer};
use crate::capacity::Capacity;

/// A broadcast ring that has not been split into its handles yet.
///
/// [`RingBuffer`] is the state the endpoints share. This type owns one
/// until it is split, and is the only way to obtain the first
/// [`Producer`] and [`Consumer`] working on it.
pub struct SplitRing<T>(pub(super) RingBuffer<T>);

impl<T> SplitRing<T> {
    /// Creates a new broadcast ring buffer.
    ///
    /// `max_consumers` sets the maximum number of concurrent consumers.
    /// The first consumer is created by [`split`](Self::split). Additional
    /// consumers are created by cloning an existing consumer.
    ///
    /// # Panics
    ///
    /// Panics if `max_consumers` is 0.
    #[must_use]
    pub fn new(capacity: Capacity, max_consumers: usize) -> Self {
        Self(RingBuffer::new(capacity, max_consumers))
    }

    /// Splits the ring buffer into a [`Producer`] and the first [`Consumer`].
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = Arc::new(self.0);

        let slot_index = arc.consumers.subscribe_first();

        let park_slot = arc.producer_park_slots.lease();
        let producer = Producer {
            queue: Arc::clone(&arc),
            floor_cache: FloorCache::new(),
            park_slot,
        };
        let consumer = Consumer {
            queue: arc,
            slot_index,
        };
        (producer, consumer)
    }
}
