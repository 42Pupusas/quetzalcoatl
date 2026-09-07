//! Arc-wrapped broadcast ring buffer for large types.
//!
//! Wraps values in `Arc<T>` internally so that consumers get cheap
//! reference-counted clones instead of full `T::clone()` copies.
//! Best for large structs with many consumers.

use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::capacity::Capacity;

use super::RingBuffer;

/// Creates an Arc-wrapped broadcast ring buffer.
///
/// Equivalent to [`RingBuffer<Arc<T>>`] with ergonomic wrappers:
/// the producer accepts `T` directly (wraps in `Arc` internally),
/// and consumers receive `Arc<T>` (cheap refcount clone).
///
/// # When to use
///
/// Use this instead of plain [`RingBuffer`] when `T` is large and
/// cloning is expensive. The trade-off is one heap allocation per
/// push (for the `Arc`), but every consumer pop becomes O(1)
/// regardless of `T`'s size.
pub struct ArcRingBuffer<T>(RingBuffer<Arc<T>>);

impl<T> ArcRingBuffer<T> {
    /// Creates a new Arc-wrapped broadcast ring buffer.
    ///
    /// # Panics
    ///
    /// Panics if `max_consumers` is 0.
    #[must_use]
    pub fn new(capacity: Capacity, max_consumers: usize) -> Self {
        Self(RingBuffer::new(capacity, max_consumers))
    }

    /// Splits into a producer and the first consumer.
    pub fn split(self) -> (ArcProducer<T>, ArcConsumer<T>) {
        let (p, c) = self.0.split();
        (ArcProducer(p), ArcConsumer(c))
    }
}

/// Producer that wraps values in `Arc<T>` before publishing.
///
/// Obtained via [`ArcRingBuffer::split`]. Cloneable for multiple producers.
pub struct ArcProducer<T>(super::Producer<Arc<T>>);

impl<T> Clone for ArcProducer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + Sync> ArcProducer<T> {
    /// Pushes a value, wrapping it in `Arc` internally.
    ///
    /// Returns `Err(val)` if the buffer is full. Note: the `Arc`
    /// allocation happens before the fullness check, so a failed
    /// push still pays the allocation cost.
    ///
    /// # Panics
    ///
    /// Panics if the internal `Arc` unwrap fails (should never happen).
    pub fn push(&self, val: T) -> Result<(), T> {
        self.0.push(Arc::new(val)).map_err(|arc| {
            // Unwrap the Arc to return the original T.
            // SAFETY: We just created this Arc with refcount 1.
            Arc::try_unwrap(arc).ok().expect("Arc refcount should be 1")
        })
    }

    /// Pushes a value, wrapping it in `Arc` internally and blocking the
    /// calling thread when the ring is full until a consumer advances.
    /// Returns `Err(val)` only when all consumers have been dropped.
    ///
    /// See [`super::Producer::push_block`].
    ///
    /// # Panics
    ///
    /// Panics if the internal `Arc` unwrap fails (should never happen).
    pub fn push_block(&self, val: T) -> Result<(), T> {
        self.0.push_block(Arc::new(val)).map_err(|arc| {
            // SAFETY: We just created this Arc with refcount 1.
            Arc::try_unwrap(arc).ok().expect("Arc refcount should be 1")
        })
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns an
    /// [`ArcSlotWriter`] for writing.
    #[must_use]
    pub fn reserve(&mut self) -> Option<ArcSlotWriter<'_, T>> {
        self.0.reserve().map(|w| ArcSlotWriter { inner: w })
    }

    /// Reserves a slot for zero-copy writing, blocking the calling
    /// thread when the ring is full until a consumer advances. Returns
    /// `None` only when all consumers have been dropped.
    ///
    /// See [`super::Producer::reserve_block`].
    #[must_use]
    pub fn reserve_block(&mut self) -> Option<ArcSlotWriter<'_, T>> {
        self.0.reserve_block().map(|w| ArcSlotWriter { inner: w })
    }

    /// Returns the number of items currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.0.is_full()
    }

    /// Pushes a value asynchronously, wrapping it in `Arc` internally.
    /// See [`super::Producer::push_async`].
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub async fn push_async(&self, val: T) -> Result<(), T> {
        self.0.push_async(Arc::new(val)).await.map_err(|arc| {
            // SAFETY: We just created this Arc with refcount 1.
            Arc::try_unwrap(arc).ok().expect("Arc refcount should be 1")
        })
    }
}

/// Write handle for the Arc-wrapped broadcast buffer.
///
/// Obtained via [`ArcProducer::reserve`].
pub struct ArcSlotWriter<'a, T> {
    inner: super::SlotWriter<'a, Arc<T>>,
}

impl<'a, T> ArcSlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<Arc<T>> {
        self.inner.slot_mut()
    }

    /// Writes a value (wrapped in `Arc`) into the reserved slot,
    /// returning an [`ArcWrittenSlot`] that can be committed.
    pub fn write(self, val: T) -> ArcWrittenSlot<'a, T> {
        ArcWrittenSlot {
            inner: self.inner.write(Arc::new(val)),
        }
    }

    /// Commits without verifying initialization.
    ///
    /// # Safety
    ///
    /// The caller must have initialized the slot data via
    /// [`slot_mut`](Self::slot_mut).
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        // SAFETY: Caller guarantees the slot was initialized via slot_mut().
        // We forward the invariant to the inner SlotWriter.
        self.inner.commit_unchecked();
    }
}

/// An Arc-wrapped slot that has been initialized.
///
/// Call [`commit`](Self::commit) to publish.
pub struct ArcWrittenSlot<'a, T> {
    inner: super::WrittenSlot<'a, Arc<T>>,
}

impl<T> ArcWrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to all consumers.
    #[inline]
    pub fn commit(self) {
        self.inner.commit();
    }
}

/// Consumer that receives `Arc<T>` from the broadcast buffer.
///
/// Obtained via [`ArcRingBuffer::split`] or by cloning an existing
/// consumer. No `T: Clone` bound needed — cloning the `Arc` is O(1).
pub struct ArcConsumer<T>(super::Consumer<Arc<T>>);

impl<T> Clone for ArcConsumer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + Sync> ArcConsumer<T> {
    /// Pops the next item as an `Arc<T>`.
    ///
    /// This is a cheap refcount clone regardless of `T`'s size.
    #[must_use]
    pub fn pop(&mut self) -> Option<Arc<T>> {
        self.0.pop()
    }

    /// Returns a zero-copy read reference to the next item.
    ///
    /// The returned [`ArcSlotReader`] dereferences to `&T` (through
    /// the `Arc`). The consumer's head advances when the reader drops.
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<ArcSlotReader<'_, T>> {
        self.0.pop_ref().map(|r| ArcSlotReader(r))
    }

    /// Returns the number of items this consumer has yet to read.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if this consumer has no items to read.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` if this consumer's backlog has reached capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.0.is_full()
    }

    /// Pops the next item asynchronously as an `Arc<T>`. See
    /// [`super::Consumer::pop_async`].
    #[cfg(feature = "async")]
    #[allow(clippy::future_not_send)]
    pub async fn pop_async(&mut self) -> Option<Arc<T>> {
        self.0.pop_async().await
    }
}

/// Zero-copy read reference that dereferences through `Arc<T>` to `&T`.
pub struct ArcSlotReader<'a, T>(super::SlotReader<'a, Arc<T>>);

impl<T> std::ops::Deref for ArcSlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SlotReader derefs to &Arc<T>, then Arc derefs to &T
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::drop_counter::DropCounter;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ring<T>(cap: usize) -> ArcRingBuffer<T> {
        ArcRingBuffer::new(Capacity::exact(cap), 4)
    }

    #[test]
    fn a_pushed_value_comes_back_through_the_arc() {
        let (producer, mut consumer) = ring::<u64>(4).split();
        producer.push(42).unwrap();
        assert_eq!(*consumer.pop().unwrap(), 42);
    }

    #[test]
    fn a_ring_without_seats_is_rejected() {
        let attempt = std::panic::catch_unwind(|| ArcRingBuffer::<u64>::new(Capacity::exact(4), 0));
        assert!(attempt.is_err());
    }

    /// The point of the wrapper: every consumer gets the same allocation
    /// rather than a full copy of a large payload.
    #[test]
    fn two_consumers_receive_the_same_allocation() {
        let (producer, mut c1) = ring::<[u8; 2048]>(8).split();
        let mut c2 = c1.clone();
        producer.push([0xAB; 2048]).unwrap();

        let v1 = c1.pop().unwrap();
        let v2 = c2.pop().unwrap();
        assert!(Arc::ptr_eq(&v1, &v2));
        assert_eq!(v1[0], 0xAB);
    }

    /// The wrapper exists so `T` need not be `Clone`; the plain ring's
    /// `pop` requires it.
    #[test]
    fn a_payload_that_cannot_be_cloned_still_broadcasts() {
        #[derive(Debug)]
        struct NotClone(u64);

        let (producer, mut consumer) = ring::<NotClone>(4).split();
        producer.push(NotClone(99)).unwrap();
        assert_eq!(consumer.pop().unwrap().0, 99);
    }

    #[test]
    fn a_full_ring_hands_the_value_back() {
        let (producer, _consumer) = ring::<u32>(2).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(producer.push(3).unwrap_err(), 3);
    }

    /// A rejected push unwraps the `Arc` it speculatively allocated. The
    /// caller must get the original value back, dropped exactly once.
    #[test]
    fn a_rejected_value_is_returned_and_dropped_once() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (producer, _consumer) = ring::<DropCounter>(2).split();
        for _ in 0..2 {
            producer
                .push(DropCounter {
                    counter: Arc::clone(&counter),
                })
                .unwrap();
        }

        let returned = producer
            .push(DropCounter {
                counter: Arc::clone(&counter),
            })
            .unwrap_err();
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        drop(returned);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_reader_dereferences_past_the_arc_to_the_value() {
        let (producer, mut consumer) = ring::<String>(4).split();
        producer.push("hello".to_string()).unwrap();
        assert_eq!(&*consumer.pop_ref().unwrap(), "hello");
    }

    #[test]
    fn a_reserved_slot_publishes_what_was_written_to_it() {
        let (mut producer, mut consumer) = ring::<u64>(4).split();
        producer.reserve().unwrap().write(42).commit();
        assert_eq!(*consumer.pop().unwrap(), 42);
    }

    #[test]
    fn a_reservation_is_refused_when_the_ring_is_full() {
        let (mut producer, _consumer) = ring::<u64>(2).split();
        producer.reserve().unwrap().write(1).commit();
        producer.reserve().unwrap().write(2).commit();
        assert!(producer.reserve().is_none());
    }

    /// The raw path: initialize the slot through `slot_mut`, then commit
    /// without the writer having seen a value.
    #[test]
    fn a_slot_initialized_by_hand_can_be_committed_unchecked() {
        let (mut producer, mut consumer) = ring::<u64>(4).split();
        let mut writer = producer.reserve().unwrap();
        writer.slot_mut().write(Arc::new(7));
        // SAFETY: the slot was just initialized by the write above.
        unsafe { writer.commit_unchecked() };
        assert_eq!(*consumer.pop().unwrap(), 7);
    }

    #[test]
    fn an_uncommitted_reservation_publishes_nothing() {
        let (mut producer, mut consumer) = ring::<u64>(4).split();
        drop(producer.reserve().unwrap());
        assert!(consumer.pop().is_none());
    }

    #[test]
    fn a_push_on_an_empty_ring_does_not_block() {
        let (producer, mut consumer) = ring::<u64>(4).split();
        producer.push_block(1).unwrap();
        producer.push_block(2).unwrap();
        assert_eq!(*consumer.pop().unwrap(), 1);
        assert_eq!(*consumer.pop().unwrap(), 2);
    }

    /// With nobody left to drain, a blocking push must give the value
    /// back rather than wait forever.
    #[test]
    fn a_blocking_push_gives_up_once_every_consumer_has_gone() {
        let (producer, consumer) = ring::<u64>(2).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        drop(consumer);
        assert_eq!(producer.push_block(3).unwrap_err(), 3);
    }

    #[test]
    fn a_blocking_reservation_succeeds_on_an_empty_ring() {
        let (mut producer, mut consumer) = ring::<u64>(4).split();
        producer.reserve_block().unwrap().write(5).commit();
        assert_eq!(*consumer.pop().unwrap(), 5);
    }

    #[test]
    fn a_blocking_reservation_gives_up_once_every_consumer_has_gone() {
        let (mut producer, consumer) = ring::<u64>(2).split();
        producer.reserve_block().unwrap().write(1).commit();
        producer.reserve_block().unwrap().write(2).commit();
        drop(consumer);
        assert!(producer.reserve_block().is_none());
    }

    #[test]
    fn a_blocking_push_waits_for_a_consumer_to_advance() {
        let (producer, mut consumer) = ring::<u64>(2).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        let reader = std::thread::spawn(move || {
            assert_eq!(*consumer.pop().unwrap(), 1);
            consumer
        });
        let consumer = reader.join().unwrap();

        producer.push_block(3).unwrap();
        drop(consumer);
    }

    #[test]
    fn a_cloned_producer_writes_into_the_same_ring() {
        let (producer, mut consumer) = ring::<u64>(4).split();
        let second = producer.clone();
        producer.push(1).unwrap();
        second.push(2).unwrap();

        assert_eq!(*consumer.pop().unwrap(), 1);
        assert_eq!(*consumer.pop().unwrap(), 2);
    }

    #[test]
    fn an_untouched_ring_is_empty_from_both_ends() {
        let (producer, consumer) = ring::<u64>(4).split();
        assert!(producer.is_empty());
        assert!(!producer.is_full());
        assert_eq!(producer.len(), 0);
        assert!(consumer.is_empty());
        assert!(!consumer.is_full());
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn a_filled_ring_reports_full_from_both_ends() {
        let (producer, consumer) = ring::<u64>(4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        assert_eq!(producer.len(), 4);
        assert!(producer.is_full());
        assert!(!producer.is_empty());
        assert_eq!(consumer.len(), 4);
        assert!(consumer.is_full());
        assert!(!consumer.is_empty());
    }

    /// Each consumer has its own backlog, so one draining does not empty
    /// the other's view.
    #[test]
    fn each_consumer_reports_its_own_backlog() {
        let (producer, mut c1) = ring::<u64>(4).split();
        let c2 = c1.clone();
        producer.push(1).unwrap();
        producer.push(2).unwrap();

        c1.pop().unwrap();
        assert_eq!(c1.len(), 1);
        assert_eq!(c2.len(), 2);
    }

    /// The slot keeps its `Arc` after every consumer has read it; the
    /// payload dies with the ring, exactly once however many consumers
    /// held a handle.
    #[test]
    fn the_payload_is_dropped_once_when_the_ring_goes() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let (producer, mut c1) = ring::<DropCounter>(4).split();
            let mut c2 = c1.clone();
            producer
                .push(DropCounter {
                    counter: Arc::clone(&counter),
                })
                .unwrap();

            let v1 = c1.pop().unwrap();
            let v2 = c2.pop().unwrap();
            assert_eq!(counter.load(Ordering::Relaxed), 0);
            drop(v1);
            assert_eq!(counter.load(Ordering::Relaxed), 0);
            drop(v2);
            assert_eq!(counter.load(Ordering::Relaxed), 0);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    #[cfg(feature = "async")]
    fn a_value_pushed_asynchronously_is_popped_asynchronously() {
        let (producer, mut consumer) = ring::<u64>(4).split();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            producer.push_async(11).await.unwrap();
            assert_eq!(*consumer.pop_async().await.unwrap(), 11);
        }));
    }

    #[test]
    #[cfg(feature = "async")]
    fn an_async_pop_ends_once_the_producer_has_gone() {
        let (producer, mut consumer) = ring::<u64>(4).split();
        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(producer);
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            assert!(consumer.pop_async().await.is_none());
        }));
        h.join().unwrap();
    }

    #[test]
    #[cfg(feature = "async")]
    fn an_async_push_hands_the_value_back_once_every_consumer_has_gone() {
        let (producer, consumer) = ring::<u64>(4).split();
        for i in 0..4 {
            producer.push(i).unwrap();
        }
        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(consumer);
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            assert_eq!(producer.push_async(99).await, Err(99));
        }));
        h.join().unwrap();
    }
}
