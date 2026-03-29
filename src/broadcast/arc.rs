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

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns an
    /// [`ArcSlotWriter`] for writing.
    #[must_use]
    pub fn reserve(&mut self) -> Option<ArcSlotWriter<'_, T>> {
        self.0.reserve().map(|w| ArcSlotWriter { inner: w })
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
}

/// Write handle for the Arc-wrapped broadcast buffer.
///
/// Obtained via [`ArcProducer::reserve`].
pub struct ArcSlotWriter<'a, T> {
    inner: super::SlotWriter<'a, Arc<T>>,
}

impl<T> ArcSlotWriter<'_, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<Arc<T>> {
        self.inner.slot_mut()
    }

    /// Writes a value (wrapped in `Arc`) into the reserved slot.
    pub fn write(&mut self, val: T) -> &mut Arc<T> {
        self.inner.write(Arc::new(val))
    }

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
