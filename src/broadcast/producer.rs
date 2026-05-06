use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::RingBuffer;
use crate::common::TOMBSTONE;

/// The producer side of a broadcast ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for slots
/// via atomic fetch-and-add (FAA).
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    pub(super) cached_min_head: std::cell::Cell<usize>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        #[cfg(feature = "async")]
        self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            cached_min_head: std::cell::Cell::new(0),
        }
    }
}

/// Returns true if at least one consumer slot is currently active.
/// Used by `push_async` to detect "all consumers gone" without an
/// explicit consumer-count atomic — the per-slot `active` flags are
/// the existing source of truth.
#[cfg(feature = "async")]
fn any_consumer_active<T>(q: &RingBuffer<T>) -> bool {
    q.consumer_slots
        .iter()
        .any(|s| s.active.load(Ordering::Acquire))
}

#[cfg(feature = "async")]
impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last producer gone: flag closed and flush all parked
            // consumers so their pop_async can return None after
            // their backlog drains.
            //
            // SeqCst pairs with the consumer's `closed.load(SeqCst)`
            // after register: the consumer's recheck must observe this
            // store, otherwise a consumer parked between "load closed
            // false" and "register" would miss both the close signal
            // and the flush, hanging forever.
            self.queue.closed.0.store(true, Ordering::SeqCst);
            self.queue.consumer_waker.flush();
        }
    }
}

impl<T> Producer<T> {
    #[inline]
    fn claim_slot(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        // Pre-check with L1/L2/L3 min_head cache: avoid a wasted FAA
        // when the buffer is clearly full.
        let current_tail = self.queue.tail.load(Ordering::Relaxed);
        if current_tail.wrapping_sub(self.cached_min_head.get()) >= self.queue.cap {
            let shared = self.queue.min_head_cache.load(Ordering::Acquire);
            self.cached_min_head.set(shared);

            if current_tail.wrapping_sub(shared) >= self.queue.cap {
                let min_head = self.queue.min_head();
                self.queue
                    .min_head_cache
                    .fetch_max(min_head, Ordering::Release);
                self.cached_min_head.set(min_head);

                if current_tail.wrapping_sub(min_head) >= self.queue.cap {
                    return None;
                }
            }
        }

        // Claim a unique position via FAA — always succeeds on the first
        // try, eliminating inter-producer cache-line contention entirely.
        let pos = self.queue.tail.fetch_add(1, Ordering::Relaxed);

        let slot = self.queue.slot(pos);

        // Wait for all consumers to advance past the slot's previous
        // occupant (pos - cap). Each producer spins on min_head — no
        // inter-producer contention on the tail cache line.
        if pos.wrapping_sub(self.cached_min_head.get()) >= self.queue.cap {
            let mut backoff = 0u32;
            loop {
                let min_head = self.queue.min_head();
                self.queue
                    .min_head_cache
                    .fetch_max(min_head, Ordering::Release);
                self.cached_min_head.set(min_head);
                if pos.wrapping_sub(min_head) < self.queue.cap {
                    break;
                }
                crate::common::cas_backoff(&mut backoff);
            }
        }

        // Drop old value if this slot was previously written.
        if std::mem::needs_drop::<T>() {
            let old_seq = slot.sequence.swap(0, Ordering::Acquire);
            if old_seq > 0 && old_seq != TOMBSTONE {
                // SAFETY: old_seq > 0 means data was initialized
                // by a prior push. We own the slot via FAA claim.
                unsafe {
                    slot.data.get().cast::<T>().drop_in_place();
                }
            }
        } else {
            slot.sequence.store(0, Ordering::Relaxed);
        }

        Some((slot.data.get(), &slot.sequence, pos))
    }

    /// Pushes a value into the broadcast ring buffer.
    ///
    /// Multiple producers can push concurrently. Returns `Err(val)` if
    /// the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, slot_seq, pos)) => {
                // SAFETY: We exclusively own this slot via FAA claim.
                unsafe { (*data_ptr).write(val) };
                slot_seq.store(pos * 2 + 1, Ordering::Release);
                #[cfg(feature = "async")]
                self.queue.wake_consumer_async();
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Pushes a value asynchronously, yielding to the executor when the
    /// ring is full (slowest consumer hasn't caught up). Returns
    /// `Err(val)` only when all consumers have been dropped.
    ///
    /// The future is cancel-safe: dropping it before completion leaves
    /// the ring unchanged (the value is moved back out on cancellation).
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub fn push_async(&self, val: T) -> impl std::future::Future<Output = Result<(), T>> + '_ {
        let mut val = Some(val);
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            // No active consumers means a push would just sit until
            // overwritten — treat as Err so producers can detect the
            // shutdown.
            if !any_consumer_active(&self.queue) {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    // Producers register on slot index = current tail
                    // mod PARK_SLOTS; the consumer's wake fires the
                    // matching slot.
                    let slot = self.queue.tail.load(Ordering::Relaxed);
                    self.queue.producer_waker.register(slot, cx);
                    if !any_consumer_active(&self.queue) {
                        return Poll::Ready(Err(val.take().unwrap()));
                    }
                    match self.push(val.take().unwrap()) {
                        Ok(()) => Poll::Ready(Ok(())),
                        Err(returned) => {
                            val = Some(returned);
                            Poll::Pending
                        }
                    }
                }
            }
        })
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Takes `&mut self` to guarantee at most one outstanding reservation
    /// per producer handle. If dropped without writing, the slot is
    /// tombstoned and consumers silently skip it.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        let queue = &*self.queue;
        self.claim_slot()
            .map(|(data_ptr, slot_sequence, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_sequence,
                pos,
                queue,
            })
    }

    /// Returns the number of items currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }
}

/// A write-reservation into a broadcast ring buffer slot.
///
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → slot is tombstoned (consumers skip it).
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_sequence: &'a AtomicUsize,
    pos: usize,
    #[allow(dead_code)] // only read when feature = "async" is enabled
    queue: &'a RingBuffer<T>,
}

// SAFETY: SlotWriter holds exclusive access to the slot (FAA claim).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send + Sync> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Requires [`commit_unchecked`](Self::commit_unchecked) (unsafe) to publish.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Exclusive access via FAA claim. The pointer is valid
        // because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access via FAA claim, valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_sequence: this.slot_sequence,
            pos: this.pos,
            queue: this.queue,
            committed: false,
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
        self.slot_sequence
            .store(self.pos * 2 + 1, Ordering::Release);
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        self.slot_sequence.store(TOMBSTONE, Ordering::Release);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and slot is tombstoned.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_sequence: &'a AtomicUsize,
    pos: usize,
    #[allow(dead_code)] // only read when feature = "async" is enabled
    queue: &'a RingBuffer<T>,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send + Sync> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to all consumers.
    #[inline]
    pub fn commit(mut self) {
        self.slot_sequence
            .store(self.pos * 2 + 1, Ordering::Release);
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        self.committed = true;
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot data.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            self.slot_sequence.store(TOMBSTONE, Ordering::Release);
            #[cfg(feature = "async")]
            self.queue.wake_consumer_async();
        }
    }
}
