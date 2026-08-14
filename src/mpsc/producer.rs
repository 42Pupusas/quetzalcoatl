use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::RingBuffer;
use crate::common::park::{BACKOFF_PARK_THRESHOLD, PARK_MASK};
use crate::common::TOMBSTONE;

/// The producer side of an MPSC ring buffer.
///
/// Generic over `R`: the ring reference. Defaults to `Arc<RingBuffer<T>>`
/// (owned split via [`RingBuffer::split`]). When `R = &'a RingBuffer<T>`
/// (borrowed split via [`RingBuffer::split_borrowed`]), `T` may carry
/// lifetimes shorter than `'static`.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// (when `R = Arc`) — each clone shares the same underlying buffer and
/// claims slots through an atomic compare-and-exchange operation on the tail.
pub struct Producer<T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    pub(super) queue: R,
    pub(super) cached_head: std::cell::Cell<usize>,
    /// Stable park slot for this producer (mod `PARK_SLOTS`). Used by
    /// [`Producer::push_block`] to register on the wake bitmap.
    pub(super) park_slot: usize,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        let park_idx = self
            .queue
            .producer_park_idx
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        Self {
            queue: Arc::clone(&self.queue),
            cached_head: std::cell::Cell::new(0),
            park_slot: park_idx & PARK_MASK,
        }
    }
}

// SAFETY: Producer holds write access. The Cell fields are only
// touched by a single thread at a time (no Sync). R is Send, and
// RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Producer<T, R> {}

impl<T, R: Deref<Target = RingBuffer<T>>> Producer<T, R> {
    pub(super) const fn new_with(queue: R, park_slot: usize) -> Self {
        Self {
            queue,
            cached_head: std::cell::Cell::new(0),
            park_slot,
        }
    }

    #[inline]
    fn ring(&self) -> &RingBuffer<T> {
        &self.queue
    }

    #[inline]
    fn claim_slot(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        let mut current_tail = self.ring().tail.load(Ordering::Relaxed);
        let mut backoff = 0u32;
        loop {
            if current_tail.wrapping_sub(self.cached_head.get()) >= self.ring().cap {
                let head = self.ring().head.load(Ordering::Acquire);
                self.cached_head.set(head);

                if current_tail.wrapping_sub(head) >= self.ring().cap {
                    return None;
                }
            }

            match self.ring().tail.compare_exchange_weak(
                current_tail,
                current_tail + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(pos) => {
                    let slot = self.ring().slot(pos);
                    return Some((slot.data.get(), &slot.sequence, pos));
                }
                Err(observed_tail) => {
                    current_tail = observed_tail;
                    crate::common::cas_backoff(&mut backoff);
                }
            }
        }
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Multiple producers can push concurrently.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, slot_seq, pos)) => {
                unsafe { (*data_ptr).write(val) };
                slot_seq.store(pos * 2 + 1, Ordering::Release);
                self.ring().wake_consumer();
                #[cfg(feature = "async")]
                self.ring().wake_consumer_async();
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Pushes a value, blocking the calling thread when the ring is
    /// full until the consumer makes space. Returns `Err(val)` only
    /// when the [`Consumer`](super::Consumer) has been dropped (no
    /// consumer left to drain).
    ///
    /// Spins via the shared backoff schedule first, then sets a wake
    /// bit in the producer wake bitmap and parks. The consumer
    /// signals after every pop (or batched at end of `drain`) gated
    /// on a single `Relaxed` load.
    pub fn push_block(&self, mut val: T) -> Result<(), T> {
        let q = self.ring();
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        loop {
            if q.consumer_closed.0.load(Ordering::Acquire) {
                return Err(val);
            }
            match self.push(val) {
                Ok(()) => return Ok(()),
                Err(returned) => val = returned,
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            q.producer_park.ensure_handle_installed(self.park_slot);
            q.producer_park.wake.fetch_or(bit_mask, Ordering::SeqCst);
            // Pairs with the SeqCst fence in WakeSet::wake_one. The
            // re-check below reads `head`/`sequence` with Acquire from
            // inside push(); a SeqCst RMW above does not place those
            // loads in the total order, so without this fence the
            // consumer can load the wake bitmap as 0 while we read a
            // stale full ring, and both sides sleep.
            std::sync::atomic::fence(Ordering::SeqCst);

            // SeqCst: post-arm half of the close handshake.
            if q.consumer_closed.0.load(Ordering::SeqCst) {
                q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return Err(val);
            }
            match self.push(val) {
                Ok(()) => {
                    q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                    return Ok(());
                }
                Err(returned) => val = returned,
            }

            std::thread::park();
            q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
            backoff = 0;
        }
    }

    /// Pushes a value asynchronously, yielding to the executor when the
    /// ring is full until the consumer makes space. Returns `Err(val)`
    /// if the consumer has been dropped.
    ///
    /// The future is cancel-safe: dropping it before completion leaves
    /// the ring unchanged (the value is moved back out on cancellation).
    ///
    /// Each producer clone has its own park slot so multiple async
    /// producers can wait concurrently without contending on a single
    /// waker entry; the consumer's `wake_n(count)` after a drain releases
    /// up to `count` of them per batch.
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub fn push_async(&self, val: T) -> impl std::future::Future<Output = Result<(), T>> + '_ {
        let mut val = Some(val);
        let slot = self.park_slot;
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            if self.ring().consumer_closed.0.load(Ordering::Acquire) {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    self.ring().producer_waker.register(slot, cx);
                    if self.ring().consumer_closed.0.load(Ordering::Acquire) {
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
    /// tombstoned and the consumer silently skips it.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        let queue = self.ring();
        self.claim_slot()
            .map(|(data_ptr, slot_seq, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_seq,
                pos,
                queue,
            })
    }

    /// Reserves a slot for zero-copy writing, blocking the calling
    /// thread when the ring is full until the consumer makes space.
    /// Returns `None` only when the [`Consumer`](super::Consumer)
    /// has been dropped (no consumer left to drain).
    ///
    /// Same wait protocol as [`push_block`](Self::push_block). Useful
    /// when you want zero-copy writes plus blocking.
    pub fn reserve_block(&mut self) -> Option<SlotWriter<'_, T>> {
        let queue = self.ring();
        self.claim_slot_block()
            .map(|(data_ptr, slot_seq, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_seq,
                pos,
                queue,
            })
    }

    fn claim_slot_block(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        loop {
            if self.ring().consumer_closed.0.load(Ordering::Acquire) {
                return None;
            }
            if let Some(claim) = self.claim_slot() {
                return Some(claim);
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            self.ring()
                .producer_park
                .ensure_handle_installed(self.park_slot);
            self.ring()
                .producer_park
                .wake
                .fetch_or(bit_mask, Ordering::SeqCst);
            std::sync::atomic::fence(Ordering::SeqCst);

            if self.ring().consumer_closed.0.load(Ordering::SeqCst) {
                self.ring()
                    .producer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                return None;
            }
            if let Some(claim) = self.claim_slot() {
                self.ring()
                    .producer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                return Some(claim);
            }

            std::thread::park();
            self.ring()
                .producer_park
                .wake
                .fetch_and(!bit_mask, Ordering::Relaxed);
            backoff = 0;
        }
    }

    /// Returns the number of items currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ring().len()
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ring().is_empty()
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.ring().is_full()
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Producer<T, R> {
    fn drop(&mut self) {
        if self.ring().producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // SeqCst: pairs with the consumer's post-arm SeqCst load
            // and with wake_consumer's SeqCst load of consumer_parked.
            self.ring().closed.0.store(true, Ordering::SeqCst);
            self.ring().wake_consumer();
            #[cfg(feature = "async")]
            self.ring().consumer_waker.flush();
        }
    }
}

/// A write-reservation into an MPSC ring buffer slot.
///
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → slot is tombstoned (consumer skips it).
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    pos: usize,
    queue: &'a RingBuffer<T>,
}

// SAFETY: SlotWriter holds exclusive access to the claimed slot.
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Requires [`commit_unchecked`](Self::commit_unchecked) (unsafe) to publish.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: The claim gives exclusive access. The pointer is valid
        // because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: The claim gives exclusive access and a valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_seq: this.slot_seq,
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
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        self.slot_seq.store(TOMBSTONE, Ordering::Release);
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and slot is tombstoned.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    pos: usize,
    queue: &'a RingBuffer<T>,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to the consumer.
    #[inline]
    pub fn commit(mut self) {
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
        self.committed = true;
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot data.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            self.slot_seq.store(TOMBSTONE, Ordering::Release);
            self.queue.wake_consumer();
            #[cfg(feature = "async")]
            self.queue.wake_consumer_async();
        }
    }
}
