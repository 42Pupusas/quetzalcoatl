use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::reservation_release::ReservationRelease;
use super::RingBuffer;
use crate::common::backoff::Backoff;
#[cfg(feature = "async")]
use crate::common::park_registration::ParkRegistration;
use crate::common::park_registry::ParkSlot;
use crate::common::{SlotSequence, UncommittedSlot};

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
    /// Park slot leased for this producer's lifetime and returned on
    /// drop. Used by [`Producer::push_block`] to register on the wake
    /// bitmap.
    pub(super) park_slot: ParkSlot,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.queue.producer_count.register();
        let park_slot = self.queue.producer_slots.lease();
        Self {
            queue: Arc::clone(&self.queue),
            cached_head: std::cell::Cell::new(0),
            park_slot,
        }
    }
}

// SAFETY: Producer holds write access. The Cell fields are only
// touched by a single thread at a time (no Sync). R is Send, and
// RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Producer<T, R> {}

impl<'a, T> Producer<T, &'a RingBuffer<T>> {
    /// Creates an additional producer handle for the same ring.
    ///
    /// The borrowed counterpart to `Clone`, which is only available when
    /// `R = Arc`. The new handle gets its own park slot and cursor
    /// cache.
    ///
    /// This hangs off the producer rather than the ring because
    /// [`RingBuffer::split_borrowed`](super::RingBuffer::split_borrowed)
    /// takes `&mut self`: while these handles are alive the ring is
    /// mutably borrowed and cannot be borrowed again. An existing handle
    /// already holds the shared reference, so it can hand out siblings
    /// with the same lifetime.
    #[must_use]
    pub fn new_producer(&self) -> Self {
        let queue: &'a RingBuffer<T> = self.queue;
        queue.producer_count.register();
        let park_slot = queue.producer_slots.lease();
        Self::new_with(queue, park_slot)
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Producer<T, R> {
    pub(super) const fn new_with(queue: R, park_slot: ParkSlot) -> Self {
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

    /// How many wakers the producer-side overflow currently holds.
    #[cfg(all(test, feature = "async"))]
    pub(crate) fn overflow_depth(&self) -> usize {
        self.ring().producer_waker.overflow.depth()
    }

    #[inline]
    fn claim_slot(&self) -> Option<(*mut MaybeUninit<T>, &SlotSequence, usize)> {
        let mut current_tail = self.ring().cursors.tail().load(Ordering::Relaxed);
        let mut backoff = Backoff::new();
        loop {
            if current_tail.wrapping_sub(self.cached_head.get()) >= self.ring().capacity.get() {
                let head = self.ring().cursors.head().load(Ordering::Acquire);
                self.cached_head.set(head);

                if current_tail.wrapping_sub(head) >= self.ring().capacity.get() {
                    return None;
                }
            }

            match self.ring().cursors.tail().compare_exchange_weak(
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
                    backoff.spin();
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
                slot_seq.publish(pos);
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
        let slot = self.park_slot;
        let mut backoff = Backoff::new();
        loop {
            if q.consumer_closed.is_closed() {
                return Err(val);
            }
            match self.push(val) {
                Ok(()) => return Ok(()),
                Err(returned) => val = returned,
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }

            q.producer_park.arm(slot);
            // Pairs with the SeqCst fence in WakeSet::wake_one. The
            // re-check below reads `head`/`sequence` with Acquire from
            // inside push(); a SeqCst RMW above does not place those
            // loads in the total order, so without this fence the
            // consumer can load the wake bitmap as 0 while we read a
            // stale full ring, and both sides sleep.
            std::sync::atomic::fence(Ordering::SeqCst);

            // SeqCst: post-arm half of the close handshake.
            if q.consumer_closed.is_closed_for_parking() {
                q.producer_park.disarm(slot);
                return Err(val);
            }
            match self.push(val) {
                Ok(()) => {
                    q.producer_park.disarm(slot);
                    return Ok(());
                }
                Err(returned) => val = returned,
            }

            q.producer_park.park();
            q.producer_park.disarm(slot);
            backoff.reset();
        }
    }

    /// Pushes a value asynchronously, yielding to the executor when the
    /// ring is full until the consumer makes space. Returns `Err(val)`
    /// if the consumer has been dropped.
    ///
    /// The future is cancel-safe: dropping it before completion leaves
    /// the ring unchanged (the value is moved back out on
    /// cancellation) and withdraws its park registration, so a
    /// cancel/retry loop does not accumulate dead wakers.
    ///
    /// Each producer clone has its own park slot so multiple async
    /// producers can wait concurrently without contending on a single
    /// waker entry; the consumer's `wake_n(count)` after a drain releases
    /// up to `count` of them per batch.
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub fn push_async(&self, val: T) -> impl std::future::Future<Output = Result<(), T>> + '_ {
        let mut val = Some(val);
        let mut parked = ParkRegistration::new(&self.ring().producer_waker, self.park_slot);
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            if self.ring().consumer_closed.is_closed() {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    parked.arm(cx);
                    if self.ring().consumer_closed.is_closed() {
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

    fn claim_slot_block(&self) -> Option<(*mut MaybeUninit<T>, &SlotSequence, usize)> {
        let slot = self.park_slot;
        let mut backoff = Backoff::new();
        loop {
            if self.ring().consumer_closed.is_closed() {
                return None;
            }
            if let Some(claim) = self.claim_slot() {
                return Some(claim);
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }

            self.ring().producer_park.arm(slot);
            std::sync::atomic::fence(Ordering::SeqCst);

            if self.ring().consumer_closed.is_closed_for_parking() {
                self.ring().producer_park.disarm(slot);
                return None;
            }
            if let Some(claim) = self.claim_slot() {
                self.ring().producer_park.disarm(slot);
                return Some(claim);
            }

            self.ring().producer_park.park();
            self.ring().producer_park.disarm(slot);
            backoff.reset();
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
        self.ring().producer_slots.release(self.park_slot);
        if self.ring().producer_count.release() {
            // SeqCst: pairs with the consumer's post-arm SeqCst load
            // and with wake_consumer's SeqCst load of the parked flag.
            self.ring().closed.close();
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
    slot_seq: &'a SlotSequence,
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
            // SAFETY: just initialized above; the claim is exclusive and
            // the Producer's Arc keeps the buffer alive.
            value: unsafe { UncommittedSlot::armed(this.slot_data) },
            slot_seq: this.slot_seq,
            pos: this.pos,
            queue: this.queue,
        }
    }

    /// Commits without verifying initialization.
    ///
    /// # Safety
    ///
    /// The caller must have initialized the slot data via
    /// [`slot_mut`](Self::slot_mut).
    ///
    /// The tombstoning `Drop` is disarmed *before* the sequence is
    /// published, not after the wake: a panicking waker between the two
    /// would otherwise tombstone a position the consumer can already
    /// see, losing the value it published.
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        let this = std::mem::ManuallyDrop::new(self);
        this.slot_seq.publish(this.pos);
        this.queue.wake_consumer();
        #[cfg(feature = "async")]
        this.queue.wake_consumer_async();
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        self.slot_seq.tombstone();
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
    value: UncommittedSlot<T>,
    slot_seq: &'a SlotSequence,
    pos: usize,
    queue: &'a RingBuffer<T>,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to the consumer.
    ///
    /// Once the sequence is published the consumer owns the value, so
    /// the guard is disarmed first: a panic in the wake path (a custom
    /// waker may panic) must not let `Drop` drop a value the consumer
    /// can already see.
    #[inline]
    pub fn commit(mut self) {
        self.value.commit();
        self.slot_seq.publish(self.pos);
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.value.is_armed() {
            return;
        }
        // The tombstone is armed before the value is dropped so that a
        // panicking `T::drop` still hands this position back. Unwinding
        // past the release would leave the position claimed forever: the
        // consumer stops there waiting for a publication that never
        // comes.
        let _release = ReservationRelease::new(self.slot_seq, self.queue);
        self.value.drop_if_uncommitted();
    }
}
