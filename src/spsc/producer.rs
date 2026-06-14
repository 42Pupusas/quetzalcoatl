use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;
#[cfg(feature = "async")]
use std::task::Poll;

/// The producer side of an SPSC ring buffer.
///
/// Generic over `R`: the ring reference. Defaults to `Arc<RingBuffer<T>>`
/// (owned split via [`RingBuffer::split`]). When `R = &'a RingBuffer<T>`
/// (borrowed split via [`RingBuffer::split_borrowed`]), `T` may carry
/// lifetimes shorter than `'static` — the borrow checker proves the ring
/// outlives both handles.
///
/// Not cloneable — only one producer exists per buffer.
///
/// `Producer` is [`Send`] but not [`Sync`] (due to internal [`Cell`]s).
pub struct Producer<T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    pub(super) queue: R,
    write_pos: Cell<usize>,
    cached_head: Cell<usize>,
}

// No Clone impl — SPSC enforces a single producer.

// SAFETY: Producer holds exclusive write access. The Cell fields are only
// touched by the single producer thread. R is Send, and RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Producer<T, R> {}

impl<T> Producer<T> {
    pub(super) const fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            write_pos: Cell::new(0),
            cached_head: Cell::new(0),
        }
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Producer<T, R> {
    pub(super) const fn new_with(queue: R) -> Self {
        Self {
            queue,
            write_pos: Cell::new(0),
            cached_head: Cell::new(0),
        }
    }

    #[inline]
    fn ring(&self) -> &RingBuffer<T> {
        &self.queue
    }

    /// Claims the next slot using the local write cursor.
    /// Returns the write position on success, or `None` if the buffer is full.
    #[inline]
    fn try_claim(&self) -> Option<usize> {
        let pos = self.write_pos.get();

        if pos - self.cached_head.get() >= self.ring().cap {
            let head = self.ring().head.load(Ordering::Acquire);
            self.cached_head.set(head);

            if pos - head >= self.ring().cap {
                return None;
            }
        }

        self.write_pos.set(pos + 1);
        Some(pos)
    }

    /// Non-mutating check: returns true if there's currently space
    /// for at least one push/reserve. Used by `reserve_block` as
    /// the pre-park gate so we don't construct a `SlotWriter` only
    /// to drop it (which is correct but wasteful).
    #[inline]
    fn has_space(&self) -> bool {
        let pos = self.write_pos.get();
        if pos - self.cached_head.get() < self.ring().cap {
            return true;
        }
        let head = self.ring().head.load(Ordering::Acquire);
        self.cached_head.set(head);
        pos - head < self.ring().cap
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let Some(pos) = self.try_claim() else {
            return Err(val);
        };

        // SAFETY: Single producer owns this slot — no aliasing.
        unsafe {
            self.ring().slot(pos).get().write(MaybeUninit::new(val));
        }

        // SeqCst: stronger than Release on two grounds. (1) Release alone
        // ensures the data write above is visible to the consumer through
        // the Acquire load on `tail`. (2) SeqCst additionally orders this
        // store with the consumer's SeqCst store of `consumer_parked = true`
        // in `pop_block`, closing the missed-wakeup race: either we observe
        // `parked == true` in `wake_consumer` (and unpark), or the consumer's
        // post-flag re-check observes the new `tail` (and skips parking).
        self.ring().tail.store(pos + 1, Ordering::SeqCst);

        self.ring().wake_consumer();
        #[cfg(feature = "async")]
        self.ring().wake_consumer_async();

        Ok(())
    }

    /// Pushes a value, blocking the calling thread when the ring is
    /// full until the consumer makes space. Returns `Err(val)` only
    /// when the [`Consumer`](super::Consumer) has been dropped (no
    /// consumer left to drain).
    ///
    /// Spins via the shared backoff schedule first, then registers
    /// the producer parker handle and parks. The consumer signals
    /// after every pop, gated on a single `Relaxed` load.
    pub fn push_block(&self, val: T) -> Result<(), T> {
        crate::common::SingleParkerProducer::push_block(self, val)
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns a
    /// [`SlotWriter`] that provides direct mutable access to the slot.
    /// The slot is not visible to the consumer until committed.
    ///
    /// Takes `&mut self` to guarantee at most one outstanding reservation.
    /// If dropped without writing, the reservation is silently rolled back.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        let pos = self.try_claim()?;

        let slot_data = self.ring().slot(pos).get().cast::<MaybeUninit<T>>();

        Some(SlotWriter {
            slot_data,
            tail: &self.ring().tail,
            write_pos: &self.write_pos,
            pos,
            queue: self.ring(),
        })
    }

    /// Reserves a slot for zero-copy writing, blocking the calling
    /// thread when the ring is full until the consumer makes space.
    /// Returns `None` only when the [`Consumer`](super::Consumer)
    /// has been dropped (no consumer left to drain).
    ///
    /// Same wait protocol as [`push_block`](Self::push_block). Useful
    /// when you want zero-copy writes plus blocking; otherwise prefer
    /// [`reserve`](Self::reserve) for non-blocking or
    /// [`push_block`](Self::push_block) for value-copy writes.
    pub fn reserve_block(&mut self) -> Option<SlotWriter<'_, T>> {
        let mut backoff = 0u32;
        loop {
            // Check consumer_closed *before* attempting reserve.
            // Once the consumer drops it sets `consumer_closed = true`;
            // any free space we'd find afterward is permanent and
            // must not be filled.
            if self.ring().consumer_closed.0.load(Ordering::Acquire) {
                return None;
            }
            // Non-mutating gate: avoids constructing-then-dropping a
            // SlotWriter twice per loop iteration. Drop is benign for
            // SlotWriter (it just rolls back write_pos), but it's
            // wasted work.
            if self.has_space() {
                return self.reserve();
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            let _ = self.ring().producer_parker.set(std::thread::current());
            // SeqCst pairs with the consumer's `producer_parked.load`
            // after `head.store(Release)`.
            self.ring().producer_parked.0.store(true, Ordering::SeqCst);

            if self.ring().consumer_closed.0.load(Ordering::Acquire) {
                self.ring()
                    .producer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                return None;
            }
            if self.has_space() {
                self.ring()
                    .producer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                return self.reserve();
            }

            std::thread::park();
            self.ring()
                .producer_parked
                .0
                .store(false, Ordering::Relaxed);
        }
    }

    /// Pushes a value asynchronously, yielding to the executor when the
    /// ring is full until the consumer makes space. Returns `Err(val)`
    /// if the consumer has been dropped.
    ///
    /// The future is cancel-safe: dropping it before completion leaves
    /// the ring unchanged (the value is moved back out on cancellation).
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub fn push_async(&self, val: T) -> impl std::future::Future<Output = Result<(), T>> + '_ {
        let mut val = Some(val);
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            if self.ring().consumer_closed.0.load(Ordering::Acquire) {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    // Register waker, then re-check to close the lost-wake race:
                    // if a pop happened between our failed push and this register,
                    // wake_producer_async was already called and we'd park forever.
                    self.ring().producer_waker.register(0, cx);
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

impl<T, R: Deref<Target = RingBuffer<T>>> crate::common::SingleParkerProducer<T>
    for Producer<T, R>
{
    fn try_push(&self, val: T) -> Result<(), T> {
        self.push(val)
    }
    fn consumer_gone(&self) -> bool {
        self.ring().consumer_closed.0.load(Ordering::Acquire)
    }
    fn arm_park(&self) {
        // Idempotently install our parker handle, then publish "parked".
        // SeqCst pairs with the consumer's `producer_parked.load` after
        // `head.store(Release)`: either our re-check sees freed space, or
        // the consumer sees our flag and unparks us.
        let _ = self.ring().producer_parker.set(std::thread::current());
        self.ring().producer_parked.0.store(true, Ordering::SeqCst);
    }
    fn disarm_park(&self) {
        self.ring()
            .producer_parked
            .0
            .store(false, Ordering::Relaxed);
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Producer<T, R> {
    fn drop(&mut self) {
        self.ring().producer_closed.0.store(true, Ordering::Release);
        if let Some(handle) = self.ring().consumer_parker.get() {
            handle.unpark();
        }
        #[cfg(feature = "async")]
        self.ring().consumer_waker.flush();
    }
}

/// A write-reservation into a ring buffer slot.
///
/// Obtained via [`Producer::reserve`]. Call [`write`](Self::write) to
/// initialize the slot and get a [`WrittenSlot`] that can be committed.
///
/// For fine-grained control, use [`slot_mut`](Self::slot_mut) to access
/// the raw `MaybeUninit<T>`, then [`commit_unchecked`](Self::commit_unchecked)
/// (unsafe) to publish.
///
/// If dropped, the reservation is silently rolled back.
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    tail: &'a AtomicUsize,
    write_pos: &'a Cell<usize>,
    pos: usize,
    queue: &'a RingBuffer<T>,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The raw pointer points into the RingBuffer kept alive by the Producer's
// reference (Arc or borrow).
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use [`commit_unchecked`](Self::commit_unchecked) (unsafe) after
    /// initializing through this reference. If dropped without
    /// committing, any data written through this reference is leaked.
    ///
    /// Prefer [`write`](Self::write) for the safe path.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Single producer has exclusive access. The pointer
        // is valid because the Producer's reference keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the slot, consuming this `SlotWriter` and
    /// returning a [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        // Prevent SlotWriter::drop from rolling back write_pos.
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access, valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            tail: this.tail,
            write_pos: this.write_pos,
            pos: this.pos,
            queue: this.queue,
            committed: false,
        }
    }

    /// Commits without verifying initialization.
    ///
    /// # Safety
    ///
    /// The caller must have initialized the slot data (via
    /// [`slot_mut`](Self::slot_mut) + [`MaybeUninit::write`]).
    /// Committing without initializing causes the consumer to read
    /// uninitialized memory (undefined behavior).
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        // SeqCst: see Producer::push for the full explanation. Pairs with
        // the consumer's SeqCst `parked.store(true)` to close the missed-
        // wakeup race; subsumes the Release semantics for data publication.
        self.tail.store(self.pos + 1, Ordering::SeqCst);
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        // No data was written (write() consumes self) — just roll back.
        self.write_pos.set(self.pos);
    }
}

/// A slot that has been initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to make the data visible to the consumer.
/// If dropped without committing, the value is dropped and the
/// reservation is rolled back.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    tail: &'a AtomicUsize,
    write_pos: &'a Cell<usize>,
    pos: usize,
    queue: &'a RingBuffer<T>,
    committed: bool,
}

// SAFETY: Same as SlotWriter — exclusive access to a slot in a RingBuffer
// kept alive by the Producer's reference.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to the consumer.
    #[inline]
    pub fn commit(mut self) {
        // SeqCst: see Producer::push. Pairs with the consumer's SeqCst
        // `parked.store(true)` to close the missed-wakeup race.
        self.tail.store(self.pos + 1, Ordering::SeqCst);
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        self.committed = true;
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            self.write_pos.set(self.pos);
        }
    }
}
