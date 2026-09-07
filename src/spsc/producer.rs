use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;
#[cfg(feature = "async")]
#[cfg(feature = "async")]
use crate::common::park_registration::ParkRegistration;
#[cfg(feature = "async")]
use crate::common::park_registry::ParkSlot;
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

    /// Returns the next writable position, or `None` if the buffer is
    /// full.
    ///
    /// Does **not** advance `write_pos`. The cursor moves only once a
    /// value has been written and published (`push`, `commit`,
    /// `commit_unchecked`), so an abandoned reservation — including one
    /// leaked via [`std::mem::forget`], which runs no destructor —
    /// leaves the cursor on the uninitialized slot and the next claim
    /// reuses it. Advancing here instead would let a forgotten
    /// `SlotWriter` push `tail` past a slot no one ever initialized.
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

        self.write_pos.set(pos + 1);

        // SeqCst: stronger than Release on two grounds. (1) Release alone
        // ensures the data write above is visible to the consumer through
        // the Acquire load on `tail`. (2) SeqCst additionally orders this
        // store with the consumer's SeqCst arm in `pop_block`, closing the
        // missed-wakeup race: either we observe `parked == true` in
        // `wake_consumer` (and unpark), or the consumer's post-arm re-check
        // observes the new `tail` (and skips parking). Being SeqCst is also
        // what lets `wake_consumer` skip the store-buffer fence.
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
    /// If dropped or forgotten without committing, the slot is left
    /// uninitialized and the next claim reuses it.
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
            if self.ring().consumer_closed.is_closed() {
                return None;
            }
            // Non-mutating gate: avoids constructing-then-dropping a
            // SlotWriter twice per loop iteration.
            if self.has_space() {
                return self.reserve();
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            self.ring().producer_park.arm();

            // SeqCst: the post-arm re-check is the second half of the
            // close handshake with Consumer::drop.
            if self.ring().consumer_closed.is_closed_for_parking() {
                self.ring().producer_park.disarm();
                return None;
            }
            if self.has_space() {
                self.ring().producer_park.disarm();
                return self.reserve();
            }

            std::thread::park();
            self.ring().producer_park.disarm();
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
        let mut parked = ParkRegistration::new(&self.ring().producer_waker, ParkSlot::SOLE);
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            if self.ring().consumer_closed.is_closed() {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    // Register waker, then re-check to close the lost-wake race:
                    // if a pop happened between our failed push and this register,
                    // wake_producer_async was already called and we'd park forever.
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
        // SeqCst: post-arm half of the close handshake, as the trait
        // requires. `push_block` calls this again after `arm_park`.
        self.ring().consumer_closed.is_closed_for_parking()
    }
    fn arm_park(&self) {
        self.ring().producer_park.arm();
    }
    fn disarm_park(&self) {
        self.ring().producer_park.disarm();
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Producer<T, R> {
    fn drop(&mut self) {
        // Close then wake, in that order: the SeqCst store inside
        // `close` and the SeqCst load inside `wake_consumer` are the two
        // halves of the handshake. A bare unpark here would claim the
        // park handle without ever loading the parked flag, leaving the
        // store and the consumer's re-check unordered, so the consumer
        // could park after we decided not to wake it.
        self.ring().producer_closed.close();
        self.ring().wake_consumer();
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
/// If dropped or forgotten without committing, the slot is left
/// uninitialized for the next claim to reuse.
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
        self.write_pos.set(self.pos + 1);
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
        // Nothing to undo: `try_claim` never advanced `write_pos`, and
        // no value was written (`write` consumes `self`). The slot stays
        // uninitialized and the next claim reuses this position.
    }
}

/// A slot that has been initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to make the data visible to the consumer.
/// If dropped without committing, the value is dropped and the slot is
/// left free for the next claim.
///
/// Forgetting this guard (e.g. [`std::mem::forget`]) leaks the value in
/// place without publishing it; the slot is later overwritten by the
/// next claim. That leaks, which is safe, and never exposes
/// uninitialized or aliased data to the consumer.
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
    /// Once `tail` is published the consumer owns the value, so the
    /// guard is disarmed first: a panic in the wake path must not let
    /// `Drop` drop a value the consumer can already see.
    #[inline]
    pub fn commit(mut self) {
        self.committed = true;
        self.write_pos.set(self.pos + 1);
        // SeqCst: see Producer::push. Pairs with the consumer's SeqCst
        // `parked.store(true)` to close the missed-wakeup race.
        self.tail.store(self.pos + 1, Ordering::SeqCst);
        self.queue.wake_consumer();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot, and it was never
            // published, so the consumer cannot have observed it.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            // `write_pos` was never advanced — nothing to roll back.
        }
    }
}
