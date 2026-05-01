use std::cell::Cell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;

/// The producer side of an SPSC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one producer exists per buffer.
///
/// `Producer` is [`Send`] but not [`Sync`] (due to internal [`Cell`]s).
/// Wrapping it in a `Mutex` is valid but pointless — the buffer is SPSC
/// by design, so there is no benefit to sharing the producer across threads.
/// If you need multi-producer semantics, use [`mpsc::RingBuffer`](crate::mpsc::RingBuffer).
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local write cursor — always >= the atomic tail. Incremented on every
    /// push or reserve.
    write_pos: Cell<usize>,
    /// Cached snapshot of `head` to avoid cross-cache-line reads on every push.
    /// Since `head` only ever increases, a stale value is safe — it just makes
    /// the buffer appear fuller than it is. We re-fetch only when needed.
    cached_head: Cell<usize>,
}

// No Clone impl — SPSC enforces a single producer.

impl<T> Producer<T> {
    pub(super) const fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            write_pos: Cell::new(0),
            cached_head: Cell::new(0),
        }
    }

    /// Claims the next slot using the local write cursor.
    /// Returns the write position on success, or `None` if the buffer is full.
    #[inline]
    fn try_claim(&self) -> Option<usize> {
        let pos = self.write_pos.get();

        if pos - self.cached_head.get() >= self.queue.cap {
            let head = self.queue.head.load(Ordering::Acquire);
            self.cached_head.set(head);

            if pos - head >= self.queue.cap {
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
        if pos - self.cached_head.get() < self.queue.cap {
            return true;
        }
        let head = self.queue.head.load(Ordering::Acquire);
        self.cached_head.set(head);
        pos - head < self.queue.cap
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
            self.queue.slot(pos).get().write(MaybeUninit::new(val));
        }

        // Release: ensures the data write above is visible before tail advances.
        self.queue.tail.store(pos + 1, Ordering::Release);

        // Wake the consumer if it's parked in pop_block. Relaxed gate
        // keeps the no-park hot path free.
        self.queue.wake_consumer();

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
    pub fn push_block(&self, mut val: T) -> Result<(), T> {
        let q = &*self.queue;
        let mut backoff = 0u32;
        loop {
            // Check consumer_closed *before* attempting push. Once
            // the consumer drops it sets `consumer_closed = true`;
            // any free space we'd find afterward is permanent and
            // must not be filled.
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

            // Idempotently install our parker handle.
            let _ = q.producer_parker.set(std::thread::current());
            // SeqCst pairs with the consumer's `producer_parked.load`
            // after `head.store(Release)`: either we succeed in the
            // re-check below, or the consumer sees our flag and unparks
            // us.
            q.producer_parked.0.store(true, Ordering::SeqCst);

            if q.consumer_closed.0.load(Ordering::Acquire) {
                q.producer_parked.0.store(false, Ordering::Relaxed);
                return Err(val);
            }
            match self.push(val) {
                Ok(()) => {
                    q.producer_parked.0.store(false, Ordering::Relaxed);
                    return Ok(());
                }
                Err(returned) => val = returned,
            }

            std::thread::park();
            q.producer_parked.0.store(false, Ordering::Relaxed);
        }
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

        let slot_data = self.queue.slot(pos).get().cast::<MaybeUninit<T>>();

        Some(SlotWriter {
            slot_data,
            tail: &self.queue.tail,
            write_pos: &self.write_pos,
            pos,
            queue: &self.queue,
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
            if self.queue.consumer_closed.0.load(Ordering::Acquire) {
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

            let _ = self.queue.producer_parker.set(std::thread::current());
            // SeqCst pairs with the consumer's `producer_parked.load`
            // after `head.store(Release)`.
            self.queue.producer_parked.0.store(true, Ordering::SeqCst);

            if self.queue.consumer_closed.0.load(Ordering::Acquire) {
                self.queue.producer_parked.0.store(false, Ordering::Relaxed);
                return None;
            }
            if self.has_space() {
                self.queue.producer_parked.0.store(false, Ordering::Relaxed);
                return self.reserve();
            }

            std::thread::park();
            self.queue.producer_parked.0.store(false, Ordering::Relaxed);
        }
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

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Mark the queue closed so a parked consumer in pop_block can
        // observe it and return None. Release synchronizes with the
        // consumer's Acquire-load of producer_closed.
        self.queue.producer_closed.0.store(true, Ordering::Release);
        // Wake the consumer if it's parked, so it observes the close
        // instead of waiting forever.
        if let Some(handle) = self.queue.consumer_parker.get() {
            handle.unpark();
        }
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
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
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
        // is valid because the Producer's Arc keeps the RingBuffer alive.
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
        self.tail.store(self.pos + 1, Ordering::Release);
        self.queue.wake_consumer();
        // Skip the SlotWriter drop (which would roll back write_pos).
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
// kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to the consumer.
    #[inline]
    pub fn commit(mut self) {
        self.tail.store(self.pos + 1, Ordering::Release);
        self.queue.wake_consumer();
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
