use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;

/// The producer side of an SPMC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one producer exists per buffer.
///
/// On drop, sets the ring buffer's `closed` flag so any consumer
/// spinning on overshoot can return `None` instead of hanging.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local write cursor. Single producer means no atomics needed for
    /// our own state — we publish via `ready[s]` per slot, update the
    /// shared `tail` (Release) for consumer pre-FAA peeks, and read
    /// `done[s]` to confirm the slot is free for reuse.
    pub(super) write_pos: std::cell::Cell<usize>,
}

impl<T> Producer<T> {
    /// Tries to claim slot at logical position `tail`.
    ///
    /// The slot is free iff `done[s] == tail` — i.e. the previous
    /// consumer at logical position `tail - cap` released it for us
    /// (or, for the first lap, this is the slot's initial state, since
    /// `done[s]` is initialized to `s`).
    fn try_claim(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        let pos = self.write_pos.get();

        let done = self.queue.done_slot(pos);
        if done.0.load(Ordering::Acquire) != pos {
            return None;
        }

        let ready = self.queue.ready_slot(pos);
        let data_ptr = self.queue.data_slot(pos).get();

        self.write_pos.set(pos + 1);
        Some((data_ptr, &ready.0, pos))
    }

    /// Non-mutating check: returns true if `try_claim` would
    /// currently succeed. Used by `reserve_block` as the pre-park
    /// gate so we don't construct then drop a `SlotWriter` per
    /// iteration.
    #[inline]
    fn has_space(&self) -> bool {
        let pos = self.write_pos.get();
        self.queue.done_slot(pos).0.load(Ordering::Acquire) == pos
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.try_claim() {
            Some((data_ptr, slot_ready, pos)) => {
                // SAFETY: We are the sole producer and the slot is free.
                unsafe { (*data_ptr).write(val) };
                // Publish: ready[s] = pos + 1 tells the consumer at
                // logical position `pos` that data is available.
                slot_ready.store(pos + 1, Ordering::Release);
                // Consumers Acquire-load `tail` in their pre-FAA peek;
                // Release ensures they see published slot data.
                self.queue
                    .tail
                    .store(self.write_pos.get(), Ordering::Release);
                // Wake one consumer parked in pop_block, if any.
                // Self-gated on a Relaxed load of the bitmap.
                self.queue.consumer_park.wake_one();
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Pushes a value, blocking the calling thread when the ring is
    /// full until a consumer makes space. Returns `Err(val)` only
    /// when the last [`Consumer`](super::Consumer) has been dropped
    /// (no consumer left to drain).
    ///
    /// Spins via the shared backoff schedule first, then registers
    /// the producer parker handle and parks. Consumers signal after
    /// every pop, gated on a single `Relaxed` load.
    pub fn push_block(&self, mut val: T) -> Result<(), T> {
        let q = &*self.queue;
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

            let _ = q.producer_parker.set(std::thread::current());
            // SeqCst pairs with consumer's `producer_parked.load`
            // after `done.store(Release)`.
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
    /// Returns `None` if the buffer is full. Takes `&mut self` to
    /// guarantee at most one outstanding reservation. If dropped
    /// without writing, the reservation is silently rolled back.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        self.try_claim()
            .map(|(data_ptr, slot_ready, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_ready,
                tail: &self.queue.tail,
                write_pos: &self.write_pos,
                pos,
                queue: &self.queue,
            })
    }

    /// Reserves a slot for zero-copy writing, blocking the calling
    /// thread when the ring is full until a consumer makes space.
    /// Returns `None` only when the last
    /// [`Consumer`](super::Consumer) has been dropped (no consumer
    /// left to drain).
    ///
    /// Same wait protocol as [`push_block`](Self::push_block). Useful
    /// when you want zero-copy writes plus blocking.
    pub fn reserve_block(&mut self) -> Option<SlotWriter<'_, T>> {
        let mut backoff = 0u32;
        loop {
            if self.queue.consumer_closed.0.load(Ordering::Acquire) {
                return None;
            }
            // Non-mutating gate.
            if self.has_space() {
                return self.reserve();
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            let _ = self.queue.producer_parker.set(std::thread::current());
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
        // Mark the queue closed so consumers spinning on overshoot
        // can return None. Release synchronizes with consumer's
        // Acquire-load of the closed flag.
        self.queue.closed.0.store(true, Ordering::Release);
        // Wake every parked consumer so they can observe `closed`
        // and exit pop_block.
        self.queue.consumer_park.flush();
    }
}

/// A write-reservation into an SPMC ring buffer slot.
///
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → silently rolled back.
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_ready: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
    queue: &'a RingBuffer<T>,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Requires [`commit_unchecked`](Self::commit_unchecked) (unsafe) to publish.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Single producer has exclusive access. The pointer is valid
        // because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access (single producer), valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_ready: this.slot_ready,
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
    /// The caller must have initialized the slot data via
    /// [`slot_mut`](Self::slot_mut).
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        self.slot_ready.store(self.pos + 1, Ordering::Release);
        self.tail.store(self.write_pos.get(), Ordering::Release);
        self.queue.consumer_park.wake_one();
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        // ready was never written; done was unchanged. Just roll back
        // the local cursor.
        self.write_pos.set(self.pos);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and reservation rolled back.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_ready: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
    queue: &'a RingBuffer<T>,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to consumers.
    #[inline]
    pub fn commit(mut self) {
        self.slot_ready.store(self.pos + 1, Ordering::Release);
        self.tail.store(self.write_pos.get(), Ordering::Release);
        self.queue.consumer_park.wake_one();
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
            // Slot freeness is gated by `done`, not `ready`. Since we
            // never published `ready`, no consumer can claim this slot
            // — but `done[s]` still says `pos`, which is what producer's
            // try_claim checks. Roll back the local cursor to retry.
            self.write_pos.set(self.pos);
        }
    }
}
