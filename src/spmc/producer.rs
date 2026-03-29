use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The producer side of an SPMC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one producer exists per buffer.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    /// Local write cursor. Because there is only one producer, no atomic
    /// operations are needed — we simply increment after each claim.
    pub(super) write_pos: std::cell::Cell<usize>,
}

impl<T> Producer<T> {
    /// Checks whether the next slot is available for writing.
    ///
    /// Uses the slot's sequence number as the sole check:
    /// `seq == pos` means the slot is free for the producer at this position.
    fn try_claim(&self) -> Option<(*mut MaybeUninit<T>, *const AtomicUsize, usize)> {
        let pos = self.write_pos.get();

        // SAFETY: `pos & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(pos & self.queue.mask) };

        // The sequence number is the authoritative "slot is free" signal.
        // seq == pos * 2 means the consumer has finished reading and released
        // this slot (or it was never written to yet, for the initial fill).
        let seq = slot.sequence.load(Ordering::Acquire);
        if seq != pos * 2 {
            return None;
        }

        // Advance write_pos — this is our local claim.
        self.write_pos.set(pos + 1);

        Some((slot.data.get(), &raw const slot.sequence, pos))
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full. Only one producer may
    /// exist, so no CAS is needed.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.try_claim() {
            Some((data_ptr, seq_ptr, pos)) => {
                // SAFETY: We are the sole producer and the slot is free
                // (seq == pos verified in try_claim).
                unsafe { (*data_ptr).write(val) };

                // Publish the data: set sequence to pos * 2 + 1 so consumers
                // know data is valid.
                // SAFETY: seq_ptr points into the RingBuffer kept alive
                // by Arc.
                unsafe {
                    (*seq_ptr).store(pos * 2 + 1, Ordering::Release);
                }

                // Update tail for len()/is_empty()/is_full() queries.
                self.queue
                    .tail
                    .store(self.write_pos.get(), Ordering::Release);

                Ok(())
            }
            None => Err(val),
        }
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns a
    /// [`SlotWriter`] that provides direct mutable access to the slot.
    ///
    /// # Contract
    ///
    /// You **must** call [`SlotWriter::commit`] after writing data.
    /// Dropping a `SlotWriter` without committing aborts the process.
    #[inline]
    #[must_use]
    pub fn reserve(&self) -> Option<SlotWriter<'_, T>> {
        self.try_claim().map(|(data_ptr, seq_ptr, pos)| SlotWriter {
            slot_data: data_ptr,
            // SAFETY: seq_ptr points into the RingBuffer kept alive by our Arc.
            slot_seq: unsafe { &*seq_ptr },
            tail: &self.queue.tail,
            write_pos: self.write_pos.get(),
            pos,
            committed: false,
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

/// A write-reservation into a ring buffer slot.
///
/// Obtained via [`Producer::reserve`]. Provides direct mutable access
/// to the slot's memory, enabling zero-copy writes for large types.
///
/// # Contract
///
/// You **must** call [`commit`](SlotWriter::commit) after writing data.
/// Dropping a `SlotWriter` without committing will **abort the process**
/// because the slot cannot be reclaimed (the write position has already
/// advanced).
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_seq: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: usize,
    pos: usize,
    committed: bool,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The references point into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<T> SlotWriter<'_, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use this for fine-grained control over initialization.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: We have exclusive access as the sole producer. The pointer
        // is valid because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the reserved slot and returns a mutable
    /// reference to the now-initialized data.
    pub fn write(&mut self, val: T) -> &mut T {
        // SAFETY: Same as slot_mut — exclusive access, valid pointer.
        unsafe { (*self.slot_data).write(val) }
    }

    /// Commits the write, making the slot visible to consumers.
    ///
    /// Sets the slot's sequence to `pos + 1` with `Release` ordering
    /// and consumes the `SlotWriter`.
    ///
    /// # Safety contract
    ///
    /// The caller must have initialized the slot data (via [`write`] or
    /// [`slot_mut`](Self::slot_mut) + `MaybeUninit::write`) before
    /// calling `commit`. Committing without initializing causes a
    /// consumer to read uninitialized memory (undefined behavior).
    #[inline]
    pub fn commit(mut self) {
        self.slot_seq.store(self.pos * 2 + 1, Ordering::Release);
        self.tail.store(self.write_pos, Ordering::Release);
        self.committed = true;
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        assert!(
            self.committed,
            "SlotWriter<{}> dropped without commit — \
             the ring buffer slot is permanently stuck.",
            std::any::type_name::<T>()
        );
    }
}
