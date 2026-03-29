use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

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
    /// push or reserve. The atomic tail only advances when data is published
    /// (immediately in `push`, on `commit` for `reserve`).
    pub(super) write_pos: std::cell::Cell<usize>,
    /// Cached snapshot of `head` to avoid cross-cache-line reads on every push.
    /// Since `head` only ever increases, a stale value is safe — it just makes
    /// the buffer appear fuller than it is. We re-fetch only when needed.
    pub(super) cached_head: std::cell::Cell<usize>,
}

// No Clone impl — SPSC enforces a single producer.

impl<T> Producer<T> {
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

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let Some(pos) = self.try_claim() else {
            return Err(val);
        };

        // SAFETY: Single producer owns this slot. `pos & mask` < cap.
        unsafe {
            (*self.queue.buf.get_unchecked(pos & self.queue.mask)).get()
                .write(MaybeUninit::new(val));
        }

        // Release: ensures the data write above is visible before tail advances.
        self.queue.tail.store(pos + 1, Ordering::Release);

        Ok(())
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns a
    /// [`SlotWriter`] that provides direct mutable access to the slot.
    /// The slot is not visible to the consumer until [`SlotWriter::commit`]
    /// is called.
    ///
    /// Multiple slots may be reserved before committing. Commits **must**
    /// happen in the same order as the corresponding reserves (FIFO),
    /// otherwise the consumer will observe uninitialized memory.
    ///
    /// # Contract
    ///
    /// You **must** call [`SlotWriter::commit`] after writing data.
    /// Dropping a `SlotWriter` without committing aborts the process.
    #[must_use]
    pub fn reserve(&self) -> Option<SlotWriter<'_, T>> {
        let pos = self.try_claim()?;

        // SAFETY: Single producer owns this slot. `pos & mask` < cap.
        let slot_data = unsafe {
            (*self.queue.buf.get_unchecked(pos & self.queue.mask)).get()
                .cast::<MaybeUninit<T>>()
        };

        Some(SlotWriter {
            slot_data,
            tail: &self.queue.tail,
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
/// because the consumer would never see the data at this index.
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    tail: &'a AtomicUsize,
    pos: usize,
    committed: bool,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<T> SlotWriter<'_, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use this for fine-grained control over initialization.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Single producer has exclusive access. The pointer
        // is valid because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the reserved slot and returns a mutable
    /// reference to the now-initialized data.
    pub fn write(&mut self, val: T) -> &mut T {
        // SAFETY: Same as slot_mut — exclusive access, valid pointer.
        unsafe { (*self.slot_data).write(val) }
    }

    /// Commits the write, making the slot visible to the consumer.
    ///
    /// Advances the tail pointer with `Release` ordering so the
    /// consumer's `Acquire` load sees the written data.
    ///
    /// # Safety contract
    ///
    /// The caller must have initialized the slot data (via [`write`] or
    /// [`slot_mut`](Self::slot_mut) + `MaybeUninit::write`) before calling `commit`.
    /// Committing without initializing causes the consumer to read
    /// uninitialized memory (undefined behavior).
    pub fn commit(mut self) {
        self.tail.store(self.pos + 1, Ordering::Release);
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
