use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;

/// The consumer side of an SPMC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for items
/// via atomic CAS on the head pointer.
///
/// Drains remaining items when dropped.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<T> Consumer<T> {
    /// Exponential backoff for CAS contention. Marked `#[inline(never)]` to
    /// keep the hot pop loop's instruction footprint small — this code only
    /// matters under real multi-consumer contention.
    #[inline(never)]
    fn cas_backoff(failures: &mut u32) {
        // Under Miri, spin_loop() is an interleaving point. Exponential
        // spin counts explode the state space, so we just yield instead.
        #[cfg(miri)]
        {
            let _ = failures;
            std::thread::yield_now();
        }
        #[cfg(not(miri))]
        {
            let f = *failures;
            if f > 1 {
                for _ in 0..1u32 << f {
                    std::hint::spin_loop();
                }
            }
            *failures = f.saturating_add(1).min(6);
        }
    }

    /// Atomically claims the next available slot via CAS loop.
    ///
    /// Uses the per-slot sequence number as the sole synchronization point,
    /// avoiding a load of the producer-contended `tail` cache line entirely.
    /// `seq == head * 2 + 1` means the producer has written data at this
    /// position. The Acquire on the sequence provides happens-before with
    /// the producer's data write.
    ///
    /// Returns raw pointers to the slot's data and sequence, plus the
    /// claimed head position, or `None` if the buffer is empty.
    fn claim_slot(
        &self,
    ) -> Option<(*const MaybeUninit<T>, *const AtomicUsize, usize)> {
        // Cache immutable RingBuffer fields in locals. Without this, the
        // compiler reloads buf/mask from the Arc on every loop iteration
        // because `lock cmpxchg` acts as a compiler fence and the compiler
        // conservatively assumes memory behind the Arc may have changed.
        let q = &*self.queue;
        let buf = &q.buf;
        let mask = q.mask;

        let mut backoff = 0u32;
        loop {
            let head = q.head.load(Ordering::Relaxed);

            // SAFETY: `head & mask` is always < cap by construction
            let slot = unsafe { buf.get_unchecked(head & mask) };

            // The sequence number is the sole synchronization point.
            // seq == head * 2 + 1 means the producer has finished writing
            // data to this slot. The Acquire ordering synchronizes with the
            // producer's Release store on the sequence, ensuring the data
            // write is visible. Any other value means either empty or
            // not-yet-committed — both cases require returning None.
            let seq = slot.sequence.load(Ordering::Acquire);
            if seq != head * 2 + 1 {
                return None;
            }

            // Ready! Try to claim this slot via CAS.
            match q.head.compare_exchange_weak(
                head,
                head + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some((
                        slot.data.get().cast_const(),
                        &raw const slot.sequence,
                        head,
                    ));
                }
                Err(_) => {
                    // Another consumer beat us — retry.
                    Self::cas_backoff(&mut backoff);
                }
            }
        }
    }

    /// Pops an item from the ring buffer.
    ///
    /// Multiple consumers can pop concurrently. Uses a CAS loop to
    /// atomically claim slots. Each item is consumed by exactly one
    /// consumer.
    ///
    /// Returns `None` if the buffer is empty.
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let (data_ptr, seq_ptr, head) = self.claim_slot()?;

        // SAFETY: We atomically claimed this slot via CAS. The producer
        // set seq == head + 1 after writing data.
        let val = unsafe { data_ptr.cast::<T>().read() };

        // Release the slot: set sequence to (head + cap) * 2 so the producer
        // knows this slot is free for reuse at position head + cap.
        // SAFETY: seq_ptr points into the RingBuffer kept alive by Arc.
        unsafe {
            (*seq_ptr).store((head + self.queue.cap) * 2, Ordering::Release);
        }

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    ///
    /// Unlike [`pop`](Self::pop), this does not copy the data out. Instead,
    /// it returns a [`SlotReader`] that dereferences to `&T`. The slot is
    /// released when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the buffer is empty.
    ///
    /// Requires `&mut self` to guarantee only one [`SlotReader`] per
    /// consumer clone at a time.
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let (data_ptr, seq_ptr, head) = self.claim_slot()?;

        Some(SlotReader {
            data_ptr,
            seq_ptr,
            head,
            cap: self.queue.cap,
            _consumer: self,
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

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without copying.
///
/// When dropped, drops the `T` value and releases the slot by updating
/// the sequence number so the producer can reuse the slot.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    seq_ptr: *const AtomicUsize,
    head: usize,
    cap: usize,
    _consumer: &'a mut Consumer<T>,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was claimed via CAS after verifying seq == head + 1.
        // The data is initialized and we have exclusive access via CAS
        // claim + &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (seq == head + 1 was verified before CAS).
        // Exclusive access guaranteed by CAS + &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // Release the slot so the producer can reuse it.
        // SAFETY: seq_ptr points into the RingBuffer kept alive by
        // consumer's Arc.
        unsafe {
            (*self.seq_ptr).store((self.head + self.cap) * 2, Ordering::Release);
        }
    }
}
