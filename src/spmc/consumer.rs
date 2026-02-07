use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// The ready flag is the sole synchronization point. It subsumes the
    /// tail check: `ready=true` means the producer has both claimed AND
    /// written the slot. `ready=false` means either empty or reserved-
    /// but-not-committed — both cases require returning `None`.
    ///
    /// Returns raw pointers to the slot's data and ready flag, or `None`
    /// if the buffer is empty.
    fn claim_slot(
        &self,
    ) -> Option<(*const MaybeUninit<T>, *const AtomicBool)> {
        let mut backoff = 0u32;
        loop {
            let head = self.queue.head.load(Ordering::Relaxed);

            // SAFETY: `head & mask` is always < cap by construction
            let slot =
                unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

            // The ready flag is the authoritative signal. It guarantees the
            // producer has finished writing data to this slot.
            if !slot.ready.load(Ordering::Acquire) {
                return None;
            }

            // Ready! Try to claim this slot via CAS.
            match self.queue.head.compare_exchange_weak(
                head,
                head + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some((
                        slot.data.get().cast_const(),
                        &raw const slot.ready,
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
        let (data_ptr, ready_ptr) = self.claim_slot()?;

        // SAFETY: We atomically claimed this slot via CAS. The producer
        // set ready=true after writing data.
        let val = unsafe { data_ptr.cast::<T>().read() };

        // Clear the ready flag so the producer knows this slot is free.
        // SAFETY: ready_ptr points into the RingBuffer kept alive by Arc.
        unsafe {
            (*ready_ptr).store(false, Ordering::Release);
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
        let (data_ptr, ready_ptr) = self.claim_slot()?;

        Some(SlotReader {
            data_ptr,
            ready_ptr,
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
/// When dropped, drops the `T` value and clears the slot's ready flag
/// so the producer can reuse the slot.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    ready_ptr: *const AtomicBool,
    _consumer: &'a mut Consumer<T>,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was claimed via CAS after verifying ready=true.
        // The data is initialized and we have exclusive access via CAS
        // claim + &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (ready=true was verified before CAS).
        // Exclusive access guaranteed by CAS + &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // Clear the ready flag so the producer can reuse this slot.
        // SAFETY: ready_ptr points into the RingBuffer kept alive by
        // consumer's Arc.
        unsafe {
            (*self.ready_ptr).store(false, Ordering::Release);
        }
    }
}
