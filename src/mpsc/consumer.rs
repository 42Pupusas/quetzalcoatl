use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;
use crate::common::TOMBSTONE;

/// The consumer side of an MPSC ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Not
/// cloneable — only one consumer exists per buffer.
///
/// Drains remaining items when dropped.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
}

impl<T> Consumer<T> {
    /// Non-mutating check: returns true if a `pop`/`pop_ref` would
    /// likely succeed at the current `head`. Used as a pre-park
    /// gate so we don't construct a `SlotReader` (or `pop` a value)
    /// just to discard it.
    ///
    /// May return false when a tombstone is at `head` even though
    /// the next non-tombstoned slot is ready — this just causes an
    /// extra spin/park iteration where `pop_ref` itself will skip
    /// the tombstone and return the real item.
    #[inline]
    fn has_item(&self) -> bool {
        let head = self.queue.head.load(Ordering::Relaxed);
        let seq = self.queue.slot(head).sequence.load(Ordering::Acquire);
        seq == head * 2 + 1 || seq == TOMBSTONE
    }

    /// Returns None if the queue is empty or if a slot has been claimed
    /// by a producer but not yet written (non-blocking behavior).
    ///
    /// Automatically skips tombstoned slots (abandoned reservations).
    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        loop {
            let head = self.queue.head.load(Ordering::Relaxed);

            let slot = self.queue.slot(head);

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                // Abandoned slot — release it for reuse and skip.
                slot.sequence
                    .store((head + self.queue.cap) * 2, Ordering::Release);
                self.queue.head.store(head + 1, Ordering::Release);
                continue;
            }

            // The sequence number is the sole synchronization point. It subsumes
            // the tail check: seq == head * 2 + 1 means a producer has both claimed
            // AND written the slot. Any other value means either empty or
            // claimed-but-not-written — both cases require returning None.
            if seq != head * 2 + 1 {
                return None;
            }

            // SAFETY: We checked that the slot is ready
            let val = unsafe { (*slot.data.get()).assume_init_read() };

            // Release the slot: set sequence to (head + cap) * 2 so producers
            // know this slot is free for reuse.
            slot.sequence
                .store((head + self.queue.cap) * 2, Ordering::Release);

            // Advance head
            self.queue.head.store(head + 1, Ordering::Release);

            // Wake one parked producer if any. `wake_one` self-gates
            // on a `Relaxed` load; missed wakes are bounded by the
            // producer's park-timeout backstop.
            self.queue.producer_park.wake_one();

            return Some(val);
        }
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    ///
    /// Unlike [`pop`](Self::pop), this does not copy the data out. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The slot is released
    /// when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the queue is empty or the next slot is not yet
    /// committed (same semantics as `pop`).
    ///
    /// Automatically skips tombstoned slots (abandoned reservations).
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        loop {
            let head = self.queue.head.load(Ordering::Relaxed);

            let slot = self.queue.slot(head);

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                // Abandoned slot — release it for reuse and skip.
                slot.sequence
                    .store((head + self.queue.cap) * 2, Ordering::Release);
                self.queue.head.store(head + 1, Ordering::Release);
                continue;
            }

            if seq != head * 2 + 1 {
                return None;
            }

            // SAFETY: seq == head * 2 + 1 guarantees the slot has been initialized.
            let data_ptr = slot.data.get().cast_const();
            let seq_ptr = &raw const slot.sequence;

            return Some(SlotReader {
                data_ptr,
                seq_ptr,
                consumer: self,
                head,
            });
        }
    }

    /// Drains all available items, calling `f` for each one.
    ///
    /// Unlike calling [`pop`](Self::pop) in a loop, this amortizes the
    /// `head` pointer update: individual slot sequences are released
    /// immediately (so producers spinning on a slot can proceed), but the
    /// shared `head` pointer is written **once** at the end of the batch.
    /// This reduces cache-line invalidation traffic from O(n) to O(1) for
    /// the most contended atomic.
    ///
    /// Returns the number of items drained.
    pub fn drain(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut head = self.queue.head.load(Ordering::Relaxed);
        let mut count = 0usize;

        loop {
            let slot = self.queue.slot(head);

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                // Abandoned slot — release it for reuse and skip.
                slot.sequence
                    .store((head + self.queue.cap) * 2, Ordering::Release);
                head += 1;
                continue;
            }

            if seq != head * 2 + 1 {
                break;
            }

            // SAFETY: We checked that the slot is ready
            let val = unsafe { (*slot.data.get()).assume_init_read() };

            // Release the slot so producers can reuse it immediately.
            // Head update is deferred to batch all items into a single store.
            slot.sequence
                .store((head + self.queue.cap) * 2, Ordering::Release);

            head += 1;
            count += 1;
            f(val);
        }

        if count > 0 {
            // Single head update for the entire batch — this is the key
            // optimization. Producers use head for fullness pre-checks,
            // so batching reduces cache-line invalidations from O(n) to O(1).
            self.queue.head.store(head, Ordering::Release);
            // Wake up to `count` parked producers. A drain frees `count`
            // slots, and there may be that many producers parked on
            // push_block waiting for space — waking only one would
            // strand the rest until the next pop. `wake_n` self-gates
            // on the bitmap being non-zero.
            self.queue.producer_park.wake_n(count);
        }

        count
    }

    /// Drains up to `limit` available items, calling `f` for each one.
    ///
    /// Same amortized-head optimization as [`drain`](Self::drain), but
    /// stops after `limit` items. Useful for fairness in multi-source
    /// consumer loops.
    ///
    /// Returns the number of items drained.
    pub fn drain_up_to(&mut self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let mut head = self.queue.head.load(Ordering::Relaxed);
        let mut count = 0usize;

        while count < limit {
            let slot = self.queue.slot(head);

            let seq = slot.sequence.load(Ordering::Acquire);

            if seq == TOMBSTONE {
                slot.sequence
                    .store((head + self.queue.cap) * 2, Ordering::Release);
                head += 1;
                continue;
            }

            if seq != head * 2 + 1 {
                break;
            }

            let val = unsafe { (*slot.data.get()).assume_init_read() };

            slot.sequence
                .store((head + self.queue.cap) * 2, Ordering::Release);

            head += 1;
            count += 1;
            f(val);
        }

        if count > 0 {
            self.queue.head.store(head, Ordering::Release);
            // Wake up to `count` parked producers — see `drain` for
            // rationale.
            self.queue.producer_park.wake_n(count);
        }

        count
    }

    /// Drains items, blocking when empty, until all producers have
    /// dropped. Calls `f` for each item drained. Returns the total
    /// count.
    ///
    /// Combines [`drain`](Self::drain)'s amortized head update and
    /// `wake_n` fan-out with [`pop_block`](Self::pop_block)'s park-
    /// on-empty protocol. Use this for a long-running consumer that
    /// wants throughput (drain) and CPU efficiency on idle (block).
    pub fn drain_block(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut total = 0usize;
        loop {
            total += self.drain(&mut f);
            // After the drain, either wait for more items or exit on
            // closed (with a final drain to catch late publishes
            // racing the producer's last drop).
            match self.pop_block() {
                Some(v) => {
                    total += 1;
                    f(v);
                }
                None => return total,
            }
        }
    }

    /// Pops the next item, blocking the calling thread when the
    /// ring is empty until a producer publishes one. Returns `None`
    /// only after the last [`Producer`](super::Producer) has been
    /// dropped AND the ring has drained.
    ///
    /// Spins via the shared backoff schedule first, then registers
    /// the consumer parker handle and parks. Producers signal after
    /// every push (or batched at end of `drain`) gated on a single
    /// `Relaxed` load so the no-park hot path stays cheap.
    #[must_use]
    pub fn pop_block(&mut self) -> Option<T> {
        let mut backoff = 0u32;
        loop {
            if let Some(v) = self.pop() {
                return Some(v);
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                // Re-check after observing closed: a producer may
                // have published just before its last drop, and we
                // must drain that before returning None.
                if let Some(v) = self.pop() {
                    return Some(v);
                }
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            // Install our parker handle (idempotent on the OnceLock).
            let _ = self.queue.consumer_parker.set(std::thread::current());
            // SeqCst pairs with the producer's `consumer_parked.load`
            // after `slot_seq.store(Release)`: either we see the
            // published slot in the re-check below, or the producer
            // sees our flag and unparks us.
            self.queue.consumer_parked.0.store(true, Ordering::SeqCst);

            if let Some(v) = self.pop() {
                self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
                return Some(v);
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
                return self.pop();
            }

            std::thread::park();
            self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
        }
    }

    /// Returns a zero-copy read reference to the next item, blocking
    /// the calling thread when the ring is empty until a producer
    /// publishes one. Returns `None` only when the last producer has
    /// dropped AND the ring has drained.
    ///
    /// Same wait protocol as [`pop_block`](Self::pop_block). Useful
    /// when you want zero-copy reads plus blocking.
    #[must_use]
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T>> {
        let mut backoff = 0u32;
        loop {
            // Non-mutating gate: do NOT call pop_ref() in the gate
            // because the discarded SlotReader would advance head on
            // drop and consume the item we wanted to return.
            if self.has_item() {
                return self.pop_ref();
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                if self.has_item() {
                    return self.pop_ref();
                }
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            let _ = self.queue.consumer_parker.set(std::thread::current());
            self.queue.consumer_parked.0.store(true, Ordering::SeqCst);

            if self.has_item() {
                self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
                return self.pop_ref();
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
                if self.has_item() {
                    return self.pop_ref();
                }
                return None;
            }

            std::thread::park();
            self.queue.consumer_parked.0.store(false, Ordering::Relaxed);
        }
    }

    /// Returns `true` once the last [`Producer`](super::Producer)
    /// has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.closed.0.load(Ordering::Acquire)
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
        // Flag consumer-closed *before* draining so producers parked
        // in push_block observe it and return Err(val) instead of
        // racing the drain to push into the slots we're about to
        // free. Without this, a parked producer can see free space
        // from drain() and return Ok even though the consumer is on
        // its way out.
        self.queue.consumer_closed.0.store(true, Ordering::Release);
        self.queue.producer_park.flush();
        while self.pop().is_some() {}
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without copying.
///
/// When dropped, drops the `T` value, releases the slot by updating
/// the sequence number, and advances the head pointer.
pub struct SlotReader<'a, T> {
    data_ptr: *const MaybeUninit<T>,
    seq_ptr: *const AtomicUsize,
    consumer: &'a mut Consumer<T>,
    head: usize,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was checked seq == head * 2 + 1 (Acquire) in pop_ref.
        // The data is initialized and we have exclusive access via &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (seq check verified in pop_ref).
        // Exclusive access guaranteed by &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // Release the slot so producers can reuse it.
        // SAFETY: seq_ptr points into the RingBuffer kept alive by
        // consumer's Arc.
        let cap = self.consumer.queue.cap;
        unsafe {
            (*self.seq_ptr).store((self.head + cap) * 2, Ordering::Release);
        }

        self.consumer
            .queue
            .head
            .store(self.head + 1, Ordering::Release);

        self.consumer.queue.producer_park.wake_one();
    }
}
