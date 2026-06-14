use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::{Config, DefaultConfig, RingBuffer};
use crate::common::park::{BACKOFF_PARK_THRESHOLD, PARK_MASK};

/// Tight-spin iterations on the primary slot's `done` before
/// falling back to bitmap scan.
const PRIMARY_SHORT_SPIN: u32 = 4;

/// Cloneable producer for an MPMC ring.
///
/// Each producer reserves a batch of logical positions from the
/// shared `claim` cursor, then publishes into them out of order —
/// using whichever batch slot's `done[s]` releases first, rather
/// than waiting on a particular slot.
pub struct Producer<T, C: Config = DefaultConfig> {
    pub(super) queue: Arc<RingBuffer<T, C>>,
    /// Start position of the current batch.
    batch_start: Cell<usize>,
    /// Bitmap of reserved-but-unpublished positions in the current
    /// batch. Bit `i` set ↔ `batch_start + i` is available to use.
    batch_unused: Cell<u32>,
    /// Original batch size (≤ `C::PRODUCER_BATCH`); diagnostic.
    batch_size: Cell<u32>,
    /// Stable park slot for this producer (mod `PARK_SLOTS`).
    park_slot: usize,
}

impl<T, C: Config> Clone for Producer<T, C> {
    fn clone(&self) -> Self {
        let n = self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            batch_start: Cell::new(0),
            batch_unused: Cell::new(0),
            batch_size: Cell::new(0),
            park_slot: (n + 1) & PARK_MASK,
        }
    }
}

// SAFETY: Cell is !Sync, but Producer is Send because each handle
// is single-threaded by contract. The Arc keeps the RingBuffer alive.
unsafe impl<T: Send, C: Config> Send for Producer<T, C> {}

impl<T, C: Config> Producer<T, C> {
    pub(super) const fn new(queue: Arc<RingBuffer<T, C>>) -> Self {
        Self {
            queue,
            batch_start: Cell::new(0),
            batch_unused: Cell::new(0),
            batch_size: Cell::new(0),
            park_slot: 0,
        }
    }

    /// Refills the per-producer batch from the shared `claim`
    /// cursor. Returns `Some((start, unused))` on success, or `None`
    /// if the watermark says full and a per-slot check confirms it.
    #[inline]
    fn refill_batch(&self, q: &RingBuffer<T, C>) -> Option<(usize, u32)> {
        // Bound the batch by free space estimated from the
        // (lagging) `consumed` watermark — safe to underestimate.
        let claim = q.claim.load(Ordering::Relaxed);
        let consumed = q.consumed.load(Ordering::Acquire);
        let in_flight = claim.wrapping_sub(consumed);
        let free = if in_flight >= q.cap {
            // Watermark says full; per-slot check before giving up.
            let next_done = q.done_slot(claim).load(Ordering::Acquire);
            if next_done != claim {
                return None;
            }
            1
        } else {
            q.cap - in_flight
        };
        let batch = C::PRODUCER_BATCH.min(free);

        let start = q.claim.fetch_add(batch, Ordering::Relaxed);
        let unused = if batch >= 32 {
            u32::MAX
        } else {
            #[allow(clippy::cast_possible_truncation)]
            let b = batch as u32;
            (1u32 << b) - 1
        };
        self.batch_start.set(start);
        self.batch_unused.set(unused);
        #[allow(clippy::cast_possible_truncation)]
        self.batch_size.set(batch as u32);
        Some((start, unused))
    }

    /// Acquires a free slot for a write. Returns the `(bit, pos)`
    /// of a position whose `done[s]` indicates it is free for the
    /// current round, or `None` if no slot in the batch is free. On
    /// success, the corresponding bit is removed from `batch_unused`
    /// (the caller is now responsible for it — either commit by storing
    /// `ready[s] = pos+1`, or restore the bit to `batch_unused` to
    /// abandon the reservation).
    ///
    /// Always non-blocking: callers that block (`push_block`,
    /// `reserve_block`) run their own spin-then-park loop and retry
    /// this on each lap.
    #[inline]
    fn acquire_slot(&self) -> Option<(u32, usize)> {
        let q = &*self.queue;

        // Refill the batch if exhausted.
        let unused = self.batch_unused.get();
        let (start, unused) = if unused == 0 {
            self.refill_batch(q)?
        } else {
            (self.batch_start.get(), unused)
        };

        let primary_bit = unused.trailing_zeros();
        let primary_pos = start + primary_bit as usize;
        let mut bit = primary_bit;
        let mut pos = primary_pos;

        // Short spin on the primary first — saves the bitmap-scan
        // cost when the consumer is about to release.
        let mut found_ready = false;
        for _ in 0..PRIMARY_SHORT_SPIN {
            if q.done_slot(primary_pos).load(Ordering::Acquire) == primary_pos {
                found_ready = true;
                break;
            }
            std::hint::spin_loop();
        }

        // Primary blocked? Scan other unused bits for any slot
        // already released — out-of-order publishing within batch.
        if !found_ready {
            if let Some((b, p)) = q.scan_unused(start, unused & !(1u32 << primary_bit)) {
                bit = b;
                pos = p;
                found_ready = true;
            }
        }

        if !found_ready {
            return None;
        }

        self.batch_unused.set(unused & !(1u32 << bit));
        Some((bit, pos))
    }

    /// Pushes a value. Returns `Err(val)` if the ring is full from
    /// this producer's perspective (approximate — based on the
    /// `consumed` watermark, which lags real consumer progress).
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let Some((_bit, pos)) = self.acquire_slot() else {
            return Err(val);
        };
        let q = &*self.queue;

        // SAFETY: we own this position via the batch reservation,
        // and `done[s] == pos` confirmed the slot is free for our
        // round.
        let data_ptr = q.data_slot(pos).get();
        unsafe { (*data_ptr).write(val) };

        // Publish: ready[s] = pos + 1 (state = published). SeqCst
        // (not Release) drains the store buffer so the SeqCst load of
        // `consumer_park.wake` inside wake_one cannot miss a freshly-
        // parked consumer's bit. Symmetric to Consumer::pop's
        // SeqCst store on `done` — both directions need the
        // store-buffer drain to close the Dekker race against the
        // peer's parking sequence (fetch_or(SC) + fence(SC) + recheck).
        q.ready_slot(pos).store(pos + 1, Ordering::SeqCst);

        // Wake one consumer parked in pop_block, if any.
        q.consumer_park.wake_one();
        #[cfg(feature = "async")]
        q.wake_consumer_async();
        Ok(())
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the ring is full from this producer's
    /// perspective (same approximate full-check as [`push`](Self::push)).
    /// On success, returns a [`SlotWriter`] that provides direct
    /// mutable access to the slot. The slot is not visible to
    /// consumers until committed.
    ///
    /// Takes `&mut self` to guarantee at most one outstanding
    /// reservation per producer handle. If dropped without writing,
    /// the reservation is silently rolled back (the batch bit is
    /// restored, so a subsequent `push` or `reserve` from this
    /// handle can reuse the slot without taking another batch).
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T, C>> {
        let (bit, pos) = self.acquire_slot()?;
        Some(SlotWriter {
            producer: self,
            pos,
            bit,
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
    pub fn reserve_block(&mut self) -> Option<SlotWriter<'_, T, C>> {
        let bit_mask = 1u64 << self.park_slot;
        let park_slot = self.park_slot;
        let mut backoff = 0u32;
        loop {
            if self.queue.consumer_closed.0.load(Ordering::Acquire) {
                return None;
            }
            // The discarded SlotWriter on `is_some()` restores the
            // bit on drop, so the second reserve() reuses the same
            // slot. Self-cancelling — no destructive mutation.
            if self.reserve().is_some() {
                return self.reserve();
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            self.queue.producer_park.ensure_handle_installed(park_slot);
            self.queue
                .producer_park
                .wake
                .fetch_or(bit_mask, Ordering::SeqCst);

            if self.queue.consumer_closed.0.load(Ordering::Acquire) {
                self.queue
                    .producer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                return None;
            }
            if self.reserve().is_some() {
                self.queue
                    .producer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                return self.reserve();
            }

            // park_timeout backstop — see push_block.
            std::thread::park_timeout(std::time::Duration::from_millis(1));
            self.queue
                .producer_park
                .wake
                .fetch_and(!bit_mask, Ordering::Relaxed);
        }
    }

    /// Pushes a value, blocking the calling thread when the ring
    /// is full until a consumer makes space. Returns `Err(val)`
    /// only when the last [`Consumer`](super::Consumer) has been
    /// dropped (no consumer left to drain).
    ///
    /// Uses the same futex-style wake bitmap as the producer slow
    /// path: spins briefly first, then sets a wake bit and parks.
    /// Consumers signal after every `done[s].store(Release)`,
    /// gated on a single `Relaxed` load.
    pub fn push_block(&self, mut val: T) -> Result<(), T> {
        let q = &*self.queue;
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        loop {
            match self.push(val) {
                Ok(()) => return Ok(()),
                Err(returned) => val = returned,
            }
            if q.consumer_closed.0.load(Ordering::Acquire) {
                return Err(val);
            }
            // Spin until cas_backoff fully escalates (~tens of μs
            // including yields) before paying for park.
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            q.producer_park.ensure_handle_installed(self.park_slot);
            // SeqCst pairs with consumer's `producer_park.wake.load`
            // after `done.store(Release)`: either we succeed in
            // the re-check below, or the consumer sees our bit
            // and unparks us.
            q.producer_park.wake.fetch_or(bit_mask, Ordering::SeqCst);
            // Fence: see park_until_slot_free for the rationale.
            std::sync::atomic::fence(Ordering::SeqCst);

            match self.push(val) {
                Ok(()) => {
                    q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                    return Ok(());
                }
                Err(returned) => val = returned,
            }
            if q.consumer_closed.0.load(Ordering::Acquire) {
                q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return Err(val);
            }

            // park_timeout (not park) as a final-line backstop:
            // round-robin in WakeSet eliminates the wake-bit
            // starvation; the SeqCst pairing on `wake` closes the
            // Dekker race. Together those should be sufficient, but
            // an extremely rare residual race (cap=16 stress, ~3%
            // of 5000-item runs) could still hang. The 1ms timeout
            // caps that hang at 1ms — a parked producer always
            // re-checks its done value within that window. Fast
            // path (consumer unparks immediately) is unchanged.
            std::thread::park_timeout(std::time::Duration::from_millis(1));
            q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
        }
    }

    /// Pushes a value asynchronously, yielding to the executor when
    /// the ring is full until a consumer makes space. Returns
    /// `Err(val)` only when the last consumer has been dropped (no
    /// consumer left to drain).
    ///
    /// The future is cancel-safe: dropping it before completion leaves
    /// the ring unchanged (the value is moved back out on cancellation).
    ///
    /// Each producer clone has its own park slot so multiple async
    /// producers can wait concurrently without contending on a single
    /// waker entry.
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub fn push_async(&self, val: T) -> impl std::future::Future<Output = Result<(), T>> + '_ {
        let mut val = Some(val);
        let slot = self.park_slot;
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            if self.queue.consumer_closed.0.load(Ordering::Acquire) {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    self.queue.producer_waker.register(slot, cx);
                    if self.queue.consumer_closed.0.load(Ordering::Acquire) {
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
}

impl<T, C: Config> Drop for Producer<T, C> {
    fn drop(&mut self) {
        // Tombstone any reserved-but-unpublished batch positions:
        // mark them as already-claimed-and-released so the
        // next-round producer can proceed. Without this, dropped
        // producers strand slots and stall the ring.
        let q = &*self.queue;
        let cap = q.cap;
        let start = self.batch_start.get();
        let mut unused = self.batch_unused.get();
        while unused != 0 {
            let bit = unused.trailing_zeros() as usize;
            let pos = start + bit;
            // Wait for previous round's consumer to release before
            // tombstoning, else we'd stomp on prior-round state. Wake
            // any parked consumers each iteration: a consumer can be
            // parked on `consumer_park` waiting for a state==1 slot we
            // already published in this batch, and without an explicit
            // wake here the consumer never advances past the slot whose
            // `done` we're spinning on — classic deadlock during the
            // producer's last drop.
            let done = q.done_slot(pos);
            let mut backoff = 0u32;
            while done.load(Ordering::Acquire) != pos {
                q.consumer_park.wake_one();
                #[cfg(feature = "async")]
                q.wake_consumer_async();
                crate::common::cas_backoff(&mut backoff);
            }
            q.ready_slot(pos).store(pos + 2, Ordering::Release);
            q.done_slot(pos).store(pos + cap, Ordering::Release);
            unused &= unused - 1;
        }

        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // SeqCst pairs with consumer's `closed.load(SeqCst)` in
            // pop_block's recheck after fetch_or — see consumer.rs.
            self.queue.closed.0.store(true, Ordering::SeqCst);
            // Wake every parked producer and consumer so they can
            // observe `closed` and exit. Cold path, last-drop only.
            self.queue.producer_park.flush();
            self.queue.consumer_park.flush();
            #[cfg(feature = "async")]
            self.queue.consumer_waker.flush();
        }
    }
}

/// A write-reservation into an MPMC ring buffer slot.
///
/// Obtained via [`Producer::reserve`]. Call [`write`](Self::write) to
/// initialize the slot and get a [`WrittenSlot`] that can be committed.
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// If dropped without being written or committed, the reservation is
/// silently rolled back: the slot's bit is restored to the producer's
/// `batch_unused` bitmap, so a subsequent `push` or `reserve` from
/// the same producer can reuse it without claiming a new batch.
pub struct SlotWriter<'a, T, C: Config = DefaultConfig> {
    producer: &'a Producer<T, C>,
    pos: usize,
    bit: u32,
}

// SAFETY: SlotWriter borrows the producer mutably (via &mut self in
// reserve), so it has exclusive access to that producer's batch state
// and to the slot's data via the position bit.
unsafe impl<T: Send, C: Config> Send for SlotWriter<'_, T, C> {}

impl<'a, T, C: Config> SlotWriter<'a, T, C> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use [`commit_unchecked`](Self::commit_unchecked) (unsafe) after
    /// initializing through this reference. If dropped without
    /// committing, any data written through this reference is leaked.
    ///
    /// Prefer [`write`](Self::write) for the safe path.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut std::mem::MaybeUninit<T> {
        // SAFETY: We hold the bit for `pos` exclusively (acquire_slot
        // confirmed `done[pos] == pos` and removed the bit from
        // batch_unused). The `&mut self` borrow on the producer
        // serializes against any other access from the same handle.
        unsafe { &mut *self.producer.queue.data_slot(self.pos).get() }
    }

    /// Writes a value into the slot, consuming this `SlotWriter` and
    /// returning a [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T, C> {
        // SAFETY: exclusive access (see slot_mut).
        let data_ptr = self.producer.queue.data_slot(self.pos).get();
        unsafe { (*data_ptr).write(val) };
        // Skip SlotWriter::drop — WrittenSlot now owns the rollback.
        let this = std::mem::ManuallyDrop::new(self);
        WrittenSlot {
            producer: this.producer,
            pos: this.pos,
            bit: this.bit,
            committed: false,
        }
    }

    /// Commits without verifying initialization.
    ///
    /// # Safety
    ///
    /// The caller must have initialized the slot data via
    /// [`slot_mut`](Self::slot_mut) + [`MaybeUninit::write`].
    /// Committing without initializing causes consumers to read
    /// uninitialized memory (undefined behavior).
    #[inline]
    pub unsafe fn commit_unchecked(self) {
        let q = &*self.producer.queue;
        // SeqCst — see Producer::push.
        q.ready_slot(self.pos).store(self.pos + 1, Ordering::SeqCst);
        q.consumer_park.wake_one();
        #[cfg(feature = "async")]
        q.wake_consumer_async();
        // Skip SlotWriter::drop (which would restore the bit).
        std::mem::forget(self);
    }
}

impl<T, C: Config> Drop for SlotWriter<'_, T, C> {
    fn drop(&mut self) {
        // No data was written (write() consumes self into a
        // WrittenSlot, and commit_unchecked forgets self). Restore
        // the bit so the slot can be reused without re-claiming a
        // batch position. Producer::drop will tombstone it if this
        // handle is dropped before the bit is consumed.
        let unused = self.producer.batch_unused.get();
        self.producer.batch_unused.set(unused | (1u32 << self.bit));
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to make the data visible to
/// consumers. If dropped without committing, the value is dropped
/// in place and the reservation is rolled back (the bit is restored
/// to `batch_unused`).
pub struct WrittenSlot<'a, T, C: Config = DefaultConfig> {
    producer: &'a Producer<T, C>,
    pos: usize,
    bit: u32,
    committed: bool,
}

// SAFETY: same as SlotWriter — exclusive access via the producer
// borrow and the held batch bit.
unsafe impl<T: Send, C: Config> Send for WrittenSlot<'_, T, C> {}

impl<T, C: Config> WrittenSlot<'_, T, C> {
    /// Commits the write, making the slot visible to consumers.
    #[inline]
    pub fn commit(mut self) {
        let q = &*self.producer.queue;
        // SeqCst — see Producer::push.
        q.ready_slot(self.pos).store(self.pos + 1, Ordering::SeqCst);
        q.consumer_park.wake_one();
        #[cfg(feature = "async")]
        q.wake_consumer_async();
        self.committed = true;
    }
}

impl<T, C: Config> Drop for WrittenSlot<'_, T, C> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot; we still hold
            // the bit for `pos` exclusively (no consumer has seen
            // ready[pos] = pos+1 because we never published).
            unsafe {
                self.producer
                    .queue
                    .data_slot(self.pos)
                    .get()
                    .cast::<T>()
                    .drop_in_place();
            }
            // Restore the bit so the position can be reused.
            let unused = self.producer.batch_unused.get();
            self.producer.batch_unused.set(unused | (1u32 << self.bit));
        }
    }
}
