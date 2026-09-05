use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::consumer_floor::ConsumerFloor;
use super::RingBuffer;
use crate::common::park::{BACKOFF_PARK_THRESHOLD, PARK_MASK};
use crate::common::TOMBSTONE;

/// The producer side of a broadcast ring buffer.
///
/// Obtained via [`RingBuffer::split`](super::RingBuffer::split). Cloneable
/// — each clone shares the same underlying buffer and competes for slots
/// by compare-and-swap on the tail.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    pub(super) cached_min_head: std::cell::Cell<usize>,
    /// Stable park slot for this producer handle (mod `PARK_SLOTS`).
    /// Used by [`Producer::push_block`] / [`Producer::reserve_block`] to
    /// register on the producer wake bitmap.
    pub(super) park_slot: usize,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        #[cfg(feature = "async")]
        self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        let park_idx = self
            .queue
            .producer_park_idx
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        Self {
            queue: Arc::clone(&self.queue),
            cached_min_head: std::cell::Cell::new(0),
            park_slot: park_idx & PARK_MASK,
        }
    }
}

/// Returns true if at least one consumer slot is currently active.
/// Used by `push_block` / `push_async` to detect "all consumers gone"
/// without an explicit consumer-count atomic — the per-slot `active`
/// flags are the existing source of truth.
fn any_consumer_active<T>(q: &RingBuffer<T>) -> bool {
    q.consumer_slots
        .iter()
        .any(|s| s.active.load(Ordering::Acquire))
}

#[cfg(feature = "async")]
impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last producer gone: flag closed and flush all parked
            // consumers so their pop_async can return None after
            // their backlog drains.
            //
            // SeqCst pairs with the consumer's `closed.load(SeqCst)`
            // after register: the consumer's recheck must observe this
            // store, otherwise a consumer parked between "load closed
            // false" and "register" would miss both the close signal
            // and the flush, hanging forever.
            self.queue.closed.0.store(true, Ordering::SeqCst);
            self.queue.consumer_waker.flush();
        }
    }
}

impl<T> Producer<T> {
    /// Claims a position, or returns `None` if the ring is full.
    ///
    /// The claim is a CAS rather than a `fetch_add`. A `fetch_add`
    /// always succeeds, so a fullness check before it is only a hint:
    /// with `k` free slots, any number of concurrent producers can pass
    /// the check and claim positions past capacity. The losers would
    /// then wait inside a non-blocking `push` for a consumer that may
    /// never advance. Validating the position as part of the claim means
    /// a producer only ever owns a slot it is allowed to write.
    #[inline]
    fn claim_slot(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        let mut backoff = 0u32;
        let mut current_tail = self.queue.tail.load(Ordering::Relaxed);
        let pos = loop {
            if !self.permits_with_refresh(current_tail) {
                return None;
            }
            match self.queue.tail.compare_exchange_weak(
                current_tail,
                current_tail + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break current_tail,
                Err(observed) => {
                    current_tail = observed;
                    crate::common::cas_backoff(&mut backoff);
                }
            }
        };

        let slot = self.queue.slot(pos);

        // Drop old value if this slot was previously written.
        if std::mem::needs_drop::<T>() {
            let old_seq = slot.sequence.swap(0, Ordering::Acquire);
            if old_seq > 0 && old_seq != TOMBSTONE {
                // SAFETY: old_seq > 0 means data was initialized
                // by a prior push. We own the slot via our CAS claim.
                unsafe {
                    slot.data.get().cast::<T>().drop_in_place();
                }
            }
        } else {
            slot.sequence.store(0, Ordering::Relaxed);
        }

        Some((slot.data.get(), &slot.sequence, pos))
    }

    /// Pushes a value into the broadcast ring buffer.
    ///
    /// Multiple producers can push concurrently. Never blocks.
    ///
    /// Returns `Err(val)` if the buffer is full, or if no consumers are
    /// registered — a broadcast with no subscribers has nowhere to
    /// deliver to, and accepting the write would let concurrent
    /// producers lap the ring and overwrite each other.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, slot_seq, pos)) => {
                // SAFETY: We exclusively own this slot via our CAS claim.
                unsafe { (*data_ptr).write(val) };
                slot_seq.store(pos * 2 + 1, Ordering::Release);
                #[cfg(feature = "async")]
                self.queue.wake_consumer_async();
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Conservative fullness check used by the blocking variants to
    /// decide whether a `reserve` would succeed without attempting a
    /// claim. Refreshes the cached `min_head` from the shared cache,
    /// then a full scan only if still apparently full. Returns `true`
    /// if there is at least one writable slot.
    #[inline]
    fn has_space(&self) -> bool {
        self.permits_with_refresh(self.queue.tail.load(Ordering::Relaxed))
    }

    /// Whether `pos` is writable, consulting the per-producer cache,
    /// then the shared cache, then a full registry scan — stopping at
    /// the first level that says yes.
    ///
    /// Both caches hold a bare position and cannot represent "no
    /// consumers", so they are only consulted while a consumer is
    /// registered. Otherwise a cache seeded at 0 would permit the first
    /// `cap` positions with no consumer ever having existed.
    #[inline]
    fn permits_with_refresh(&self, pos: usize) -> bool {
        if any_consumer_active(&self.queue) {
            if self.cached_floor().permits(pos, self.queue.cap) {
                return true;
            }
            let shared = self.queue.min_head_cache.load(Ordering::Acquire);
            self.cached_min_head.set(shared);
            if ConsumerFloor::At(shared).permits(pos, self.queue.cap) {
                return true;
            }
        }
        self.refresh_floor().permits(pos, self.queue.cap)
    }

    /// The last floor this producer observed, as a constraint.
    ///
    /// The cache holds a plain position, so it is always treated as
    /// constraining; a stale floor only costs a redundant scan.
    #[inline]
    const fn cached_floor(&self) -> ConsumerFloor {
        ConsumerFloor::At(self.cached_min_head.get())
    }

    /// Rescans the registry and republishes the result to both cache
    /// levels.
    #[inline]
    fn refresh_floor(&self) -> ConsumerFloor {
        let floor = self.queue.consumer_floor();
        if let ConsumerFloor::At(head) = floor {
            self.queue.min_head_cache.fetch_max(head, Ordering::Release);
            self.cached_min_head.set(head);
        }
        floor
    }

    /// Pushes a value, blocking the calling thread when the ring is full
    /// (the slowest consumer hasn't caught up) until a consumer advances
    /// its head. Returns `Err(val)` only when **all** consumers have been
    /// dropped — a push with no consumer would sit until overwritten, so
    /// that's treated as a closed channel.
    ///
    /// Spins via the shared backoff schedule first, then sets a wake bit
    /// in the producer wake bitmap and parks. Consumers `wake_one` after
    /// every head advance (and `flush` on drop). Same wait protocol as
    /// [`mpsc::Producer::push_block`](crate::mpsc::Producer::push_block).
    ///
    /// Note: unlike [`push`](Self::push), the value is moved out and back
    /// on each retry, so `T` need not be `Clone`.
    pub fn push_block(&self, mut val: T) -> Result<(), T> {
        let q = &*self.queue;
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        // Gate on `has_space()` (a real min_head scan) *before* calling
        // `push`. `push`/`claim_slot` does an unconditional `tail.fetch_add`
        // and then spins unboundedly on `min_head` if the slot it claimed
        // is still occupied — so calling it on a full ring busy-spins
        // forever and never reaches the park below. `has_space` claims
        // nothing, so we only `push` once a slot is genuinely free.
        loop {
            if !any_consumer_active(q) {
                return Err(val);
            }
            if self.has_space() {
                // Space confirmed; `push` won't block in claim_slot's
                // min_head spin. A racing producer (SubRepl has N
                // producers) could still take the last slot first, in
                // which case `push` returns Err — loop and re-evaluate
                // rather than recurse.
                match self.push(val) {
                    Ok(()) => return Ok(()),
                    Err(returned) => {
                        val = returned;
                        backoff = 0;
                        continue;
                    }
                }
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            q.producer_park.ensure_handle_installed(self.park_slot);
            q.producer_park.wake.fetch_or(bit_mask, Ordering::SeqCst);
            // Pairs with WakeSet::wake_one's fence: the re-checks below
            // load consumer state with Acquire, which the SeqCst RMW
            // above does not place in the total order.
            std::sync::atomic::fence(Ordering::SeqCst);

            // Re-check after arming the wake bit: a consumer that advanced
            // (or dropped) between our has_space check and the fetch_or
            // would otherwise have nothing left to wake us. Either we make
            // progress here, or the consumer's wake_one/flush sees the bit.
            if !any_consumer_active(q) {
                q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return Err(val);
            }
            if self.has_space() {
                q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                backoff = 0;
                continue;
            }

            std::thread::park();
            q.producer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
            backoff = 0;
        }
    }

    /// Reserves a slot for zero-copy writing, blocking the calling thread
    /// when the ring is full until a consumer advances. Returns `None`
    /// only when all consumers have been dropped.
    ///
    /// Same wait protocol as [`push_block`](Self::push_block).
    pub fn reserve_block(&mut self) -> Option<SlotWriter<'_, T>> {
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        loop {
            if !any_consumer_active(&self.queue) {
                return None;
            }
            if self.has_space() {
                return self.reserve();
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            self.queue
                .producer_park
                .ensure_handle_installed(self.park_slot);
            self.queue
                .producer_park
                .wake
                .fetch_or(bit_mask, Ordering::SeqCst);
            // See push_block: pairs with WakeSet::wake_one's fence.
            std::sync::atomic::fence(Ordering::SeqCst);

            if !any_consumer_active(&self.queue) {
                self.queue
                    .producer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                return None;
            }
            if self.has_space() {
                self.queue
                    .producer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                return self.reserve();
            }

            std::thread::park();
            self.queue
                .producer_park
                .wake
                .fetch_and(!bit_mask, Ordering::Relaxed);
        }
    }

    /// Pushes a value asynchronously, yielding to the executor when the
    /// ring is full (slowest consumer hasn't caught up). Returns
    /// `Err(val)` only when all consumers have been dropped.
    ///
    /// The future is cancel-safe: dropping it before completion leaves
    /// the ring unchanged (the value is moved back out on cancellation).
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub fn push_async(&self, val: T) -> impl std::future::Future<Output = Result<(), T>> + '_ {
        let mut val = Some(val);
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            // No active consumers means a push would just sit until
            // overwritten — treat as Err so producers can detect the
            // shutdown.
            if !any_consumer_active(&self.queue) {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    // Producers register on slot index = current tail
                    // mod PARK_SLOTS; the consumer's wake fires the
                    // matching slot.
                    let slot = self.queue.tail.load(Ordering::Relaxed);
                    self.queue.producer_waker.register(slot, cx);
                    if !any_consumer_active(&self.queue) {
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

    /// Reserves a slot for zero-copy writing.
    ///
    /// Takes `&mut self` to guarantee at most one outstanding reservation
    /// per producer handle. If dropped without writing, the slot is
    /// tombstoned and consumers silently skip it.
    ///
    /// Returns `None` if the buffer is full or no consumers are
    /// registered.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        let queue = &*self.queue;
        self.claim_slot()
            .map(|(data_ptr, slot_sequence, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_sequence,
                pos,
                queue,
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

/// A write-reservation into a broadcast ring buffer slot.
///
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → slot is tombstoned (consumers skip it).
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_sequence: &'a AtomicUsize,
    pos: usize,
    #[allow(dead_code)] // only read when feature = "async" is enabled
    queue: &'a RingBuffer<T>,
}

// SAFETY: SlotWriter holds exclusive access to the slot (CAS claim).
// The raw pointer points into the RingBuffer kept alive by the Producer's Arc.
unsafe impl<T: Send + Sync> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Requires [`commit_unchecked`](Self::commit_unchecked) (unsafe) to publish.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Exclusive access via CAS claim. The pointer is valid
        // because the Producer's Arc keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access via CAS claim, valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_sequence: this.slot_sequence,
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
        self.slot_sequence
            .store(self.pos * 2 + 1, Ordering::Release);
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        self.slot_sequence.store(TOMBSTONE, Ordering::Release);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and slot is tombstoned.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_sequence: &'a AtomicUsize,
    pos: usize,
    #[allow(dead_code)] // only read when feature = "async" is enabled
    queue: &'a RingBuffer<T>,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's Arc.
unsafe impl<T: Send + Sync> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to all consumers.
    #[inline]
    pub fn commit(mut self) {
        self.slot_sequence
            .store(self.pos * 2 + 1, Ordering::Release);
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        self.committed = true;
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot data.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            self.slot_sequence.store(TOMBSTONE, Ordering::Release);
            #[cfg(feature = "async")]
            self.queue.wake_consumer_async();
        }
    }
}
