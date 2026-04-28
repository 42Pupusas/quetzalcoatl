use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::{Config, DefaultConfig, RingBuffer, PARK_MASK};

/// Cloneable consumer for an MPMC ring.
///
/// Each consumer keeps a private `next_scan` cursor and walks the
/// ring looking for `published` slots, CAS-claiming the first one
/// it finds. No shared head cursor — contention is distributed
/// across the `cap` per-slot atomics.
pub struct Consumer<T, C: Config = DefaultConfig> {
    pub(super) queue: Arc<RingBuffer<T, C>>,
    /// Private scan cursor (logical position).
    next_scan: Cell<usize>,
    /// Pops since the last `consumed` flush.
    local_consumed: Cell<usize>,
    /// Stable park slot for this consumer (mod `PARK_SLOTS`).
    park_slot: usize,
}

impl<T, C: Config> Clone for Consumer<T, C> {
    fn clone(&self) -> Self {
        // Stagger starting scan position by `cap/8` per clone so
        // new consumers don't all race slot 0 on the first pop.
        let n = self
            .queue
            .clone_counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let stagger = (n * (self.queue.cap / 8).max(1)) & self.queue.mask;
        // Distinct park slot per clone, mod PARK_SLOTS.
        let park_idx = self
            .queue
            .consumer_count
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        // Bump the live-consumer count so producers can detect
        // "all consumers gone" via consumer_closed.
        self.queue
            .consumer_count_live
            .fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            next_scan: Cell::new(stagger),
            local_consumed: Cell::new(0),
            park_slot: park_idx & PARK_MASK,
        }
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send because each handle
// is single-threaded by contract. The Arc keeps the RingBuffer alive.
unsafe impl<T: Send, C: Config> Send for Consumer<T, C> {}

impl<T, C: Config> Consumer<T, C> {
    pub(super) const fn new(queue: Arc<RingBuffer<T, C>>) -> Self {
        Self {
            queue,
            next_scan: Cell::new(0),
            local_consumed: Cell::new(0),
            park_slot: 0,
        }
    }

    /// Pops the first published slot found at or after `next_scan`,
    /// returning `None` after one full lap of the ring without
    /// finding any published item.
    ///
    /// Items are returned in the order slots become published, not
    /// in producer push order — see the module docs.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let q = &*self.queue;
        let cap = q.cap;
        let mask = q.mask;

        let start_scan = self.next_scan.get();
        let mut scan = start_scan;
        let max_iters = cap;
        let mut iters = 0usize;

        loop {
            // Decode `ready[s] = s + R*cap + state`, state ∈ {0,1,2}.
            let s = scan & mask;
            let r = q.ready_slot(scan).load(Ordering::Acquire);
            let delta = r.wrapping_sub(s);
            // delta % cap == delta & mask, since cap is a power of two.
            let state = delta & mask;
            let round_pos = r.wrapping_sub(state);

            if state == 1 {
                let claimed_marker = round_pos + 2;
                if q.ready_slot(scan)
                    .compare_exchange(r, claimed_marker, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    // SAFETY: the successful CAS gives us unique
                    // ownership of slot `s` for round R, and the
                    // producer for that round had committed the
                    // value before storing state=1.
                    let val = unsafe { q.data_slot(scan).get().cast::<T>().read() };
                    q.done_slot(scan).store(round_pos + cap, Ordering::Release);
                    // Relaxed wake gate: a missed wake here is
                    // covered by the producer's 200μs park timeout,
                    // while a SeqCst load would tax every pop with
                    // a full barrier.
                    if q.producer_wake.load(Ordering::Relaxed) != 0 {
                        super::wake_one_producer(q);
                    }
                    // Advance scan past the consumed slot, keeping
                    // it monotonic in case we claimed a future slot.
                    self.next_scan.set((round_pos + 1).max(start_scan + 1));

                    let lc = self.local_consumed.get() + 1;
                    if lc >= C::CONSUMED_FLUSH {
                        q.consumed.fetch_add(lc, Ordering::Relaxed);
                        self.local_consumed.set(0);
                    } else {
                        self.local_consumed.set(lc);
                    }

                    return Some(val);
                }
                // CAS lost — skip far enough that the contended
                // line cools before our next attempt.
                scan += C::CAS_FAIL_SKIP - 1;
                iters += C::CAS_FAIL_SKIP - 1;
            }

            scan += 1;
            iters += 1;
            if iters >= max_iters {
                self.next_scan.set(scan);
                return None;
            }
        }
    }

    /// Pops the next item, blocking the calling thread when the
    /// ring is empty until a producer publishes one. Returns
    /// `None` only when the ring is closed AND empty (no live
    /// producers, nothing left to drain).
    ///
    /// Uses the same futex-style wake bitmap as the producer slow
    /// path: spins briefly first, then sets a wake bit and parks.
    /// Producers signal after every `ready[s].store(Release)`,
    /// gated on a single `Relaxed` load so the no-park hot path
    /// stays cheap.
    #[must_use]
    pub fn pop_block(&self) -> Option<T> {
        let q = &*self.queue;
        let bit_mask = 1u64 << self.park_slot;
        let mut backoff = 0u32;
        loop {
            if let Some(v) = self.pop() {
                return Some(v);
            }
            // Empty AND closed AND really nothing left → done.
            if q.closed.0.load(Ordering::Acquire) {
                if let Some(v) = self.pop() {
                    return Some(v);
                }
                return None;
            }
            // Spin until cas_backoff fully escalates (~tens of μs
            // including yields) before paying for park.
            if backoff < 12 {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            ensure_consumer_handle_installed(q, self.park_slot);
            // SeqCst pairs with producer's `consumer_wake.load`
            // after `ready.store(Release)`: either we see a
            // published slot in the re-check below, or the
            // producer sees our bit and unparks us.
            q.consumer_wake.fetch_or(bit_mask, Ordering::SeqCst);

            if let Some(v) = self.pop() {
                q.consumer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return Some(v);
            }
            if q.closed.0.load(Ordering::Acquire) {
                q.consumer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return self.pop();
            }

            std::thread::park_timeout(Duration::from_micros(200));
            q.consumer_wake.fetch_and(!bit_mask, Ordering::Relaxed);
        }
    }

    /// Best-effort approximate queue length. Not precise — this
    /// design doesn't maintain a cheap exact `len`.
    #[must_use]
    pub fn approx_len(&self) -> usize {
        let claim = self.queue.claim.load(Ordering::Relaxed);
        let scan = self.next_scan.get();
        claim.wrapping_sub(scan).min(self.queue.cap)
    }

    /// Returns `true` once the last [`Producer`](super::Producer)
    /// has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.closed.0.load(Ordering::Acquire)
    }

    /// Diagnostic snapshot: `(next_scan, claim, ready[..], done[..])`.
    #[doc(hidden)]
    pub fn debug_snapshot(&self) -> (usize, usize, Vec<usize>, Vec<usize>) {
        let (claim, r, d) = self.queue.debug_snapshot();
        (self.next_scan.get(), claim, r, d)
    }
}

impl<T, C: Config> Drop for Consumer<T, C> {
    fn drop(&mut self) {
        // Flush the per-consumer count so producers' free-space
        // estimate doesn't permanently lag this consumer's work.
        let lc = self.local_consumed.get();
        if lc > 0 {
            self.queue.consumed.fetch_add(lc, Ordering::Relaxed);
        }

        // Last-consumer drop: flag `consumer_closed` and wake any
        // producers parked in `push_block` so they can observe it
        // and return Err.
        if self
            .queue
            .consumer_count_live
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.queue.consumer_closed.0.store(true, Ordering::Release);
            let mut bits = self.queue.producer_wake.swap(0, Ordering::AcqRel);
            while bits != 0 {
                let b = bits.trailing_zeros() as usize;
                if let Some(handle) = self.queue.producer_parkers[b].get() {
                    handle.unpark();
                }
                bits &= bits - 1;
            }
        }
    }
}

/// Idempotently installs the current thread's `Thread` handle in
/// `q.consumer_parkers[slot]`. See producer-side analogue.
#[inline]
fn ensure_consumer_handle_installed<T, C: Config>(q: &RingBuffer<T, C>, slot: usize) {
    let _ = q.consumer_parkers[slot].set(std::thread::current());
}
