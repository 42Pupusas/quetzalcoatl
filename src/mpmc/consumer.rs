use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

/// The consumer side of an MPMC ring buffer.
///
/// Cloneable — each clone competes for items via batched bounded-CAS
/// claim on the head pointer.
///
/// # Claim protocol
///
/// Same batched bounded-CAS as the SPMC consumer: claim a batch of up
/// to `BATCH_SIZE` positions in one CAS clamped by `tail`, then drain
/// from a private cursor. The added wrinkle for MPMC is that
/// publication may be out of order across producers — claiming a
/// position based on `tail` doesn't guarantee `ready[s]` is set yet, so
/// each pop spins on `ready[s] == pos + 1` before reading.
pub struct Consumer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
    cached_tail: Cell<usize>,
    batch_next: Cell<usize>,
    batch_end: Cell<usize>,
}

/// Maximum positions claimed in one CAS on `head`. Larger = fewer
/// coherence-traffic-heavy CAS instructions on the contended head line,
/// at the cost of worse cross-consumer fairness (one consumer can take
/// `BATCH_SIZE` consecutive items before another sees any) and more
/// buffering wasted to in-flight batches when many consumers are active.
///
/// 128 was chosen by perf-profiling: at 32, `claim_batch` consumed ~37%
/// of cycles with the `lock cmpxchg` itself accounting for ~43% of those
/// cycles. Quadrupling the batch size cuts CAS frequency 4× while still
/// leaving room for 8 consumers on a 1024-slot ring without starving.
const BATCH_SIZE: usize = 128;

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        self.queue.consumer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
            cached_tail: Cell::new(0),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
        }
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send (each clone is single-
// threaded by contract). The queue Arc keeps the RingBuffer alive.
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Consumer<T> {
    pub(super) const fn new(queue: Arc<RingBuffer<T>>) -> Self {
        Self {
            queue,
            cached_tail: Cell::new(0),
            batch_next: Cell::new(0),
            batch_end: Cell::new(0),
        }
    }

    /// Claims the next position from the local batch, or claims a new
    /// batch via bounded CAS on `head` clamped by `tail`.
    #[inline]
    fn claim_pos(&self) -> Option<usize> {
        let next = self.batch_next.get();
        let end = self.batch_end.get();
        if next < end {
            self.batch_next.set(next + 1);
            return Some(next);
        }
        self.claim_batch()
    }

    #[cold]
    #[inline(never)]
    fn claim_batch(&self) -> Option<usize> {
        let q = &*self.queue;
        let mut backoff = 0u32;
        let mut head = q.head.load(Ordering::Relaxed);

        loop {
            let mut tail = self.cached_tail.get();
            if head >= tail {
                tail = q.tail.load(Ordering::Acquire);
                self.cached_tail.set(tail);
                if head >= tail {
                    if q.closed.0.load(Ordering::Acquire) {
                        let tail2 = q.tail.load(Ordering::Acquire);
                        self.cached_tail.set(tail2);
                        let head2 = q.head.load(Ordering::Relaxed);
                        if head2 >= tail2 {
                            return None;
                        }
                        head = head2;
                        continue;
                    }
                    return None;
                }
            }

            let avail = tail - head;
            let take = avail.min(BATCH_SIZE);
            let new_head = head + take;

            // Instrumentation: would the FAA-by-K fast path have fired?
            // Threshold: avail must absorb a full batch from every live
            // consumer plus our own (slack of +1 for clone-races).
            #[cfg(feature = "mpmc-instrument")]
            {
                let n = q.consumer_count.0.load(Ordering::Relaxed);
                let threshold = BATCH_SIZE.saturating_mul(n.saturating_add(1));
                if avail >= threshold {
                    q.instr_faa_eligible.0.fetch_add(1, Ordering::Relaxed);
                } else {
                    q.instr_cas_fallback.0.fetch_add(1, Ordering::Relaxed);
                }
            }

            match q
                .head
                .compare_exchange_weak(head, new_head, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.batch_next.set(head + 1);
                    self.batch_end.set(new_head);
                    return Some(head);
                }
                Err(actual) => {
                    head = actual;
                    crate::common::cas_backoff(&mut backoff);
                }
            }
        }
    }

    /// Pops an item from the ring buffer.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let pos = self.claim_pos()?;
        let q = &*self.queue;

        // Wait for the producer at `pos` to publish. Bounded CAS by
        // `tail` ensures the producer has at least *claimed* `pos`, but
        // multi-producer publication can be out of order — a producer
        // for `pos + k` may have raised `tail` before the producer for
        // `pos` has finished writing. Different consumers spin on
        // different slots, so there is no cross-consumer contention.
        let ready = q.ready_slot(pos);
        let mut backoff = 0u32;
        while ready.0.load(Ordering::Acquire) != pos + 1 {
            crate::common::cas_backoff(&mut backoff);
        }

        // SAFETY: ready[s] == pos + 1 confirms the producer at logical
        // position `pos` has published valid data. We have unique
        // ownership of this position via the bounded CAS claim.
        let data_ptr = q.data_slot(pos).get().cast_const();
        let val = unsafe { data_ptr.cast::<T>().read() };

        // Release via the consumer-write `done` line. The producer at
        // logical position `pos + cap` will Acquire-load `done[s] ==
        // pos + cap` and proceed.
        q.done_slot(pos).0.store(pos + q.cap, Ordering::Release);

        Some(val)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.closed.0.load(Ordering::Acquire)
    }

    /// Returns `(faa_eligible, cas_fallback)` claim_batch decisions
    /// recorded under the `mpmc-instrument` feature. Diagnostic only.
    #[cfg(feature = "mpmc-instrument")]
    #[must_use]
    pub fn instrument_counts(&self) -> (usize, usize) {
        self.queue.instrument_counts()
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.queue.consumer_count.fetch_sub(1, Ordering::Relaxed);
        // Release any positions claimed but never popped: drop the
        // value (producer published before bounded CAS let us claim)
        // and store `done[s] = pos + cap` so the producer at the next
        // round can reuse the slot.
        let q = &*self.queue;
        let cap = q.cap;
        let next = self.batch_next.get();
        let end = self.batch_end.get();
        for pos in next..end {
            // Mirror pop's per-slot ready spin: the producer for `pos`
            // may not have finished publishing when we claimed the
            // batch.
            let ready = q.ready_slot(pos);
            let mut backoff = 0u32;
            while ready.0.load(Ordering::Acquire) != pos + 1 {
                crate::common::cas_backoff(&mut backoff);
            }
            // SAFETY: data is initialized; bounded CAS made us the
            // unique owner of this position.
            unsafe {
                q.data_slot(pos).get().cast::<T>().drop_in_place();
            }
            q.done_slot(pos).0.store(pos + cap, Ordering::Release);
        }
    }
}
