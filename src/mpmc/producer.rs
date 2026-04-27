use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

/// The producer side of an MPMC ring buffer.
///
/// Cloneable — each clone shares the underlying buffer and claims slots
/// via FAA on `claim`, eliminating inter-producer cache-line contention
/// on the claim cursor.
///
/// On the last producer's drop, the queue's `closed` flag is set so
/// consumers can distinguish "transiently empty" from "permanently
/// drained."
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        // Track live producers so the last drop can mark `closed`.
        // Relaxed: the count is only inspected at drop, where the Drop
        // path itself uses AcqRel via fetch_sub.
        self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<T> Producer<T> {
    /// Pushes a value into the ring buffer.
    ///
    /// Multiple producers can push concurrently. Returns `Err(val)` if
    /// the buffer is full from this producer's perspective (consumer
    /// hasn't released a slot for the current round).
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let q = &*self.queue;

        // Pre-check: would claiming a new position overshoot the ring?
        // We compare claim - head against cap. This is racy (head may
        // have advanced), but a stale value just makes the buffer look
        // fuller than it is — safe to refresh-and-retry.
        let claim = q.claim.load(Ordering::Relaxed);
        let head = q.head.load(Ordering::Acquire);
        if claim.wrapping_sub(head) >= q.cap {
            return Err(val);
        }

        // Claim a unique position via FAA. Unlike CAS, this always
        // succeeds on the first try.
        let pos = q.claim.fetch_add(1, Ordering::Relaxed);

        // Verify the slot is free for our round. `done[s] == pos` means
        // the previous round's consumer at logical position `pos - cap`
        // released this slot for us (or, on the first lap, this is the
        // initial state since `done[s]` is initialized to `s`).
        //
        // If the slot is not yet free, our pre-check was stale — a
        // concurrent producer raced ahead of us and claimed past
        // capacity. Spin until the consumer catches up. Different
        // producers spin on different slots (different cache lines), so
        // there is no cross-producer contention here.
        let done = q.done_slot(pos);
        let mut backoff = 0u32;
        while done.0.load(Ordering::Acquire) != pos {
            // If the queue closed under us we still need to publish so
            // we don't leak the FAA-claimed position; but consumers
            // can't make progress past our pos until we publish, so
            // closure mid-spin can't actually happen for a producer
            // (only Producer::drop sets closed, and we're a producer).
            crate::common::cas_backoff(&mut backoff);
        }

        // SAFETY: We are the unique owner of this position via FAA, and
        // `done[s] == pos` confirms the previous round's consumer has
        // released the slot. No other producer or consumer touches the
        // data slot until we publish `ready[s]`.
        let data_ptr = q.data_slot(pos).get();
        unsafe { (*data_ptr).write(val) };

        // Publish: ready[s] = pos + 1 tells the consumer at logical
        // position `pos` that data is available.
        q.ready_slot(pos).0.store(pos + 1, Ordering::Release);

        // Advance the loose published-watermark. Consumers Acquire-load
        // tail to bound their batched CAS claim. Per-slot ready is the
        // authoritative publication signal, so tail can lag a published
        // position without correctness impact — it just means consumers
        // briefly under-bound their batch.
        q.advance_tail(pos + 1);

        Ok(())
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
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // The last producer to drop sets `closed`. AcqRel on the
        // decrement gives us a happens-before with the matching final
        // decrement on any other producer's drop.
        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.queue.closed.0.store(true, Ordering::Release);
        }
    }
}
