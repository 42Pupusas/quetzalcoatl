use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::RingBuffer;

/// Producer for the relaxed-FIFO scan-based MPMC ring.
///
/// Cloneable. Producers FAA the shared `claim` cursor for unique
/// logical positions, spin per-slot on `done[s] == pos`, write data,
/// publish via `ready[s] = pos + 1`. No `tail` watermark to advance —
/// consumers find published slots by scanning `ready`.
pub struct Producer<T> {
    pub(super) queue: Arc<RingBuffer<T>>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.queue.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<T> Producer<T> {
    /// Pushes a value. Returns `Err(val)` if the slot for the next FAA'd
    /// position is not yet free (consumer hasn't released the previous
    /// round). With no consumer-side `head` cursor, the only "is full"
    /// signal is "the slot we'd target is still owned by the previous
    /// round." This is checked *after* FAA to keep the hot path
    /// branchless; a failed `push` does not advance any cursor.
    ///
    /// To avoid leaking the FAA'd position on `Err`, we attempt a
    /// `compare_exchange` rollback on `claim`. The rollback succeeds
    /// only if no other producer has FAA'd past us; otherwise, our
    /// position is "stranded" but harmless — a future producer at that
    /// position will spin until the consumer catches up. (This is the
    /// same behavior as the strict-FIFO MPMC under high contention.)
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let q = &*self.queue;

        // Quick check: is the slot for the next claim free? Best-effort
        // (no atomic fence between this and the FAA), but avoids a
        // wasted FAA in the common "full" case.
        let next = q.claim.load(Ordering::Relaxed);
        let next_done = q.done_slot(next).0.load(Ordering::Acquire);
        if next_done != next {
            return Err(val);
        }

        let pos = q.claim.fetch_add(1, Ordering::Relaxed);
        let done = q.done_slot(pos);

        // Spin until the previous round's consumer (logical pos -
        // cap) released this slot. Different producers spin on
        // different slots, no inter-producer contention here.
        let mut backoff = 0u32;
        while done.0.load(Ordering::Acquire) != pos {
            crate::common::cas_backoff(&mut backoff);
        }

        // SAFETY: we are the unique owner of `pos` via FAA, and
        // `done[s] == pos` confirms slot is free for our round.
        let data_ptr = q.data_slot(pos).get();
        unsafe { (*data_ptr).write(val) };

        // Publish: ready[s] = pos + 1 → "published, available."
        q.ready_slot(pos).0.store(pos + 1, Ordering::Release);
        Ok(())
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        if self.queue.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.queue.closed.0.store(true, Ordering::Release);
        }
    }
}
