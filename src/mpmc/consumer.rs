use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::slot_release::SlotRelease;
use super::{Config, DefaultConfig, RingBuffer};
use crate::common::park::{BACKOFF_PARK_THRESHOLD, PARK_MASK};

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

    /// Non-mutating check: returns true if there's an item this
    /// consumer would likely claim on the next `pop`/`pop_ref`.
    /// Used as a pre-park gate so we don't CAS-claim a slot we'd
    /// then have to release on `SlotReader::drop`.
    ///
    /// Scans up to one full lap looking for any slot in state 1
    /// (published, unclaimed). Doesn't update `next_scan` or take
    /// any locks — purely an Acquire load per slot. May see false
    /// negatives under contention (another consumer claims the
    /// slot between this scan and our retry); benign — we just
    /// re-park.
    #[inline]
    fn has_item(&self) -> bool {
        let q = &*self.queue;
        let mask = q.mask;
        let start = self.next_scan.get();
        for offset in 0..q.cap {
            let scan = start + offset;
            let s = scan & mask;
            let r = q.ready_slot(scan).load(Ordering::Acquire);
            let delta = r.wrapping_sub(s);
            let state = delta & mask;
            if state == 1 {
                return true;
            }
        }
        false
    }

    /// Scans for a published slot and CAS-claims it. Returns the
    /// `(pos, round_pos)` of the claimed slot on success. After a
    /// successful claim the slot is in state "claimed but not
    /// released" (`ready[s] = round_pos + 2`) and the caller is
    /// responsible for reading the value and storing
    /// `done[s] = round_pos + cap` to release the slot for reuse.
    ///
    /// Updates `next_scan` and bumps `local_consumed` (with a flush
    /// to the shared watermark every `CONSUMED_FLUSH`) on success;
    /// on failure (full lap with nothing claimable) advances
    /// `next_scan` past the lap.
    #[inline]
    fn claim_slot(&self) -> Option<(usize, usize)> {
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

                    return Some((scan, round_pos));
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

    /// Pops the first published slot found at or after `next_scan`,
    /// returning `None` after one full lap of the ring without
    /// finding any published item.
    ///
    /// Items are returned in the order slots become published, not
    /// in producer push order — see the module docs.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let (pos, round_pos) = self.claim_slot()?;
        let q = &*self.queue;
        // SAFETY: the successful CAS in claim_slot gave us unique
        // ownership of slot `pos & mask` for this round. The
        // producer committed the value before storing state=1.
        let val = unsafe { q.data_slot(pos).get().cast::<T>().read() };
        // SeqCst (not Release) on done.store: drains the store buffer
        // so the SeqCst load of `producer_park.wake` inside wake_one
        // can't be satisfied before our store reaches global
        // visibility. wake_one's leading SeqCst fence is theoretically
        // equivalent, but writing the SeqCst on the store keeps the
        // pairing local to the call site.
        q.done_slot(pos).store(round_pos + q.cap, Ordering::SeqCst);
        // Wake one parked producer if any.
        q.producer_park.wake_one();
        #[cfg(feature = "async")]
        q.wake_producer_async();
        Some(val)
    }

    /// Returns a zero-copy read reference to the next published
    /// item, returning `None` after one full lap of the ring without
    /// finding any published item.
    ///
    /// Unlike [`pop`](Self::pop), this does not copy the data out.
    /// It returns a [`SlotReader`] that dereferences to `&T`. The
    /// slot is held in state "claimed but not released" until the
    /// reader is dropped — the producer at the corresponding next-
    /// round position will block until then. Keep the reader's
    /// lifetime short to avoid stalling producers.
    ///
    /// Items are returned in the order slots become published.
    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T, C>> {
        let claimed = self.claim_slot()?;
        Some(self.reader_for(claimed))
    }

    /// Wraps an already-claimed `(pos, round_pos)` in its reader.
    ///
    /// Separating this from the claim lets the blocking path distinguish
    /// a lost claim from an empty ring, which returning `Option<Reader>`
    /// from one call cannot: the reader borrows `self`, so the caller
    /// cannot retry after a `None`.
    #[inline]
    const fn reader_for(&mut self, claimed: (usize, usize)) -> SlotReader<'_, T, C> {
        let (pos, round_pos) = claimed;
        SlotReader {
            consumer: self,
            pos,
            round_pos,
        }
    }

    /// Drains all currently-claimable items, calling `f` for each.
    /// Returns the number drained.
    ///
    /// Each item still requires a CAS-claim (mpmc's slot ownership
    /// is per-slot, not via a shared head), so drain doesn't avoid
    /// CAS traffic. The wins are: (1) one `wake_n(count)` for the
    /// whole batch instead of `count` `wake_one` calls, and (2) a
    /// tighter inner loop with no Option wrapping per item.
    ///
    /// "Currently-claimable" means: slots with `state == 1` at or
    /// after `next_scan`, up to one full lap. May terminate early
    /// if other consumers race in and claim items first.
    pub fn drain(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut count = 0usize;
        while let Some((pos, round_pos)) = self.claim_slot() {
            let q = &*self.queue;
            // SAFETY: claim_slot's CAS gave us unique ownership of
            // (pos, round_pos). Producer committed before publishing.
            let val = unsafe { q.data_slot(pos).get().cast::<T>().read() };
            // SeqCst (see Consumer::pop): drains the store buffer so
            // the upcoming wake_n's wake.load cannot miss a parked
            // producer bit set just after our store landed.
            q.done_slot(pos).store(round_pos + q.cap, Ordering::SeqCst);
            count += 1;
            f(val);
        }
        if count > 0 {
            // Single batched wake. With many parked producers (one
            // per next-round slot we just released), `wake_n(count)`
            // releases up to `count` of them — each `wake_one` would
            // strand the rest until the next pop/drain.
            self.queue.producer_park.wake_n(count);
            #[cfg(feature = "async")]
            self.queue.wake_producer_async_n(count);
        }
        count
    }

    /// Drains up to `limit` items, calling `f` for each. Returns
    /// the number drained. Useful for fairness in multi-source
    /// consumer loops.
    pub fn drain_up_to(&mut self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let mut count = 0usize;
        while count < limit {
            let Some((pos, round_pos)) = self.claim_slot() else {
                break;
            };
            let q = &*self.queue;
            let val = unsafe { q.data_slot(pos).get().cast::<T>().read() };
            // SeqCst — see Consumer::pop.
            q.done_slot(pos).store(round_pos + q.cap, Ordering::SeqCst);
            count += 1;
            f(val);
        }
        if count > 0 {
            self.queue.producer_park.wake_n(count);
            #[cfg(feature = "async")]
            self.queue.wake_producer_async_n(count);
        }
        count
    }

    /// Drains items, blocking when empty, until the ring is closed
    /// AND drained. Calls `f` for each item. Returns the total count.
    ///
    /// Combines [`drain`](Self::drain)'s batched wake with
    /// [`pop_block`](Self::pop_block)'s park-on-empty protocol.
    pub fn drain_block(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut total = 0usize;
        loop {
            total += self.drain(&mut f);
            // pop_block re-checks close after wake; if close + empty
            // it returns None and we exit. Otherwise we got one more
            // item and can resume draining (typically batches resume
            // in the next drain() pass).
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
    /// ring is empty until a producer publishes one. Returns
    /// `None` only when the ring is closed AND empty (no live
    /// producers, nothing left to drain).
    ///
    /// Uses the same futex-style wake bitmap as the producer slow
    /// path: spins briefly first, then sets a wake bit and parks.
    /// Producers signal after every `ready[s].store(Release)`,
    /// gated on a single `Relaxed` load so the no-park hot path
    /// stays cheap. Close paths flush the wake set, so a parked
    /// consumer always either sees a publish in its re-check or is
    /// unparked by a producer/close.
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
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            q.consumer_park.ensure_handle_installed(self.park_slot);
            // SeqCst pairs with producer's `consumer_park.wake.load`
            // after `ready.store(Release)`: either we see a
            // published slot in the re-check below, or the
            // producer sees our bit and unparks us.
            q.consumer_park.wake.fetch_or(bit_mask, Ordering::SeqCst);
            // Fence so the recheck below is totally ordered with the
            // producer's `ready.store(Release)`. Without it the recheck
            // can be hoisted above our SeqCst fetch_or in the
            // modification order, hitting the same lost-wake race we
            // patched in producer's park_until_slot_free.
            std::sync::atomic::fence(Ordering::SeqCst);

            if let Some(v) = self.pop() {
                q.consumer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return Some(v);
            }
            // SeqCst on closed.load forces a total order with the last-
            // producer drop's `closed.store`: same Acquire-Release race
            // as broadcast — same-address pairing is theoretically
            // sufficient but x86 TSO under stress permits the recheck
            // to observe stale `closed=false`, causing an indefinite
            // park if no further publish follows.
            if q.closed.0.load(Ordering::SeqCst) {
                q.consumer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
                return self.pop();
            }

            // park_timeout backstop — see Producer::push_block.
            std::thread::park_timeout(std::time::Duration::from_millis(1));
            q.consumer_park.wake.fetch_and(!bit_mask, Ordering::Relaxed);
        }
    }

    /// Pops a value asynchronously, yielding to the executor when the
    /// ring is empty until a producer publishes one. Returns `None`
    /// when the ring is closed AND empty (no live producers, nothing
    /// left to drain).
    ///
    /// The future is cancel-safe: dropping it before completion does
    /// not consume any item.
    ///
    /// Each consumer clone has its own park slot so multiple async
    /// consumers can wait concurrently without contending on a single
    /// waker entry.
    #[cfg(feature = "async")]
    #[allow(clippy::future_not_send)]
    pub fn pop_async(&self) -> impl std::future::Future<Output = Option<T>> + '_ {
        let slot = self.park_slot;
        std::future::poll_fn(move |cx| {
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            self.queue.consumer_waker.register(slot, cx);
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                return Poll::Ready(self.pop());
            }
            Poll::Pending
        })
    }

    /// Returns a zero-copy read reference to the next item, blocking
    /// the calling thread when the ring is empty until a producer
    /// publishes one. Returns `None` only when the ring is closed
    /// AND empty (no live producers, nothing left to drain).
    ///
    /// Same wait protocol as [`pop_block`](Self::pop_block). Useful
    /// when you want zero-copy reads plus blocking.
    /// A claim is what proves ownership; `has_item` only reports that
    /// some slot looked published. With several consumers competing,
    /// a peer can claim that slot between the two, and returning the
    /// resulting `None` would end a blocking read on an open, non-empty
    /// ring. A lost claim is therefore contention, not emptiness: the
    /// loop restarts.
    #[must_use]
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T, C>> {
        let bit_mask = 1u64 << self.park_slot;
        let park_slot = self.park_slot;
        let mut backoff = 0u32;
        loop {
            // Non-mutating gate: avoid CAS-claiming a slot that the
            // discarded SlotReader would then have to release.
            if self.has_item() {
                if let Some(claimed) = self.claim_slot() {
                    return Some(self.reader_for(claimed));
                }
                backoff = 0;
                continue;
            }
            if self.queue.closed.0.load(Ordering::Acquire) {
                if let Some(claimed) = self.claim_slot() {
                    return Some(self.reader_for(claimed));
                }
                // Closed and nothing claimable: a peer took whatever
                // `has_item` may have seen, and no producer will publish
                // again, so the ring is drained for this consumer.
                return None;
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            self.queue.consumer_park.ensure_handle_installed(park_slot);
            self.queue
                .consumer_park
                .wake
                .fetch_or(bit_mask, Ordering::SeqCst);
            // See pop_block: pairs with WakeSet::wake_one's fence.
            std::sync::atomic::fence(Ordering::SeqCst);

            if self.has_item() {
                self.queue
                    .consumer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                if let Some(claimed) = self.claim_slot() {
                    return Some(self.reader_for(claimed));
                }
                backoff = 0;
                continue;
            }
            // SeqCst: post-arm half of the close handshake, paired
            // with the SeqCst fetch_or on the park bitmask above.
            if self.queue.closed.0.load(Ordering::SeqCst) {
                self.queue
                    .consumer_park
                    .wake
                    .fetch_and(!bit_mask, Ordering::Relaxed);
                if let Some(claimed) = self.claim_slot() {
                    return Some(self.reader_for(claimed));
                }
                return None;
            }

            // park_timeout backstop — see Consumer::pop_block.
            std::thread::park_timeout(std::time::Duration::from_millis(1));
            self.queue
                .consumer_park
                .wake
                .fetch_and(!bit_mask, Ordering::Relaxed);
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
            // SeqCst: pairs with each producer's post-arm SeqCst load.
            self.queue.consumer_closed.0.store(true, Ordering::SeqCst);
            self.queue.producer_park.flush();
            #[cfg(feature = "async")]
            self.queue.producer_waker.flush();
        }
    }
}

/// A zero-copy read reference to an item in an MPMC ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`,
/// allowing direct reads from the slot without copying.
///
/// While this reader exists the slot is in state "claimed but not
/// released": the producer for the corresponding next-round position
/// will block until the reader is dropped. Keep the reader's
/// lifetime short under contention to avoid stalling producers.
///
/// On drop: drops the value in place, stores
/// `done[s] = round_pos + cap` to release the slot, and wakes one
/// parked producer (if any).
pub struct SlotReader<'a, T, C: Config = DefaultConfig> {
    consumer: &'a mut Consumer<T, C>,
    pos: usize,
    round_pos: usize,
}

impl<T, C: Config> std::ops::Deref for SlotReader<'_, T, C> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: claim_slot's CAS gave us unique ownership of this
        // (slot, round) pair, and the producer committed the value
        // before publishing state=1.
        unsafe { (*self.consumer.queue.data_slot(self.pos).get()).assume_init_ref() }
    }
}

impl<T, C: Config> Drop for SlotReader<'_, T, C> {
    fn drop(&mut self) {
        let q = &*self.consumer.queue;
        // The release is armed before the value is dropped so that a
        // panicking `T::drop` still hands this slot back. Leaving it
        // "claimed but not released" would make `RingBuffer::drop` drop
        // the same value again.
        let _release = SlotRelease::new(q, self.pos, self.round_pos);
        // SAFETY: we hold the slot exclusively (state == 2 since
        // claim_slot's CAS); the value is initialized.
        unsafe {
            q.data_slot(self.pos).get().cast::<T>().drop_in_place();
        }
    }
}
