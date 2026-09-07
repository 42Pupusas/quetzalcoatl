use std::cell::Cell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::consumed_watermark::ConsumedTally;
use super::drain_wake::DrainWake;
use super::scan_budget::ScanBudget;
use super::slot_release::SlotRelease;
use super::PARK_BACKSTOP;
use super::{Config, DefaultConfig, RingBuffer};
use crate::common::backoff::Backoff;
#[cfg(feature = "async")]
use crate::common::park_registration::ParkRegistration;
use crate::common::park_registry::ParkSlot;

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
    consumed: ConsumedTally,
    /// Park slot leased for this consumer's lifetime, returned on drop.
    park_slot: ParkSlot,
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
        let stagger = self
            .queue
            .capacity
            .index_of(n * (self.queue.capacity.get() / 8).max(1));
        self.queue.consumer_count_live.register();
        Self {
            queue: Arc::clone(&self.queue),
            next_scan: Cell::new(stagger),
            consumed: ConsumedTally::new(),
            park_slot: self.queue.consumer_slots.lease(),
        }
    }
}

// SAFETY: Cell is !Sync, but Consumer is Send because each handle
// is single-threaded by contract. The Arc keeps the RingBuffer alive.
unsafe impl<T: Send, C: Config> Send for Consumer<T, C> {}

impl<T, C: Config> Consumer<T, C> {
    pub(super) fn new(queue: Arc<RingBuffer<T, C>>) -> Self {
        let park_slot = queue.consumer_slots.lease();
        Self {
            queue,
            next_scan: Cell::new(0),
            consumed: ConsumedTally::new(),
            park_slot,
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
        let start = self.next_scan.get();
        for offset in 0..q.capacity.get() {
            if q.ready_state(start + offset).is_published() {
                return true;
            }
        }
        false
    }

    /// Scans for a published slot and CAS-claims it. Returns the
    /// `(pos, round_pos)` of the claimed slot on success. After a
    /// successful claim the slot is claimed but not released, and the
    /// caller is responsible for reading the value and releasing the
    /// slot's `done` word for reuse.
    ///
    /// Updates `next_scan` and bumps `local_consumed` (with a flush
    /// to the shared watermark every `CONSUMED_FLUSH`) on success;
    /// on failure (full lap with nothing claimable) advances
    /// `next_scan` past the lap.
    #[inline]
    fn claim_slot(&self) -> Option<(usize, usize)> {
        let q = &*self.queue;
        let cap = q.capacity.get();

        let start_scan = self.next_scan.get();
        let mut budget = ScanBudget::new(start_scan, cap, C::CAS_FAIL_SKIP);

        loop {
            let scan = budget.position();
            let state = q.ready_state(scan);
            let round_pos = state.round_pos();

            if state.is_published() {
                if q.ready_slot(scan).claim(round_pos) {
                    // Advance scan past the consumed slot, keeping
                    // it monotonic in case we claimed a future slot.
                    self.next_scan.set((round_pos + 1).max(start_scan + 1));

                    self.consumed.record(&q.consumed, C::CONSUMED_FLUSH);

                    return Some((scan, round_pos));
                }
                // CAS lost — move clear so the contended line cools
                // before the next attempt.
                budget.skip_contended();
            }

            budget.step();
            if budget.exhausted() {
                self.next_scan.set(budget.position());
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
        // producer committed the value before publishing.
        let val = unsafe { q.data_slot(pos).get().cast::<T>().read() };
        q.done_slot(pos).release(round_pos, q.capacity);
        // Wake one parked producer if any.
        q.producer_park.wake_one();
        q.notify_producers();
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
    ///
    /// The wake is batched into a single `wake_n`: with many parked
    /// producers (one per next-round slot released) it frees up to
    /// `count` of them, where a `wake_one` per item would strand the
    /// rest until the next pop/drain. It is owned by a guard so that a
    /// panicking `f` still delivers it; see [`DrainWake`].
    pub fn drain(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut wake = DrainWake::new(&*self.queue);
        let mut count = 0usize;
        while let Some((pos, round_pos)) = self.claim_slot() {
            let q = &*self.queue;
            // SAFETY: claim_slot's CAS gave us unique ownership of
            // (pos, round_pos). Producer committed before publishing.
            let val = unsafe { q.data_slot(pos).get().cast::<T>().read() };
            q.done_slot(pos).release(round_pos, q.capacity);
            wake.released();
            count += 1;
            f(val);
        }
        count
    }

    /// Drains up to `limit` items, calling `f` for each. Returns
    /// the number drained. Useful for fairness in multi-source
    /// consumer loops.
    ///
    /// Panic behaviour matches [`drain`](Self::drain).
    pub fn drain_up_to(&mut self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let mut wake = DrainWake::new(&*self.queue);
        let mut count = 0usize;
        while count < limit {
            let Some((pos, round_pos)) = self.claim_slot() else {
                break;
            };
            let q = &*self.queue;
            let val = unsafe { q.data_slot(pos).get().cast::<T>().read() };
            q.done_slot(pos).release(round_pos, q.capacity);
            wake.released();
            count += 1;
            f(val);
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
        let slot = self.park_slot;
        let mut backoff = Backoff::new();
        let mut watch = q.watch_backstop_consumer();
        loop {
            if let Some(v) = self.pop() {
                watch.made_progress();
                return Some(v);
            }
            // Empty AND closed AND really nothing left → done.
            if q.closed.is_closed() {
                if let Some(v) = self.pop() {
                    return Some(v);
                }
                return None;
            }
            // Spin until the schedule fully escalates (~tens of μs
            // including yields) before paying for park.
            if backoff.spin_unless_exhausted() {
                continue;
            }

            // The SeqCst fetch_or inside arm pairs with the producer's
            // `consumer_park.wake.load` after `ready.store(Release)`:
            // either we see a published slot in the re-check below, or
            // the producer sees our bit and unparks us.
            q.consumer_park.arm(slot);
            // Fence so the recheck below is totally ordered with the
            // producer's `ready.store(Release)`. Without it the recheck
            // can be hoisted above our SeqCst fetch_or in the
            // modification order, hitting the same lost-wake race we
            // patched in producer's park_until_slot_free.
            std::sync::atomic::fence(Ordering::SeqCst);

            if let Some(v) = self.pop() {
                watch.made_progress();
                q.consumer_park.disarm(slot);
                return Some(v);
            }
            if q.closed.is_closed_for_parking() {
                q.consumer_park.disarm(slot);
                return self.pop();
            }

            // Bounded park backstop — see Producer::push_block.
            watch.about_to_park(q.consumer_park.others_parked(slot), self.has_item());
            slot.park_bounded(PARK_BACKSTOP);
            // A wake claims the handle, so one still armed means the
            // sleep ended on the timeout instead.
            watch.parked(
                q.consumer_park.is_armed(slot),
                q.consumer_park.others_parked(slot),
            );
            q.consumer_park.disarm(slot);
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
        let mut parked = ParkRegistration::new(&self.queue.consumer_waker, self.park_slot);
        std::future::poll_fn(move |cx| {
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.queue.closed.is_closed() {
                return Poll::Ready(self.pop());
            }
            parked.arm(cx);
            if let Some(v) = self.pop() {
                return Poll::Ready(Some(v));
            }
            if self.queue.closed.is_closed() {
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
        let park_slot = self.park_slot;
        let mut backoff = Backoff::new();
        let mut watch = self.queue.watch_backstop_consumer();
        loop {
            // Non-mutating gate: avoid CAS-claiming a slot that the
            // discarded SlotReader would then have to release.
            if self.has_item() {
                if let Some(claimed) = self.claim_slot() {
                    watch.made_progress();
                    return Some(self.reader_for(claimed));
                }
                backoff.reset();
                continue;
            }
            if self.queue.closed.is_closed() {
                if let Some(claimed) = self.claim_slot() {
                    return Some(self.reader_for(claimed));
                }
                // Closed and nothing claimable: a peer took whatever
                // `has_item` may have seen, and no producer will publish
                // again, so the ring is drained for this consumer.
                return None;
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }

            self.queue.consumer_park.arm(park_slot);
            // See pop_block: pairs with WakeSet::wake_one's fence.
            std::sync::atomic::fence(Ordering::SeqCst);

            if self.has_item() {
                self.queue.consumer_park.disarm(park_slot);
                if let Some(claimed) = self.claim_slot() {
                    watch.made_progress();
                    return Some(self.reader_for(claimed));
                }
                backoff.reset();
                continue;
            }
            // SeqCst: post-arm half of the close handshake, paired
            // with the SeqCst fetch_or on the park bitmask above.
            if self.queue.closed.is_closed_for_parking() {
                self.queue.consumer_park.disarm(park_slot);
                if let Some(claimed) = self.claim_slot() {
                    return Some(self.reader_for(claimed));
                }
                return None;
            }

            // Bounded park backstop — see Consumer::pop_block.
            watch.about_to_park(
                self.queue.consumer_park.others_parked(park_slot),
                self.has_item(),
            );
            park_slot.park_bounded(PARK_BACKSTOP);
            // A wake claims the handle, so one still armed means the
            // sleep ended on the timeout instead.
            watch.parked(
                self.queue.consumer_park.is_armed(park_slot),
                self.queue.consumer_park.others_parked(park_slot),
            );
            self.queue.consumer_park.disarm(park_slot);
        }
    }

    /// Best-effort approximate queue length. Not precise — this
    /// design doesn't maintain a cheap exact `len`.
    #[must_use]
    pub fn approx_len(&self) -> usize {
        let claim = self.queue.claim.load(Ordering::Relaxed);
        let scan = self.next_scan.get();
        claim.wrapping_sub(scan).min(self.queue.capacity.get())
    }

    /// Returns `true` once the last [`Producer`](super::Producer)
    /// has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.queue.closed.is_closed()
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
        self.consumed.flush(&self.queue.consumed);

        self.queue.consumer_slots.release(self.park_slot);

        // Last-consumer drop: flag `consumer_closed` and wake any
        // producers parked in `push_block` so they can observe it
        // and return Err.
        if self.queue.consumer_count_live.release() {
            // SeqCst: pairs with each producer's post-arm SeqCst load.
            self.queue.consumer_closed.close();
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
