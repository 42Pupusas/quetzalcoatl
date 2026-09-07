//! The blocking loops for the rings whose waiting side is one thread.
//!
//! Where a side has a single endpoint (spsc's producer and consumer,
//! spmc's producer, mpsc's consumer) a lone `SoleParker` answers "is
//! anyone waiting?", and the block-until-ready loop built on it is the
//! same in every case: try, check for a closed peer, spin, then arm,
//! re-check, and park. Only the ring fields differ, so the loops live
//! here once and implementors supply the primitives.
//!
//! Each loop re-checks *after* arming, and that repetition is the
//! point rather than caution. Arming and closing race across two
//! locations: the waiter stores "parked" then loads "closed", the
//! closer stores "closed" then loads "parked". Without a total order
//! over both, each can read the other's stale value — the closer sends
//! no wake, the waiter parks forever. The `SeqCst` store in `arm_park`
//! is not enough on its own because it does not order the `Acquire`
//! loads inside `try_pop`/`try_push` that follow, so each loop places
//! an explicit `SeqCst` fence between arming and the re-check.

use super::backoff::Backoff;
use std::sync::atomic::{fence, Ordering};

/// The blocking push for rings with exactly one producer.
pub trait SingleParkerProducer<T> {
    /// Non-blocking push; `Err(val)` hands the value back when full.
    fn try_push(&self, val: T) -> Result<(), T>;
    /// True once the consumer side is gone — stop blocking, return `Err`.
    ///
    /// Implementors must load the close flag with `SeqCst`; see the
    /// module docs.
    fn consumer_gone(&self) -> bool;
    /// Install this thread's park handle and publish "parked" (`SeqCst`).
    fn arm_park(&self);
    /// Clear the "parked" flag (`Relaxed`).
    fn disarm_park(&self);

    /// Push, blocking while the ring is full until a consumer frees
    /// space. Returns `Err(val)` only when the consumer side is gone.
    fn push_block(&self, mut val: T) -> Result<(), T> {
        let mut backoff = Backoff::new();
        loop {
            if self.consumer_gone() {
                return Err(val);
            }
            match self.try_push(val) {
                Ok(()) => return Ok(()),
                Err(returned) => val = returned,
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }
            self.arm_park();
            fence(Ordering::SeqCst);
            if self.consumer_gone() {
                self.disarm_park();
                return Err(val);
            }
            match self.try_push(val) {
                Ok(()) => {
                    self.disarm_park();
                    return Ok(());
                }
                Err(returned) => val = returned,
            }
            std::thread::park();
            self.disarm_park();
        }
    }
}

/// The blocking pop for rings with exactly one consumer.
pub trait SingleParkerConsumer<T> {
    /// Non-blocking pop.
    fn try_pop(&mut self) -> Option<T>;
    /// True once the producer side is gone — drain, then return `None`.
    ///
    /// Implementors must load the close flag with `SeqCst`; see the
    /// module docs.
    fn producer_gone(&self) -> bool;
    /// Install this thread's park handle and publish "parked" (`SeqCst`).
    fn arm_park(&self);
    /// Clear the "parked" flag (`Relaxed`).
    fn disarm_park(&self);

    /// Pop, blocking while the ring is empty until a producer
    /// publishes. Returns `None` only once the producer is gone and
    /// the ring has drained.
    fn pop_block(&mut self) -> Option<T> {
        let mut backoff = Backoff::new();
        loop {
            if let Some(v) = self.try_pop() {
                return Some(v);
            }
            if self.producer_gone() {
                // The producer may have published immediately before
                // its drop; that item is drained before None.
                return self.try_pop();
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }
            self.arm_park();
            fence(Ordering::SeqCst);
            if let Some(v) = self.try_pop() {
                self.disarm_park();
                return Some(v);
            }
            if self.producer_gone() {
                self.disarm_park();
                return self.try_pop();
            }
            std::thread::park();
            self.disarm_park();
        }
    }
}

/// The zero-copy blocking pop for rings with exactly one consumer.
///
/// The returned reader borrows the consumer, so the trait needs a GAT.
/// The pre-park gate is [`ready_for_claim`], never `try_pop_ref`: a
/// reader created only to test for readiness would advance the head
/// when dropped and consume the very item it was checking for.
///
/// The gate has to be strong enough that the claim after it cannot
/// fail. Merely reporting "the slot at head is not empty" is not: an
/// abandoned reservation answers yes, and the `try_pop_ref` that
/// follows releases it and reports an empty ring, turning a blocking
/// read on an open channel with live producers into `None`. So
/// [`ready_for_claim`] resolves tombstones itself and reports only a
/// genuine value. Nothing else advances the head in these topologies,
/// so a value the gate observed is still there for the claim.
///
/// [`ready_for_claim`]: Self::ready_for_claim
pub trait SingleParkerConsumerRef {
    /// The borrowing read guard (e.g. `SlotReader<'a, ..>`).
    type Reader<'a>
    where
        Self: 'a;

    /// Whether a value is ready to claim.
    ///
    /// Releases any abandoned positions it walks over — they free ring
    /// capacity, so skipping them is progress worth publishing — but
    /// never consumes a value, which is what keeps it usable as a
    /// pre-park gate.
    ///
    /// Implementors must guarantee that a `true` here makes the
    /// following [`try_pop_ref`](Self::try_pop_ref) return `Some`.
    fn ready_for_claim(&mut self) -> bool;
    /// Claim the ready item by reference. Only called immediately
    /// after `ready_for_claim` returned true.
    fn try_pop_ref(&mut self) -> Option<Self::Reader<'_>>;
    /// True once the producer side is gone.
    fn producer_gone(&self) -> bool;
    /// Install this thread's park handle and publish "parked" (`SeqCst`).
    fn arm_park(&self);
    /// Clear the "parked" flag (`Relaxed`).
    fn disarm_park(&self);

    /// Zero-copy pop, blocking until an item is ready. Returns `None`
    /// only once the producer is gone and the ring has drained.
    fn pop_ref_block(&mut self) -> Option<Self::Reader<'_>> {
        let mut backoff = Backoff::new();
        loop {
            if self.ready_for_claim() {
                return self.try_pop_ref();
            }
            if self.producer_gone() {
                if self.ready_for_claim() {
                    return self.try_pop_ref();
                }
                return None;
            }
            if backoff.spin_unless_exhausted() {
                continue;
            }
            self.arm_park();
            fence(Ordering::SeqCst);
            if self.ready_for_claim() {
                self.disarm_park();
                return self.try_pop_ref();
            }
            if self.producer_gone() {
                self.disarm_park();
                if self.ready_for_claim() {
                    return self.try_pop_ref();
                }
                return None;
            }
            std::thread::park();
            self.disarm_park();
        }
    }
}
