use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::batch_release::BatchRelease;
use super::slot_release::SlotRelease;
use super::RingBuffer;
#[cfg(feature = "async")]
use crate::common::park_registration::{ParkSite, ParkedFuture};
#[cfg(feature = "async")]
use crate::common::park_registry::ParkSlot;
#[cfg(feature = "async")]
use crate::common::wake_async::WakerSet;
use crate::common::{SeqSlot, SlotSequence};

/// The consumer side of an MPSC ring buffer.
///
/// Generic over `R`: the ring reference. Defaults to `Arc<RingBuffer<T>>`
/// (owned split via [`RingBuffer::split`]). When `R = &'a RingBuffer<T>`
/// (borrowed split via [`RingBuffer::split_borrowed`]), `T` may carry
/// lifetimes shorter than `'static`.
///
/// Not cloneable — only one consumer exists per buffer.
///
/// Drains remaining items when dropped.
pub struct Consumer<T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    pub(super) queue: R,
}

// SAFETY: Consumer holds exclusive read access. R is Send, RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Consumer<T, R> {}

impl<T, R: Deref<Target = RingBuffer<T>>> Consumer<T, R> {
    #[inline]
    fn ring(&self) -> &RingBuffer<T> {
        &self.queue
    }

    /// The value at the head, with its position and slot, after
    /// releasing any abandoned positions in the way.
    ///
    /// Skipping an abandoned position frees a slot exactly as consuming
    /// a value does, so the batch that skips publishes `head` and wakes
    /// the producers when it drops, before this returns. Leaving the
    /// wake out stranded the capacity until an unrelated pop happened
    /// to come along.
    #[inline]
    fn head_value(&self) -> Option<(usize, &SeqSlot<T>, *const MaybeUninit<T>)> {
        let ring = self.ring();
        // SAFETY: this is the ring's only consumer, and every caller is
        // reached through `&mut self`. `settle` moves nothing out.
        let mut batch = unsafe { BatchRelease::new(ring, ring.ledger.head()) };
        let (slot, data_ptr) = batch.settle()?;
        Some((batch.head(), slot, data_ptr))
    }

    /// Whether a value sits at the head, releasing any abandoned
    /// positions in the way.
    ///
    /// A tombstone is not an item. Reporting it as one made
    /// `pop_ref_block` claim, find nothing, and return `None` from a
    /// blocking read while producers were still live.
    #[inline]
    fn ready_for_claim(&self) -> bool {
        self.head_value().is_some()
    }

    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        let (head, slot, data_ptr) = self.head_value()?;

        // SAFETY: the slot is published at `head`, so it is initialized
        // and synchronized via the Acquire load that classified it.
        let val = unsafe { (*data_ptr).assume_init_read() };

        slot.sequence.release(head, self.ring().capacity);

        // The credit is added with a locked instruction, which drains
        // the store buffer: the `sequence` store above is visible
        // before the bitmap load inside `wake_one_published`. That
        // makes `wake_one`'s leading fence redundant, so we pay one
        // barrier per pop instead of two.
        self.ring().ledger.publish_head(head.wrapping_add(1));

        self.ring().producer_park.wake_one_published();
        self.ring().notify_producers();

        Some(val)
    }

    #[inline]
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T, R>> {
        let (head, slot, data_ptr) = self.head_value()?;
        let seq_ptr = &raw const slot.sequence;

        Some(SlotReader {
            data_ptr,
            seq_ptr,
            consumer: self,
            head,
        })
    }

    /// Drains all currently available items, calling `f` for each.
    /// Returns the number of values delivered.
    ///
    /// Tombstoned positions are skipped and counted as released even
    /// though they deliver nothing, so a batch that finds only
    /// tombstones still publishes `head` and hands the space back.
    ///
    /// If `f` panics, the positions already taken stay consumed: the
    /// cursor is published while unwinding.
    pub fn drain(&mut self, mut f: impl FnMut(T)) -> usize {
        let ring = self.ring();
        // SAFETY: `&mut self` is the ring's only consumer, and stays
        // borrowed for as long as the batch lives.
        let mut batch = unsafe { BatchRelease::new(ring, ring.ledger.head()) };
        while let Some(val) = batch.take() {
            f(val);
        }
        batch.delivered()
    }

    /// Drains up to `limit` items, calling `f` for each. Returns the
    /// number of values delivered.
    ///
    /// The limit counts delivered values; skipped tombstones are not
    /// charged against it. Panic and release behaviour match
    /// [`drain`](Self::drain).
    pub fn drain_up_to(&mut self, limit: usize, mut f: impl FnMut(T)) -> usize {
        let ring = self.ring();
        // SAFETY: as in `drain`.
        let mut batch = unsafe { BatchRelease::new(ring, ring.ledger.head()) };
        while batch.delivered() < limit {
            let Some(val) = batch.take() else { break };
            f(val);
        }
        batch.delivered()
    }

    pub fn drain_block(&mut self, mut f: impl FnMut(T)) -> usize {
        let mut total = 0usize;
        loop {
            total += self.drain(&mut f);
            match self.pop_block() {
                Some(v) => {
                    total += 1;
                    f(v);
                }
                None => return total,
            }
        }
    }

    #[must_use]
    pub fn pop_block(&mut self) -> Option<T> {
        crate::common::SingleParkerConsumer::pop_block(self)
    }

    #[cfg(feature = "async")]
    pub fn pop_async(&mut self) -> impl std::future::Future<Output = Option<T>> + '_ {
        ParkedFuture::new(self, ParkSlot::SOLE, |this, parker, cx| {
            if let Some(v) = this.pop() {
                return Poll::Ready(Some(v));
            }
            if this.ring().closed.is_closed() {
                return Poll::Ready(this.pop());
            }
            parker.arm(&this.ring().consumer_waker, cx);
            if let Some(v) = this.pop() {
                return Poll::Ready(Some(v));
            }
            if this.ring().closed.is_closed() {
                return Poll::Ready(this.pop());
            }
            Poll::Pending
        })
    }

    #[must_use]
    pub fn pop_ref_block(&mut self) -> Option<SlotReader<'_, T, R>> {
        crate::common::SingleParkerConsumerRef::pop_ref_block(self)
    }

    /// Returns `true` once the last [`Producer`](super::Producer)
    /// has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.ring().closed.is_closed()
    }

    /// Returns the number of items currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ring().len()
    }

    /// Returns `true` if the buffer contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ring().is_empty()
    }

    /// Returns `true` if the buffer is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.ring().is_full()
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> crate::common::SingleParkerConsumer<T>
    for Consumer<T, R>
{
    fn try_pop(&mut self) -> Option<T> {
        self.pop()
    }
    fn producer_gone(&self) -> bool {
        // SeqCst: post-arm half of the close handshake.
        self.ring().closed.is_closed_for_parking()
    }
    fn arm_park(&self) {
        self.ring().consumer_park.arm();
    }
    fn disarm_park(&self) {
        self.ring().consumer_park.disarm();
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> crate::common::SingleParkerConsumerRef
    for Consumer<T, R>
{
    type Reader<'a>
        = SlotReader<'a, T, R>
    where
        Self: 'a;
    fn ready_for_claim(&mut self) -> bool {
        Self::ready_for_claim(self)
    }
    fn try_pop_ref(&mut self) -> Option<SlotReader<'_, T, R>> {
        self.pop_ref()
    }
    fn producer_gone(&self) -> bool {
        // SeqCst: post-arm half of the close handshake.
        self.ring().closed.is_closed_for_parking()
    }
    fn arm_park(&self) {
        self.ring().consumer_park.arm();
    }
    fn disarm_park(&self) {
        self.ring().consumer_park.disarm();
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Consumer<T, R> {
    fn drop(&mut self) {
        // SeqCst: pairs with each producer's post-arm SeqCst load of
        // consumer_closed, ordering it against their SeqCst fetch_or on
        // the park bitmask.
        self.ring().consumer_closed.close();
        self.ring().producer_park.flush();
        #[cfg(feature = "async")]
        self.ring().producer_waker.flush();
        while self.pop().is_some() {}
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
#[cfg(feature = "async")]
impl<T, R: Deref<Target = RingBuffer<T>>> ParkSite for Consumer<T, R> {
    fn park_set(&mut self) -> &WakerSet {
        &self.queue.consumer_waker
    }
}

/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without copying.
///
/// When dropped, drops the `T` value, releases the slot by updating
/// the sequence number, and advances the head pointer.
pub struct SlotReader<'a, T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    data_ptr: *const MaybeUninit<T>,
    seq_ptr: *const SlotSequence,
    consumer: &'a mut Consumer<T, R>,
    head: usize,
}

impl<T, R: Deref<Target = RingBuffer<T>>> std::ops::Deref for SlotReader<'_, T, R> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: pop_ref classified the slot as Ready at `head` (Acquire).
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for SlotReader<'_, T, R> {
    fn drop(&mut self) {
        // The release is armed before the value is dropped so that a
        // panicking `T::drop` still hands this slot back. Leaving the
        // sequence published and `head` stale would make
        // `RingBuffer::drop` drop the same value again.
        //
        // SAFETY: `seq_ptr` points into the RingBuffer kept alive by the
        // consumer's reference.
        let _release = SlotRelease::new(self.consumer.ring(), unsafe { &*self.seq_ptr }, self.head);
        // SAFETY: The value is initialized (seq check verified in pop_ref).
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }
    }
}
