use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async")]
use std::task::Poll;

use super::RingBuffer;
use crate::common::park::BACKOFF_PARK_THRESHOLD;

/// The producer side of an SPMC ring buffer.
///
/// Generic over `R`: the ring reference. Defaults to `Arc<RingBuffer<T>>`
/// (owned split via [`RingBuffer::split`]). When `R = &'a RingBuffer<T>`
/// (borrowed split via [`RingBuffer::split_borrowed`]), `T` may carry
/// lifetimes shorter than `'static`.
///
/// Not cloneable — only one producer exists per buffer.
pub struct Producer<T, R: Deref<Target = RingBuffer<T>> = Arc<RingBuffer<T>>> {
    pub(super) queue: R,
    pub(super) write_pos: std::cell::Cell<usize>,
}

// SAFETY: Producer holds exclusive write access. The Cell field is only
// touched by the single producer thread. R is Send, RingBuffer is Sync.
unsafe impl<T: Send, R: Deref<Target = RingBuffer<T>> + Send> Send for Producer<T, R> {}

impl<T, R: Deref<Target = RingBuffer<T>>> Producer<T, R> {
    pub(super) const fn new_with(queue: R) -> Self {
        Self {
            queue,
            write_pos: std::cell::Cell::new(0),
        }
    }

    #[inline]
    fn ring(&self) -> &RingBuffer<T> {
        &self.queue
    }

    fn try_claim(&self) -> Option<(*mut MaybeUninit<T>, &AtomicUsize, usize)> {
        let pos = self.write_pos.get();

        let done = self.ring().done_slot(pos);
        if done.0.load(Ordering::Acquire) != pos {
            return None;
        }

        let ready = self.ring().ready_slot(pos);
        let data_ptr = self.ring().data_slot(pos).get();

        self.write_pos.set(pos + 1);
        Some((data_ptr, &ready.0, pos))
    }

    #[inline]
    fn has_space(&self) -> bool {
        let pos = self.write_pos.get();
        self.ring().done_slot(pos).0.load(Ordering::Acquire) == pos
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Returns `Err(val)` if the buffer is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.try_claim() {
            Some((data_ptr, slot_ready, pos)) => {
                // SAFETY: We are the sole producer and the slot is free.
                unsafe { (*data_ptr).write(val) };
                slot_ready.store(pos + 1, Ordering::Release);
                self.ring()
                    .tail
                    .store(self.write_pos.get(), Ordering::Release);
                self.ring().consumer_park.wake_one();
                #[cfg(feature = "async")]
                self.ring().wake_consumer_async();
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Pushes a value, blocking the calling thread when the ring is
    /// full until a consumer makes space. Returns `Err(val)` only
    /// when the last [`Consumer`](super::Consumer) has been dropped.
    pub fn push_block(&self, val: T) -> Result<(), T> {
        crate::common::SingleParkerProducer::push_block(self, val)
    }

    /// Pushes a value asynchronously, yielding to the executor when the
    /// ring is full until a consumer makes space. Returns `Err(val)`
    /// only when the last consumer has been dropped.
    #[cfg(feature = "async")]
    #[allow(clippy::missing_panics_doc, clippy::future_not_send)]
    pub fn push_async(&self, val: T) -> impl std::future::Future<Output = Result<(), T>> + '_ {
        let mut val = Some(val);
        std::future::poll_fn(move |cx| {
            let v = val.take().expect("polled after completion");
            if self.ring().consumer_closed.0.load(Ordering::Acquire) {
                return Poll::Ready(Err(v));
            }
            match self.push(v) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(returned) => {
                    val = Some(returned);
                    self.ring().producer_waker.register(0, cx);
                    if self.ring().consumer_closed.0.load(Ordering::Acquire) {
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
    /// Returns `None` if the buffer is full. Takes `&mut self` to
    /// guarantee at most one outstanding reservation. If dropped
    /// without writing, the reservation is silently rolled back.
    #[inline]
    #[must_use]
    pub fn reserve(&mut self) -> Option<SlotWriter<'_, T>> {
        self.try_claim()
            .map(|(data_ptr, slot_ready, pos)| SlotWriter {
                slot_data: data_ptr,
                slot_ready,
                tail: &self.ring().tail,
                write_pos: &self.write_pos,
                pos,
                queue: self.ring(),
            })
    }

    /// Reserves a slot for zero-copy writing, blocking the calling
    /// thread when the ring is full until a consumer makes space.
    /// Returns `None` only when the last
    /// [`Consumer`](super::Consumer) has been dropped.
    pub fn reserve_block(&mut self) -> Option<SlotWriter<'_, T>> {
        let mut backoff = 0u32;
        loop {
            if self.ring().consumer_closed.0.load(Ordering::Acquire) {
                return None;
            }
            if self.has_space() {
                return self.reserve();
            }
            if backoff < BACKOFF_PARK_THRESHOLD {
                crate::common::cas_backoff(&mut backoff);
                continue;
            }

            let _ = self.ring().producer_parker.set(std::thread::current());
            self.ring().producer_parked.0.store(true, Ordering::SeqCst);

            // SeqCst: post-arm half of the close handshake.
            if self.ring().consumer_closed.0.load(Ordering::SeqCst) {
                self.ring()
                    .producer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                return None;
            }
            if self.has_space() {
                self.ring()
                    .producer_parked
                    .0
                    .store(false, Ordering::Relaxed);
                return self.reserve();
            }

            std::thread::park();
            self.ring()
                .producer_parked
                .0
                .store(false, Ordering::Relaxed);
        }
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

impl<T, R: Deref<Target = RingBuffer<T>>> crate::common::SingleParkerProducer<T>
    for Producer<T, R>
{
    fn try_push(&self, val: T) -> Result<(), T> {
        self.push(val)
    }
    fn consumer_gone(&self) -> bool {
        // SeqCst: post-arm half of the close handshake.
        self.ring().consumer_closed.0.load(Ordering::SeqCst)
    }
    fn arm_park(&self) {
        let _ = self.ring().producer_parker.set(std::thread::current());
        self.ring().producer_parked.0.store(true, Ordering::SeqCst);
    }
    fn disarm_park(&self) {
        self.ring()
            .producer_parked
            .0
            .store(false, Ordering::Relaxed);
    }
}

impl<T, R: Deref<Target = RingBuffer<T>>> Drop for Producer<T, R> {
    fn drop(&mut self) {
        // SeqCst: pairs with each consumer's post-arm SeqCst load.
        self.ring().closed.0.store(true, Ordering::SeqCst);
        self.ring().consumer_park.flush();
        #[cfg(feature = "async")]
        self.ring().consumer_waker.flush();
    }
}

/// A write-reservation into an SPMC ring buffer slot.
///
/// Call [`write`](Self::write) to initialize and get a [`WrittenSlot`].
/// For raw access, use [`slot_mut`](Self::slot_mut) then
/// [`commit_unchecked`](Self::commit_unchecked) (unsafe).
///
/// Dropped without writing → silently rolled back.
pub struct SlotWriter<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_ready: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
    queue: &'a RingBuffer<T>,
}

// SAFETY: SlotWriter holds exclusive access to the slot (single producer).
// The raw pointer points into the RingBuffer kept alive by the Producer's
// reference (Arc or borrow).
unsafe impl<T: Send> Send for SlotWriter<'_, T> {}

impl<'a, T> SlotWriter<'a, T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: Single producer has exclusive access.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value, consuming this `SlotWriter` and returning a
    /// [`WrittenSlot`] that can be safely committed.
    pub fn write(self, val: T) -> WrittenSlot<'a, T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        // SAFETY: Exclusive access (single producer), valid pointer.
        unsafe { (*this.slot_data).write(val) };
        WrittenSlot {
            slot_data: this.slot_data,
            slot_ready: this.slot_ready,
            tail: this.tail,
            write_pos: this.write_pos,
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
        self.slot_ready.store(self.pos + 1, Ordering::Release);
        self.tail.store(self.write_pos.get(), Ordering::Release);
        self.queue.consumer_park.wake_one();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        std::mem::forget(self);
    }
}

impl<T> Drop for SlotWriter<'_, T> {
    fn drop(&mut self) {
        self.write_pos.set(self.pos);
    }
}

/// A slot initialized via [`SlotWriter::write`].
///
/// Call [`commit`](Self::commit) to publish. Dropped without
/// committing → value is dropped and reservation rolled back.
pub struct WrittenSlot<'a, T> {
    slot_data: *mut MaybeUninit<T>,
    slot_ready: &'a AtomicUsize,
    tail: &'a AtomicUsize,
    write_pos: &'a std::cell::Cell<usize>,
    pos: usize,
    queue: &'a RingBuffer<T>,
    committed: bool,
}

// SAFETY: Same as SlotWriter -- exclusive access to a slot in a RingBuffer
// kept alive by the Producer's reference.
unsafe impl<T: Send> Send for WrittenSlot<'_, T> {}

impl<T> WrittenSlot<'_, T> {
    /// Commits the write, making the slot visible to consumers.
    #[inline]
    pub fn commit(mut self) {
        self.slot_ready.store(self.pos + 1, Ordering::Release);
        self.tail.store(self.write_pos.get(), Ordering::Release);
        self.queue.consumer_park.wake_one();
        #[cfg(feature = "async")]
        self.queue.wake_consumer_async();
        self.committed = true;
    }
}

impl<T> Drop for WrittenSlot<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: write() initialized this slot.
            unsafe {
                self.slot_data.cast::<T>().drop_in_place();
            }
            self.write_pos.set(self.pos);
        }
    }
}
