#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::perf)]
#![allow(clippy::missing_errors_doc)]

/// A power-of-two capacity for use with [`RingBuffer`].
///
/// Guarantees that the capacity is always a power of two, enabling
/// fast bitwise-AND indexing instead of expensive modulo operations.
#[derive(Debug, Clone, Copy)]
pub struct Capacity {
    cap: usize,
    mask: usize,
}

impl Capacity {
    /// Creates a capacity of exactly `cap` elements.
    ///
    /// # Panics
    ///
    /// Panics if `cap` is not a power of two or is zero.
    #[must_use]
    pub fn exact(cap: usize) -> Self {
        assert!(cap > 0 && cap.is_power_of_two(), "capacity must be a non-zero power of two");
        Self {
            cap,
            mask: cap - 1,
        }
    }

    /// Creates a capacity of at least `min` elements by rounding up
    /// to the next power of two.
    ///
    /// # Panics
    ///
    /// Panics if `min` is zero.
    #[must_use]
    pub fn at_least(min: usize) -> Self {
        assert!(min > 0, "capacity must be non-zero");
        let cap = min.next_power_of_two();
        Self {
            cap,
            mask: cap - 1,
        }
    }

    /// Returns the actual capacity (always a power of two).
    #[must_use]
    pub const fn get(self) -> usize {
        self.cap
    }
}

struct Slot<T> {
    data: std::cell::UnsafeCell<std::mem::MaybeUninit<T>>,
    ready: std::sync::atomic::AtomicBool,
}

/// Buffer with cache-line-aligned allocation.
///
/// Ensures the buffer start is aligned to 64 bytes so that slot access
/// patterns are predictable for the hardware prefetcher.
struct AlignedBuf<T> {
    ptr: std::ptr::NonNull<T>,
    len: usize,
}

impl<T> AlignedBuf<T> {
    fn layout(len: usize) -> std::alloc::Layout {
        let size = std::mem::size_of::<T>().checked_mul(len).expect("capacity overflow");
        let align = std::mem::align_of::<T>().max(64);
        std::alloc::Layout::from_size_align(size, align).expect("invalid layout")
    }

    fn new_with(len: usize, mut init: impl FnMut() -> T) -> Self {
        assert!(len > 0);
        let layout = Self::layout(len);
        // SAFETY: layout has non-zero size (len > 0 asserted above)
        let ptr = unsafe { std::alloc::alloc(layout).cast::<T>() };
        let ptr = std::ptr::NonNull::new(ptr).expect("allocation failed");
        for i in 0..len {
            unsafe { ptr.as_ptr().add(i).write(init()) };
        }
        Self { ptr, len }
    }
}

impl<T> std::ops::Deref for AlignedBuf<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<T> Drop for AlignedBuf<T> {
    fn drop(&mut self) {
        unsafe {
            for i in 0..self.len {
                std::ptr::drop_in_place(self.ptr.as_ptr().add(i));
            }
            std::alloc::dealloc(self.ptr.as_ptr().cast::<u8>(), Self::layout(self.len));
        }
    }
}

// SAFETY: AlignedBuf is just an owning pointer to a heap allocation.
// Send/Sync follow from T's bounds, same as Box<[T]>.
unsafe impl<T: Send> Send for AlignedBuf<T> {}
unsafe impl<T: Sync> Sync for AlignedBuf<T> {}

/// Cache-line-sized padding to prevent false sharing between atomics.
#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

pub struct Producer<T> {
    queue: std::sync::Arc<RingBuffer<T>>,
    /// Cached snapshot of `head` to avoid cross-cache-line reads on every push.
    /// Since `head` only ever increases, a stale value is safe — it just makes
    /// the buffer appear fuller than it is. We re-fetch only when needed.
    cached_head: std::cell::Cell<usize>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: std::sync::Arc::clone(&self.queue),
            cached_head: std::cell::Cell::new(0),
        }
    }
}

impl<T> Producer<T> {
    /// Exponential backoff for CAS contention. Marked `#[inline(never)]` to
    /// keep the hot push loop's instruction footprint small — this code only
    /// matters under real multi-producer contention.
    #[inline(never)]
    fn cas_backoff(failures: &mut u32) {
        let f = *failures;
        if f > 1 {
            for _ in 0..1u32 << f {
                std::hint::spin_loop();
            }
        }
        *failures = f.saturating_add(1).min(6);
    }

    /// Atomically claims the next available slot via CAS loop.
    ///
    /// Returns raw pointers to the slot's data and ready flag, or `None`
    /// if the buffer is full. Used by both `push` and `reserve`.
    fn claim_slot(
        &self,
    ) -> Option<(
        *mut std::mem::MaybeUninit<T>,
        *const std::sync::atomic::AtomicBool,
    )> {
        let mut backoff = 0u32;
        let mut tail = self.queue.tail.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            // Fast path: check against cached head (avoids cross-cache-line read)
            if tail - self.cached_head.get() >= self.queue.cap {
                // Cached head says full — refresh from the real atomic
                let head = self.queue.head.load(std::sync::atomic::Ordering::Acquire);
                self.cached_head.set(head);

                if tail - head >= self.queue.cap {
                    return None;
                }
            }

            // Atomically reserve this slot
            match self.queue.tail.compare_exchange_weak(
                tail,
                tail + 1,
                std::sync::atomic::Ordering::Release,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // SAFETY: `tail & mask` is always < cap by construction
                    let slot =
                        unsafe { self.queue.buf.get_unchecked(tail & self.queue.mask) };
                    return Some((slot.data.get(), &raw const slot.ready));
                }
                Err(actual) => {
                    // Use the actual tail returned by CAS instead of reloading
                    tail = actual;
                    Self::cas_backoff(&mut backoff);
                }
            }
        }
    }

    /// Pushes a value into the ring buffer.
    ///
    /// Multiple producers can push concurrently. Uses CAS loop to
    /// atomically reserve slots.
    pub fn push(&self, val: T) -> Result<(), T> {
        match self.claim_slot() {
            Some((data_ptr, ready_ptr)) => {
                // SAFETY: We atomically claimed this slot via CAS
                unsafe { (*data_ptr).write(val) };
                // SAFETY: ready_ptr points into the RingBuffer kept alive by Arc
                unsafe {
                    (*ready_ptr).store(true, std::sync::atomic::Ordering::Release);
                }
                Ok(())
            }
            None => Err(val),
        }
    }

    /// Reserves a slot for zero-copy writing.
    ///
    /// Returns `None` if the buffer is full. On success, returns a
    /// [`SlotWriter`] that provides direct mutable access to the slot.
    ///
    /// # Contract
    ///
    /// You **must** call [`SlotWriter::commit`] after writing data.
    /// Dropping a `SlotWriter` without committing aborts the process.
    #[must_use]
    pub fn reserve(&self) -> Option<SlotWriter<T>> {
        self.claim_slot().map(|(data_ptr, ready_ptr)| SlotWriter {
            slot_data: data_ptr,
            slot_ready: ready_ptr,
            _ring: std::sync::Arc::clone(&self.queue),
            committed: false,
        })
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

/// A write-reservation into a ring buffer slot.
///
/// Obtained via [`Producer::reserve`]. Provides direct mutable access
/// to the slot's memory, enabling zero-copy writes for large types.
///
/// # Contract
///
/// You **must** call [`commit`](SlotWriter::commit) after writing data.
/// Dropping a `SlotWriter` without committing will **abort the process**
/// because the slot cannot be reclaimed (the tail has already advanced).
pub struct SlotWriter<T> {
    slot_data: *mut std::mem::MaybeUninit<T>,
    slot_ready: *const std::sync::atomic::AtomicBool,
    _ring: std::sync::Arc<RingBuffer<T>>,
    committed: bool,
}

// SAFETY: SlotWriter holds exclusive access to the slot (claimed via CAS).
// The raw pointers point into the Arc<RingBuffer<T>> which is kept alive.
unsafe impl<T: Send> Send for SlotWriter<T> {}

impl<T> SlotWriter<T> {
    /// Returns a mutable reference to the uninitialized slot memory.
    ///
    /// Use this for fine-grained control over initialization.
    #[must_use]
    pub fn slot_mut(&mut self) -> &mut std::mem::MaybeUninit<T> {
        // SAFETY: We have exclusive access via CAS claim. The pointer
        // is valid because _ring keeps the RingBuffer alive.
        unsafe { &mut *self.slot_data }
    }

    /// Writes a value into the reserved slot and returns a mutable
    /// reference to the now-initialized data.
    pub fn write(&mut self, val: T) -> &mut T {
        // SAFETY: Same as slot_mut — exclusive access, valid pointer.
        unsafe { (*self.slot_data).write(val) }
    }

    /// Commits the write, making the slot visible to the consumer.
    ///
    /// Sets the slot's ready flag with `Release` ordering and consumes
    /// the `SlotWriter`.
    ///
    /// # Safety contract
    ///
    /// The caller must have initialized the slot data (via [`write`] or
    /// [`slot_mut`] + `MaybeUninit::write`) before calling `commit`.
    /// Committing without initializing causes the consumer to read
    /// uninitialized memory (undefined behavior).
    pub fn commit(mut self) {
        // SAFETY: slot_ready points into the RingBuffer kept alive by _ring.
        unsafe {
            (*self.slot_ready).store(true, std::sync::atomic::Ordering::Release);
        }
        self.committed = true;
    }
}

impl<T> Drop for SlotWriter<T> {
    fn drop(&mut self) {
        if !self.committed {
            eprintln!(
                "FATAL: SlotWriter<{}> dropped without commit. \
                 The ring buffer slot is permanently stuck. Aborting.",
                std::any::type_name::<T>()
            );
            std::process::abort();
        }
    }
}

pub struct Consumer<T> {
    queue: std::sync::Arc<RingBuffer<T>>,
}
impl<T> Consumer<T> {
    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    ///
    /// Returns None if the queue is empty or if a slot has been claimed
    /// by a producer but not yet written (non-blocking behavior).
    #[inline]
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        let head = self.queue.head.load(std::sync::atomic::Ordering::Relaxed);
        let tail = self.queue.tail.load(std::sync::atomic::Ordering::Relaxed);

        if tail == head {
            return None;
        }

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        // Check if slot is ready (producer may have claimed but not written yet)
        if !slot.ready.load(std::sync::atomic::Ordering::Acquire) {
            return None; // Slot claimed but not written yet
        }

        // SAFETY: We checked that the slot is ready
        let val = unsafe { (*slot.data.get()).assume_init_read() };

        // Clear ready flag for next lap around the ring
        slot.ready.store(false, std::sync::atomic::Ordering::Relaxed);

        // Advance head
        self.queue
            .head
            .store(head + 1, std::sync::atomic::Ordering::Release);

        Some(val)
    }

    /// Returns a zero-copy read reference to the next item in the buffer.
    ///
    /// Unlike [`pop`], this does not copy the data out. Instead, it returns
    /// a [`SlotReader`] that dereferences to `&T`. The slot is released
    /// when the `SlotReader` is dropped.
    ///
    /// Returns `None` if the queue is empty or the next slot is not yet
    /// committed (same semantics as `pop`).
    #[must_use]
    pub fn pop_ref(&mut self) -> Option<SlotReader<'_, T>> {
        let head = self.queue.head.load(std::sync::atomic::Ordering::Relaxed);
        let tail = self.queue.tail.load(std::sync::atomic::Ordering::Relaxed);

        if tail == head {
            return None;
        }

        // SAFETY: `head & mask` is always < cap by construction
        let slot = unsafe { self.queue.buf.get_unchecked(head & self.queue.mask) };

        if !slot.ready.load(std::sync::atomic::Ordering::Acquire) {
            return None;
        }

        // SAFETY: ready=true guarantees the slot has been initialized.
        // We store raw pointers to avoid the borrow conflict between
        // borrowing slot data and holding &mut self.
        let data_ptr = slot.data.get().cast_const();
        let ready_ptr = &raw const slot.ready;

        Some(SlotReader {
            data_ptr,
            ready_ptr,
            consumer: self,
            head,
        })
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
impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

/// A zero-copy read reference to an item in the ring buffer.
///
/// Obtained via [`Consumer::pop_ref`]. Dereferences to `&T`, allowing
/// direct reads from the slot without copying.
///
/// When dropped, drops the `T` value, clears the slot's ready flag,
/// and advances the head pointer.
pub struct SlotReader<'a, T> {
    data_ptr: *const std::mem::MaybeUninit<T>,
    ready_ptr: *const std::sync::atomic::AtomicBool,
    consumer: &'a mut Consumer<T>,
    head: usize,
}

impl<T> std::ops::Deref for SlotReader<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: The slot was checked ready=true (Acquire) in pop_ref.
        // The data is initialized and we have exclusive access via &mut Consumer.
        unsafe { (*self.data_ptr).assume_init_ref() }
    }
}

impl<T> Drop for SlotReader<'_, T> {
    fn drop(&mut self) {
        // SAFETY: The value is initialized (ready=true was checked in pop_ref).
        // Exclusive access guaranteed by &mut Consumer.
        unsafe {
            std::ptr::drop_in_place(self.data_ptr.cast_mut().cast::<T>());
        }

        // SAFETY: ready_ptr points into the RingBuffer kept alive by
        // consumer's Arc.
        unsafe {
            (*self.ready_ptr).store(false, std::sync::atomic::Ordering::Relaxed);
        }

        self.consumer
            .queue
            .head
            .store(self.head + 1, std::sync::atomic::Ordering::Release);
    }
}

#[repr(C)]
pub struct RingBuffer<T> {
    buf: AlignedBuf<Slot<T>>,
    cap: usize,
    mask: usize,
    head: CachePadded<std::sync::atomic::AtomicUsize>,
    tail: CachePadded<std::sync::atomic::AtomicUsize>,
}
// Safety: Multiple producers use CAS to atomically claim tail slots.
// Single consumer touches head. Ready flags ensure proper synchronization.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}
impl<T> RingBuffer<T> {
    #[must_use]
    pub fn new(capacity: Capacity) -> Self {
        let cap = capacity.get();
        let buf = AlignedBuf::new_with(cap, || Slot {
            data: std::cell::UnsafeCell::new(std::mem::MaybeUninit::uninit()),
            ready: std::sync::atomic::AtomicBool::new(false),
        });

        Self {
            buf,
            head: CachePadded(std::sync::atomic::AtomicUsize::new(0)),
            tail: CachePadded(std::sync::atomic::AtomicUsize::new(0)),
            cap,
            mask: capacity.mask,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(std::sync::atomic::Ordering::Relaxed);
        let head = self.head.load(std::sync::atomic::Ordering::Relaxed);
        tail - head
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() == self.cap
    }

    /// Split the ring buffer into a producer and consumer pair
    #[must_use]
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let arc = std::sync::Arc::new(self);
        let producer = Producer {
            queue: arc.clone(),
            cached_head: std::cell::Cell::new(0),
        };
        let consumer = Consumer { queue: arc };
        (producer, consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_exact_valid() {
        let c = Capacity::exact(16);
        assert_eq!(c.get(), 16);
    }

    #[test]
    #[should_panic(expected = "capacity must be a non-zero power of two")]
    fn capacity_exact_rejects_non_power_of_two() {
        let _ = Capacity::exact(3);
    }

    #[test]
    #[should_panic(expected = "capacity must be a non-zero power of two")]
    fn capacity_exact_rejects_zero() {
        let _ = Capacity::exact(0);
    }

    #[test]
    fn capacity_at_least_rounds_up() {
        assert_eq!(Capacity::at_least(3).get(), 4);
        assert_eq!(Capacity::at_least(5).get(), 8);
        assert_eq!(Capacity::at_least(1000).get(), 1024);
    }

    #[test]
    fn capacity_at_least_preserves_power_of_two() {
        assert_eq!(Capacity::at_least(4).get(), 4);
        assert_eq!(Capacity::at_least(1024).get(), 1024);
    }

    #[test]
    #[should_panic(expected = "capacity must be non-zero")]
    fn capacity_at_least_rejects_zero() {
        let _ = Capacity::at_least(0);
    }

    #[test]
    fn capacity_one() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(1)).split();
        assert_eq!(producer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn zero_sized_types() {
        let (producer, mut consumer) = RingBuffer::<()>::new(Capacity::exact(4)).split();

        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn stress_push_pop() {
        let cap = Capacity::exact(128 * 1024 * 1024);
        let n = cap.get() as u64;
        let instant = std::time::Instant::now();
        let (producer, _consumer) = RingBuffer::<u64>::new(cap).split();

        for i in 0..n {
            producer
                .push(i)
                .unwrap_or_else(|_| panic!("Failed to push {i}",));
        }
        println!("Took {}ms", instant.elapsed().as_millis());
    }

    #[test]
    fn pop_empty_is_idempotent() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(1)).split();
        assert_eq!(consumer.pop(), None);
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
        assert_eq!(consumer.pop(), None);
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn length_invariants() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(2)).split();
        assert_eq!(producer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        producer.push(2).unwrap();
        assert_eq!(producer.len(), 2);
        assert_eq!(producer.push(3), Err(3));
        assert_eq!(producer.len(), 2);
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.len(), 1);
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn overwrite_oldest_element() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(4)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();
        producer.push(4).unwrap();

        assert_eq!(producer.push(5), Err(5));
        assert_eq!(consumer.pop(), Some(1));
        producer.push(5).unwrap();

        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), Some(5));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn wraparound_behavior() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(4)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        producer.push(3).unwrap();
        producer.push(4).unwrap();
        producer.push(5).unwrap(); // wraps here
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), Some(5));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn fill_to_capacity() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(16)).split();

        for i in 0..16 {
            producer.push(i).unwrap();
        }
        assert_eq!(producer.len(), 16);
        assert!(!producer.is_empty());
        assert!(producer.is_full());

        for i in 0..16 {
            assert_eq!(consumer.pop(), Some(i));
        }
        assert_eq!(consumer.len(), 0);
        assert!(consumer.is_empty());
        assert!(!consumer.is_full());
    }

    #[test]
    fn pop_empty_buffer() {
        let (_, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(16)).split();
        assert_eq!(consumer.pop(), None);
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
    }
    #[test]
    fn push_to_buffer() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(Capacity::exact(16)).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    // MPSC-specific tests
    #[test]
    #[ignore = "too slow for Miri"]
    fn multiple_producers_concurrent() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(1024)).split();

        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    for i in 0..100 {
                        p.push(thread_id * 100 + i).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let mut received = Vec::new();
        while let Some(v) = consumer.pop() {
            received.push(v);
        }

        assert_eq!(received.len(), 1000);
        // Verify all values are present (order may vary due to concurrency)
        received.sort_unstable();
        let expected: Vec<u64> = (0..1000).collect();
        assert_eq!(received, expected);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn dynamic_producer_creation() {
        let (producer, mut consumer) = RingBuffer::<usize>::new(Capacity::exact(128)).split();

        // Simulate dynamic producer creation (e.g., new connections)
        let mut handles = Vec::new();
        for i in 0..5 {
            let p = producer.clone();
            let handle = std::thread::spawn(move || {
                p.push(i).unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut received = Vec::new();
        while let Some(v) = consumer.pop() {
            received.push(v);
        }

        assert_eq!(received.len(), 5);
        received.sort_unstable();
        assert_eq!(received, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    #[ignore = "too slow for Miri"]
    fn mpsc_stress_test() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::at_least(10000)).split();

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let p = producer.clone();
                std::thread::spawn(move || {
                    for i in 0..1000 {
                        while p.push(thread_id * 1000 + i).is_err() {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let mut received = Vec::new();
        while let Some(v) = consumer.pop() {
            received.push(v);
        }

        assert_eq!(received.len(), 4000);
    }

    // -----------------------------------------------------------------------
    // Miri-targeted tests: small sizes exercising all unsafe code paths
    // -----------------------------------------------------------------------

    /// Tracks drops via a shared counter to verify no leaks or double-frees.
    #[derive(Clone, Debug)]
    struct DropCounter {
        counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Items still in the buffer when Consumer is dropped must be dropped.
    #[test]
    fn drop_items_on_consumer_drop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, consumer) = RingBuffer::new(Capacity::exact(4)).split();

        for _ in 0..4 {
            producer.push(DropCounter { counter: counter.clone() }).unwrap();
        }

        // Dropping consumer should drain and drop all 4 items
        drop(consumer);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    /// Items popped normally should be dropped exactly once.
    #[test]
    fn drop_items_on_pop() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) = RingBuffer::new(Capacity::exact(4)).split();

        for _ in 0..3 {
            producer.push(DropCounter { counter: counter.clone() }).unwrap();
        }

        // Pop 2 — should drop when they go out of scope
        let a = consumer.pop().unwrap();
        let b = consumer.pop().unwrap();
        drop(a);
        drop(b);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);

        // Remaining 1 dropped when consumer is dropped
        drop(consumer);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    /// Exercise `get_unchecked` on every index by wrapping around multiple times.
    #[test]
    fn wraparound_exercises_all_slots() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        // 3 full laps = 12 push/pops, covering slot indices 0-3 three times
        for lap in 0..3u32 {
            for i in 0..4 {
                producer.push(lap * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(consumer.pop(), Some(lap * 4 + i));
            }
        }
        assert_eq!(consumer.pop(), None);
    }

    /// Concurrent push/pop with a tiny buffer — Miri checks for data races.
    #[test]
    fn concurrent_data_race_check() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 16u64;

        let handle = std::thread::spawn(move || {
            for i in 0..n {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        let mut received = 0u64;
        while received < n {
            if consumer.pop().is_some() {
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }

        handle.join().unwrap();
        assert_eq!(received, n);
    }

    /// Two producers, tiny buffer — checks CAS + ready flag synchronization.
    #[test]
    fn concurrent_mpsc_data_race_check() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 8u64;

        let p2 = producer.clone();
        let h1 = std::thread::spawn(move || {
            for i in 0..n {
                while producer.push(i).is_err() {
                    std::thread::yield_now();
                }
            }
        });
        let h2 = std::thread::spawn(move || {
            for i in 0..n {
                while p2.push(100 + i).is_err() {
                    std::thread::yield_now();
                }
            }
        });

        let mut received = 0u64;
        while received < n * 2 {
            if consumer.pop().is_some() {
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }

        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(received, n * 2);
    }

    /// `AlignedBuf` deallocation correctness — drop types with drop glue.
    #[test]
    fn aligned_buf_drop_correctness() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let _buf = AlignedBuf::new_with(8, || DropCounter { counter: counter.clone() });
        }
        // 8 DropCounters created inside AlignedBuf, all should be dropped
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 8);
    }

    // -----------------------------------------------------------------------
    // Zero-copy API tests
    // -----------------------------------------------------------------------

    #[test]
    fn reserve_write_commit_pop_ref_cycle() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

        let mut writer = producer.reserve().unwrap();
        writer.write(42);
        writer.commit();

        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 42);
        drop(reader);

        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn reserve_slot_mut_commit() {
        let (producer, mut consumer) = RingBuffer::<[u8; 64]>::new(Capacity::exact(4)).split();

        let mut writer = producer.reserve().unwrap();
        writer.slot_mut().write([0xAB; 64]);
        writer.commit();

        let reader = consumer.pop_ref().unwrap();
        assert_eq!(reader[0], 0xAB);
        assert_eq!(reader[63], 0xAB);
    }

    #[test]
    fn reserve_returns_none_when_full() {
        let (producer, _consumer) = RingBuffer::<u64>::new(Capacity::exact(2)).split();

        let w1 = producer.reserve().unwrap();
        let w2 = producer.reserve().unwrap();
        assert!(producer.reserve().is_none());

        // Must commit to avoid abort
        let mut w1 = w1;
        let mut w2 = w2;
        w1.write(1);
        w1.commit();
        w2.write(2);
        w2.commit();
    }

    #[test]
    fn pop_ref_returns_none_when_empty() {
        let (_producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn pop_ref_returns_none_when_not_ready() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();

        // Reserve but don't commit — slot is claimed but not ready
        let mut writer = producer.reserve().unwrap();
        assert!(consumer.pop_ref().is_none());

        // Now commit and it should be readable
        writer.write(99);
        writer.commit();
        let reader = consumer.pop_ref().unwrap();
        assert_eq!(*reader, 99);
    }

    #[test]
    fn mixed_push_reserve_pop_pop_ref() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(8)).split();

        // Mix of push and reserve
        producer.push(1).unwrap();
        let mut w = producer.reserve().unwrap();
        w.write(2);
        w.commit();
        producer.push(3).unwrap();

        // Mix of pop and pop_ref
        assert_eq!(consumer.pop(), Some(1));
        let r = consumer.pop_ref().unwrap();
        assert_eq!(*r, 2);
        drop(r);
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn slot_reader_drops_value() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (producer, mut consumer) = RingBuffer::new(Capacity::exact(4)).split();

        producer
            .push(DropCounter {
                counter: counter.clone(),
            })
            .unwrap();

        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);

        {
            let reader = consumer.pop_ref().unwrap();
            // Value is alive while reader exists
            assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
            drop(reader);
        }

        // Value should be dropped when SlotReader is dropped
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn reserve_pop_ref_wraparound() {
        let (producer, mut consumer) = RingBuffer::<u32>::new(Capacity::exact(4)).split();

        // 3 full laps via reserve/pop_ref
        for lap in 0..3u32 {
            for i in 0..4 {
                let mut w = producer.reserve().unwrap();
                w.write(lap * 4 + i);
                w.commit();
            }
            for i in 0..4 {
                let r = consumer.pop_ref().unwrap();
                assert_eq!(*r, lap * 4 + i);
                drop(r);
            }
        }
        assert!(consumer.pop_ref().is_none());
    }

    #[test]
    fn concurrent_reserve_pop_ref() {
        let (producer, mut consumer) = RingBuffer::<u64>::new(Capacity::exact(4)).split();
        let n = 16u64;

        let handle = std::thread::spawn(move || {
            for i in 0..n {
                loop {
                    if let Some(mut w) = producer.reserve() {
                        w.write(i);
                        w.commit();
                        break;
                    }
                    std::thread::yield_now();
                }
            }
        });

        let mut received = 0u64;
        while received < n {
            if let Some(reader) = consumer.pop_ref() {
                assert_eq!(*reader, received);
                drop(reader);
                received += 1;
            } else {
                std::thread::yield_now();
            }
        }

        handle.join().unwrap();
    }
}
