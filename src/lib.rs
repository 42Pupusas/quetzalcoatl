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

    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    ///
    /// Multiple producers can push concurrently. Uses CAS loop to
    /// atomically reserve slots.
    pub fn push(&self, val: T) -> Result<(), T> {
        let mut backoff = 0u32;
        let mut tail = self.queue.tail.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            // Fast path: check against cached head (avoids cross-cache-line read)
            if tail - self.cached_head.get() >= self.queue.cap {
                // Cached head says full — refresh from the real atomic
                let head = self.queue.head.load(std::sync::atomic::Ordering::Acquire);
                self.cached_head.set(head);

                if tail - head >= self.queue.cap {
                    return Err(val);
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
                    let slot = &self.queue.buf[tail & self.queue.mask];

                    // SAFETY: We atomically claimed this slot via CAS
                    unsafe { (*slot.data.get()).write(val) };

                    // Mark slot as ready for consumer
                    slot.ready.store(true, std::sync::atomic::Ordering::Release);

                    return Ok(());
                }
                Err(actual) => {
                    // Use the actual tail returned by CAS instead of reloading
                    tail = actual;
                    Self::cas_backoff(&mut backoff);
                }
            }
        }
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

pub struct Consumer<T> {
    queue: std::sync::Arc<RingBuffer<T>>,
}
impl<T> Consumer<T> {
    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    ///
    /// Returns None if the queue is empty or if a slot has been claimed
    /// by a producer but not yet written (non-blocking behavior).
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        let head = self.queue.head.load(std::sync::atomic::Ordering::Relaxed);
        let tail = self.queue.tail.load(std::sync::atomic::Ordering::Acquire);

        if tail == head {
            return None;
        }

        let slot_idx = head & self.queue.mask;

        // Check if slot is ready (producer may have claimed but not written yet)
        if !self.queue.buf[slot_idx]
            .ready
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return None; // Slot claimed but not written yet
        }

        // Read data
        // SAFETY: We checked that the slot is ready
        let val = unsafe { (*self.queue.buf[slot_idx].data.get()).assume_init_read() };

        // Clear ready flag for next lap around the ring
        self.queue.buf[slot_idx]
            .ready
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Advance head
        self.queue
            .head
            .store(head + 1, std::sync::atomic::Ordering::Release);

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
}
impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

#[repr(C)]
pub struct RingBuffer<T> {
    buf: Box<[Slot<T>]>,
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
        let buf: Box<[Slot<T>]> = (0..cap)
            .map(|_| Slot {
                data: std::cell::UnsafeCell::new(std::mem::MaybeUninit::uninit()),
                ready: std::sync::atomic::AtomicBool::new(false),
            })
            .collect();

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
    #[should_panic]
    fn capacity_exact_rejects_non_power_of_two() {
        let _ = Capacity::exact(3);
    }

    #[test]
    #[should_panic]
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
    #[should_panic]
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
}
