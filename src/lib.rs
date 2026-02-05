#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::perf)]
#![allow(clippy::missing_errors_doc)]

struct Slot<T> {
    data: std::cell::UnsafeCell<std::mem::MaybeUninit<T>>,
    ready: std::sync::atomic::AtomicBool,
}

pub struct Producer<T> {
    queue: std::sync::Arc<RingBuffer<T>>,
}
impl<T> Producer<T> {
    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    ///
    /// Multiple producers can push concurrently. Uses CAS loop to
    /// atomically reserve slots.
    pub fn push(&self, val: T) -> Result<(), T> {
        loop {
            let tail = self.queue.tail.load(std::sync::atomic::Ordering::Relaxed);
            let head = self.queue.head.load(std::sync::atomic::Ordering::Acquire);

            if tail - head >= self.queue.cap {
                return Err(val);
            }

            // Atomically reserve this slot
            if self
                .queue
                .tail
                .compare_exchange_weak(
                    tail,
                    tail + 1,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                let slot_idx = tail % self.queue.cap;

                // Write data to claimed slot
                // SAFETY: We atomically claimed this slot via CAS
                unsafe {
                    (*self.queue.buf[slot_idx].data.get()).write(val);
                }

                // Mark slot as ready for consumer
                self.queue.buf[slot_idx]
                    .ready
                    .store(true, std::sync::atomic::Ordering::Release);

                return Ok(());
            }
            // CAS failed, retry
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

        let slot_idx = head % self.queue.cap;

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

pub struct RingBuffer<T> {
    buf: Box<[Slot<T>]>,
    cap: usize,
    head: std::sync::atomic::AtomicUsize,
    tail: std::sync::atomic::AtomicUsize,
}
// Safety: Multiple producers use CAS to atomically claim tail slots.
// Single consumer touches head. Ready flags ensure proper synchronization.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}
impl<T> RingBuffer<T> {
    #[must_use]
    pub fn new(cap: usize) -> Self {
        let buf: Box<[Slot<T>]> = (0..cap)
            .map(|_| Slot {
                data: std::cell::UnsafeCell::new(std::mem::MaybeUninit::uninit()),
                ready: std::sync::atomic::AtomicBool::new(false),
            })
            .collect();

        Self {
            buf,
            head: std::sync::atomic::AtomicUsize::new(0),
            tail: std::sync::atomic::AtomicUsize::new(0),
            cap,
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
        };
        let consumer = Consumer { queue: arc };
        (producer, consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise(cap: usize) -> Vec<i32> {
        let (producer, mut consumer) = RingBuffer::<i32>::new(cap).split();
        for i in 0..1000 {
            producer.push(i).unwrap();
            if i % 3 == 0 {
                assert!(consumer.pop().is_some());
            }
        }

        let mut out = Vec::new();
        while let Some(v) = consumer.pop() {
            out.push(v);
        }

        out
    }

    #[test]
    fn power_of_two_vs_non_power_of_two() {
        let a = exercise(1024); // power of two
        let b = exercise(1000); // non-power of two

        assert_eq!(a, b);
    }

    #[test]
    fn capacity_one() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(1).split();
        assert_eq!(producer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn zero_sized_types() {
        let (producer, mut consumer) = RingBuffer::<()>::new(3).split();

        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
    }

    #[test]
    fn stress_push_pop() {
        let instant = std::time::Instant::now();
        let (producer, _consumer) = RingBuffer::<u64>::new(100_000_000).split();

        for i in 0..100_000_000 {
            producer
                .push(i)
                .unwrap_or_else(|_| panic!("Failed to push {}", i));
        }
        println!("Took {}ms", instant.elapsed().as_millis());
    }

    #[test]
    fn pop_empty_is_idempotent() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(1).split();
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
        let (producer, mut consumer) = RingBuffer::<u8>::new(2).split();
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
        let (producer, mut consumer) = RingBuffer::<u8>::new(3).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();

        assert_eq!(producer.push(4), Err(4));
        assert_eq!(consumer.pop(), Some(1));
        producer.push(5).unwrap();

        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), Some(5));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn wraparound_behavior() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(3).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        producer.push(3).unwrap();
        producer.push(4).unwrap(); // wraps here
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn fill_to_capacity() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(10).split();

        for i in 0..10 {
            producer.push(i).unwrap();
        }
        assert_eq!(producer.len(), 10);
        assert!(!producer.is_empty());
        assert!(producer.is_full());

        for i in 0..10 {
            assert_eq!(consumer.pop(), Some(i));
        }
        assert_eq!(consumer.len(), 0);
        assert!(consumer.is_empty());
        assert!(!consumer.is_full());
    }

    #[test]
    fn pop_empty_buffer() {
        let (_, mut consumer) = RingBuffer::<u8>::new(10).split();
        assert_eq!(consumer.pop(), None);
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
    }
    #[test]
    fn push_to_buffer() {
        let (producer, mut consumer) = RingBuffer::<u8>::new(10).split();
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
        let (producer, mut consumer) = RingBuffer::<u64>::new(1000).split();
        let producer = std::sync::Arc::new(producer);

        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let p = std::sync::Arc::clone(&producer);
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
        let (producer, mut consumer) = RingBuffer::<usize>::new(100).split();
        let producer = std::sync::Arc::new(producer);

        // Simulate dynamic producer creation (e.g., new connections)
        let mut handles = Vec::new();
        for i in 0..5 {
            let p = std::sync::Arc::clone(&producer);
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
        let (producer, mut consumer) = RingBuffer::<u64>::new(10000).split();
        let producer = std::sync::Arc::new(producer);

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let p = std::sync::Arc::clone(&producer);
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
