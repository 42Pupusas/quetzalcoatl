#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::perf)]
#![allow(clippy::missing_errors_doc)]

pub struct Producer<T> {
    queue: std::sync::Arc<RingBuffer<T>>,
}
impl<T> Producer<T> {
    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    pub fn push(&mut self, val: T) -> Result<(), T> {
        let head = self.queue.head.load(std::sync::atomic::Ordering::Relaxed);
        let tail = self.queue.tail.load(std::sync::atomic::Ordering::Relaxed);
        if tail - head == self.queue.cap {
            return Err(val);
        }
        // SAFETY: We are the only ones that can access the buffer
        unsafe {
            (*self.queue.buf[tail % self.queue.cap].get()).write(val);
        };
        self.queue
            .tail
            .store(tail + 1, std::sync::atomic::Ordering::Release);

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

pub struct Consumer<T> {
    queue: std::sync::Arc<RingBuffer<T>>,
}
impl<T> Consumer<T> {
    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        let tail = self.queue.tail.load(std::sync::atomic::Ordering::Acquire);
        let head = self.queue.head.load(std::sync::atomic::Ordering::Relaxed);

        if tail == head {
            return None;
        }

        let val = unsafe { (*self.queue.buf[head % self.queue.cap].get()).assume_init_read() };
        self.queue
            .head
            .store(head + 1, std::sync::atomic::Ordering::Relaxed);
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
    buf: Box<[std::cell::UnsafeCell<std::mem::MaybeUninit<T>>]>,
    cap: usize,
    head: std::sync::atomic::AtomicUsize,
    tail: std::sync::atomic::AtomicUsize,
}
// Safety: single producer touches tail, single consumer touches head
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}
impl<T> RingBuffer<T> {
    #[must_use]
    pub fn new(cap: usize) -> Self {
        let mut buf = Vec::<std::mem::MaybeUninit<T>>::with_capacity(cap);
        // SAFETY: We just set the length of the buffer to the capacity
        // so we know it's initialized.
        unsafe { buf.set_len(cap) };
        Self {
            buf: buf
                .into_iter()
                .map(|_| std::mem::MaybeUninit::uninit())
                .map(std::cell::UnsafeCell::new)
                .collect(),
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
        let (mut producer, mut consumer) = RingBuffer::<i32>::new(cap).split();
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
        let (mut producer, mut consumer) = RingBuffer::<u8>::new(1).split();
        assert_eq!(producer.len(), 0);
        producer.push(1).unwrap();
        assert_eq!(producer.len(), 1);
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.len(), 0);
    }

    #[test]
    fn zero_sized_types() {
        let (mut producer, mut consumer) = RingBuffer::<()>::new(3).split();

        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
        producer.push(()).unwrap();
        assert_eq!(consumer.pop(), Some(()));
    }

    #[test]
    fn stress_push_pop() {
        let instant = std::time::Instant::now();
        let (mut producer, _consumer) = RingBuffer::<u64>::new(100_000_000).split();

        for i in 0..100_000_000 {
            producer
                .push(i)
                .unwrap_or_else(|_| panic!("Failed to push {}", i));
        }
        println!("Took {}ms", instant.elapsed().as_millis());
    }

    #[test]
    fn pop_empty_is_idempotent() {
        let (mut producer, mut consumer) = RingBuffer::<u8>::new(1).split();
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
        let (mut producer, mut consumer) = RingBuffer::<u8>::new(2).split();
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
        let (mut producer, mut consumer) = RingBuffer::<u8>::new(3).split();
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
        let (mut producer, mut consumer) = RingBuffer::<u8>::new(3).split();
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
        let (mut producer, mut consumer) = RingBuffer::<u8>::new(10).split();

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
        let (mut producer, mut consumer) = RingBuffer::<u8>::new(10).split();
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert_eq!(consumer.pop(), None);
    }
}
