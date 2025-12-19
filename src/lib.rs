#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::perf)]
#![allow(clippy::missing_errors_doc)]

pub struct RingBuffer<T> {
    buf: Vec<std::mem::MaybeUninit<T>>,
    head: usize,
    tail: usize,
    cap: usize,
}
impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        debug_assert!(self.buf.len() == self.cap);
        while self.pop().is_some() {}
    }
}
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
                .collect(),
            head: 0,
            tail: 0,
            cap,
        }
    }
    /// Returns the number of elements in the buffer.
    /// The inner buffer has a fixed capacity, so we cannot just use `len` to
    /// get the number of elements in the buffer.
    ///
    /// We calculate the number of elements in the buffer by subtracting the
    /// index of the last element pushed to the buffer from the index of the
    /// first element pushed to the buffer.
    ///
    /// Since older elements are popped first, the len will never be greater
    /// than the capacity.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.tail - self.head
    }

    /// Checks if the buffer is empty.
    ///
    /// Because the buffer has a fixed capacity, we cannot just use `is_empty`
    /// to check if the buffer is empty.
    ///
    /// Every time an element is pushed to the buffer, its indeces chagne.
    /// If the indeces are equal, then the buffer is empty.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    /// Checks if the buffer is full.
    ///
    /// Because the buffer has a fixed capacity, we cannot just use `is_full`
    /// to check if the buffer is full.
    ///
    /// We check the length of the buffer by the difference between the index
    /// of the last element pushed to the buffer and the index of the first
    /// element pushed to the buffer.
    ///
    /// If the length is equal to the capacity, then the buffer is full.
    #[inline]
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.len() == self.cap
    }

    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    pub fn push(&mut self, val: T) {
        debug_assert!(self.buf.len() == self.cap);

        if self.len() == self.cap {
            // We are dropping the oldest element, so we need to shift the
            // head index
            let idx = self.head % self.cap;
            unsafe {
                self.buf[idx].assume_init_drop();
            }
            self.head += 1;
        }
        let idx = self.tail % self.cap;
        self.buf[idx].write(val);
        self.tail += 1;
    }
    /// Index can be grown indefinitely, once it overflows, it will
    /// wrap around to 0, so the modulo operation is safe.
    #[must_use]
    pub fn pop(&mut self) -> Option<T> {
        debug_assert!(self.buf.len() == self.cap);

        if self.is_empty() {
            return None;
        }
        let idx = self.head % self.cap;
        // SAFETY: We already checked that the buffer is not empty, so we know
        // that there is at least one element in the buffer.
        let val = unsafe { self.buf[idx].assume_init_read() };
        self.head += 1;
        Some(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise(cap: usize) -> Vec<i32> {
        let mut rb = RingBuffer::new(cap);

        for i in 0..10 {
            rb.push(i);
            if i % 3 == 0 {
                let _ = rb.pop();
            }
        }

        let mut out = Vec::new();
        while let Some(v) = rb.pop() {
            out.push(v);
        }

        out
    }

    #[test]
    fn power_of_two_vs_non_power_of_two() {
        let a = exercise(8); // power of two
        let b = exercise(7); // non-power of two

        assert_eq!(a, b);
    }

    #[test]
    fn capacity_one() {
        let mut rb = RingBuffer::<u8>::new(1);
        rb.push(1);
        assert_eq!(rb.pop(), Some(1));
        assert_eq!(rb.pop(), None);
        rb.push(2);
        assert_eq!(rb.pop(), Some(2));
        assert_eq!(rb.pop(), None);
        rb.push(3);
        rb.push(4);
        assert_eq!(rb.pop(), Some(4));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn zero_sized_types() {
        let mut rb = RingBuffer::<()>::new(3);
        rb.push(());
        rb.push(());
        rb.push(());
        assert_eq!(rb.pop(), Some(()));
        assert_eq!(rb.pop(), Some(()));
        assert_eq!(rb.pop(), Some(()));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn stress_push_pop() {
        let mut rb = RingBuffer::<u64>::new(10_000);

        for i in 0..100_000 {
            rb.push(i);
            if i % 2 == 0 {
                let _ = rb.pop();
            }
        }

        while rb.pop().is_some() {}
    }

    #[test]
    fn pop_empty_is_idempotent() {
        let mut rb = RingBuffer::<u8>::new(1);

        assert_eq!(rb.pop(), None);
        assert_eq!(rb.pop(), None);

        rb.push(42);
        assert_eq!(rb.pop(), Some(42));
        assert_eq!(rb.pop(), None);
    }

    #[derive(Clone)]
    struct DropCounter(std::rc::Rc<std::cell::Cell<usize>>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            let v = self.0.get();
            self.0.set(v + 1);
        }
    }

    #[test]
    fn drops_exactly_once() {
        let drops = std::rc::Rc::new(std::cell::Cell::new(0));

        {
            let mut rb = RingBuffer::<DropCounter>::new(3);
            rb.push(DropCounter(drops.clone()));
            rb.push(DropCounter(drops.clone()));
            rb.push(DropCounter(drops.clone())); // overwrites one
            let _ = rb.pop(); // drops one more
        } // remaining dropped here

        assert_eq!(drops.get(), 3);
    }

    #[test]
    fn length_invariants() {
        let mut rb = RingBuffer::<u8>::new(2);

        assert_eq!(rb.len(), 0);

        rb.push(1);
        assert_eq!(rb.len(), 1);

        rb.push(2);
        assert_eq!(rb.len(), 2);

        rb.push(3); // overwrite
        assert_eq!(rb.len(), 2);

        let _ = rb.pop();
        assert_eq!(rb.len(), 1);

        let _ = rb.pop();
        assert_eq!(rb.len(), 0);
    }

    #[test]
    fn overwrite_oldest_element() {
        let mut rb = RingBuffer::<u8>::new(3);
        rb.push(1);
        rb.push(2);
        rb.push(3);
        rb.push(4);
        assert_eq!(rb.pop(), Some(2));
        assert_eq!(rb.pop(), Some(3));
        assert_eq!(rb.pop(), Some(4));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn wraparound_behavior() {
        let mut rb = RingBuffer::<u8>::new(3);

        rb.push(1);
        rb.push(2);
        assert_eq!(rb.pop(), Some(1));

        rb.push(3);
        rb.push(4); // wraps here

        assert_eq!(rb.pop(), Some(2));
        assert_eq!(rb.pop(), Some(3));
        assert_eq!(rb.pop(), Some(4));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn fill_to_capacity() {
        let mut rb = RingBuffer::<u8>::new(10);
        assert_eq!(rb.buf.len(), 10);

        for i in 0..10 {
            rb.push(i);
        }
        assert_eq!(rb.len(), 10);
        assert!(!rb.is_empty());
        assert!(rb.is_full());

        for i in 0..10 {
            assert_eq!(rb.pop(), Some(i));
        }
        assert_eq!(rb.len(), 0);
        assert!(rb.is_empty());
        assert!(!rb.is_full());
    }

    #[test]
    fn pop_empty_buffer() {
        let mut rb = RingBuffer::<u8>::new(10);
        assert_eq!(rb.pop(), None);
        assert!(rb.is_empty());
        assert_eq!(rb.len(), 0);
    }
    #[test]
    fn push_to_buffer() {
        let mut rb = RingBuffer::<u8>::new(10);
        assert_eq!(rb.buf.len(), 10);

        rb.push(1);
        rb.push(2);
        rb.push(3);

        assert_eq!(rb.pop(), Some(1));
        assert_eq!(rb.pop(), Some(2));
        assert_eq!(rb.pop(), Some(3));
        assert_eq!(rb.pop(), None);
    }
}
