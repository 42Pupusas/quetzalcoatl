use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;

pub struct Slot<T> {
    pub data: UnsafeCell<MaybeUninit<T>>,
    pub ready: AtomicBool,
}

/// Buffer with cache-line-aligned allocation.
///
/// Ensures the buffer start is aligned to 64 bytes so that slot access
/// patterns are predictable for the hardware prefetcher.
pub struct AlignedBuf<T> {
    ptr: std::ptr::NonNull<T>,
    len: usize,
}

impl<T> AlignedBuf<T> {
    fn layout(len: usize) -> std::alloc::Layout {
        let size = std::mem::size_of::<T>().checked_mul(len).expect("capacity overflow");
        let align = std::mem::align_of::<T>().max(64);
        std::alloc::Layout::from_size_align(size, align).expect("invalid layout")
    }

    pub fn new_with(len: usize, mut init: impl FnMut() -> T) -> Self {
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
pub struct CachePadded<T>(pub T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
