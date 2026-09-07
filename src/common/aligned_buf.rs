//! The ring's backing allocation.
//!
//! Slots are read and written by position, so the hardware prefetcher
//! sees a strided walk over the buffer. Starting that walk at a
//! 64-byte boundary keeps a slot's stride aligned with the cache lines
//! it crosses; an arbitrary start offsets every slot against the lines
//! and splits some of them across two.
//!
//! Zero-sized element types allocate nothing: the pointer is dangling
//! but well-aligned, and every offset from it is the same address.

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::ptr::NonNull;

/// An owned, cache-line-aligned array of `len` initialized elements.
pub struct AlignedBuf<T> {
    ptr: NonNull<T>,
    len: usize,
}

impl<T> AlignedBuf<T> {
    fn layout(len: usize) -> Layout {
        let size = std::mem::size_of::<T>()
            .checked_mul(len)
            .expect("capacity overflow");
        let align = std::mem::align_of::<T>().max(64);
        Layout::from_size_align(size, align).expect("invalid layout")
    }

    pub fn new_with(len: usize, mut init: impl FnMut() -> T) -> Self {
        assert!(len > 0);

        let ptr = if std::mem::size_of::<T>() == 0 {
            NonNull::dangling()
        } else {
            let layout = Self::layout(len);
            // SAFETY: layout has non-zero size (size_of::<T>() > 0 checked above)
            let raw = unsafe { alloc(layout).cast::<T>() };
            let Some(ptr) = NonNull::new(raw) else {
                handle_alloc_error(layout);
            };
            ptr
        };

        for i in 0..len {
            // SAFETY: For ZSTs, pointer arithmetic is a no-op (size 0 strides).
            // For non-ZSTs, ptr points to a valid allocation of len elements.
            unsafe { ptr.as_ptr().add(i).write(init()) };
        }
        Self { ptr, len }
    }
}

impl<T> std::ops::Deref for AlignedBuf<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        // SAFETY: ptr points to a valid allocation of `len` elements (or is
        // dangling for ZSTs). No mutable aliases exist (&self borrow).
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<T> std::ops::DerefMut for AlignedBuf<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        // SAFETY: ptr points to a valid allocation of `len` elements (or is
        // dangling for ZSTs). &mut self guarantees exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<T> Drop for AlignedBuf<T> {
    fn drop(&mut self) {
        // SAFETY: All `len` elements are initialized (written in new_with).
        // &mut self guarantees exclusive access. For non-ZSTs the layout
        // matches the one used in new_with's alloc call.
        unsafe {
            for i in 0..self.len {
                std::ptr::drop_in_place(self.ptr.as_ptr().add(i));
            }
            if std::mem::size_of::<T>() > 0 {
                dealloc(self.ptr.as_ptr().cast::<u8>(), Self::layout(self.len));
            }
        }
    }
}

// SAFETY: AlignedBuf is just an owning pointer to a heap allocation.
// Send/Sync follow from T's bounds, same as Box<[T]>.
unsafe impl<T: Send> Send for AlignedBuf<T> {}
unsafe impl<T: Sync> Sync for AlignedBuf<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::drop_counter::DropCounter;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn every_element_is_initialized_by_the_supplied_closure() {
        let mut n = 0u32;
        let buf = AlignedBuf::new_with(4, || {
            n += 1;
            n
        });
        assert_eq!(&*buf, &[1, 2, 3, 4]);
    }

    #[test]
    fn the_allocation_starts_on_a_cache_line() {
        let buf = AlignedBuf::new_with(8, || 0u8);
        assert_eq!(buf.as_ptr() as usize % 64, 0);
    }

    #[test]
    fn elements_are_reachable_for_mutation() {
        let mut buf = AlignedBuf::new_with(3, || 0u32);
        buf[1] = 42;
        assert_eq!(&*buf, &[0, 42, 0]);
    }

    #[test]
    fn dropping_the_buffer_drops_every_element() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let _buf = AlignedBuf::new_with(8, || DropCounter {
                counter: counter.clone(),
            });
        }
        assert_eq!(counter.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn a_zero_sized_element_type_allocates_nothing_and_still_drops() {
        DROPS.store(0, Ordering::Relaxed);
        {
            let _buf = AlignedBuf::new_with(5, || ZeroSized);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 5);
    }

    static DROPS: AtomicUsize = AtomicUsize::new(0);

    struct ZeroSized;

    impl Drop for ZeroSized {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_zero_sized_element_type_really_is_zero_sized() {
        assert_eq!(std::mem::size_of::<ZeroSized>(), 0);
    }

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn a_zero_length_buffer_is_rejected() {
        let _ = AlignedBuf::new_with(0, || 0u8);
    }
}
