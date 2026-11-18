//! The ring's backing allocation.
//!
//! Slots are read and written by position, so the hardware prefetcher
//! sees a strided walk over the buffer. Starting that walk at a
//! 64-byte boundary keeps a slot's stride aligned with the cache lines
//! it crosses; an arbitrary start offsets every slot against the lines
//! and splits some of them across two.
//!
//! Zero-sized element types allocate nothing, as with any `Vec`.

use super::cache_line_alloc::CacheLineAlloc;

/// An owned, cache-line-aligned array of `len` initialized elements.
pub struct AlignedBuf<T> {
    slots: Box<[T], CacheLineAlloc>,
}

impl<T> AlignedBuf<T> {
    pub fn new_with(len: usize, init: impl FnMut() -> T) -> Self {
        assert!(len > 0);
        let mut slots = Vec::with_capacity_in(len, CacheLineAlloc);
        slots.extend(std::iter::repeat_with(init).take(len));
        Self {
            slots: slots.into_boxed_slice(),
        }
    }
}

impl<T> std::ops::Deref for AlignedBuf<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.slots
    }
}

impl<T> std::ops::DerefMut for AlignedBuf<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.slots
    }
}

#[cfg(test)]
mod tests;
