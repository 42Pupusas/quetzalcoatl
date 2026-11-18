//! An allocator that starts every block on a cache line.
//!
//! It forwards to [`Global`] with the alignment raised to 64. The
//! allocation and deallocation paths raise it the same way, so a
//! `Vec` or `Box` that knows only `T`'s own alignment still hands back
//! a layout that matches the block it got.

use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;

#[derive(Clone, Copy, Debug, Default)]
pub struct CacheLineAlloc;

impl CacheLineAlloc {
    const LINE: usize = 64;

    fn widen(layout: Layout) -> Result<Layout, AllocError> {
        layout.align_to(Self::LINE).map_err(|_| AllocError)
    }
}

// SAFETY: every block comes from `Global` and goes back to `Global`
// with the same widened layout, so `Global`'s guarantees carry over.
// Neither method can unwind: a failed widen returns `AllocError`.
unsafe impl Allocator for CacheLineAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(Self::widen(layout)?)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller's `layout` fits a block `allocate` returned,
        // and `allocate` only returns after this same widen succeeded.
        let widened = unsafe { Self::widen(layout).unwrap_unchecked() };
        // SAFETY: `ptr` came from `Global.allocate(widened)` above.
        unsafe { Global.deallocate(ptr, widened) };
    }
}

#[cfg(test)]
mod tests;
