use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;

/// Sentinel sequence value marking an abandoned slot (reserved but never
/// committed). Consumers detect this and silently skip past the slot.
///
/// Uses `usize::MAX` which cannot collide with valid `pos * 2 + 1` values
/// for any practical position (would require 2^63 pushes).
pub const TOMBSTONE: usize = usize::MAX;

/// Per-slot state using a sequence number instead of a ready flag.
///
/// The sequence encodes both readiness and ownership using a 2x encoding
/// that works correctly for all capacities (including `cap == 1`):
/// - `seq == pos * 2`:     slot is free for the producer to write at position `pos`
/// - `seq == pos * 2 + 1`: slot contains data ready for a consumer at position `pos`
pub struct SeqSlot<T> {
    pub data: UnsafeCell<MaybeUninit<T>>,
    pub sequence: AtomicUsize,
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
        let size = std::mem::size_of::<T>()
            .checked_mul(len)
            .expect("capacity overflow");
        let align = std::mem::align_of::<T>().max(64);
        std::alloc::Layout::from_size_align(size, align).expect("invalid layout")
    }

    pub fn new_with(len: usize, mut init: impl FnMut() -> T) -> Self {
        assert!(len > 0);

        let ptr = if std::mem::size_of::<T>() == 0 {
            // ZSTs need no allocation — use a well-aligned dangling pointer.
            std::ptr::NonNull::dangling()
        } else {
            let layout = Self::layout(len);
            // SAFETY: layout has non-zero size (size_of::<T>() > 0 checked above)
            let raw = unsafe { std::alloc::alloc(layout).cast::<T>() };
            let Some(ptr) = std::ptr::NonNull::new(raw) else {
                std::alloc::handle_alloc_error(layout);
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
                std::alloc::dealloc(self.ptr.as_ptr().cast::<u8>(), Self::layout(self.len));
            }
        }
    }
}

// SAFETY: AlignedBuf is just an owning pointer to a heap allocation.
// Send/Sync follow from T's bounds, same as Box<[T]>.
unsafe impl<T: Send> Send for AlignedBuf<T> {}
unsafe impl<T: Sync> Sync for AlignedBuf<T> {}

/// Exponential backoff for CAS contention. Marked `#[inline(never)]` to
/// keep the hot CAS loop's instruction footprint small — this code only
/// matters under real contention.
///
/// Schedule:
/// - calls 1-2: no spin (counter ramp-up only)
/// - calls 3-7: 4, 8, 16, 32, 64 pauses (~17μs total)
/// - calls 8+: 64 pauses (capped) + `yield_now` once we've spun
///   long enough (~17μs) that the holder is more likely preempted
///   than just slow. The yield is sparse — once the counter
///   saturates — so it doesn't fire on contention bursts.
#[inline(never)]
pub fn cas_backoff(failures: &mut u32) {
    // Under Miri, spin_loop() is an interleaving point. Exponential
    // spin counts explode the state space, so we just yield instead.
    #[cfg(miri)]
    {
        let _ = failures;
        std::thread::yield_now();
    }
    #[cfg(not(miri))]
    {
        let f = *failures;
        if f > 1 {
            for _ in 0..1u32 << f.min(6) {
                std::hint::spin_loop();
            }
        }
        // After we've saturated at f=6 and called again, we've spun
        // 64 pauses repeatedly. Yield to give the holder a chance to
        // run if it was preempted.
        if f >= 8 {
            std::thread::yield_now();
        }
        *failures = f.saturating_add(1).min(12);
    }
}

/// Cache-line-sized padding to prevent false sharing between atomics.
#[repr(align(64))]
pub struct CachePadded<T>(pub T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// Tracks drops via a shared counter to verify no leaks or double-frees.
///
/// Shared across all ring buffer test modules to avoid duplication.
#[cfg(test)]
#[derive(Clone, Debug)]
pub struct DropCounter {
    pub counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `AlignedBuf` deallocation correctness — drop types with drop glue.
    #[test]
    fn aligned_buf_drop_correctness() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let _buf = AlignedBuf::new_with(8, || DropCounter {
                counter: counter.clone(),
            });
        }
        // 8 DropCounters created inside AlignedBuf, all should be dropped
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 8);
    }
}
