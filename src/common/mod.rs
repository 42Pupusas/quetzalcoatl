pub mod park;
#[cfg(feature = "async")]
pub mod wake_async;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;

/// Spawns a *progress-based* deadlock watchdog for the async cross-thread
/// tests.
///
/// The watchdog aborts the process only if `progress` fails to advance for
/// 30 consecutive seconds — i.e. the test made no forward progress, the
/// signature of a real deadlock. A plain wall-clock deadline is wrong here:
/// under coverage instrumentation the lock-free hot loops run orders of
/// magnitude slower (and tests run heavily thread-oversubscribed), so a
/// fixed budget false-fires on a test that is merely slow but still
/// progressing. Tracking progress distinguishes "slow" from "stuck".
///
/// Callers bump `progress` on each item produced/consumed, set `done` when
/// the test finishes, then join the returned handle.
#[cfg(all(test, feature = "async"))]
pub fn spawn_progress_watchdog(
    progress: std::sync::Arc<std::sync::atomic::AtomicU64>,
    done: std::sync::Arc<std::sync::atomic::AtomicBool>,
    label: &'static str,
) -> std::thread::JoinHandle<()> {
    use std::sync::atomic::Ordering;
    std::thread::spawn(move || {
        let mut last = 0u64;
        let mut last_change = std::time::Instant::now();
        while !done.load(Ordering::Acquire) {
            let cur = progress.load(Ordering::Relaxed);
            if cur != last {
                last = cur;
                last_change = std::time::Instant::now();
            } else if last_change.elapsed() > std::time::Duration::from_secs(30) {
                eprintln!("\n\n{label}: no progress for 30s, deadlocked — aborting\n");
                std::process::abort();
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    })
}

/// Shared blocking-push loop for the single-producer topologies (spsc,
/// spmc). Both park the producer thread with the identical mechanism
/// (`producer_parker: OnceLock<Thread>` + `producer_parked` flag), so
/// their `push_block` bodies were byte-for-byte the same. The loop lives
/// here once; each implementor supplies the four primitives that differ
/// only in which ring fields they touch.
pub trait SingleParkerProducer<T> {
    /// Non-blocking push; `Err(val)` hands the value back when full.
    fn try_push(&self, val: T) -> Result<(), T>;
    /// True once the consumer side is gone — stop blocking, return `Err`.
    fn consumer_gone(&self) -> bool;
    /// Install this thread's park handle and publish "parked" (`SeqCst`,
    /// pairing with the consumer's wake-side load).
    fn arm_park(&self);
    /// Clear the "parked" flag (`Relaxed`).
    fn disarm_park(&self);

    /// Push, blocking the thread while the ring is full until a consumer
    /// frees space. Returns `Err(val)` only when the consumer side is
    /// gone. Spins via the shared backoff schedule, then parks.
    fn push_block(&self, mut val: T) -> Result<(), T> {
        let mut backoff = 0u32;
        loop {
            if self.consumer_gone() {
                return Err(val);
            }
            match self.try_push(val) {
                Ok(()) => return Ok(()),
                Err(returned) => val = returned,
            }
            if backoff < park::BACKOFF_PARK_THRESHOLD {
                cas_backoff(&mut backoff);
                continue;
            }
            self.arm_park();
            if self.consumer_gone() {
                self.disarm_park();
                return Err(val);
            }
            match self.try_push(val) {
                Ok(()) => {
                    self.disarm_park();
                    return Ok(());
                }
                Err(returned) => val = returned,
            }
            std::thread::park();
            self.disarm_park();
        }
    }
}

/// Shared blocking-pop loop for the single-consumer topologies (spsc,
/// mpsc). Both park the consumer thread with the identical mechanism
/// (`consumer_parker` + `consumer_parked`), so their `pop_block` bodies
/// were identical. The loop lives here once; implementors supply the
/// primitives that differ only in which ring fields they touch.
pub trait SingleParkerConsumer<T> {
    /// Non-blocking pop.
    fn try_pop(&mut self) -> Option<T>;
    /// True once the producer side is gone — drain then return `None`.
    fn producer_gone(&self) -> bool;
    /// Install this thread's park handle and publish "parked" (`SeqCst`).
    fn arm_park(&self);
    /// Clear the "parked" flag (`Relaxed`).
    fn disarm_park(&self);

    /// Pop, blocking the thread while the ring is empty until a producer
    /// publishes. Returns `None` only once the producer is gone and the
    /// ring has drained.
    fn pop_block(&mut self) -> Option<T> {
        let mut backoff = 0u32;
        loop {
            if let Some(v) = self.try_pop() {
                return Some(v);
            }
            if self.producer_gone() {
                // Re-pop: the producer may have published just before its
                // drop; that item must be drained before returning None.
                return self.try_pop();
            }
            if backoff < park::BACKOFF_PARK_THRESHOLD {
                cas_backoff(&mut backoff);
                continue;
            }
            self.arm_park();
            if let Some(v) = self.try_pop() {
                self.disarm_park();
                return Some(v);
            }
            if self.producer_gone() {
                self.disarm_park();
                return self.try_pop();
            }
            std::thread::park();
            self.disarm_park();
        }
    }
}

/// Zero-copy counterpart to [`SingleParkerConsumer`]: the shared blocking
/// loop for `pop_ref_block` on the single-consumer topologies (spsc,
/// mpsc). The returned reader borrows `self`, so the trait uses a GAT.
/// The gate is `has_item` (non-mutating) — never `try_pop_ref` — because
/// a discarded reader would advance the head on drop and consume the
/// item we meant to return.
pub trait SingleParkerConsumerRef {
    /// The borrowing read guard (e.g. `SlotReader<'a, ..>`).
    type Reader<'a>
    where
        Self: 'a;

    /// Non-mutating "is an item ready?" gate.
    fn has_item(&self) -> bool;
    /// Claim the ready item by reference. Only called right after
    /// `has_item()` returned true.
    fn try_pop_ref(&mut self) -> Option<Self::Reader<'_>>;
    /// True once the producer side is gone.
    fn producer_gone(&self) -> bool;
    /// Install park handle + publish "parked" (`SeqCst`).
    fn arm_park(&self);
    /// Clear the "parked" flag (`Relaxed`).
    fn disarm_park(&self);

    /// Zero-copy pop, blocking until an item is ready. Returns `None`
    /// only once the producer is gone and the ring has drained.
    fn pop_ref_block(&mut self) -> Option<Self::Reader<'_>> {
        let mut backoff = 0u32;
        loop {
            if self.has_item() {
                return self.try_pop_ref();
            }
            if self.producer_gone() {
                if self.has_item() {
                    return self.try_pop_ref();
                }
                return None;
            }
            if backoff < park::BACKOFF_PARK_THRESHOLD {
                cas_backoff(&mut backoff);
                continue;
            }
            self.arm_park();
            if self.has_item() {
                self.disarm_park();
                return self.try_pop_ref();
            }
            if self.producer_gone() {
                self.disarm_park();
                if self.has_item() {
                    return self.try_pop_ref();
                }
                return None;
            }
            std::thread::park();
            self.disarm_park();
        }
    }
}

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
