//! What parking costs, and how much of it a pooled allocator removes.
//!
//! `ThreadParker::arm` boxes a `Thread` on every park, and the waker
//! frees that box, usually on another thread. `Parker<A>` repeats that
//! handshake generic over the allocator, so `global` against `pool`
//! differs only in where the box comes from. The `crate_*` benches run
//! the real code: `crate_wake_set` checks that `Parker<Global>` costs
//! what the crate does, and `crate_spsc` puts the allocation in scale
//! against a whole blocking round trip.

use divan::Bencher;
use quetzalcoatl::capacity::Capacity;
use quetzalcoatl::park::{ParkSlot, WakeSet};
use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::thread::{self, Thread};

fn main() {
    divan::main();
}

/// A small per-ring cache of `Thread`-sized blocks.
///
/// Each cell holds at most one block and is changed only by `swap` and
/// `compare_exchange`, so whoever takes a block out is its sole owner
/// and no ABA window exists. Any other layout goes straight to `Global`.
struct SlotPool {
    cells: [AtomicPtr<u8>; Self::CELLS],
}

impl SlotPool {
    const CELLS: usize = 4;
    const BLOCK: Layout = Layout::new::<Thread>();

    fn new() -> Self {
        Self {
            cells: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        }
    }

    fn take(&self) -> Option<NonNull<u8>> {
        self.cells.iter().find_map(|cell| {
            if cell.load(Ordering::Relaxed).is_null() {
                return None;
            }
            NonNull::new(cell.swap(ptr::null_mut(), Ordering::Acquire))
        })
    }

    fn stash(&self, block: NonNull<u8>) -> bool {
        self.cells.iter().any(|cell| {
            cell.compare_exchange(
                ptr::null_mut(),
                block.as_ptr(),
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
        })
    }
}

// SAFETY: a block is either a `Global` block of `BLOCK` layout parked in
// a cell, or handed to exactly one caller; `take` and `stash` move it
// with atomic swaps, so no block is ever given out twice. Blocks of any
// other layout never enter a cell. Neither method can unwind.
unsafe impl Allocator for SlotPool {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout == Self::BLOCK {
            if let Some(block) = self.take() {
                return Ok(NonNull::slice_from_raw_parts(block, layout.size()));
            }
        }
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout == Self::BLOCK && self.stash(ptr) {
            return;
        }
        // SAFETY: every block this pool hands out came from `Global`
        // with this layout.
        unsafe { Global.deallocate(ptr, layout) };
    }
}

impl Drop for SlotPool {
    fn drop(&mut self) {
        for cell in &mut self.cells {
            if let Some(block) = NonNull::new(*cell.get_mut()) {
                // SAFETY: a cell holds only `Global` blocks of `BLOCK` layout.
                unsafe { Global.deallocate(block, Self::BLOCK) };
            }
        }
    }
}

/// `ThreadParker` with the allocator as a parameter.
///
/// The swaps are `SeqCst`, and the fast-path load before the wake swap
/// is left out, so the baton below can hand over with a plain flag and
/// no fences.
struct Parker<A: Allocator + Clone> {
    handle: AtomicPtr<Thread>,
    alloc: A,
}

impl<A: Allocator + Clone> Parker<A> {
    fn new(alloc: A) -> Self {
        Self {
            handle: AtomicPtr::new(ptr::null_mut()),
            alloc,
        }
    }

    fn arm(&self) {
        let (next, _) =
            Box::into_raw_with_allocator(Box::new_in(thread::current(), self.alloc.clone()));
        self.free(self.handle.swap(next, Ordering::SeqCst));
    }

    fn wake(&self) -> bool {
        let claimed = self.handle.swap(ptr::null_mut(), Ordering::SeqCst);
        if claimed.is_null() {
            return false;
        }
        // SAFETY: the swap took `claimed` out of the slot, so this
        // thread owns it, and `arm` built it with `self.alloc`.
        unsafe { Box::from_raw_in(claimed, self.alloc.clone()) }.unpark();
        true
    }

    fn free(&self, handle: *mut Thread) {
        if !handle.is_null() {
            // SAFETY: the caller took `handle` out of the slot, and
            // `arm` built it with `self.alloc`.
            drop(unsafe { Box::from_raw_in(handle, self.alloc.clone()) });
        }
    }

    /// One arm and one wake on the calling thread. No thread sleeps:
    /// this is the bookkeeping a park adds on top of the futex call.
    fn bench_cycle(&self, bencher: Bencher) {
        bencher.bench_local(|| {
            self.arm();
            self.wake()
        });
    }
}

impl<A: Allocator + Clone> Drop for Parker<A> {
    fn drop(&mut self) {
        let handle = *self.handle.get_mut();
        self.free(handle);
    }
}

/// Two threads passing a turn back and forth, each one parking until
/// the other hands it over. Each box is allocated by the thread that
/// parks and freed by the thread that wakes it, as in the rings.
struct Baton<A: Allocator + Clone> {
    turn: AtomicBool,
    stop: AtomicBool,
    main: Parker<A>,
    peer: Parker<A>,
}

impl<A: Allocator + Clone + Sync> Baton<A> {
    const MAIN: bool = false;
    const PEER: bool = true;

    fn new(alloc: A) -> Self {
        Self {
            turn: AtomicBool::new(Self::MAIN),
            stop: AtomicBool::new(false),
            main: Parker::new(alloc.clone()),
            peer: Parker::new(alloc),
        }
    }

    fn pass(&self, to: bool, parker: &Parker<A>) {
        self.turn.store(to, Ordering::SeqCst);
        parker.wake();
    }

    /// Parks until it is `me`'s turn; `false` once the bench is over.
    fn await_turn(&self, me: bool, parker: &Parker<A>) -> bool {
        loop {
            parker.arm();
            if self.stop.load(Ordering::SeqCst) {
                return false;
            }
            if self.turn.load(Ordering::SeqCst) == me {
                return true;
            }
            thread::park();
        }
    }

    fn serve(&self) {
        while self.await_turn(Self::PEER, &self.peer) {
            self.pass(Self::MAIN, &self.main);
        }
    }

    fn bench(&self, bencher: Bencher) {
        thread::scope(|scope| {
            scope.spawn(|| self.serve());
            bencher.bench_local(|| {
                self.pass(Self::PEER, &self.peer);
                self.await_turn(Self::MAIN, &self.main)
            });
            self.stop.store(true, Ordering::SeqCst);
            self.peer.wake();
        });
    }
}

mod arm_wake {
    use super::*;

    #[divan::bench]
    fn global(bencher: Bencher) {
        Parker::new(Global).bench_cycle(bencher);
    }

    #[divan::bench]
    fn pool(bencher: Bencher) {
        let pool = SlotPool::new();
        Parker::new(&pool).bench_cycle(bencher);
    }

    #[divan::bench]
    fn crate_wake_set(bencher: Bencher) {
        let set = WakeSet::new();
        bencher.bench_local(|| {
            set.arm(ParkSlot::SOLE);
            set.wake_one();
        });
    }
}

mod ping_pong {
    use super::*;

    #[divan::bench]
    fn global(bencher: Bencher) {
        Baton::new(Global).bench(bencher);
    }

    #[divan::bench]
    fn pool(bencher: Bencher) {
        let pool = SlotPool::new();
        Baton::new(&pool).bench(bencher);
    }

    /// The same exchange through two spsc rings and `pop_block`, which
    /// spins before it parks.
    #[divan::bench]
    fn crate_spsc(bencher: Bencher) {
        let (to_peer, mut from_main) =
            quetzalcoatl::spsc::SplitRing::<u64>::new(Capacity::exact(2)).split();
        let (to_main, mut from_peer) =
            quetzalcoatl::spsc::SplitRing::<u64>::new(Capacity::exact(2)).split();
        thread::scope(|scope| {
            scope.spawn(move || {
                while let Some(value) = from_main.pop_block() {
                    let _ = to_main.push(value);
                }
            });
            bencher.bench_local(move || {
                let _ = to_peer.push(1);
                from_peer.pop_block()
            });
        });
    }
}
