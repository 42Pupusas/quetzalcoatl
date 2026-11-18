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

/// The hand-rolled buffer this replaced leaked both the allocation
/// and the elements already built when `init` panicked.
#[test]
fn a_panicking_initializer_drops_what_it_already_built() {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut built = 0;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        AlignedBuf::new_with(4, || {
            built += 1;
            assert!(built < 3, "third element refuses");
            DropCounter {
                counter: counter.clone(),
            }
        })
    }));
    assert!(result.is_err());
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[test]
#[should_panic(expected = "assertion failed")]
fn a_zero_length_buffer_is_rejected() {
    let _ = AlignedBuf::new_with(0, || 0u8);
}
