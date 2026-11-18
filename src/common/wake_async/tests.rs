use super::{WakerSet, WakerSlot};
use crate::common::park_registry::ParkSlot;
use crate::common::registration_id::{Registration, RegistrationIds};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

struct CountingWaker {
    count: AtomicUsize,
}

impl CountingWaker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
        })
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn an_empty_slot_displaces_nothing() {
    let slot = WakerSlot::new();
    let ids = RegistrationIds::new();
    assert!(slot
        .store(Registration::new(
            ids.mint(),
            Waker::from(CountingWaker::new())
        ))
        .is_none());
}

/// Re-arming under one id replaces the entry in place, so an
/// executor polling a pending future repeatedly displaces nothing.
/// This holds under Miri too: identity is the id, not a vtable
/// pointer comparison that codegen has to deduplicate.
#[test]
fn re_registering_under_one_id_displaces_nothing() {
    let slot = WakerSlot::new();
    let ids = RegistrationIds::new();
    let id = ids.mint();
    let waker = Waker::from(CountingWaker::new());

    assert!(slot.store(Registration::new(id, waker.clone())).is_none());
    assert!(slot.store(Registration::new(id, waker)).is_none());
}

#[test]
fn a_second_registration_is_handed_back_rather_than_dropped() {
    let slot = WakerSlot::new();
    let ids = RegistrationIds::new();
    let first = CountingWaker::new();
    let _ = slot.store(Registration::new(
        ids.mint(),
        Waker::from(Arc::clone(&first)),
    ));

    let displaced = slot
        .store(Registration::new(
            ids.mint(),
            Waker::from(CountingWaker::new()),
        ))
        .expect("the first registration must be handed back");
    displaced.wake();
    assert_eq!(first.count(), 1);
}

/// Two registrations from one task share a waker, so only the id
/// distinguishes them: the second must be reported as displacing
/// the first rather than mistaken for a re-arm.
#[test]
fn two_registrations_from_one_task_displace_each_other() {
    let slot = WakerSlot::new();
    let ids = RegistrationIds::new();
    let task = CountingWaker::new();

    let _ = slot.store(Registration::new(
        ids.mint(),
        Waker::from(Arc::clone(&task)),
    ));
    let displaced = slot.store(Registration::new(
        ids.mint(),
        Waker::from(Arc::clone(&task)),
    ));

    assert!(
        displaced.is_some(),
        "a sibling's registration was silently dropped"
    );
}

/// A single future parking and being woken must never reach past
/// the overflow's empty fast path. `pending` is never cleared, so
/// every later wake on this ring calls into the overflow; only the
/// null-head check keeps that free.
#[test]
fn a_slotted_waiter_never_touches_the_overflow_stack() {
    let set = WakerSet::new();
    let waker = Waker::from(CountingWaker::new());
    let slot = ParkSlot::from_exclusive_index(0);

    for _ in 0..64 {
        set.register(slot, &Context::from_waker(&waker));
        set.wake_all();
    }

    assert_eq!(set.overflow.take_count(), 0);
}

/// A cancelled future leaves its registration behind. Each
/// cancellation from a slotless waiter pushes another entry that
/// nothing removes until the next wake, so the stack grows with
/// the number of cancellations rather than the number of waiters.
#[test]
fn cancelled_registrations_accumulate_on_the_overflow() {
    let set = WakerSet::new();
    let waker = Waker::from(CountingWaker::new());

    for _ in 0..1_000 {
        set.register(ParkSlot::Shared, &Context::from_waker(&waker));
    }

    assert_eq!(set.overflow.depth(), 1_000);
    set.wake_all();
    assert_eq!(set.overflow.depth(), 0);
}

/// Two futures sharing one endpoint's slot must both be woken: the
/// displaced registration belongs to a future that is still parked.
#[test]
fn a_displaced_registration_is_still_woken() {
    let set = WakerSet::new();
    let first = CountingWaker::new();
    let second = CountingWaker::new();
    let slot = ParkSlot::from_exclusive_index(0);

    let first_waker = Waker::from(Arc::clone(&first));
    let second_waker = Waker::from(Arc::clone(&second));
    set.register(slot, &Context::from_waker(&first_waker));
    set.register(slot, &Context::from_waker(&second_waker));

    set.wake_all();
    assert_eq!(first.count(), 1);
    assert_eq!(second.count(), 1);
}
