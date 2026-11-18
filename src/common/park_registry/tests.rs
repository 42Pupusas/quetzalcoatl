use super::{ParkRegistry, ParkSlot, PARK_SLOTS};

#[test]
fn a_fresh_registry_leases_the_first_slot() {
    let registry = ParkRegistry::new();
    assert_eq!(registry.lease(), ParkSlot::Leased(0));
    assert_eq!(registry.leased(), 1);
}

#[test]
fn concurrent_leases_are_distinct() {
    let registry = ParkRegistry::new();
    let held: Vec<_> = (0..PARK_SLOTS).map(|_| registry.lease()).collect();
    let mut indices: Vec<_> = held.iter().filter_map(|s| s.index()).collect();
    indices.sort_unstable();
    indices.dedup();
    assert_eq!(indices.len(), PARK_SLOTS);
}

#[test]
fn a_full_registry_hands_out_shared_slots() {
    let registry = ParkRegistry::new();
    for _ in 0..PARK_SLOTS {
        assert!(registry.lease().is_leased());
    }
    assert_eq!(registry.lease(), ParkSlot::Shared);
    assert_eq!(registry.lease(), ParkSlot::Shared);
}

#[test]
fn a_released_slot_is_leased_again() {
    let registry = ParkRegistry::new();
    let first = registry.lease();
    registry.release(first);
    assert_eq!(registry.lease(), first);
    assert_eq!(registry.leased(), 1);
}

/// The defect a monotonic counter has: creating and dropping
/// endpoints repeatedly must not start aliasing live waiters.
#[test]
fn churning_endpoints_never_aliases_a_live_slot() {
    let registry = ParkRegistry::new();
    let long_lived = registry.lease();

    for _ in 0..(PARK_SLOTS * 4) {
        let transient = registry.lease();
        assert_ne!(transient, long_lived);
        registry.release(transient);
    }
    assert_eq!(registry.leased(), 1);
}

#[test]
fn releasing_a_shared_slot_frees_nothing() {
    let registry = ParkRegistry::new();
    let leased = registry.lease();
    registry.release(ParkSlot::Shared);
    assert_eq!(registry.leased(), 1);
    registry.release(leased);
    assert_eq!(registry.leased(), 0);
}

#[test]
fn a_shared_slot_is_invisible_to_the_wake_bitmap() {
    assert_eq!(ParkSlot::Shared.mask(), 0);
    assert_eq!(ParkSlot::Shared.index(), None);
    assert!(!ParkSlot::Shared.is_leased());
}

#[test]
fn an_index_past_the_wake_table_overflows_rather_than_aliasing() {
    assert_eq!(ParkSlot::from_exclusive_index(0), ParkSlot::Leased(0));
    assert_eq!(
        ParkSlot::from_exclusive_index(PARK_SLOTS - 1),
        ParkSlot::Leased(u32::try_from(PARK_SLOTS).unwrap() - 1)
    );
    assert_eq!(ParkSlot::from_exclusive_index(PARK_SLOTS), ParkSlot::Shared);
    assert_eq!(
        ParkSlot::from_exclusive_index(PARK_SLOTS + 1),
        ParkSlot::Shared,
        "masking would alias slot 1 and steal a live waiter's wakeups"
    );
}

#[test]
fn a_leased_slot_masks_only_its_own_bit() {
    assert_eq!(ParkSlot::Leased(0).mask(), 1);
    assert_eq!(ParkSlot::Leased(5).mask(), 1 << 5);
    assert_eq!(ParkSlot::Leased(5).index(), Some(5));
}

#[test]
#[allow(clippy::needless_collect)]
fn leases_taken_from_many_threads_are_distinct() {
    let registry = std::sync::Arc::new(ParkRegistry::new());
    // Collected deliberately: the threads must all be spawned, and
    // so contending, before any is joined.
    let taken: Vec<_> = (0..PARK_SLOTS)
        .map(|_| {
            let registry = std::sync::Arc::clone(&registry);
            std::thread::spawn(move || registry.lease())
        })
        .collect();

    let mut indices: Vec<_> = taken
        .into_iter()
        .filter_map(|h| h.join().unwrap().index())
        .collect();
    indices.sort_unstable();
    indices.dedup();
    assert_eq!(indices.len(), PARK_SLOTS);
}
