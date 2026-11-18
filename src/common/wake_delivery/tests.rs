use super::{ParkSlot, WakeDelivery, WakeSet};

#[test]
fn a_claimed_handle_means_a_peer_unparked_us() {
    assert_eq!(
        WakeDelivery::from_park_state(false, false),
        WakeDelivery::Delivered
    );
}

/// The handle is claimed after the bit is cleared, so a claimed
/// handle settles the question whatever the bit says.
#[test]
fn a_claimed_handle_outranks_a_republished_bit() {
    assert_eq!(
        WakeDelivery::from_park_state(false, true),
        WakeDelivery::Delivered
    );
}

#[test]
fn an_armed_handle_with_its_bit_still_set_means_nobody_tried() {
    let state = WakeDelivery::from_park_state(true, true);
    assert_eq!(state, WakeDelivery::Untouched);
    assert!(state.is_unwoken());
    assert!(!state.is_in_flight());
}

/// The window this type exists to name: `wake_one` clears the bit
/// and is descheduled before claiming the handle.
#[test]
fn an_armed_handle_with_its_bit_taken_means_a_wake_is_in_flight() {
    let state = WakeDelivery::from_park_state(true, false);
    assert_eq!(state, WakeDelivery::InFlight);
    assert!(
        state.is_unwoken(),
        "no unpark has reached us yet, so the park did end on the timeout"
    );
    assert_ne!(
        state,
        WakeDelivery::Untouched,
        "a peer owns our wake, so this is not evidence of a lost one"
    );
}

#[test]
fn a_fresh_waiters_park_state_reads_as_untouched() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(3));
    assert_eq!(
        WakeDelivery::sample(&set, ParkSlot::Leased(3)),
        WakeDelivery::Untouched
    );
}

#[test]
fn a_woken_waiters_park_state_reads_as_delivered() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(3));
    set.wake_one();
    assert_eq!(
        WakeDelivery::sample(&set, ParkSlot::Leased(3)),
        WakeDelivery::Delivered
    );
}

/// Staged exactly as `wake_one` would leave it mid-flight: the bit
/// retired, the handle not yet claimed.
#[test]
fn a_bit_taken_without_the_handle_reads_as_in_flight() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(3));
    set.disarm(ParkSlot::Leased(3));
    assert_eq!(
        WakeDelivery::sample(&set, ParkSlot::Leased(3)),
        WakeDelivery::InFlight
    );
}

/// A slotless waiter publishes no bit and arms no handle, so this
/// classification cannot see it either way — it is reached through
/// the overflow stack, which keeps no per-waiter state to sample.
#[test]
fn a_slotless_waiter_is_unwoken_without_being_untouched() {
    let set = WakeSet::new();
    let state = WakeDelivery::sample(&set, ParkSlot::Shared);
    assert_eq!(state, WakeDelivery::Slotless);
    assert!(state.is_unwoken());
    assert!(!state.is_in_flight());
}
