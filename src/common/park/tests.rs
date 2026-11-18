use super::{ParkSlot, WakeInterest, WakeSet, PARK_SLOTS};
use std::sync::atomic::Ordering;

impl WakeSet {
    fn parked_bits(&self) -> u64 {
        self.wake.load(Ordering::Relaxed)
    }

    fn arm_all(&self, slots: &[u32]) {
        for &bit in slots {
            self.arm(ParkSlot::Leased(bit));
        }
    }

    fn parked_slots(&self) -> Vec<u32> {
        (0..u32::try_from(PARK_SLOTS).unwrap())
            .filter(|bit| self.parked_bits() & (1u64 << bit) != 0)
            .collect()
    }
}

#[test]
fn a_fresh_set_has_nobody_parked() {
    let set = WakeSet::new();
    assert_eq!(set.parked_bits(), 0);
    assert!(!set.is_armed(ParkSlot::Leased(0)));
}

#[test]
fn arming_publishes_the_slots_bit_and_its_handle() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(7));
    assert_eq!(set.parked_bits(), 1 << 7);
    assert!(set.is_armed(ParkSlot::Leased(7)));
}

#[test]
fn disarming_clears_only_its_own_bit() {
    let set = WakeSet::new();
    set.arm_all(&[3, 9]);
    set.disarm(ParkSlot::Leased(3));
    assert_eq!(set.parked_slots(), vec![9]);
}

#[test]
fn standing_down_with_the_bit_still_set_wakes_nobody() {
    let set = WakeSet::new();
    set.arm_all(&[3, 9]);
    set.stand_down(ParkSlot::Leased(3));
    assert_eq!(set.parked_slots(), vec![9]);
    assert!(set.is_armed(ParkSlot::Leased(9)));
}

#[test]
fn standing_down_after_a_wake_took_the_bit_passes_the_wake_on() {
    let set = WakeSet::new();
    set.arm_all(&[3, 9]);
    // A waker retires slot 3 and claims its handle.
    set.wake_one();
    assert_eq!(set.parked_slots(), vec![9]);
    set.stand_down(ParkSlot::Leased(3));
    assert_eq!(set.parked_slots(), Vec::<u32>::new());
    assert!(!set.is_armed(ParkSlot::Leased(9)));
}

#[test]
fn standing_down_by_position_wakes_every_parked_waiter() {
    let set = WakeSet::new();
    set.arm_all(&[3, 9, 12]);
    set.wake_one();
    set.stand_down_waking_all(ParkSlot::Leased(3));
    assert_eq!(set.parked_slots(), Vec::<u32>::new());
    assert!(!set.is_armed(ParkSlot::Leased(9)));
    assert!(!set.is_armed(ParkSlot::Leased(12)));
}

#[test]
fn a_slotless_waiter_stands_down_without_touching_the_bitmap() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(5));
    set.stand_down(ParkSlot::Shared);
    set.stand_down_waking_all(ParkSlot::Shared);
    assert_eq!(set.parked_slots(), vec![5]);
}

#[test]
fn a_slotless_waiter_publishes_no_bit_and_arms_no_handle() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Shared);
    assert_eq!(set.parked_bits(), 0);
    assert!(!set.is_armed(ParkSlot::Shared));
}

/// Having no bit is why it must be registered somewhere else: the
/// bitmap cannot represent it, so the overflow stack does.
#[test]
fn a_slotless_waiter_registers_on_the_overflow() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Shared);
    assert_eq!(set.overflow.depth(), 1);
}

#[test]
fn a_leased_waiter_never_touches_the_overflow() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(4));
    assert!(set.overflow.is_empty());
}

/// Every wake path has to drain the overflow, or a slotless
/// waiter parks with nobody able to reach it.
#[test]
fn every_wake_path_drains_the_overflow() {
    for (name, wake) in [
        (
            "wake_one",
            &(|s: &WakeSet| s.wake_one()) as &dyn Fn(&WakeSet),
        ),
        ("wake_n", &|s: &WakeSet| s.wake_n(1)),
        ("flush", &|s: &WakeSet| s.flush()),
        ("wake_one_interested", &|s: &WakeSet| {
            s.wake_one_interested(|_| WakeInterest::Reserved);
        }),
    ] {
        let set = WakeSet::new();
        set.arm(ParkSlot::Shared);
        assert_eq!(set.overflow.depth(), 1, "{name}: registration lost");
        wake(&set);
        assert!(
            set.overflow.is_empty(),
            "{name} left a slotless waiter registered and unreachable"
        );
    }
}

/// The behaviour the timeout used to provide, now provided by a
/// peer: a slotless waiter parks untimed and a wake releases it.
/// Without the overflow drain this hangs.
#[cfg(not(loom))]
#[test]
fn a_slotless_waiter_parks_untimed_and_a_peer_wake_releases_it() {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    let set = Arc::new(WakeSet::new());
    let released = Arc::new(AtomicBool::new(false));

    let waiter = {
        let set = Arc::clone(&set);
        let released = Arc::clone(&released);
        std::thread::spawn(move || {
            set.arm(ParkSlot::Shared);
            set.park();
            released.store(true, Ordering::Release);
        })
    };

    while set.overflow.is_empty() {
        std::thread::yield_now();
    }
    assert!(
        !released.load(Ordering::Acquire),
        "the waiter returned before any wake — the park was not untimed"
    );
    set.wake_one();
    waiter.join().unwrap();
    assert!(released.load(Ordering::Acquire));
}

#[test]
fn waking_an_empty_set_is_a_no_op() {
    let set = WakeSet::new();
    set.wake_one();
    set.wake_n(8);
    set.flush();
    assert_eq!(set.parked_bits(), 0);
}

#[test]
fn a_wake_releases_exactly_one_slot() {
    let set = WakeSet::new();
    set.arm_all(&[1, 2, 3]);
    set.wake_one();
    assert_eq!(set.parked_slots().len(), 2);
}

#[test]
fn waking_a_slot_claims_the_handle_it_armed() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(0));
    set.wake_one();
    assert!(!set.is_armed(ParkSlot::Leased(0)));
}

#[test]
fn successive_wakes_walk_the_parked_slots() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1]);
    set.wake_one();
    set.wake_one();
    assert_eq!(set.parked_bits(), 0, "both parked slots must be woken");
}

/// The saturated-mpmc deadlock the round-robin cursor exists to
/// fix: the waiter at the lower slot cannot make progress and
/// re-parks immediately, while the higher one holds the position
/// everybody needs. Picking the lowest set bit every time feeds
/// each wake to the stuck waiter and the higher slot parks forever.
#[test]
fn a_reparking_low_slot_does_not_starve_a_higher_one() {
    let set = WakeSet::new();
    set.arm_all(&[2, 3]);

    set.wake_one();
    assert_eq!(set.parked_slots(), vec![3], "the lower slot wakes first");

    set.arm(ParkSlot::Leased(2));
    set.wake_one();

    assert_eq!(
        set.parked_slots(),
        vec![2],
        "the second wake must reach slot 3, not the slot that just re-parked"
    );
}

#[test]
fn a_batch_wake_releases_the_count_it_was_given() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1, 2, 3, 4]);
    set.wake_n(3);
    assert_eq!(set.parked_slots().len(), 2);
}

#[test]
fn a_batch_wake_stops_at_the_number_parked() {
    let set = WakeSet::new();
    set.arm_all(&[5, 6]);
    set.wake_n(32);
    assert_eq!(set.parked_bits(), 0);
}

/// A slot's bit can be set while its handle is already claimed.
/// `arm` publishes the handle *then* the bit, and a waker claims
/// the handle *after* clearing the bit, so the two are not
/// updated atomically together: a waker that clears a bit and
/// then finds the handle gone has consumed a wake it never
/// delivered. If it stops there, a genuinely parked waiter on
/// another slot is never woken, and nothing records the debt.
///
/// Reached in the ring like this, with waiter `w` on bit 0 and
/// peers `p1`/`p2`:
///
/// - `w` arms bit 0 and parks.
/// - `p1` clears bit 0, taking ownership of the wake, and is
///   descheduled before it claims the handle.
/// - `w` wakes for an unrelated reason (a spurious unpark, or the
///   1 ms backstop the ring had at the time) and re-arms: a fresh
///   handle, and bit 0 set again.
/// - `p1` resumes and claims that handle. Bit 0 is now set with
///   no handle behind it.
/// - `p2` publishes work and wakes. It picks bit 0, clears it,
///   finds no handle, and returns having woken nobody — while
///   the waiter on bit 1 stays parked on work that is ready.
#[test]
fn a_wake_is_not_consumed_by_a_slot_whose_handle_is_already_claimed() {
    let set = WakeSet::new();
    set.arm(ParkSlot::Leased(0));
    set.arm(ParkSlot::Leased(1));

    assert!(
        set.parkers[0].wake(),
        "claim slot 0's handle, leaving its bit set behind it"
    );
    assert_eq!(set.parked_bits(), 0b11, "both bits are still published");

    set.wake_one();

    assert!(
        !set.is_armed(ParkSlot::Leased(1)),
        "the wake must reach slot 1, the only slot with a waiter behind it"
    );
}

/// The routing the mpmc producer side needs: a release only one
/// parked waiter can use must reach that waiter, whatever the
/// round-robin cursor would have picked.
#[test]
fn a_targeted_wake_reaches_the_waiter_that_wants_it() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1, 2]);
    // Cursor would pick slot 0 next.
    set.cursor.store(0, Ordering::Relaxed);

    set.wake_one_interested(|slot| {
        if slot == 2 {
            WakeInterest::Reserved
        } else {
            WakeInterest::Any
        }
    });

    assert!(set.is_armed(ParkSlot::Leased(0)));
    assert!(set.is_armed(ParkSlot::Leased(1)));
    assert!(
        !set.is_armed(ParkSlot::Leased(2)),
        "the wake must reach the one waiter that wanted it"
    );
    assert_eq!(set.parked_bits(), 0b011);
}

#[test]
fn a_targeted_wake_with_no_taker_falls_back_to_round_robin() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1]);
    set.cursor.store(1, Ordering::Relaxed);

    set.wake_one_interested(|_| WakeInterest::Any);

    assert!(
        !set.is_armed(ParkSlot::Leased(1)),
        "nobody reserved it, so the cursor's pick among the takers is served"
    );
    assert!(set.is_armed(ParkSlot::Leased(0)));
}

/// A release every parked waiter declines must wake nobody.
/// Waking one anyway spends the release on a waiter that re-parks,
/// and the waiter it was owed to is never reached.
#[test]
fn a_release_every_waiter_declines_wakes_nobody() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1, 2]);

    set.wake_one_interested(|_| WakeInterest::Declines);

    assert_eq!(
        set.parked_bits(),
        0b111,
        "a waiter that cannot use the release must stay parked"
    );
}

/// Ranking is by claim, not by position: the reserving waiter is
/// served even when a waiter that would take anything sits ahead
/// of it in round-robin order.
#[test]
fn a_reserving_waiter_outranks_one_that_would_take_anything() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1]);
    set.cursor.store(0, Ordering::Relaxed);

    set.wake_one_interested(|slot| {
        if slot == 1 {
            WakeInterest::Reserved
        } else {
            WakeInterest::Any
        }
    });

    assert!(
        set.is_armed(ParkSlot::Leased(0)),
        "the waiter that would take anything must yield to the one that reserved it"
    );
    assert!(!set.is_armed(ParkSlot::Leased(1)));
}

/// With nobody reserving, the release still reaches a waiter that
/// can use it rather than stopping at one that declines.
#[test]
fn a_declining_waiter_does_not_absorb_a_release_another_can_use() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1]);
    set.cursor.store(0, Ordering::Relaxed);

    set.wake_one_interested(|slot| {
        if slot == 0 {
            WakeInterest::Declines
        } else {
            WakeInterest::Any
        }
    });

    assert!(
        set.is_armed(ParkSlot::Leased(0)),
        "the declining waiter must not be woken"
    );
    assert!(
        !set.is_armed(ParkSlot::Leased(1)),
        "the waiter that could use the release must get it"
    );
}

#[test]
fn a_targeted_wake_on_an_empty_set_wakes_nobody() {
    let set = WakeSet::new();
    set.wake_one_interested(|_| WakeInterest::Reserved);
    assert_eq!(set.parked_bits(), 0);
}

/// A wanting slot whose handle is already claimed is a bit with
/// nobody behind it; the wake must move on to the next taker.
#[test]
fn a_targeted_wake_skips_a_wanting_slot_whose_handle_is_claimed() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1]);
    assert!(set.parkers[0].wake());

    set.wake_one_interested(|_| WakeInterest::Reserved);

    assert!(
        !set.is_armed(ParkSlot::Leased(1)),
        "slot 0 advertised a waiter that was not there"
    );
    assert_eq!(set.parked_bits(), 0);
}

/// The same defect on the batch path: `wake_n(1)` must deliver
/// one wake, not merely retire one bit.
#[test]
fn a_batch_wake_skips_slots_whose_handles_are_already_claimed() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1, 2]);
    assert!(set.parkers[0].wake());
    assert!(set.parkers[1].wake());

    set.wake_n(1);

    assert!(
        !set.is_armed(ParkSlot::Leased(2)),
        "the one requested wake must reach the one real waiter"
    );
}

/// `wake_n` counts delivered unparks, so a set of bits that can
/// never deliver one must still terminate: every iteration clears
/// the bit it picked, so the bitmap drains and the `ws == 0` exit
/// is reached rather than the loop spinning on undeliverable bits.
/// Round-robin is a fairness policy, not a delivery guarantee,
/// and the difference matters because mpmc producers are not
/// interchangeable waiters. Each parks holding a batch of
/// *specific* reserved positions, and `DoneWord::is_free_for`
/// matches an exact position, so a producer woken for a slot
/// outside its batch cannot use it and re-parks.
///
/// So "the wake went to another waiter" is only harmless when
/// that waiter could actually consume it. When it could not, the
/// event is spent: `wake_one` delivers one unpark per publish,
/// and the producer that *was* waiting on that exact position
/// stays parked. This is the starvation the cursor was added to
/// fix, and the cursor does not close it — it only changes which
/// waiter is passed over.
///
/// Here slot 0 is woken and re-parks (it cannot use the freed
/// position), while slot 1 is the one that needed it. The second
/// wake must reach slot 1 rather than returning to slot 0.
#[test]
fn a_waiter_that_cannot_use_its_wake_does_not_reclaim_the_next_one() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1]);

    set.wake_one();
    assert_eq!(set.parked_slots(), vec![1], "the first wake serves slot 0");

    set.arm(ParkSlot::Leased(0));
    set.wake_one();

    assert_eq!(
        set.parked_slots(),
        vec![0],
        "a waiter that re-parked must not consume the wake owed to slot 1"
    );
}

/// The cursor is shared per side, so it is advanced by whichever
/// waker ran last rather than per waiter. Two publishers waking
/// concurrently must still walk both parked slots rather than
/// both landing on the same one.
#[test]
fn two_wakes_from_one_cursor_reach_two_distinct_slots() {
    let set = WakeSet::new();
    set.arm_all(&[4, 5]);

    set.wake_one();
    set.wake_one();

    assert_eq!(
        set.parked_bits(),
        0,
        "both waiters were parked and two wakes were issued, so neither may be skipped"
    );
}

#[test]
fn a_batch_wake_terminates_when_no_bit_can_deliver() {
    let set = WakeSet::new();
    let every: Vec<u32> = (0..u32::try_from(PARK_SLOTS).unwrap()).collect();
    set.arm_all(&every);
    for bit in 0..PARK_SLOTS {
        assert!(set.parkers[bit].wake());
    }

    set.wake_n(8);

    assert_eq!(set.parked_bits(), 0);
}

#[test]
fn a_wake_with_no_reachable_waiter_clears_the_stale_bits() {
    let set = WakeSet::new();
    set.arm_all(&[0, 1]);
    assert!(set.parkers[0].wake());
    assert!(set.parkers[1].wake());

    set.wake_one();

    assert_eq!(
        set.parked_bits(),
        0,
        "bits with no handle behind them must not persist as wake sinks"
    );
}

#[test]
fn a_flush_releases_every_parked_slot() {
    let set = WakeSet::new();
    let every: Vec<u32> = (0..u32::try_from(PARK_SLOTS).unwrap()).collect();
    set.arm_all(&every);
    assert_eq!(set.parked_bits(), u64::MAX);
    set.flush();
    assert_eq!(set.parked_bits(), 0);
}

/// A shift or mask that truncated would leave the highest slot
/// parked with its bit still set.
#[test]
fn the_last_slot_is_reachable() {
    let set = WakeSet::new();
    let last = u32::try_from(PARK_SLOTS).unwrap() - 1;
    set.arm(ParkSlot::Leased(last));
    set.wake_one();
    assert_eq!(set.parked_bits(), 0);
}

#[test]
fn a_wake_reaches_a_waiter_that_parked_on_another_thread() {
    let set = std::sync::Arc::new(WakeSet::new());
    let peer = std::sync::Arc::clone(&set);

    let waiter = std::thread::spawn(move || {
        peer.arm(ParkSlot::Leased(1));
        std::thread::park();
        peer.disarm(ParkSlot::Leased(1));
    });

    crate::common::park_probe::ParkProbe::expect_until("the waiter to park", || {
        set.parked_bits() & (1 << 1) != 0
    });
    set.wake_one();
    waiter.join().unwrap();
}
