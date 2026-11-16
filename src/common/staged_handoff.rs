//! The schedule that loses a wake without `WakeSet::stand_down`, driven
//! through a real blocking call.
//!
//! Two waiters share one [`WakeSet`], and the first runs the real
//! `push_block` or `pop_block`:
//!
//! 1. The first waiter's pre-arm check fails and it stops before `arm`.
//! 2. The test frees one unit of work. No bit is set, so no wake is
//!    owed.
//! 3. The first waiter arms, and its re-check takes that work. It stops
//!    before clearing its bit.
//! 4. The second waiter finds nothing and parks.
//! 5. The test frees one more unit. The one wake it carries picks the
//!    first waiter's bit, which is still set.
//! 6. The first waiter clears its bit. Either it passes the wake on, or
//!    the second waiter sleeps beside work.
//!
//! The test supplies the work and the two threads; this drives the
//! stops. The control orders step 6 before step 5.

use super::park::WakeSet;
use super::park_probe::ParkProbe;
use super::park_registry::ParkSlot;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct StagedHandoff<'a> {
    set: &'a WakeSet,
    first: ParkSlot,
    second: ParkSlot,
}

impl<'a> StagedHandoff<'a> {
    /// The first waiter must sit at the lower slot, so the round-robin
    /// cursor at zero picks it for the release in step 5.
    pub fn new(set: &'a WakeSet, first: ParkSlot, second: ParkSlot) -> Self {
        assert!(
            first.index() < second.index(),
            "the first waiter needs the lower slot",
        );
        Self { set, first, second }
    }

    /// Arranges both stops for the first waiter. Call before it starts.
    pub fn hold_first(&self) {
        self.set.gate.before_arm.hold(self.first);
        self.set.gate.before_clear.hold(self.first);
    }

    /// Waits for step 1.
    pub fn wait_first_before_arm(&self) {
        ParkProbe::expect_until("the first waiter to reach arm", || {
            self.set.gate.before_arm.is_reached()
        });
    }

    /// Lets the first waiter arm and waits for step 3.
    pub fn let_first_arm(&self) {
        self.set.gate.before_arm.open();
        ParkProbe::expect_until("the first waiter to reach its stand-down", || {
            self.set.gate.before_clear.is_reached()
        });
    }

    /// Waits for step 4, once the second waiter has been started.
    pub fn wait_second_parked(&self) {
        ParkProbe::expect_until("the second waiter to arm", || {
            self.set.wake.load(Ordering::SeqCst) & self.second.mask() != 0
        });
        // Long enough for its re-check to find nothing.
        std::thread::sleep(Duration::from_millis(50));
    }

    /// Step 6: lets the first waiter clear its bit.
    pub fn let_first_stand_down(&self) {
        self.set.gate.before_clear.open();
    }

    /// Whether the second waiter finished within the probe budget.
    pub fn second_woke<T>(second: &JoinHandle<T>) -> bool {
        ParkProbe::wait_until(|| second.is_finished())
    }
}
