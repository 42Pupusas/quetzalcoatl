use super::Arc;
use crate::capacity::Capacity;
use crate::common::staged_handoff::StagedHandoff;
use crate::spmc::SplitRing;

/// An empty ring of four, one producer and two consumers. The
/// first consumer runs the real `pop_block`, held open between its
/// pop and its stand-down by the park gate; see [`StagedHandoff`].
///
/// An item is published before the first consumer arms, and its
/// re-check takes it. The second consumer then finds the ring empty
/// and parks. The producer publishes again, and that publish has
/// one wake.
struct StagedWake;

impl StagedWake {
    /// Runs the schedule and returns whether the parked consumer
    /// was woken. `first_stands_down_first` orders the first
    /// consumer's stand-down before the second publish; without
    /// it the publish falls in the window.
    fn parked_consumer_is_woken(first_stands_down_first: bool) -> bool {
        let (producer, first) = SplitRing::<u32>::new(Capacity::exact(4)).split();
        let second = first.clone();
        let ring = Arc::clone(&first.queue);
        let handoff = StagedHandoff::new(&ring.consumer_park, first.park_slot, second.park_slot);

        handoff.hold_first();
        let first_done = std::thread::spawn(move || first.pop_block());
        handoff.wait_first_before_arm();
        producer.push(0).unwrap();
        handoff.let_first_arm();

        let parked = std::thread::spawn(move || second.pop_block());
        handoff.wait_second_parked();

        if first_stands_down_first {
            handoff.let_first_stand_down();
            assert_eq!(first_done.join().unwrap(), Some(0));
            producer.push(1).unwrap();
        } else {
            producer.push(1).unwrap();
            handoff.let_first_stand_down();
            assert_eq!(first_done.join().unwrap(), Some(0));
        }

        let woken = StagedHandoff::second_woke(&parked);
        if !woken {
            // Closing the ring releases the thread so the test
            // process can end.
            drop(producer);
        }
        assert_eq!(parked.join().unwrap(), Some(1));
        woken
    }
}

/// Control for the test below: the first consumer stands down
/// before the publish, so the harness has to deliver the wake.
#[test]
fn a_publish_after_the_first_consumer_stands_down_wakes_the_parked_one() {
    assert!(StagedWake::parked_consumer_is_woken(true));
}

/// A consumer's wake bit stays set from its arm until it stands
/// down, so a publish can pick a consumer whose post-arm re-check
/// has already taken an item. That wake must still reach a
/// consumer that is parked beside the new item.
#[test]
fn a_wake_spent_on_a_consumer_that_already_popped_reaches_a_parked_one() {
    assert!(StagedWake::parked_consumer_is_woken(false));
}
