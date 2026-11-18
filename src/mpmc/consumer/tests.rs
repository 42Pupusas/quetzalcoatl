use super::{Arc, Consumer, DefaultConfig};
use crate::capacity::Capacity;
use crate::mpmc::SplitRing;

/// A ring holding exactly one published item, at a chosen position.
///
/// The consumer's scan is what decides whether a blocking caller
/// parks, so the cases that matter are the ones where the single
/// item sits far from where the cursor happens to start. Filling
/// the ring and claiming every other position leaves that item
/// alone in a ring whose other slots are all in states the scan
/// must look past.
struct LoneItem {
    consumer: Consumer<u64, DefaultConfig>,
}

impl LoneItem {
    const CAP: usize = 8;

    /// Publishes a full ring, then claims every position but
    /// `keep`, so `keep` is the only one a scan can find.
    fn holding(keep: usize) -> Self {
        let (producer, consumer) =
            SplitRing::<u64, DefaultConfig>::new(Capacity::exact(Self::CAP)).split();
        for i in 0..Self::CAP {
            producer.push(i as u64).expect("the ring starts empty");
        }
        for position in 0..Self::CAP {
            if position != keep {
                consumer.next_scan.set(position);
                consumer
                    .claim_slot()
                    .expect("a full ring publishes every position");
            }
        }
        Self { consumer }
    }

    /// Points the scan cursor at `start` and reports what the
    /// consumer would decide from there: whether the pre-park gate
    /// sees work, and which position a claim actually lands on.
    fn scanned_from(&self, start: usize) -> (bool, Option<usize>) {
        self.consumer.next_scan.set(start);
        let gate = self.consumer.has_item();
        let claimed = self.consumer.claim_slot().map(|(_, round_pos)| round_pos);
        (gate, claimed)
    }
}

/// An empty ring of eight, one producer and two consumers. The
/// first consumer runs the real `pop_block`, held open between its
/// pop and its stand-down by the park gate; see
/// [`StagedHandoff`](crate::common::staged_handoff::StagedHandoff).
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
        use crate::common::staged_handoff::StagedHandoff;

        let (producer, first) = SplitRing::<u32, DefaultConfig>::new(Capacity::exact(8)).split();
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

/// The property a blocking consumer rests on: both the pre-park
/// gate and the claim find the item wherever the cursor started,
/// so a `None` means the ring is empty rather than that the scan
/// began in the wrong place.
///
/// This covers `has_item`, which decides whether `pop_ref_block`
/// parks and which nothing else exercises directly. It does *not*
/// reach the `ScanBudget` lap defect: single-threaded, every CAS is
/// won, so position and coverage advance together and no jump is
/// ever taken. Ending the lap on a move count instead of coverage
/// leaves this test passing and is caught in `ScanBudget`'s own
/// tests, which can stage a lost CAS without threads.
#[test]
fn a_lone_item_is_found_from_every_starting_cursor() {
    for keep in 0..LoneItem::CAP {
        for start in 0..LoneItem::CAP {
            let ring = LoneItem::holding(keep);
            let (gate, claimed) = ring.scanned_from(start);
            assert!(
                gate,
                "the pre-park gate missed the item at {keep} scanning from {start}",
            );
            assert_eq!(
                claimed,
                Some(keep),
                "the scan from {start} did not claim the item at {keep}",
            );
        }
    }
}

/// The converse, and the reason the gate may not simply return
/// true: a drained ring has to report empty from every cursor, or
/// a blocking consumer spins instead of parking.
#[test]
fn a_drained_ring_reports_empty_from_every_starting_cursor() {
    for start in 0..LoneItem::CAP {
        let ring = LoneItem::holding(0);
        ring.scanned_from(0);
        let (gate, claimed) = ring.scanned_from(start);
        assert!(!gate, "the gate saw work in a drained ring from {start}");
        assert_eq!(claimed, None, "a drained ring yielded a claim from {start}");
    }
}
