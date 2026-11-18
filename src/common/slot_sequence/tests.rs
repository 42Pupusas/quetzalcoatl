use super::*;

fn word_of(seq: &SlotSequence) -> usize {
    seq.word.load(Ordering::Relaxed)
}

#[test]
fn a_fresh_word_is_free_for_its_own_position() {
    for pos in [0usize, 1, 7, 4096] {
        let mut seq = SlotSequence::free_at(pos);
        assert!(!seq.holds_value_at(pos), "free must not read as published");
        assert_eq!(word_of(&seq), pos * 4);
    }
}

#[test]
fn publishing_marks_the_slot_as_holding_that_positions_value() {
    for pos in [0usize, 1, 7, 4096] {
        let mut seq = SlotSequence::free_at(pos);
        seq.publish(pos);
        assert!(seq.holds_value_at(pos));
    }
}

#[test]
fn a_published_value_belongs_to_one_position_only() {
    let mut seq = SlotSequence::free_at(5);
    seq.publish(5);
    for other in [0usize, 4, 6, 10] {
        assert!(!seq.holds_value_at(other));
    }
}

#[test]
fn releasing_frees_the_slot_for_the_next_lap() {
    let capacity = Capacity::exact(8);
    for head in [0usize, 1, 7, 4096] {
        let seq = SlotSequence::free_at(head);
        seq.publish(head);
        seq.release(head, capacity);
        assert_eq!(word_of(&seq), word_of(&SlotSequence::free_at(head + 8)));
    }
}

#[test]
fn a_released_slot_is_not_yet_free_for_positions_before_the_next_lap() {
    let capacity = Capacity::exact(4);
    let seq = SlotSequence::free_at(0);
    seq.release(0, capacity);
    assert_eq!(word_of(&seq), word_of(&SlotSequence::free_at(4)));
    assert_ne!(word_of(&seq), word_of(&SlotSequence::free_at(0)));
}

#[test]
fn a_tombstone_is_not_a_published_value_at_any_position() {
    let mut seq = SlotSequence::free_at(3);
    seq.tombstone(3);
    for pos in [0usize, 3, 4, 4096] {
        assert!(!seq.holds_value_at(pos));
    }
}

/// Each window is checked on its own: words are distinct only for
/// positions fewer than `2^(usize::BITS - 2)` apart. The windows
/// cover the origin, the positions where `pos * 4` wraps, and the
/// one whose published word the old sentinel collided with.
#[test]
fn no_two_words_collide_within_a_window() {
    for start in [0, usize::MAX / 4 - 32, usize::MAX / 2 - 32] {
        let mut seen = std::collections::HashSet::new();
        for pos in start..start + 64 {
            for state in [
                SlotSequence::FREE,
                SlotSequence::PUBLISHED,
                SlotSequence::ABANDONED,
            ] {
                assert!(
                    seen.insert(SlotSequence::encode(pos, state)),
                    "word repeated at {pos}, state {state}"
                );
            }
        }
    }
}

/// A producer publishes the position it claimed, which is reduced
/// to 32 bits; the consumer classifies against its unreduced head.
#[test]
#[cfg(target_pointer_width = "64")]
fn a_reduced_position_reads_as_ready_at_the_unreduced_one() {
    let data = UnsafeCell::new(MaybeUninit::new(7u32));
    let head = (1usize << 32) + 5;
    let seq = SlotSequence::free_at(5);
    seq.publish(5);
    assert!(matches!(seq.classify(&data, head), SlotSnapshot::Ready(_)));
    assert!(matches!(
        seq.classify(&data, head + 1),
        SlotSnapshot::NotReady
    ));
}

#[test]
fn a_value_published_where_the_sentinel_used_to_sit_reads_as_ready() {
    let pos = usize::MAX / 2;
    let data = UnsafeCell::new(MaybeUninit::new(7u32));
    let mut seq = SlotSequence::free_at(pos);
    seq.publish(pos);
    assert!(matches!(seq.classify(&data, pos), SlotSnapshot::Ready(_)));
    assert!(seq.holds_value_at(pos));
}

#[test]
fn classify_reports_ready_only_for_the_published_position() {
    let data = UnsafeCell::new(MaybeUninit::new(7u32));
    let seq = SlotSequence::free_at(5);
    seq.publish(5);
    assert!(matches!(seq.classify(&data, 5), SlotSnapshot::Ready(_)));
    assert!(matches!(seq.classify(&data, 6), SlotSnapshot::NotReady));
}

#[test]
fn classify_reports_a_free_slot_as_not_ready() {
    let data = UnsafeCell::new(MaybeUninit::new(7u32));
    let seq = SlotSequence::free_at(5);
    assert!(matches!(seq.classify(&data, 5), SlotSnapshot::NotReady));
}

#[test]
fn classify_reports_a_tombstone_only_at_its_own_position() {
    let data = UnsafeCell::new(MaybeUninit::new(7u32));
    let seq = SlotSequence::free_at(5);
    seq.tombstone(5);
    assert!(matches!(seq.classify(&data, 5), SlotSnapshot::Tombstoned));
    for pos in [0usize, 4, 6, 4096] {
        assert!(matches!(seq.classify(&data, pos), SlotSnapshot::NotReady));
    }
}

#[test]
fn a_ready_snapshot_points_at_the_storage_it_was_given() {
    let data = UnsafeCell::new(MaybeUninit::new(7u32));
    let seq = SlotSequence::free_at(0);
    seq.publish(0);
    let SlotSnapshot::Ready(ptr) = seq.classify(&data, 0) else {
        panic!("expected Ready");
    };
    assert_eq!(ptr, data.get().cast_const());
    // SAFETY: the storage is initialized above and classify reported it ready.
    assert_eq!(unsafe { (*ptr).assume_init() }, 7);
}
