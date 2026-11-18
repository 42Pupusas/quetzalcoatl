use super::*;

fn cap() -> Capacity {
    Capacity::exact(8)
}

#[test]
fn a_fresh_word_is_free_for_its_own_slot() {
    for slot in 0..cap().get() {
        let word = ReadyWord::free_at(slot);
        assert_eq!(word.state(slot, cap()), ReadyState::Free(slot));
    }
}

#[test]
fn publishing_marks_the_slot_as_holding_that_positions_value() {
    let word = ReadyWord::free_at(3);
    word.publish(3);
    assert_eq!(word.state(3, cap()), ReadyState::Published(3));
    assert!(word.state(3, cap()).is_published());
}

#[test]
fn a_claim_takes_the_published_value_exactly_once() {
    let word = ReadyWord::free_at(3);
    word.publish(3);
    assert!(word.claim(3));
    assert!(!word.claim(3));
    assert_eq!(word.state(3, cap()), ReadyState::Claimed(3));
}

#[test]
fn a_claim_of_an_unpublished_slot_fails() {
    let word = ReadyWord::free_at(3);
    assert!(!word.claim(3));
    assert_eq!(word.state(3, cap()), ReadyState::Free(3));
}

#[test]
fn an_abandoned_reservation_reads_as_claimed() {
    let word = ReadyWord::free_at(5);
    word.abandon(5);
    assert_eq!(word.state(5, cap()), ReadyState::Claimed(5));
}

#[test]
fn the_three_states_are_distinct_within_a_round() {
    let slot = 2;
    let free = ReadyWord::free_at(slot);
    let published = ReadyWord::free_at(slot);
    published.publish(slot);
    let claimed = ReadyWord::free_at(slot);
    claimed.publish(slot);
    assert!(claimed.claim(slot));

    assert_eq!(free.state(slot, cap()), ReadyState::Free(slot));
    assert_eq!(published.state(slot, cap()), ReadyState::Published(slot));
    assert_eq!(claimed.state(slot, cap()), ReadyState::Claimed(slot));
}

#[test]
fn a_later_round_decodes_to_its_own_position() {
    let slot = 1;
    let capacity = cap();
    for round in 0..4 {
        let round_pos = slot + round * capacity.get();
        let word = ReadyWord::free_at(slot);
        word.publish(round_pos);
        assert_eq!(
            word.state(slot, capacity),
            ReadyState::Published(round_pos),
            "round {round}"
        );
    }
}

/// `round_pos + 1` overflowed in a debug build at the last
/// position, where the word itself wraps to a small value.
#[test]
fn the_last_positions_publish_claim_and_abandon_without_overflow() {
    let capacity = cap();
    for round_pos in [usize::MAX - 7, usize::MAX - 1, usize::MAX - 7 - 8] {
        let slot = capacity.index_of(round_pos);
        let word = ReadyWord::free_at(slot);
        word.publish(round_pos);
        assert_eq!(word.state(slot, capacity), ReadyState::Published(round_pos));
        assert!(word.claim(round_pos));
        assert_eq!(word.state(slot, capacity), ReadyState::Claimed(round_pos));
        word.abandon(round_pos);
        assert_eq!(word.state(slot, capacity), ReadyState::Claimed(round_pos));
    }
}

#[test]
fn the_state_never_reaches_the_capacity() {
    let capacity = cap();
    assert!(
        ReadyWord::CLAIMED < capacity.get(),
        "a claimed word must not alias the next round's free word"
    );
}

#[test]
fn a_unique_borrow_decodes_the_same_state() {
    let mut word = ReadyWord::free_at(4);
    word.publish(4);
    assert_eq!(word.state_mut(4, cap()), ReadyState::Published(4));
}

#[test]
fn the_word_is_the_width_of_the_atomic_it_wraps() {
    assert_eq!(
        std::mem::size_of::<ReadyWord>(),
        std::mem::size_of::<AtomicUsize>()
    );
    assert_eq!(
        std::mem::align_of::<ReadyWord>(),
        std::mem::align_of::<AtomicUsize>()
    );
}
