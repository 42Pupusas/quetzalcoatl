use super::*;

fn cap() -> Capacity {
    Capacity::exact(8)
}

#[test]
fn a_fresh_word_frees_the_slot_for_its_first_round() {
    for slot in 0..cap().get() {
        let word = DoneWord::released_for(slot);
        assert!(word.is_free_for(slot));
    }
}

#[test]
fn a_fresh_word_frees_nothing_but_the_first_round() {
    let word = DoneWord::released_for(3);
    assert!(!word.is_free_for(3 + cap().get()));
}

#[test]
fn releasing_frees_the_slot_one_lap_ahead() {
    let word = DoneWord::released_for(3);
    word.release(3, cap());
    assert!(word.is_free_for(3 + cap().get()));
}

#[test]
fn a_released_slot_is_not_free_for_the_round_that_just_ended() {
    let word = DoneWord::released_for(3);
    word.release(3, cap());
    assert!(!word.is_free_for(3));
}

#[test]
fn successive_rounds_each_hand_the_slot_one_lap_on() {
    let capacity = cap();
    let slot = 2;
    let word = DoneWord::released_for(slot);
    for round in 0..5 {
        let round_pos = slot + round * capacity.get();
        assert!(word.is_free_for(round_pos), "round {round}");
        word.release(round_pos, capacity);
    }
}

#[test]
fn a_release_is_visible_to_the_teardown_check() {
    let capacity = cap();
    let mut word = DoneWord::released_for(4);
    assert!(!word.was_released_after(4, capacity));
    word.release(4, capacity);
    assert!(word.was_released_after(4, capacity));
}

#[test]
fn a_release_for_another_round_does_not_answer_for_this_one() {
    let capacity = cap();
    let mut word = DoneWord::released_for(4);
    word.release(4, capacity);
    assert!(!word.was_released_after(4 + capacity.get(), capacity));
}

#[test]
fn a_release_at_the_top_of_the_position_space_frees_the_wrapped_lap() {
    let capacity = cap();
    let pos = usize::MAX - 2;
    let word = DoneWord::released_for(capacity.index_of(pos));
    word.release(pos, capacity);
    assert!(word.is_free_for(pos.wrapping_add(capacity.get())));
}

#[test]
fn the_word_is_the_width_of_the_atomic_it_wraps() {
    assert_eq!(
        std::mem::size_of::<DoneWord>(),
        std::mem::size_of::<AtomicUsize>()
    );
    assert_eq!(
        std::mem::align_of::<DoneWord>(),
        std::mem::align_of::<AtomicUsize>()
    );
}
