use super::*;

fn cap() -> Capacity {
    Capacity::exact(8)
}

#[test]
fn a_fresh_word_frees_the_slot_for_its_first_lap() {
    for slot in 0..cap().get() {
        let word = DoneWord::released_for(slot);
        assert!(word.is_free_for(slot));
    }
}

#[test]
fn a_fresh_word_frees_nothing_but_the_first_lap() {
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
fn a_released_slot_is_not_free_for_the_position_that_just_ended() {
    let word = DoneWord::released_for(3);
    word.release(3, cap());
    assert!(!word.is_free_for(3));
}

#[test]
fn successive_laps_each_hand_the_slot_one_lap_on() {
    let capacity = cap();
    let slot = 2;
    let word = DoneWord::released_for(slot);
    for lap in 0..5 {
        let pos = slot + lap * capacity.get();
        assert!(word.is_free_for(pos), "lap {lap}");
        word.release(pos, capacity);
    }
}

#[test]
fn a_release_at_the_top_of_the_position_space_frees_the_wrapped_lap() {
    let capacity = cap();
    let pos = usize::MAX - 2;
    let mut word = DoneWord::released_for(pos);
    word.release(pos, capacity);
    assert!(word.is_free_for(pos.wrapping_add(capacity.get())));
    assert!(!word.holds_unconsumed_value_at(pos, capacity));
}

#[test]
fn an_unreleased_slot_still_owes_its_value_a_destructor() {
    let capacity = cap();
    let mut word = DoneWord::released_for(4);
    assert!(word.holds_unconsumed_value_at(4, capacity));
}

#[test]
fn a_released_slot_owes_nothing() {
    let capacity = cap();
    let mut word = DoneWord::released_for(4);
    word.release(4, capacity);
    assert!(!word.holds_unconsumed_value_at(4, capacity));
}

#[test]
fn a_release_for_one_lap_does_not_answer_for_the_next() {
    let capacity = cap();
    let mut word = DoneWord::released_for(4);
    word.release(4, capacity);
    assert!(word.holds_unconsumed_value_at(4 + capacity.get(), capacity));
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
