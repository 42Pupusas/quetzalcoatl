use super::*;

#[test]
fn a_fresh_word_reads_as_unwritten() {
    let mut word = SequenceWord::unwritten();
    assert_eq!(word.state_mut(), SlotState::Unwritten);
    assert!(!word.state_mut().holds_value());
}

#[test]
fn a_fresh_word_is_not_a_claim_in_progress() {
    assert!(!SequenceWord::unwritten().is_claim_in_progress());
}

#[test]
fn a_claim_marks_the_slot_in_progress() {
    let word = SequenceWord::unwritten();
    word.claim();
    assert!(word.is_claim_in_progress());
}

#[test]
fn no_position_produces_the_unwritten_word() {
    for pos in [0usize, 1, usize::MAX / 4, usize::MAX / 2, usize::MAX] {
        assert_eq!(
            SlotState::decode(SlotState::published_word(pos)),
            SlotState::Published
        );
        assert_ne!(SlotState::published_word(pos), SlotState::UNWRITTEN);
        assert_ne!(SlotState::abandoned_word(pos), SlotState::UNWRITTEN);
    }
}

#[test]
fn publishing_and_abandoning_both_resolve_the_claim() {
    let published = SequenceWord::unwritten();
    published.publish(7);
    assert!(!published.is_claim_in_progress());

    let abandoned = SequenceWord::unwritten();
    abandoned.abandon(7);
    assert!(!abandoned.is_claim_in_progress());
}

#[test]
fn a_claim_reports_the_published_value_it_displaces() {
    let word = SequenceWord::unwritten();
    word.publish(3);
    assert!(word.claim_taking_previous().holds_value());
}

#[test]
fn a_claim_over_an_abandoned_slot_owes_no_destructor() {
    let word = SequenceWord::unwritten();
    word.abandon(3);
    assert!(!word.claim_taking_previous().holds_value());
}

#[test]
fn a_claim_leaves_the_slot_unresolved_again() {
    let word = SequenceWord::unwritten();
    word.publish(3);
    let _ = word.claim_taking_previous();
    assert!(word.is_claim_in_progress());
}

#[test]
fn the_teardown_state_agrees_with_what_was_published() {
    let mut word = SequenceWord::unwritten();
    assert_eq!(word.state_mut(), SlotState::Unwritten);
    word.publish(9);
    assert_eq!(word.state_mut(), SlotState::Published);
    word.abandon(9);
    assert_eq!(word.state_mut(), SlotState::Abandoned);
}

#[test]
fn the_word_is_the_width_of_the_atomic_it_wraps() {
    assert_eq!(
        std::mem::size_of::<SequenceWord>(),
        std::mem::size_of::<AtomicUsize>()
    );
    assert_eq!(
        std::mem::align_of::<SequenceWord>(),
        std::mem::align_of::<AtomicUsize>()
    );
}

#[test]
fn words_decode_to_their_state_at_every_position() {
    for pos in [
        0usize,
        1,
        2,
        7,
        4096,
        usize::MAX / 4,
        usize::MAX / 2,
        usize::MAX,
    ] {
        assert_eq!(
            SlotState::decode(SlotState::published_word(pos)),
            SlotState::Published
        );
        assert_eq!(
            SlotState::decode(SlotState::abandoned_word(pos)),
            SlotState::Abandoned
        );
    }
    assert_eq!(SlotState::decode(SlotState::VACANT), SlotState::Vacant);
}

/// `pos * 2 + 2` was zero at `usize::MAX / 2`, so an abandoned
/// reservation there read as a claim in progress.
#[test]
fn no_resolved_word_equals_the_vacant_word() {
    for pos in [
        0usize,
        usize::MAX / 4,
        usize::MAX / 2,
        usize::MAX - 1,
        usize::MAX,
    ] {
        assert_ne!(SlotState::published_word(pos), SlotState::VACANT);
        assert_ne!(SlotState::abandoned_word(pos), SlotState::VACANT);
    }
}

#[test]
fn a_resolved_slot_is_not_a_claim_in_progress_past_half_the_position_space() {
    let pos = usize::MAX / 2;
    let abandoned = SequenceWord::unwritten();
    abandoned.abandon(pos);
    assert!(!abandoned.is_claim_in_progress());
    let published = SequenceWord::unwritten();
    published.publish(pos);
    assert!(!published.is_claim_in_progress());
}

/// The encodings must not alias: a claim, a commit and an abandoned
/// reservation at the same position are three distinct words.
#[test]
fn the_three_states_are_distinct_at_every_position() {
    for pos in [0usize, 1, 2, 7, 4096] {
        let published = SlotState::published_word(pos);
        let abandoned = SlotState::abandoned_word(pos);
        assert_ne!(published, abandoned);
        assert_ne!(published, SlotState::VACANT);
        assert_ne!(abandoned, SlotState::VACANT);
    }
}

/// The bug this encoding exists to fix: with `cap == 1` a consumer
/// that skips an abandoned position must not read the same marker as
/// abandoned again at its next head.
#[test]
fn an_abandoned_marker_names_only_its_own_position() {
    let data: UnsafeCell<MaybeUninit<u32>> = UnsafeCell::new(MaybeUninit::uninit());
    let seq = AtomicUsize::new(SlotState::abandoned_word(0));
    assert!(matches!(
        SlotState::classify(&seq, &data, 0),
        SlotSnapshot::Tombstoned
    ));
    assert!(matches!(
        SlotState::classify(&seq, &data, 1),
        SlotSnapshot::NotReady
    ));
}

#[test]
fn only_published_slots_hold_a_value() {
    assert!(SlotState::Published.holds_value());
    assert!(!SlotState::Abandoned.holds_value());
    assert!(!SlotState::Vacant.holds_value());
}

#[test]
fn classify_is_exact_past_half_the_position_space() {
    let data: UnsafeCell<MaybeUninit<u32>> = UnsafeCell::new(MaybeUninit::uninit());
    let pos = usize::MAX / 2;
    let seq = AtomicUsize::new(SlotState::published_word(pos));
    assert!(matches!(
        SlotState::classify(&seq, &data, pos),
        SlotSnapshot::Ready(_)
    ));
    seq.store(SlotState::abandoned_word(pos), Ordering::Release);
    assert!(matches!(
        SlotState::classify(&seq, &data, pos),
        SlotSnapshot::Tombstoned
    ));
    assert!(matches!(
        SlotState::classify(&seq, &data, pos - 1),
        SlotSnapshot::NotReady
    ));
}

#[test]
fn classify_matches_only_the_named_position() {
    let data: UnsafeCell<MaybeUninit<u32>> = UnsafeCell::new(MaybeUninit::uninit());
    let seq = AtomicUsize::new(SlotState::abandoned_word(5));
    assert!(matches!(
        SlotState::classify(&seq, &data, 5),
        SlotSnapshot::Tombstoned
    ));
    assert!(matches!(
        SlotState::classify(&seq, &data, 6),
        SlotSnapshot::NotReady
    ));

    seq.store(SlotState::published_word(5), Ordering::Release);
    assert!(matches!(
        SlotState::classify(&seq, &data, 5),
        SlotSnapshot::Ready(_)
    ));
    assert!(matches!(
        SlotState::classify(&seq, &data, 4),
        SlotSnapshot::NotReady
    ));
}
