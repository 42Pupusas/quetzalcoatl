use super::*;

#[test]
fn a_slot_never_used_has_no_prior_occupant() {
    assert!(SlotReuse::prior_occupant_resolved(
        &SequenceWord::unwritten()
    ));
}

#[test]
fn outstanding_reservation_blocks_reuse() {
    let seq = SequenceWord::unwritten();
    seq.claim();
    assert!(!SlotReuse::prior_occupant_resolved(&seq));
}

#[test]
fn published_or_abandoned_occupant_permits_reuse() {
    let published = SequenceWord::unwritten();
    published.publish(0);
    assert!(SlotReuse::prior_occupant_resolved(&published));

    let abandoned = SequenceWord::unwritten();
    abandoned.abandon(0);
    assert!(SlotReuse::prior_occupant_resolved(&abandoned));
}
