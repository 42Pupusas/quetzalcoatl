use super::*;

#[test]
fn a_fresh_watermark_has_consumed_nothing() {
    assert_eq!(ConsumedWatermark::new().get(), 0);
}

#[test]
fn adding_advances_the_watermark() {
    let watermark = ConsumedWatermark::new();
    watermark.add(3);
    watermark.add(4);
    assert_eq!(watermark.get(), 7);
}

#[test]
fn the_watermark_keeps_its_own_cache_line() {
    assert_eq!(
        std::mem::align_of::<ConsumedWatermark>(),
        std::mem::align_of::<CachePadded<AtomicUsize>>()
    );
}

#[test]
fn an_untouched_ring_has_every_position_free() {
    assert_eq!(ConsumedWatermark::new().free(0, 8), Some(8));
}

#[test]
fn free_space_is_the_capacity_less_the_positions_in_flight() {
    let watermark = ConsumedWatermark::new();
    watermark.add(2);
    assert_eq!(watermark.free(5, 8), Some(5));
}

#[test]
fn a_ring_with_every_position_in_flight_reports_no_estimate() {
    assert_eq!(ConsumedWatermark::new().free(8, 8), None);
}

/// The watermark only ever understates progress, so a producer can
/// hold a claim beyond what it accounts for. That must read as "no
/// estimate" rather than wrapping into a huge free count.
#[test]
fn a_claim_beyond_the_watermark_reports_no_estimate() {
    assert_eq!(ConsumedWatermark::new().free(9, 8), None);
}

#[test]
fn free_space_survives_the_position_rollover() {
    let watermark = ConsumedWatermark::new();
    watermark.add(usize::MAX);
    assert_eq!(watermark.free(2, 8), Some(5));
}

#[test]
fn a_fresh_tally_publishes_nothing() {
    let watermark = ConsumedWatermark::new();
    ConsumedTally::new().flush(&watermark);
    assert_eq!(watermark.get(), 0);
}

#[test]
fn pops_below_the_threshold_stay_private() {
    let watermark = ConsumedWatermark::new();
    let tally = ConsumedTally::new();
    for _ in 0..3 {
        tally.record(&watermark, 4);
    }
    assert_eq!(watermark.get(), 0);
}

#[test]
fn reaching_the_threshold_publishes_the_whole_batch() {
    let watermark = ConsumedWatermark::new();
    let tally = ConsumedTally::new();
    for _ in 0..4 {
        tally.record(&watermark, 4);
    }
    assert_eq!(watermark.get(), 4);
}

#[test]
fn each_batch_publishes_once() {
    let watermark = ConsumedWatermark::new();
    let tally = ConsumedTally::new();
    for _ in 0..12 {
        tally.record(&watermark, 4);
    }
    assert_eq!(watermark.get(), 12);
}

#[test]
fn flushing_publishes_the_pops_that_never_filled_a_batch() {
    let watermark = ConsumedWatermark::new();
    let tally = ConsumedTally::new();
    for _ in 0..6 {
        tally.record(&watermark, 4);
    }
    tally.flush(&watermark);
    assert_eq!(watermark.get(), 6);
}

/// Flushing twice must not count the same pops again.
#[test]
fn a_flushed_tally_owes_nothing_further() {
    let watermark = ConsumedWatermark::new();
    let tally = ConsumedTally::new();
    tally.record(&watermark, 4);
    tally.flush(&watermark);
    tally.flush(&watermark);
    assert_eq!(watermark.get(), 1);
}

/// With `flush_at` of 1 every pop is published immediately, leaving
/// the tally with nothing to flush.
#[test]
fn a_threshold_of_one_never_holds_a_pop_back() {
    let watermark = ConsumedWatermark::new();
    let tally = ConsumedTally::new();
    tally.record(&watermark, 1);
    tally.record(&watermark, 1);
    assert_eq!(watermark.get(), 2);
    tally.flush(&watermark);
    assert_eq!(watermark.get(), 2);
}
