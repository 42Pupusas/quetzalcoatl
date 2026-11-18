use super::Cursors;
use crate::capacity::Capacity;
use std::sync::atomic::Ordering;

#[test]
fn a_fresh_pair_is_empty() {
    let cursors = Cursors::new();
    assert_eq!(cursors.len(), 0);
    assert!(cursors.is_empty());
}

#[test]
fn length_is_the_gap_between_the_cursors() {
    let cursors = Cursors::new();
    cursors.tail().store(7, Ordering::Relaxed);
    assert_eq!(cursors.len(), 7);
    cursors.publish_head(5);
    assert_eq!(cursors.len(), 2);
}

#[test]
fn a_ring_is_full_when_the_gap_reaches_capacity() {
    let cursors = Cursors::new();
    cursors.tail().store(4, Ordering::Relaxed);
    assert!(cursors.is_full(4));
    assert!(!cursors.is_full(8));
    cursors.publish_head(1);
    assert!(!cursors.is_full(4));
}

#[test]
fn length_survives_the_position_rollover() {
    let cursors = Cursors::new();
    cursors.publish_head(usize::MAX - 1);
    cursors
        .tail()
        .store(usize::MAX.wrapping_add(3), Ordering::Relaxed);
    assert_eq!(cursors.len(), 4);
}

#[test]
fn the_occupied_range_spans_the_unconsumed_positions() {
    let mut cursors = Cursors::new();
    cursors.publish_head(2);
    cursors.tail().store(5, Ordering::Relaxed);
    assert_eq!(
        cursors.occupied(Capacity::exact(4)).collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
}

/// A range from `head` to a rolled-over `tail` is empty, which
/// left these values undropped at teardown.
#[test]
fn the_occupied_positions_continue_across_the_rollover() {
    let mut cursors = Cursors::new();
    cursors.publish_head(usize::MAX - 1);
    cursors.tail().store(1, Ordering::Relaxed);
    assert_eq!(
        cursors.occupied(Capacity::exact(4)).collect::<Vec<_>>(),
        vec![usize::MAX - 1, usize::MAX, 0]
    );
}

#[test]
fn a_drained_ring_has_nothing_to_drop() {
    let mut cursors = Cursors::new();
    cursors.publish_head(5);
    cursors.tail().store(5, Ordering::Relaxed);
    assert!(cursors.occupied(Capacity::exact(4)).next().is_none());
}

#[test]
fn a_head_that_overtook_the_tail_yields_no_positions() {
    let mut cursors = Cursors::new();
    cursors.publish_head(9);
    cursors.tail().store(4, Ordering::Relaxed);
    assert!(cursors.occupied(Capacity::exact(4)).next().is_none());
}

#[test]
fn a_published_head_is_visible_to_another_thread() {
    let cursors = std::sync::Arc::new(Cursors::new());
    let peer = std::sync::Arc::clone(&cursors);
    let handle = std::thread::spawn(move || {
        while peer.head().load(Ordering::SeqCst) != 3 {
            std::hint::spin_loop();
        }
    });
    cursors.publish_head(3);
    handle.join().unwrap();
}

#[test]
fn each_cursor_occupies_its_own_cache_line() {
    assert_eq!(std::mem::align_of::<Cursors>(), 64);
    assert_eq!(std::mem::size_of::<Cursors>(), 128);
}
