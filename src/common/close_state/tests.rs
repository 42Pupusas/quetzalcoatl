use super::CloseState;

#[test]
fn a_fresh_state_is_open() {
    let state = CloseState::new();
    assert!(!state.is_closed());
    assert!(!state.is_closed_for_parking());
}

#[test]
fn closing_is_visible_to_both_readers() {
    let state = CloseState::new();
    state.close();
    assert!(state.is_closed());
    assert!(state.is_closed_for_parking());
}

#[test]
fn closing_twice_leaves_it_closed() {
    let state = CloseState::new();
    state.close();
    state.close();
    assert!(state.is_closed());
}

#[test]
fn a_close_is_visible_to_another_thread() {
    let state = std::sync::Arc::new(CloseState::new());
    let peer = std::sync::Arc::clone(&state);
    let handle = std::thread::spawn(move || {
        while !peer.is_closed_for_parking() {
            std::hint::spin_loop();
        }
    });
    state.close();
    handle.join().unwrap();
}

#[test]
fn the_flag_occupies_its_own_cache_line() {
    assert_eq!(std::mem::align_of::<CloseState>(), 64);
}
