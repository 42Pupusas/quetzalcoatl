use super::*;

#[test]
fn the_padding_claims_a_whole_cache_line() {
    assert_eq!(std::mem::align_of::<CachePadded<u8>>(), 64);
}

#[test]
fn two_padded_values_land_on_different_lines() {
    let pair = [CachePadded(0u64), CachePadded(0u64)];
    let first = std::ptr::from_ref(&pair[0]) as usize;
    let second = std::ptr::from_ref(&pair[1]) as usize;
    assert!(second - first >= 64);
}

#[test]
fn the_inner_value_is_reachable_by_deref() {
    assert_eq!(*CachePadded(7u32), 7);
}
