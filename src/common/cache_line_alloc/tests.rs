use super::CacheLineAlloc;

#[test]
fn a_byte_buffer_starts_on_a_cache_line() {
    let buf: Vec<u8, _> = Vec::with_capacity_in(3, CacheLineAlloc);
    assert_eq!(buf.as_ptr().addr() % 64, 0);
}

#[test]
fn a_wider_alignment_than_a_line_is_kept() {
    #[repr(align(128))]
    struct Wide(#[allow(dead_code)] u8);
    let wide = Box::new_in(Wide(0), CacheLineAlloc);
    assert_eq!(std::ptr::from_ref(&*wide).addr() % 128, 0);
}
