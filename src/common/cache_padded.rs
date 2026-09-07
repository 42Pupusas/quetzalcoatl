//! Padding that keeps neighbouring atomics off each other's cache line.
//!
//! Two atomics sharing a line are one variable as far as the coherence
//! protocol is concerned: a write to either invalidates the line for
//! every core holding it, so a producer touching its cursor stalls a
//! consumer reading the other. The contents stay correct and the
//! throughput collapses, which is why this is applied to the fields
//! that opposite endpoints touch concurrently rather than everywhere.

/// A value aligned to its own 64-byte cache line.
#[repr(align(64))]
pub struct CachePadded<T>(pub T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

#[cfg(test)]
mod tests {
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
}
