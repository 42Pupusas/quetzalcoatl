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
mod tests;
