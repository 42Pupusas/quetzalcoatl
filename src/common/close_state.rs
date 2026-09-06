//! One side's "my peers are gone" flag.
//!
//! Every ring carries one of these per direction: a producer side that
//! the last producer sets on drop, and a consumer side that the last
//! consumer sets. A blocked peer reads it to decide whether to keep
//! waiting or give up and return.
//!
//! The orderings are not free choices. Closing and parking is a Dekker
//! handshake across *two* locations: the closer stores "closed" and
//! then loads "is anyone parked?", while the waiter stores "I am
//! parked" and then loads "closed?". `Release`/`Acquire` orders a store
//! against a later load of the *same* location and leaves this pair
//! unordered, so both sides may read the stale value: the closer sees
//! nobody parked and issues no wake, the waiter sees an open ring and
//! parks forever. Only a total order over both locations excludes that
//! interleaving, so [`close`](Self::close) stores `SeqCst` and
//! [`is_closed_for_parking`](Self::is_closed_for_parking) loads
//! `SeqCst`.
//!
//! Callers that are not about to park have no handshake to complete and
//! use [`is_closed`](Self::is_closed), whose `Acquire` load is the
//! cheaper half of the pair. Keeping both behind named methods is the
//! point of the type: the choice between them is a question about the
//! caller ("am I about to sleep?"), not a memory-ordering puzzle to be
//! re-solved at each of the thirty-odd sites that ask.

use super::atomics::AtomicBool;
use super::CachePadded;
use std::sync::atomic::Ordering;

/// A one-way close flag: set once by the last endpoint on its side,
/// read by the peers that would otherwise block forever.
pub struct CloseState {
    closed: CachePadded<AtomicBool>,
}

impl CloseState {
    #[cfg(not(loom))]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            closed: CachePadded(AtomicBool::new(false)),
        }
    }

    #[cfg(loom)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            closed: CachePadded(AtomicBool::new(false)),
        }
    }

    /// Publishes the close. `SeqCst` so the caller's following load of
    /// the peer's "parked" flag cannot be reordered before it; see the
    /// module docs.
    ///
    /// The caller must wake the parked peers *after* this returns —
    /// closing without waking leaves anyone already parked asleep.
    #[inline]
    pub fn close(&self) {
        self.closed.0.store(true, Ordering::SeqCst);
    }

    /// Reads the flag for a caller that will *not* park on the result.
    #[inline]
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.0.load(Ordering::Acquire)
    }

    /// Reads the flag as the second half of the park handshake, for a
    /// caller that parks when it returns `false`.
    #[inline]
    #[must_use]
    pub fn is_closed_for_parking(&self) -> bool {
        self.closed.0.load(Ordering::SeqCst)
    }
}

impl Default for CloseState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
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
}
