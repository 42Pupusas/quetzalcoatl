//! How many handles remain on one side of a ring.
//!
//! Every ring counts its endpoints: producers in spsc, mpsc, mpmc and
//! broadcast, consumers in spmc and mpmc. The count exists for exactly
//! one question — "was that the last one?" — because the last drop is
//! what closes the ring and releases the peers blocked against it.
//!
//! The two orderings differ and the difference is the whole reason
//! this is a type. Registering a new handle is `Relaxed`: the handle
//! cannot be used before it exists, so nothing needs ordering against
//! the increment. The drop is `AcqRel`, and that is not decoration.
//! The last endpoint is about to close the ring and wake the peers, so
//! every write made by the endpoints that dropped earlier has to be
//! visible to it first: `Release` publishes each departing endpoint's
//! writes, `Acquire` collects them in whichever endpoint reads 1. A
//! `Relaxed` decrement still identifies the last handle correctly and
//! still lets it observe a ring the earlier handles had not finished
//! writing.
//!
//! [`release`](EndpointCount::release) returns whether the caller was
//! the last, and its `#[must_use]` is the point: the answer is the
//! only reason to call it. A caller who discards it has decremented a
//! counter and dropped the close on the floor, which is the shape of
//! the bug this type exists to make unwritable.

use super::CachePadded;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The live handle count for one side of a ring.
pub struct EndpointCount {
    live: CachePadded<AtomicUsize>,
}

impl EndpointCount {
    /// A count holding the one endpoint that a fresh ring is split
    /// into.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            live: CachePadded(AtomicUsize::new(1)),
        }
    }

    /// Registers a newly cloned handle.
    ///
    /// `Relaxed`: the clone is not reachable by another thread until
    /// the caller publishes it, and that publication carries its own
    /// ordering.
    #[inline]
    pub fn register(&self) {
        self.live.fetch_add(1, Ordering::Relaxed);
    }

    /// Registers a newly cloned handle unless `limit` handles are
    /// already live, reporting whether it was registered.
    ///
    /// A rejected clone leaves the count untouched, so it cannot make a
    /// concurrent [`release`](Self::release) miss being the last.
    #[inline]
    #[must_use]
    pub fn try_register(&self, limit: usize) -> bool {
        self.live
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                (live < limit).then(|| live + 1)
            })
            .is_ok()
    }

    /// Retires a dropping handle, reporting whether it was the last.
    ///
    /// A `true` obliges the caller to close its side and wake the
    /// peers; see the module docs for why the answer must not be
    /// discarded, and why the ordering is `AcqRel`.
    #[inline]
    #[must_use]
    pub fn release(&self) -> bool {
        self.live.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// The number of live handles.
    ///
    /// Only meaningful as an after-the-fact observation: by the time a
    /// caller acts on it another thread may have cloned or dropped a
    /// handle. [`release`](Self::release) is the racefree question, so
    /// this stays confined to tests.
    #[cfg(test)]
    #[must_use]
    pub fn live(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }
}

impl Default for EndpointCount {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
