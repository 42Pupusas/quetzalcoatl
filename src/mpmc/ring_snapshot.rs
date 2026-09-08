//! A point-in-time reading of an MPMC ring's slot and park state, for
//! reporting a stalled ring.

use std::fmt;

use super::{Config, RingBuffer};
use crate::common::park::PARK_SLOTS;

/// Every word a stalled ring can be diagnosed from.
///
/// Taken under no lock; each field is one load, so the reading is
/// consistent only on a ring nothing is touching, which is the only
/// ring worth reading this way.
pub(super) struct RingSnapshot {
    claim: usize,
    ready: Vec<usize>,
    done: Vec<usize>,
    producer_park: u64,
    consumer_park: u64,
    awaited: Vec<(usize, Option<(u64, u32)>)>,
}

impl RingSnapshot {
    pub(super) fn take<T, C: Config>(ring: &RingBuffer<T, C>) -> Self {
        let (claim, ready, done) = ring.debug_snapshot();
        let (producer_park, consumer_park) = ring.debug_park_snapshot();
        let awaited = (0..PARK_SLOTS)
            .filter(|slot| producer_park & (1u64 << slot) != 0)
            .map(|slot| (slot, ring.awaited[slot].announced()))
            .collect();
        Self {
            claim,
            ready,
            done,
            producer_park,
            consumer_park,
            awaited,
        }
    }
}

impl fmt::Display for RingSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "claim = {}", self.claim)?;
        writeln!(f, "ready = {:?}", self.ready)?;
        writeln!(f, "done  = {:?}", self.done)?;
        writeln!(f, "producer_park = {:#x}", self.producer_park)?;
        writeln!(f, "consumer_park = {:#x}", self.consumer_park)?;
        writeln!(f, "awaited = {:?}", self.awaited)
    }
}
