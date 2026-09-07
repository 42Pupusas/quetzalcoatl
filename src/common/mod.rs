//! Building blocks shared by the ring topologies.
//!
//! Each module here owns one concern the rings would otherwise
//! re-implement: the slot encoding, the backing allocation, the
//! endpoint counts and close flags, the park and wake machinery, and
//! the blocking loops built on top of them.

pub mod aligned_buf;
pub mod atomics;
pub mod backoff;
pub mod cache_padded;
pub mod close_state;
pub mod cursors;
#[cfg(test)]
pub mod drop_counter;
pub mod endpoint_count;
#[cfg(all(test, loom, feature = "async"))]
mod loom_models;
pub mod park;
#[cfg(all(test, loom))]
mod park_handshake_model;
#[cfg(test)]
pub mod park_probe;
#[cfg(feature = "async")]
pub mod park_registration;
pub mod park_registry;
#[cfg(all(test, feature = "async"))]
pub mod progress_watchdog;
pub mod seq_slot;
pub mod single_parker;
pub mod slot_sequence;
pub mod sole_parker;
pub mod thread_parker;
pub mod uncommitted_slot;
#[cfg(feature = "async")]
pub mod wake_async;
#[cfg(feature = "backstop-metrics")]
pub mod wake_delivery;
#[cfg(feature = "async")]
pub mod waker_overflow;

pub use aligned_buf::AlignedBuf;
pub use cache_padded::CachePadded;
#[cfg(test)]
pub use drop_counter::{BorrowedDropCounter, DropCounter};
pub use seq_slot::{SeqSlot, SlotSnapshot};
pub use slot_sequence::SlotSequence;
pub use uncommitted_slot::UncommittedSlot;
pub use single_parker::{SingleParkerConsumer, SingleParkerConsumerRef, SingleParkerProducer};
