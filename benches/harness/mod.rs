//! Matched-semantics comparison workloads.
//!
//! Each workload exposes two families of methods:
//!
//! - `*_spin`: the caller retries a non-blocking try-operation and
//!   spins. Threads keep their core while they wait.
//! - `*_block`: the caller uses the blocking operation. Threads park
//!   and release their core while they wait.
//!
//! A benchmark must compare a method to a method of the same family.
//! A spin method against a block method measures the scheduling
//! policy, not the channel.

pub mod mpsc;
pub mod spsc;
