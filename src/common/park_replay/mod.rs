//! Schedule replay of the park handshake against its Lean model.
//!
//! `proofs/Qtz/ParkSafe.lean` proves that the model never loses a wake.
//! The proof covers the Rust only as far as the model takes the same
//! steps the Rust does. This checks that, for small configurations.
//!
//! Each test runs `lake exe park_graph <name>` in `proofs/`, which
//! prints the model's reachable step graph for that configuration, so
//! the tests need `lake` and always check against the current model.
//! Here the real blocking code runs one model step at a time: every
//! actor stops at each [`ReplayPoint`](super::replay_point::ReplayPoint)
//! and a [`Session`] picks which one moves next. The explorer tries
//! every choice in every state it reaches, breadth first, maps each
//! state to the model's encoding, and fails on a step the model has no
//! edge for. Model edges the Rust never takes are reported, not failed:
//! the Rust picks one wake target where the model allows any.
//!
//! Each flavor replays its own `push_block` against its variant: mpsc
//! (`src/mpsc/replay.rs`), broadcast and mpmc likewise. A run marks its
//! ring's producer wake set with
//! [`ParkGate::follow_in_replay`](super::park_gate::ParkGate::follow_in_replay),
//! so the stops inside the other wake sets, such as mpmc's consumer
//! side, do not stop.

mod explore;
mod fixture;
mod session;
mod snapshot;

pub use explore::{Explorer, Live};
pub use fixture::{ModelGraph, RunConfig};
pub use session::Session;
pub use snapshot::ReplayTarget;
