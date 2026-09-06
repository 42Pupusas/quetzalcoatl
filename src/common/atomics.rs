//! Atomic type aliases: loom's mocks when modeling, std otherwise.
//!
//! Loom intercepts every load, store, and read-modify-write on these
//! types to enumerate the schedules a real scheduler merely samples.
//! Any atomic that bypasses them is invisible to the model, so the
//! modeled primitives route their atomics through this module.
//!
//! Scope: the async wake machinery (`wake_async`, `waker_overflow`,
//! `park_registry`). Thread parking stays on std — loom does not mock
//! `std::thread::park`, so a parked model fiber would block the one
//! real thread the model runs on. The ring slot machinery is out of
//! scope for the same reason a model of it would be: far too large to
//! enumerate.
//!
//! Not a cargo feature: the switch is the `cfg(loom)` flag that cargo
//! itself sets from `[target.'cfg(loom)'.dependencies]`, so default
//! builds compile zero loom code and fetch nothing.

// With `async` off the wake machinery is compiled out and only
// `AtomicU64` has consumers, so the rest of the re-exports go unused.
#![cfg_attr(not(feature = "async"), allow(unused_imports))]

#[cfg(loom)]
pub use loom::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicU64};
#[cfg(not(loom))]
pub use std::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicU64};

/// Backs test-only counters; the name only exists under `cfg(test)`.
#[cfg(all(test, loom))]
pub use loom::sync::atomic::AtomicUsize;
#[cfg(all(test, not(loom)))]
pub use std::sync::atomic::AtomicUsize;
