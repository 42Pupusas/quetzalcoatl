#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::perf)]
#![allow(clippy::missing_errors_doc)]

pub mod capacity;
pub(crate) mod common;
pub mod mpsc;
pub mod spsc;
pub mod broadcast;
