//! Station
//!
//! This crate for IPC communication over a network with multiple machines or locally with a single
//! machine. The implemented IPC patterns are IPC via a publish-subscribe model as well as a
//! request-response model. This library aims for flexibility in the networking protocol used to
//! transmit messages.

pub(crate) mod net;
pub(crate) mod pubsub;
pub(crate) mod rpc;

pub mod config;
pub mod process;

pub use config::Config;
pub use process::Process;
pub use rpc::RpcError;
