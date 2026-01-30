//! ObjectIO Client - Internal RPC clients
//!
//! This crate provides gRPC clients for inter-service communication.

pub mod meta;
pub mod osd;

// Re-exports
pub use meta::MetaClient;
pub use osd::OsdClient;
