//! ObjectIO Common - Shared types and utilities
//!
//! This crate provides common types, error definitions, and utilities
//! used across all ObjectIO components.

pub mod checksum;
pub mod config;
pub mod error;
pub mod types;

pub use checksum::{Checksum, ChecksumCalculator};
pub use config::Config;
pub use error::{Error, Result};
pub use types::*;
