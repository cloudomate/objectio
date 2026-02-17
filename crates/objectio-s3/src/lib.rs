//! ObjectIO S3 API - S3-compatible HTTP API
//!
//! This crate implements the S3 REST API for ObjectIO.

pub mod auth;
pub mod error;
pub mod handlers;
pub mod metrics;
pub mod xml;

// Re-exports
pub use auth::SigV4Authenticator;
pub use error::S3Error;
pub use metrics::{
    IcebergOperation, OperationTimer, ProtectionConfig, S3Metrics, S3Operation, s3_metrics,
};
