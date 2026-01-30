//! Error types for ObjectIO
//!
//! This module defines the common error types used throughout the system.

use crate::types::{BucketNameError, ObjectKeyError};
use thiserror::Error;

/// Common result type for ObjectIO operations
pub type Result<T> = std::result::Result<T, Error>;

/// Common error type for ObjectIO
#[derive(Debug, Error)]
pub enum Error {
    // Storage errors
    #[error("disk I/O error: {0}")]
    DiskIo(#[from] std::io::Error),

    #[error("block not found: {block_id}")]
    BlockNotFound { block_id: String },

    #[error("insufficient disk space: required {required} bytes, available {available} bytes")]
    InsufficientSpace { required: u64, available: u64 },

    #[error("disk is full")]
    DiskFull,

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("data corruption detected")]
    DataCorruption,

    #[error("storage error: {0}")]
    Storage(String),

    // Erasure coding errors
    #[error("insufficient shards for reconstruction: have {available}, need {required}")]
    InsufficientShards { available: usize, required: usize },

    #[error("erasure coding error: {0}")]
    ErasureCoding(String),

    // Metadata errors
    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("object not found: {bucket}/{key}")]
    ObjectNotFound { bucket: String, key: String },

    #[error("object already exists: {bucket}/{key}")]
    ObjectAlreadyExists { bucket: String, key: String },

    #[error("invalid bucket name: {0}")]
    InvalidBucketName(#[from] BucketNameError),

    #[error("invalid object key: {0}")]
    InvalidObjectKey(#[from] ObjectKeyError),

    // Placement errors
    #[error("insufficient nodes for placement: have {available}, need {required}")]
    InsufficientNodes { available: usize, required: usize },

    #[error("insufficient racks for placement: have {available}, need {required}")]
    InsufficientRacks { available: usize, required: usize },

    #[error("insufficient datacenters for placement: have {available}, need {required}")]
    InsufficientDatacenters { available: usize, required: usize },

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("disk not found: {0}")]
    DiskNotFound(String),

    // S3 API errors
    #[error("access denied")]
    AccessDenied,

    #[error("invalid access key")]
    InvalidAccessKey,

    #[error("signature mismatch")]
    SignatureDoesNotMatch,

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("method not allowed: {0}")]
    MethodNotAllowed(String),

    #[error("precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("entity too large: max {max_size} bytes")]
    EntityTooLarge { max_size: u64 },

    // Multipart upload errors
    #[error("no such upload: {upload_id}")]
    NoSuchUpload { upload_id: String },

    #[error("invalid part: {part_number}")]
    InvalidPart { part_number: u32 },

    #[error("invalid part order")]
    InvalidPartOrder,

    // Network/RPC errors
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("request timeout")]
    Timeout,

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    // Internal errors
    #[error("internal error: {0}")]
    Internal(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),
}

impl Error {
    /// Create a new internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Create a not implemented error
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        Self::NotImplemented(feature.into())
    }

    /// Create an invalid request error
    pub fn invalid_request(msg: impl Into<String>) -> Self {
        Self::InvalidRequest(msg.into())
    }

    /// Create an invalid argument error
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }

    /// Check if this is a retryable error
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::ServiceUnavailable(_) | Self::ConnectionFailed(_)
        )
    }

    /// Check if this is a not found error
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::BucketNotFound(_)
                | Self::ObjectNotFound { .. }
                | Self::BlockNotFound { .. }
                | Self::NodeNotFound(_)
                | Self::DiskNotFound(_)
                | Self::NoSuchUpload { .. }
        )
    }

    /// Get HTTP status code for S3 API compatibility
    #[must_use]
    pub fn http_status_code(&self) -> u16 {
        match self {
            // 400 Bad Request
            Self::InvalidRequest(_)
            | Self::InvalidArgument(_)
            | Self::InvalidBucketName(_)
            | Self::InvalidObjectKey(_)
            | Self::InvalidPart { .. }
            | Self::InvalidPartOrder => 400,

            // 403 Forbidden
            Self::AccessDenied | Self::InvalidAccessKey | Self::SignatureDoesNotMatch => 403,

            // 404 Not Found
            Self::BucketNotFound(_)
            | Self::ObjectNotFound { .. }
            | Self::NoSuchUpload { .. }
            | Self::BlockNotFound { .. } => 404,

            // 405 Method Not Allowed
            Self::MethodNotAllowed(_) => 405,

            // 409 Conflict
            Self::BucketAlreadyExists(_) | Self::ObjectAlreadyExists { .. } => 409,

            // 412 Precondition Failed
            Self::PreconditionFailed(_) => 412,

            // 413 Payload Too Large
            Self::EntityTooLarge { .. } => 413,

            // 500 Internal Server Error
            Self::Internal(_)
            | Self::DiskIo(_)
            | Self::DataCorruption
            | Self::ChecksumMismatch { .. }
            | Self::ErasureCoding(_)
            | Self::Storage(_)
            | Self::Serialization(_)
            | Self::Deserialization(_) => 500,

            // 501 Not Implemented
            Self::NotImplemented(_) => 501,

            // 503 Service Unavailable
            Self::ServiceUnavailable(_)
            | Self::Timeout
            | Self::ConnectionFailed(_)
            | Self::InsufficientNodes { .. }
            | Self::InsufficientRacks { .. }
            | Self::InsufficientDatacenters { .. }
            | Self::InsufficientShards { .. }
            | Self::InsufficientSpace { .. }
            | Self::DiskFull
            | Self::Configuration(_)
            | Self::NodeNotFound(_)
            | Self::DiskNotFound(_) => 503,
        }
    }

    /// Get S3 error code for API compatibility
    #[must_use]
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::InvalidAccessKey => "InvalidAccessKeyId",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::BucketNotFound(_) => "NoSuchBucket",
            Self::BucketAlreadyExists(_) => "BucketAlreadyExists",
            Self::ObjectNotFound { .. } => "NoSuchKey",
            Self::InvalidBucketName(_) => "InvalidBucketName",
            Self::InvalidRequest(_) | Self::InvalidArgument(_) => "InvalidArgument",
            Self::EntityTooLarge { .. } => "EntityTooLarge",
            Self::NoSuchUpload { .. } => "NoSuchUpload",
            Self::InvalidPart { .. } => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::NotImplemented(_) => "NotImplemented",
            Self::ServiceUnavailable(_) | Self::Timeout => "ServiceUnavailable",
            _ => "InternalError",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_retryable() {
        assert!(Error::Timeout.is_retryable());
        assert!(Error::ServiceUnavailable("test".into()).is_retryable());
        assert!(!Error::AccessDenied.is_retryable());
    }

    #[test]
    fn test_error_not_found() {
        assert!(Error::BucketNotFound("test".into()).is_not_found());
        assert!(Error::ObjectNotFound {
            bucket: "b".into(),
            key: "k".into()
        }
        .is_not_found());
        assert!(!Error::AccessDenied.is_not_found());
    }

    #[test]
    fn test_error_http_status() {
        assert_eq!(Error::AccessDenied.http_status_code(), 403);
        assert_eq!(Error::BucketNotFound("test".into()).http_status_code(), 404);
        assert_eq!(Error::Internal("test".into()).http_status_code(), 500);
    }
}
