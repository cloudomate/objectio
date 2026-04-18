//! S3 error types and responses

use thiserror::Error;

/// S3-specific error type
#[derive(Debug, Error)]
pub enum S3Error {
    #[error("access denied")]
    AccessDenied,

    #[error("no such bucket: {0}")]
    NoSuchBucket(String),

    #[error("no such key: {0}")]
    NoSuchKey(String),

    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl S3Error {
    /// Get the S3 error code
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::NoSuchBucket(_) => "NoSuchBucket",
            Self::NoSuchKey(_) => "NoSuchKey",
            Self::BucketAlreadyExists(_) => "BucketAlreadyExists",
            Self::Internal(_) => "InternalError",
        }
    }

    /// Get the HTTP status code
    #[must_use]
    pub const fn status_code(&self) -> u16 {
        match self {
            Self::AccessDenied => 403,
            Self::NoSuchBucket(_) | Self::NoSuchKey(_) => 404,
            Self::BucketAlreadyExists(_) => 409,
            Self::Internal(_) => 500,
        }
    }
}
