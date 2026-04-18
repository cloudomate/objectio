//! Authentication error types

use thiserror::Error;

/// Authentication and authorization errors
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing authorization header")]
    MissingAuthHeader,

    #[error("invalid authorization header format")]
    InvalidAuthHeader,

    #[error("invalid signature version: expected AWS4-HMAC-SHA256")]
    InvalidSignatureVersion,

    #[error("missing credential")]
    MissingCredential,

    #[error("invalid credential format")]
    InvalidCredentialFormat,

    #[error("access key not found: {0}")]
    AccessKeyNotFound(String),

    #[error("access key is inactive")]
    AccessKeyInactive,

    #[error("user is suspended")]
    UserSuspended,

    #[error("signature mismatch")]
    SignatureMismatch,

    #[error("request has expired")]
    RequestExpired,

    #[error("missing required signed header: {0}")]
    MissingSignedHeader(String),

    #[error("invalid date format")]
    InvalidDateFormat,

    #[error("missing x-amz-date or date header")]
    MissingDateHeader,

    #[error("access denied")]
    AccessDenied,

    #[error("user not found: {0}")]
    UserNotFound(String),

    #[error("user already exists: {0}")]
    UserAlreadyExists(String),

    #[error("internal error: {0}")]
    Internal(String),
}
