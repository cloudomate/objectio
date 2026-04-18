//! External policy evaluator trait and types
//!
//! This module defines the abstraction for external policy engines,
//! allowing integration with OpenFGA, webhooks (MinIO-compatible), OPA, etc.

use async_trait::async_trait;
use std::collections::HashMap;

use crate::provider::AuthenticatedIdentity;

/// S3 action being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Action {
    // Object operations
    GetObject,
    PutObject,
    DeleteObject,
    HeadObject,
    CopyObject,
    ListObjectVersions,

    // Bucket operations
    CreateBucket,
    DeleteBucket,
    ListBucket,
    HeadBucket,
    GetBucketLocation,
    GetBucketPolicy,
    PutBucketPolicy,
    DeleteBucketPolicy,
    GetBucketAcl,
    PutBucketAcl,

    // Multipart operations
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListMultipartUploads,
    ListParts,

    // Service operations
    ListBuckets,
}

impl S3Action {
    /// Convert to S3 action string (e.g., "s3:GetObject")
    pub fn as_str(&self) -> &'static str {
        match self {
            S3Action::GetObject => "s3:GetObject",
            S3Action::PutObject => "s3:PutObject",
            S3Action::DeleteObject => "s3:DeleteObject",
            S3Action::HeadObject => "s3:GetObject", // HEAD uses GetObject permission
            S3Action::CopyObject => "s3:PutObject", // Copy is a write operation
            S3Action::ListObjectVersions => "s3:GetObjectVersion",

            S3Action::CreateBucket => "s3:CreateBucket",
            S3Action::DeleteBucket => "s3:DeleteBucket",
            S3Action::ListBucket => "s3:ListBucket",
            S3Action::HeadBucket => "s3:ListBucket", // HEAD uses ListBucket permission
            S3Action::GetBucketLocation => "s3:GetBucketLocation",
            S3Action::GetBucketPolicy => "s3:GetBucketPolicy",
            S3Action::PutBucketPolicy => "s3:PutBucketPolicy",
            S3Action::DeleteBucketPolicy => "s3:DeleteBucketPolicy",
            S3Action::GetBucketAcl => "s3:GetBucketAcl",
            S3Action::PutBucketAcl => "s3:PutBucketAcl",

            S3Action::CreateMultipartUpload => "s3:PutObject",
            S3Action::UploadPart => "s3:PutObject",
            S3Action::CompleteMultipartUpload => "s3:PutObject",
            S3Action::AbortMultipartUpload => "s3:AbortMultipartUpload",
            S3Action::ListMultipartUploads => "s3:ListBucketMultipartUploads",
            S3Action::ListParts => "s3:ListMultipartUploadParts",

            S3Action::ListBuckets => "s3:ListAllMyBuckets",
        }
    }

    /// Map to OpenFGA relation (for ReBAC)
    pub fn to_openfga_relation(&self) -> &'static str {
        match self {
            // Read operations
            S3Action::GetObject
            | S3Action::HeadObject
            | S3Action::ListBucket
            | S3Action::HeadBucket
            | S3Action::GetBucketLocation
            | S3Action::GetBucketPolicy
            | S3Action::GetBucketAcl
            | S3Action::ListObjectVersions
            | S3Action::ListMultipartUploads
            | S3Action::ListParts => "reader",

            // Write operations
            S3Action::PutObject
            | S3Action::DeleteObject
            | S3Action::CopyObject
            | S3Action::CreateMultipartUpload
            | S3Action::UploadPart
            | S3Action::CompleteMultipartUpload
            | S3Action::AbortMultipartUpload => "writer",

            // Owner operations
            S3Action::CreateBucket
            | S3Action::DeleteBucket
            | S3Action::PutBucketPolicy
            | S3Action::DeleteBucketPolicy
            | S3Action::PutBucketAcl
            | S3Action::ListBuckets => "owner",
        }
    }

    /// Parse from action string
    pub fn parse_action(s: &str) -> Option<Self> {
        match s {
            "s3:GetObject" => Some(S3Action::GetObject),
            "s3:PutObject" => Some(S3Action::PutObject),
            "s3:DeleteObject" => Some(S3Action::DeleteObject),
            "s3:CreateBucket" => Some(S3Action::CreateBucket),
            "s3:DeleteBucket" => Some(S3Action::DeleteBucket),
            "s3:ListBucket" => Some(S3Action::ListBucket),
            "s3:GetBucketPolicy" => Some(S3Action::GetBucketPolicy),
            "s3:PutBucketPolicy" => Some(S3Action::PutBucketPolicy),
            "s3:DeleteBucketPolicy" => Some(S3Action::DeleteBucketPolicy),
            "s3:ListAllMyBuckets" => Some(S3Action::ListBuckets),
            _ => None,
        }
    }
}

/// Full request context for policy evaluation
#[derive(Debug, Clone)]
pub struct ExternalPolicyRequest {
    /// Authenticated identity
    pub identity: AuthenticatedIdentity,
    /// S3 action
    pub action: S3Action,
    /// Bucket name
    pub bucket: String,
    /// Object key (if applicable)
    pub object_key: Option<String>,
    /// Is the requester the bucket owner?
    pub is_owner: bool,
    /// Source IP address
    pub source_ip: Option<std::net::IpAddr>,
    /// Request timestamp
    pub request_time: chrono::DateTime<chrono::Utc>,
    /// Additional context (headers, query params, etc.)
    pub context: HashMap<String, String>,
    /// Object size (for PUT operations)
    pub object_size: Option<u64>,
    /// Content type (for PUT operations)
    pub content_type: Option<String>,
}

impl ExternalPolicyRequest {
    /// Create a new policy request
    pub fn new(
        identity: AuthenticatedIdentity,
        action: S3Action,
        bucket: impl Into<String>,
    ) -> Self {
        Self {
            identity,
            action,
            bucket: bucket.into(),
            object_key: None,
            is_owner: false,
            source_ip: None,
            request_time: chrono::Utc::now(),
            context: HashMap::new(),
            object_size: None,
            content_type: None,
        }
    }

    /// Set object key
    pub fn with_object_key(mut self, key: impl Into<String>) -> Self {
        self.object_key = Some(key.into());
        self
    }

    /// Set owner flag
    pub fn with_is_owner(mut self, is_owner: bool) -> Self {
        self.is_owner = is_owner;
        self
    }

    /// Set source IP
    pub fn with_source_ip(mut self, ip: std::net::IpAddr) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Add context value
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// Set object size
    pub fn with_object_size(mut self, size: u64) -> Self {
        self.object_size = Some(size);
        self
    }

    /// Set content type
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Get the resource ARN for this request
    pub fn resource_arn(&self) -> String {
        match &self.object_key {
            Some(key) => format!("arn:obio:s3:::{}/{}", self.bucket, key),
            None => format!("arn:obio:s3:::{}", self.bucket),
        }
    }
}

/// External policy decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalPolicyDecision {
    /// Explicitly allowed
    Allow,
    /// Explicitly denied
    Deny,
}

impl ExternalPolicyDecision {
    /// Check if decision allows the action
    pub fn is_allowed(&self) -> bool {
        matches!(self, ExternalPolicyDecision::Allow)
    }
}

/// Error from external policy evaluation
#[derive(Debug, thiserror::Error)]
pub enum ExternalPolicyError {
    #[error("Policy engine unavailable: {0}")]
    Unavailable(String),
    #[error("Invalid policy: {0}")]
    InvalidPolicy(String),
    #[error("Timeout")]
    Timeout,
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Trait for pluggable external policy evaluators
///
/// This is used for external policy engines like OpenFGA, webhooks, OPA.
/// The builtin PolicyEvaluator (for bucket policies) is separate.
#[async_trait]
pub trait ExternalPolicyEvaluator: Send + Sync {
    /// Evaluator name for logging/metrics
    fn name(&self) -> &str;

    /// Evaluate policy for the request
    async fn evaluate(
        &self,
        request: &ExternalPolicyRequest,
    ) -> Result<ExternalPolicyDecision, ExternalPolicyError>;

    /// Health check for the policy engine
    async fn health_check(&self) -> bool {
        true // Default: always healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_action_to_string() {
        assert_eq!(S3Action::GetObject.as_str(), "s3:GetObject");
        assert_eq!(S3Action::PutObject.as_str(), "s3:PutObject");
        assert_eq!(S3Action::HeadObject.as_str(), "s3:GetObject"); // HEAD uses GetObject
    }

    #[test]
    fn test_s3_action_to_openfga_relation() {
        assert_eq!(S3Action::GetObject.to_openfga_relation(), "reader");
        assert_eq!(S3Action::PutObject.to_openfga_relation(), "writer");
        assert_eq!(S3Action::DeleteBucket.to_openfga_relation(), "owner");
    }

    #[test]
    fn test_policy_request_resource_arn() {
        let identity = crate::provider::AuthenticatedIdentity::new(
            "user1",
            "builtin",
            "arn:obio:iam::objectio:user/user1",
        );

        let request = ExternalPolicyRequest::new(identity.clone(), S3Action::GetObject, "mybucket");
        assert_eq!(request.resource_arn(), "arn:obio:s3:::mybucket");

        let request = ExternalPolicyRequest::new(identity, S3Action::GetObject, "mybucket")
            .with_object_key("mykey");
        assert_eq!(request.resource_arn(), "arn:obio:s3:::mybucket/mykey");
    }
}
