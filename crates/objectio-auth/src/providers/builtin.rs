//! Builtin AWS Signature identity provider
//!
//! This provider uses SigV4Verifier and SigV2Verifier with UserStore
//! for authentication. It supports both AWS Signature Version 4 and
//! Version 2 for compatibility with legacy clients.

use async_trait::async_trait;
use std::sync::Arc;

use crate::error::AuthError;
use crate::provider::{AuthProviderError, AuthRequest, AuthenticatedIdentity, IdentityProvider};
use crate::sigv2::SigV2Verifier;
use crate::sigv4::SigV4Verifier;
use crate::store::UserStore;
use crate::user::AuthResult;

/// Builtin AWS Signature identity provider using local UserStore
/// Supports both SigV4 (recommended) and SigV2 (legacy)
pub struct BuiltinSigV4Provider {
    sigv4_verifier: SigV4Verifier,
    sigv2_verifier: SigV2Verifier,
    user_store: Arc<UserStore>,
}

impl BuiltinSigV4Provider {
    /// Create a new builtin AWS auth provider
    pub fn new(user_store: Arc<UserStore>, region: impl Into<String>) -> Self {
        let sigv4_verifier = SigV4Verifier::new(user_store.clone(), region);
        let sigv2_verifier = SigV2Verifier::new(user_store.clone());
        Self {
            sigv4_verifier,
            sigv2_verifier,
            user_store,
        }
    }

    /// Get a reference to the user store
    pub fn user_store(&self) -> &Arc<UserStore> {
        &self.user_store
    }
}

#[async_trait]
impl IdentityProvider for BuiltinSigV4Provider {
    fn name(&self) -> &str {
        "builtin"
    }

    fn can_handle(&self, request: &AuthRequest<'_>) -> bool {
        request.has_aws_auth()
    }

    async fn authenticate(
        &self,
        request: &AuthRequest<'_>,
    ) -> Result<AuthenticatedIdentity, AuthProviderError> {
        // Build an http::Request from the AuthRequest for the verifier
        // The verifier needs a full http::Request, so we construct a minimal one
        let mut builder = http::Request::builder()
            .method(request.method)
            .uri(if let Some(query) = request.query {
                format!("{}?{}", request.path, query)
            } else {
                request.path.to_string()
            });

        // Copy headers
        for (name, value) in request.headers.iter() {
            builder = builder.header(name, value);
        }

        let http_request = builder
            .body(())
            .map_err(|e| AuthProviderError::Internal(format!("Failed to build request: {}", e)))?;

        // Verify the signature using the appropriate verifier
        let auth_result: AuthResult = if request.has_sigv4_auth() {
            self.sigv4_verifier.verify(&http_request)
        } else if request.has_sigv2_auth() {
            tracing::debug!("Using SigV2 authentication (legacy)");
            self.sigv2_verifier.verify(&http_request)
        } else {
            return Err(AuthProviderError::UnsupportedAuthMethod);
        }
        .map_err(|e| match e {
            AuthError::MissingAuthHeader => AuthProviderError::MissingAuth,
            AuthError::InvalidAuthHeader => AuthProviderError::InvalidCredentials,
            AuthError::InvalidSignatureVersion => AuthProviderError::UnsupportedAuthMethod,
            AuthError::SignatureMismatch => AuthProviderError::InvalidCredentials,
            AuthError::RequestExpired => AuthProviderError::TokenExpired,
            AuthError::UserNotFound(_) => AuthProviderError::InvalidCredentials,
            AuthError::AccessKeyNotFound(_) => AuthProviderError::InvalidCredentials,
            AuthError::UserSuspended => AuthProviderError::InvalidCredentials,
            AuthError::AccessKeyInactive => AuthProviderError::InvalidCredentials,
            _ => AuthProviderError::Internal(e.to_string()),
        })?;

        // Look up the user to get additional info (should succeed since verify passed)
        let user = self.user_store.get_user(&auth_result.user_id).ok();

        // Convert to AuthenticatedIdentity
        let mut identity = AuthenticatedIdentity::new(
            &auth_result.user_id,
            "builtin",
            &auth_result.user_arn,
        );

        if let Some(user) = user {
            identity.display_name = Some(user.display_name.clone());
            identity.email = user.email.clone();
        }

        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_can_handle_sigv4() {
        let user_store = Arc::new(UserStore::new());
        let provider = BuiltinSigV4Provider::new(user_store, "us-east-1");

        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=xxx"
                .parse()
                .unwrap(),
        );

        let request = AuthRequest::new("GET", "/bucket/key", &headers);
        assert!(provider.can_handle(&request));
    }

    #[tokio::test]
    async fn test_can_handle_sigv2() {
        let user_store = Arc::new(UserStore::new());
        let provider = BuiltinSigV4Provider::new(user_store, "us-east-1");

        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            "AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//yllqDzg="
                .parse()
                .unwrap(),
        );

        let request = AuthRequest::new("GET", "/bucket/key", &headers);
        assert!(provider.can_handle(&request));
    }

    #[tokio::test]
    async fn test_cannot_handle_bearer() {
        let user_store = Arc::new(UserStore::new());
        let provider = BuiltinSigV4Provider::new(user_store, "us-east-1");

        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer some-token".parse().unwrap(),
        );

        let request = AuthRequest::new("GET", "/bucket/key", &headers);
        assert!(!provider.can_handle(&request));
    }
}
