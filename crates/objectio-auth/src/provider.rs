//! Pluggable identity provider trait and types
//!
//! This module defines the abstraction for identity providers, allowing
//! both builtin SigV4 authentication and external providers like OIDC/LDAP.

use async_trait::async_trait;
use std::collections::HashMap;

/// Authenticated identity from any provider
#[derive(Debug, Clone)]
pub struct AuthenticatedIdentity {
    /// Unique identifier (user ID, subject claim, DN, etc.)
    pub subject: String,
    /// Display name
    pub display_name: Option<String>,
    /// Email address
    pub email: Option<String>,
    /// Provider-specific attributes (groups, roles, custom claims)
    pub attributes: HashMap<String, Vec<String>>,
    /// Provider name (builtin, oidc, ldap)
    pub provider: String,
    /// Session/token expiry (if applicable)
    pub expires_at: Option<u64>,
    /// ARN-style identifier for policy matching
    pub arn: String,
}

impl AuthenticatedIdentity {
    /// Create a new authenticated identity
    pub fn new(
        subject: impl Into<String>,
        provider: impl Into<String>,
        arn: impl Into<String>,
    ) -> Self {
        Self {
            subject: subject.into(),
            display_name: None,
            email: None,
            attributes: HashMap::new(),
            provider: provider.into(),
            expires_at: None,
            arn: arn.into(),
        }
    }

    /// Set display name
    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }

    /// Set email
    pub fn with_email(mut self, email: impl Into<String>) -> Self {
        self.email = Some(email.into());
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, values: Vec<String>) -> Self {
        self.attributes.insert(key.into(), values);
        self
    }

    /// Set expiry timestamp
    pub fn with_expiry(mut self, expires_at: u64) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Get groups from attributes
    pub fn groups(&self) -> Vec<&str> {
        self.attributes
            .get("groups")
            .map(|g| g.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }
}

/// Request context for authentication
#[derive(Debug)]
pub struct AuthRequest<'a> {
    /// HTTP method
    pub method: &'a str,
    /// Request path
    pub path: &'a str,
    /// HTTP headers
    pub headers: &'a http::HeaderMap,
    /// Query parameters
    pub query: Option<&'a str>,
    /// Request body (for signature verification)
    pub body: Option<&'a [u8]>,
    /// Source IP address
    pub source_ip: Option<std::net::IpAddr>,
}

impl<'a> AuthRequest<'a> {
    /// Create a new auth request
    pub fn new(method: &'a str, path: &'a str, headers: &'a http::HeaderMap) -> Self {
        Self {
            method,
            path,
            headers,
            query: None,
            body: None,
            source_ip: None,
        }
    }

    /// Set query parameters
    pub fn with_query(mut self, query: &'a str) -> Self {
        self.query = Some(query);
        self
    }

    /// Set request body
    pub fn with_body(mut self, body: &'a [u8]) -> Self {
        self.body = Some(body);
        self
    }

    /// Set source IP
    pub fn with_source_ip(mut self, ip: std::net::IpAddr) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Get Authorization header value
    pub fn authorization_header(&self) -> Option<&str> {
        self.headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
    }

    /// Check if request has AWS SigV4 authentication
    pub fn has_sigv4_auth(&self) -> bool {
        self.authorization_header()
            .map(|s| s.starts_with("AWS4-HMAC-SHA256"))
            .unwrap_or(false)
    }

    /// Check if request has AWS SigV2 authentication
    pub fn has_sigv2_auth(&self) -> bool {
        self.authorization_header()
            .map(|s| s.starts_with("AWS ") && !s.starts_with("AWS4-"))
            .unwrap_or(false)
    }

    /// Check if request has any AWS signature authentication (SigV2 or SigV4)
    pub fn has_aws_auth(&self) -> bool {
        self.has_sigv4_auth() || self.has_sigv2_auth()
    }

    /// Check if request has Bearer token authentication
    pub fn has_bearer_auth(&self) -> bool {
        self.authorization_header()
            .map(|s| s.starts_with("Bearer "))
            .unwrap_or(false)
    }

    /// Check if request has Basic authentication
    pub fn has_basic_auth(&self) -> bool {
        self.authorization_header()
            .map(|s| s.starts_with("Basic "))
            .unwrap_or(false)
    }

    /// Extract Bearer token if present
    pub fn bearer_token(&self) -> Option<&str> {
        self.authorization_header()
            .filter(|s| s.starts_with("Bearer "))
            .map(|s| &s[7..])
    }
}

/// Authentication error types
#[derive(Debug, thiserror::Error)]
pub enum AuthProviderError {
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Token expired")]
    TokenExpired,
    #[error("Provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Missing authentication")]
    MissingAuth,
    #[error("Unsupported authentication method")]
    UnsupportedAuthMethod,
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Trait for pluggable identity providers
#[async_trait]
pub trait IdentityProvider: Send + Sync {
    /// Provider name for logging/metrics
    fn name(&self) -> &str;

    /// Check if this provider can handle the request
    /// (e.g., based on Authorization header format)
    fn can_handle(&self, request: &AuthRequest<'_>) -> bool;

    /// Authenticate the request and return identity
    async fn authenticate(
        &self,
        request: &AuthRequest<'_>,
    ) -> Result<AuthenticatedIdentity, AuthProviderError>;

    /// Optional: refresh/validate an existing identity
    async fn refresh(
        &self,
        identity: &AuthenticatedIdentity,
    ) -> Result<AuthenticatedIdentity, AuthProviderError> {
        // Default: return as-is (stateless providers)
        Ok(identity.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authenticated_identity_builder() {
        let identity =
            AuthenticatedIdentity::new("user123", "builtin", "arn:obio:iam::objectio:user/user123")
                .with_display_name("Test User")
                .with_email("test@example.com")
                .with_attribute("groups", vec!["admins".to_string(), "users".to_string()])
                .with_expiry(1234567890);

        assert_eq!(identity.subject, "user123");
        assert_eq!(identity.display_name, Some("Test User".to_string()));
        assert_eq!(identity.email, Some("test@example.com".to_string()));
        assert_eq!(identity.groups(), vec!["admins", "users"]);
        assert_eq!(identity.expires_at, Some(1234567890));
    }

    #[test]
    fn test_auth_request_helpers() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=...".parse().unwrap(),
        );

        let request = AuthRequest::new("GET", "/bucket/key", &headers);
        assert!(request.has_sigv4_auth());
        assert!(!request.has_bearer_auth());
        assert!(!request.has_basic_auth());
    }
}
