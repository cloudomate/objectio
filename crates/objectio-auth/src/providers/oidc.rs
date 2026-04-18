//! OIDC/JWT identity provider
//!
//! Validates JWT tokens from an external OIDC provider (Keycloak, Auth0, Okta, Google, etc.)
//! using the provider's JWKS endpoint for signature verification.
//!
//! Enable with the `oidc` feature flag.

use crate::provider::{AuthProviderError, AuthRequest, AuthenticatedIdentity, IdentityProvider};
use async_trait::async_trait;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use parking_lot::RwLock;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

/// OIDC provider configuration
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// OIDC issuer URL (e.g., `https://keycloak.example.com/realms/myrealm`)
    pub issuer_url: String,
    /// OAuth2 client ID (registered in the OIDC provider)
    pub client_id: String,
    /// OAuth2 client secret (for client_credentials grant)
    pub client_secret: String,
    /// Expected audience in JWT (often same as client_id)
    pub audience: String,
    /// Optional JWKS URI override (if not set, discovered from issuer)
    pub jwks_uri: Option<String>,
    /// Optional token endpoint override (if not set, discovered from issuer)
    pub token_endpoint: Option<String>,
    /// JWT claim name for groups/roles (varies by provider:
    /// Keycloak="roles", Azure AD="roles", Okta="groups", Auth0="permissions")
    pub groups_claim: String,
    /// JWT claim name for the user's role (optional, for admin detection)
    pub role_claim: String,
    /// Scopes to request when exchanging credentials (default: "openid profile email")
    pub scopes: String,
}

/// Cached JWKS keys with expiry
struct JwksCache {
    jwks: JwkSet,
    cached_at: Instant,
}

/// Cached OIDC discovery endpoints
struct DiscoveryCache {
    jwks_uri: String,
    token_endpoint: String,
    authorization_endpoint: String,
    cached_at: Instant,
}

/// OIDC/JWT identity provider
#[derive(Clone)]
pub struct OidcProvider {
    config: OidcConfig,
    http_client: reqwest::Client,
    jwks_cache: Arc<RwLock<Option<JwksCache>>>,
    discovery_cache: Arc<RwLock<Option<DiscoveryCache>>>,
}

/// OIDC discovery document
#[derive(Deserialize)]
struct OidcDiscovery {
    jwks_uri: String,
    token_endpoint: String,
    #[serde(default)]
    authorization_endpoint: String,
}

/// OAuth2 token response
#[derive(Debug, Clone, serde::Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub id_token: Option<String>,
}

/// OAuth2 token error response
#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

/// JWT claims we extract
#[derive(Debug, Deserialize)]
pub struct Claims {
    pub sub: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub exp: u64,
    // Groups are extracted dynamically via the configured claim name
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Cache TTL for JWKS keys (5 minutes)
const JWKS_CACHE_TTL_SECS: u64 = 300;

impl OidcProvider {
    /// Create a new OIDC provider
    pub fn new(config: OidcConfig) -> Self {
        info!(
            "OIDC provider initialized: issuer={}, client_id={}, audience={}",
            config.issuer_url, config.client_id, config.audience
        );
        Self {
            config,
            http_client: reqwest::Client::new(),
            jwks_cache: Arc::new(RwLock::new(None)),
            discovery_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the OIDC config (for exposing to handlers)
    pub fn config(&self) -> &OidcConfig {
        &self.config
    }

    /// Fetch and cache the OIDC discovery document
    async fn get_discovery(&self) -> Result<(String, String, String), AuthProviderError> {
        // Check cache (discovery endpoints rarely change, use same TTL)
        {
            let cache = self.discovery_cache.read();
            if let Some(ref cached) = *cache
                && cached.cached_at.elapsed().as_secs() < JWKS_CACHE_TTL_SECS
            {
                return Ok((
                    cached.jwks_uri.clone(),
                    cached.token_endpoint.clone(),
                    cached.authorization_endpoint.clone(),
                ));
            }
        }

        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            self.config.issuer_url.trim_end_matches('/')
        );

        debug!("Fetching OIDC discovery from {}", discovery_url);
        let discovery: OidcDiscovery = self
            .http_client
            .get(&discovery_url)
            .send()
            .await
            .map_err(|e| {
                AuthProviderError::ProviderUnavailable(format!("OIDC discovery failed: {e}"))
            })?
            .json()
            .await
            .map_err(|e| {
                AuthProviderError::ConfigurationError(format!("Invalid OIDC discovery: {e}"))
            })?;

        *self.discovery_cache.write() = Some(DiscoveryCache {
            jwks_uri: discovery.jwks_uri.clone(),
            token_endpoint: discovery.token_endpoint.clone(),
            authorization_endpoint: discovery.authorization_endpoint.clone(),
            cached_at: Instant::now(),
        });

        Ok((
            discovery.jwks_uri,
            discovery.token_endpoint,
            discovery.authorization_endpoint,
        ))
    }

    /// Get the JWKS URI (from config override or discovery)
    async fn resolve_jwks_uri(&self) -> Result<String, AuthProviderError> {
        if let Some(uri) = &self.config.jwks_uri {
            return Ok(uri.clone());
        }
        Ok(self.get_discovery().await?.0)
    }

    /// Get the authorization endpoint (from discovery)
    pub async fn resolve_authorization_endpoint(&self) -> Result<String, AuthProviderError> {
        Ok(self.get_discovery().await?.2)
    }

    /// Get the token endpoint (from config override or discovery)
    async fn resolve_token_endpoint(&self) -> Result<String, AuthProviderError> {
        if let Some(ep) = &self.config.token_endpoint {
            return Ok(ep.clone());
        }
        Ok(self.get_discovery().await?.1)
    }

    /// Exchange client credentials for an access token (OAuth2 client_credentials grant).
    /// Called by the `/iceberg/v1/oauth/tokens` endpoint.
    pub async fn exchange_client_credentials(
        &self,
        client_id: &str,
        client_secret: &str,
        scope: Option<&str>,
    ) -> Result<TokenResponse, AuthProviderError> {
        let token_endpoint = self.resolve_token_endpoint().await?;

        let scope = scope.unwrap_or(&self.config.scopes);

        debug!("Exchanging client credentials at {}", token_endpoint);
        let resp = self
            .http_client
            .post(&token_endpoint)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("scope", scope),
            ])
            .send()
            .await
            .map_err(|e| {
                AuthProviderError::ProviderUnavailable(format!("Token exchange failed: {e}"))
            })?;

        if resp.status().is_success() {
            resp.json::<TokenResponse>()
                .await
                .map_err(|e| AuthProviderError::Internal(format!("Invalid token response: {e}")))
        } else {
            let err = resp
                .json::<TokenErrorResponse>()
                .await
                .unwrap_or(TokenErrorResponse {
                    error: "unknown_error".to_string(),
                    error_description: None,
                });
            warn!(
                "Token exchange failed: {} {:?}",
                err.error, err.error_description
            );
            Err(AuthProviderError::InvalidCredentials)
        }
    }

    /// Exchange an authorization code for tokens (OAuth2 authorization_code grant).
    /// Used by the console OIDC login callback.
    pub async fn exchange_authorization_code(
        &self,
        code: &str,
        redirect_uri: &str,
    ) -> Result<TokenResponse, AuthProviderError> {
        let token_endpoint = self.resolve_token_endpoint().await?;

        debug!("Exchanging authorization code at {}", token_endpoint);
        let resp = self
            .http_client
            .post(&token_endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", redirect_uri),
                ("client_id", &self.config.client_id),
                ("client_secret", &self.config.client_secret),
            ])
            .send()
            .await
            .map_err(|e| {
                AuthProviderError::ProviderUnavailable(format!(
                    "Authorization code exchange failed: {e}"
                ))
            })?;

        if resp.status().is_success() {
            resp.json::<TokenResponse>()
                .await
                .map_err(|e| AuthProviderError::Internal(format!("Invalid token response: {e}")))
        } else {
            let err = resp
                .json::<TokenErrorResponse>()
                .await
                .unwrap_or(TokenErrorResponse {
                    error: "unknown_error".to_string(),
                    error_description: None,
                });
            warn!(
                "Authorization code exchange failed: {} {:?}",
                err.error, err.error_description
            );
            Err(AuthProviderError::InvalidCredentials)
        }
    }

    /// Fetch or return cached JWKS keys
    async fn get_jwks(&self, force_refresh: bool) -> Result<JwkSet, AuthProviderError> {
        // Check cache
        if !force_refresh {
            let cache = self.jwks_cache.read();
            if let Some(ref cached) = *cache
                && cached.cached_at.elapsed().as_secs() < JWKS_CACHE_TTL_SECS
            {
                return Ok(cached.jwks.clone());
            }
        }

        // Fetch fresh JWKS
        let jwks_uri = self.resolve_jwks_uri().await?;
        debug!("Fetching JWKS from {}", jwks_uri);

        let jwks: JwkSet = self
            .http_client
            .get(&jwks_uri)
            .send()
            .await
            .map_err(|e| AuthProviderError::ProviderUnavailable(format!("JWKS fetch failed: {e}")))?
            .json()
            .await
            .map_err(|e| AuthProviderError::ConfigurationError(format!("Invalid JWKS: {e}")))?;

        // Update cache
        *self.jwks_cache.write() = Some(JwksCache {
            jwks: jwks.clone(),
            cached_at: Instant::now(),
        });

        Ok(jwks)
    }

    /// Validate a JWT token and extract claims
    pub async fn validate_token(&self, token: &str) -> Result<Claims, AuthProviderError> {
        let header = decode_header(token).map_err(|e| {
            debug!("Invalid JWT header: {}", e);
            AuthProviderError::InvalidCredentials
        })?;

        let kid = header
            .kid
            .as_deref()
            .ok_or(AuthProviderError::InvalidCredentials)?;

        // Try cached JWKS first
        let jwks = self.get_jwks(false).await?;
        let jwk = match jwks.find(kid) {
            Some(jwk) => jwk.clone(),
            None => {
                // Key not found — force refresh in case of key rotation
                debug!("Key ID '{}' not in cache, refreshing JWKS", kid);
                let refreshed = self.get_jwks(true).await?;
                refreshed
                    .find(kid)
                    .cloned()
                    .ok_or(AuthProviderError::InvalidCredentials)?
            }
        };

        let algorithm = jwk
            .common
            .key_algorithm
            .and_then(|a| match a {
                jsonwebtoken::jwk::KeyAlgorithm::RS256 => Some(Algorithm::RS256),
                jsonwebtoken::jwk::KeyAlgorithm::RS384 => Some(Algorithm::RS384),
                jsonwebtoken::jwk::KeyAlgorithm::RS512 => Some(Algorithm::RS512),
                jsonwebtoken::jwk::KeyAlgorithm::ES256 => Some(Algorithm::ES256),
                jsonwebtoken::jwk::KeyAlgorithm::ES384 => Some(Algorithm::ES384),
                jsonwebtoken::jwk::KeyAlgorithm::PS256 => Some(Algorithm::PS256),
                jsonwebtoken::jwk::KeyAlgorithm::PS384 => Some(Algorithm::PS384),
                jsonwebtoken::jwk::KeyAlgorithm::PS512 => Some(Algorithm::PS512),
                _ => None,
            })
            .unwrap_or(Algorithm::RS256);

        let decoding_key = DecodingKey::from_jwk(&jwk).map_err(|e| {
            warn!("Failed to create decoding key from JWK: {}", e);
            AuthProviderError::ConfigurationError(format!("Invalid JWK: {e}"))
        })?;

        let mut validation = Validation::new(algorithm);
        validation.set_issuer(&[&self.config.issuer_url]);
        validation.set_audience(&[&self.config.audience]);
        validation.leeway = 60; // 60 seconds clock skew tolerance

        let token_data = decode::<Claims>(token, &decoding_key, &validation).map_err(|e| {
            debug!("JWT validation failed: {}", e);
            match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                    AuthProviderError::TokenExpired
                }
                _ => AuthProviderError::InvalidCredentials,
            }
        })?;

        Ok(token_data.claims)
    }

    /// Extract groups from claims using the configured claim name
    pub fn extract_groups(&self, claims: &Claims) -> Vec<String> {
        claims
            .extra
            .get(&self.config.groups_claim)
            .and_then(|v| {
                // Handle both array of strings and single string
                if let Some(arr) = v.as_array() {
                    Some(
                        arr.iter()
                            .filter_map(|s| s.as_str().map(String::from))
                            .collect(),
                    )
                } else if let Some(s) = v.as_str() {
                    Some(vec![s.to_string()])
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl IdentityProvider for OidcProvider {
    fn name(&self) -> &str {
        "oidc"
    }

    fn can_handle(&self, request: &AuthRequest<'_>) -> bool {
        request.has_bearer_auth()
    }

    async fn authenticate(
        &self,
        request: &AuthRequest<'_>,
    ) -> Result<AuthenticatedIdentity, AuthProviderError> {
        let token = request
            .bearer_token()
            .ok_or(AuthProviderError::MissingAuth)?;

        let claims = self.validate_token(token).await?;
        let groups = self.extract_groups(&claims);

        let subject = &claims.sub;
        let arn = format!("arn:obio:iam::oidc:user/{subject}");

        let mut identity = AuthenticatedIdentity::new(subject.clone(), "oidc", arn)
            .with_expiry(claims.exp)
            .with_attribute("groups", groups);

        if let Some(email) = claims.email {
            identity = identity.with_email(email);
        }
        if let Some(name) = claims.name {
            identity = identity.with_display_name(name);
        }

        debug!(
            "OIDC authenticated: sub={}, email={:?}",
            subject, identity.email
        );

        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_handle_bearer() {
        let provider = OidcProvider::new(OidcConfig {
            issuer_url: "https://example.com".to_string(),
            client_id: "test-client".to_string(),
            client_secret: String::new(),
            audience: "test".to_string(),
            jwks_uri: None,
            token_endpoint: None,
            groups_claim: "groups".to_string(),
            role_claim: "role".to_string(),
            scopes: "openid".to_string(),
        });

        let mut headers = http::HeaderMap::new();
        headers.insert("authorization", "Bearer eyJhbGci...".parse().unwrap());
        let req = AuthRequest::new("GET", "/", &headers);
        assert!(provider.can_handle(&req));

        let mut headers2 = http::HeaderMap::new();
        headers2.insert("authorization", "AWS4-HMAC-SHA256 ...".parse().unwrap());
        let req2 = AuthRequest::new("GET", "/", &headers2);
        assert!(!provider.can_handle(&req2));
    }

    #[test]
    fn test_extract_groups() {
        let provider = OidcProvider::new(OidcConfig {
            issuer_url: "https://example.com".to_string(),
            client_id: "test-client".to_string(),
            client_secret: String::new(),
            audience: "test".to_string(),
            jwks_uri: None,
            token_endpoint: None,
            groups_claim: "roles".to_string(),
            role_claim: "role".to_string(),
            scopes: "openid".to_string(),
        });

        let mut extra = std::collections::HashMap::new();
        extra.insert("roles".to_string(), serde_json::json!(["admin", "editor"]));
        let claims = Claims {
            sub: "user1".to_string(),
            email: None,
            name: None,
            exp: 0,
            extra,
        };

        let groups = provider.extract_groups(&claims);
        assert_eq!(groups, vec!["admin", "editor"]);
    }
}
