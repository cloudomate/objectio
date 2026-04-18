//! Iceberg REST Catalog authentication
//!
//! Provides:
//! - JWT validation middleware (validates Bearer tokens via external OIDC provider)
//! - OAuth2 token endpoint (`POST /v1/oauth/tokens`) for client_credentials grant
//!
//! The token endpoint proxies to the external OIDC provider's token endpoint,
//! so Iceberg clients (PyIceberg, Spark, Trino) can exchange credentials for
//! access tokens using the standard Iceberg REST catalog auth flow.

use crate::auth_middleware::AuthState;
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use objectio_auth::{AuthResult, IdentityProvider, OidcProvider};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, warn};

/// Combined auth state for Iceberg — supports SigV4 + optional OIDC
pub struct IcebergAuthState {
    pub sigv4_state: Arc<AuthState>,
    pub oidc_provider: Option<Arc<OidcProvider>>,
}

/// Unified Iceberg auth middleware — tries SigV4, then OIDC, then session cookie.
/// If none succeed, passes through unauthenticated (handlers check `Option<Extension<AuthResult>>`).
pub async fn iceberg_unified_auth_layer(
    State(state): State<Arc<IcebergAuthState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // 1. Try SigV4 (AWS Signature V4 — same credentials as S3)
    if let Some(ref header) = auth_header
        && (header.contains("AWS4-HMAC-SHA256") || header.starts_with("AWS "))
    {
        match crate::auth_middleware::parse_authorization_header(header) {
            Ok(parsed) => {
                if let Ok(cred) = state
                    .sigv4_state
                    .lookup_credential(parsed.access_key_id())
                    .await
                {
                    let verify_result = match &parsed {
                        crate::auth_middleware::ParsedAuth::V4 {
                            signed_headers,
                            signature,
                            ..
                        } => crate::auth_middleware::verify_request_v4(
                            &request,
                            signed_headers,
                            signature,
                            &cred,
                            &state.sigv4_state.region,
                        ),
                        crate::auth_middleware::ParsedAuth::V2 { signature, .. } => {
                            crate::auth_middleware::verify_request_v2(&request, signature, &cred)
                        }
                    };
                    if let Ok(auth_result) = verify_result {
                        debug!("Iceberg SigV4 auth: user={}", auth_result.user_id);
                        request.extensions_mut().insert(auth_result);
                        return next.run(request).await;
                    }
                }
            }
            Err(e) => {
                debug!("Iceberg SigV4 parse failed: {:?}", e);
            }
        }
    }

    // 2. Try OIDC Bearer token
    if let Some(ref header) = auth_header
        && header.starts_with("Bearer ")
        && let Some(ref oidc) = state.oidc_provider
    {
        let auth_request = objectio_auth::AuthRequest::new(
            request.method().as_str(),
            request.uri().path(),
            request.headers(),
        );
        match oidc.authenticate(&auth_request).await {
            Ok(identity) => {
                debug!("Iceberg OIDC auth: sub={}", identity.subject);
                let group_arns: Vec<String> = identity
                    .groups()
                    .iter()
                    .map(|g| format!("arn:obio:iam::oidc:group/{g}"))
                    .collect();
                let auth_result = AuthResult {
                    user_id: identity.subject.clone(),
                    user_arn: identity.arn.clone(),
                    access_key_id: format!("oidc:{}", identity.subject),
                    group_arns,
                    tenant: String::new(),
                };
                request.extensions_mut().insert(auth_result);
                return next.run(request).await;
            }
            Err(e) => {
                warn!("Iceberg OIDC auth failed: {}", e);
            }
        }
    }

    // 3. Try console session cookie
    if let Some(session) = crate::console_auth::validate_session_from_headers(request.headers()) {
        debug!("Iceberg session auth: user={}", session.user);
        let auth_result = AuthResult {
            user_id: session.user.clone(),
            user_arn: format!("arn:objectio:iam::user/{}", session.user),
            access_key_id: session.access_key,
            group_arns: Vec::new(),
            tenant: String::new(),
        };
        request.extensions_mut().insert(auth_result);
        return next.run(request).await;
    }

    // 4. No auth — pass through unauthenticated
    next.run(request).await
}

// Old OIDC-only middleware removed — replaced by iceberg_unified_auth_layer above

/// OAuth2 token request (form-encoded body from Iceberg clients)
#[derive(Debug, Deserialize)]
pub struct TokenRequest {
    pub grant_type: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
}

/// OAuth2 token endpoint handler (`POST /v1/oauth/tokens`).
///
/// Proxies the `client_credentials` grant to the external OIDC provider's
/// token endpoint. Iceberg clients call this first to get a Bearer token,
/// then use it on subsequent API calls.
pub async fn oauth_tokens(
    State(provider): State<Arc<OidcProvider>>,
    axum::Form(req): axum::Form<TokenRequest>,
) -> Response {
    if req.grant_type != "client_credentials" {
        let body = serde_json::json!({
            "error": "unsupported_grant_type",
            "error_description": format!(
                "Only client_credentials grant is supported, got: {}",
                req.grant_type
            )
        });
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    }

    // Use client_id/secret from request, or fall back to configured defaults
    let client_id = req
        .client_id
        .as_deref()
        .unwrap_or(&provider.config().client_id);
    let client_secret = req
        .client_secret
        .as_deref()
        .unwrap_or(&provider.config().client_secret);

    if client_id.is_empty() || client_secret.is_empty() {
        let body = serde_json::json!({
            "error": "invalid_client",
            "error_description": "client_id and client_secret are required"
        });
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    }

    match provider
        .exchange_client_credentials(client_id, client_secret, req.scope.as_deref())
        .await
    {
        Ok(token_resp) => (StatusCode::OK, axum::Json(token_resp)).into_response(),
        Err(e) => {
            warn!("OAuth2 token exchange failed: {}", e);
            let body = serde_json::json!({
                "error": "invalid_client",
                "error_description": e.to_string()
            });
            (StatusCode::UNAUTHORIZED, axum::Json(body)).into_response()
        }
    }
}
