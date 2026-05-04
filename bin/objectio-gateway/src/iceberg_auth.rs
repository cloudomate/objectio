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
                    if let Ok(mut auth_result) = verify_result {
                        // Stitch group memberships so policies attached to
                        // an IAM group cascade to its members on the Unity/
                        // Iceberg path too. (Auth-middleware does the same
                        // for S3 routes.)
                        let (g_arns, g_ids) = state
                            .sigv4_state
                            .lookup_user_groups(&auth_result.user_id)
                            .await;
                        auth_result.group_arns = g_arns;
                        auth_result.group_ids = g_ids;
                        debug!(
                            "Iceberg SigV4 auth: user={} groups={:?}",
                            auth_result.user_id, auth_result.group_ids
                        );
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
                // Federation-shaped ARNs (always present, traceable to OIDC).
                let mut group_arns: Vec<String> = identity
                    .groups()
                    .iter()
                    .map(|g| format!("arn:obio:iam::oidc:group/{g}"))
                    .collect();
                // Bridge OIDC group claims → IAM groups by name. When the
                // claim's group name matches a local IAM group, attach BOTH
                // the IAM ARN (for any policy that addresses the IAM ARN
                // directly) and the IAM group_id (so policy walkers can
                // call list_attached_policies(group_id=...) for inherited
                // permissions). No-config: relies on naming convention
                // (Keycloak group "data-team" ↔ IAM group "data-team").
                let mut group_ids: Vec<String> = Vec::new();
                if !identity.groups().is_empty()
                    && let Ok(resp) = state
                        .sigv4_state
                        .meta_client
                        .clone()
                        .list_groups(objectio_proto::metadata::ListGroupsRequest {
                            max_results: 1000,
                            marker: String::new(),
                        })
                        .await
                {
                    let iam_groups = resp.into_inner().groups;
                    for claim in identity.groups() {
                        if let Some(g) =
                            iam_groups.iter().find(|g| g.group_name == *claim)
                        {
                            group_arns.push(g.arn.clone());
                            group_ids.push(g.group_id.clone());
                        }
                    }
                }
                let auth_result = AuthResult {
                    user_id: identity.subject.clone(),
                    user_arn: identity.arn.clone(),
                    access_key_id: format!("oidc:{}", identity.subject),
                    group_arns,
                    group_ids,
                    tenant: String::new(),
                    auth_mode: objectio_auth::AuthMode::Permanent,
                };
                request.extensions_mut().insert(auth_result);
                return next.run(request).await;
            }
            Err(e) => {
                warn!("Iceberg OIDC auth failed: {}", e);
            }
        }
    }

    // 3. Try console session cookie. The cookie payload only carries the
    // user UUID — synthesizing `arn:.../user/<UUID>` from it would never
    // match admin-by-name checks like `require_admin`. Resolve through meta
    // (cached after first hit) so the ARN matches what SigV4 produces:
    // `arn:objectio:iam::user/admin`, `arn:objectio:iam::tenant/<t>:user/<name>`, etc.
    if let Some(session) = crate::console_auth::validate_session_from_headers(request.headers()) {
        debug!(
            "Iceberg session auth: user={} access_key={}",
            session.user, session.access_key
        );
        // SSO sessions carry the literal sentinel "oidc" in place of an
        // access_key (OIDC users have no AK/SK pair). For those, look up
        // the user by id; for AK/SK sessions, the existing credential
        // lookup gives us the canonical ARN + tenant.
        let auth_result = if session.access_key == "oidc" {
            let mut meta = state.sigv4_state.meta_client.clone();
            match meta
                .get_user(objectio_proto::metadata::GetUserRequest {
                    user_id: session.user.clone(),
                })
                .await
            {
                Ok(resp) => match resp.into_inner().user {
                    Some(user) => {
                        let (group_arns, group_ids) =
                            state.sigv4_state.lookup_user_groups(&user.user_id).await;
                        AuthResult {
                            user_id: user.user_id,
                            user_arn: user.arn,
                            access_key_id: format!("session:{}", session.user),
                            group_arns,
                            group_ids,
                            tenant: user.tenant,
                            auth_mode: objectio_auth::AuthMode::Permanent,
                        }
                    }
                    None => {
                        warn!(
                            "SSO session user_id={} no longer exists in meta — rejecting",
                            session.user
                        );
                        return clear_session_response();
                    }
                },
                Err(e) => {
                    warn!(
                        "SSO session user lookup failed for {}: {:?} — rejecting",
                        session.user, e
                    );
                    return clear_session_response();
                }
            }
        } else {
            match state
                .sigv4_state
                .lookup_credential(&session.access_key)
                .await
            {
                Ok(cred) => {
                    let (group_arns, group_ids) =
                        state.sigv4_state.lookup_user_groups(&cred.user_id).await;
                    AuthResult {
                        user_id: cred.user_id,
                        user_arn: cred.user_arn,
                        access_key_id: cred.access_key_id,
                        group_arns,
                        group_ids,
                        tenant: cred.tenant,
                        auth_mode: objectio_auth::AuthMode::Permanent,
                    }
                }
                Err(e) => {
                    // The cookie is signed and unexpired but the access key it
                    // references no longer exists in meta (most often because
                    // the data dir was wiped between dev restarts; possible in
                    // prod after a key rotation). Returning 401 here forces the
                    // browser to drop the stale session and re-login — much
                    // safer than synthesizing a fake non-admin ARN that turns
                    // every privileged call into a confusing 403.
                    warn!(
                        "session cookie present but credential lookup failed for {}: {:?} — rejecting (client should re-login)",
                        session.access_key, e
                    );
                    return clear_session_response();
                }
            }
        };
        request.extensions_mut().insert(auth_result);
        return next.run(request).await;
    }

    // 4. No auth — pass through unauthenticated
    next.run(request).await
}

/// 401 + Set-Cookie that clears the stale browser session. Shared by
/// both AK/SK and SSO session paths so the browser drops the bad
/// cookie on the next 401 instead of looping. Cookie name must match
/// the one console_auth sets (`objectio-session`, with a dash) — the
/// underscore form silently fails to clear and the user gets an
/// unrecoverable 401 loop.
fn clear_session_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            axum::http::header::SET_COOKIE,
            "objectio-session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax",
        )],
        "session expired — please log in again",
    )
        .into_response()
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
