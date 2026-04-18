//! Console authentication — session-token based login for the web console.
//!
//! Supports two login methods:
//! 1. **AK/SK**: `POST /_console/api/login` with access key + secret key
//! 2. **OIDC SSO**: `GET /_console/api/oidc/authorize` → redirect to provider → callback
//!
//! Both methods result in the same `objectio-session` cookie.

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use hmac::{Hmac, Mac};
use objectio_proto::metadata::GetAccessKeyForAuthRequest;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::s3::AppState;

/// State for OIDC console routes
pub struct ConsoleOidcState {
    pub oidc_provider: Option<Arc<objectio_auth::OidcProvider>>,
    pub external_endpoint: String,
    pub meta_client: objectio_proto::metadata::metadata_service_client::MetadataServiceClient<
        tonic::transport::Channel,
    >,
}

/// Build an OidcProvider from a stored identity config (identity/openid/{name})
fn build_oidc_provider_from_config(
    config: &serde_json::Value,
) -> Option<objectio_auth::OidcProvider> {
    let issuer_url = config.get("issuer_url")?.as_str()?.to_string();
    let client_id = config.get("client_id")?.as_str()?.to_string();
    let client_secret = config
        .get("client_secret")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let audience = config
        .get("audience")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&client_id)
        .to_string();
    let scopes = config
        .get("scopes")
        .and_then(|v| v.as_str())
        .unwrap_or("openid profile email")
        .to_string();
    let groups_claim = config
        .get("claim_name")
        .and_then(|v| v.as_str())
        .unwrap_or("groups")
        .to_string();

    Some(objectio_auth::OidcProvider::new(
        objectio_auth::OidcConfig {
            issuer_url,
            client_id,
            client_secret,
            audience,
            jwks_uri: None,
            token_endpoint: None,
            groups_claim,
            role_claim: String::new(),
            scopes,
        },
    ))
}

type HmacSha256 = Hmac<Sha256>;

/// Secret used to sign session tokens — derived from a fixed prefix.
/// In production this should be a configurable secret.
const TOKEN_SECRET: &[u8] = b"objectio-console-session-v1";

/// Session token validity: 24 hours
const SESSION_TTL_SECS: u64 = 86400;

#[derive(Deserialize)]
pub struct LoginRequest {
    #[serde(rename = "accessKey")]
    access_key: String,
    #[serde(rename = "secretKey")]
    secret_key: String,
}

#[derive(Serialize)]
pub struct SessionInfo {
    /// Canonical user id (UUID). Used as the lookup key for meta and for
    /// IAM-policy `user_id` attachments.
    pub user: String,
    pub access_key: String,
    pub expires_at: u64,
    pub tenant: String,
    /// Human-readable display name for the console UI. Populated on login
    /// and by `/_console/api/session` (via a meta lookup); empty on paths
    /// where only the signed session payload is available.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub display_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub email: String,
}

/// POST /_console/api/login
pub async fn console_login(
    State(state): State<Arc<AppState>>,
    Json(body): Json<LoginRequest>,
) -> Response {
    // Validate credentials via meta service
    let mut client = state.meta_client.clone();
    let resp = match client
        .get_access_key_for_auth(GetAccessKeyForAuthRequest {
            access_key_id: body.access_key.clone(),
        })
        .await
    {
        Ok(r) => r.into_inner(),
        Err(_) => {
            return (StatusCode::UNAUTHORIZED, "Invalid credentials").into_response();
        }
    };

    let access_key_meta = match resp.access_key {
        Some(ak) => ak,
        None => {
            return (StatusCode::UNAUTHORIZED, "Invalid credentials").into_response();
        }
    };

    if access_key_meta.secret_access_key != body.secret_key {
        return (StatusCode::UNAUTHORIZED, "Invalid credentials").into_response();
    }

    let user_id = access_key_meta.user_id;
    let (tenant, display_name, email) = match resp.user {
        Some(u) => (u.tenant, u.display_name, u.email),
        None => (String::new(), String::new(), String::new()),
    };

    // Build session token: base64(user_id|access_key|tenant|expires_at|hmac)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let expires = now + SESSION_TTL_SECS;

    let payload = format!("{user_id}|{}|{tenant}|{expires}", body.access_key);
    let sig = sign_payload(&payload);
    let token = format!("{payload}|{sig}");
    let token_b64 = base64_encode(&token);

    // Set cookie
    let cookie = format!(
        "objectio-session={token_b64}; Path=/; HttpOnly; SameSite=Strict; Max-Age={SESSION_TTL_SECS}"
    );

    let session = SessionInfo {
        user: user_id,
        access_key: body.access_key,
        expires_at: expires,
        tenant: tenant.clone(),
        display_name,
        email,
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::SET_COOKIE, cookie)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            serde_json::to_string(&session).unwrap_or_default(),
        ))
        .unwrap()
}

/// GET /_console/api/session
pub async fn console_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let Some(mut info) = validate_session_from_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, "No valid session").into_response();
    };
    // Enrich with display_name + email so the console can show a friendly
    // label instead of the raw user_id UUID. Best-effort: a failed lookup
    // just falls through with the id.
    if info.display_name.is_empty()
        && let Ok(resp) = state
            .meta_client
            .clone()
            .get_user(objectio_proto::metadata::GetUserRequest {
                user_id: info.user.clone(),
            })
            .await
        && let Some(u) = resp.into_inner().user
    {
        info.display_name = u.display_name;
        info.email = u.email;
    }
    Json(info).into_response()
}

/// POST /_console/api/logout
pub async fn console_logout() -> Response {
    let cookie = "objectio-session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0";
    Response::builder()
        .status(StatusCode::OK)
        .header(header::SET_COOKIE, cookie)
        .body(axum::body::Body::from("{}"))
        .unwrap()
}

/// Validate session token from request headers (cookie or Authorization bearer).
/// Returns `Some(SessionInfo)` if valid, `None` otherwise.
pub fn validate_session_from_headers(headers: &HeaderMap) -> Option<SessionInfo> {
    // Try cookie first
    let token_b64 = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|c| {
                let c = c.trim();
                c.strip_prefix("objectio-session=")
            })
        })
        // Fallback: Authorization: Bearer <token>
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
        })?;

    let token = base64_decode(token_b64)?;

    // Parse: user_id|access_key|tenant|expires_at|sig
    let (payload, sig) = token.rsplit_once('|')?;

    // Verify HMAC
    let expected_sig = sign_payload(payload);
    if sig != expected_sig {
        // Try legacy format (colon-delimited, no tenant)
        return validate_legacy_token(&token);
    }

    // Parse payload fields: user_id|access_key|tenant|expires_at
    let fields: Vec<&str> = payload.splitn(4, '|').collect();
    if fields.len() != 4 {
        return None;
    }
    let user_id = fields[0];
    let access_key = fields[1];
    let tenant = fields[2];
    let expires_at: u64 = fields[3].parse().ok()?;

    // Check expiry
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now > expires_at {
        return None;
    }

    Some(SessionInfo {
        user: user_id.to_string(),
        access_key: access_key.to_string(),
        expires_at,
        tenant: tenant.to_string(),
        display_name: String::new(),
        email: String::new(),
    })
}

/// Parse legacy colon-delimited tokens (pre-tenant format)
fn validate_legacy_token(token: &str) -> Option<SessionInfo> {
    let (payload, sig) = token.rsplit_once(':')?;
    let expected_sig = sign_payload(payload);
    if sig != expected_sig {
        return None;
    }
    let fields: Vec<&str> = payload.splitn(3, ':').collect();
    if fields.len() != 3 {
        return None;
    }
    let expires_at: u64 = fields[2].parse().ok()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now > expires_at {
        return None;
    }
    Some(SessionInfo {
        user: fields[0].to_string(),
        access_key: fields[1].to_string(),
        expires_at,
        tenant: String::new(),
        display_name: String::new(),
        email: String::new(),
    })
}

fn sign_payload(payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(TOKEN_SECRET).expect("HMAC key");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn base64_encode(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes())
}

fn base64_decode(s: &str) -> Option<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .ok()?;
    String::from_utf8(bytes).ok()
}

// ============================================================
// Self-service: My Account (any authenticated user)
// ============================================================

/// GET /_console/api/me/keys — list own access keys
pub async fn my_list_keys(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let session = match validate_session_from_headers(&headers) {
        Some(s) => s,
        None => return (StatusCode::UNAUTHORIZED, "No session").into_response(),
    };

    // The session user is a user_id (UUID). Look up their access keys.
    let mut client = state.meta_client.clone();
    match client
        .list_access_keys(objectio_proto::metadata::ListAccessKeysRequest {
            user_id: session.user.clone(),
        })
        .await
    {
        Ok(resp) => {
            let keys: Vec<serde_json::Value> = resp
                .into_inner()
                .access_keys
                .iter()
                .map(|k| {
                    serde_json::json!({
                        "access_key_id": k.access_key_id,
                        "status": k.status,
                        "created_at": k.created_at,
                    })
                })
                .collect();
            axum::Json(serde_json::json!({ "access_keys": keys })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

/// POST /_console/api/me/keys — create own access key
pub async fn my_create_key(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let session = match validate_session_from_headers(&headers) {
        Some(s) => s,
        None => return (StatusCode::UNAUTHORIZED, "No session").into_response(),
    };

    let mut client = state.meta_client.clone();
    match client
        .create_access_key(objectio_proto::metadata::CreateAccessKeyRequest {
            user_id: session.user.clone(),
        })
        .await
    {
        Ok(resp) => {
            let key = resp.into_inner().access_key.unwrap_or_default();
            axum::Json(serde_json::json!({
                "access_key_id": key.access_key_id,
                "secret_access_key": key.secret_access_key,
                "created_at": key.created_at,
            }))
            .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

/// DELETE /_console/api/me/keys/{key_id} — delete own access key
pub async fn my_delete_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(key_id): axum::extract::Path<String>,
) -> Response {
    let session = match validate_session_from_headers(&headers) {
        Some(s) => s,
        None => return (StatusCode::UNAUTHORIZED, "No session").into_response(),
    };

    // Verify the key belongs to this user first
    let mut client = state.meta_client.clone();
    if let Ok(resp) = client
        .list_access_keys(objectio_proto::metadata::ListAccessKeysRequest {
            user_id: session.user.clone(),
        })
        .await
    {
        let owns_key = resp
            .into_inner()
            .access_keys
            .iter()
            .any(|k| k.access_key_id == key_id);
        if !owns_key {
            return (StatusCode::FORBIDDEN, "Key does not belong to you").into_response();
        }
    }

    match client
        .delete_access_key(objectio_proto::metadata::DeleteAccessKeyRequest {
            access_key_id: key_id,
        })
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

// ============================================================
// OIDC SSO Login
// ============================================================

/// GET /_console/api/oidc/enabled — check if OIDC is configured, list providers
pub async fn oidc_enabled(State(state): State<Arc<ConsoleOidcState>>) -> Json<serde_json::Value> {
    let has_global = state.oidc_provider.is_some();

    // List per-tenant OIDC providers from config store
    let mut providers = Vec::new();

    // Fetch tenant OIDC configs from meta service
    let mut client = state.meta_client.clone();
    if let Ok(resp) = client
        .list_config(objectio_proto::metadata::ListConfigRequest {
            prefix: "identity/openid/".to_string(),
        })
        .await
    {
        for entry in &resp.into_inner().entries {
            let provider_name = entry
                .key
                .strip_prefix("identity/openid/")
                .unwrap_or(&entry.key);
            if let Ok(config) = serde_json::from_slice::<serde_json::Value>(&entry.value) {
                let display = config
                    .get("display_name")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("SSO");
                let enabled = config
                    .get("enabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                if enabled {
                    providers.push(serde_json::json!({
                        "name": provider_name,
                        "label": format!("User SSO{}", if display != "SSO" { format!(" ({display})") } else { String::new() }),
                    }));
                }
            }
        }
    }

    // Add global/system SSO if configured
    if has_global {
        providers.insert(
            0,
            serde_json::json!({
                "name": "system",
                "label": "System SSO",
            }),
        );
    }

    Json(serde_json::json!({
        "enabled": has_global || !providers.is_empty(),
        "providers": providers,
    }))
}

/// Query params for authorize
#[derive(Deserialize, Default)]
pub struct AuthorizeParams {
    /// OIDC provider name (e.g. "system" for global, "entra" for a stored config)
    #[serde(default)]
    pub provider: String,
    /// Tenant name (looks up tenant's oidc_provider)
    #[serde(default)]
    pub tenant: String,
}

/// GET /_console/api/oidc/authorize — redirect to OIDC provider
pub async fn oidc_authorize(
    State(state): State<Arc<ConsoleOidcState>>,
    Query(params): Query<AuthorizeParams>,
) -> Response {
    // Resolve the OIDC provider: by provider name, by tenant, or global default
    let (oidc, tenant_name) = if !params.provider.is_empty() && params.provider != "system" {
        // Look up named provider from config store
        let mut client = state.meta_client.clone();
        let config_key = format!("identity/openid/{}", params.provider);
        match client
            .get_config(objectio_proto::metadata::GetConfigRequest { key: config_key })
            .await
        {
            Ok(resp) => {
                let entry = resp.into_inner().entry.unwrap_or_default();
                match serde_json::from_slice::<serde_json::Value>(&entry.value) {
                    Ok(config) => match build_oidc_provider_from_config(&config) {
                        Some(p) => (p, params.tenant.clone()),
                        None => {
                            return (StatusCode::BAD_REQUEST, "Invalid OIDC config")
                                .into_response();
                        }
                    },
                    Err(_) => {
                        return (StatusCode::BAD_REQUEST, "Invalid OIDC config").into_response();
                    }
                }
            }
            Err(_) => return (StatusCode::NOT_FOUND, "OIDC provider not found").into_response(),
        }
    } else if !params.tenant.is_empty() {
        // Look up tenant's OIDC provider
        let mut client = state.meta_client.clone();
        match client
            .get_tenant(objectio_proto::metadata::GetTenantRequest {
                name: params.tenant.clone(),
            })
            .await
        {
            Ok(resp) => {
                let tc = resp.into_inner().tenant.unwrap_or_default();
                if tc.oidc_provider.is_empty() {
                    return (
                        StatusCode::BAD_REQUEST,
                        "Tenant has no OIDC provider configured",
                    )
                        .into_response();
                }
                let config_key = format!("identity/openid/{}", tc.oidc_provider);
                match client
                    .get_config(objectio_proto::metadata::GetConfigRequest { key: config_key })
                    .await
                {
                    Ok(resp2) => {
                        let entry = resp2.into_inner().entry.unwrap_or_default();
                        match serde_json::from_slice::<serde_json::Value>(&entry.value) {
                            Ok(config) => match build_oidc_provider_from_config(&config) {
                                Some(p) => (p, params.tenant.clone()),
                                None => {
                                    return (StatusCode::BAD_REQUEST, "Invalid OIDC config")
                                        .into_response();
                                }
                            },
                            Err(_) => {
                                return (StatusCode::BAD_REQUEST, "Invalid OIDC config")
                                    .into_response();
                            }
                        }
                    }
                    Err(_) => {
                        return (StatusCode::NOT_FOUND, "OIDC provider config not found")
                            .into_response();
                    }
                }
            }
            Err(_) => return (StatusCode::NOT_FOUND, "Tenant not found").into_response(),
        }
    } else {
        // Global default
        match state.oidc_provider.as_ref() {
            Some(p) => ((**p).clone(), String::new()),
            None => return (StatusCode::BAD_REQUEST, "OIDC not configured").into_response(),
        }
    };

    let auth_endpoint = match oidc.resolve_authorization_endpoint().await {
        Ok(ep) => ep,
        Err(e) => {
            warn!("Failed to resolve OIDC authorization endpoint: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "OIDC discovery failed").into_response();
        }
    };

    let callback_url = format!("{}/_console/api/oidc/callback", state.external_endpoint);

    // Encode provider + tenant in state for callback resolution
    let csrf = format!("{:x}", uuid::Uuid::new_v4().as_u128());
    let state_value = format!(
        "{}:{}:{}",
        csrf,
        params.provider.replace(':', "_"),
        tenant_name.replace(':', "_")
    );

    let config = oidc.config();
    let auth_url = format!(
        "{}?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}&response_mode=query",
        auth_endpoint,
        urlencoding::encode(&config.client_id),
        urlencoding::encode(&callback_url),
        urlencoding::encode(&config.scopes),
        urlencoding::encode(&state_value),
    );

    debug!(
        "OIDC authorize redirect (provider={}, tenant={}): {}",
        params.provider, tenant_name, auth_url
    );

    let state_cookie =
        format!("oidc-state={state_value}; Path=/; HttpOnly; SameSite=Lax; Max-Age=600");

    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, auth_url)
        .header(header::SET_COOKIE, state_cookie)
        .body(axum::body::Body::empty())
        .unwrap()
}

/// Callback query params from OIDC provider
#[derive(Deserialize)]
pub struct OidcCallbackParams {
    pub code: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub error_description: Option<String>,
}

/// GET /_console/api/oidc/callback — handle OIDC provider redirect
pub async fn oidc_callback(
    State(state): State<Arc<ConsoleOidcState>>,
    headers: HeaderMap,
    Query(params): Query<OidcCallbackParams>,
) -> Response {
    // Check for error from provider
    if let Some(ref err) = params.error {
        warn!(
            "OIDC callback error: {} {:?}",
            err, params.error_description
        );
        return Response::builder()
            .status(StatusCode::FOUND)
            .header(
                header::LOCATION,
                format!(
                    "/_console/?error={}",
                    urlencoding::encode(params.error_description.as_deref().unwrap_or(err))
                ),
            )
            .body(axum::body::Body::empty())
            .unwrap();
    }

    // Validate CSRF state
    let state_cookie = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|c| {
                let c = c.trim();
                c.strip_prefix("oidc-state=").map(String::from)
            })
        })
        .unwrap_or_default();

    if state_cookie.is_empty() || state_cookie != params.state {
        warn!("OIDC callback: state mismatch");
        return Response::builder()
            .status(StatusCode::FOUND)
            .header(header::LOCATION, "/_console/?error=Invalid+state")
            .body(axum::body::Body::empty())
            .unwrap();
    }

    // Parse state: "csrf:provider:tenant"
    let state_parts: Vec<&str> = params.state.splitn(3, ':').collect();
    let provider_name = state_parts.get(1).unwrap_or(&"").to_string();
    let tenant_from_state = state_parts.get(2).unwrap_or(&"").to_string();

    // Resolve OIDC provider (same logic as authorize)
    let oidc = if !provider_name.is_empty() && provider_name != "system" {
        let mut client = state.meta_client.clone();
        let config_key = format!("identity/openid/{provider_name}");
        match client
            .get_config(objectio_proto::metadata::GetConfigRequest { key: config_key })
            .await
        {
            Ok(resp) => {
                let entry = resp.into_inner().entry.unwrap_or_default();
                match serde_json::from_slice::<serde_json::Value>(&entry.value)
                    .ok()
                    .and_then(|c| build_oidc_provider_from_config(&c))
                {
                    Some(p) => p,
                    None => {
                        return Response::builder()
                            .status(StatusCode::FOUND)
                            .header(header::LOCATION, "/_console/?error=Invalid+provider+config")
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                }
            }
            Err(_) => {
                return Response::builder()
                    .status(StatusCode::FOUND)
                    .header(header::LOCATION, "/_console/?error=Provider+not+found")
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
        }
    } else {
        match state.oidc_provider.as_ref() {
            Some(p) => (**p).clone(),
            None => {
                return (StatusCode::BAD_REQUEST, "OIDC not configured").into_response();
            }
        }
    };

    // Exchange authorization code for tokens
    let callback_url = format!("{}/_console/api/oidc/callback", state.external_endpoint);
    let token_resp = match oidc
        .exchange_authorization_code(&params.code, &callback_url)
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            warn!("OIDC token exchange failed: {e}");
            return Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, "/_console/?error=Token+exchange+failed")
                .body(axum::body::Body::empty())
                .unwrap();
        }
    };

    // Validate the token and extract identity
    // Try id_token first (has user claims), fall back to access_token
    let token_to_validate = token_resp
        .id_token
        .as_deref()
        .unwrap_or(&token_resp.access_token);

    let (user_id, _groups) = match oidc.validate_token(token_to_validate).await {
        Ok(claims) => {
            let sub = claims
                .extra
                .get("preferred_username")
                .or(claims.extra.get("email"))
                .and_then(|v| v.as_str())
                .unwrap_or(&claims.sub)
                .to_string();
            let groups = oidc.extract_groups(&claims);
            (sub, groups)
        }
        Err(e) => {
            warn!("OIDC token validation failed: {e}");
            return Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, "/_console/?error=Token+validation+failed")
                .body(axum::body::Body::empty())
                .unwrap();
        }
    };

    // Resolve tenant: from state, or look up which tenant uses this provider
    let tenant = if !tenant_from_state.is_empty() {
        tenant_from_state
    } else if !provider_name.is_empty() && provider_name != "system" {
        // Look up all tenants to find which one uses this provider
        let mut t_client = state.meta_client.clone();
        let resolved = if let Ok(resp) = t_client
            .list_tenants(objectio_proto::metadata::ListTenantsRequest {})
            .await
        {
            resp.into_inner()
                .tenants
                .iter()
                .find(|t| t.oidc_provider == provider_name)
                .map(|t| t.name.clone())
        } else {
            None
        };
        match resolved {
            Some(t) => t,
            None => {
                warn!(
                    "OIDC provider '{}' not mapped to any tenant — login denied",
                    provider_name
                );
                return Response::builder()
                    .status(StatusCode::FOUND)
                    .header(
                        header::LOCATION,
                        "/_console/?error=No+tenant+configured+for+this+provider",
                    )
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
        }
    } else {
        String::new() // system admin (global SSO)
    };

    // Auto-provision OIDC user in meta service (idempotent)
    let mut meta = state.meta_client.clone();
    let provisioned_user_id = match meta
        .create_user(objectio_proto::metadata::CreateUserRequest {
            display_name: user_id.clone(),
            email: String::new(),
            tenant: tenant.clone(),
        })
        .await
    {
        Ok(resp) => resp
            .into_inner()
            .user
            .map(|u| u.user_id)
            .unwrap_or(user_id.clone()),
        Err(e) => {
            // Already exists — look up by display name
            if e.code() == tonic::Code::AlreadyExists {
                // Find user by listing and matching display name
                if let Ok(resp) = meta
                    .list_users(objectio_proto::metadata::ListUsersRequest {
                        max_results: 1000,
                        marker: String::new(),
                    })
                    .await
                {
                    resp.into_inner()
                        .users
                        .iter()
                        .find(|u| u.display_name == user_id)
                        .map(|u| u.user_id.clone())
                        .unwrap_or(user_id.clone())
                } else {
                    user_id.clone()
                }
            } else {
                debug!("Failed to auto-provision OIDC user: {e}");
                user_id.clone()
            }
        }
    };

    // Build session cookie (same format as AK/SK login)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let expires = now + SESSION_TTL_SECS;
    // tenant already resolved above

    let payload = format!("{provisioned_user_id}|oidc|{tenant}|{expires}");
    let sig = sign_payload(&payload);
    let token = format!("{payload}|{sig}");
    let token_b64 = base64_encode(&token);

    let session_cookie = format!(
        "objectio-session={token_b64}; Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SECS}"
    );

    // Clear state cookie + set session cookie + redirect to console
    let clear_state = "oidc-state=; Path=/; HttpOnly; Max-Age=0";

    debug!(
        "OIDC login successful: oidc_sub={}, provisioned_id={}",
        user_id, provisioned_user_id
    );

    Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, "/_console/")
        .header(header::SET_COOKIE, session_cookie)
        .header("Set-Cookie", clear_state)
        .body(axum::body::Body::empty())
        .unwrap()
}
