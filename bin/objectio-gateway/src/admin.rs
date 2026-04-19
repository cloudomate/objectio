//! Admin configuration API handlers
//!
//! Provides REST endpoints for managing cluster configuration:
//! - `GET    /_admin/config`             — list all config sections
//! - `GET    /_admin/config/{section}`   — get config for a section
//! - `PUT    /_admin/config/{section}`   — set config for a section
//! - `DELETE /_admin/config/{section}`   — delete config
//!
//! OIDC provider configs at `identity/openid/{name}` get special handling:
//! secrets are redacted on GET, and values are validated on PUT.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use objectio_auth::AuthResult;
use objectio_proto::metadata::{
    CreatePoolRequest, CreateTenantRequest, DeleteConfigRequest, DeletePoolRequest,
    DeleteTenantRequest, GetConfigRequest, GetDrainStatusRequest, GetListingNodesRequest,
    GetPoolRequest, GetRebalanceStatusRequest, GetTenantRequest, ListConfigRequest,
    ListPoolsRequest, ListTenantsRequest, OsdAdminState as ProtoOsdAdminState,
    PoolConfig, SetConfigRequest, SetOsdAdminStateRequest, TenantConfig,
    UpdatePoolRequest, UpdateTenantRequest,
};
use objectio_proto::storage::storage_service_client::StorageServiceClient;

/// Convert JSON to PoolConfig (prost types don't implement Deserialize)
fn json_to_pool(v: &serde_json::Value) -> PoolConfig {
    PoolConfig {
        name: v["name"].as_str().unwrap_or_default().to_string(),
        ec_type: v["ec_type"].as_i64().unwrap_or_default() as i32,
        ec_k: v["ec_k"].as_u64().unwrap_or(3) as u32,
        ec_m: v["ec_m"].as_u64().unwrap_or(2) as u32,
        ec_local_parity: v["ec_local_parity"].as_u64().unwrap_or_default() as u32,
        ec_global_parity: v["ec_global_parity"].as_u64().unwrap_or_default() as u32,
        replication_count: v["replication_count"].as_u64().unwrap_or_default() as u32,
        osd_tags: v["osd_tags"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        failure_domain: v["failure_domain"].as_str().unwrap_or("rack").to_string(),
        quota_bytes: v["quota_bytes"].as_u64().unwrap_or_default(),
        description: v["description"].as_str().unwrap_or_default().to_string(),
        enabled: v["enabled"].as_bool().unwrap_or(true),
        created_at: 0,
        updated_at: 0,
        // Placement-group sizing. 0 keeps the legacy per-object CRUSH
        // path (backward-compat). Admins who want PG-based placement
        // pass "pg_count": 256 (or higher) on pool creation.
        pg_count: v["pg_count"].as_u64().unwrap_or_default() as u32,
        tier: v["tier"].as_str().unwrap_or_default().to_string(),
    }
}

/// Convert JSON to TenantConfig
fn json_to_tenant(v: &serde_json::Value) -> TenantConfig {
    TenantConfig {
        name: v["name"].as_str().unwrap_or_default().to_string(),
        display_name: v["display_name"].as_str().unwrap_or_default().to_string(),
        default_pool: v["default_pool"].as_str().unwrap_or_default().to_string(),
        allowed_pools: v["allowed_pools"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        quota_bytes: v["quota_bytes"].as_u64().unwrap_or_default(),
        quota_buckets: v["quota_buckets"].as_u64().unwrap_or_default(),
        quota_objects: v["quota_objects"].as_u64().unwrap_or_default(),
        admin_users: v["admin_users"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        oidc_provider: v["oidc_provider"].as_str().unwrap_or_default().to_string(),
        labels: v["labels"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        enabled: v["enabled"].as_bool().unwrap_or(true),
        created_at: 0,
        updated_at: 0,
    }
}

/// Convert a PoolConfig to JSON (prost types don't implement Serialize)
fn pool_to_json(p: &PoolConfig) -> serde_json::Value {
    serde_json::json!({
        "name": p.name,
        "ec_type": p.ec_type,
        "ec_k": p.ec_k,
        "ec_m": p.ec_m,
        "ec_local_parity": p.ec_local_parity,
        "ec_global_parity": p.ec_global_parity,
        "replication_count": p.replication_count,
        "osd_tags": p.osd_tags,
        "failure_domain": p.failure_domain,
        "quota_bytes": p.quota_bytes,
        "description": p.description,
        "enabled": p.enabled,
        "created_at": p.created_at,
        "updated_at": p.updated_at,
    })
}

/// Convert a TenantConfig to JSON
fn tenant_to_json(t: &TenantConfig) -> serde_json::Value {
    serde_json::json!({
        "name": t.name,
        "display_name": t.display_name,
        "default_pool": t.default_pool,
        "allowed_pools": t.allowed_pools,
        "quota_bytes": t.quota_bytes,
        "quota_buckets": t.quota_buckets,
        "quota_objects": t.quota_objects,
        "admin_users": t.admin_users,
        "oidc_provider": t.oidc_provider,
        "labels": t.labels,
        "enabled": t.enabled,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
    })
}
use serde::Deserialize;
use std::sync::Arc;
use tracing::{info, warn};

use crate::s3::AppState;

/// Query params for list config
#[derive(Debug, Deserialize)]
pub struct ListConfigParams {
    #[serde(default)]
    pub prefix: String,
}

/// List all config entries (optionally filtered by prefix)
pub async fn admin_list_config(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Query(params): Query<ListConfigParams>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }

    let mut client = state.meta_client.clone();
    match client
        .list_config(ListConfigRequest {
            prefix: params.prefix,
        })
        .await
    {
        Ok(resp) => {
            let entries = resp.into_inner().entries;
            let result: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| {
                    let value = redact_if_secret(&e.key, &e.value);
                    serde_json::json!({
                        "key": e.key,
                        "value": value,
                        "updated_at": e.updated_at,
                        "updated_by": e.updated_by,
                        "version": e.version,
                    })
                })
                .collect();
            Json(result).into_response()
        }
        Err(e) => {
            warn!("Failed to list config: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response()
        }
    }
}

/// Get config for a specific section/key
pub async fn admin_get_config(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(section): Path<String>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }

    let mut client = state.meta_client.clone();
    match client
        .get_config(GetConfigRequest {
            key: section.clone(),
        })
        .await
    {
        Ok(resp) => {
            let resp = resp.into_inner();
            if !resp.found {
                return (StatusCode::NOT_FOUND, "Config not found").into_response();
            }
            if let Some(entry) = resp.entry {
                let value = redact_if_secret(&entry.key, &entry.value);
                let result = serde_json::json!({
                    "key": entry.key,
                    "value": value,
                    "updated_at": entry.updated_at,
                    "updated_by": entry.updated_by,
                    "version": entry.version,
                });
                Json(result).into_response()
            } else {
                (StatusCode::NOT_FOUND, "Config not found").into_response()
            }
        }
        Err(e) => {
            warn!("Failed to get config '{}': {}", section, e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response()
        }
    }
}

/// Set config for a specific section/key
pub async fn admin_set_config(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(section): Path<String>,
    body: Bytes,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }

    // Enterprise gates on specific config keys. `identity/openid/*` controls
    // OIDC provider registration; gating here stops Community users from
    // wiring Keycloak/Entra as an identity source.
    if section.starts_with("identity/openid/")
        && let Err(r) = crate::license_gate::require_feature(
            &state,
            objectio_license::Feature::Oidc,
        )
    {
        return r;
    }

    // Validate JSON
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
    }

    let updated_by = auth
        .as_ref()
        .map(|Extension(a)| a.user_id.clone())
        .unwrap_or_else(|| "anonymous".to_string());

    let mut client = state.meta_client.clone();
    match client
        .set_config(SetConfigRequest {
            key: section.clone(),
            value: body.to_vec(),
            updated_by,
        })
        .await
    {
        Ok(resp) => {
            let entry = resp.into_inner().entry;
            info!("Config updated: {}", section);
            if let Some(entry) = entry {
                let value = redact_if_secret(&entry.key, &entry.value);
                Json(serde_json::json!({
                    "key": entry.key,
                    "value": value,
                    "version": entry.version,
                }))
                .into_response()
            } else {
                StatusCode::OK.into_response()
            }
        }
        Err(e) => {
            warn!("Failed to set config '{}': {}", section, e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response()
        }
    }
}

/// Delete config for a specific section/key
pub async fn admin_delete_config(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(section): Path<String>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }

    let mut client = state.meta_client.clone();
    match client
        .delete_config(DeleteConfigRequest {
            key: section.clone(),
        })
        .await
    {
        Ok(resp) => {
            if resp.into_inner().success {
                info!("Config deleted: {}", section);
                StatusCode::NO_CONTENT.into_response()
            } else {
                (StatusCode::NOT_FOUND, "Config not found").into_response()
            }
        }
        Err(e) => {
            warn!("Failed to delete config '{}': {}", section, e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response()
        }
    }
}

/// Identity of the caller, normalized across SigV4 and console session auth.
#[derive(Default, Clone, Debug)]
pub struct CallerIdentity {
    pub user_id: String,
    pub user_arn: String,
    pub tenant: String,
    pub authenticated: bool,
}

/// Pull the caller's identity from either SigV4 auth extension or a valid
/// console session cookie. Returns a default (unauthenticated) identity if
/// neither is present.
pub fn extract_caller(
    auth: &Option<Extension<AuthResult>>,
    headers: &axum::http::HeaderMap,
) -> CallerIdentity {
    if let Some(Extension(a)) = auth {
        return CallerIdentity {
            user_id: a.user_id.clone(),
            user_arn: a.user_arn.clone(),
            tenant: a.tenant.clone(),
            authenticated: true,
        };
    }
    if let Some(s) = crate::console_auth::validate_session_from_headers(headers) {
        return CallerIdentity {
            user_id: s.user,
            user_arn: String::new(),
            tenant: s.tenant,
            authenticated: true,
        };
    }
    CallerIdentity::default()
}

const SYSTEM_ADMIN_USER_ARN: &str = "arn:objectio:iam::user/admin";

/// True iff the caller is a system-scope admin. Tenant users (tenant != "")
/// never satisfy this.
///
/// - SigV4 path: requires the canonical admin ARN (strict).
/// - Session path: any authenticated session with an empty tenant scope
///   passes. The session payload does not carry the ARN, so we rely on the
///   invariant that users in the system scope are, by definition,
///   system-scope admins.
pub fn is_system_admin(caller: &CallerIdentity) -> bool {
    if !caller.authenticated {
        return false;
    }
    if !caller.user_arn.is_empty() {
        return caller.user_arn == SYSTEM_ADMIN_USER_ARN;
    }
    caller.tenant.is_empty()
}

/// Check whether the caller is allowed to act as an admin for `target_tenant`:
/// - system admin always passes
/// - else caller.tenant must equal target_tenant AND caller.user_id (or ARN)
///   must be listed in TenantConfig.admin_users.
///
/// Returns `None` on success, or a ready-to-send error response on denial.
pub async fn require_tenant_admin_access(
    state: &AppState,
    auth: &Option<Extension<AuthResult>>,
    headers: &axum::http::HeaderMap,
    target_tenant: &str,
) -> Option<Response> {
    let caller = extract_caller(auth, headers);
    if !caller.authenticated {
        return Some((StatusCode::UNAUTHORIZED, "Authentication required").into_response());
    }
    if is_system_admin(&caller) {
        return None;
    }
    // Tenant users can only act on their own tenant
    if target_tenant.is_empty() || caller.tenant != target_tenant {
        return Some(
            (
                StatusCode::FORBIDDEN,
                "Not authorized for this tenant scope",
            )
                .into_response(),
        );
    }
    // Consult TenantConfig.admin_users
    let mut client = state.meta_client.clone();
    let tenant = match client
        .get_tenant(GetTenantRequest {
            name: target_tenant.to_string(),
        })
        .await
    {
        Ok(resp) => {
            let r = resp.into_inner();
            if !r.found {
                return Some((StatusCode::FORBIDDEN, "Tenant not found").into_response());
            }
            match r.tenant {
                Some(t) => t,
                None => {
                    return Some((StatusCode::FORBIDDEN, "Tenant not found").into_response());
                }
            }
        }
        Err(_) => {
            return Some(
                (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load tenant").into_response(),
            );
        }
    };
    let is_tenant_admin = tenant
        .admin_users
        .iter()
        .any(|entry| entry == &caller.user_id || entry == &caller.user_arn);
    if is_tenant_admin {
        return None;
    }
    Some((StatusCode::FORBIDDEN, "Not a tenant admin").into_response())
}

/// Validate admin access from either SigV4 auth or console session cookie.
/// This is the "tenant-unaware" gate — passes for system admin or any
/// authenticated session. Tenant-scoped endpoints should prefer
/// [`require_tenant_admin_access`] instead so tenant users cannot touch
/// other tenants.
pub fn require_admin_or_session(
    auth: &Option<Extension<AuthResult>>,
    headers: &axum::http::HeaderMap,
) -> Option<Response> {
    // If SigV4 auth is present, use it
    if let Some(Extension(auth_result)) = auth {
        if auth_result.user_arn.ends_with("user/admin") {
            return None; // allowed
        }
        return Some((StatusCode::FORBIDDEN, "Admin access required").into_response());
    }

    // Otherwise check for console session cookie
    // Login already validated credentials — any valid session is allowed
    if crate::console_auth::validate_session_from_headers(headers).is_some() {
        return None;
    }

    Some((StatusCode::UNAUTHORIZED, "Authentication required").into_response())
}

/// Gate that only allows the true system admin — used for cluster-wide
/// operations like pool/tenant/node management where tenant admins must
/// never be allowed.
pub fn require_system_admin(
    auth: &Option<Extension<AuthResult>>,
    headers: &axum::http::HeaderMap,
) -> Option<Response> {
    let caller = extract_caller(auth, headers);
    if !caller.authenticated {
        return Some((StatusCode::UNAUTHORIZED, "Authentication required").into_response());
    }
    if is_system_admin(&caller) {
        return None;
    }
    Some((StatusCode::FORBIDDEN, "System admin access required").into_response())
}

/// Extract tenant from SigV4 auth or session cookie. Empty = system admin.
fn extract_tenant(auth: &Option<Extension<AuthResult>>, headers: &axum::http::HeaderMap) -> String {
    auth.as_ref()
        .map(|Extension(a)| a.tenant.clone())
        .or_else(|| crate::console_auth::validate_session_from_headers(headers).map(|s| s.tenant))
        .unwrap_or_default()
}

/// Redact sensitive fields in OIDC config responses
fn redact_if_secret(key: &str, value: &[u8]) -> serde_json::Value {
    let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(value) else {
        // Not valid JSON, return as base64
        return serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            value,
        ));
    };

    // Redact secrets in OIDC configs
    if key.starts_with("identity/openid/")
        && let Some(obj) = json.as_object_mut()
        && let Some(secret) = obj.get_mut("client_secret")
        && secret.as_str().is_some_and(|s| !s.is_empty())
    {
        *secret = serde_json::Value::String("********".to_string());
    }

    json
}

// ============ Server Pools ============

pub async fn admin_list_pools(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client.list_pools(ListPoolsRequest {}).await {
        Ok(resp) => Json(
            resp.into_inner()
                .pools
                .iter()
                .map(pool_to_json)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_create_pool(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let pool = json_to_pool(&body);
    // LRC (ec_type = 1, ERASURE_LRC) is Enterprise-only. MDS and replication
    // stay available on Community.
    if pool.ec_type == objectio_proto::metadata::ErasureType::ErasureLrc as i32
        && let Err(r) = crate::license_gate::require_feature(
            &state,
            objectio_license::Feature::Lrc,
        )
    {
        return r;
    }
    // Note: no node-count gate here. A pool is a logical placement policy,
    // not an addition of storage nodes. Node-count enforcement lives at
    // meta's RegisterOsd path, where new nodes actually join the cluster.
    let mut client = state.meta_client.clone();
    match client
        .create_pool(CreatePoolRequest { pool: Some(pool) })
        .await
    {
        Ok(resp) => (
            StatusCode::CREATED,
            Json(resp.into_inner().pool.map(|p| pool_to_json(&p))),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_get_pool(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client.get_pool(GetPoolRequest { name }).await {
        Ok(resp) => {
            let r = resp.into_inner();
            if r.found {
                Json(r.pool.map(|p| pool_to_json(&p))).into_response()
            } else {
                (StatusCode::NOT_FOUND, "Pool not found").into_response()
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_update_pool(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut pool = json_to_pool(&body);
    if pool.ec_type == objectio_proto::metadata::ErasureType::ErasureLrc as i32
        && let Err(r) = crate::license_gate::require_feature(
            &state,
            objectio_license::Feature::Lrc,
        )
    {
        return r;
    }
    pool.name = name;
    let mut client = state.meta_client.clone();
    match client
        .update_pool(UpdatePoolRequest { pool: Some(pool) })
        .await
    {
        Ok(resp) => Json(resp.into_inner().pool.map(|p| pool_to_json(&p))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_delete_pool(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client.delete_pool(DeletePoolRequest { name }).await {
        Ok(resp) => {
            if resp.into_inner().success {
                StatusCode::NO_CONTENT.into_response()
            } else {
                (StatusCode::NOT_FOUND, "Pool not found").into_response()
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

// ============ Tenants ============

pub async fn admin_list_tenants(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    // System admin sees all tenants. A tenant admin (or any tenant user)
    // sees only their own tenant — this endpoint powers the console's
    // tenant picker.
    let caller = extract_caller(&auth, &headers);
    if !caller.authenticated {
        return (StatusCode::UNAUTHORIZED, "Authentication required").into_response();
    }
    let mut client = state.meta_client.clone();
    match client.list_tenants(ListTenantsRequest {}).await {
        Ok(resp) => {
            let mut tenants = resp.into_inner().tenants;
            if !is_system_admin(&caller) {
                tenants.retain(|t| t.name == caller.tenant);
            }
            Json(tenants.iter().map(tenant_to_json).collect::<Vec<_>>()).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_create_tenant(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    if let Err(r) = crate::license_gate::require_feature(
        &state,
        objectio_license::Feature::MultiTenancy,
    ) {
        return r;
    }
    let tenant = json_to_tenant(&body);
    let mut client = state.meta_client.clone();
    match client
        .create_tenant(CreateTenantRequest {
            tenant: Some(tenant),
        })
        .await
    {
        Ok(resp) => (
            StatusCode::CREATED,
            Json(resp.into_inner().tenant.map(|t| tenant_to_json(&t))),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_get_tenant(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    // Tenant admins can read their own tenant's config; anyone else
    // (including tenant admins targeting a different tenant) must be
    // system admin.
    let caller = extract_caller(&auth, &headers);
    if !caller.authenticated {
        return (StatusCode::UNAUTHORIZED, "Authentication required").into_response();
    }
    if !is_system_admin(&caller) && caller.tenant != name {
        return (StatusCode::FORBIDDEN, "Not authorized for this tenant").into_response();
    }
    let mut client = state.meta_client.clone();
    match client.get_tenant(GetTenantRequest { name }).await {
        Ok(resp) => {
            let r = resp.into_inner();
            if r.found {
                Json(r.tenant.map(|t| tenant_to_json(&t))).into_response()
            } else {
                (StatusCode::NOT_FOUND, "Tenant not found").into_response()
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_update_tenant(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut tenant = json_to_tenant(&body);
    tenant.name = name;
    let mut client = state.meta_client.clone();
    match client
        .update_tenant(UpdateTenantRequest {
            tenant: Some(tenant),
        })
        .await
    {
        Ok(resp) => Json(resp.into_inner().tenant.map(|t| tenant_to_json(&t))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_delete_tenant(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client.delete_tenant(DeleteTenantRequest { name }).await {
        Ok(resp) => {
            if resp.into_inner().success {
                StatusCode::NO_CONTENT.into_response()
            } else {
                (StatusCode::NOT_FOUND, "Tenant not found").into_response()
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

// ============================================================================
// Tenant admin_users management (system admin only)
// ============================================================================

/// POST /_admin/tenants/{name}/admins  body: {"user_id": "..."} or {"user_arn": "..."}
///
/// Adds a user to this tenant's admin list. Either user_id (UUID) or
/// user_arn may be provided; both are accepted going forward by the
/// tenant-admin gate, so either works.
pub async fn admin_add_tenant_admin(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let entry = body["user_id"]
        .as_str()
        .or_else(|| body["user_arn"].as_str())
        .unwrap_or_default()
        .to_string();
    if entry.is_empty() {
        return (StatusCode::BAD_REQUEST, "user_id or user_arn required").into_response();
    }

    let mut client = state.meta_client.clone();
    let Some(mut tenant) = get_tenant_or_404(&mut client, &name).await else {
        return (StatusCode::NOT_FOUND, "Tenant not found").into_response();
    };
    if !tenant.admin_users.iter().any(|u| u == &entry) {
        tenant.admin_users.push(entry);
    }
    match client
        .update_tenant(UpdateTenantRequest {
            tenant: Some(tenant),
        })
        .await
    {
        Ok(resp) => Json(resp.into_inner().tenant.map(|t| tenant_to_json(&t))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

/// DELETE /_admin/tenants/{name}/admins/{user}
///
/// Removes an entry from the tenant admin list. The path segment is matched
/// exactly against stored entries (so pass the same format used when adding).
pub async fn admin_remove_tenant_admin(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path((name, user)): Path<(String, String)>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    let Some(mut tenant) = get_tenant_or_404(&mut client, &name).await else {
        return (StatusCode::NOT_FOUND, "Tenant not found").into_response();
    };
    let before = tenant.admin_users.len();
    tenant.admin_users.retain(|u| u != &user);
    if tenant.admin_users.len() == before {
        return (StatusCode::NOT_FOUND, "Admin entry not found").into_response();
    }
    match client
        .update_tenant(UpdateTenantRequest {
            tenant: Some(tenant),
        })
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

async fn get_tenant_or_404(
    client: &mut objectio_proto::metadata::metadata_service_client::MetadataServiceClient<
        tonic::transport::Channel,
    >,
    name: &str,
) -> Option<TenantConfig> {
    client
        .get_tenant(GetTenantRequest {
            name: name.to_string(),
        })
        .await
        .ok()
        .and_then(|r| {
            let r = r.into_inner();
            if r.found { r.tenant } else { None }
        })
}

/// Look up the tenant owning a bucket. Empty string = system-scope bucket.
/// Returns None if the bucket does not exist.
async fn lookup_bucket_tenant(state: &AppState, bucket: &str) -> Option<String> {
    let mut client = state.meta_client.clone();
    let resp = client
        .get_bucket(objectio_proto::metadata::GetBucketRequest {
            name: bucket.to_string(),
        })
        .await
        .ok()?
        .into_inner();
    resp.bucket.map(|b| b.tenant)
}

/// Look up a user's tenant. Returns None if the user does not exist.
async fn lookup_user_tenant(state: &AppState, user_id: &str) -> Option<String> {
    let mut client = state.meta_client.clone();
    let resp = client
        .get_user(objectio_proto::metadata::GetUserRequest {
            user_id: user_id.to_string(),
        })
        .await
        .ok()?
        .into_inner();
    resp.user.map(|u| u.tenant)
}

/// Gate a request on the tenant that owns `user_id`.
async fn require_user_tenant_admin(
    state: &AppState,
    auth: &Option<Extension<AuthResult>>,
    headers: &HeaderMap,
    user_id: &str,
) -> Option<Response> {
    let tenant = match lookup_user_tenant(state, user_id).await {
        Some(t) => t,
        None => return Some((StatusCode::NOT_FOUND, "User not found").into_response()),
    };
    if tenant.is_empty() {
        require_system_admin(auth, headers)
    } else {
        require_tenant_admin_access(state, auth, headers, &tenant).await
    }
}

/// Gate a request on the tenant that owns `bucket`. System admin passes for
/// any bucket; tenant admins only for buckets in their own tenant.
async fn require_bucket_tenant_admin(
    state: &AppState,
    auth: &Option<Extension<AuthResult>>,
    headers: &HeaderMap,
    bucket: &str,
) -> Option<Response> {
    let tenant = match lookup_bucket_tenant(state, bucket).await {
        Some(t) => t,
        None => return Some((StatusCode::NOT_FOUND, "Bucket not found").into_response()),
    };
    if tenant.is_empty() {
        require_system_admin(auth, headers)
    } else {
        require_tenant_admin_access(state, auth, headers, &tenant).await
    }
}

// ============================================================================
// Buckets (admin API — bypasses SigV4 for console)
// ============================================================================

pub async fn admin_list_buckets(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let tenant = extract_tenant(&auth, &headers);

    let mut client = state.meta_client.clone();
    match client
        .list_buckets(objectio_proto::metadata::ListBucketsRequest {
            owner: String::new(),
            tenant,
        })
        .await
    {
        Ok(resp) => {
            let buckets: Vec<serde_json::Value> = resp
                .into_inner()
                .buckets
                .iter()
                .map(|b| {
                    serde_json::json!({
                        "name": b.name,
                        "created_at": b.created_at,
                        "owner": b.owner,
                        "versioning": b.versioning,
                        "pool": b.pool,
                        "tenant": b.tenant,
                    })
                })
                .collect();
            Json(serde_json::json!({ "buckets": buckets })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_create_bucket(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let name = body["name"].as_str().unwrap_or_default().to_string();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "name is required").into_response();
    }
    // Note: no capacity gate here. Creating a bucket is a logical carve-out
    // of existing capacity, not an addition of physical storage. Raw-capacity
    // enforcement lives at meta's RegisterOsd path where new disks actually
    // grow the cluster's footprint.
    // Default to caller's tenant; body may override (system admin only).
    let caller_tenant = extract_tenant(&auth, &headers);
    let body_tenant = body["tenant"].as_str().unwrap_or_default().to_string();
    let tenant = if body_tenant.is_empty() {
        caller_tenant
    } else {
        body_tenant
    };
    if tenant.is_empty() {
        if let Some(deny) = require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        require_tenant_admin_access(&state, &auth, &headers, &tenant).await
    {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .create_bucket(objectio_proto::metadata::CreateBucketRequest {
            name: name.clone(),
            owner: "admin".to_string(),
            storage_class: "STANDARD".to_string(),
            region: String::new(),
            tenant,
        })
        .await
    {
        Ok(_) => Json(serde_json::json!({ "name": name })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_delete_bucket(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    // Resolve bucket → tenant so tenant admins cannot drop other tenants'
    // buckets.
    let mut client = state.meta_client.clone();
    let tenant = match client
        .get_bucket(objectio_proto::metadata::GetBucketRequest { name: name.clone() })
        .await
    {
        Ok(resp) => resp.into_inner().bucket.map(|b| b.tenant).unwrap_or_default(),
        Err(_) => return (StatusCode::NOT_FOUND, "Bucket not found").into_response(),
    };
    if tenant.is_empty() {
        if let Some(deny) = require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        require_tenant_admin_access(&state, &auth, &headers, &tenant).await
    {
        return deny;
    }
    match client
        .delete_bucket(objectio_proto::metadata::DeleteBucketRequest { name })
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

// Bucket policy management
// ============================================================================

pub async fn admin_get_bucket_policy(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
) -> Response {
    if let Some(deny) = require_bucket_tenant_admin(&state, &auth, &headers, &bucket).await {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .get_bucket_policy(objectio_proto::metadata::GetBucketPolicyRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            if inner.has_policy {
                Json(serde_json::json!({
                    "has_policy": true,
                    "policy": serde_json::from_str::<serde_json::Value>(&inner.policy_json).unwrap_or_default(),
                }))
                .into_response()
            } else {
                Json(serde_json::json!({ "has_policy": false, "policy": null })).into_response()
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_put_bucket_policy(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    if let Some(deny) = require_bucket_tenant_admin(&state, &auth, &headers, &bucket).await {
        return deny;
    }
    let policy_json = String::from_utf8_lossy(&body).to_string();
    // Validate JSON
    if serde_json::from_str::<serde_json::Value>(&policy_json).is_err() {
        return (StatusCode::BAD_REQUEST, "Invalid JSON").into_response();
    }
    let mut client = state.meta_client.clone();
    match client
        .set_bucket_policy(objectio_proto::metadata::SetBucketPolicyRequest {
            bucket: bucket.clone(),
            policy_json,
        })
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_delete_bucket_policy(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
) -> Response {
    if let Some(deny) = require_bucket_tenant_admin(&state, &auth, &headers, &bucket).await {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .delete_bucket_policy(objectio_proto::metadata::DeleteBucketPolicyRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

// ============================================================================

/// Query params for admin object listing
#[derive(Debug, serde::Deserialize)]
pub struct AdminListObjectsParams {
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub delimiter: String,
    #[serde(rename = "max-keys", default)]
    pub max_keys: Option<u32>,
}

pub async fn admin_list_objects(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Query(params): Query<AdminListObjectsParams>,
) -> Response {
    if let Some(deny) = require_bucket_tenant_admin(&state, &auth, &headers, &bucket).await {
        return deny;
    }

    let max_keys = params.max_keys.unwrap_or(1000);
    let mut meta_client = state.meta_client.clone();

    // Use the scatter-gather engine (same as the S3 list_objects handler)
    let all_objects = match state
        .scatter_gather
        .list_objects(&mut meta_client, &bucket, &params.prefix, max_keys, None)
        .await
    {
        Ok(result) => result.objects,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to list objects: {e}"),
            )
                .into_response();
        }
    };

    // Apply delimiter to split into objects and common prefixes
    let delimiter = if params.delimiter.is_empty() {
        None
    } else {
        Some(params.delimiter.as_str())
    };
    let prefix_str = &params.prefix;

    let mut contents = Vec::new();
    let mut common_prefixes = std::collections::BTreeSet::new();

    for obj in &all_objects {
        if obj.is_delete_marker {
            continue;
        }
        if let Some(delim) = delimiter
            && obj.key.len() > prefix_str.len()
        {
            let after_prefix = &obj.key[prefix_str.len()..];
            if let Some(pos) = after_prefix.find(delim) {
                let cp = format!("{}{}", prefix_str, &after_prefix[..=pos]);
                common_prefixes.insert(cp);
                continue;
            }
        }
        contents.push(serde_json::json!({
            "key": obj.key,
            "size": obj.size,
            "etag": obj.etag,
            "last_modified": obj.modified_at,
        }));
        if contents.len() >= max_keys as usize {
            break;
        }
    }

    Json(serde_json::json!({
        "contents": contents,
        "common_prefixes": common_prefixes.into_iter().collect::<Vec<_>>(),
        "prefix": params.prefix,
    }))
    .into_response()
}

// ============================================================================
// Warehouses
// ============================================================================

pub async fn admin_list_warehouses(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let tenant = extract_tenant(&auth, &headers);
    let mut client = state.meta_client.clone();
    match client
        .iceberg_list_warehouses(objectio_proto::metadata::IcebergListWarehousesRequest { tenant })
        .await
    {
        Ok(resp) => {
            let warehouses: Vec<serde_json::Value> = resp
                .into_inner()
                .warehouses
                .iter()
                .map(|w| {
                    serde_json::json!({
                        "name": w.name,
                        "bucket": w.bucket,
                        "location": w.location,
                        "tenant": w.tenant,
                        "created_at": w.created_at,
                        "properties": w.properties,
                    })
                })
                .collect();
            Json(serde_json::json!({ "warehouses": warehouses })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_create_warehouse(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let name = body["name"].as_str().unwrap_or_default().to_string();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "name is required").into_response();
    }
    // Use caller tenant if body leaves it empty. Tenant admins can only
    // create warehouses inside their tenant; system admin can target any.
    let tenant = {
        let body_tenant = body["tenant"].as_str().unwrap_or_default().to_string();
        if body_tenant.is_empty() {
            extract_tenant(&auth, &headers)
        } else {
            body_tenant
        }
    };
    if tenant.is_empty() {
        if let Some(deny) = require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        require_tenant_admin_access(&state, &auth, &headers, &tenant).await
    {
        return deny;
    }
    let properties: std::collections::HashMap<String, String> = body["properties"]
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let mut client = state.meta_client.clone();
    match client
        .iceberg_create_warehouse(objectio_proto::metadata::IcebergCreateWarehouseRequest {
            name,
            tenant,
            properties,
        })
        .await
    {
        Ok(resp) => {
            let wh = resp.into_inner().warehouse.unwrap_or_default();
            Json(serde_json::json!({
                "name": wh.name,
                "bucket": wh.bucket,
                "location": wh.location,
                "tenant": wh.tenant,
                "created_at": wh.created_at,
            }))
            .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_delete_warehouse(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    // Find the warehouse via a system-wide list to discover its tenant so we
    // can gate the delete properly. There is no dedicated Get RPC yet.
    let mut client = state.meta_client.clone();
    let tenant = match client
        .iceberg_list_warehouses(objectio_proto::metadata::IcebergListWarehousesRequest {
            tenant: String::new(),
        })
        .await
    {
        Ok(resp) => resp
            .into_inner()
            .warehouses
            .into_iter()
            .find(|w| w.name == name)
            .map(|w| w.tenant)
            .unwrap_or_default(),
        Err(_) => return (StatusCode::NOT_FOUND, "Warehouse not found").into_response(),
    };
    if tenant.is_empty() {
        if let Some(deny) = require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        require_tenant_admin_access(&state, &auth, &headers, &tenant).await
    {
        return deny;
    }
    match client
        .iceberg_delete_warehouse(objectio_proto::metadata::IcebergDeleteWarehouseRequest { name })
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

// ============================================================================
// IAM Policies (PBAC)
// ============================================================================

pub async fn admin_list_policies(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    // Policies are cluster-global. Tenant admins don't manage them.
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .list_policies(objectio_proto::metadata::ListPoliciesRequest {})
        .await
    {
        Ok(resp) => {
            let policies: Vec<serde_json::Value> = resp
                .into_inner()
                .policies
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "name": p.name,
                        "policy": serde_json::from_str::<serde_json::Value>(&p.policy_json).unwrap_or_default(),
                        "created_at": p.created_at,
                    })
                })
                .collect();
            Json(serde_json::json!({ "policies": policies })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_create_policy(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let name = body["name"].as_str().unwrap_or_default().to_string();
    let policy_json = if body["policy"].is_string() {
        body["policy"].as_str().unwrap_or("{}").to_string()
    } else {
        serde_json::to_string(&body["policy"]).unwrap_or_default()
    };
    let mut client = state.meta_client.clone();
    match client
        .create_policy(objectio_proto::metadata::CreatePolicyRequest { name, policy_json })
        .await
    {
        Ok(resp) => {
            let p = resp.into_inner().policy.unwrap_or_default();
            Json(serde_json::json!({ "name": p.name })).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_delete_policy(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .delete_policy(objectio_proto::metadata::DeletePolicyRequest { name })
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_attach_policy(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let user_id = body["user_id"].as_str().unwrap_or_default().to_string();
    let group_id = body["group_id"].as_str().unwrap_or_default().to_string();
    // If targeting a specific user, scope the check to that user's tenant.
    // Group targets remain system-admin-only (groups aren't tenant-scoped yet).
    if !user_id.is_empty() {
        if let Some(deny) = require_user_tenant_admin(&state, &auth, &headers, &user_id).await {
            return deny;
        }
    } else if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .attach_policy(objectio_proto::metadata::AttachPolicyRequest {
            policy_name: body["policy_name"].as_str().unwrap_or_default().to_string(),
            user_id,
            group_id,
        })
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_detach_policy(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let user_id = body["user_id"].as_str().unwrap_or_default().to_string();
    let group_id = body["group_id"].as_str().unwrap_or_default().to_string();
    if !user_id.is_empty() {
        if let Some(deny) = require_user_tenant_admin(&state, &auth, &headers, &user_id).await {
            return deny;
        }
    } else if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .detach_policy(objectio_proto::metadata::DetachPolicyRequest {
            policy_name: body["policy_name"].as_str().unwrap_or_default().to_string(),
            user_id,
            group_id,
        })
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_list_attached_policies(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let user_id = params.get("user_id").cloned().unwrap_or_default();
    if !user_id.is_empty() {
        if let Some(deny) = require_user_tenant_admin(&state, &auth, &headers, &user_id).await {
            return deny;
        }
    } else if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .list_attached_policies(objectio_proto::metadata::ListAttachedPoliciesRequest {
            user_id: params.get("user_id").cloned().unwrap_or_default(),
            group_id: params.get("group_id").cloned().unwrap_or_default(),
        })
        .await
    {
        Ok(resp) => Json(serde_json::json!({ "policy_names": resp.into_inner().policy_names }))
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

// ============================================================================
// Table Sharing (tenant-aware wrappers)
// ============================================================================

pub async fn admin_list_shares_tenant(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let tenant = extract_tenant(&auth, &headers);
    let mut client = state.meta_client.clone();
    match client
        .delta_list_shares(objectio_proto::metadata::DeltaListSharesRequest {
            max_results: 0,
            page_token: String::new(),
            tenant,
        })
        .await
    {
        Ok(resp) => {
            let shares: Vec<serde_json::Value> = resp
                .into_inner()
                .shares
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "name": s.name,
                        "comment": s.comment,
                        "tenant": s.tenant,
                    })
                })
                .collect();
            Json(serde_json::json!({ "shares": shares })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

pub async fn admin_create_share_tenant(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let name = body["name"].as_str().unwrap_or_default().to_string();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "name is required").into_response();
    }
    let tenant = {
        let body_tenant = body["tenant"].as_str().unwrap_or_default().to_string();
        if body_tenant.is_empty() {
            extract_tenant(&auth, &headers)
        } else {
            body_tenant
        }
    };
    if tenant.is_empty() {
        if let Some(deny) = require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        require_tenant_admin_access(&state, &auth, &headers, &tenant).await
    {
        return deny;
    }
    let mut client = state.meta_client.clone();
    match client
        .delta_create_share(objectio_proto::metadata::DeltaCreateShareRequest {
            name: name.clone(),
            comment: body["comment"].as_str().unwrap_or_default().to_string(),
            tenant,
        })
        .await
    {
        Ok(resp) => {
            let s = resp.into_inner().share.unwrap_or_default();
            Json(serde_json::json!({ "name": s.name, "comment": s.comment, "tenant": s.tenant }))
                .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.message().to_string()).into_response(),
    }
}

pub async fn admin_list_recipients_tenant(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let tenant = extract_tenant(&auth, &headers);
    let mut client = state.meta_client.clone();
    // List all recipients, then filter by shares that belong to tenant's shares
    match client
        .delta_list_recipients(objectio_proto::metadata::DeltaListRecipientsRequest {
            max_results: 0,
            page_token: String::new(),
        })
        .await
    {
        Ok(resp) => {
            let mut recipients: Vec<serde_json::Value> = resp
                .into_inner()
                .recipients
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "name": r.name,
                        "shares": r.shares,
                    })
                })
                .collect();
            // If tenant user, filter recipients to only show those with access to tenant's shares
            if !tenant.is_empty()
                && let Ok(shares_resp) = client
                    .delta_list_shares(objectio_proto::metadata::DeltaListSharesRequest {
                        max_results: 0,
                        page_token: String::new(),
                        tenant: tenant.clone(),
                    })
                    .await
            {
                let tenant_shares: std::collections::HashSet<String> = shares_resp
                    .into_inner()
                    .shares
                    .iter()
                    .map(|s| s.name.clone())
                    .collect();
                recipients.retain(|r| {
                    r["shares"].as_array().is_some_and(|arr| {
                        arr.iter()
                            .any(|s| tenant_shares.contains(s.as_str().unwrap_or("")))
                    })
                });
            }
            Json(serde_json::json!({ "recipients": recipients })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    }
}

// ============================================================================
// Nodes / Drives
// ============================================================================

pub async fn admin_list_nodes(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }

    let mut meta = state.meta_client.clone();
    // Admin UI needs every OSD including operator-disabled ones so it
    // can render Mark Out state and let the operator flip it back.
    let nodes = match meta
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
            include_all_states: true,
        })
        .await
    {
        Ok(resp) => resp.into_inner().nodes,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response();
        }
    };

    // Deduplicate by address (scatter-gather may return dupes for different shards)
    let mut seen = std::collections::HashSet::new();
    let mut unique_addrs: Vec<(String, Vec<u8>, i32)> = Vec::new();
    for node in &nodes {
        if seen.insert(node.address.clone()) {
            unique_addrs.push((
                node.address.clone(),
                node.node_id.clone(),
                node.admin_state,
            ));
        }
    }

    let mut result = Vec::new();

    for (addr, node_id, admin_state_i32) in &unique_addrs {
        let admin_state_str = admin_state_label(*admin_state_i32);
        let osd_addr = if addr.starts_with("http") {
            addr.clone()
        } else {
            format!("http://{addr}")
        };
        let status = match StorageServiceClient::connect(osd_addr).await {
            Ok(mut client) => {
                match client
                    .get_status(objectio_proto::storage::GetStatusRequest {})
                    .await
                {
                    Ok(resp) => {
                        let s = resp.into_inner();
                        let disks: Vec<serde_json::Value> = s
                            .disks
                            .iter()
                            .map(|d| {
                                serde_json::json!({
                                    "disk_id": hex::encode(&d.disk_id),
                                    "path": d.path,
                                    "total_capacity": d.total_capacity,
                                    "used_capacity": d.used_capacity,
                                    "status": d.status,
                                    "shard_count": d.shard_count,
                                })
                            })
                            .collect();
                        serde_json::json!({
                            "node_id": hex::encode(node_id),
                            "node_name": s.node_name,
                            "address": addr,
                            "total_capacity": s.total_capacity,
                            "used_capacity": s.used_capacity,
                            "shard_count": s.shard_count,
                            "uptime_seconds": s.uptime_seconds,
                            "disks": disks,
                            "online": true,
                            "kubernetes_node": s.kubernetes_node,
                            "pod_name": s.pod_name,
                            "hostname": s.hostname,
                            "os_info": s.os_info,
                            "cpu_cores": s.cpu_cores,
                            "memory_bytes": s.memory_bytes,
                            "version": s.version,
                            "admin_state": admin_state_str,
                        })
                    }
                    Err(_) => {
                        let mut n = offline_node(node_id, addr);
                        n["admin_state"] = serde_json::Value::String(admin_state_str.into());
                        n
                    }
                }
            }
            Err(_) => {
                let mut n = offline_node(node_id, addr);
                n["admin_state"] = serde_json::Value::String(admin_state_str.into());
                n
            }
        };
        result.push(status);
    }

    Json(serde_json::json!({ "nodes": result })).into_response()
}

/// Wire enum i32 → lowercase label consumed by the console. Unknown
/// values fall back to "in" — conservative, preserves placement.
fn admin_state_label(s: i32) -> &'static str {
    match ProtoOsdAdminState::try_from(s) {
        Ok(ProtoOsdAdminState::OsdAdminIn) => "in",
        Ok(ProtoOsdAdminState::OsdAdminOut) => "out",
        Ok(ProtoOsdAdminState::OsdAdminDraining) => "draining",
        Err(_) => "in",
    }
}

/// `PUT /_admin/osds/{node_id}/admin-state`
///
/// Flip the operator-declared state of an OSD (In / Out / Draining).
/// Routes through the meta's Raft — non-leader meta returns a forward
/// hint the gateway surfaces as a 503; console retries.
///
/// Body: `{"state":"in"|"out"|"draining"}`
#[derive(Debug, serde::Deserialize)]
pub struct AdminStatePayload {
    pub state: String,
}

pub async fn admin_set_osd_state(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    axum::extract::Path(node_id_hex): axum::extract::Path<String>,
    Json(body): Json<AdminStatePayload>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }

    // Parse 32-char hex → 16 raw bytes.
    let node_id_bytes = match hex::decode(&node_id_hex) {
        Ok(b) if b.len() == 16 => b,
        Ok(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "node_id must be 32 hex chars (16 bytes)",
            )
                .into_response();
        }
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "node_id must be valid hex").into_response();
        }
    };

    let wire_state = match body.state.to_ascii_lowercase().as_str() {
        "in" => ProtoOsdAdminState::OsdAdminIn,
        "out" => ProtoOsdAdminState::OsdAdminOut,
        "draining" => ProtoOsdAdminState::OsdAdminDraining,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown state: {other} (expected in | out | draining)"),
            )
                .into_response();
        }
    };

    // Attribute the change to the authenticated user when possible so
    // the audit log reads right; fall back to "console" when the noauth
    // path is in use.
    let requested_by = match &auth {
        Some(Extension(a)) => a.user_id.clone(),
        None => "console".to_string(),
    };

    let mut meta = state.meta_client.clone();
    let resp = meta
        .set_osd_admin_state(SetOsdAdminStateRequest {
            node_id: node_id_bytes,
            state: wire_state as i32,
            requested_by,
        })
        .await;

    match resp {
        Ok(r) => {
            let r = r.into_inner();
            Json(serde_json::json!({
                "found": r.found,
                "changed": r.changed,
                "state": body.state.to_ascii_lowercase(),
            }))
            .into_response()
        }
        Err(e) => {
            // FailedPrecondition from meta means Raft isn't the leader
            // or is not initialised; surface as 503 so the console can
            // retry rather than treating it as a permanent error.
            let code = if e.code() == tonic::Code::FailedPrecondition {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (code, e.message().to_string()).into_response()
        }
    }
}

// ============================================================================
// Host lifecycle — /_admin/hosts (Add Host) and /_admin/osds/{id}/reboot
// ============================================================================

/// Map a `HostProviderError` to an axum `Response` with the right
/// HTTP status. Unsupported → 501, InvalidRequest → 400,
/// Unavailable → 503, Other → 500. Shared by every host endpoint.
fn host_provider_error_to_response(e: crate::host_provider::HostProviderError) -> Response {
    use crate::host_provider::HostProviderError;
    let (code, msg) = match e {
        HostProviderError::Unsupported(m) => (StatusCode::NOT_IMPLEMENTED, m.to_string()),
        HostProviderError::InvalidRequest(m) => (StatusCode::BAD_REQUEST, m),
        HostProviderError::Unavailable(m) => (StatusCode::SERVICE_UNAVAILABLE, m),
        HostProviderError::Other(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
    };
    (code, msg).into_response()
}

/// `POST /_admin/hosts`
///
/// Body: `{"count": N}` (N ≥ 1; default 1 if missing).
///
/// Asks the configured host provider to add N more OSD hosts. On k8s
/// this scales the OSD StatefulSet; each new pod self-registers
/// against meta when it comes up.
#[derive(Debug, serde::Deserialize, Default)]
pub struct AddHostPayload {
    #[serde(default = "AddHostPayload::default_count")]
    pub count: i32,
}

impl AddHostPayload {
    const fn default_count() -> i32 {
        1
    }
}

pub async fn admin_add_hosts(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    body: Option<Json<AddHostPayload>>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let payload = body.map(|Json(p)| p).unwrap_or_default();
    match state.host_provider.add_hosts(payload.count).await {
        Ok(outcome) => Json(serde_json::json!({
            "provider": state.host_provider.name(),
            "previous_replicas": outcome.previous_replicas,
            "new_replicas": outcome.new_replicas,
            "pods_added": outcome.pods_added,
        }))
        .into_response(),
        Err(e) => host_provider_error_to_response(e),
    }
}

/// `POST /_admin/osds/{node_id}/reboot`
///
/// Reboots the pod / machine hosting the given OSD. Resolves the OSD
/// to its hostname / pod-name by looking it up in the meta's node
/// list; the host provider then acts on that. Without a platform
/// provider, returns 501.
pub async fn admin_reboot_osd(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    axum::extract::Path(node_id_hex): axum::extract::Path<String>,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }

    // Resolve node_id → pod/hostname via meta + the OSD's get_status.
    // We need a concrete hostname to hand to the platform provider;
    // `host_provider.reboot("objectio-osd-2")` is how the k8s
    // implementation knows which pod to delete.
    let mut meta = state.meta_client.clone();
    let nodes = match meta
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
            include_all_states: true,
        })
        .await
    {
        Ok(resp) => resp.into_inner().nodes,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response();
        }
    };

    // Parse hex once so we can byte-compare to each listing's node_id.
    let node_id_bytes = match hex::decode(&node_id_hex) {
        Ok(b) if b.len() == 16 => b,
        _ => {
            return (StatusCode::BAD_REQUEST, "node_id must be 32 hex chars")
                .into_response();
        }
    };

    // Find the matching node's address, then fetch the OSD's pod_name
    // from its status RPC. Pod name is what k8s delete-pod needs.
    let addr = match nodes.iter().find(|n| n.node_id == node_id_bytes) {
        Some(n) => n.address.clone(),
        None => {
            return (StatusCode::NOT_FOUND, format!("no OSD {node_id_hex}"))
                .into_response();
        }
    };
    let osd_addr = if addr.starts_with("http") {
        addr
    } else {
        format!("http://{addr}")
    };
    let pod_name = match StorageServiceClient::connect(osd_addr.clone()).await {
        Ok(mut client) => match client
            .get_status(objectio_proto::storage::GetStatusRequest {})
            .await
        {
            Ok(r) => r.into_inner().pod_name,
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("osd unreachable at {osd_addr}: {e}"),
                )
                    .into_response();
            }
        },
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("osd connect failed: {e}"),
            )
                .into_response();
        }
    };
    if pod_name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "osd did not report a pod_name (not running under k8s?)",
        )
            .into_response();
    }

    match state.host_provider.reboot(&pod_name).await {
        Ok(outcome) => Json(serde_json::json!({
            "provider": state.host_provider.name(),
            "pod": outcome.pod,
            "requested": outcome.requested,
        }))
        .into_response(),
        Err(e) => host_provider_error_to_response(e),
    }
}

/// `GET /_admin/drain-status`
///
/// Returns the current per-OSD drain progress snapshot (one entry per
/// Draining OSD). Empty response when no drains are in flight.
pub async fn admin_drain_status(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut meta = state.meta_client.clone();
    let resp = match meta.get_drain_status(GetDrainStatusRequest {}).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string())
                .into_response();
        }
    };

    let drains: Vec<serde_json::Value> = resp
        .drains
        .into_iter()
        .map(|d| {
            serde_json::json!({
                "node_id": hex::encode(&d.node_id),
                "shards_remaining": d.shards_remaining,
                "initial_shards": d.initial_shards,
                "shards_migrated": d.shards_migrated,
                "updated_at": d.updated_at,
                "last_error": d.last_error,
            })
        })
        .collect();

    Json(serde_json::json!({ "drains": drains })).into_response()
}

/// `GET /_admin/rebalance-status` — cluster-wide rebalance progress.
pub async fn admin_rebalance_status(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut meta = state.meta_client.clone();
    let r = match meta.get_rebalance_status(GetRebalanceStatusRequest {}).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string())
                .into_response();
        }
    };
    Json(serde_json::json!({
        "started": r.started,
        "paused": r.paused,
        "last_sweep_at": r.last_sweep_at,
        "scanned_this_pass": r.scanned_this_pass,
        "drifts_seen_this_pass": r.drifts_seen_this_pass,
        "shards_rebalanced_total": r.shards_rebalanced_total,
        "last_error": r.last_error,
    }))
    .into_response()
}

/// `POST /_admin/rebalance/pause` — pause the cluster rebalancer.
/// Persisted via Raft (`rebalance/paused = true`) so it survives
/// leader failover and restarts.
pub async fn admin_rebalance_pause(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    admin_rebalance_set_paused(&state, &auth, &headers, true).await
}

/// `POST /_admin/rebalance/resume` — re-enable the cluster rebalancer.
pub async fn admin_rebalance_resume(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    admin_rebalance_set_paused(&state, &auth, &headers, false).await
}

async fn admin_rebalance_set_paused(
    state: &Arc<AppState>,
    auth: &Option<Extension<AuthResult>>,
    headers: &HeaderMap,
    paused: bool,
) -> Response {
    if let Some(deny) = require_system_admin(auth, headers) {
        return deny;
    }
    let who = auth
        .as_ref()
        .map(|Extension(a)| a.user_id.clone())
        .unwrap_or_else(|| "console".to_string());

    let mut meta = state.meta_client.clone();
    let req = SetConfigRequest {
        key: "rebalance/paused".to_string(),
        value: if paused { b"true".to_vec() } else { b"false".to_vec() },
        updated_by: who,
    };
    match meta.set_config(req).await {
        Ok(_) => {
            Json(serde_json::json!({ "paused": paused })).into_response()
        }
        Err(e) => {
            let code = if e.code() == tonic::Code::FailedPrecondition {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (code, e.message().to_string()).into_response()
        }
    }
}

/// `GET /_admin/host-provider`
///
/// Returns the provider name — used by the console to decide whether
/// to render Add Host / Reboot buttons as active, or show a hint that
/// a platform provider needs configuring.
pub async fn admin_host_provider_info(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    Json(serde_json::json!({
        "provider": state.host_provider.name(),
        "supports_add_host": state.host_provider.name() != "noop",
        "supports_reboot": state.host_provider.name() != "noop",
    }))
    .into_response()
}

// ============================================================================
// License management
// ============================================================================

const LICENSE_CONFIG_KEY: &str = "license/active";

/// Snapshot of cluster-wide usage counted against license caps.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClusterUsage {
    pub node_count: u64,
    pub raw_capacity_bytes: u64,
}

/// Walk every OSD, sum its reported disk total_capacity, and count distinct
/// OSD addresses. Used by the license page and by the scale-up-block gates.
///
/// Best-effort: nodes we can't reach are skipped (counted as 0 bytes).
/// An OSD that's offline briefly shouldn't open a path to exceed the cap.
pub async fn compute_cluster_usage(state: &AppState) -> ClusterUsage {
    let mut meta = state.meta_client.clone();
    let Ok(resp) = meta
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
            include_all_states: false,
        })
        .await
    else {
        return ClusterUsage::default();
    };
    let nodes = resp.into_inner().nodes;
    let mut seen_addrs = std::collections::HashSet::new();
    let mut raw_capacity_bytes: u64 = 0;
    for node in &nodes {
        if !seen_addrs.insert(node.address.clone()) {
            continue;
        }
        let osd_addr = if node.address.starts_with("http") {
            node.address.clone()
        } else {
            format!("http://{}", node.address)
        };
        if let Ok(mut client) = StorageServiceClient::connect(osd_addr).await
            && let Ok(status) = client
                .get_status(objectio_proto::storage::GetStatusRequest {})
                .await
        {
            raw_capacity_bytes = raw_capacity_bytes.saturating_add(status.into_inner().total_capacity);
        }
    }
    ClusterUsage {
        node_count: seen_addrs.len() as u64,
        raw_capacity_bytes,
    }
}


/// GET /_admin/license — currently installed license summary.
/// Always returns 200, even for Community tier, so the console can render
/// the "install license" CTA without a second request.
pub async fn admin_get_license(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let license = state.license();
    // Gather current usage so the console can render vs-limit bars.
    let usage = compute_cluster_usage(&state).await;
    Json(serde_json::json!({
        "tier": license.tier.as_str(),
        "licensee": license.licensee,
        "issued_at": license.issued_at,
        "expires_at": license.expires_at,
        "features": license.features,
        "enabled_features": objectio_license::Feature::all()
            .iter()
            .filter(|f| license.allows(**f))
            .map(|f| f.as_str())
            .collect::<Vec<_>>(),
        "limits": {
            "max_nodes": license.max_nodes,
            "max_raw_capacity_bytes": license.max_raw_capacity_bytes,
        },
        "usage": {
            "node_count": usage.node_count,
            "raw_capacity_bytes": usage.raw_capacity_bytes,
        },
    }))
    .into_response()
}

/// PUT /_admin/license — install a signed license file.
/// Body: raw license JSON (what `objectio-license-gen issue` produces).
/// The license is verified before persistence; on success the hot-swap takes
/// effect immediately and gated routers unlock without a restart.
pub async fn admin_put_license(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let license = match objectio_license::License::load_from_bytes(&body, now) {
        Ok(l) => l,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "InvalidLicense",
                    "message": e.to_string(),
                })),
            )
                .into_response();
        }
    };

    // Persist to meta config so the license survives gateway restarts.
    let actor = auth
        .as_ref()
        .map(|Extension(a)| a.user_arn.clone())
        .unwrap_or_else(|| "console".to_string());
    let mut client = state.meta_client.clone();
    if let Err(e) = client
        .set_config(SetConfigRequest {
            key: LICENSE_CONFIG_KEY.to_string(),
            value: body.to_vec(),
            updated_by: actor,
        })
        .await
    {
        warn!("failed to persist license to meta: {e}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            e.message().to_string(),
        )
            .into_response();
    }
    state.set_license(Arc::new(license.clone()));
    info!(
        "license installed: tier={} licensee={}",
        license.tier, license.licensee
    );
    Json(serde_json::json!({
        "tier": license.tier.as_str(),
        "licensee": license.licensee,
        "expires_at": license.expires_at,
    }))
    .into_response()
}

/// DELETE /_admin/license — remove the installed license and revert to
/// Community tier. Gated routers start returning 403 again immediately.
pub async fn admin_delete_license(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_system_admin(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    let _ = client
        .delete_config(DeleteConfigRequest {
            key: LICENSE_CONFIG_KEY.to_string(),
        })
        .await;
    state.set_license(Arc::new(objectio_license::License::community()));
    info!("license removed — reverted to Community tier");
    StatusCode::NO_CONTENT.into_response()
}

// ============================================================================
// Cluster info — gateway self-topology + live distance to every OSD
// ============================================================================

/// GET /_admin/cluster-info — returns the gateway's configured
/// self-topology plus, for every active OSD, the computed
/// [`TopologyDistance`]. Used by the console to render "this gateway is in
/// rack-02; closest OSDs are …" and by operators to sanity-check that
/// `--topology-*` flags were passed correctly.
pub async fn admin_cluster_info(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    let nodes = match client
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
            include_all_states: false,
        })
        .await
    {
        Ok(resp) => resp.into_inner().nodes,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    };

    let me = &state.self_topology;
    let osd_entries: Vec<serde_json::Value> = nodes
        .iter()
        .map(|n| {
            let fd = n.failure_domain.clone().unwrap_or_default();
            let peer = objectio_placement::FailureDomainInfo::new_full(
                &fd.region, &fd.zone, &fd.datacenter, &fd.rack, &fd.host,
            );
            let dist = objectio_placement::distance(me, &peer);
            serde_json::json!({
                "node_id": hex::encode(&n.node_id),
                "address": n.address,
                "failure_domain": {
                    "region": fd.region,
                    "zone": fd.zone,
                    "datacenter": fd.datacenter,
                    "rack": fd.rack,
                    "host": fd.host,
                },
                "distance": dist.as_str(),
                "is_local": dist.is_local(),
            })
        })
        .collect();

    Json(serde_json::json!({
        "self_topology": {
            "region": me.region,
            "zone": me.zone,
            "datacenter": me.datacenter,
            "rack": me.rack,
            "host": me.host,
            "configured": !me.region.is_empty(),
        },
        "osds": osd_entries,
    }))
    .into_response()
}

// ============================================================================
// Topology + placement validation
// ============================================================================

/// GET /_admin/topology — aggregated OSD tree with per-level counts.
/// The tree is region → zone → datacenter → rack → host → osds; empty
/// levels collapse to a synthetic "(none)" node so the console still
/// renders a usable hierarchy.
pub async fn admin_get_topology(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let mut client = state.meta_client.clone();
    let nodes = match client
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
            include_all_states: false,
        })
        .await
    {
        Ok(resp) => resp.into_inner().nodes,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    };

    // Build a nested counts structure as JSON. Each level carries the
    // display name + how many distinct children it has + an array of
    // children. Small enough for every size of cluster we'll deploy.
    use std::collections::BTreeMap;
    type Tree = BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<String>>>>>>;
    let mut tree: Tree = BTreeMap::new();
    for n in &nodes {
        let fd = n.failure_domain.clone().unwrap_or_default();
        let region = if fd.region.is_empty() { "(none)".into() } else { fd.region };
        let zone = if fd.zone.is_empty() { "(none)".into() } else { fd.zone };
        let dc = if fd.datacenter.is_empty() { "(none)".into() } else { fd.datacenter };
        let rack = if fd.rack.is_empty() { "(none)".into() } else { fd.rack };
        let host = if fd.host.is_empty() { "(none)".into() } else { fd.host };
        tree.entry(region)
            .or_default()
            .entry(zone)
            .or_default()
            .entry(dc)
            .or_default()
            .entry(rack)
            .or_default()
            .entry(host)
            .or_default()
            .push(hex::encode(&n.node_id));
    }

    // Render.
    let regions: Vec<serde_json::Value> = tree
        .into_iter()
        .map(|(region_name, zones)| {
            let zones_json: Vec<serde_json::Value> = zones
                .into_iter()
                .map(|(zone_name, dcs)| {
                    let dcs_json: Vec<serde_json::Value> = dcs
                        .into_iter()
                        .map(|(dc_name, racks)| {
                            let racks_json: Vec<serde_json::Value> = racks
                                .into_iter()
                                .map(|(rack_name, hosts)| {
                                    let hosts_json: Vec<serde_json::Value> = hosts
                                        .into_iter()
                                        .map(|(host_name, osds)| {
                                            serde_json::json!({
                                                "host": host_name,
                                                "osds": osds,
                                            })
                                        })
                                        .collect();
                                    serde_json::json!({ "rack": rack_name, "hosts": hosts_json })
                                })
                                .collect();
                            serde_json::json!({ "datacenter": dc_name, "racks": racks_json })
                        })
                        .collect();
                    serde_json::json!({ "zone": zone_name, "datacenters": dcs_json })
                })
                .collect();
            serde_json::json!({ "region": region_name, "zones": zones_json })
        })
        .collect();

    // Per-level distinct counts for quick "can pool X place?" questions.
    let mut r_set = std::collections::HashSet::new();
    let mut z_set = std::collections::HashSet::new();
    let mut d_set = std::collections::HashSet::new();
    let mut rk_set = std::collections::HashSet::new();
    let mut h_set = std::collections::HashSet::new();
    for n in &nodes {
        let fd = n.failure_domain.clone().unwrap_or_default();
        r_set.insert(fd.region.clone());
        z_set.insert(format!("{}:{}", fd.region, fd.zone));
        d_set.insert(format!("{}:{}:{}", fd.region, fd.zone, fd.datacenter));
        rk_set.insert(format!("{}:{}:{}:{}", fd.region, fd.zone, fd.datacenter, fd.rack));
        h_set.insert(format!(
            "{}:{}:{}:{}:{}",
            fd.region, fd.zone, fd.datacenter, fd.rack, fd.host
        ));
    }

    Json(serde_json::json!({
        "osd_count": nodes.len(),
        "distinct": {
            "region": r_set.len(),
            "zone": z_set.len(),
            "datacenter": d_set.len(),
            "rack": rk_set.len(),
            "host": h_set.len(),
        },
        "tree": regions,
    }))
    .into_response()
}

/// GET /_admin/placement/validate?pool=NAME — answer "can this pool place
/// data in the current topology?". Returns the required spread level,
/// how many distinct domains exist, and a satisfiability verdict.
pub async fn admin_validate_placement(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    if let Some(deny) = require_admin_or_session(&auth, &headers) {
        return deny;
    }
    let Some(pool_name) = params.get("pool") else {
        return (StatusCode::BAD_REQUEST, "?pool=NAME required").into_response();
    };
    let mut client = state.meta_client.clone();
    // Fetch pool config
    let pool = match client
        .get_pool(GetPoolRequest {
            name: pool_name.clone(),
        })
        .await
    {
        Ok(r) => {
            let r = r.into_inner();
            if !r.found {
                return (StatusCode::NOT_FOUND, "pool not found").into_response();
            }
            match r.pool {
                Some(p) => p,
                None => return (StatusCode::NOT_FOUND, "pool not found").into_response(),
            }
        }
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    };

    let shard_count = u64::from(pool.ec_k) + u64::from(pool.ec_m);
    let level = pool.failure_domain.as_str();

    // Fetch nodes and count distinct domain keys at the pool's level.
    let nodes = match client
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
            include_all_states: false,
        })
        .await
    {
        Ok(resp) => resp.into_inner().nodes,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()).into_response(),
    };
    let mut keys = std::collections::HashSet::new();
    for n in &nodes {
        let fd = n.failure_domain.clone().unwrap_or_default();
        // Pool `failure_domain` strings come from the existing PoolConfig —
        // accept every synonym we've ever written. `osd` / `node` / `disk`
        // all mean "each OSD is its own domain" for Phase 1; we'll split
        // disk out as a finer level once OSDs report per-disk health.
        let key = match level {
            "region" => fd.region,
            "zone" => format!("{}:{}", fd.region, fd.zone),
            "datacenter" | "dc" => format!("{}:{}:{}", fd.region, fd.zone, fd.datacenter),
            "host" => format!(
                "{}:{}:{}:{}:{}",
                fd.region, fd.zone, fd.datacenter, fd.rack, fd.host
            ),
            "osd" | "node" | "disk" => hex::encode(&n.node_id),
            // Default (including "rack") — collapse to rack-level key.
            _ => format!("{}:{}:{}:{}", fd.region, fd.zone, fd.datacenter, fd.rack),
        };
        keys.insert(key);
    }
    let available = keys.len() as u64;
    let satisfiable = available >= shard_count;
    Json(serde_json::json!({
        "pool": pool_name,
        "required_level": level,
        "required_count": shard_count,
        "available_count": available,
        "satisfiable": satisfiable,
        "reason": if satisfiable {
            format!("{} distinct {}s available for {} shards", available, level, shard_count)
        } else {
            format!(
                "Pool needs {} distinct {}s but topology has only {}. Add more {}s or relax the pool's failure_domain.",
                shard_count, level, available, level
            )
        },
    }))
    .into_response()
}

fn offline_node(node_id: &[u8], addr: &str) -> serde_json::Value {
    serde_json::json!({
        "node_id": hex::encode(node_id),
        "node_name": "",
        "address": addr,
        "total_capacity": 0,
        "used_capacity": 0,
        "shard_count": 0,
        "uptime_seconds": 0,
        "disks": [],
        "online": false,
    })
}
