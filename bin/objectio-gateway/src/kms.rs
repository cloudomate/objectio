//! Local KMS provider — stands in for a proper external KMS while keeping
//! the system self-contained.
//!
//! Architecture:
//! 1. Each KMS key's 32-byte master key (the KEK) is generated on the gateway
//!    and wrapped with the gateway's service `MasterKey` before being sent to
//!    meta for persistence. Meta never sees plaintext KEK material.
//! 2. On `generate_data_key` / `decrypt` calls, the gateway looks up the
//!    wrapped KEK in its in-memory cache (falling back to a meta gRPC fetch),
//!    unwraps it with the service master key, and uses it to wrap/unwrap a
//!    fresh per-object DEK.
//! 3. The per-object DEK is bound to the caller-supplied encryption context
//!    (AES-GCM associated data), so a blob wrapped with one context cannot
//!    be unwrapped with a different context.

use objectio_kms::{
    DEK_LEN, GeneratedDataKey, KmsError, KmsProvider, MASTER_KEY_LEN, MasterKey,
    unwrap_dek_with_context, wrap_dek_with_context,
};
use objectio_proto::metadata::{
    GetKmsKeyRequest, metadata_service_client::MetadataServiceClient,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, warn};

/// Local KMS provider backed by the meta service for persistence.
pub struct LocalKmsProvider {
    meta_client: MetadataServiceClient<Channel>,
    service_master_key: MasterKey,
    /// Plaintext KEK cache, keyed by KMS key id. Populated lazily on first
    /// use; unbounded for now — at typical KMS-key counts (<10k) the memory
    /// cost is ~320KB which is negligible. Add an LRU if that changes.
    cache: RwLock<HashMap<String, [u8; MASTER_KEY_LEN]>>,
}

impl LocalKmsProvider {
    pub fn new(meta_client: MetadataServiceClient<Channel>, service_master_key: MasterKey) -> Self {
        Self {
            meta_client,
            service_master_key,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Strip the ARN prefix so we look up by the short key id meta stores.
    /// Accepts both `arn:obio:kms:::{id}` and `{id}` forms.
    fn normalize_key_id(key_id: &str) -> &str {
        key_id
            .strip_prefix("arn:obio:kms:::")
            .unwrap_or(key_id)
    }

    /// Fetch and unwrap a KEK from meta. Caches the plaintext KEK so
    /// subsequent ops on the same key are zero-RPC.
    async fn load_kek(&self, key_id: &str) -> Result<[u8; MASTER_KEY_LEN], KmsError> {
        let key_id = Self::normalize_key_id(key_id);
        if let Some(kek) = self.cache.read().get(key_id) {
            return Ok(*kek);
        }
        let mut client = self.meta_client.clone();
        let resp = client
            .get_kms_key(GetKmsKeyRequest {
                key_id: key_id.to_string(),
            })
            .await
            .map_err(|e| KmsError::ProviderError(format!("meta get_kms_key failed: {e}")))?
            .into_inner();
        if !resp.found {
            return Err(KmsError::KeyNotFound(key_id.to_string()));
        }
        let Some(k) = resp.key else {
            return Err(KmsError::KeyNotFound(key_id.to_string()));
        };
        if k.status != 0 {
            // 0 = KMS_KEY_ENABLED in the proto.
            return Err(KmsError::KeyDisabled(key_id.to_string()));
        }
        // Unwrap the KEK with the *empty* context — the service master key
        // wrap of KEKs isn't bound to a per-object context. (Per-object DEK
        // wrapping uses the caller's context as AAD; that's separate.)
        let kek = self
            .service_master_key
            .unwrap_dek(&k.wrapped_key_material)
            .map_err(|e| {
                warn!("failed to unwrap KEK for '{key_id}': {e}");
                KmsError::InvalidWrap(format!("unwrap KEK for '{key_id}': {e}"))
            })?;
        self.cache.write().insert(key_id.to_string(), kek);
        debug!("cached plaintext KEK for '{key_id}'");
        Ok(kek)
    }

    /// Invalidate the in-memory cache for a key (e.g. after deletion).
    pub fn invalidate(&self, key_id: &str) {
        self.cache.write().remove(key_id);
    }

    /// Wrap freshly-generated 32-byte key material with the gateway's service
    /// master key — used by the `CreateKey` admin flow before sending to meta.
    #[must_use]
    pub fn wrap_key_material(&self, raw: &[u8; MASTER_KEY_LEN]) -> Vec<u8> {
        self.service_master_key.wrap_dek(raw)
    }
}

#[async_trait::async_trait]
impl KmsProvider for LocalKmsProvider {
    async fn generate_data_key(
        &self,
        key_id: &str,
        encryption_context: &HashMap<String, String>,
    ) -> Result<GeneratedDataKey, KmsError> {
        let kek = self.load_kek(key_id).await?;
        let dek = objectio_kms::generate_dek();
        let wrapped = wrap_dek_with_context(&kek, &dek, encryption_context);
        Ok(GeneratedDataKey {
            plaintext_dek: dek,
            wrapped_dek: wrapped,
        })
    }

    async fn decrypt(
        &self,
        key_id: &str,
        wrapped_dek: &[u8],
        encryption_context: &HashMap<String, String>,
    ) -> Result<[u8; DEK_LEN], KmsError> {
        let kek = self.load_kek(key_id).await?;
        unwrap_dek_with_context(&kek, wrapped_dek, encryption_context)
    }

    async fn key_exists(&self, key_id: &str) -> Result<bool, KmsError> {
        let key_id = Self::normalize_key_id(key_id);
        if self.cache.read().contains_key(key_id) {
            return Ok(true);
        }
        let mut client = self.meta_client.clone();
        let resp = client
            .get_kms_key(GetKmsKeyRequest {
                key_id: key_id.to_string(),
            })
            .await
            .map_err(|e| KmsError::ProviderError(format!("meta get_kms_key failed: {e}")))?
            .into_inner();
        Ok(resp.found)
    }
}


// ============================================================================
// Vault KMS provider
// ============================================================================

/// `KmsProvider` backed by HashiCorp Vault's Transit secrets engine.
///
/// Configuration (env vars):
/// - `VAULT_ADDR` — Vault API base URL (e.g. `https://vault.example.com:8200`).
/// - `VAULT_TOKEN` — token with `transit/datakey/plaintext/*` +
///   `transit/decrypt/*` + `transit/keys/*` capabilities.
/// - `VAULT_TRANSIT_PATH` (optional, default `transit`) — mount path of the
///   Transit secrets engine if remounted under a different name.
///
/// Per-object encryption context is canonicalized and base64-encoded, then
/// passed to Vault as the `context` parameter so decrypt requires the same
/// context (AEAD binding at the Vault layer).
pub struct VaultKmsProvider {
    client: reqwest::Client,
    base_url: String,
    token: String,
    /// Path prefix — usually `transit`, but users may remount. Full URLs look
    /// like `{base_url}/v1/{transit_path}/datakey/plaintext/{key_id}`.
    transit_path: String,
}

impl VaultKmsProvider {
    /// Build from explicit parts — used by the reload path when the config
    /// comes from meta rather than env vars.
    #[must_use]
    pub fn from_parts(
        client: reqwest::Client,
        base_url: String,
        token: String,
        transit_path: String,
    ) -> Self {
        Self {
            client,
            base_url,
            token,
            transit_path,
        }
    }

    fn url(&self, tail: &str) -> String {
        format!("{}/v1/{}/{}", self.base_url, self.transit_path, tail)
    }

    fn encode_context(context: &std::collections::HashMap<String, String>) -> String {
        use base64::Engine;
        let aad = objectio_kms::canonical_context_aad(context);
        base64::engine::general_purpose::STANDARD.encode(aad)
    }
}

#[async_trait::async_trait]
impl objectio_kms::KmsProvider for VaultKmsProvider {
    async fn generate_data_key(
        &self,
        key_id: &str,
        encryption_context: &std::collections::HashMap<String, String>,
    ) -> Result<objectio_kms::GeneratedDataKey, objectio_kms::KmsError> {
        use base64::Engine;
        let url = self.url(&format!("datakey/plaintext/{key_id}"));
        let mut body = serde_json::json!({});
        if !encryption_context.is_empty() {
            body["context"] = serde_json::Value::String(Self::encode_context(encryption_context));
        }
        let resp = self
            .client
            .post(&url)
            .header("X-Vault-Token", &self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault body: {e}")))?;
        if !status.is_success() {
            if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::BAD_REQUEST
            {
                return Err(objectio_kms::KmsError::KeyNotFound(key_id.to_string()));
            }
            return Err(objectio_kms::KmsError::ProviderError(format!(
                "vault {status}: {text}"
            )));
        }
        let json: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault json: {e}")))?;
        let plaintext_b64 = json["data"]["plaintext"]
            .as_str()
            .ok_or_else(|| objectio_kms::KmsError::ProviderError("vault: no plaintext".into()))?;
        let ciphertext = json["data"]["ciphertext"]
            .as_str()
            .ok_or_else(|| objectio_kms::KmsError::ProviderError("vault: no ciphertext".into()))?;
        let pt_bytes = base64::engine::general_purpose::STANDARD
            .decode(plaintext_b64)
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault b64: {e}")))?;
        if pt_bytes.len() != objectio_kms::DEK_LEN {
            return Err(objectio_kms::KmsError::ProviderError(format!(
                "vault returned {}-byte DEK, expected {}",
                pt_bytes.len(),
                objectio_kms::DEK_LEN
            )));
        }
        let mut plaintext_dek = [0u8; objectio_kms::DEK_LEN];
        plaintext_dek.copy_from_slice(&pt_bytes);
        Ok(objectio_kms::GeneratedDataKey {
            plaintext_dek,
            wrapped_dek: ciphertext.as_bytes().to_vec(),
        })
    }

    async fn decrypt(
        &self,
        key_id: &str,
        wrapped_dek: &[u8],
        encryption_context: &std::collections::HashMap<String, String>,
    ) -> Result<[u8; objectio_kms::DEK_LEN], objectio_kms::KmsError> {
        use base64::Engine;
        let ciphertext = std::str::from_utf8(wrapped_dek).map_err(|_| {
            objectio_kms::KmsError::InvalidWrap("vault ciphertext must be UTF-8 string".into())
        })?;
        let url = self.url(&format!("decrypt/{key_id}"));
        let mut body = serde_json::json!({ "ciphertext": ciphertext });
        if !encryption_context.is_empty() {
            body["context"] = serde_json::Value::String(Self::encode_context(encryption_context));
        }
        let resp = self
            .client
            .post(&url)
            .header("X-Vault-Token", &self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault body: {e}")))?;
        if !status.is_success() {
            if status == reqwest::StatusCode::NOT_FOUND {
                return Err(objectio_kms::KmsError::KeyNotFound(key_id.to_string()));
            }
            return Err(objectio_kms::KmsError::InvalidWrap(format!(
                "vault {status}: {text}"
            )));
        }
        let json: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault json: {e}")))?;
        let plaintext_b64 = json["data"]["plaintext"]
            .as_str()
            .ok_or_else(|| objectio_kms::KmsError::ProviderError("vault: no plaintext".into()))?;
        let pt_bytes = base64::engine::general_purpose::STANDARD
            .decode(plaintext_b64)
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault b64: {e}")))?;
        if pt_bytes.len() != objectio_kms::DEK_LEN {
            return Err(objectio_kms::KmsError::InvalidWrap(format!(
                "vault returned {}-byte DEK, expected {}",
                pt_bytes.len(),
                objectio_kms::DEK_LEN
            )));
        }
        let mut dek = [0u8; objectio_kms::DEK_LEN];
        dek.copy_from_slice(&pt_bytes);
        Ok(dek)
    }

    async fn key_exists(&self, key_id: &str) -> Result<bool, objectio_kms::KmsError> {
        let url = self.url(&format!("keys/{key_id}"));
        let resp = self
            .client
            .get(&url)
            .header("X-Vault-Token", &self.token)
            .send()
            .await
            .map_err(|e| objectio_kms::KmsError::ProviderError(format!("vault: {e}")))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !resp.status().is_success() {
            return Err(objectio_kms::KmsError::ProviderError(format!(
                "vault key-exists {}",
                resp.status()
            )));
        }
        Ok(true)
    }
}

// ============================================================================
// Backend selection + provider construction
// ============================================================================

/// Config for the active KMS backend. Persisted in meta at key `kms/config`
/// and also constructible from CLI flag + env. When present in meta, takes
/// precedence over the CLI/env path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum KmsBackendConfig {
    /// SSE-KMS disabled. SSE-S3 still works if a service master key is set.
    Disabled,
    /// Local: each KMS key's material is generated on the gateway and wrapped
    /// by the service master key, persisted in meta.
    Local,
    /// HashiCorp Vault Transit engine.
    Vault {
        addr: String,
        #[serde(default = "default_transit_path")]
        transit_path: String,
        /// Raw token — persisted in meta. `GET /_admin/kms/config` redacts it.
        token: String,
    },
}

fn default_transit_path() -> String {
    "transit".to_string()
}

impl KmsBackendConfig {
    /// Best-effort resolution from CLI flag + env at startup. Returns
    /// `Disabled` if the user asked for Vault but env isn't set.
    #[must_use]
    pub fn from_cli(backend: &str) -> Self {
        match backend {
            "local" => Self::Local,
            "vault" => {
                let addr = std::env::var("VAULT_ADDR").unwrap_or_default();
                let token = std::env::var("VAULT_TOKEN").unwrap_or_default();
                let transit_path =
                    std::env::var("VAULT_TRANSIT_PATH").unwrap_or_else(|_| "transit".to_string());
                if addr.is_empty() || token.is_empty() {
                    Self::Disabled
                } else {
                    Self::Vault {
                        addr,
                        transit_path,
                        token,
                    }
                }
            }
            _ => Self::Disabled,
        }
    }

    /// Short label for logs / `GET /_admin/kms/status`.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Local => "local",
            Self::Vault { .. } => "vault",
        }
    }
}

/// The meta `config` table key that stores the KMS backend configuration.
///
/// Shares the generic `ConfigEntry` machinery with OIDC, cluster settings,
/// etc. so we don't need a bespoke persistence surface.
pub const KMS_CONFIG_KEY: &str = "kms/config";

/// Fetch the persisted KMS backend config from meta. Returns `None` if the
/// entry is missing, empty, or fails to decode — callers should fall back
/// to CLI/env defaults in that case.
pub async fn load_backend_config_from_meta(
    meta_client: objectio_proto::metadata::metadata_service_client::MetadataServiceClient<
        tonic::transport::Channel,
    >,
) -> Option<KmsBackendConfig> {
    use objectio_proto::metadata::GetConfigRequest;
    let mut client = meta_client;
    let resp = client
        .get_config(GetConfigRequest {
            key: KMS_CONFIG_KEY.to_string(),
        })
        .await
        .ok()?
        .into_inner();
    if !resp.found {
        return None;
    }
    let entry = resp.entry?;
    if entry.value.is_empty() {
        return None;
    }
    match serde_json::from_slice::<KmsBackendConfig>(&entry.value) {
        Ok(c) => Some(c),
        Err(e) => {
            tracing::warn!(
                "kms/config in meta is not valid JSON ({e}); falling back to CLI/env"
            );
            None
        }
    }
}

/// Build the KMS providers from a resolved config + master key. Shared
/// between startup and the admin reload path.
///
/// Returns `(kms_local, kms)` where `kms_local` is populated only for the
/// `Local` backend (for admin endpoints) and `kms` is the polymorphic
/// handle used by SSE PUT/GET.
#[must_use]
pub fn build_kms_provider(
    meta_client: objectio_proto::metadata::metadata_service_client::MetadataServiceClient<
        tonic::transport::Channel,
    >,
    master_key: Option<&objectio_kms::MasterKey>,
    config: &KmsBackendConfig,
) -> (
    Option<std::sync::Arc<LocalKmsProvider>>,
    Option<std::sync::Arc<dyn objectio_kms::KmsProvider>>,
) {
    use std::sync::Arc;
    match config {
        KmsBackendConfig::Disabled => (None, None),
        KmsBackendConfig::Local => {
            let Some(mk) = master_key else {
                tracing::warn!(
                    "KMS backend=local requested but no service master key is configured; SSE-KMS disabled"
                );
                return (None, None);
            };
            let local = Arc::new(LocalKmsProvider::new(meta_client, mk.clone()));
            (Some(Arc::clone(&local)), Some(local as Arc<dyn objectio_kms::KmsProvider>))
        }
        KmsBackendConfig::Vault {
            addr,
            transit_path,
            token,
        } => {
            if addr.is_empty() || token.is_empty() {
                tracing::warn!("KMS backend=vault but addr/token missing; SSE-KMS disabled");
                return (None, None);
            }
            let client = match reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("failed to build vault reqwest client: {e}");
                    return (None, None);
                }
            };
            let provider = VaultKmsProvider::from_parts(
                client,
                addr.trim_end_matches('/').to_string(),
                token.clone(),
                transit_path.clone(),
            );
            (None, Some(Arc::new(provider) as Arc<dyn objectio_kms::KmsProvider>))
        }
    }
}

// ============================================================================
// Admin API handlers: /_admin/kms/*
// ============================================================================
//
// Policy surface (AIStor-style) — when a caller hits one of these endpoints,
// the handler checks the user is an admin, and (in phase 2c) short-circuits.
// Phase 2d will thread `kms:*` actions through `PolicyEvaluator` for
// finer-grained grants like `{Action: "kms:KeyStatus", Resource: "arn:obio:kms:::keys-abc-*"}`.

use crate::s3::{AppState, S3Error};
use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use objectio_auth::AuthResult;
use objectio_auth::policy::{BucketPolicy, PolicyDecision, RequestContext};
use objectio_proto::metadata::{
    CreateKmsKeyRequest, DeleteKmsKeyRequest, GetKmsKeyRequest as MetaGetKmsKeyRequest,
    GetPolicyRequest, ListAttachedPoliciesRequest, ListKmsKeysRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Check whether the caller is allowed to perform `action` on `resource`
/// under the KMS admin API.
///
/// Precedence:
/// 1. Built-in admin user (`user/admin`) — always allowed.
/// 2. Console session — always allowed (the console is an admin tool).
/// 3. SigV4-authenticated user — evaluate their attached named IAM policies;
///    any explicit Allow wins, any explicit Deny wins, else deny.
/// 4. No auth at all — 401.
#[allow(clippy::result_large_err)]
async fn check_kms_policy(
    state: &AppState,
    auth: &Option<Extension<AuthResult>>,
    headers: &HeaderMap,
    action: &str,
    resource: &str,
) -> Option<Response> {
    // 1 + 4. Admin short-circuit + no-auth check.
    let auth_result = match auth {
        Some(Extension(a)) => {
            if a.user_arn.ends_with("user/admin") {
                return None;
            }
            a
        }
        None => {
            // 2. Console session bypass — only for system-admin sessions.
            // Tenant users must fall through to SigV4 + IAM policy eval.
            if let Some(s) = crate::console_auth::validate_session_from_headers(headers) {
                if s.tenant.is_empty() {
                    return None;
                }
                return Some((StatusCode::FORBIDDEN, "Not authorized").into_response());
            }
            return Some(
                (StatusCode::UNAUTHORIZED, "Authentication required").into_response(),
            );
        }
    };

    // 3. Evaluate attached IAM policies for this user against (action, resource).
    // The AuthResult carries the canonical user_id (matches what was used at
    // AttachPolicy time). user_arn is a display-name-based ARN and won't
    // match attachments keyed by user_id.
    let mut client = state.meta_client.clone();
    let attached = match client
        .list_attached_policies(ListAttachedPoliciesRequest {
            user_id: auth_result.user_id.clone(),
            group_id: String::new(),
        })
        .await
    {
        Ok(r) => r.into_inner().policy_names,
        Err(e) => {
            tracing::error!("list_attached_policies for {}: {e}", auth_result.user_id);
            return Some(S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    };

    let mut any_allow = false;
    for name in attached {
        let Ok(resp) = client
            .get_policy(GetPolicyRequest { name: name.clone() })
            .await
        else {
            continue;
        };
        let inner = resp.into_inner();
        if !inner.found {
            continue;
        }
        let Some(policy_obj) = inner.policy else {
            continue;
        };
        let Ok(policy) = BucketPolicy::from_json(&policy_obj.policy_json) else {
            tracing::warn!(
                "attached policy '{name}' for {} failed to parse",
                auth_result.user_id
            );
            continue;
        };
        let ctx = RequestContext::new(&auth_result.user_arn, action, resource);
        match state.policy_evaluator.evaluate(&policy, &ctx) {
            PolicyDecision::Deny => {
                return Some(S3Error::xml_response(
                    "AccessDenied",
                    &format!("Action {action} on {resource} denied by attached policy {name}"),
                    StatusCode::FORBIDDEN,
                ));
            }
            PolicyDecision::Allow => any_allow = true,
            PolicyDecision::ImplicitDeny => {}
        }
    }

    if any_allow {
        None
    } else {
        Some(S3Error::xml_response(
            "AccessDenied",
            &format!("No attached policy allows {action} on {resource}"),
            StatusCode::FORBIDDEN,
        ))
    }
}

fn kms_available(state: &AppState) -> Option<Response> {
    if state.kms_local().is_some() {
        None
    } else if state.kms().is_some() {
        // KMS is configured but the backend is external (Vault, AWS KMS) —
        // local admin endpoints can't manage external keys.
        Some(S3Error::xml_response(
            "NotImplemented",
            "KMS admin endpoints manage local keys only; the active backend is external (Vault or AWS KMS) — manage keys in that system directly",
            StatusCode::NOT_IMPLEMENTED,
        ))
    } else {
        Some(S3Error::xml_response(
            "ServiceUnavailable",
            "SSE master key not configured — KMS admin endpoints are disabled",
            StatusCode::SERVICE_UNAVAILABLE,
        ))
    }
}

/// `GET /_admin/kms/status` — whether KMS is operational.
pub async fn admin_kms_status(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:Status", "arn:obio:kms:::").await {
        return deny;
    }
    let enabled = state.kms().is_some();
    let backend = if state.kms_local().is_some() {
        "local"
    } else if state.kms().is_some() {
        "external"
    } else {
        "disabled"
    };
    let body = json!({
        "enabled": enabled,
        "backend": backend,
        "master_key_configured": state.master_key.is_some(),
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

/// `GET /_admin/kms/version` — server version.
pub async fn admin_kms_version(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:Version", "arn:obio:kms:::").await {
        return deny;
    }
    let body = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": "objectio-kms/v1",
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

/// `GET /_admin/kms/api` — list of supported endpoints.
pub async fn admin_kms_api(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:API", "arn:obio:kms:::").await {
        return deny;
    }
    let body = json!({
        "endpoints": [
            {"method": "GET",    "path": "/_admin/kms/status"},
            {"method": "GET",    "path": "/_admin/kms/version"},
            {"method": "GET",    "path": "/_admin/kms/api"},
            {"method": "POST",   "path": "/_admin/kms/keys"},
            {"method": "GET",    "path": "/_admin/kms/keys"},
            {"method": "GET",    "path": "/_admin/kms/keys/{key_id}"},
            {"method": "DELETE", "path": "/_admin/kms/keys/{key_id}"},
        ]
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

#[derive(Deserialize)]
pub struct CreateKmsKeyBody {
    #[serde(default)]
    pub key_id: String, // Optional — server generates if empty
    #[serde(default)]
    pub description: String,
}

#[derive(Serialize)]
struct KmsKeyView {
    key_id: String,
    arn: String,
    description: String,
    status: &'static str,
    created_at: u64,
    updated_at: u64,
    created_by: String,
}

fn status_name(code: i32) -> &'static str {
    // Mirrors the KmsKeyStatus enum in proto.
    match code {
        0 => "Enabled",
        1 => "Disabled",
        2 => "PendingDeletion",
        _ => "Unknown",
    }
}

fn to_view(k: &objectio_proto::metadata::KmsKey) -> KmsKeyView {
    KmsKeyView {
        key_id: k.key_id.clone(),
        arn: k.arn.clone(),
        description: k.description.clone(),
        status: status_name(k.status),
        created_at: k.created_at,
        updated_at: k.updated_at,
        created_by: k.created_by.clone(),
    }
}

/// `POST /_admin/kms/keys` — create a new KMS key.
pub async fn admin_create_kms_key(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<CreateKmsKeyBody>,
) -> Response {
    // The final key id may be server-generated; evaluate against the wildcard
    // ARN so policies can grant `kms:CreateKey` on `arn:obio:kms:::*`.
    let create_resource = if body.key_id.trim().is_empty() {
        "arn:obio:kms:::*".to_string()
    } else {
        format!("arn:obio:kms:::{}", body.key_id.trim())
    };
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:CreateKey", &create_resource).await {
        return deny;
    }
    if let Some(r) = kms_available(&state) {
        return r;
    }
    // Generate 32 random bytes for the KEK and wrap it with the gateway's
    // service master key before persisting — meta never sees plaintext material.
    let local = state
        .kms_local()
        .expect("checked by kms_available");

    let mut raw = [0u8; MASTER_KEY_LEN];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut raw);
    let wrapped = local.wrap_key_material(&raw);

    let created_by = auth
        .as_ref()
        .map(|Extension(a)| a.user_arn.clone())
        .unwrap_or_else(|| "console".to_string());

    let mut client = state.meta_client.clone();
    match client
        .create_kms_key(CreateKmsKeyRequest {
            key_id: body.key_id,
            description: body.description,
            wrapped_key_material: wrapped,
            created_by,
        })
        .await
    {
        Ok(resp) => {
            let Some(k) = resp.into_inner().key else {
                return S3Error::xml_response(
                    "InternalError",
                    "KMS key creation returned no key",
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            };
            let v = to_view(&k);
            Response::builder()
                .status(StatusCode::CREATED)
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(serde_json::to_string(&v).unwrap()))
                .unwrap()
        }
        Err(e) if e.code() == tonic::Code::AlreadyExists => S3Error::xml_response(
            "KmsKeyAlreadyExists",
            e.message(),
            StatusCode::CONFLICT,
        ),
        Err(e) => {
            tracing::error!("create_kms_key failed: {e}");
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

#[derive(Deserialize, Default)]
pub struct ListKmsKeysParams {
    #[serde(default)]
    pub max_results: Option<u32>,
    #[serde(default)]
    pub page_token: Option<String>,
}

/// `GET /_admin/kms/keys` — list KMS keys.
pub async fn admin_list_kms_keys(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Query(params): Query<ListKmsKeysParams>,
) -> Response {
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:ListKeys", "arn:obio:kms:::*").await {
        return deny;
    }
    if let Some(r) = kms_available(&state) {
        return r;
    }
    let mut client = state.meta_client.clone();
    match client
        .list_kms_keys(ListKmsKeysRequest {
            max_results: params.max_results.unwrap_or(100),
            page_token: params.page_token.unwrap_or_default(),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            let keys: Vec<KmsKeyView> = inner.keys.iter().map(to_view).collect();
            let body = json!({
                "keys": keys,
                "next_page_token": inner.next_page_token,
            });
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap()
        }
        Err(e) => S3Error::xml_response(
            "InternalError",
            &e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

/// `GET /_admin/kms/keys/{key_id}` — key status.
pub async fn admin_get_kms_key(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Response {
    let resource = format!("arn:obio:kms:::{key_id}");
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:KeyStatus", &resource).await {
        return deny;
    }
    if let Some(r) = kms_available(&state) {
        return r;
    }
    let mut client = state.meta_client.clone();
    match client.get_kms_key(MetaGetKmsKeyRequest { key_id }).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            if !inner.found {
                return S3Error::xml_response(
                    "NoSuchKmsKey",
                    "The specified KMS key does not exist",
                    StatusCode::NOT_FOUND,
                );
            }
            let Some(k) = inner.key else {
                return S3Error::xml_response(
                    "InternalError",
                    "KMS key entry missing",
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            };
            let v = to_view(&k);
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(serde_json::to_string(&v).unwrap()))
                .unwrap()
        }
        Err(e) => S3Error::xml_response(
            "InternalError",
            &e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

/// `DELETE /_admin/kms/keys/{key_id}` — delete a key and drop the cached KEK.
pub async fn admin_delete_kms_key(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Response {
    let resource = format!("arn:obio:kms:::{key_id}");
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:DeleteKey", &resource).await {
        return deny;
    }
    if let Some(r) = kms_available(&state) {
        return r;
    }
    // Drop the in-memory cached plaintext KEK.
    if let Some(local) = state.kms_local() {
        local.invalidate(&key_id);
    }
    let mut client = state.meta_client.clone();
    match client.delete_kms_key(DeleteKmsKeyRequest { key_id: key_id.clone() }).await {
        Ok(_) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(axum::body::Body::empty())
            .unwrap(),
        Err(e) => S3Error::xml_response(
            "InternalError",
            &e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

// ============================================================================
// Dynamic backend config — /_admin/kms/config
// ============================================================================

/// View of the backend config shown to admins. Never serializes the raw
/// Vault token; always returns a redacted placeholder so a GET can safely
/// be rendered in the console.
#[derive(Serialize)]
#[serde(tag = "backend", rename_all = "lowercase")]
enum KmsConfigView {
    Disabled,
    Local,
    Vault {
        addr: String,
        transit_path: String,
        /// `true` when a token is persisted (redacted in the view). The
        /// raw token bytes never leave the gateway process.
        token_set: bool,
    },
}

fn redact_config(cfg: &KmsBackendConfig) -> KmsConfigView {
    match cfg {
        KmsBackendConfig::Disabled => KmsConfigView::Disabled,
        KmsBackendConfig::Local => KmsConfigView::Local,
        KmsBackendConfig::Vault {
            addr,
            transit_path,
            token,
        } => KmsConfigView::Vault {
            addr: addr.clone(),
            transit_path: transit_path.clone(),
            token_set: !token.is_empty(),
        },
    }
}

/// `GET /_admin/kms/config` — current backend config (token redacted).
pub async fn admin_kms_get_config(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = check_kms_policy(&state, &auth, &headers, "kms:Status", "arn:obio:kms:::").await {
        return deny;
    }
    let cfg = load_backend_config_from_meta(state.meta_client.clone())
        .await
        .unwrap_or(KmsBackendConfig::Disabled);
    let view = redact_config(&cfg);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            serde_json::to_string(&view).unwrap_or_else(|_| "{}".to_string()),
        ))
        .unwrap()
}

/// `PUT /_admin/kms/config` — persist new backend config in meta and
/// hot-swap the providers. Body is the JSON `KmsBackendConfig` payload.
///
/// For the Vault variant, an empty `token` field on an update means "keep
/// the existing token" — admins can edit addr/transit_path from the UI
/// without re-entering the token every time.
pub async fn admin_kms_put_config(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Some(deny) = check_kms_policy(
        &state,
        &auth,
        &headers,
        "kms:CreateKey",
        "arn:obio:kms:::*",
    )
    .await
    {
        return deny;
    }
    let mut new_cfg: KmsBackendConfig = match serde_json::from_slice(&body) {
        Ok(c) => c,
        Err(e) => {
            return S3Error::xml_response(
                "InvalidArgument",
                &format!("kms/config body must be valid JSON: {e}"),
                StatusCode::BAD_REQUEST,
            );
        }
    };
    // External KMS backends (Vault) require an Enterprise license. The
    // built-in Local backend is always available because it's what backs
    // SSE-S3 on Community too.
    if matches!(new_cfg, KmsBackendConfig::Vault { .. })
        && let Err(r) = crate::license_gate::require_feature(
            &state,
            objectio_license::Feature::Kms,
        )
    {
        return r;
    }
    // Preserve existing Vault token when the payload omits it (empty string).
    if let KmsBackendConfig::Vault { token, .. } = &mut new_cfg
        && token.is_empty()
    {
        let existing = load_backend_config_from_meta(state.meta_client.clone()).await;
        if let Some(KmsBackendConfig::Vault { token: old, .. }) = existing {
            *token = old;
        }
    }
    let bytes = serde_json::to_vec(&new_cfg).unwrap_or_default();
    let actor = auth
        .as_ref()
        .map(|Extension(a)| a.user_arn.clone())
        .unwrap_or_else(|| "console".to_string());
    let mut client = state.meta_client.clone();
    if let Err(e) = client
        .set_config(objectio_proto::metadata::SetConfigRequest {
            key: KMS_CONFIG_KEY.to_string(),
            value: bytes,
            updated_by: actor,
        })
        .await
    {
        tracing::error!("persist kms/config failed: {e}");
        return S3Error::xml_response(
            "InternalError",
            &e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }
    // Rebuild providers from the new config and atomic-swap.
    let (local, provider) =
        build_kms_provider(state.meta_client.clone(), state.master_key.as_ref(), &new_cfg);
    state.set_kms(provider, local);
    tracing::info!("KMS backend reloaded to {}", new_cfg.label());
    let view = redact_config(&new_cfg);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            serde_json::to_string(&view).unwrap_or_else(|_| "{}".to_string()),
        ))
        .unwrap()
}

/// `DELETE /_admin/kms/config` — remove persisted config. Next boot will
/// fall back to CLI/env. The running provider keeps serving until the next
/// restart to avoid orphaning in-flight requests.
pub async fn admin_kms_delete_config(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    if let Some(deny) = check_kms_policy(
        &state,
        &auth,
        &headers,
        "kms:DeleteKey",
        "arn:obio:kms:::*",
    )
    .await
    {
        return deny;
    }
    let mut client = state.meta_client.clone();
    let _ = client
        .delete_config(objectio_proto::metadata::DeleteConfigRequest {
            key: KMS_CONFIG_KEY.to_string(),
        })
        .await;
    tracing::info!("KMS backend config cleared from meta");
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(axum::body::Body::empty())
        .unwrap()
}

/// `POST /_admin/kms/test` body — specifies which key and optional context
/// the operator wants to validate.
#[derive(Deserialize)]
pub struct KmsTestBody {
    #[serde(default)]
    pub key_id: String,
    #[serde(default)]
    pub encryption_context: HashMap<String, String>,
}

/// `POST /_admin/kms/test` — end-to-end round-trip against the currently
/// active provider: generate a fresh DEK, decrypt it back, compare.
/// Returns `{ok: bool, error: String}` so the console can render a clear
/// green/red status on the Save form.
pub async fn admin_kms_test(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Json(body): Json<KmsTestBody>,
) -> Response {
    if let Some(deny) = check_kms_policy(
        &state,
        &auth,
        &headers,
        "kms:KeyStatus",
        "arn:obio:kms:::*",
    )
    .await
    {
        return deny;
    }
    let Some(provider) = state.kms() else {
        let body = json!({"ok": false, "error": "KMS is disabled on this gateway"});
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
    };
    if body.key_id.trim().is_empty() {
        let body = json!({"ok": false, "error": "key_id is required"});
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
    }
    let result = match provider
        .generate_data_key(&body.key_id, &body.encryption_context)
        .await
    {
        Ok(data_key) => {
            match provider
                .decrypt(&body.key_id, &data_key.wrapped_dek, &body.encryption_context)
                .await
            {
                Ok(dek) if dek == data_key.plaintext_dek => {
                    json!({"ok": true, "error": null})
                }
                Ok(_) => json!({
                    "ok": false,
                    "error": "DEK round-trip mismatch (decrypt returned a different key)"
                }),
                Err(e) => json!({"ok": false, "error": format!("decrypt: {e}")}),
            }
        }
        Err(e) => json!({"ok": false, "error": format!("generate_data_key: {e}")}),
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(result.to_string()))
        .unwrap()
}
