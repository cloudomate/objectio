//! S3 API handlers

/// Maximum shard size in bytes (must fit in a storage block)
/// Block size is 4MB with ~96 bytes overhead, so use 4MB - 4KB for safety margin
const MAX_SHARD_SIZE: usize = 4 * 1024 * 1024 - 4096; // ~4MB per shard

use crate::osd_pool::{
    OsdPool, copy_object_meta_on_osd, delete_object_meta_from_osd,
    delete_object_meta_from_osd_versioned, get_object_meta_from_osd, put_object_meta_to_osd,
    put_object_meta_to_osd_versioned, read_shard_from_osd, write_shard_to_osd,
};
use crate::scatter_gather::ScatterGatherEngine;
use axum::{
    Extension,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use objectio_auth::{
    AuthResult,
    policy::{BucketPolicy, PolicyDecision, PolicyEvaluator, RequestContext},
};
use base64::Engine;
use objectio_common::ErasureConfig;
use objectio_erasure::{
    ErasureCodec,
    backend::{LrcBackend, LrcConfig, RustSimdLrcBackend},
};
use objectio_proto::metadata::{
    AbortMultipartUploadRequest,
    CompleteMultipartUploadRequest as ProtoCompleteMultipartUploadRequest,
    CreateAccessKeyRequest,
    CreateBucketRequest,
    CreateMultipartUploadRequest,
    // IAM types
    CreateUserRequest,
    BucketSseConfiguration,
    DeleteAccessKeyRequest,
    DeleteBucketEncryptionRequest,
    DeleteBucketLifecycleRequest,
    DeleteBucketPolicyRequest,
    DeleteBucketRequest,
    DeleteUserRequest,
    ErasureType,
    GetBucketEncryptionRequest,
    GetBucketLifecycleRequest,
    GetMultipartUploadRequest,
    GetAccessKeyForAuthRequest,
    GetBucketPolicyRequest,
    GetBucketRequest,
    GetBucketVersioningRequest,
    GetListingNodesRequest,
    GetObjectLockConfigRequest,
    GetPlacementRequest,
    GetUserRequest,
    LegalHold,
    LifecycleConfiguration as ProtoLifecycleConfig,
    LifecycleRule as ProtoLifecycleRule,
    ListAccessKeysRequest,
    ListBucketsRequest,
    ListMultipartUploadsRequest,
    ListPartsRequest,
    ListUsersRequest,
    ObjectLockConfiguration as ProtoObjectLockConfig,
    ObjectMeta,
    ObjectRetention,
    PartInfo,
    PutBucketEncryptionRequest,
    PutBucketLifecycleRequest,
    PutBucketVersioningRequest,
    PutObjectLockConfigRequest,
    RegisterPartRequest,
    RetentionMode,
    RetentionRule,
    SetBucketPolicyRequest,
    ShardLocation,
    SseAlgorithm,
    SseRule,
    StripeMeta,
    VersioningState,
    metadata_service_client::MetadataServiceClient,
};
use quick_xml::se::to_string as to_xml;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Application state shared across handlers
pub struct AppState {
    pub meta_client: MetadataServiceClient<Channel>,
    pub osd_pool: Arc<OsdPool>,
    pub ec_k: u32,
    pub ec_m: u32,
    pub policy_evaluator: PolicyEvaluator,
    pub scatter_gather: ScatterGatherEngine,
    /// SSE-S3 master key for wrapping per-object DEKs. `None` when
    /// no master key was configured — PUTs targeting buckets with
    /// default encryption will fail in that case.
    pub master_key: Option<objectio_kms::MasterKey>,
    /// KMS provider used by SSE-KMS PUT/GET paths. Polymorphic so the same
    /// code handles local, Vault, and AWS KMS backends identically.
    ///
    /// Held behind a `RwLock` so `PUT /_admin/kms/config` can hot-swap the
    /// backend without restarting the gateway. Accessors below clone the
    /// inner `Arc` out of the lock before any async call so the lock is
    /// never held across an `await`.
    pub kms: parking_lot::RwLock<Option<Arc<dyn objectio_kms::KmsProvider>>>,
    /// Concrete handle to the local KMS provider (only when backend=local).
    /// Used by `/_admin/kms/*` key-management endpoints. External backends
    /// leave this `None` and those endpoints return `NotImplemented`.
    pub kms_local: parking_lot::RwLock<Option<Arc<crate::kms::LocalKmsProvider>>>,
    /// Installed license, gating Enterprise features. No license → Community
    /// tier (stored as `License::community()`). Held behind a `RwLock` so the
    /// `PUT /_admin/license` endpoint can swap it without restart.
    pub license: parking_lot::RwLock<Arc<objectio_license::License>>,
    /// The gateway's own failure-domain position. Drives locality-aware
    /// read routing (Phase 2): shards on OSDs that share enclosing levels
    /// are tried first. Fully-empty when not configured — routing then
    /// falls back to round-robin and this struct is inert.
    pub self_topology: objectio_placement::FailureDomainInfo,
}

impl AppState {
    /// Snapshot of the current KMS provider for polymorphic SSE use.
    /// Returns an owned `Arc` so the caller can `.await` without holding
    /// the internal lock.
    pub fn kms(&self) -> Option<Arc<dyn objectio_kms::KmsProvider>> {
        self.kms.read().clone()
    }

    /// Snapshot of the current local KMS provider (for admin endpoints).
    pub fn kms_local(&self) -> Option<Arc<crate::kms::LocalKmsProvider>> {
        self.kms_local.read().clone()
    }

    /// Atomic swap — used by `PUT /_admin/kms/config` and at startup.
    /// Both fields are updated under the same lock order so external SSE
    /// paths always see a consistent pair.
    pub fn set_kms(
        &self,
        kms: Option<Arc<dyn objectio_kms::KmsProvider>>,
        kms_local: Option<Arc<crate::kms::LocalKmsProvider>>,
    ) {
        *self.kms.write() = kms;
        *self.kms_local.write() = kms_local;
    }

    /// Snapshot of the currently installed license. Cloned as an owned `Arc`
    /// so callers can hold it across async work without blocking swaps.
    pub fn license(&self) -> Arc<objectio_license::License> {
        Arc::clone(&self.license.read())
    }

    /// Hot-swap the installed license — used at startup and by
    /// `PUT /_admin/license`.
    pub fn set_license(&self, license: Arc<objectio_license::License>) {
        *self.license.write() = license;
    }

    /// Convenience: is a given Enterprise feature currently licensed?
    pub fn has_feature(&self, feature: objectio_license::Feature) -> bool {
        self.license().allows(feature)
    }
}

/// Decision produced by [`resolve_sse_decision`].
#[derive(Debug, Clone)]
struct SseDecision {
    algorithm: SseAlgorithm,
    /// KMS key id (or ARN) — populated only for `SseKms`.
    kms_key_id: String,
    /// Optional encryption context for SSE-KMS. Bound to the DEK wrap as AEAD.
    encryption_context: HashMap<String, String>,
}

/// Resolve the effective SSE algorithm for an operation.
///
/// Precedence follows AWS: explicit `x-amz-server-side-encryption*` request
/// headers win, else the bucket default encryption, else plaintext.
#[allow(clippy::result_large_err)]
async fn resolve_sse_decision(
    meta_client: &mut MetadataServiceClient<Channel>,
    bucket: &str,
    headers: Option<&HeaderMap>,
) -> Result<Option<SseDecision>, Response> {
    // 1. Request headers.
    if let Some(h) = headers
        && let Some(algo_hdr) = h
            .get("x-amz-server-side-encryption")
            .and_then(|v| v.to_str().ok())
    {
        match algo_hdr {
            "AES256" => {
                return Ok(Some(SseDecision {
                    algorithm: SseAlgorithm::SseS3,
                    kms_key_id: String::new(),
                    encryption_context: HashMap::new(),
                }));
            }
            "aws:kms" => {
                let kms_key_id = h
                    .get("x-amz-server-side-encryption-aws-kms-key-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let encryption_context = parse_encryption_context_header(h)?;
                if kms_key_id.is_empty() {
                    return Err(S3Error::xml_response(
                        "InvalidArgument",
                        "x-amz-server-side-encryption: aws:kms requires x-amz-server-side-encryption-aws-kms-key-id when no bucket default KMS key is configured",
                        StatusCode::BAD_REQUEST,
                    ));
                }
                return Ok(Some(SseDecision {
                    algorithm: SseAlgorithm::SseKms,
                    kms_key_id,
                    encryption_context,
                }));
            }
            other => {
                return Err(S3Error::xml_response(
                    "InvalidArgument",
                    &format!("x-amz-server-side-encryption value '{other}' is not recognized"),
                    StatusCode::BAD_REQUEST,
                ));
            }
        }
    }

    // 2. Bucket default.
    let bucket_enc = match meta_client
        .get_bucket_encryption(GetBucketEncryptionRequest {
            bucket: bucket.to_string(),
        })
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            error!("Failed to fetch bucket encryption for {bucket}: {e}");
            return Err(S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    };
    let Some(rule) = bucket_enc.config.and_then(|c| c.rules.into_iter().next()) else {
        return Ok(None);
    };
    let rule_algo = SseAlgorithm::try_from(rule.algorithm).unwrap_or(SseAlgorithm::SseNone);
    match rule_algo {
        SseAlgorithm::SseNone | SseAlgorithm::SseC => Ok(None),
        SseAlgorithm::SseS3 => Ok(Some(SseDecision {
            algorithm: SseAlgorithm::SseS3,
            kms_key_id: String::new(),
            encryption_context: HashMap::new(),
        })),
        SseAlgorithm::SseKms => {
            if rule.kms_key_id.is_empty() {
                return Err(S3Error::xml_response(
                    "InvalidArgument",
                    "Bucket default encryption is aws:kms but has no KMSMasterKeyID",
                    StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
            Ok(Some(SseDecision {
                algorithm: SseAlgorithm::SseKms,
                kms_key_id: rule.kms_key_id,
                encryption_context: HashMap::new(),
            }))
        }
    }
}

/// Customer-supplied SSE-C material, validated.
///
/// The raw `key` never touches persistent storage — it lives in memory
/// only for the duration of the request.
struct SseCKey {
    key: [u8; objectio_kms::DEK_LEN],
    /// Base64-encoded MD5 of the raw key, echoed back in response headers.
    md5_b64: String,
}

/// Parse + validate SSE-C customer-key headers.
///
/// Returns `Ok(None)` when the headers are absent (i.e. this request isn't
/// SSE-C), `Ok(Some(_))` when all three are present and consistent, and
/// `Err(Response)` for partial/malformed header sets.
#[allow(clippy::result_large_err)]
fn parse_sse_c_headers(headers: &HeaderMap) -> Result<Option<SseCKey>, Response> {
    let algo = headers
        .get("x-amz-server-side-encryption-customer-algorithm")
        .and_then(|v| v.to_str().ok());
    let key = headers
        .get("x-amz-server-side-encryption-customer-key")
        .and_then(|v| v.to_str().ok());
    let md5 = headers
        .get("x-amz-server-side-encryption-customer-key-md5")
        .and_then(|v| v.to_str().ok());
    match (algo, key, md5) {
        (None, None, None) => Ok(None),
        (Some(a), Some(k), Some(m)) => {
            if a != "AES256" {
                return Err(S3Error::xml_response(
                    "InvalidArgument",
                    "x-amz-server-side-encryption-customer-algorithm must be AES256",
                    StatusCode::BAD_REQUEST,
                ));
            }
            let key_bytes = base64::engine::general_purpose::STANDARD
                .decode(k)
                .map_err(|e| {
                    S3Error::xml_response(
                        "InvalidArgument",
                        &format!("customer key must be valid base64: {e}"),
                        StatusCode::BAD_REQUEST,
                    )
                })?;
            if key_bytes.len() != objectio_kms::DEK_LEN {
                return Err(S3Error::xml_response(
                    "InvalidArgument",
                    &format!(
                        "customer key must decode to {} bytes, got {}",
                        objectio_kms::DEK_LEN,
                        key_bytes.len()
                    ),
                    StatusCode::BAD_REQUEST,
                ));
            }
            // MD5 binding — catches key-header corruption and prevents a
            // wrong key from silently producing garbage plaintext on GET.
            let computed = md5::compute(&key_bytes);
            let computed_b64 =
                base64::engine::general_purpose::STANDARD.encode(computed.0);
            if computed_b64 != m {
                return Err(S3Error::xml_response(
                    "InvalidArgument",
                    "x-amz-server-side-encryption-customer-key-md5 does not match MD5 of the customer key",
                    StatusCode::BAD_REQUEST,
                ));
            }
            let mut arr = [0u8; objectio_kms::DEK_LEN];
            arr.copy_from_slice(&key_bytes);
            Ok(Some(SseCKey {
                key: arr,
                md5_b64: m.to_string(),
            }))
        }
        _ => Err(S3Error::xml_response(
            "InvalidRequest",
            "SSE-C requires all three x-amz-server-side-encryption-customer-* headers",
            StatusCode::BAD_REQUEST,
        )),
    }
}

/// Parse the optional `x-amz-server-side-encryption-context` header.
///
/// AWS S3 delivers this as a base64-encoded JSON object of string→string.
#[allow(clippy::result_large_err)]
fn parse_encryption_context_header(
    headers: &HeaderMap,
) -> Result<HashMap<String, String>, Response> {
    let Some(raw) = headers
        .get("x-amz-server-side-encryption-context")
        .and_then(|v| v.to_str().ok())
    else {
        return Ok(HashMap::new());
    };
    let bytes = match base64::engine::general_purpose::STANDARD.decode(raw) {
        Ok(b) => b,
        Err(e) => {
            return Err(S3Error::xml_response(
                "InvalidArgument",
                &format!("x-amz-server-side-encryption-context must be valid base64: {e}"),
                StatusCode::BAD_REQUEST,
            ));
        }
    };
    match serde_json::from_slice::<HashMap<String, String>>(&bytes) {
        Ok(m) => Ok(m),
        Err(e) => Err(S3Error::xml_response(
            "InvalidArgument",
            &format!("x-amz-server-side-encryption-context must decode to a JSON object of strings: {e}"),
            StatusCode::BAD_REQUEST,
        )),
    }
}

/// Apply SSE to an incoming PUT. Consults the effective SSE decision
/// (request header, else bucket default) and encrypts the body with
/// AES-256-CTR using a per-object DEK.
///
/// For SSE-S3 the DEK is wrapped by the gateway's service master key.
/// For SSE-KMS it's wrapped via the `KmsProvider` (which in turn
/// unwraps a KEK held only on the gateway and binds the wrap to the
/// caller's encryption context).
#[allow(clippy::result_large_err)]
async fn apply_put_sse(
    state: &Arc<AppState>,
    meta_client: &mut MetadataServiceClient<Channel>,
    bucket: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<
    (
        Bytes,
        SseAlgorithm,
        String,
        Vec<u8>,
        Vec<u8>,
        HashMap<String, String>,
        Option<&'static str>,
        String, // sse_c_key_md5 — only populated for SSE-C; empty otherwise
    ),
    Response,
> {
    // SSE-C takes precedence over everything. AWS rejects a PUT that mixes
    // SSE-C customer-* headers with server-side algorithm headers, so the
    // presence of *any* SSE-C header activates this path. Warehouse-bucket
    // guard below.
    if let Some(cust) = parse_sse_c_headers(headers)? {
        // Hard-block SSE-C on warehouse buckets — query engines can't send
        // the customer key on every read, so Iceberg/Delta reads would all
        // fail. Refusing at PUT time is clearer than a silent 403 later.
        if is_warehouse_bucket(bucket) {
            return Err(S3Error::xml_response(
                "InvalidEncryptionAlgorithmError",
                "SSE-C is not supported on warehouse buckets — query engines cannot provide the customer key on every read. Use SSE-S3 or SSE-KMS instead.",
                StatusCode::BAD_REQUEST,
            ));
        }
        let iv = objectio_kms::generate_iv();
        let mut buf = body.to_vec();
        objectio_kms::encrypt_in_place(&cust.key, &iv, &mut buf);
        return Ok((
            Bytes::from(buf),
            SseAlgorithm::SseC,
            String::new(),  // no KMS key
            Vec::new(),     // no wrapped DEK — the client holds the key
            iv.to_vec(),
            HashMap::new(),
            None,           // SSE-C uses customer-algorithm headers, not x-amz-server-side-encryption
            cust.md5_b64,
        ));
    }

    let Some(decision) = resolve_sse_decision(meta_client, bucket, Some(headers)).await? else {
        return Ok((
            body,
            SseAlgorithm::SseNone,
            String::new(),
            Vec::new(),
            Vec::new(),
            HashMap::new(),
            None,
            String::new(),
        ));
    };

    match decision.algorithm {
        SseAlgorithm::SseS3 => {
            let Some(mk) = state.master_key.as_ref() else {
                error!(
                    "Bucket {bucket} requires SSE-S3 but gateway has no master key configured"
                );
                return Err(S3Error::xml_response(
                    "ServiceUnavailable",
                    "SSE master key not configured on the gateway — contact the administrator",
                    StatusCode::SERVICE_UNAVAILABLE,
                ));
            };
            let dek = objectio_kms::generate_dek();
            let iv = objectio_kms::generate_iv();
            let mut buf = body.to_vec();
            objectio_kms::encrypt_in_place(&dek, &iv, &mut buf);
            Ok((
                Bytes::from(buf),
                SseAlgorithm::SseS3,
                String::new(),
                mk.wrap_dek(&dek),
                iv.to_vec(),
                HashMap::new(),
                Some("AES256"),
                String::new(),
            ))
        }
        SseAlgorithm::SseKms => {
            // Enterprise gate. AWS returns 400 for unsupported encryption
            // modes, so we match that shape — machine-readable detail is in
            // the body.
            if !state.has_feature(objectio_license::Feature::Kms) {
                return Err(S3Error::xml_response(
                    "EnterpriseLicenseRequired",
                    "SSE-KMS requires an Enterprise license. Install one via PUT /_admin/license.",
                    StatusCode::FORBIDDEN,
                ));
            }
            let Some(kms) = state.kms() else {
                return Err(S3Error::xml_response(
                    "ServiceUnavailable",
                    "SSE-KMS is not configured on this gateway",
                    StatusCode::SERVICE_UNAVAILABLE,
                ));
            };
            let data_key = match kms
                .generate_data_key(&decision.kms_key_id, &decision.encryption_context)
                .await
            {
                Ok(g) => g,
                Err(objectio_kms::KmsError::KeyNotFound(id)) => {
                    return Err(S3Error::xml_response(
                        "KMS.NotFoundException",
                        &format!("KMS key '{id}' does not exist"),
                        StatusCode::BAD_REQUEST,
                    ));
                }
                Err(objectio_kms::KmsError::KeyDisabled(id)) => {
                    return Err(S3Error::xml_response(
                        "KMS.DisabledException",
                        &format!("KMS key '{id}' is disabled"),
                        StatusCode::BAD_REQUEST,
                    ));
                }
                Err(e) => {
                    error!("KMS generate_data_key for {} failed: {e}", decision.kms_key_id);
                    return Err(S3Error::xml_response(
                        "InternalError",
                        &e.to_string(),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    ));
                }
            };
            let iv = objectio_kms::generate_iv();
            let mut buf = body.to_vec();
            objectio_kms::encrypt_in_place(&data_key.plaintext_dek, &iv, &mut buf);
            Ok((
                Bytes::from(buf),
                SseAlgorithm::SseKms,
                decision.kms_key_id,
                data_key.wrapped_dek,
                iv.to_vec(),
                decision.encryption_context,
                Some("aws:kms"),
                String::new(),
            ))
        }
        _ => unreachable!("resolve_sse_decision returned unsupported algorithm"),
    }
}

/// Buckets whose objects are read by query engines (Iceberg, Delta Sharing),
/// which can't pass SSE-C headers on every read. Hard-block SSE-C writes
/// here rather than letting data land that's later unreadable.
///
/// Heuristic: buckets auto-provisioned by the Iceberg REST Catalog start
/// with `iceberg-`. The gateway's `--warehouse-location` flag also names
/// a catchall warehouse bucket (default `objectio-warehouse`); honor that
/// too.
fn is_warehouse_bucket(bucket: &str) -> bool {
    bucket.starts_with("iceberg-") || bucket == "objectio-warehouse"
}

/// Extract user metadata from request headers (x-amz-meta-* headers)
fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if name_str.starts_with("x-amz-meta-")
            && let Ok(value_str) = value.to_str()
        {
            // Strip the x-amz-meta- prefix for storage
            let key = name_str.strip_prefix("x-amz-meta-").unwrap_or(&name_str);
            metadata.insert(key.to_string(), value_str.to_string());
        }
    }
    metadata
}

/// Add user metadata headers to response builder
fn add_metadata_headers(
    mut builder: http::response::Builder,
    user_metadata: &HashMap<String, String>,
) -> http::response::Builder {
    for (key, value) in user_metadata {
        let header_name = format!("x-amz-meta-{}", key);
        builder = builder.header(header_name, value);
    }
    builder
}

/// Parsed Range header
#[derive(Debug, Clone, Copy)]
struct ByteRange {
    start: u64,
    end: u64, // inclusive
}

/// Parse HTTP Range header (e.g., "bytes=0-99" or "bytes=100-" or "bytes=-50")
fn parse_range_header(range_header: &str, total_size: u64) -> Option<ByteRange> {
    let range_header = range_header.trim();
    if !range_header.starts_with("bytes=") {
        return None;
    }

    let range_spec = &range_header[6..]; // strip "bytes="
    let parts: Vec<&str> = range_spec.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start_str = parts[0].trim();
    let end_str = parts[1].trim();

    if start_str.is_empty() && end_str.is_empty() {
        return None;
    }

    // Handle suffix range (bytes=-500 means last 500 bytes)
    if start_str.is_empty() {
        let suffix_len: u64 = end_str.parse().ok()?;
        if suffix_len == 0 || suffix_len > total_size {
            return Some(ByteRange {
                start: 0,
                end: total_size - 1,
            });
        }
        return Some(ByteRange {
            start: total_size - suffix_len,
            end: total_size - 1,
        });
    }

    let start: u64 = start_str.parse().ok()?;

    // Handle open-ended range (bytes=100- means from 100 to end)
    if end_str.is_empty() {
        if start >= total_size {
            return None;
        }
        return Some(ByteRange {
            start,
            end: total_size - 1,
        });
    }

    let end: u64 = end_str.parse().ok()?;

    // Validate range
    if start > end || start >= total_size {
        return None;
    }

    // Clamp end to total_size - 1
    let end = std::cmp::min(end, total_size - 1);

    Some(ByteRange { start, end })
}

/// Given a byte range and stripe metadata, return `(stripe_index, stripe_byte_offset)`
/// pairs for only the stripes that overlap the range.
fn overlapping_stripes(
    stripes: &[StripeMeta],
    object_size: u64,
    range: &ByteRange,
) -> Vec<(usize, u64)> {
    let mut offset = 0u64;
    let mut result = Vec::new();
    for (idx, stripe) in stripes.iter().enumerate() {
        let effective_size = if stripe.data_size > 0 {
            stripe.data_size
        } else if stripes.len() == 1 {
            object_size
        } else {
            // Multi-stripe without data_size: include conservatively
            // (the stripe loop will error out for this case)
            0
        };
        let stripe_end = offset + effective_size;
        // Include stripe if it overlaps the range, or if we can't determine its size
        if effective_size == 0 || (offset <= range.end && stripe_end > range.start) {
            result.push((idx, offset));
        }
        offset = stripe_end;
    }
    result
}

/// Populate policy-engine context variables from incoming S3 request headers.
///
/// These are the AWS IAM/S3 condition keys relevant to object-level SSE
/// enforcement — bucket policies like `Deny unless s3:x-amz-server-side-encryption`
/// read these values from `RequestContext.variables`.
fn sse_condition_vars(headers: Option<&HeaderMap>) -> HashMap<String, String> {
    let mut vars = HashMap::new();
    // Currently the gateway terminates TLS upstream (Cloudflare/ingress), so
    // every request here effectively came in over HTTPS. Mark it so
    // `aws:SecureTransport = "true"` conditions work.
    vars.insert("aws:SecureTransport".to_string(), "true".to_string());
    let Some(h) = headers else { return vars };
    if let Some(v) = h
        .get("x-amz-server-side-encryption")
        .and_then(|v| v.to_str().ok())
    {
        vars.insert(
            "s3:x-amz-server-side-encryption".to_string(),
            v.to_string(),
        );
    }
    if let Some(v) = h
        .get("x-amz-server-side-encryption-aws-kms-key-id")
        .and_then(|v| v.to_str().ok())
    {
        vars.insert(
            "s3:x-amz-server-side-encryption-aws-kms-key-id".to_string(),
            v.to_string(),
        );
    }
    vars
}

/// Check bucket policy and return error response if access is denied.
///
/// `headers` (when provided) is used to populate request-side condition
/// keys like `s3:x-amz-server-side-encryption`. Most callers pass the
/// incoming HTTP request headers; callers without a header-carrying
/// context (internal RPCs) can pass `None`.
async fn check_bucket_policy(
    state: &AppState,
    bucket: &str,
    user_arn: &str,
    action: &str,
    resource: &str,
    headers: Option<&HeaderMap>,
) -> Option<Response> {
    let mut client = state.meta_client.clone();

    // Get bucket policy
    match client
        .get_bucket_policy(GetBucketPolicyRequest {
            bucket: bucket.to_string(),
        })
        .await
    {
        Ok(response) => {
            let policy_resp = response.into_inner();
            if policy_resp.has_policy {
                // Parse and evaluate policy
                match BucketPolicy::from_json(&policy_resp.policy_json) {
                    Ok(policy) => {
                        let mut context = RequestContext::new(user_arn, action, resource);
                        for (k, v) in sse_condition_vars(headers) {
                            context = context.with_variable(k, v);
                        }
                        let decision = state.policy_evaluator.evaluate(&policy, &context);

                        match decision {
                            PolicyDecision::Deny => {
                                debug!(
                                    "Policy denied access: {} {} {}",
                                    user_arn, action, resource
                                );
                                Some(S3Error::xml_response(
                                    "AccessDenied",
                                    "Access Denied by bucket policy",
                                    StatusCode::FORBIDDEN,
                                ))
                            }
                            PolicyDecision::ImplicitDeny => {
                                // No explicit allow in policy - check if user is bucket owner
                                // For now, we allow implicit deny (owner has full access)
                                // In production, you'd check if user is the bucket owner
                                None
                            }
                            PolicyDecision::Allow => None,
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse bucket policy: {}", e);
                        // Invalid policy, don't block - log and continue
                        None
                    }
                }
            } else {
                // No policy set - allow (owner-only access by default)
                None
            }
        }
        Err(e) => {
            // Error fetching policy - don't block on policy errors
            if e.code() != tonic::Code::NotFound {
                error!("Failed to fetch bucket policy: {}", e);
            }
            None
        }
    }
}

/// Build ARN for an S3 resource
fn build_s3_arn(bucket: &str, key: Option<&str>) -> String {
    match key {
        Some(k) => format!("arn:obio:s3:::{}/{}", bucket, k),
        None => format!("arn:obio:s3:::{}", bucket),
    }
}

/// Query parameters for list objects
#[derive(Debug, Deserialize, Default)]
pub struct ListObjectsParams {
    prefix: Option<String>,
    delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    max_keys: Option<u32>,
    #[serde(rename = "continuation-token")]
    continuation_token: Option<String>,
    /// If present (even empty), this is a policy request
    policy: Option<String>,
    /// If present, this is a list object versions request
    versions: Option<String>,
    /// If present, this is a get bucket versioning request
    versioning: Option<String>,
    /// If present, this is a get object-lock configuration request
    #[serde(rename = "object-lock")]
    object_lock: Option<String>,
    /// If present, this is a get lifecycle configuration request
    lifecycle: Option<String>,
    /// If present, this is a get bucket encryption request
    encryption: Option<String>,
}

impl ListObjectsParams {
    /// Check if this is a policy operation (has ?policy in query string)
    pub fn is_policy_request(&self) -> bool {
        self.policy.is_some()
    }
}

/// Query parameters for POST bucket operations
#[derive(Debug, Deserialize, Default)]
pub struct PostBucketParams {
    /// If present, this is a delete objects request
    delete: Option<String>,
    /// If present, this is a list multipart uploads request (also handled by GET)
    #[allow(dead_code)]
    uploads: Option<String>,
}

impl PostBucketParams {
    /// Check if this is a delete objects request (has ?delete in query string)
    pub fn is_delete_request(&self) -> bool {
        self.delete.is_some()
    }
}

/// Query parameters for PUT bucket operations
#[derive(Debug, Deserialize, Default)]
pub struct PutBucketParams {
    /// If present (even empty), this is a policy request
    policy: Option<String>,
    /// If present, this is a versioning request
    versioning: Option<String>,
    /// If present, this is an object-lock configuration request
    #[serde(rename = "object-lock")]
    object_lock: Option<String>,
    /// If present, this is a lifecycle configuration request
    lifecycle: Option<String>,
    /// If present, this is a put bucket encryption request
    encryption: Option<String>,
}

/// Query parameters for DELETE bucket operations
#[derive(Debug, Deserialize, Default)]
pub struct DeleteBucketParams {
    /// If present (even empty), this is a policy request
    policy: Option<String>,
    /// If present, this is a list multipart uploads request
    #[allow(dead_code)]
    uploads: Option<String>,
    /// If present, this is a lifecycle configuration delete request
    lifecycle: Option<String>,
    /// If present, this is a bucket encryption delete request
    encryption: Option<String>,
}

/// Query parameters for PUT object operations (handles both simple PUT and multipart)
#[derive(Debug, Deserialize, Default)]
pub struct PutObjectParams {
    /// Upload ID for multipart part upload
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    /// Part number for multipart part upload (1-10000)
    #[serde(rename = "partNumber")]
    part_number: Option<u32>,
    /// If present, this is a put object retention request
    retention: Option<String>,
    /// If present, this is a put legal hold request
    #[serde(rename = "legal-hold")]
    legal_hold: Option<String>,
}

/// Query parameters for GET object operations (handles both GET and list parts)
#[derive(Debug, Deserialize, Default)]
pub struct GetObjectParams {
    /// Upload ID for list parts request
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    /// Max parts to return for list parts
    #[serde(rename = "max-parts")]
    max_parts: Option<u32>,
    /// Part number marker for pagination
    #[serde(rename = "part-number-marker")]
    part_number_marker: Option<u32>,
    /// Version ID for retrieving specific version (used by version-aware GET)
    #[serde(rename = "versionId")]
    #[allow(dead_code)]
    version_id: Option<String>,
    /// If present, this is a get object retention request
    retention: Option<String>,
    /// If present, this is a get legal hold request
    #[serde(rename = "legal-hold")]
    legal_hold: Option<String>,
}

/// Query parameters for POST object operations (handles multipart initiate/complete)
#[derive(Debug, Deserialize, Default)]
pub struct PostObjectParams {
    /// If present, initiate multipart upload
    uploads: Option<String>,
    /// Upload ID for complete multipart upload
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
}

/// Query parameters for DELETE object operations (handles both delete and abort)
#[derive(Debug, Deserialize, Default)]
pub struct DeleteObjectParams {
    /// Upload ID for abort multipart upload
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    /// Version ID for deleting specific version
    #[serde(rename = "versionId")]
    version_id: Option<String>,
}

// XML response types for S3 API

#[derive(Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
pub struct ListBucketsResult {
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "Buckets")]
    pub buckets: Buckets,
}

#[derive(Serialize)]
pub struct Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

#[derive(Serialize)]
pub struct Buckets {
    #[serde(rename = "Bucket")]
    pub bucket: Vec<Bucket>,
}

#[derive(Serialize)]
pub struct Bucket {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "CreationDate")]
    pub creation_date: String,
}

#[derive(Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResult {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: String,
    #[serde(rename = "Delimiter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    #[serde(rename = "KeyCount")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_count: Option<u32>,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "NextContinuationToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
    #[serde(rename = "CommonPrefixes")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
    #[serde(rename = "Contents")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub contents: Vec<ObjectContent>,
}

#[derive(Serialize)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

#[derive(Serialize)]
pub struct ObjectContent {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

#[derive(Serialize)]
#[serde(rename = "Error")]
pub struct S3Error {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(rename = "RequestId")]
    pub request_id: String,
}

impl S3Error {
    pub(crate) fn xml_response(code: &str, message: &str, status: StatusCode) -> Response {
        let error = S3Error {
            code: code.to_string(),
            message: message.to_string(),
            resource: None,
            request_id: Uuid::new_v4().to_string(),
        };

        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
            to_xml(&error).unwrap_or_default()
        );

        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/xml")
            .body(Body::from(xml))
            .unwrap()
    }
}

// ============================================================================
// Multipart Upload XML Types
// ============================================================================

/// Response for InitiateMultipartUpload
#[derive(Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
pub struct InitiateMultipartUploadResult {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// Response for CompleteMultipartUpload
#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
pub struct CompleteMultipartUploadResult {
    #[serde(rename = "Location")]
    pub location: String,
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// Response for ListParts
#[derive(Serialize)]
#[serde(rename = "ListPartsResult")]
pub struct ListPartsResult {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
    #[serde(rename = "PartNumberMarker")]
    pub part_number_marker: u32,
    #[serde(rename = "NextPartNumberMarker")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_part_number_marker: Option<u32>,
    #[serde(rename = "MaxParts")]
    pub max_parts: u32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Part")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<PartItem>,
}

/// Part item in ListParts response
#[derive(Serialize)]
pub struct PartItem {
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
}

/// Response for ListMultipartUploads
#[derive(Serialize)]
#[serde(rename = "ListMultipartUploadsResult")]
#[allow(dead_code)]
pub struct ListMultipartUploadsResult {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "KeyMarker")]
    pub key_marker: String,
    #[serde(rename = "UploadIdMarker")]
    pub upload_id_marker: String,
    #[serde(rename = "NextKeyMarker")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_key_marker: Option<String>,
    #[serde(rename = "NextUploadIdMarker")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_upload_id_marker: Option<String>,
    #[serde(rename = "MaxUploads")]
    pub max_uploads: u32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Upload")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub uploads: Vec<UploadItem>,
}

/// Upload item in ListMultipartUploads response
#[derive(Serialize)]
#[allow(dead_code)]
pub struct UploadItem {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
    #[serde(rename = "Initiated")]
    pub initiated: String,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

/// Request body for CompleteMultipartUpload (XML from client)
#[derive(Debug, Deserialize)]
#[serde(rename = "CompleteMultipartUpload")]
pub struct CompleteMultipartUploadXml {
    #[serde(rename = "Part", default)]
    pub parts: Vec<CompletePart>,
}

/// Part in CompleteMultipartUpload request
#[derive(Debug, Deserialize)]
pub struct CompletePart {
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    #[serde(rename = "ETag")]
    pub etag: String,
}

// ============================================================================
// DeleteObjects XML Types
// ============================================================================

/// Request body for DeleteObjects (XML from client)
#[derive(Debug, Deserialize)]
#[serde(rename = "Delete")]
pub struct DeleteObjectsRequest {
    #[serde(rename = "Quiet", default)]
    #[allow(dead_code)]
    pub quiet: bool,
    #[serde(rename = "Object", default)]
    pub objects: Vec<DeleteObjectIdentifier>,
}

/// Object identifier in DeleteObjects request
#[derive(Debug, Deserialize)]
pub struct DeleteObjectIdentifier {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId")]
    #[serde(default)]
    pub version_id: Option<String>,
}

/// Response for DeleteObjects
#[derive(Serialize)]
#[serde(rename = "DeleteResult")]
pub struct DeleteObjectsResult {
    #[serde(rename = "Deleted")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<DeletedObject>,
    #[serde(rename = "Error")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<DeleteError>,
}

/// Successfully deleted object
#[derive(Serialize)]
pub struct DeletedObject {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// Error deleting object
#[derive(Serialize)]
pub struct DeleteError {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
}

/// CopyObject response
#[derive(Serialize)]
#[serde(rename = "CopyObjectResult")]
pub struct CopyObjectResult {
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

fn timestamp_to_iso(ts: u64) -> String {
    use chrono::{DateTime, Utc};
    // Convert Unix timestamp to ISO 8601 format
    DateTime::<Utc>::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

fn timestamp_to_http_date(ts: u64) -> String {
    use chrono::{DateTime, Utc};
    // Convert Unix timestamp to HTTP date format (RFC 7231)
    // Example: "Sun, 06 Nov 1994 08:49:37 GMT"
    DateTime::<Utc>::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 GMT".to_string())
}

/// List all buckets (GET /)
pub async fn list_buckets(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
) -> Response {
    let tenant = auth
        .as_ref()
        .map(|Extension(a)| a.tenant.clone())
        .unwrap_or_default();

    let mut client = state.meta_client.clone();

    match client
        .list_buckets(ListBucketsRequest {
            owner: String::new(),
            tenant,
        })
        .await
    {
        Ok(response) => {
            let buckets = response.into_inner();
            let result = ListBucketsResult {
                owner: Owner {
                    id: "objectio".to_string(),
                    display_name: "ObjectIO User".to_string(),
                },
                buckets: Buckets {
                    bucket: buckets
                        .buckets
                        .into_iter()
                        .map(|b| Bucket {
                            name: b.name,
                            creation_date: timestamp_to_iso(b.created_at),
                        })
                        .collect(),
                },
            };

            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to list buckets: {}", e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

/// Create bucket or set bucket policy/versioning/lock/lifecycle
/// (PUT /{bucket} or PUT /{bucket}?policy|versioning|object-lock|lifecycle)
pub async fn create_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<PutBucketParams>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if params.policy.is_some() {
        return put_bucket_policy_internal(state, bucket, body).await;
    }
    if params.versioning.is_some() {
        return put_bucket_versioning_internal(state, bucket, body).await;
    }
    if params.object_lock.is_some() {
        return put_object_lock_config_internal(state, bucket, body).await;
    }
    if params.lifecycle.is_some() {
        return put_bucket_lifecycle_internal(state, bucket, body).await;
    }
    if params.encryption.is_some() {
        return put_bucket_encryption_internal(state, bucket, body).await;
    }

    // Check for object lock at bucket creation
    let enable_lock = headers
        .get("x-amz-bucket-object-lock-enabled")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    let mut client = state.meta_client.clone();

    let tenant = auth
        .as_ref()
        .map(|Extension(a)| a.tenant.clone())
        .unwrap_or_default();

    match client
        .create_bucket(CreateBucketRequest {
            name: bucket.clone(),
            owner: "default".to_string(),
            storage_class: "STANDARD".to_string(),
            region: "us-east-1".to_string(),
            tenant,
        })
        .await
    {
        Ok(_) => {
            // If object lock requested, enable versioning and lock config
            if enable_lock {
                let _ = client
                    .put_bucket_versioning(PutBucketVersioningRequest {
                        bucket: bucket.clone(),
                        state: VersioningState::VersioningEnabled.into(),
                    })
                    .await;
                let _ = client
                    .put_object_lock_configuration(PutObjectLockConfigRequest {
                        bucket: bucket.clone(),
                        config: Some(ProtoObjectLockConfig {
                            enabled: true,
                            default_retention: None,
                        }),
                    })
                    .await;
            }
            info!(
                "Created bucket: {}{}",
                bucket,
                if enable_lock {
                    " (object-lock enabled)"
                } else {
                    ""
                }
            );
            Response::builder()
                .status(StatusCode::OK)
                .header("Location", format!("/{}", bucket))
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::AlreadyExists {
                S3Error::xml_response(
                    "BucketAlreadyExists",
                    "Bucket already exists",
                    StatusCode::CONFLICT,
                )
            } else {
                error!("Failed to create bucket: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// POST bucket operations (POST /{bucket}?delete - batch delete objects)
pub async fn post_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<PostBucketParams>,
    auth: Option<Extension<AuthResult>>,
    body: Bytes,
) -> Response {
    // Check if this is a delete objects request
    if params.is_delete_request() {
        return delete_objects(State(state), Path(bucket), auth, body).await;
    }

    // Unknown POST operation on bucket
    S3Error::xml_response(
        "InvalidRequest",
        "Invalid POST request on bucket",
        StatusCode::BAD_REQUEST,
    )
}

/// Delete bucket or delete bucket policy (DELETE /{bucket} or DELETE /{bucket}?policy)
pub async fn delete_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<DeleteBucketParams>,
) -> Response {
    if params.policy.is_some() {
        return delete_bucket_policy_internal(state, bucket).await;
    }
    if params.lifecycle.is_some() {
        return delete_bucket_lifecycle_internal(state, bucket).await;
    }
    if params.encryption.is_some() {
        return delete_bucket_encryption_internal(state, bucket).await;
    }

    let mut client = state.meta_client.clone();

    match client
        .delete_bucket(DeleteBucketRequest {
            name: bucket.clone(),
        })
        .await
    {
        Ok(_) => {
            info!("Deleted bucket: {}", bucket);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response("NoSuchBucket", "Bucket not found", StatusCode::NOT_FOUND)
            } else if e.code() == tonic::Code::FailedPrecondition {
                S3Error::xml_response(
                    "BucketNotEmpty",
                    "Bucket is not empty",
                    StatusCode::CONFLICT,
                )
            } else {
                error!("Failed to delete bucket: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// Head bucket (HEAD /{bucket})
pub async fn head_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Response {
    let mut client = state.meta_client.clone();

    match client.get_bucket(GetBucketRequest { name: bucket }).await {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}

/// Head bucket with trailing slash (s3fs compatibility)
/// Route: HEAD /{bucket}/
pub async fn head_bucket_trailing(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Response {
    head_bucket(State(state), Path(bucket)).await
}

/// List objects with trailing slash (s3fs compatibility)
/// Route: GET /{bucket}/
pub async fn list_objects_trailing(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsParams>,
    auth: Option<Extension<AuthResult>>,
) -> Response {
    list_objects(State(state), Path(bucket), Query(params), auth).await
}

/// List objects or get bucket policy (GET /{bucket} or GET /{bucket}?policy)
pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsParams>,
    auth: Option<Extension<AuthResult>>,
) -> Response {
    if params.is_policy_request() {
        return get_bucket_policy_internal(state, bucket).await;
    }
    if params.versioning.is_some() {
        return get_bucket_versioning_internal(state, bucket).await;
    }
    if params.object_lock.is_some() {
        return get_object_lock_config_internal(state, bucket).await;
    }
    if params.lifecycle.is_some() {
        return get_bucket_lifecycle_internal(state, bucket).await;
    }
    if params.encryption.is_some() {
        return get_bucket_encryption_internal(state, bucket).await;
    }
    if params.versions.is_some() {
        return list_object_versions_internal(
            state,
            bucket,
            params.prefix.clone().unwrap_or_default(),
            params.max_keys.unwrap_or(1000),
        )
        .await;
    }

    // Check bucket policy if user is authenticated
    if let Some(Extension(auth_result)) = &auth {
        let resource = build_s3_arn(&bucket, None);
        if let Some(deny_response) = check_bucket_policy(
            &state,
            &bucket,
            &auth_result.user_arn,
            "s3:ListBucket",
            &resource,
            None,
        )
        .await
        {
            return deny_response;
        }
    }

    let prefix = params.prefix.clone().unwrap_or_default();
    let delimiter = params.delimiter.clone();
    let max_keys = params.max_keys.unwrap_or(1000);
    let continuation_token = params.continuation_token.as_deref();

    // First verify bucket exists
    let mut client = state.meta_client.clone();
    match client
        .get_bucket(GetBucketRequest {
            name: bucket.clone(),
        })
        .await
    {
        Ok(_) => {}
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                return S3Error::xml_response(
                    "NoSuchBucket",
                    "The specified bucket does not exist",
                    StatusCode::NOT_FOUND,
                );
            }
            error!("Failed to get bucket: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    }

    // Use scatter-gather to list objects from all OSDs
    let mut meta_client = state.meta_client.clone();
    match state
        .scatter_gather
        .list_objects(
            &mut meta_client,
            &bucket,
            &prefix,
            max_keys,
            continuation_token,
        )
        .await
    {
        Ok(list_result) => {
            // Process delimiter to extract common prefixes
            let (contents, common_prefixes) = if let Some(ref delim) = delimiter {
                let mut prefixes_set = std::collections::BTreeSet::new();
                let mut filtered_contents = Vec::new();

                for obj in list_result.objects {
                    // Get the part of the key after the prefix
                    let key_after_prefix = if obj.key.starts_with(&prefix) {
                        &obj.key[prefix.len()..]
                    } else {
                        &obj.key[..]
                    };

                    // Check if the key contains the delimiter after the prefix
                    if let Some(delim_pos) = key_after_prefix.find(delim.as_str()) {
                        // Extract the common prefix (prefix + everything up to and including delimiter)
                        let common_prefix =
                            format!("{}{}{}", prefix, &key_after_prefix[..delim_pos], delim);
                        prefixes_set.insert(common_prefix);
                    } else {
                        // No delimiter found - include this object in contents
                        filtered_contents.push(ObjectContent {
                            key: obj.key,
                            last_modified: timestamp_to_iso(obj.modified_at),
                            etag: obj.etag,
                            size: obj.size,
                            storage_class: obj.storage_class,
                        });
                    }
                }

                let common_prefixes: Vec<CommonPrefix> = prefixes_set
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: p })
                    .collect();

                (filtered_contents, common_prefixes)
            } else {
                // No delimiter - return all objects
                let contents = list_result
                    .objects
                    .into_iter()
                    .map(|o| ObjectContent {
                        key: o.key,
                        last_modified: timestamp_to_iso(o.modified_at),
                        etag: o.etag,
                        size: o.size,
                        storage_class: o.storage_class,
                    })
                    .collect();
                (contents, Vec::new())
            };

            let key_count = contents.len() + common_prefixes.len();
            let result = ListBucketResult {
                name: bucket,
                prefix,
                delimiter,
                max_keys,
                is_truncated: list_result.is_truncated,
                next_continuation_token: list_result.next_continuation_token,
                key_count: Some(key_count as u32),
                common_prefixes,
                contents,
            };

            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            use crate::scatter_gather::ScatterGatherError;
            match e {
                ScatterGatherError::NoNodesAvailable => {
                    warn!("No OSD nodes available for listing");
                    // Return empty result if no nodes available (cluster might be starting up)
                    let result = ListBucketResult {
                        name: bucket,
                        prefix,
                        delimiter,
                        max_keys,
                        is_truncated: false,
                        next_continuation_token: None,
                        key_count: Some(0),
                        common_prefixes: vec![],
                        contents: vec![],
                    };
                    let xml = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                        to_xml(&result).unwrap_or_default()
                    );
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "application/xml")
                        .body(Body::from(xml))
                        .unwrap()
                }
                ScatterGatherError::InvalidToken
                | ScatterGatherError::TokenSignatureMismatch
                | ScatterGatherError::TopologyChanged { .. } => S3Error::xml_response(
                    "InvalidArgument",
                    "Invalid continuation token",
                    StatusCode::BAD_REQUEST,
                ),
                _ => {
                    error!("Scatter-gather list failed: {}", e);
                    S3Error::xml_response(
                        "InternalError",
                        &e.to_string(),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                }
            }
        }
    }
}

/// Put object (PUT /{bucket}/{key})
/// Outcome of comparing a CopyObject's source SSE state against the
/// destination SSE decision resolved from the request + dest bucket.
struct CopyDecision {
    needs_reencrypt: bool,
}

/// Decide whether a CopyObject can stay on the metadata-only fast path.
///
/// Returns `Err(Response)` when either side uses SSE-C (not yet supported)
/// or when reading the source / resolving the dest decision fails.
#[allow(clippy::result_large_err)]
async fn copy_sse_decision(
    state: &Arc<AppState>,
    meta_client: &mut MetadataServiceClient<Channel>,
    source_bucket: &str,
    source_key: &str,
    dest_bucket: &str,
    copy_headers: &HeaderMap,
) -> Result<CopyDecision, Response> {
    // Peek source's SSE state via its ObjectMeta. We need the primary OSD
    // for the source to read the meta; do a cheap GetPlacement.
    let src_placement = meta_client
        .get_placement(GetPlacementRequest {
            bucket: source_bucket.to_string(),
            key: source_key.to_string(),
            size: 0,
            storage_class: "STANDARD".to_string(),
        })
        .await
        .map_err(|e| {
            S3Error::xml_response(
                "NoSuchKey",
                &format!("The specified source does not exist: {e}"),
                StatusCode::NOT_FOUND,
            )
        })?
        .into_inner();
    if src_placement.nodes.is_empty() {
        return Err(S3Error::xml_response(
            "InternalError",
            "No storage nodes available for source",
            StatusCode::SERVICE_UNAVAILABLE,
        ));
    }
    let src_osd = &src_placement.nodes[0];
    let source_meta =
        match get_object_meta_from_osd(&state.osd_pool, src_osd, source_bucket, source_key).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                return Err(S3Error::xml_response(
                    "NoSuchKey",
                    "The specified source does not exist",
                    StatusCode::NOT_FOUND,
                ));
            }
            Err(e) => {
                return Err(S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
        };

    let src_algo = SseAlgorithm::try_from(source_meta.encryption_algorithm)
        .unwrap_or(SseAlgorithm::SseNone);
    if src_algo == SseAlgorithm::SseC {
        return Err(S3Error::xml_response(
            "NotImplemented",
            "CopyObject for SSE-C source objects is not yet supported",
            StatusCode::NOT_IMPLEMENTED,
        ));
    }

    let dst_decision = resolve_sse_decision(meta_client, dest_bucket, Some(copy_headers)).await?;
    if let Some(ref d) = dst_decision
        && d.algorithm == SseAlgorithm::SseC
    {
        return Err(S3Error::xml_response(
            "NotImplemented",
            "CopyObject with an SSE-C destination is not yet supported",
            StatusCode::NOT_IMPLEMENTED,
        ));
    }

    // Fast-path eligibility: source and destination share exactly the same
    // SSE parameters. Otherwise the bytes need to be re-encrypted.
    let needs_reencrypt = match (src_algo, dst_decision.as_ref()) {
        (SseAlgorithm::SseNone, None) => false,
        (SseAlgorithm::SseS3, Some(d)) if d.algorithm == SseAlgorithm::SseS3 => false,
        (SseAlgorithm::SseKms, Some(d))
            if d.algorithm == SseAlgorithm::SseKms
                && d.kms_key_id == source_meta.kms_key_id =>
        {
            false
        }
        _ => true,
    };

    Ok(CopyDecision { needs_reencrypt })
}

/// Slow-path CopyObject: decrypt the source through the GET handler, then
/// re-PUT through the normal PUT handler so the destination bucket's SSE
/// settings (or request headers) control the re-encryption.
///
/// Cheap and correct but buffers the object in memory — same shape as the
/// rest of our PUT path (which is also `body: Bytes`). Streaming copies are
/// a separate future improvement.
async fn copy_object_reencrypt(
    state: Arc<AppState>,
    dest_bucket: String,
    dest_key: String,
    source_bucket: String,
    source_key: String,
    auth: Option<Extension<AuthResult>>,
    copy_headers: HeaderMap,
) -> Response {
    debug!(
        "CopyObject re-encrypt: {}/{} -> {}/{}",
        source_bucket, source_key, dest_bucket, dest_key
    );

    // 1. Read the source object as plaintext. The existing GET handler takes
    //    care of reconstruction + decryption.
    let get_resp = get_object(
        State(Arc::clone(&state)),
        Path((source_bucket.clone(), source_key.clone())),
        auth.clone(),
        HeaderMap::new(),
    )
    .await;
    if !get_resp.status().is_success() {
        return get_resp;
    }
    let (_parts, body) = get_resp.into_parts();
    let plaintext = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            error!("CopyObject re-encrypt: failed to buffer source: {e}");
            return S3Error::xml_response(
                "InternalError",
                "Failed to buffer source object for re-encryption",
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    // 2. Build headers for the destination PUT — carry over SSE settings
    //    and content-type from the copy request, drop x-amz-copy-source so
    //    the PUT handler doesn't recurse back into CopyObject.
    let mut put_headers = HeaderMap::new();
    for (name, value) in copy_headers.iter() {
        let lower = name.as_str().to_lowercase();
        if lower.starts_with("x-amz-server-side-encryption")
            || lower == "content-type"
            || lower.starts_with("x-amz-meta-")
        {
            put_headers.insert(name.clone(), value.clone());
        }
    }

    // 3. Re-PUT through the regular handler. Encryption/erasure-coding/
    //    metadata writing all happen through the same code path single-part
    //    PUTs use, so SSE transitions "just work".
    let put_resp = put_object(
        State(Arc::clone(&state)),
        Path((dest_bucket.clone(), dest_key.clone())),
        auth,
        put_headers,
        plaintext,
    )
    .await;
    if !put_resp.status().is_success() {
        return put_resp;
    }

    // 4. CopyObject wants a CopyObjectResult XML body, not the PUT's empty
    //    response. ETag comes from the PUT we just issued.
    let etag = put_resp
        .headers()
        .get("ETag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let sse_headers: Vec<(http::HeaderName, http::HeaderValue)> = put_resp
        .headers()
        .iter()
        .filter(|(k, _)| k.as_str().starts_with("x-amz-server-side-encryption"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let now_millis_display = timestamp_to_iso(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    );
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
        to_xml(&CopyObjectResult {
            etag: etag.clone(),
            last_modified: now_millis_display,
        })
        .unwrap_or_default()
    );
    info!(
        "CopyObject re-encrypt: {}/{} -> {}/{} ({} bytes)",
        source_bucket,
        source_key,
        dest_bucket,
        dest_key,
        plaintext_len_hint(&put_resp),
    );
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .header("ETag", &etag);
    for (k, v) in sse_headers {
        builder = builder.header(k, v);
    }
    builder.body(Body::from(xml)).unwrap()
}

/// Best-effort size hint for logging — parses `Content-Length` if present.
fn plaintext_len_hint(resp: &Response) -> u64 {
    resp.headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

pub async fn put_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Check for copy source header (CopyObject operation)
    let copy_source = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            // URL decode and strip leading slash if present
            let decoded = urlencoding::decode(s).unwrap_or_else(|_| s.into());
            decoded.trim_start_matches('/').to_string()
        });

    // CopyObject: metadata-only fast path when source and destination SSE
    // match; decrypt→re-encrypt slow path when they differ. SSE-C on either
    // side is deliberately unsupported (requires a separate set of
    // copy-source-* customer-key headers that we don't wire through yet).
    if let Some(ref source) = copy_source {
        // Parse source bucket/key (format: "bucket/key" or "/bucket/key")
        let parts: Vec<&str> = source.splitn(2, '/').collect();
        if parts.len() != 2 {
            return S3Error::xml_response(
                "InvalidArgument",
                "Invalid x-amz-copy-source format",
                StatusCode::BAD_REQUEST,
            );
        }
        let source_bucket = parts[0];
        let source_key = parts[1];

        // Auth check (PutObject on destination)
        if let Some(Extension(auth_result)) = &auth {
            let resource = build_s3_arn(&bucket, Some(&key));
            if let Some(deny_response) = check_bucket_policy(
                &state,
                &bucket,
                &auth_result.user_arn,
                "s3:PutObject",
                &resource,
                Some(&headers),
            )
            .await
            {
                return deny_response;
            }
        }

        let mut meta_client = state.meta_client.clone();

        // Pre-flight: peek at the source object's SSE state + resolve the
        // destination SSE decision from headers/bucket-default. If they
        // match, stay on the metadata-only fast path below. If they differ
        // (and neither side is SSE-C), take the re-encrypting slow path.
        let copy_decision = match copy_sse_decision(
            &state,
            &mut meta_client,
            source_bucket,
            source_key,
            &bucket,
            &headers,
        )
        .await
        {
            Ok(d) => d,
            Err(resp) => return resp,
        };
        if copy_decision.needs_reencrypt {
            // Box-pin to break the `put_object ↔ copy_object_reencrypt`
            // async recursion. The cycle is infrequent (only on
            // SSE-transition copies) so the boxed allocation is fine.
            return Box::pin(copy_object_reencrypt(
                state.clone(),
                bucket.clone(),
                key.clone(),
                source_bucket.to_string(),
                source_key.to_string(),
                auth.clone(),
                headers.clone(),
            ))
            .await;
        }

        debug!(
            "CopyObject fast-path: {}/{} -> {}/{}",
            source_bucket, source_key, bucket, key
        );

        // Get source OSD via CRUSH placement
        let src_placement = match meta_client
            .get_placement(GetPlacementRequest {
                bucket: source_bucket.to_string(),
                key: source_key.to_string(),
                size: 0,
                storage_class: "STANDARD".to_string(),
            })
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                error!("CopyObject: failed to get source placement: {}", e);
                return S3Error::xml_response(
                    "NoSuchKey",
                    "The specified key does not exist",
                    StatusCode::NOT_FOUND,
                );
            }
        };

        if src_placement.nodes.is_empty() {
            return S3Error::xml_response(
                "InternalError",
                "No storage nodes available for source",
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
        let src_osd = &src_placement.nodes[0];

        // Get dest OSD via CRUSH placement
        let dst_placement = match meta_client
            .get_placement(GetPlacementRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                size: 0,
                storage_class: "STANDARD".to_string(),
            })
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                error!("CopyObject: failed to get dest placement: {}", e);
                return S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

        if dst_placement.nodes.is_empty() {
            return S3Error::xml_response(
                "InternalError",
                "No storage nodes available for destination",
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
        let dst_osd = &dst_placement.nodes[0];

        // Fast-path: copy ObjectMeta without touching shard data
        let dest_meta = if src_osd.node_id == dst_osd.node_id {
            // Same OSD: single atomic RPC
            match copy_object_meta_on_osd(
                &state.osd_pool,
                src_osd,
                source_bucket,
                source_key,
                &bucket,
                &key,
            )
            .await
            {
                Ok(m) => m,
                Err(e) => {
                    error!("CopyObject: copy_object_meta failed: {}", e);
                    return S3Error::xml_response(
                        "NoSuchKey",
                        "The specified key does not exist",
                        StatusCode::NOT_FOUND,
                    );
                }
            }
        } else {
            // Different OSDs: read meta from source, write to dest
            let source_meta =
                match get_object_meta_from_osd(&state.osd_pool, src_osd, source_bucket, source_key)
                    .await
                {
                    Ok(Some(m)) => m,
                    Ok(None) => {
                        return S3Error::xml_response(
                            "NoSuchKey",
                            "The specified key does not exist",
                            StatusCode::NOT_FOUND,
                        );
                    }
                    Err(e) => {
                        error!("CopyObject: failed to read source meta: {}", e);
                        return S3Error::xml_response(
                            "InternalError",
                            &e.to_string(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                };

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let new_etag = format!("{:x}", Uuid::new_v4().as_u128());
            let dest_meta = ObjectMeta {
                bucket: bucket.clone(),
                key: key.clone(),
                etag: new_etag,
                created_at: now,
                modified_at: now,
                ..source_meta
            };

            if let Err(e) =
                put_object_meta_to_osd(&state.osd_pool, dst_osd, &bucket, &key, dest_meta.clone())
                    .await
            {
                error!("CopyObject: failed to write dest meta: {}", e);
                return S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
            dest_meta
        };

        info!(
            "CopyObject fast-path: {}/{} -> {}/{} ({} bytes, no data I/O)",
            source_bucket, source_key, bucket, key, dest_meta.size
        );

        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
            to_xml(&CopyObjectResult {
                etag: dest_meta.etag.clone(),
                last_modified: timestamp_to_iso(dest_meta.modified_at),
            })
            .unwrap_or_default()
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/xml")
            .header("ETag", dest_meta.etag)
            .body(Body::from(xml))
            .unwrap();
    }

    debug!(
        "PUT object: {}/{}, size={}, ec={}+{}",
        bucket,
        key,
        body.len(),
        state.ec_k,
        state.ec_m,
    );

    // Check bucket policy if user is authenticated
    if let Some(Extension(auth_result)) = &auth {
        let resource = build_s3_arn(&bucket, Some(&key));
        if let Some(deny_response) = check_bucket_policy(
            &state,
            &bucket,
            &auth_result.user_arn,
            "s3:PutObject",
            &resource,
            Some(&headers),
        )
        .await
        {
            return deny_response;
        }
    }

    let mut meta_client = state.meta_client.clone();

    // Generate object ID and ETag (MD5 of the *plaintext* body — matches AWS
    // SSE-S3/SSE-KMS ETag semantics; computed before we possibly encrypt).
    let object_id = *Uuid::new_v4().as_bytes();
    let etag = format!("\"{:x}\"", md5::compute(&body));
    let original_size = body.len() as u64;

    // SSE: if the request header or bucket default asks for encryption,
    // encrypt the body before it enters the erasure-coding path. Shards
    // on OSDs see ciphertext; the storage layer is oblivious.
    let (
        body,
        sse_algorithm,
        sse_kms_key_id,
        sse_encrypted_dek,
        sse_iv,
        sse_encryption_context,
        sse_response_header,
        sse_c_key_md5,
    ) = match apply_put_sse(&state, &mut meta_client, &bucket, &headers, body).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    // Check bucket versioning state
    let versioning_enabled = match meta_client
        .get_bucket_versioning(GetBucketVersioningRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => resp.into_inner().state() == VersioningState::VersioningEnabled,
        Err(_) => false,
    };
    let version_id = if versioning_enabled {
        Uuid::new_v4().to_string()
    } else {
        String::new()
    };

    // Get placement from metadata service
    let placement = match meta_client
        .get_placement(GetPlacementRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            size: original_size,
            storage_class: "STANDARD".to_string(),
        })
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            error!("Failed to get placement: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &format!("Failed to get placement: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let ec_k = placement.ec_k;
    let ec_m = placement.ec_m;
    let ec_type = ErasureType::try_from(placement.ec_type).unwrap_or(ErasureType::ErasureMds);
    let replication_count = placement.replication_count;

    // Replication mode: no EC, just write raw data to each replica
    // For large files, split into multiple stripes (each stripe <= MAX_SHARD_SIZE)
    if ec_type == ErasureType::ErasureReplication {
        let total_replicas = replication_count.max(1) as usize;

        // Split data into stripes (each stripe must fit in a block)
        let stripe_size = MAX_SHARD_SIZE;
        let num_stripes = body.len().div_ceil(stripe_size);

        debug!(
            "Replication mode: writing {} replicas x {} stripes for {}/{} (total size={})",
            total_replicas,
            num_stripes,
            bucket,
            key,
            body.len()
        );

        let mut all_stripes = Vec::with_capacity(num_stripes);
        let mut total_success = 0;

        for stripe_idx in 0..num_stripes {
            let stripe_start = stripe_idx * stripe_size;
            let stripe_end = std::cmp::min(stripe_start + stripe_size, body.len());
            let stripe_data = &body[stripe_start..stripe_end];
            let stripe_data_size = stripe_data.len() as u64;

            // Write this stripe to all replicas
            let mut write_futures = Vec::with_capacity(total_replicas);
            for i in 0..total_replicas {
                let placement_node = if i < placement.nodes.len() {
                    placement.nodes[i].clone()
                } else if !placement.nodes.is_empty() {
                    placement.nodes[i % placement.nodes.len()].clone()
                } else {
                    error!("No placement nodes available");
                    return S3Error::xml_response(
                        "InternalError",
                        "No storage nodes available",
                        StatusCode::SERVICE_UNAVAILABLE,
                    );
                };

                let pool = state.osd_pool.clone();
                let obj_id = object_id;
                let shard_data = stripe_data.to_vec();
                let pos = i as u32;
                let s_idx = stripe_idx as u64;

                write_futures.push(async move {
                    let result = write_shard_to_osd(
                        &pool,
                        &placement_node,
                        &obj_id,
                        s_idx, // stripe_id
                        pos,
                        shard_data,
                        1, // ec_k=1 for replication (full data)
                        0, // ec_m=0 for replication (no parity)
                    )
                    .await;
                    (pos, result, placement_node)
                });
            }

            let results = futures::future::join_all(write_futures).await;

            let mut success_count = 0;
            let mut shard_locs = Vec::with_capacity(total_replicas);

            for (pos, result, placement_node) in results {
                match result {
                    Ok(location) => {
                        success_count += 1;
                        shard_locs.push(ShardLocation {
                            position: pos,
                            node_id: location.node_id,
                            disk_id: location.disk_id,
                            offset: location.offset,
                            shard_type: placement_node.shard_type,
                            local_group: placement_node.local_group,
                        });
                        debug!(
                            "Wrote stripe {} replica {} to {}",
                            stripe_idx, pos, placement_node.node_address
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to write stripe {} replica {} to {}: {}",
                            stripe_idx, pos, placement_node.node_address, e
                        );
                        // Do NOT add failed replica locations to metadata —
                        // reading from an unwritten location returns garbage.
                    }
                }
            }

            // For replication, we need at least 1 successful write per stripe
            if success_count < 1 {
                error!(
                    "Replication failed for stripe {}: {} successful writes, need at least 1",
                    stripe_idx, success_count
                );
                return S3Error::xml_response(
                    "InternalError",
                    &format!(
                        "Replication failed for stripe {}: {} successful writes, need 1",
                        stripe_idx, success_count
                    ),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }

            total_success += success_count;
            shard_locs.sort_by_key(|l| l.position);

            all_stripes.push(StripeMeta {
                stripe_id: stripe_idx as u64,
                ec_k: 1,
                ec_m: 0,
                shards: shard_locs,
                ec_type: ErasureType::ErasureReplication.into(),
                ec_local_parity: 0,
                ec_global_parity: 0,
                local_group_size: 0,
                data_size: stripe_data_size,
                object_id: object_id.to_vec(), // Store object_id used for shards
                ..Default::default()
            });
        }

        // Store object metadata on primary OSD
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let object_meta = ObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            object_id: object_id.to_vec(),
            size: original_size,
            content_type: content_type.clone(),
            etag: etag.clone(),
            created_at: timestamp,
            modified_at: timestamp,
            stripes: all_stripes,
            user_metadata: extract_user_metadata(&headers),
            version_id: version_id.clone(),
            storage_class: "STANDARD".to_string(),
            is_delete_marker: false,
            retention: None,
            legal_hold: None,
            encryption_algorithm: sse_algorithm as i32,
            kms_key_id: sse_kms_key_id.clone(),
            encrypted_dek: sse_encrypted_dek.clone(),
            encryption_iv: sse_iv.clone(),
            encryption_context: sse_encryption_context.clone(),
        };

        let primary_osd = &placement.nodes[0];
        if let Err(e) = put_object_meta_to_osd_versioned(
            &state.osd_pool,
            primary_osd,
            &bucket,
            &key,
            object_meta,
            versioning_enabled,
        )
        .await
        {
            error!("Failed to store object metadata on OSD: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &format!("Failed to store object metadata: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }

        info!(
            "Created object (replication): {}/{}, size={}, stripes={}, replicas_written={}",
            bucket, key, original_size, num_stripes, total_success,
        );

        let mut resp = Response::builder()
            .status(StatusCode::OK)
            .header("ETag", etag);
        if !version_id.is_empty() {
            resp = resp.header("x-amz-version-id", &version_id);
        }
        if let Some(v) = sse_response_header {
            resp = resp.header("x-amz-server-side-encryption", v);
            if v == "aws:kms" && !sse_kms_key_id.is_empty() {
                resp = resp.header(
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    &sse_kms_key_id,
                );
            }
        }
        if sse_algorithm == SseAlgorithm::SseC {
            resp = resp
                .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
                .header(
                    "x-amz-server-side-encryption-customer-key-md5",
                    &sse_c_key_md5,
                );
        }
        return resp.body(Body::empty()).unwrap();
    }

    // EC mode: encode data with erasure coding
    // For large files, split into multiple stripes (each shard <= MAX_SHARD_SIZE)
    let total_shards = (ec_k + ec_m) as usize;

    // Calculate max stripe data size: each encoded shard must fit in MAX_SHARD_SIZE
    // shard_size = stripe_data_size / ec_k (approximately)
    // So max_stripe_data_size = MAX_SHARD_SIZE * ec_k
    let max_stripe_data_size = MAX_SHARD_SIZE * ec_k as usize;
    let num_stripes = body.len().div_ceil(max_stripe_data_size);

    debug!(
        "EC mode: encoding {}/{} ({} bytes) into {} stripes with {}+{} shards each",
        bucket,
        key,
        body.len(),
        num_stripes,
        ec_k,
        ec_m
    );

    let mut all_stripes = Vec::with_capacity(num_stripes);
    let mut total_shards_written = 0;

    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * max_stripe_data_size;
        let stripe_end = std::cmp::min(stripe_start + max_stripe_data_size, body.len());
        let stripe_data = &body[stripe_start..stripe_end];
        let stripe_data_size = stripe_data.len() as u64;

        // Encode this stripe with erasure coding - use LRC if specified
        let shards: Vec<Vec<u8>> = match ec_type {
            ErasureType::ErasureLrc => {
                // Use LRC backend with local parity groups
                let lrc_config = LrcConfig::new(
                    ec_k as u8,
                    placement.ec_local_parity as u8,
                    placement.ec_global_parity as u8,
                );
                let backend = match RustSimdLrcBackend::new(lrc_config) {
                    Ok(b) => b,
                    Err(e) => {
                        error!("Failed to create LRC backend: {}", e);
                        return S3Error::xml_response(
                            "InternalError",
                            &format!("LRC codec error: {}", e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                };

                // Pad data to shard size
                let shard_size = stripe_data.len().div_ceil(ec_k as usize);
                let padded_size = shard_size * ec_k as usize;
                let mut padded_data = stripe_data.to_vec();
                padded_data.resize(padded_size, 0);

                // Split into data shards
                let data_shards: Vec<&[u8]> =
                    padded_data.chunks(shard_size).take(ec_k as usize).collect();

                match backend.encode_lrc(&data_shards, shard_size) {
                    Ok(encoded) => encoded.all_shards(),
                    Err(e) => {
                        error!("Failed to encode stripe {} with LRC: {}", stripe_idx, e);
                        return S3Error::xml_response(
                            "InternalError",
                            &format!("LRC encoding failed for stripe {}: {}", stripe_idx, e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                }
            }
            _ => {
                // Use standard MDS Reed-Solomon
                let codec = match ErasureCodec::new(ErasureConfig::new(ec_k as u8, ec_m as u8)) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to create erasure codec: {}", e);
                        return S3Error::xml_response(
                            "InternalError",
                            &format!("Erasure coding error: {}", e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                };

                match codec.encode(stripe_data) {
                    Ok(s) => s.into_iter().map(|s| s.to_vec()).collect(),
                    Err(e) => {
                        error!("Failed to encode stripe {}: {}", stripe_idx, e);
                        return S3Error::xml_response(
                            "InternalError",
                            &format!("Erasure encoding failed for stripe {}: {}", stripe_idx, e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                }
            }
        };

        debug!(
            "Stripe {}: encoded {} bytes into {} shards of {} bytes each",
            stripe_idx,
            stripe_data.len(),
            shards.len(),
            shards.first().map(|s| s.len()).unwrap_or(0)
        );

        // Write shards to OSDs in parallel
        let mut write_futures = Vec::with_capacity(total_shards);

        // Use placements from metadata service, or fall back to round-robin if not enough
        for (i, shard) in shards.iter().enumerate() {
            let placement_node = if i < placement.nodes.len() {
                placement.nodes[i].clone()
            } else if !placement.nodes.is_empty() {
                // Round-robin if not enough placements
                placement.nodes[i % placement.nodes.len()].clone()
            } else {
                error!("No placement nodes available");
                return S3Error::xml_response(
                    "InternalError",
                    "No storage nodes available",
                    StatusCode::SERVICE_UNAVAILABLE,
                );
            };

            let pool = state.osd_pool.clone();
            let obj_id = object_id;
            let shard_data = shard.clone();
            let pos = i as u32;
            let s_idx = stripe_idx as u64;

            write_futures.push(async move {
                let result = write_shard_to_osd(
                    &pool,
                    &placement_node,
                    &obj_id,
                    s_idx, // stripe_id
                    pos,
                    shard_data,
                    ec_k,
                    ec_m,
                )
                .await;
                (pos, result, placement_node)
            });
        }

        // Wait for all writes and collect results
        let results = futures::future::join_all(write_futures).await;

        let mut success_count = 0;
        let mut shard_locs = Vec::with_capacity(total_shards);

        for (pos, result, placement_node) in results {
            match result {
                Ok(location) => {
                    success_count += 1;
                    shard_locs.push(ShardLocation {
                        position: pos,
                        node_id: location.node_id,
                        disk_id: location.disk_id,
                        offset: location.offset,
                        // Use shard type from placement, or default to data/parity based on position
                        shard_type: placement_node.shard_type,
                        local_group: placement_node.local_group,
                    });
                    debug!(
                        "Wrote stripe {} shard {} to {}",
                        stripe_idx, pos, placement_node.node_address
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to write stripe {} shard {} to {}: {}",
                        stripe_idx, pos, placement_node.node_address, e
                    );
                    // Do NOT add failed shard locations to metadata — the shard
                    // was never written, so reading from this location would
                    // return unrelated data and corrupt EC reconstruction.
                }
            }
        }

        // Check write quorum - need at least k shards to reconstruct data
        let quorum = ec_k as usize;
        if success_count < quorum {
            error!(
                "Write quorum not met for stripe {}: {} successful, need {} (ec_k={}, ec_m={}, total_shards={})",
                stripe_idx, success_count, quorum, ec_k, ec_m, total_shards
            );
            return S3Error::xml_response(
                "InternalError",
                &format!(
                    "Write quorum not met for stripe {}: {} successful writes, need {}",
                    stripe_idx, success_count, quorum
                ),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }

        total_shards_written += success_count;

        // Sort shard locations by position
        shard_locs.sort_by_key(|l| l.position);

        // Add stripe metadata
        all_stripes.push(StripeMeta {
            stripe_id: stripe_idx as u64,
            ec_k,
            ec_m,
            shards: shard_locs,
            // Use the EC type from placement response
            ec_type: placement.ec_type,
            ec_local_parity: placement.ec_local_parity,
            ec_global_parity: placement.ec_global_parity,
            local_group_size: placement.local_group_size,
            data_size: stripe_data_size, // Store this stripe's data size for decoding
            object_id: object_id.to_vec(), // Store object_id used for shards
            ..Default::default()
        });
    }

    // Store object metadata on primary OSD (position 0)
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Build ObjectMeta for OSD storage
    let object_meta = ObjectMeta {
        bucket: bucket.clone(),
        key: key.clone(),
        object_id: object_id.to_vec(),
        size: original_size,
        content_type: content_type.clone(),
        etag: etag.clone(),
        created_at: timestamp,
        modified_at: timestamp,
        stripes: all_stripes,
        user_metadata: extract_user_metadata(&headers),
        version_id: version_id.clone(),
        storage_class: "STANDARD".to_string(),
        is_delete_marker: false,
        retention: None,
        legal_hold: None,
        encryption_algorithm: sse_algorithm as i32,
        kms_key_id: sse_kms_key_id.clone(),
        encrypted_dek: sse_encrypted_dek,
        encryption_iv: sse_iv,
        encryption_context: sse_encryption_context,
    };

    // Get primary OSD (position 0 in placement)
    let primary_osd = &placement.nodes[0];

    // Store metadata on primary OSD
    if let Err(e) = put_object_meta_to_osd_versioned(
        &state.osd_pool,
        primary_osd,
        &bucket,
        &key,
        object_meta,
        versioning_enabled,
    )
    .await
    {
        error!("Failed to store object metadata on OSD: {}", e);
        return S3Error::xml_response(
            "InternalError",
            &format!("Failed to store object metadata: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    info!(
        "Created object: {}/{}, size={}, stripes={}, shards_written={}, primary_osd={}",
        bucket, key, original_size, num_stripes, total_shards_written, primary_osd.node_address,
    );

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", etag);
    if !version_id.is_empty() {
        resp = resp.header("x-amz-version-id", &version_id);
    }
    if let Some(v) = sse_response_header {
        resp = resp.header("x-amz-server-side-encryption", v);
        if v == "aws:kms" && !sse_kms_key_id.is_empty() {
            resp = resp.header(
                "x-amz-server-side-encryption-aws-kms-key-id",
                &sse_kms_key_id,
            );
        }
    }
    if sse_algorithm == SseAlgorithm::SseC {
        resp = resp
            .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
            .header(
                "x-amz-server-side-encryption-customer-key-md5",
                &sse_c_key_md5,
            );
    }
    resp.body(Body::empty()).unwrap()
}

/// Get object (GET /{bucket}/{key})
pub async fn get_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    debug!("GET object: {}/{}", bucket, key);

    // Parse Range header if present
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

    // Check bucket policy if user is authenticated
    if let Some(Extension(auth_result)) = &auth {
        let resource = build_s3_arn(&bucket, Some(&key));
        if let Some(deny_response) = check_bucket_policy(
            &state,
            &bucket,
            &auth_result.user_arn,
            "s3:GetObject",
            &resource,
            Some(&headers),
        )
        .await
        {
            return deny_response;
        }
    }

    let mut meta_client = state.meta_client.clone();

    // Get placement to find primary OSD (CRUSH is deterministic)
    let placement = match meta_client
        .get_placement(GetPlacementRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            size: 0, // Size not needed for lookup
            storage_class: "STANDARD".to_string(),
        })
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            error!("Failed to get placement: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &format!("Failed to get placement: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    if placement.nodes.is_empty() {
        return S3Error::xml_response(
            "InternalError",
            "No storage nodes available",
            StatusCode::SERVICE_UNAVAILABLE,
        );
    }

    // Build node_id -> address map from placement for shard reads
    let mut node_address_map: HashMap<Vec<u8>, String> = placement
        .nodes
        .iter()
        .map(|n| (n.node_id.clone(), n.node_address.clone()))
        .collect();

    // Build a node_id → failure_domain map for locality-aware EC read
    // ranking (Phase 2). One GetListingNodes call populates both this map
    // and fills any gaps in node_address_map. Empty on older meta servers
    // that don't carry failure_domain in ListingNode — in that case every
    // node ranks as `Unknown` distance and the ranked sort is a no-op.
    let mut node_topo_map: HashMap<Vec<u8>, objectio_placement::FailureDomainInfo> =
        HashMap::new();
    if let Ok(resp) = meta_client
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
        })
        .await
    {
        for n in resp.into_inner().nodes {
            node_address_map
                .entry(n.node_id.clone())
                .or_insert_with(|| n.address.clone());
            let fd = n.failure_domain.unwrap_or_default();
            node_topo_map.insert(
                n.node_id,
                objectio_placement::FailureDomainInfo::new_full(
                    &fd.region,
                    &fd.zone,
                    &fd.datacenter,
                    &fd.rack,
                    &fd.host,
                ),
            );
        }
    }

    // If we have stored shard locations pointing to nodes not in placement
    // (e.g. topology changed), fetch all active nodes as fallback
    // This is done lazily below only if a node_id is missing from the map.

    // Get object metadata from primary OSD (position 0)
    let primary_osd = &placement.nodes[0];
    let object = match get_object_meta_from_osd(&state.osd_pool, primary_osd, &bucket, &key).await {
        Ok(Some(obj)) => obj,
        Ok(None) => {
            return S3Error::xml_response("NoSuchKey", "Object not found", StatusCode::NOT_FOUND);
        }
        Err(e) => {
            error!("Failed to get object metadata from OSD: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    // Check for stripes
    if object.stripes.is_empty() {
        error!("Object has no stripe metadata: {}/{}", bucket, key);
        return S3Error::xml_response(
            "InternalError",
            "Object has no stripe metadata",
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    // Resolve SSE state up front so per-stripe decryption can be inlined.
    // For SSE-S3 we unwrap the DEK once and decrypt each stripe's contribution
    // to `all_data` below, using that stripe's IV (multipart) or the
    // object-level IV (legacy single-stripe).
    let object_sse_algo =
        SseAlgorithm::try_from(object.encryption_algorithm).unwrap_or(SseAlgorithm::SseNone);
    // `sse_response_header` is the value for `x-amz-server-side-encryption`
    // when the object is SSE-S3/KMS. `sse_c_key_md5` is populated for SSE-C
    // and drives the `x-amz-server-side-encryption-customer-*` headers.
    let (
        get_sse_dek,
        sse_response_header,
        sse_c_key_md5,
    ): (
        Option<[u8; objectio_kms::DEK_LEN]>,
        Option<&'static str>,
        String,
    ) = match object_sse_algo {
        SseAlgorithm::SseNone => (None, None, String::new()),
        SseAlgorithm::SseS3 => {
            let Some(mk) = state.master_key.as_ref() else {
                return S3Error::xml_response(
                    "ServiceUnavailable",
                    "SSE master key not configured on the gateway — cannot decrypt object",
                    StatusCode::SERVICE_UNAVAILABLE,
                );
            };
            match mk.unwrap_dek(&object.encrypted_dek) {
                Ok(dek) => (Some(dek), Some("AES256"), String::new()),
                Err(e) => {
                    error!("Failed to unwrap DEK for {}/{}: {}", bucket, key, e);
                    return S3Error::xml_response(
                        "InternalError",
                        "Failed to unwrap object DEK",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
            }
        }
        SseAlgorithm::SseKms => {
            let Some(kms) = state.kms() else {
                return S3Error::xml_response(
                    "ServiceUnavailable",
                    "SSE-KMS is not configured on this gateway — cannot decrypt object",
                    StatusCode::SERVICE_UNAVAILABLE,
                );
            };
            match kms
                .decrypt(
                    &object.kms_key_id,
                    &object.encrypted_dek,
                    &object.encryption_context,
                )
                .await
            {
                Ok(dek) => (Some(dek), Some("aws:kms"), String::new()),
                Err(objectio_kms::KmsError::KeyNotFound(_)) => {
                    return S3Error::xml_response(
                        "KMS.NotFoundException",
                        "Object's KMS key no longer exists",
                        StatusCode::NOT_FOUND,
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to unwrap KMS DEK for {}/{} (key {}): {}",
                        bucket, key, object.kms_key_id, e
                    );
                    return S3Error::xml_response(
                        "InternalError",
                        "Failed to unwrap object DEK",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
            }
        }
        SseAlgorithm::SseC => {
            let cust = match parse_sse_c_headers(&headers) {
                Ok(Some(c)) => c,
                Ok(None) => {
                    return S3Error::xml_response(
                        "InvalidRequest",
                        "The object was stored using a form of SSE-C; the customer key must be provided on the GET request",
                        StatusCode::BAD_REQUEST,
                    );
                }
                Err(resp) => return resp,
            };
            (Some(cust.key), None, cust.md5_b64)
        }
    };

    // Resolve byte range before fetching any stripe data
    let total_size = object.size;
    let resolved_range = match range_header {
        Some(range_str) => match parse_range_header(range_str, total_size) {
            Some(range) => Some(range),
            None => {
                // Invalid range — return 416 without fetching any stripes
                return Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header("Content-Range", format!("bytes */{total_size}"))
                    .body(Body::empty())
                    .unwrap();
            }
        },
        None => None,
    };

    // Log stripe layout for multi-stripe objects (multipart uploads)
    if object.stripes.len() > 1 {
        let stripe_sizes: Vec<u64> = object.stripes.iter().map(|s| s.data_size).collect();
        let stripe_total: u64 = stripe_sizes.iter().sum();
        info!(
            "Multi-stripe object {}/{}: {} stripes, stripe_sizes={:?}, stripe_total={}, object.size={}",
            bucket,
            key,
            object.stripes.len(),
            stripe_sizes,
            stripe_total,
            total_size
        );
        if stripe_total != total_size {
            warn!(
                "Stripe data_size sum ({}) does not match object size ({}) for {}/{}",
                stripe_total, total_size, bucket, key
            );
        }
    }

    // Determine which stripes to fetch (skip non-overlapping stripes for range requests)
    let stripe_plan: Vec<(usize, u64)> = if let Some(ref range) = resolved_range {
        let plan = overlapping_stripes(&object.stripes, total_size, range);
        debug!(
            "Range request bytes={}-{} for {}/{}: fetching {} of {} stripes",
            range.start,
            range.end,
            bucket,
            key,
            plan.len(),
            object.stripes.len()
        );
        plan
    } else {
        // Full object: all stripes, offsets unused since we don't slice
        object
            .stripes
            .iter()
            .enumerate()
            .map(|(i, _)| (i, 0u64))
            .collect()
    };

    // Pre-allocate with appropriate capacity
    let capacity = if let Some(ref range) = resolved_range {
        (range.end - range.start + 1) as usize
    } else {
        object.size as usize
    };
    let mut all_data = Vec::with_capacity(capacity);

    for &(stripe_idx, stripe_byte_offset) in &stripe_plan {
        let stripe = &object.stripes[stripe_idx];
        let ec_k = stripe.ec_k as usize;
        let ec_m = stripe.ec_m as usize;
        let stripe_ec_type =
            ErasureType::try_from(stripe.ec_type).unwrap_or(ErasureType::ErasureMds);

        // Use stripe's data_size if available, otherwise fall back to object size (for backwards compat)
        let stripe_data_size = if stripe.data_size > 0 {
            stripe.data_size as usize
        } else if object.stripes.len() == 1 {
            object.size as usize
        } else {
            // For multi-stripe without data_size, we can't properly decode
            error!(
                "Multi-stripe object missing data_size on stripe {}",
                stripe_idx
            );
            return S3Error::xml_response(
                "InternalError",
                "Object metadata is incomplete (missing stripe data_size)",
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        };

        // Replication mode: just read raw data from any replica
        if stripe_ec_type == ErasureType::ErasureReplication {
            debug!(
                "Reading replicated stripe {} of {}/{}: size={}",
                stripe_idx, bucket, key, stripe_data_size
            );

            // Try each replica until we get the data
            let mut data_read = false;
            for shard_loc in &stripe.shards {
                let node_addr = resolve_node_address(
                    &mut node_address_map,
                    &mut meta_client,
                    &shard_loc.node_id,
                )
                .await;
                let node_placement = objectio_proto::metadata::NodePlacement {
                    position: shard_loc.position,
                    node_id: shard_loc.node_id.clone(),
                    node_address: node_addr,
                    disk_id: shard_loc.disk_id.clone(),
                    shard_type: shard_loc.shard_type,
                    local_group: shard_loc.local_group,
                };

                // Use stripe's object_id if available (for multipart uploads)
                // Fall back to object.object_id for backwards compat
                let shard_object_id = if !stripe.object_id.is_empty() {
                    &stripe.object_id
                } else {
                    &object.object_id
                };

                match read_shard_from_osd(
                    &state.osd_pool,
                    &node_placement,
                    shard_object_id,
                    stripe.stripe_id,
                    shard_loc.position,
                )
                .await
                {
                    Ok(data) => {
                        debug!(
                            "Read replicated data from replica {} ({} bytes)",
                            shard_loc.position,
                            data.len()
                        );
                        // Truncate to actual data size (in case of padding)
                        let actual_data = if data.len() > stripe_data_size {
                            data[..stripe_data_size].to_vec()
                        } else {
                            data
                        };
                        let (mut slice, slice_start_in_stripe): (Vec<u8>, u64) =
                            if let Some(ref range) = resolved_range {
                                let stripe_end = stripe_byte_offset + stripe_data_size as u64;
                                let slice_start =
                                    range.start.saturating_sub(stripe_byte_offset) as usize;
                                let slice_end = std::cmp::min(range.end + 1, stripe_end)
                                    .saturating_sub(stripe_byte_offset)
                                    as usize;
                                (
                                    actual_data[slice_start..slice_end].to_vec(),
                                    slice_start as u64,
                                )
                            } else {
                                (actual_data, 0)
                            };
                        if let Some(dek) = get_sse_dek.as_ref()
                            && let Err(resp) = decrypt_stripe_slice(
                                dek,
                                stripe,
                                &object,
                                stripe_byte_offset,
                                slice_start_in_stripe,
                                &mut slice,
                            )
                        {
                            return resp;
                        }
                        all_data.extend(slice);
                        data_read = true;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to read replica {} from stripe {}: {}",
                            shard_loc.position, stripe_idx, e
                        );
                    }
                }
            }

            if !data_read {
                error!(
                    "Failed to read any replica for stripe {} of {}/{}",
                    stripe_idx, bucket, key
                );
                return S3Error::xml_response(
                    "InternalError",
                    "Failed to read object: no replicas available",
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }

            continue; // Move to next stripe
        }

        // EC mode: need to read k shards and decode
        let total_shards = ec_k + ec_m;

        debug!(
            "Reading EC stripe {} of {}/{}: size={}, ec={}+{}",
            stripe_idx, bucket, key, stripe_data_size, ec_k, ec_m
        );

        // Read shards from OSDs - we need at least k shards
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; total_shards];
        let mut read_count = 0;

        // Create a map of position -> shard location for quick lookup
        let shard_map: HashMap<u32, &ShardLocation> =
            stripe.shards.iter().map(|s| (s.position, s)).collect();

        // Use stripe's object_id if available (for multipart uploads)
        // Fall back to object.object_id for backwards compat
        let ec_shard_object_id = if !stripe.object_id.is_empty() {
            &stripe.object_id
        } else {
            &object.object_id
        };

        // Rank all shard positions by topological distance to this
        // gateway so reads pull from the nearest OSDs first. Any k of the
        // total_shards positions decode correctly, so we no longer need a
        // separate data-first / parity-fallback split — a single ranked
        // pass handles both. Secondary sort key is position, which
        // preserves the legacy "data shards before parity" preference
        // when topology info is absent or ties.
        let me = &state.self_topology;
        let mut ranked_positions: Vec<(u32, objectio_placement::TopologyDistance)> = (0..total_shards
            as u32)
            .filter_map(|pos| {
                let shard_loc = shard_map.get(&pos)?;
                let dist = node_topo_map.get(&shard_loc.node_id).map_or(
                    objectio_placement::TopologyDistance::Unknown,
                    |fd| objectio_placement::distance(me, fd),
                );
                Some((pos, dist))
            })
            .collect();
        ranked_positions.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        for (pos, dist) in ranked_positions {
            if read_count >= ec_k {
                break;
            }
            let Some(shard_loc) = shard_map.get(&pos) else {
                continue;
            };
            let node_addr = resolve_node_address(
                &mut node_address_map,
                &mut meta_client,
                &shard_loc.node_id,
            )
            .await;
            let node_placement = objectio_proto::metadata::NodePlacement {
                position: shard_loc.position,
                node_id: shard_loc.node_id.clone(),
                node_address: node_addr,
                disk_id: shard_loc.disk_id.clone(),
                shard_type: shard_loc.shard_type,
                local_group: shard_loc.local_group,
            };

            match read_shard_from_osd(
                &state.osd_pool,
                &node_placement,
                ec_shard_object_id,
                stripe.stripe_id,
                pos,
            )
            .await
            {
                Ok(data) => {
                    let bytes = data.len();
                    debug!("Read shard {} ({} bytes, {})", pos, bytes, dist.as_str());
                    // Record per-locality read traffic so operators can see
                    // how much cross-zone/cross-dc bandwidth a typical
                    // object read consumes (Phase 2.4).
                    objectio_s3::observe_locality_read_bytes(dist.as_str(), bytes as u64);
                    shards[pos as usize] = Some(data);
                    read_count += 1;
                }
                Err(e) => {
                    warn!("Failed to read shard {}: {}", pos, e);
                }
            }
        }

        // Check if we have enough shards
        if read_count < ec_k {
            error!(
                "Insufficient shards to reconstruct stripe {}: have {}, need {}",
                stripe_idx, read_count, ec_k
            );
            return S3Error::xml_response(
                "InternalError",
                &format!(
                    "Cannot read object: only {} shards available for stripe {}, need {}",
                    read_count, stripe_idx, ec_k
                ),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }

        // Decode using erasure coding
        let codec = match ErasureCodec::new(ErasureConfig::new(ec_k as u8, ec_m as u8)) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create erasure codec: {}", e);
                return S3Error::xml_response(
                    "InternalError",
                    &format!("Erasure coding error: {}", e),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

        let stripe_data = match codec.decode(&mut shards, stripe_data_size) {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to decode stripe {}: {}", stripe_idx, e);
                return S3Error::xml_response(
                    "InternalError",
                    &format!("Erasure decoding failed for stripe {}: {}", stripe_idx, e),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

        let (mut slice, slice_start_in_stripe): (Vec<u8>, u64) =
            if let Some(ref range) = resolved_range {
                let stripe_end = stripe_byte_offset + stripe_data_size as u64;
                let slice_start = range.start.saturating_sub(stripe_byte_offset) as usize;
                let slice_end = std::cmp::min(range.end + 1, stripe_end)
                    .saturating_sub(stripe_byte_offset) as usize;
                (stripe_data[slice_start..slice_end].to_vec(), slice_start as u64)
            } else {
                (stripe_data, 0)
            };
        if let Some(dek) = get_sse_dek.as_ref()
            && let Err(resp) = decrypt_stripe_slice(
                dek,
                stripe,
                &object,
                stripe_byte_offset,
                slice_start_in_stripe,
                &mut slice,
            )
        {
            return resp;
        }
        all_data.extend(slice);
    }

    info!(
        "Read object: {}/{}, size={}, stripes_fetched={}/{}{}",
        bucket,
        key,
        all_data.len(),
        stripe_plan.len(),
        object.stripes.len(),
        if let Some(ref r) = resolved_range {
            format!(", range=bytes {}-{}", r.start, r.end)
        } else {
            String::new()
        }
    );

    // Verify data integrity for full (non-range) reads
    if resolved_range.is_none() && all_data.len() as u64 != total_size {
        error!(
            "Data size mismatch for {}/{}: reassembled {} bytes but object.size={}",
            bucket,
            key,
            all_data.len(),
            total_size
        );
    }

    // Build response — range requests already have sliced data.
    // Decryption (when the object is SSE-encrypted) has already been
    // applied per-stripe inside the fetch loop above.
    if let Some(ref range) = resolved_range {
        let content_range = format!("bytes {}-{}/{total_size}", range.start, range.end);

        let mut builder = Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_TYPE, &object.content_type)
            .header(header::CONTENT_LENGTH, all_data.len().to_string())
            .header(header::CONTENT_RANGE, content_range)
            .header("ETag", &object.etag)
            .header("Accept-Ranges", "bytes")
            .header(
                header::LAST_MODIFIED,
                timestamp_to_http_date(object.modified_at),
            );
        if let Some(v) = sse_response_header {
            builder = builder.header("x-amz-server-side-encryption", v);
            if v == "aws:kms" && !object.kms_key_id.is_empty() {
                builder = builder.header(
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    &object.kms_key_id,
                );
            }
        }
        if !sse_c_key_md5.is_empty() {
            builder = builder
                .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
                .header(
                    "x-amz-server-side-encryption-customer-key-md5",
                    &sse_c_key_md5,
                );
        }

        let builder = add_metadata_headers(builder, &object.user_metadata);

        builder.body(Body::from(all_data)).unwrap()
    } else {
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, &object.content_type)
            .header(header::CONTENT_LENGTH, all_data.len().to_string())
            .header("ETag", &object.etag)
            .header("Accept-Ranges", "bytes")
            .header(
                header::LAST_MODIFIED,
                timestamp_to_http_date(object.modified_at),
            );
        if let Some(v) = sse_response_header {
            builder = builder.header("x-amz-server-side-encryption", v);
            if v == "aws:kms" && !object.kms_key_id.is_empty() {
                builder = builder.header(
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    &object.kms_key_id,
                );
            }
        }
        if !sse_c_key_md5.is_empty() {
            builder = builder
                .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
                .header(
                    "x-amz-server-side-encryption-customer-key-md5",
                    &sse_c_key_md5,
                );
        }

        let builder = add_metadata_headers(builder, &object.user_metadata);

        builder.body(Body::from(all_data)).unwrap()
    }
}

/// Decrypt `buf` — one stripe's contribution to the GET response.
///
/// Picks the right IV + counter offset so a single helper works for both
/// the legacy single-stripe whole-body encryption scheme and the new
/// per-stripe IV scheme used by multipart SSE.
#[allow(clippy::result_large_err)]
fn decrypt_stripe_slice(
    dek: &[u8; objectio_kms::DEK_LEN],
    stripe: &StripeMeta,
    object: &ObjectMeta,
    stripe_byte_offset_in_object: u64,
    slice_start_in_stripe: u64,
    buf: &mut [u8],
) -> Result<(), Response> {
    // Per-stripe IV (multipart & new single-part): decrypt from offset within
    // the stripe. Otherwise fall back to the object-level IV (legacy whole-body
    // CTR), using the absolute byte offset within the object.
    let (iv_bytes, effective_offset) = if !stripe.encryption_iv.is_empty() {
        (&stripe.encryption_iv[..], slice_start_in_stripe)
    } else {
        (
            &object.encryption_iv[..],
            stripe_byte_offset_in_object + slice_start_in_stripe,
        )
    };
    if iv_bytes.len() != objectio_kms::IV_LEN {
        error!(
            "Bad IV length on {}/{}: got {}, want {}",
            object.bucket,
            object.key,
            iv_bytes.len(),
            objectio_kms::IV_LEN
        );
        return Err(S3Error::xml_response(
            "InternalError",
            "Object has malformed encryption metadata",
            StatusCode::INTERNAL_SERVER_ERROR,
        ));
    }
    let mut iv = [0u8; objectio_kms::IV_LEN];
    iv.copy_from_slice(iv_bytes);
    objectio_kms::decrypt_in_place(dek, &iv, effective_offset, buf);
    Ok(())
}

/// Resolve the gRPC address for a node.
///
/// First checks the in-memory map (populated from placement response).
/// On cache miss, fetches all active nodes via `GetListingNodes` and
/// populates the map so subsequent lookups are free.
async fn resolve_node_address(
    node_map: &mut HashMap<Vec<u8>, String>,
    meta_client: &mut MetadataServiceClient<Channel>,
    node_id: &[u8],
) -> String {
    // Fast path: already in the map (populated from placement or a prior listing call)
    if let Some(addr) = node_map.get(node_id) {
        return addr.clone();
    }

    // Slow path: node not in placement (topology may have changed).
    // Fetch all active nodes and populate the map.
    if let Ok(resp) = meta_client
        .get_listing_nodes(GetListingNodesRequest {
            bucket: String::new(),
        })
        .await
    {
        for n in &resp.into_inner().nodes {
            node_map
                .entry(n.node_id.clone())
                .or_insert_with(|| n.address.clone());
        }
    }

    node_map.get(node_id).cloned().unwrap_or_else(|| {
        warn!(
            "Could not resolve address for node {:?}, no listing entry found",
            Uuid::from_slice(node_id).map_or_else(|_| format!("{node_id:?}"), |u| u.to_string()),
        );
        String::from("http://localhost:9002")
    })
}

/// Head object (HEAD /{bucket}/{key})
pub async fn head_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    auth: Option<Extension<AuthResult>>,
) -> Response {
    // If key is empty (trailing slash on bucket), treat as head_bucket
    if key.is_empty() {
        return head_bucket(State(state), Path(bucket)).await;
    }

    // Check bucket policy if user is authenticated
    if let Some(Extension(auth_result)) = &auth {
        let resource = build_s3_arn(&bucket, Some(&key));
        if let Some(deny_response) = check_bucket_policy(
            &state,
            &bucket,
            &auth_result.user_arn,
            "s3:GetObject",
            &resource,
            None,
        )
        .await
        {
            return deny_response;
        }
    }

    let mut meta_client = state.meta_client.clone();

    // Get placement to find primary OSD
    let placement = match meta_client
        .get_placement(GetPlacementRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            size: 0,
            storage_class: "STANDARD".to_string(),
        })
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(_) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap();
        }
    };

    if placement.nodes.is_empty() {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::empty())
            .unwrap();
    }

    // Get object metadata from primary OSD
    let primary_osd = &placement.nodes[0];
    match get_object_meta_from_osd(&state.osd_pool, primary_osd, &bucket, &key).await {
        Ok(Some(obj)) => {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, &obj.content_type)
                .header(header::CONTENT_LENGTH, obj.size.to_string())
                .header("ETag", &obj.etag)
                .header(
                    header::LAST_MODIFIED,
                    timestamp_to_http_date(obj.modified_at),
                );

            // Surface server-side encryption to HEAD responses so clients can
            // see how an object was stored without downloading it.
            let sse_algo = SseAlgorithm::try_from(obj.encryption_algorithm)
                .unwrap_or(SseAlgorithm::SseNone);
            match sse_algo {
                SseAlgorithm::SseS3 => {
                    builder = builder.header("x-amz-server-side-encryption", "AES256");
                }
                SseAlgorithm::SseKms => {
                    builder = builder.header("x-amz-server-side-encryption", "aws:kms");
                    if !obj.kms_key_id.is_empty() {
                        builder = builder.header(
                            "x-amz-server-side-encryption-aws-kms-key-id",
                            &obj.kms_key_id,
                        );
                    }
                }
                SseAlgorithm::SseC => {
                    // Advertise the customer-algorithm marker so clients know
                    // this object needs a customer-key on GET. We don't know
                    // the key's md5 server-side; clients already do.
                    builder = builder
                        .header("x-amz-server-side-encryption-customer-algorithm", "AES256");
                }
                SseAlgorithm::SseNone => {}
            }

            // Add user metadata headers
            let builder = add_metadata_headers(builder, &obj.user_metadata);

            builder.body(Body::empty()).unwrap()
        }
        Ok(None) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}

/// Delete object (DELETE /{bucket}/{key})
pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    auth: Option<Extension<AuthResult>>,
    version_id: Option<String>,
    headers: HeaderMap,
) -> Response {
    // Check bucket policy if user is authenticated
    if let Some(Extension(auth_result)) = &auth {
        let resource = build_s3_arn(&bucket, Some(&key));
        if let Some(deny_response) = check_bucket_policy(
            &state,
            &bucket,
            &auth_result.user_arn,
            "s3:DeleteObject",
            &resource,
            Some(&headers),
        )
        .await
        {
            return deny_response;
        }
    }

    let mut meta_client = state.meta_client.clone();

    // Get placement to find primary OSD
    let placement = match meta_client
        .get_placement(GetPlacementRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            size: 0,
            storage_class: "STANDARD".to_string(),
        })
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(_) => {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap();
        }
    };

    if placement.nodes.is_empty() {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap();
    }

    let primary_osd = &placement.nodes[0];

    // Lock enforcement: check retention and legal hold before deleting
    if let Ok(Some(meta)) =
        get_object_meta_from_osd(&state.osd_pool, primary_osd, &bucket, &key).await
    {
        // Check legal hold
        if meta.legal_hold.as_ref().is_some_and(|lh| lh.status) {
            return S3Error::xml_response(
                "AccessDenied",
                "Object is under legal hold and cannot be deleted",
                StatusCode::FORBIDDEN,
            );
        }

        // Check retention
        if let Some(retention) = &meta.retention {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if retention.retain_until_date > now {
                let bypass = headers
                    .get("x-amz-bypass-governance-retention")
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v.eq_ignore_ascii_case("true"));

                if retention.mode() == RetentionMode::RetentionCompliance {
                    return S3Error::xml_response(
                        "AccessDenied",
                        "Object is under compliance retention and cannot be deleted",
                        StatusCode::FORBIDDEN,
                    );
                }
                if retention.mode() == RetentionMode::RetentionGovernance && !bypass {
                    return S3Error::xml_response(
                        "AccessDenied",
                        "Object is under governance retention. Use x-amz-bypass-governance-retention header to override",
                        StatusCode::FORBIDDEN,
                    );
                }
            }
        }
    }

    // Check versioning state
    let versioning_enabled = match meta_client
        .get_bucket_versioning(GetBucketVersioningRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => resp.into_inner().state() == VersioningState::VersioningEnabled,
        Err(_) => false,
    };

    if versioning_enabled && version_id.is_none() {
        // Versioned delete without version_id: create a delete marker
        let marker_version_id = Uuid::new_v4().to_string();
        let delete_marker = ObjectMeta {
            bucket: bucket.clone(),
            key: key.clone(),
            object_id: Uuid::new_v4().as_bytes().to_vec(),
            size: 0,
            etag: String::new(),
            content_type: String::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            modified_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            storage_class: String::new(),
            user_metadata: HashMap::new(),
            version_id: marker_version_id.clone(),
            is_delete_marker: true,
            stripes: Vec::new(),
            retention: None,
            legal_hold: None,
            ..Default::default()
        };

        if let Err(e) = put_object_meta_to_osd_versioned(
            &state.osd_pool,
            primary_osd,
            &bucket,
            &key,
            delete_marker,
            true,
        )
        .await
        {
            error!("Failed to create delete marker: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }

        info!(
            "Created delete marker: {}/{} (version={})",
            bucket, key, marker_version_id
        );
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("x-amz-version-id", &marker_version_id)
            .header("x-amz-delete-marker", "true")
            .body(Body::empty())
            .unwrap();
    }

    // Non-versioned delete, or versioned delete with specific version_id
    let vid = version_id.as_deref().unwrap_or("");
    if let Err(e) =
        delete_object_meta_from_osd_versioned(&state.osd_pool, primary_osd, &bucket, &key, vid)
            .await
    {
        warn!("Failed to delete object metadata from OSD: {}", e);
    }

    info!(
        "Deleted object: {}/{}{}",
        bucket,
        key,
        if vid.is_empty() {
            String::new()
        } else {
            format!(" (version={})", vid)
        }
    );

    let mut resp = Response::builder().status(StatusCode::NO_CONTENT);
    if let Some(ref vid) = version_id {
        resp = resp.header("x-amz-version-id", vid.as_str());
    }
    resp.body(Body::empty()).unwrap()
}

/// Delete multiple objects (POST /{bucket}?delete)
pub async fn delete_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    auth: Option<Extension<AuthResult>>,
    body: Bytes,
) -> Response {
    debug!("DELETE objects: {} (batch)", bucket);

    // Parse XML request body
    let delete_request: DeleteObjectsRequest = match quick_xml::de::from_reader(body.as_ref()) {
        Ok(req) => req,
        Err(e) => {
            error!("Failed to parse DeleteObjects request: {}", e);
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Invalid XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    if delete_request.objects.is_empty() {
        return S3Error::xml_response(
            "MalformedXML",
            "No objects specified for deletion",
            StatusCode::BAD_REQUEST,
        );
    }

    // Limit number of objects per request (S3 limit is 1000)
    if delete_request.objects.len() > 1000 {
        return S3Error::xml_response(
            "MalformedXML",
            "Too many objects specified (max 1000)",
            StatusCode::BAD_REQUEST,
        );
    }

    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    // Delete each object
    for obj in delete_request.objects {
        // Check bucket policy if user is authenticated
        if let Some(Extension(auth_result)) = &auth {
            let resource = build_s3_arn(&bucket, Some(&obj.key));
            if let Some(_deny_response) = check_bucket_policy(
                &state,
                &bucket,
                &auth_result.user_arn,
                "s3:DeleteObject",
                &resource,
                None,
            )
            .await
            {
                errors.push(DeleteError {
                    key: obj.key,
                    code: "AccessDenied".to_string(),
                    message: "Access Denied".to_string(),
                });
                continue;
            }
        }

        // Get placement to find primary OSD
        let mut meta_client = state.meta_client.clone();
        let placement = match meta_client
            .get_placement(GetPlacementRequest {
                bucket: bucket.clone(),
                key: obj.key.clone(),
                size: 0,
                storage_class: "STANDARD".to_string(),
            })
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                // Object doesn't exist - S3 still reports it as deleted
                debug!(
                    "Object {}/{} not found during delete: {}",
                    bucket, obj.key, e
                );
                deleted.push(DeletedObject {
                    key: obj.key,
                    version_id: obj.version_id,
                });
                continue;
            }
        };

        if placement.nodes.is_empty() {
            // No nodes available - still report as deleted (S3 behavior)
            deleted.push(DeletedObject {
                key: obj.key,
                version_id: obj.version_id,
            });
            continue;
        }

        // Delete object metadata from primary OSD
        let primary_osd = &placement.nodes[0];
        if let Err(e) =
            delete_object_meta_from_osd(&state.osd_pool, primary_osd, &bucket, &obj.key).await
        {
            warn!(
                "Failed to delete object {}/{} from OSD: {}",
                bucket, obj.key, e
            );
            // Continue anyway - might not exist, which is OK
        }

        deleted.push(DeletedObject {
            key: obj.key,
            version_id: obj.version_id,
        });
    }

    info!(
        "Batch delete: bucket={}, deleted={}, errors={}",
        bucket,
        deleted.len(),
        errors.len()
    );

    // Build response
    let result = DeleteObjectsResult { deleted, errors };

    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
        to_xml(&result).unwrap_or_default()
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Health check endpoint (GET /health)
pub async fn health_check() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"status":"healthy"}"#))
        .unwrap()
}

// ============================================================================
// Bucket Policy Operations (internal implementations)
// ============================================================================

/// Get bucket policy (GET /{bucket}?policy) - internal implementation
async fn get_bucket_policy_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();

    match client
        .get_bucket_policy(GetBucketPolicyRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(response) => {
            let policy_resp = response.into_inner();
            if policy_resp.has_policy {
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(policy_resp.policy_json))
                    .unwrap()
            } else {
                S3Error::xml_response(
                    "NoSuchBucketPolicy",
                    "The bucket policy does not exist",
                    StatusCode::NOT_FOUND,
                )
            }
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchBucket",
                    "The specified bucket does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else {
                error!("Failed to get bucket policy: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// Set bucket policy (PUT /{bucket}?policy) - internal implementation
async fn put_bucket_policy_internal(state: Arc<AppState>, bucket: String, body: Bytes) -> Response {
    let mut client = state.meta_client.clone();

    // Parse the policy JSON to validate it
    let policy_json = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return S3Error::xml_response(
                "MalformedPolicy",
                "The policy is not valid UTF-8",
                StatusCode::BAD_REQUEST,
            );
        }
    };

    // Validate JSON format
    if serde_json::from_str::<serde_json::Value>(&policy_json).is_err() {
        return S3Error::xml_response(
            "MalformedPolicy",
            "The policy is not valid JSON",
            StatusCode::BAD_REQUEST,
        );
    }

    match client
        .set_bucket_policy(SetBucketPolicyRequest {
            bucket: bucket.clone(),
            policy_json,
        })
        .await
    {
        Ok(_) => {
            info!("Set bucket policy for: {}", bucket);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchBucket",
                    "The specified bucket does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else if e.code() == tonic::Code::InvalidArgument {
                S3Error::xml_response("MalformedPolicy", e.message(), StatusCode::BAD_REQUEST)
            } else {
                error!("Failed to set bucket policy: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// Delete bucket policy (DELETE /{bucket}?policy) - internal implementation
async fn delete_bucket_policy_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();

    match client
        .delete_bucket_policy(DeleteBucketPolicyRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(_) => {
            info!("Deleted bucket policy for: {}", bucket);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchBucket",
                    "The specified bucket does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else {
                error!("Failed to delete bucket policy: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

// ============================================================================
// Multipart Upload Operations
// ============================================================================

/// POST /{bucket}/{key}?uploads - Initiate multipart upload
/// POST /{bucket}/{key}?uploadId=X - Complete multipart upload
pub async fn post_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<PostObjectParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if params.uploads.is_some() {
        // Initiate multipart upload
        initiate_multipart_upload_internal(state, bucket, key, &headers).await
    } else if let Some(upload_id) = params.upload_id {
        // Complete multipart upload
        complete_multipart_upload_internal(state, bucket, key, upload_id, body).await
    } else {
        S3Error::xml_response(
            "InvalidRequest",
            "POST request must include ?uploads or ?uploadId parameter",
            StatusCode::BAD_REQUEST,
        )
    }
}

/// Initiate multipart upload - internal implementation
async fn initiate_multipart_upload_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    headers: &HeaderMap,
) -> Response {
    let mut client = state.meta_client.clone();

    // SSE-C multipart: validate the customer key at CreateMultipartUpload
    // and stash its MD5 on meta. UploadPart requests must resupply the same
    // key; we never store the raw bytes. Warehouse-bucket guard matches the
    // single-part path.
    if headers
        .get("x-amz-server-side-encryption-customer-algorithm")
        .is_some()
    {
        let cust = match parse_sse_c_headers(headers) {
            Ok(Some(c)) => c,
            Ok(None) => {
                return S3Error::xml_response(
                    "InvalidRequest",
                    "partial SSE-C headers",
                    StatusCode::BAD_REQUEST,
                );
            }
            Err(resp) => return resp,
        };
        if is_warehouse_bucket(&bucket) {
            return S3Error::xml_response(
                "InvalidEncryptionAlgorithmError",
                "SSE-C is not supported on warehouse buckets — query engines cannot provide the customer key on every read",
                StatusCode::BAD_REQUEST,
            );
        }
        // The key itself never leaves the gateway address space — we only
        // persist its MD5 so UploadPart can validate callers. The raw key
        // material in `cust.key` gets dropped when `cust` goes out of scope.
        let md5 = cust.md5_b64;

        match client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                content_type: String::new(),
                user_metadata: HashMap::new(),
                encryption_algorithm: SseAlgorithm::SseC as i32,
                kms_key_id: String::new(),
                encrypted_dek: Vec::new(),
                customer_key_md5: md5.clone(),
                // SSE-C never uses a KMS encryption context — the customer key
                // stands in for KMS entirely.
                encryption_context: HashMap::new(),
            })
            .await
        {
            Ok(response) => {
                let resp = response.into_inner();
                let result = InitiateMultipartUploadResult {
                    bucket: resp.bucket,
                    key: resp.key,
                    upload_id: resp.upload_id,
                };
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                    to_xml(&result).unwrap_or_default()
                );
                info!("Initiated multipart upload (SSE-C): {}/{}", bucket, key);
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/xml")
                    .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
                    .header("x-amz-server-side-encryption-customer-key-md5", &md5)
                    .body(Body::from(xml))
                    .unwrap();
            }
            Err(e) => {
                if e.code() == tonic::Code::NotFound {
                    return S3Error::xml_response(
                        "NoSuchBucket",
                        "The specified bucket does not exist",
                        StatusCode::NOT_FOUND,
                    );
                }
                error!("Failed to initiate SSE-C multipart upload: {}", e);
                return S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        }
    }

    // Resolve SSE up front. AWS fixes the MPU's SSE config at creation time;
    // per-UploadPart SSE headers are ignored. We generate + wrap the DEK
    // once here and stash it on meta alongside the MPU state.
    let sse_decision = match resolve_sse_decision(&mut client, &bucket, Some(headers)).await {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    let (algo, kms_key_id, wrapped_dek, sse_response_header, encryption_context) =
        match sse_decision {
            None => (
                SseAlgorithm::SseNone as i32,
                String::new(),
                Vec::new(),
                None,
                HashMap::new(),
            ),
            Some(d) if d.algorithm == SseAlgorithm::SseS3 => {
                let Some(mk) = state.master_key.as_ref() else {
                    error!(
                        "Multipart upload {bucket}/{key} requires SSE-S3 but gateway has no master key"
                    );
                    return S3Error::xml_response(
                        "ServiceUnavailable",
                        "SSE master key not configured on the gateway",
                        StatusCode::SERVICE_UNAVAILABLE,
                    );
                };
                let dek = objectio_kms::generate_dek();
                (
                    SseAlgorithm::SseS3 as i32,
                    String::new(),
                    mk.wrap_dek(&dek),
                    Some("AES256"),
                    HashMap::new(),
                )
            }
            Some(d) if d.algorithm == SseAlgorithm::SseKms => {
                let Some(kms) = state.kms() else {
                    return S3Error::xml_response(
                        "ServiceUnavailable",
                        "SSE-KMS is not configured on this gateway",
                        StatusCode::SERVICE_UNAVAILABLE,
                    );
                };
                let data_key = match kms
                    .generate_data_key(&d.kms_key_id, &d.encryption_context)
                    .await
                {
                    Ok(g) => g,
                    Err(objectio_kms::KmsError::KeyNotFound(id)) => {
                        return S3Error::xml_response(
                            "KMS.NotFoundException",
                            &format!("KMS key '{id}' does not exist"),
                            StatusCode::BAD_REQUEST,
                        );
                    }
                    Err(e) => {
                        error!("KMS generate_data_key for MPU failed: {e}");
                        return S3Error::xml_response(
                            "InternalError",
                            &e.to_string(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                };
                (
                    SseAlgorithm::SseKms as i32,
                    d.kms_key_id,
                    data_key.wrapped_dek,
                    Some("aws:kms"),
                    d.encryption_context,
                )
            }
            Some(d) => {
                error!(
                    "resolve_sse_decision returned unexpected algorithm for MPU: {:?}",
                    d.algorithm
                );
                return S3Error::xml_response(
                    "InternalError",
                    "unexpected SSE resolution",
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

    let response_kms_key_id = kms_key_id.clone();
    match client
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            content_type: String::new(),
            user_metadata: HashMap::new(),
            encryption_algorithm: algo,
            kms_key_id,
            encrypted_dek: wrapped_dek,
            customer_key_md5: String::new(),
            encryption_context,
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();
            let result = InitiateMultipartUploadResult {
                bucket: resp.bucket,
                key: resp.key,
                upload_id: resp.upload_id,
            };

            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );

            info!("Initiated multipart upload: {}/{}", bucket, key);

            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml");
            if let Some(v) = sse_response_header {
                builder = builder.header("x-amz-server-side-encryption", v);
                if v == "aws:kms" && !response_kms_key_id.is_empty() {
                    builder = builder.header(
                        "x-amz-server-side-encryption-aws-kms-key-id",
                        &response_kms_key_id,
                    );
                }
            }
            builder.body(Body::from(xml)).unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchBucket",
                    "The specified bucket does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else {
                error!("Failed to initiate multipart upload: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// PUT /{bucket}/{key}?uploadId=X&partNumber=N - Upload part
pub async fn put_object_with_params(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<PutObjectParams>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // If uploadId and partNumber are present, this is a multipart part upload
    if let (Some(upload_id), Some(part_number)) = (params.upload_id, params.part_number) {
        return upload_part_internal(
            state,
            bucket,
            key,
            upload_id,
            part_number,
            headers,
            body,
        )
        .await;
    }
    if params.retention.is_some() {
        return put_object_retention_internal(state, bucket, key, body).await;
    }
    if params.legal_hold.is_some() {
        return put_object_legal_hold_internal(state, bucket, key, body).await;
    }

    // Otherwise, it's a regular PUT object
    put_object(State(state), Path((bucket, key)), auth, headers, body).await
}

/// Upload part - internal implementation
async fn upload_part_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    upload_id: String,
    part_number: u32,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    debug!(
        "Upload part: bucket={}, key={}, uploadId={}, partNumber={}, size={}",
        bucket,
        key,
        upload_id,
        part_number,
        body.len()
    );

    // Validate part number
    if part_number == 0 || part_number > 10000 {
        return S3Error::xml_response(
            "InvalidArgument",
            "Part number must be between 1 and 10000",
            StatusCode::BAD_REQUEST,
        );
    }

    // Calculate ETag for this part — AWS semantics for SSE-S3/SSE-KMS:
    // part ETag is the MD5 of the *plaintext*. Compute before we possibly
    // encrypt below.
    let etag = format!("\"{:x}\"", md5::compute(&body));
    let part_size = body.len() as u64;

    let mut meta_client = state.meta_client.clone();

    // Fetch the MPU's SSE state. The decision was made at CreateMultipartUpload
    // time — per-UploadPart SSE headers are ignored for SSE-S3/SSE-KMS. For
    // SSE-C the client must resupply their customer key on every part and we
    // validate against the stored MD5. If encryption is on, we unwrap the DEK
    // once and generate a fresh IV per stripe below.
    let (mpu_dek, sse_response_header, sse_kms_key_id, sse_c_key_md5_for_resp) = match meta_client
        .get_multipart_upload(GetMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
        })
        .await
    {
        Ok(resp) => {
            let mpu = resp.into_inner();
            if !mpu.found {
                return S3Error::xml_response(
                    "NoSuchUpload",
                    "The specified multipart upload does not exist",
                    StatusCode::NOT_FOUND,
                );
            }
            let algo = SseAlgorithm::try_from(mpu.encryption_algorithm)
                .unwrap_or(SseAlgorithm::SseNone);
            match algo {
                SseAlgorithm::SseS3 => {
                    let Some(mk) = state.master_key.as_ref() else {
                        return S3Error::xml_response(
                            "ServiceUnavailable",
                            "SSE master key not configured on the gateway",
                            StatusCode::SERVICE_UNAVAILABLE,
                        );
                    };
                    match mk.unwrap_dek(&mpu.encrypted_dek) {
                        Ok(dek) => (Some(dek), Some("AES256"), String::new(), String::new()),
                        Err(e) => {
                            error!("Failed to unwrap DEK for MPU {upload_id}: {e}");
                            return S3Error::xml_response(
                                "InternalError",
                                "Failed to unwrap MPU DEK",
                                StatusCode::INTERNAL_SERVER_ERROR,
                            );
                        }
                    }
                }
                SseAlgorithm::SseKms => {
                    let Some(kms) = state.kms() else {
                        return S3Error::xml_response(
                            "ServiceUnavailable",
                            "SSE-KMS is not configured on this gateway",
                            StatusCode::SERVICE_UNAVAILABLE,
                        );
                    };
                    // The encryption context was supplied at CreateMultipartUpload
                    // and stored on the MPU state so every UploadPart uses the
                    // same AEAD binding as the wrapped DEK.
                    match kms
                        .decrypt(&mpu.kms_key_id, &mpu.encrypted_dek, &mpu.encryption_context)
                        .await
                    {
                        Ok(dek) => (Some(dek), Some("aws:kms"), mpu.kms_key_id, String::new()),
                        Err(e) => {
                            error!("Failed to unwrap KMS DEK for MPU {upload_id}: {e}");
                            return S3Error::xml_response(
                                "InternalError",
                                "Failed to unwrap MPU DEK via KMS",
                                StatusCode::INTERNAL_SERVER_ERROR,
                            );
                        }
                    }
                }
                SseAlgorithm::SseC => {
                    // UploadPart must resupply the customer key headers. Compare
                    // the provided MD5 against what was stored at CreateMPU; if
                    // they differ we reject without ever reading the key bytes.
                    let cust = match parse_sse_c_headers(&headers) {
                        Ok(Some(c)) => c,
                        Ok(None) => {
                            return S3Error::xml_response(
                                "InvalidRequest",
                                "UploadPart on an SSE-C multipart upload must include the customer-key headers",
                                StatusCode::BAD_REQUEST,
                            );
                        }
                        Err(resp) => return resp,
                    };
                    if cust.md5_b64 != mpu.customer_key_md5 {
                        return S3Error::xml_response(
                            "InvalidArgument",
                            "Customer key MD5 does not match the MD5 provided at CreateMultipartUpload",
                            StatusCode::BAD_REQUEST,
                        );
                    }
                    (Some(cust.key), None, String::new(), cust.md5_b64)
                }
                _ => (None, None, String::new(), String::new()),
            }
        }
        Err(e) => {
            error!("Failed to fetch MPU state for {upload_id}: {e}");
            return S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    // Get placement for this part (using a unique key for the part)
    let part_key = format!("__mpu/{}/part{:05}", upload_id, part_number);
    let placement = match meta_client
        .get_placement(GetPlacementRequest {
            bucket: bucket.clone(),
            key: part_key.clone(),
            size: part_size,
            storage_class: "STANDARD".to_string(),
        })
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            error!("Failed to get placement for part: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &format!("Failed to get placement: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let ec_k = placement.ec_k;
    let ec_m = placement.ec_m;
    let ec_type = ErasureType::try_from(placement.ec_type).unwrap_or(ErasureType::ErasureMds);
    let replication_count = placement.replication_count;

    // Generate a unique object ID for this part
    let part_object_id = *Uuid::new_v4().as_bytes();

    // Replication mode: no EC, just write raw data to each replica
    // For large parts, split into multiple stripes (each stripe <= MAX_SHARD_SIZE)
    let (all_stripes, total_success, used_ec_type) = if ec_type == ErasureType::ErasureReplication {
        let total_replicas = replication_count.max(1) as usize;

        // Split data into stripes (each stripe must fit in a block)
        let stripe_size = MAX_SHARD_SIZE;
        let num_stripes = body.len().div_ceil(stripe_size);

        debug!(
            "Replication mode for part {}: writing {} replicas x {} stripes (size={})",
            part_number,
            total_replicas,
            num_stripes,
            body.len()
        );

        let mut all_stripes = Vec::with_capacity(num_stripes);
        let mut total_success = 0;

        for stripe_idx in 0..num_stripes {
            let stripe_start = stripe_idx * stripe_size;
            let stripe_end = std::cmp::min(stripe_start + stripe_size, body.len());
            // Copy this stripe's bytes out; encrypt in place if the MPU is
            // SSE-S3. A fresh IV per stripe means on GET each stripe can
            // decrypt independently starting from its own offset zero.
            let mut stripe_bytes = body[stripe_start..stripe_end].to_vec();
            let stripe_iv: Vec<u8> = if let Some(dek) = mpu_dek.as_ref() {
                let iv = objectio_kms::generate_iv();
                objectio_kms::encrypt_in_place(dek, &iv, &mut stripe_bytes);
                iv.to_vec()
            } else {
                Vec::new()
            };
            let stripe_data_size = stripe_bytes.len() as u64;

            let mut write_futures = Vec::with_capacity(total_replicas);

            for i in 0..total_replicas {
                let placement_node = if i < placement.nodes.len() {
                    placement.nodes[i].clone()
                } else if !placement.nodes.is_empty() {
                    placement.nodes[i % placement.nodes.len()].clone()
                } else {
                    error!("No placement nodes available");
                    return S3Error::xml_response(
                        "InternalError",
                        "No storage nodes available",
                        StatusCode::SERVICE_UNAVAILABLE,
                    );
                };

                let pool = state.osd_pool.clone();
                let obj_id = part_object_id;
                let shard_data = stripe_bytes.clone();
                let pos = i as u32;
                let s_idx = stripe_idx as u64;

                write_futures.push(async move {
                    let result = write_shard_to_osd(
                        &pool,
                        &placement_node,
                        &obj_id,
                        s_idx, // stripe_id
                        pos,
                        shard_data,
                        1, // ec_k=1 for replication
                        0, // ec_m=0 for replication
                    )
                    .await;
                    (pos, result, placement_node)
                });
            }

            let results = futures::future::join_all(write_futures).await;

            let mut success = 0;
            let mut locs = Vec::with_capacity(total_replicas);

            for (pos, result, placement_node) in results {
                match result {
                    Ok(location) => {
                        success += 1;
                        locs.push(ShardLocation {
                            position: pos,
                            node_id: location.node_id,
                            disk_id: location.disk_id,
                            offset: location.offset,
                            shard_type: placement_node.shard_type,
                            local_group: placement_node.local_group,
                        });
                    }
                    Err(e) => {
                        warn!(
                            "Failed to write stripe {} replica {} for part: {}",
                            stripe_idx, pos, e
                        );
                    }
                }
            }

            if success < 1 {
                error!(
                    "Replication failed for part stripe {}: no successful writes",
                    stripe_idx
                );
                return S3Error::xml_response(
                    "InternalError",
                    &format!(
                        "Replication failed for stripe {}: no successful writes",
                        stripe_idx
                    ),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }

            total_success += success;
            locs.sort_by_key(|l| l.position);

            all_stripes.push(StripeMeta {
                stripe_id: stripe_idx as u64,
                ec_k: 1,
                ec_m: 0,
                shards: locs,
                ec_type: ErasureType::ErasureReplication.into(),
                ec_local_parity: 0,
                ec_global_parity: 0,
                local_group_size: 0,
                data_size: stripe_data_size,
                object_id: part_object_id.to_vec(), // Store object_id used for shards
                encryption_iv: stripe_iv.clone(),
            });
        }

        (all_stripes, total_success, ErasureType::ErasureReplication)
    } else {
        // EC mode: encode data with erasure coding
        // For large parts, split into multiple stripes (each stripe's shards must fit in a block)
        let total_shards_per_stripe = (ec_k + ec_m) as usize;

        // Calculate max raw data per stripe: each shard is data_size/k bytes
        // To keep each shard <= MAX_SHARD_SIZE, raw data must be <= MAX_SHARD_SIZE * k
        let max_stripe_data_size = MAX_SHARD_SIZE * ec_k as usize;
        let num_stripes = body.len().div_ceil(max_stripe_data_size);

        debug!(
            "EC mode for part {}: {} stripes, {} shards/stripe (ec_k={}, ec_m={}), part_size={}",
            part_number,
            num_stripes,
            total_shards_per_stripe,
            ec_k,
            ec_m,
            body.len()
        );

        let codec = match ErasureCodec::new(ErasureConfig::new(ec_k as u8, ec_m as u8)) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to create erasure codec: {}", e);
                return S3Error::xml_response(
                    "InternalError",
                    &format!("Erasure coding error: {}", e),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

        let mut all_stripes = Vec::with_capacity(num_stripes);
        let mut total_success = 0;

        for stripe_idx in 0..num_stripes {
            let stripe_start = stripe_idx * max_stripe_data_size;
            let stripe_end = std::cmp::min(stripe_start + max_stripe_data_size, body.len());
            // Copy this stripe's bytes out; encrypt in place if the MPU is
            // SSE-S3. A fresh IV per stripe means on GET each stripe can
            // decrypt independently starting from its own offset zero.
            let mut stripe_bytes = body[stripe_start..stripe_end].to_vec();
            let stripe_iv: Vec<u8> = if let Some(dek) = mpu_dek.as_ref() {
                let iv = objectio_kms::generate_iv();
                objectio_kms::encrypt_in_place(dek, &iv, &mut stripe_bytes);
                iv.to_vec()
            } else {
                Vec::new()
            };
            let stripe_data_size = stripe_bytes.len() as u64;

            let shards: Vec<Vec<u8>> = match codec.encode(&stripe_bytes) {
                Ok(s) => s.into_iter().map(|s| s.to_vec()).collect(),
                Err(e) => {
                    error!("Failed to encode stripe {} data: {}", stripe_idx, e);
                    return S3Error::xml_response(
                        "InternalError",
                        &format!("Erasure encoding failed for stripe {}: {}", stripe_idx, e),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
            };

            let mut write_futures = Vec::with_capacity(total_shards_per_stripe);
            for (i, shard) in shards.iter().enumerate() {
                let placement_node = if i < placement.nodes.len() {
                    placement.nodes[i].clone()
                } else if !placement.nodes.is_empty() {
                    placement.nodes[i % placement.nodes.len()].clone()
                } else {
                    error!("No placement nodes available");
                    return S3Error::xml_response(
                        "InternalError",
                        "No storage nodes available",
                        StatusCode::SERVICE_UNAVAILABLE,
                    );
                };

                let pool = state.osd_pool.clone();
                let obj_id = part_object_id;
                let shard_data = shard.clone();
                let pos = i as u32;
                let s_idx = stripe_idx as u64;

                write_futures.push(async move {
                    let result = write_shard_to_osd(
                        &pool,
                        &placement_node,
                        &obj_id,
                        s_idx, // stripe_id
                        pos,
                        shard_data,
                        ec_k,
                        ec_m,
                    )
                    .await;
                    (pos, result, placement_node)
                });
            }

            let results = futures::future::join_all(write_futures).await;

            let mut success = 0;
            let mut locs = Vec::with_capacity(total_shards_per_stripe);

            for (pos, result, placement_node) in results {
                match result {
                    Ok(location) => {
                        success += 1;
                        locs.push(ShardLocation {
                            position: pos,
                            node_id: location.node_id,
                            disk_id: location.disk_id,
                            offset: location.offset,
                            shard_type: placement_node.shard_type,
                            local_group: placement_node.local_group,
                        });
                    }
                    Err(e) => {
                        warn!(
                            "Failed to write shard {} for part stripe {}: {}",
                            pos, stripe_idx, e
                        );
                    }
                }
            }

            // Check write quorum - need at least k shards to reconstruct data
            let quorum = ec_k as usize;
            if success < quorum {
                error!(
                    "Write quorum not met for part stripe {}: {} successful, need {} (ec_k={}, ec_m={})",
                    stripe_idx, success, quorum, ec_k, ec_m
                );
                return S3Error::xml_response(
                    "InternalError",
                    &format!(
                        "Write quorum not met for stripe {}: {} successful writes, need {}",
                        stripe_idx, success, quorum
                    ),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }

            total_success += success;
            locs.sort_by_key(|l| l.position);

            all_stripes.push(StripeMeta {
                stripe_id: stripe_idx as u64,
                ec_k,
                ec_m,
                shards: locs,
                ec_type: placement.ec_type,
                ec_local_parity: placement.ec_local_parity,
                ec_global_parity: placement.ec_global_parity,
                local_group_size: placement.local_group_size,
                data_size: stripe_data_size,
                object_id: part_object_id.to_vec(), // Store object_id used for shards
                encryption_iv: stripe_iv.clone(),
            });
        }

        (all_stripes, total_success, ec_type)
    };

    debug!(
        "Part {} uploaded: {} stripes, {} shards written, mode={:?}",
        part_number,
        all_stripes.len(),
        total_success,
        used_ec_type
    );

    // Register the part with metadata service (using all stripes)
    match meta_client
        .register_part(RegisterPartRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            part_number,
            etag: etag.clone(),
            size: part_size,
            stripes: all_stripes, // Multiple stripes for large parts
        })
        .await
    {
        Ok(_) => {
            info!(
                "Uploaded part {}: bucket={}, key={}, uploadId={}, size={}",
                part_number, bucket, key, upload_id, part_size
            );

            let mut builder = Response::builder().status(StatusCode::OK).header("ETag", &etag);
            if let Some(v) = sse_response_header {
                builder = builder.header("x-amz-server-side-encryption", v);
                if v == "aws:kms" && !sse_kms_key_id.is_empty() {
                    builder = builder.header(
                        "x-amz-server-side-encryption-aws-kms-key-id",
                        &sse_kms_key_id,
                    );
                }
            }
            if !sse_c_key_md5_for_resp.is_empty() {
                builder = builder
                    .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
                    .header(
                        "x-amz-server-side-encryption-customer-key-md5",
                        &sse_c_key_md5_for_resp,
                    );
            }
            builder.body(Body::empty()).unwrap()
        }
        Err(e) => {
            error!("Failed to register part: {}", e);
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchUpload",
                    "The specified multipart upload does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else {
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// Complete multipart upload - internal implementation
async fn complete_multipart_upload_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    upload_id: String,
    body: Bytes,
) -> Response {
    // Parse the CompleteMultipartUpload XML request
    let xml_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return S3Error::xml_response(
                "MalformedXML",
                "The XML provided was not well-formed",
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let complete_req: CompleteMultipartUploadXml = match quick_xml::de::from_str(&xml_str) {
        Ok(req) => req,
        Err(e) => {
            error!("Failed to parse CompleteMultipartUpload XML: {}", e);
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Failed to parse XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let mut meta_client = state.meta_client.clone();

    // Convert to proto PartInfo
    let parts: Vec<PartInfo> = complete_req
        .parts
        .into_iter()
        .map(|p| PartInfo {
            part_number: p.part_number,
            etag: p.etag,
            size: 0, // Will be filled by meta service from stored state
        })
        .collect();

    // Complete the multipart upload via metadata service
    match meta_client
        .complete_multipart_upload(ProtoCompleteMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            parts,
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();
            if let Some(object) = resp.object {
                // Log multipart assembly details
                let stripe_sizes: Vec<u64> = object.stripes.iter().map(|s| s.data_size).collect();
                let stripe_total: u64 = stripe_sizes.iter().sum();
                info!(
                    "Completed multipart {}/{}: size={}, stripes={}, stripe_sizes={:?}, stripe_total={}",
                    bucket,
                    key,
                    object.size,
                    object.stripes.len(),
                    stripe_sizes,
                    stripe_total
                );
                if stripe_total != object.size {
                    error!(
                        "Multipart stripe data_size sum ({}) != object.size ({}) for {}/{}",
                        stripe_total, object.size, bucket, key
                    );
                }

                // Store the final object metadata on primary OSD
                let placement = match meta_client
                    .get_placement(GetPlacementRequest {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        size: object.size,
                        storage_class: "STANDARD".to_string(),
                    })
                    .await
                {
                    Ok(resp) => resp.into_inner(),
                    Err(e) => {
                        error!("Failed to get placement for completed object: {}", e);
                        return S3Error::xml_response(
                            "InternalError",
                            &e.to_string(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                };

                if !placement.nodes.is_empty() {
                    let primary_osd = &placement.nodes[0];
                    if let Err(e) = put_object_meta_to_osd(
                        &state.osd_pool,
                        primary_osd,
                        &bucket,
                        &key,
                        object.clone(),
                    )
                    .await
                    {
                        error!("Failed to store object metadata on OSD: {}", e);
                        return S3Error::xml_response(
                            "InternalError",
                            &format!("Failed to store object metadata: {}", e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                }

                let result = CompleteMultipartUploadResult {
                    location: format!("/{}/{}", bucket, key),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    etag: object.etag.clone(),
                };

                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                    to_xml(&result).unwrap_or_default()
                );

                info!(
                    "Completed multipart upload: bucket={}, key={}, uploadId={}, size={}",
                    bucket, key, upload_id, object.size
                );

                let mut builder = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/xml")
                    .header("ETag", &object.etag);
                let sse_algo = SseAlgorithm::try_from(object.encryption_algorithm)
                    .unwrap_or(SseAlgorithm::SseNone);
                match sse_algo {
                    SseAlgorithm::SseS3 => {
                        builder = builder.header("x-amz-server-side-encryption", "AES256");
                    }
                    SseAlgorithm::SseKms => {
                        builder = builder.header("x-amz-server-side-encryption", "aws:kms");
                        if !object.kms_key_id.is_empty() {
                            builder = builder.header(
                                "x-amz-server-side-encryption-aws-kms-key-id",
                                &object.kms_key_id,
                            );
                        }
                    }
                    SseAlgorithm::SseNone | SseAlgorithm::SseC => {}
                }
                builder.body(Body::from(xml)).unwrap()
            } else {
                S3Error::xml_response(
                    "InternalError",
                    "No object returned from complete multipart",
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
        Err(e) => {
            error!("Failed to complete multipart upload: {}", e);
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchUpload",
                    "The specified multipart upload does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else if e.code() == tonic::Code::InvalidArgument {
                S3Error::xml_response("InvalidPart", e.message(), StatusCode::BAD_REQUEST)
            } else {
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// GET /{bucket}/{key}?uploadId=X - List parts
pub async fn get_object_with_params(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<GetObjectParams>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    // If key is empty (trailing slash on bucket), treat as list_objects
    if key.is_empty() {
        let list_params = ListObjectsParams {
            prefix: None,
            delimiter: None,
            max_keys: params.max_parts,
            continuation_token: None,
            policy: None,
            versions: None,
            versioning: None,
            object_lock: None,
            lifecycle: None,
            encryption: None,
        };
        return list_objects(State(state), Path(bucket), Query(list_params), auth).await;
    }

    // If uploadId is present, this is a list parts request
    if let Some(upload_id) = params.upload_id {
        return list_parts_internal(
            state,
            bucket,
            key,
            upload_id,
            params.max_parts.unwrap_or(1000),
            params.part_number_marker.unwrap_or(0),
        )
        .await;
    }
    if params.retention.is_some() {
        return get_object_retention_internal(state, bucket, key).await;
    }
    if params.legal_hold.is_some() {
        return get_object_legal_hold_internal(state, bucket, key).await;
    }

    // Otherwise, it's a regular GET object
    get_object(State(state), Path((bucket, key)), auth, headers).await
}

/// List parts - internal implementation
async fn list_parts_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    upload_id: String,
    max_parts: u32,
    part_number_marker: u32,
) -> Response {
    let mut client = state.meta_client.clone();

    match client
        .list_parts(ListPartsRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            part_number_marker,
            max_parts,
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();

            let result = ListPartsResult {
                bucket: resp.bucket,
                key: resp.key,
                upload_id: resp.upload_id,
                part_number_marker,
                next_part_number_marker: if resp.is_truncated {
                    Some(resp.next_part_number_marker)
                } else {
                    None
                },
                max_parts,
                is_truncated: resp.is_truncated,
                parts: resp
                    .parts
                    .into_iter()
                    .map(|p| PartItem {
                        part_number: p.part_number,
                        last_modified: timestamp_to_iso(p.last_modified),
                        etag: p.etag,
                        size: p.size,
                    })
                    .collect(),
            };

            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchUpload",
                    "The specified multipart upload does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else {
                error!("Failed to list parts: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

/// DELETE /{bucket}/{key}?uploadId=X - Abort multipart upload
pub async fn delete_object_with_params(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<DeleteObjectParams>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    // If uploadId is present, this is an abort multipart upload request
    if let Some(upload_id) = params.upload_id {
        return abort_multipart_upload_internal(state, bucket, key, upload_id).await;
    }

    // Otherwise, it's a regular DELETE object (possibly version-specific)
    delete_object(
        State(state),
        Path((bucket, key)),
        auth,
        params.version_id,
        headers,
    )
    .await
}

/// Abort multipart upload - internal implementation
async fn abort_multipart_upload_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    upload_id: String,
) -> Response {
    let mut client = state.meta_client.clone();

    match client
        .abort_multipart_upload(AbortMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
        })
        .await
    {
        Ok(_) => {
            info!(
                "Aborted multipart upload: bucket={}, key={}, uploadId={}",
                bucket, key, upload_id
            );
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            error!("Failed to abort multipart upload: {}", e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

/// GET /{bucket}?uploads - List multipart uploads
#[allow(dead_code)]
pub async fn list_multipart_uploads(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
) -> Response {
    let mut client = state.meta_client.clone();

    match client
        .list_multipart_uploads(ListMultipartUploadsRequest {
            bucket: bucket.clone(),
            prefix: String::new(),
            key_marker: String::new(),
            upload_id_marker: String::new(),
            max_uploads: 1000,
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();

            let result = ListMultipartUploadsResult {
                bucket: bucket.clone(),
                key_marker: String::new(),
                upload_id_marker: String::new(),
                next_key_marker: if resp.is_truncated {
                    Some(resp.next_key_marker)
                } else {
                    None
                },
                next_upload_id_marker: if resp.is_truncated {
                    Some(resp.next_upload_id_marker)
                } else {
                    None
                },
                max_uploads: 1000,
                is_truncated: resp.is_truncated,
                uploads: resp
                    .uploads
                    .into_iter()
                    .map(|u| UploadItem {
                        key: u.key,
                        upload_id: u.upload_id,
                        initiated: timestamp_to_iso(u.initiated),
                        storage_class: u.storage_class,
                    })
                    .collect(),
            };

            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                S3Error::xml_response(
                    "NoSuchBucket",
                    "The specified bucket does not exist",
                    StatusCode::NOT_FOUND,
                )
            } else {
                error!("Failed to list multipart uploads: {}", e);
                S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            }
        }
    }
}

// ============================================================================
// Bucket Versioning
// ============================================================================

/// XML request for PUT bucket versioning
#[derive(Deserialize)]
#[serde(rename = "VersioningConfiguration")]
struct VersioningConfigurationRequest {
    #[serde(rename = "Status")]
    status: String,
}

/// XML response for GET bucket versioning
#[derive(Serialize)]
#[serde(rename = "VersioningConfiguration")]
struct VersioningConfigurationResponse {
    #[serde(rename = "Status")]
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

async fn put_bucket_versioning_internal(
    state: Arc<AppState>,
    bucket: String,
    body: Bytes,
) -> Response {
    let config: VersioningConfigurationRequest = match quick_xml::de::from_reader(body.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Invalid versioning XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let versioning_state = match config.status.as_str() {
        "Enabled" => VersioningState::VersioningEnabled,
        "Suspended" => VersioningState::VersioningSuspended,
        _ => {
            return S3Error::xml_response(
                "MalformedXML",
                "Status must be 'Enabled' or 'Suspended'",
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let mut client = state.meta_client.clone();
    match client
        .put_bucket_versioning(PutBucketVersioningRequest {
            bucket: bucket.clone(),
            state: versioning_state.into(),
        })
        .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
        Err(e) => {
            error!("Failed to set versioning for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

async fn get_bucket_versioning_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();
    match client
        .get_bucket_versioning(GetBucketVersioningRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => {
            let state = resp.into_inner().state();
            let status = match state {
                VersioningState::VersioningEnabled => Some("Enabled".to_string()),
                VersioningState::VersioningSuspended => Some("Suspended".to_string()),
                VersioningState::VersioningDisabled => None,
            };
            let result = VersioningConfigurationResponse { status };
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to get versioning for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

// ============================================================================
// Object Lock Configuration
// ============================================================================

#[derive(Deserialize)]
#[serde(rename = "ObjectLockConfiguration")]
struct ObjectLockConfigRequest {
    #[serde(rename = "ObjectLockEnabled")]
    #[serde(default)]
    object_lock_enabled: Option<String>,
    #[serde(rename = "Rule")]
    #[serde(default)]
    rule: Option<ObjectLockRuleXml>,
}

#[derive(Deserialize)]
struct ObjectLockRuleXml {
    #[serde(rename = "DefaultRetention")]
    #[serde(default)]
    default_retention: Option<DefaultRetentionXml>,
}

#[derive(Deserialize, Serialize)]
struct DefaultRetentionXml {
    #[serde(rename = "Mode")]
    mode: String,
    #[serde(rename = "Days")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    days: Option<u32>,
    #[serde(rename = "Years")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    years: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename = "ObjectLockConfiguration")]
struct ObjectLockConfigResponse {
    #[serde(rename = "ObjectLockEnabled")]
    object_lock_enabled: String,
    #[serde(rename = "Rule")]
    #[serde(skip_serializing_if = "Option::is_none")]
    rule: Option<ObjectLockRuleResponseXml>,
}

#[derive(Serialize)]
struct ObjectLockRuleResponseXml {
    #[serde(rename = "DefaultRetention")]
    default_retention: DefaultRetentionXml,
}

async fn put_object_lock_config_internal(
    state: Arc<AppState>,
    bucket: String,
    body: Bytes,
) -> Response {
    let config: ObjectLockConfigRequest = match quick_xml::de::from_reader(body.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Invalid object lock XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let default_retention = config.rule.and_then(|r| r.default_retention).map(|dr| {
        let mode = match dr.mode.as_str() {
            "GOVERNANCE" => RetentionMode::RetentionGovernance,
            _ => RetentionMode::RetentionCompliance,
        };
        RetentionRule {
            mode: mode.into(),
            days: dr.days.unwrap_or(0),
            years: dr.years.unwrap_or(0),
        }
    });

    let mut client = state.meta_client.clone();
    match client
        .put_object_lock_configuration(PutObjectLockConfigRequest {
            bucket: bucket.clone(),
            config: Some(ProtoObjectLockConfig {
                enabled: config
                    .object_lock_enabled
                    .as_deref()
                    .is_some_and(|v| v == "Enabled"),
                default_retention,
            }),
        })
        .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
        Err(e) => {
            error!("Failed to set object lock config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

async fn get_object_lock_config_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();
    match client
        .get_object_lock_configuration(GetObjectLockConfigRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            if !inner.found {
                return S3Error::xml_response(
                    "ObjectLockConfigurationNotFoundError",
                    "Object Lock configuration does not exist for this bucket",
                    StatusCode::NOT_FOUND,
                );
            }
            let config = inner.config.unwrap_or_default();
            let rule = config.default_retention.map(|dr| {
                let mode = match dr.mode() {
                    RetentionMode::RetentionGovernance => "GOVERNANCE",
                    RetentionMode::RetentionCompliance => "COMPLIANCE",
                    RetentionMode::RetentionNone => "COMPLIANCE",
                };
                ObjectLockRuleResponseXml {
                    default_retention: DefaultRetentionXml {
                        mode: mode.to_string(),
                        days: if dr.days > 0 { Some(dr.days) } else { None },
                        years: if dr.years > 0 { Some(dr.years) } else { None },
                    },
                }
            });
            let result = ObjectLockConfigResponse {
                object_lock_enabled: if config.enabled {
                    "Enabled".to_string()
                } else {
                    "Disabled".to_string()
                },
                rule,
            };
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to get object lock config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

// ============================================================================
// Lifecycle Configuration
// ============================================================================

#[derive(Deserialize)]
#[serde(rename = "LifecycleConfiguration")]
struct LifecycleConfigRequest {
    #[serde(rename = "Rule")]
    #[serde(default)]
    rules: Vec<LifecycleRuleXml>,
}

#[derive(Deserialize, Serialize, Clone)]
struct LifecycleRuleXml {
    #[serde(rename = "ID")]
    #[serde(default)]
    id: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Filter")]
    #[serde(default)]
    filter: Option<LifecycleFilterXml>,
    #[serde(rename = "Expiration")]
    #[serde(default)]
    expiration: Option<LifecycleExpirationXml>,
    #[serde(rename = "NoncurrentVersionExpiration")]
    #[serde(default)]
    noncurrent_version_expiration: Option<NoncurrentVersionExpirationXml>,
    #[serde(rename = "AbortIncompleteMultipartUpload")]
    #[serde(default)]
    abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUploadXml>,
}

#[derive(Deserialize, Serialize, Clone, Default)]
struct LifecycleFilterXml {
    #[serde(rename = "Prefix")]
    #[serde(default)]
    prefix: String,
}

#[derive(Deserialize, Serialize, Clone)]
struct LifecycleExpirationXml {
    #[serde(rename = "Days")]
    #[serde(default)]
    days: Option<u32>,
    #[serde(rename = "ExpiredObjectDeleteMarker")]
    #[serde(default)]
    expired_object_delete_marker: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone)]
struct NoncurrentVersionExpirationXml {
    #[serde(rename = "NoncurrentDays")]
    #[serde(default)]
    noncurrent_days: Option<u32>,
}

#[derive(Deserialize, Serialize, Clone)]
struct AbortIncompleteMultipartUploadXml {
    #[serde(rename = "DaysAfterInitiation")]
    #[serde(default)]
    days_after_initiation: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename = "LifecycleConfiguration")]
struct LifecycleConfigResponse {
    #[serde(rename = "Rule")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rules: Vec<LifecycleRuleXml>,
}

async fn put_bucket_lifecycle_internal(
    state: Arc<AppState>,
    bucket: String,
    body: Bytes,
) -> Response {
    let config: LifecycleConfigRequest = match quick_xml::de::from_reader(body.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Invalid lifecycle XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let proto_rules: Vec<ProtoLifecycleRule> = config
        .rules
        .iter()
        .map(|r| ProtoLifecycleRule {
            id: r.id.clone(),
            enabled: r.status == "Enabled",
            prefix: r
                .filter
                .as_ref()
                .map(|f| f.prefix.clone())
                .unwrap_or_default(),
            expiration_days: r.expiration.as_ref().and_then(|e| e.days).unwrap_or(0),
            expiration_date: 0,
            noncurrent_version_expiration_days: r
                .noncurrent_version_expiration
                .as_ref()
                .and_then(|n| n.noncurrent_days)
                .unwrap_or(0),
            expired_object_delete_marker: r
                .expiration
                .as_ref()
                .and_then(|e| e.expired_object_delete_marker)
                .unwrap_or(false),
            abort_incomplete_multipart_upload_days: r
                .abort_incomplete_multipart_upload
                .as_ref()
                .and_then(|a| a.days_after_initiation)
                .unwrap_or(0),
        })
        .collect();

    let mut client = state.meta_client.clone();
    match client
        .put_bucket_lifecycle(PutBucketLifecycleRequest {
            bucket: bucket.clone(),
            config: Some(ProtoLifecycleConfig { rules: proto_rules }),
        })
        .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
        Err(e) => {
            error!("Failed to set lifecycle config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

async fn get_bucket_lifecycle_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();
    match client
        .get_bucket_lifecycle(GetBucketLifecycleRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            if !inner.found {
                return S3Error::xml_response(
                    "NoSuchLifecycleConfiguration",
                    "The lifecycle configuration does not exist",
                    StatusCode::NOT_FOUND,
                );
            }
            let config = inner.config.unwrap_or_default();
            let rules: Vec<LifecycleRuleXml> = config
                .rules
                .iter()
                .map(|r| LifecycleRuleXml {
                    id: r.id.clone(),
                    status: if r.enabled {
                        "Enabled".to_string()
                    } else {
                        "Disabled".to_string()
                    },
                    filter: Some(LifecycleFilterXml {
                        prefix: r.prefix.clone(),
                    }),
                    expiration: if r.expiration_days > 0 || r.expired_object_delete_marker {
                        Some(LifecycleExpirationXml {
                            days: if r.expiration_days > 0 {
                                Some(r.expiration_days)
                            } else {
                                None
                            },
                            expired_object_delete_marker: if r.expired_object_delete_marker {
                                Some(true)
                            } else {
                                None
                            },
                        })
                    } else {
                        None
                    },
                    noncurrent_version_expiration: if r.noncurrent_version_expiration_days > 0 {
                        Some(NoncurrentVersionExpirationXml {
                            noncurrent_days: Some(r.noncurrent_version_expiration_days),
                        })
                    } else {
                        None
                    },
                    abort_incomplete_multipart_upload: if r.abort_incomplete_multipart_upload_days
                        > 0
                    {
                        Some(AbortIncompleteMultipartUploadXml {
                            days_after_initiation: Some(r.abort_incomplete_multipart_upload_days),
                        })
                    } else {
                        None
                    },
                })
                .collect();

            let result = LifecycleConfigResponse { rules };
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to get lifecycle config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

async fn delete_bucket_lifecycle_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();
    match client
        .delete_bucket_lifecycle(DeleteBucketLifecycleRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(e) => {
            error!("Failed to delete lifecycle config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

// ============================================================================
// Bucket Default Server-Side Encryption
// ============================================================================

#[derive(Deserialize)]
#[serde(rename = "ServerSideEncryptionConfiguration")]
struct SseConfigRequest {
    #[serde(rename = "Rule")]
    #[serde(default)]
    rules: Vec<SseRuleXml>,
}

#[derive(Deserialize, Serialize, Clone)]
struct SseRuleXml {
    #[serde(rename = "ApplyServerSideEncryptionByDefault")]
    apply_default: Option<SseByDefaultXml>,
    #[serde(rename = "BucketKeyEnabled")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket_key_enabled: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone)]
struct SseByDefaultXml {
    #[serde(rename = "SSEAlgorithm")]
    sse_algorithm: String,
    #[serde(rename = "KMSMasterKeyID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    kms_master_key_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename = "ServerSideEncryptionConfiguration")]
struct SseConfigResponse {
    #[serde(rename = "Rule")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rules: Vec<SseRuleXml>,
}

fn parse_sse_algorithm(s: &str) -> Option<SseAlgorithm> {
    match s {
        "AES256" => Some(SseAlgorithm::SseS3),
        "aws:kms" => Some(SseAlgorithm::SseKms),
        _ => None,
    }
}

fn sse_algorithm_to_aws(alg: SseAlgorithm) -> Option<&'static str> {
    match alg {
        SseAlgorithm::SseS3 => Some("AES256"),
        SseAlgorithm::SseKms => Some("aws:kms"),
        SseAlgorithm::SseNone | SseAlgorithm::SseC => None,
    }
}

async fn put_bucket_encryption_internal(
    state: Arc<AppState>,
    bucket: String,
    body: Bytes,
) -> Response {
    let config: SseConfigRequest = match quick_xml::de::from_reader(body.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Invalid encryption XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let mut proto_rules: Vec<SseRule> = Vec::with_capacity(config.rules.len());
    for rule in &config.rules {
        let apply = rule.apply_default.as_ref().ok_or(());
        let Ok(apply) = apply else {
            return S3Error::xml_response(
                "MalformedXML",
                "Missing ApplyServerSideEncryptionByDefault",
                StatusCode::BAD_REQUEST,
            );
        };
        let algorithm = match parse_sse_algorithm(&apply.sse_algorithm) {
            Some(a) => a,
            None => {
                return S3Error::xml_response(
                    "InvalidEncryptionAlgorithmError",
                    &format!(
                        "The encryption algorithm '{}' is not supported as a bucket default",
                        apply.sse_algorithm
                    ),
                    StatusCode::BAD_REQUEST,
                );
            }
        };
        if algorithm == SseAlgorithm::SseKms
            && apply.kms_master_key_id.as_deref().unwrap_or("").is_empty()
        {
            return S3Error::xml_response(
                "InvalidArgument",
                "KMSMasterKeyID is required when SSEAlgorithm is aws:kms",
                StatusCode::BAD_REQUEST,
            );
        }
        proto_rules.push(SseRule {
            algorithm: algorithm as i32,
            kms_key_id: apply.kms_master_key_id.clone().unwrap_or_default(),
            bucket_key_enabled: rule.bucket_key_enabled.unwrap_or(false),
        });
    }

    let mut client = state.meta_client.clone();
    match client
        .put_bucket_encryption(PutBucketEncryptionRequest {
            bucket: bucket.clone(),
            config: Some(BucketSseConfiguration { rules: proto_rules }),
        })
        .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
        Err(e) => {
            error!("Failed to set encryption config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

async fn get_bucket_encryption_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();
    match client
        .get_bucket_encryption(GetBucketEncryptionRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            if !inner.found {
                return S3Error::xml_response(
                    "ServerSideEncryptionConfigurationNotFoundError",
                    "The server side encryption configuration was not found",
                    StatusCode::NOT_FOUND,
                );
            }
            let config = inner.config.unwrap_or_default();
            let rules: Vec<SseRuleXml> = config
                .rules
                .iter()
                .filter_map(|r| {
                    let algorithm = SseAlgorithm::try_from(r.algorithm).ok()?;
                    let aws_name = sse_algorithm_to_aws(algorithm)?;
                    Some(SseRuleXml {
                        apply_default: Some(SseByDefaultXml {
                            sse_algorithm: aws_name.to_string(),
                            kms_master_key_id: if r.kms_key_id.is_empty() {
                                None
                            } else {
                                Some(r.kms_key_id.clone())
                            },
                        }),
                        bucket_key_enabled: if r.bucket_key_enabled { Some(true) } else { None },
                    })
                })
                .collect();

            let result = SseConfigResponse { rules };
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to get encryption config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

async fn delete_bucket_encryption_internal(state: Arc<AppState>, bucket: String) -> Response {
    let mut client = state.meta_client.clone();
    match client
        .delete_bucket_encryption(DeleteBucketEncryptionRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(e) => {
            error!("Failed to delete encryption config for {}: {}", bucket, e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

// ============================================================================
// Object Retention & Legal Hold
// ============================================================================

#[derive(Deserialize)]
#[serde(rename = "Retention")]
struct RetentionRequest {
    #[serde(rename = "Mode")]
    mode: String,
    #[serde(rename = "RetainUntilDate")]
    retain_until_date: String,
}

#[derive(Serialize)]
#[serde(rename = "Retention")]
struct RetentionResponse {
    #[serde(rename = "Mode")]
    mode: String,
    #[serde(rename = "RetainUntilDate")]
    retain_until_date: String,
}

#[derive(Deserialize)]
#[serde(rename = "LegalHold")]
struct LegalHoldRequest {
    #[serde(rename = "Status")]
    status: String,
}

#[derive(Serialize)]
#[serde(rename = "LegalHold")]
struct LegalHoldResponse {
    #[serde(rename = "Status")]
    status: String,
}

async fn put_object_retention_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    body: Bytes,
) -> Response {
    let req: RetentionRequest = match quick_xml::de::from_reader(body.as_ref()) {
        Ok(r) => r,
        Err(e) => {
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Invalid retention XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let mode = match req.mode.as_str() {
        "GOVERNANCE" => RetentionMode::RetentionGovernance,
        "COMPLIANCE" => RetentionMode::RetentionCompliance,
        _ => {
            return S3Error::xml_response(
                "MalformedXML",
                "Mode must be GOVERNANCE or COMPLIANCE",
                StatusCode::BAD_REQUEST,
            );
        }
    };

    // Parse ISO 8601 date to unix timestamp
    let retain_until = chrono::DateTime::parse_from_rfc3339(&req.retain_until_date)
        .map(|dt| dt.timestamp() as u64)
        .unwrap_or(0);

    // Get current object metadata from primary OSD, update retention, put back
    let primary_osd = match get_primary_osd_for_object(&state, &bucket, &key).await {
        Ok(osd) => osd,
        Err(resp) => return resp,
    };

    let mut object_meta =
        match get_object_meta_from_osd(&state.osd_pool, &primary_osd, &bucket, &key).await {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                return S3Error::xml_response(
                    "NoSuchKey",
                    "Object not found",
                    StatusCode::NOT_FOUND,
                );
            }
            Err(e) => {
                error!("Failed to get object metadata: {}", e);
                return S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

    object_meta.retention = Some(ObjectRetention {
        mode: mode.into(),
        retain_until_date: retain_until,
    });

    if let Err(e) =
        put_object_meta_to_osd(&state.osd_pool, &primary_osd, &bucket, &key, object_meta).await
    {
        error!("Failed to update object retention: {}", e);
        return S3Error::xml_response(
            "InternalError",
            &e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap()
}

async fn get_object_retention_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
) -> Response {
    let primary_osd = match get_primary_osd_for_object(&state, &bucket, &key).await {
        Ok(osd) => osd,
        Err(resp) => return resp,
    };

    match get_object_meta_from_osd(&state.osd_pool, &primary_osd, &bucket, &key).await {
        Ok(Some(meta)) => match meta.retention {
            Some(retention) => {
                let mode = match retention.mode() {
                    RetentionMode::RetentionGovernance => "GOVERNANCE",
                    RetentionMode::RetentionCompliance => "COMPLIANCE",
                    RetentionMode::RetentionNone => "COMPLIANCE",
                };
                let date = chrono::DateTime::from_timestamp(retention.retain_until_date as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default();
                let result = RetentionResponse {
                    mode: mode.to_string(),
                    retain_until_date: date,
                };
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                    to_xml(&result).unwrap_or_default()
                );
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/xml")
                    .body(Body::from(xml))
                    .unwrap()
            }
            None => S3Error::xml_response(
                "NoSuchObjectLockConfiguration",
                "No retention set on this object",
                StatusCode::NOT_FOUND,
            ),
        },
        Ok(None) => S3Error::xml_response("NoSuchKey", "Object not found", StatusCode::NOT_FOUND),
        Err(e) => {
            error!("Failed to get object metadata: {}", e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

async fn put_object_legal_hold_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
    body: Bytes,
) -> Response {
    let req: LegalHoldRequest = match quick_xml::de::from_reader(body.as_ref()) {
        Ok(r) => r,
        Err(e) => {
            return S3Error::xml_response(
                "MalformedXML",
                &format!("Invalid legal hold XML: {}", e),
                StatusCode::BAD_REQUEST,
            );
        }
    };

    let status = req.status == "ON";

    let primary_osd = match get_primary_osd_for_object(&state, &bucket, &key).await {
        Ok(osd) => osd,
        Err(resp) => return resp,
    };

    let mut object_meta =
        match get_object_meta_from_osd(&state.osd_pool, &primary_osd, &bucket, &key).await {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                return S3Error::xml_response(
                    "NoSuchKey",
                    "Object not found",
                    StatusCode::NOT_FOUND,
                );
            }
            Err(e) => {
                error!("Failed to get object metadata: {}", e);
                return S3Error::xml_response(
                    "InternalError",
                    &e.to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

    object_meta.legal_hold = Some(LegalHold { status });

    if let Err(e) =
        put_object_meta_to_osd(&state.osd_pool, &primary_osd, &bucket, &key, object_meta).await
    {
        error!("Failed to update legal hold: {}", e);
        return S3Error::xml_response(
            "InternalError",
            &e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap()
}

async fn get_object_legal_hold_internal(
    state: Arc<AppState>,
    bucket: String,
    key: String,
) -> Response {
    let primary_osd = match get_primary_osd_for_object(&state, &bucket, &key).await {
        Ok(osd) => osd,
        Err(resp) => return resp,
    };

    match get_object_meta_from_osd(&state.osd_pool, &primary_osd, &bucket, &key).await {
        Ok(Some(meta)) => {
            let status = meta.legal_hold.as_ref().is_some_and(|lh| lh.status);
            let result = LegalHoldResponse {
                status: if status {
                    "ON".to_string()
                } else {
                    "OFF".to_string()
                },
            };
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&result).unwrap_or_default()
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Ok(None) => S3Error::xml_response("NoSuchKey", "Object not found", StatusCode::NOT_FOUND),
        Err(e) => {
            error!("Failed to get object metadata: {}", e);
            S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

// ============================================================================
// List Object Versions
// ============================================================================

#[derive(Serialize)]
#[serde(rename = "ListVersionsResult")]
struct ListVersionsResult {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "MaxKeys")]
    max_keys: u32,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Version")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    versions: Vec<ObjectVersionXml>,
    #[serde(rename = "DeleteMarker")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    delete_markers: Vec<DeleteMarkerXml>,
}

#[derive(Serialize)]
struct ObjectVersionXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: String,
    #[serde(rename = "IsLatest")]
    is_latest: bool,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "Size")]
    size: u64,
    #[serde(rename = "StorageClass")]
    storage_class: String,
}

#[derive(Serialize)]
struct DeleteMarkerXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: String,
    #[serde(rename = "IsLatest")]
    is_latest: bool,
    #[serde(rename = "LastModified")]
    last_modified: String,
}

async fn list_object_versions_internal(
    state: Arc<AppState>,
    bucket: String,
    prefix: String,
    max_keys: u32,
) -> Response {
    // Use scatter-gather to list versions from all OSDs
    use objectio_proto::storage::ListObjectVersionsMetaRequest;

    let nodes = match state
        .meta_client
        .clone()
        .get_listing_nodes(GetListingNodesRequest {
            bucket: bucket.clone(),
        })
        .await
    {
        Ok(resp) => resp.into_inner().nodes,
        Err(e) => {
            error!("Failed to get listing nodes: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let mut all_versions = Vec::new();
    let mut all_delete_markers = Vec::new();

    for node in &nodes {
        let addr = format!("http://{}", node.address);
        let mut client =
            match objectio_proto::storage::storage_service_client::StorageServiceClient::connect(
                addr,
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to connect to OSD {}: {}", node.address, e);
                    continue;
                }
            };

        match client
            .list_object_versions_meta(ListObjectVersionsMetaRequest {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                key_marker: String::new(),
                version_id_marker: String::new(),
                max_keys,
            })
            .await
        {
            Ok(resp) => {
                let inner = resp.into_inner();
                for obj in inner.versions {
                    let last_modified = chrono::DateTime::from_timestamp(obj.modified_at as i64, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_default();

                    if obj.is_delete_marker {
                        all_delete_markers.push(DeleteMarkerXml {
                            key: obj.key,
                            version_id: obj.version_id,
                            is_latest: false, // Set later after sorting
                            last_modified,
                        });
                    } else {
                        all_versions.push(ObjectVersionXml {
                            key: obj.key,
                            version_id: obj.version_id,
                            is_latest: false,
                            last_modified,
                            etag: obj.etag,
                            size: obj.size,
                            storage_class: if obj.storage_class.is_empty() {
                                "STANDARD".to_string()
                            } else {
                                obj.storage_class
                            },
                        });
                    }
                }
            }
            Err(e) => {
                warn!("Failed to list versions from OSD {}: {}", node.address, e);
            }
        }
    }

    // Sort by key, then by modified_at desc to determine is_latest
    all_versions.sort_by(|a, b| {
        a.key
            .cmp(&b.key)
            .then(b.last_modified.cmp(&a.last_modified))
    });

    // Mark the first version of each key as is_latest
    let mut seen_keys = std::collections::HashSet::new();
    for v in &mut all_versions {
        if seen_keys.insert(v.key.clone()) {
            v.is_latest = true;
        }
    }

    let is_truncated = all_versions.len() as u32 > max_keys;
    if is_truncated {
        all_versions.truncate(max_keys as usize);
    }

    let result = ListVersionsResult {
        name: bucket,
        prefix,
        max_keys,
        is_truncated,
        versions: all_versions,
        delete_markers: all_delete_markers,
    };

    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
        to_xml(&result).unwrap_or_default()
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

/// Helper to get the primary OSD placement for an object
async fn get_primary_osd_for_object(
    state: &AppState,
    bucket: &str,
    key: &str,
) -> Result<objectio_proto::metadata::NodePlacement, Response> {
    let placement_key = format!("{}/{}", bucket, key);
    let mut client = state.meta_client.clone();
    match client
        .get_placement(GetPlacementRequest {
            key: placement_key,
            bucket: bucket.to_string(),
            size: 0,
            storage_class: String::new(),
        })
        .await
    {
        Ok(resp) => {
            let placement = resp.into_inner();
            if placement.nodes.is_empty() {
                Err(S3Error::xml_response(
                    "InternalError",
                    "No OSD nodes available",
                    StatusCode::INTERNAL_SERVER_ERROR,
                ))
            } else {
                Ok(placement.nodes[0].clone())
            }
        }
        Err(e) => {
            error!("Failed to get placement: {}", e);
            Err(S3Error::xml_response(
                "InternalError",
                &e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }
}

// ============================================================================
// Admin API - IAM Operations
// ============================================================================

/// Admin API response types
#[derive(Serialize)]
pub struct AdminUserResponse {
    pub user_id: String,
    pub display_name: String,
    pub arn: String,
    pub status: String,
    pub created_at: u64,
    pub email: String,
    pub tenant: String,
}

#[derive(Serialize)]
pub struct AdminAccessKeyResponse {
    pub access_key_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    pub user_id: String,
    pub status: String,
    pub created_at: u64,
}

#[derive(Serialize)]
pub struct AdminListUsersResponse {
    pub users: Vec<AdminUserResponse>,
    pub is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_marker: Option<String>,
}

#[derive(Serialize)]
pub struct AdminListAccessKeysResponse {
    pub access_keys: Vec<AdminAccessKeyResponse>,
}

#[derive(Deserialize)]
pub struct CreateUserParams {
    pub display_name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub tenant: String,
}

/// Admin user ARN - only this user can access admin endpoints
const ADMIN_USER_ARN: &str = "arn:objectio:iam::user/admin";

/// Check if the authenticated user is the admin user
fn is_admin_user(auth: &AuthResult) -> bool {
    auth.user_arn == ADMIN_USER_ARN
}

/// Return a 403 Forbidden response for non-admin users
fn admin_forbidden_response() -> Response {
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"error":"Admin access required. Only the 'admin' user can access this endpoint."}"#,
        ))
        .unwrap()
}

/// Return a 401 Unauthorized response when auth is disabled
fn admin_auth_required_response() -> Response {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"error":"Admin API requires authentication. Start gateway without --no-auth flag."}"#))
        .unwrap()
}

/// Check if request is from admin user, returns error response if not.
/// Accepts SigV4 auth or console session cookie.
#[allow(clippy::result_large_err)]
fn check_admin_access(
    auth: Option<Extension<AuthResult>>,
    headers: &HeaderMap,
) -> Result<(), Response> {
    // SigV4 auth
    if let Some(Extension(ref auth_result)) = auth {
        if is_admin_user(auth_result) {
            return Ok(());
        }
        warn!("Admin API access denied for user: {}", auth_result.user_arn);
        return Err(admin_forbidden_response());
    }

    // Console session cookie — login already validated credentials
    if crate::console_auth::validate_session_from_headers(headers).is_some() {
        return Ok(());
    }

    warn!("Admin API access attempted without authentication");
    Err(admin_auth_required_response())
}

/// List users (GET /_admin/users)
pub async fn admin_list_users(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
) -> Response {
    // Extract tenant before auth check consumes auth
    let tenant = auth
        .as_ref()
        .map(|Extension(a)| a.tenant.clone())
        .or_else(|| crate::console_auth::validate_session_from_headers(&headers).map(|s| s.tenant))
        .unwrap_or_default();

    if let Err(response) = check_admin_access(auth, &headers) {
        return response;
    }

    let mut client = state.meta_client.clone();

    match client
        .list_users(ListUsersRequest {
            max_results: 1000,
            marker: String::new(),
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();
            let result = AdminListUsersResponse {
                users: resp
                    .users
                    .into_iter()
                    .filter(|u| tenant.is_empty() || u.tenant == tenant)
                    .map(|u| AdminUserResponse {
                        user_id: u.user_id,
                        display_name: u.display_name,
                        arn: u.arn,
                        status: format!("{:?}", u.status),
                        created_at: u.created_at,
                        email: u.email,
                        tenant: u.tenant,
                    })
                    .collect(),
                is_truncated: resp.is_truncated,
                next_marker: if resp.next_marker.is_empty() {
                    None
                } else {
                    Some(resp.next_marker)
                },
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&result).unwrap()))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to list users: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"error":"{}"}}"#, e)))
                .unwrap()
        }
    }
}

/// Create user (POST /_admin/users)
pub async fn admin_create_user(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = crate::admin::extract_caller(&auth, &headers);

    let params: CreateUserParams = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"error":"Invalid JSON: {}"}}"#, e)))
                .unwrap();
        }
    };

    // Default to the caller's tenant if the body leaves it empty.
    let tenant = if params.tenant.is_empty() {
        caller.tenant.clone()
    } else {
        params.tenant
    };

    // System admin can create users in any tenant (including system scope).
    // Tenant admins can only create users inside their own tenant.
    if tenant.is_empty() {
        if let Some(deny) = crate::admin::require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        crate::admin::require_tenant_admin_access(&state, &auth, &headers, &tenant).await
    {
        return deny;
    }

    let mut client = state.meta_client.clone();

    match client
        .create_user(CreateUserRequest {
            display_name: params.display_name,
            email: params.email,
            tenant,
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();
            let user = resp.user.unwrap();
            let result = AdminUserResponse {
                user_id: user.user_id,
                display_name: user.display_name,
                arn: user.arn,
                status: format!("{:?}", user.status),
                created_at: user.created_at,
                email: user.email,
                tenant: user.tenant,
            };

            info!("Created user: {}", result.user_id);
            Response::builder()
                .status(StatusCode::CREATED)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&result).unwrap()))
                .unwrap()
        }
        Err(e) => {
            error!("Failed to create user: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"error":"{}"}}"#, e)))
                .unwrap()
        }
    }
}

/// Resolve a user_id to its tenant via meta. Returns `None` if the user
/// does not exist, letting callers respond 404 appropriately.
async fn lookup_user_tenant(state: &AppState, user_id: &str) -> Option<String> {
    let mut client = state.meta_client.clone();
    let resp = client
        .get_user(GetUserRequest {
            user_id: user_id.to_string(),
        })
        .await
        .ok()?
        .into_inner();
    resp.user.map(|u| u.tenant)
}

/// Resolve an access_key_id to its tenant via meta.
async fn lookup_access_key_tenant(state: &AppState, access_key_id: &str) -> Option<String> {
    let mut client = state.meta_client.clone();
    let resp = client
        .get_access_key_for_auth(GetAccessKeyForAuthRequest {
            access_key_id: access_key_id.to_string(),
        })
        .await
        .ok()?
        .into_inner();
    resp.user.map(|u| u.tenant)
}

/// Delete user (DELETE /_admin/users/{user_id})
pub async fn admin_delete_user(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Response {
    // Look up target user's tenant so tenant admins cannot delete users
    // outside their own tenant.
    let target_tenant = match lookup_user_tenant(&state, &user_id).await {
        Some(t) => t,
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"error":"User not found"}"#))
                .unwrap();
        }
    };
    if target_tenant.is_empty() {
        if let Some(deny) = crate::admin::require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        crate::admin::require_tenant_admin_access(&state, &auth, &headers, &target_tenant).await
    {
        return deny;
    }

    let mut client = state.meta_client.clone();

    match client
        .delete_user(DeleteUserRequest {
            user_id: user_id.clone(),
        })
        .await
    {
        Ok(_) => {
            info!("Deleted user: {}", user_id);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"error":"User not found"}"#))
                    .unwrap()
            } else {
                error!("Failed to delete user: {}", e);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"error":"{}"}}"#, e)))
                    .unwrap()
            }
        }
    }
}

/// List access keys for user (GET /_admin/users/{user_id}/access-keys)
pub async fn admin_list_access_keys(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Response {
    let target_tenant = match lookup_user_tenant(&state, &user_id).await {
        Some(t) => t,
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"error":"User not found"}"#))
                .unwrap();
        }
    };
    if target_tenant.is_empty() {
        if let Some(deny) = crate::admin::require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        crate::admin::require_tenant_admin_access(&state, &auth, &headers, &target_tenant).await
    {
        return deny;
    }

    let mut client = state.meta_client.clone();

    match client
        .list_access_keys(ListAccessKeysRequest {
            user_id: user_id.clone(),
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();
            let result = AdminListAccessKeysResponse {
                access_keys: resp
                    .access_keys
                    .into_iter()
                    .map(|k| AdminAccessKeyResponse {
                        access_key_id: k.access_key_id,
                        secret_access_key: None, // Don't return secret on list
                        user_id: k.user_id,
                        status: format!("{:?}", k.status),
                        created_at: k.created_at,
                    })
                    .collect(),
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&result).unwrap()))
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"error":"User not found"}"#))
                    .unwrap()
            } else {
                error!("Failed to list access keys: {}", e);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"error":"{}"}}"#, e)))
                    .unwrap()
            }
        }
    }
}

/// Create access key for user (POST /_admin/users/{user_id}/access-keys)
pub async fn admin_create_access_key(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> Response {
    let target_tenant = match lookup_user_tenant(&state, &user_id).await {
        Some(t) => t,
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"error":"User not found"}"#))
                .unwrap();
        }
    };
    if target_tenant.is_empty() {
        if let Some(deny) = crate::admin::require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        crate::admin::require_tenant_admin_access(&state, &auth, &headers, &target_tenant).await
    {
        return deny;
    }

    let mut client = state.meta_client.clone();

    match client
        .create_access_key(CreateAccessKeyRequest {
            user_id: user_id.clone(),
        })
        .await
    {
        Ok(response) => {
            let resp = response.into_inner();
            let key = resp.access_key.unwrap();
            let result = AdminAccessKeyResponse {
                access_key_id: key.access_key_id,
                secret_access_key: Some(key.secret_access_key), // Include secret on create
                user_id: key.user_id,
                status: format!("{:?}", key.status),
                created_at: key.created_at,
            };

            info!(
                "Created access key {} for user {}",
                result.access_key_id, user_id
            );
            Response::builder()
                .status(StatusCode::CREATED)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&result).unwrap()))
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"error":"User not found"}"#))
                    .unwrap()
            } else {
                error!("Failed to create access key: {}", e);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"error":"{}"}}"#, e)))
                    .unwrap()
            }
        }
    }
}

/// Delete access key (DELETE /_admin/access-keys/{access_key_id})
pub async fn admin_delete_access_key(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    headers: HeaderMap,
    Path(access_key_id): Path<String>,
) -> Response {
    let target_tenant = match lookup_access_key_tenant(&state, &access_key_id).await {
        Some(t) => t,
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"error":"Access key not found"}"#))
                .unwrap();
        }
    };
    if target_tenant.is_empty() {
        if let Some(deny) = crate::admin::require_system_admin(&auth, &headers) {
            return deny;
        }
    } else if let Some(deny) =
        crate::admin::require_tenant_admin_access(&state, &auth, &headers, &target_tenant).await
    {
        return deny;
    }

    let mut client = state.meta_client.clone();

    match client
        .delete_access_key(DeleteAccessKeyRequest {
            access_key_id: access_key_id.clone(),
        })
        .await
    {
        Ok(_) => {
            info!("Deleted access key: {}", access_key_id);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        Err(e) => {
            if e.code() == tonic::Code::NotFound {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"error":"Access key not found"}"#))
                    .unwrap()
            } else {
                error!("Failed to delete access key: {}", e);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"error":"{}"}}"#, e)))
                    .unwrap()
            }
        }
    }
}
