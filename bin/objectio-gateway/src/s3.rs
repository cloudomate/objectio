//! S3 API handlers

/// Maximum shard size in bytes (must fit in a storage block)
/// Block size is 4MB with ~96 bytes overhead, so use 4MB - 4KB for safety margin
const MAX_SHARD_SIZE: usize = 4 * 1024 * 1024 - 4096; // ~4MB per shard

use crate::osd_pool::{
    OsdPool, delete_object_meta_from_osd, get_object_meta_from_osd, put_object_meta_to_osd,
    read_shard_from_osd, write_shard_to_osd,
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
    DeleteAccessKeyRequest,
    DeleteBucketPolicyRequest,
    DeleteBucketRequest,
    DeleteUserRequest,
    ErasureType,
    GetBucketPolicyRequest,
    GetBucketRequest,
    GetPlacementRequest,
    ListAccessKeysRequest,
    ListBucketsRequest,
    ListMultipartUploadsRequest,
    ListPartsRequest,
    ListUsersRequest,
    ObjectMeta,
    PartInfo,
    RegisterPartRequest,
    SetBucketPolicyRequest,
    ShardLocation,
    StripeMeta,
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
}

/// Extract user metadata from request headers (x-amz-meta-* headers)
fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if name_str.starts_with("x-amz-meta-") {
            if let Ok(value_str) = value.to_str() {
                // Strip the x-amz-meta- prefix for storage
                let key = name_str.strip_prefix("x-amz-meta-").unwrap_or(&name_str);
                metadata.insert(key.to_string(), value_str.to_string());
            }
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

/// Check bucket policy and return error response if access is denied
async fn check_bucket_policy(
    state: &AppState,
    bucket: &str,
    user_arn: &str,
    action: &str,
    resource: &str,
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
                        let context = RequestContext::new(user_arn, action, resource);
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
}

/// Query parameters for DELETE bucket operations
#[derive(Debug, Deserialize, Default)]
pub struct DeleteBucketParams {
    /// If present (even empty), this is a policy request
    policy: Option<String>,
    /// If present, this is a list multipart uploads request
    uploads: Option<String>,
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
    fn xml_response(code: &str, message: &str, status: StatusCode) -> Response {
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
pub async fn list_buckets(State(state): State<Arc<AppState>>) -> Response {
    let mut client = state.meta_client.clone();

    match client
        .list_buckets(ListBucketsRequest {
            owner: String::new(),
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

/// Create bucket or set bucket policy (PUT /{bucket} or PUT /{bucket}?policy)
pub async fn create_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(params): Query<PutBucketParams>,
    body: Bytes,
) -> Response {
    // Check if this is a policy request
    if params.policy.is_some() {
        return put_bucket_policy_internal(state, bucket, body).await;
    }

    let mut client = state.meta_client.clone();

    match client
        .create_bucket(CreateBucketRequest {
            name: bucket.clone(),
            owner: "default".to_string(),
            storage_class: "STANDARD".to_string(),
            region: "us-east-1".to_string(),
        })
        .await
    {
        Ok(_) => {
            info!("Created bucket: {}", bucket);
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
    // Check if this is a policy request
    if params.policy.is_some() {
        return delete_bucket_policy_internal(state, bucket).await;
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
    // Check if this is a policy request
    if params.is_policy_request() {
        return get_bucket_policy_internal(state, bucket).await;
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

    // If this is a copy operation, read the source object first
    let (body, is_copy) = if let Some(ref source) = copy_source {
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

        debug!(
            "CopyObject: {}/{} -> {}/{}",
            source_bucket, source_key, bucket, key
        );

        // Read the source object using internal get
        let mut meta_client = state.meta_client.clone();

        // Get placement for source
        let placement = match meta_client
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
                error!("Failed to get placement for copy source: {}", e);
                return S3Error::xml_response(
                    "NoSuchKey",
                    "The specified key does not exist",
                    StatusCode::NOT_FOUND,
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

        // Get source object metadata
        let primary_osd = &placement.nodes[0];
        let source_obj =
            match get_object_meta_from_osd(&state.osd_pool, primary_osd, source_bucket, source_key)
                .await
            {
                Ok(Some(obj)) => obj,
                Ok(None) => {
                    return S3Error::xml_response(
                        "NoSuchKey",
                        "The specified key does not exist",
                        StatusCode::NOT_FOUND,
                    );
                }
                Err(e) => {
                    error!("Failed to get source object metadata: {}", e);
                    return S3Error::xml_response(
                        "InternalError",
                        &e.to_string(),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
            };

        // Read source object data from stripes
        let mut source_data = Vec::with_capacity(source_obj.size as usize);
        for stripe in &source_obj.stripes {
            let ec_k = stripe.ec_k as usize;
            let ec_m = stripe.ec_m as usize;
            let stripe_ec_type =
                ErasureType::try_from(stripe.ec_type).unwrap_or(ErasureType::ErasureMds);
            let stripe_data_size = if stripe.data_size > 0 {
                stripe.data_size as usize
            } else {
                source_obj.size as usize
            };

            // Replication mode: read from first available replica
            if stripe_ec_type == ErasureType::ErasureReplication {
                let mut data_read = false;
                for shard_loc in &stripe.shards {
                    let node_placement = objectio_proto::metadata::NodePlacement {
                        position: shard_loc.position,
                        node_id: shard_loc.node_id.clone(),
                        node_address: String::new(),
                        disk_id: shard_loc.disk_id.clone(),
                        shard_type: shard_loc.shard_type,
                        local_group: shard_loc.local_group,
                    };

                    let ec_obj_id = if !stripe.object_id.is_empty() {
                        &stripe.object_id
                    } else {
                        &source_obj.object_id
                    };

                    match read_shard_from_osd(
                        &state.osd_pool,
                        &node_placement,
                        ec_obj_id,
                        stripe.stripe_id,
                        shard_loc.position,
                    )
                    .await
                    {
                        Ok(data) => {
                            let actual_data = if data.len() > stripe_data_size {
                                data[..stripe_data_size].to_vec()
                            } else {
                                data
                            };
                            source_data.extend(actual_data);
                            data_read = true;
                            break;
                        }
                        Err(e) => {
                            warn!("Failed to read replica: {}", e);
                        }
                    }
                }
                if !data_read {
                    return S3Error::xml_response(
                        "InternalError",
                        "Failed to read source object",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
            } else {
                // EC mode: decode
                let total_shards = ec_k + ec_m;
                let mut shards: Vec<Option<Vec<u8>>> = vec![None; total_shards];
                let mut read_count = 0;

                let shard_map: std::collections::HashMap<u32, &ShardLocation> =
                    stripe.shards.iter().map(|s| (s.position, s)).collect();

                let ec_obj_id = if !stripe.object_id.is_empty() {
                    &stripe.object_id
                } else {
                    &source_obj.object_id
                };

                for pos in 0..total_shards {
                    if read_count >= ec_k {
                        break;
                    }
                    if let Some(shard_loc) = shard_map.get(&(pos as u32)) {
                        let node_placement = objectio_proto::metadata::NodePlacement {
                            position: shard_loc.position,
                            node_id: shard_loc.node_id.clone(),
                            node_address: String::new(),
                            disk_id: shard_loc.disk_id.clone(),
                            shard_type: shard_loc.shard_type,
                            local_group: shard_loc.local_group,
                        };

                        if let Ok(data) = read_shard_from_osd(
                            &state.osd_pool,
                            &node_placement,
                            ec_obj_id,
                            stripe.stripe_id,
                            pos as u32,
                        )
                        .await
                        {
                            shards[pos] = Some(data);
                            read_count += 1;
                        }
                    }
                }

                if read_count < ec_k {
                    return S3Error::xml_response(
                        "InternalError",
                        "Insufficient shards to read source object",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }

                let codec = match ErasureCodec::new(ErasureConfig::new(ec_k as u8, ec_m as u8)) {
                    Ok(c) => c,
                    Err(e) => {
                        return S3Error::xml_response(
                            "InternalError",
                            &format!("Codec error: {}", e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                };

                match codec.decode(&mut shards, stripe_data_size) {
                    Ok(data) => source_data.extend(data),
                    Err(e) => {
                        return S3Error::xml_response(
                            "InternalError",
                            &format!("Decode error: {}", e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        );
                    }
                }
            }
        }

        (Bytes::from(source_data), true)
    } else {
        (body, false)
    };

    debug!(
        "PUT object: {}/{}, size={}, ec={}+{}, is_copy={}",
        bucket,
        key,
        body.len(),
        state.ec_k,
        state.ec_m,
        is_copy
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
        )
        .await
        {
            return deny_response;
        }
    }

    let mut meta_client = state.meta_client.clone();

    // Generate object ID and ETag
    let object_id = *Uuid::new_v4().as_bytes();
    let etag = format!("\"{:x}\"", md5::compute(&body));
    let original_size = body.len() as u64;

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
        let num_stripes = (body.len() + stripe_size - 1) / stripe_size;

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
                        shard_locs.push(ShardLocation {
                            position: pos,
                            node_id: placement_node.node_id.clone(),
                            disk_id: placement_node.disk_id.clone(),
                            offset: 0,
                            shard_type: placement_node.shard_type,
                            local_group: placement_node.local_group,
                        });
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
            version_id: String::new(),
            storage_class: "STANDARD".to_string(),
            is_delete_marker: false,
        };

        let primary_osd = &placement.nodes[0];
        if let Err(e) =
            put_object_meta_to_osd(&state.osd_pool, primary_osd, &bucket, &key, object_meta).await
        {
            error!("Failed to store object metadata on OSD: {}", e);
            return S3Error::xml_response(
                "InternalError",
                &format!("Failed to store object metadata: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }

        info!(
            "Created object (replication): {}/{}, size={}, stripes={}, replicas_written={}, is_copy={}",
            bucket, key, original_size, num_stripes, total_success, is_copy
        );

        // Return CopyObjectResult XML for copy operations, empty body for regular PUT
        return if is_copy {
            let copy_result = CopyObjectResult {
                etag: etag.clone(),
                last_modified: timestamp_to_iso(timestamp),
            };
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                to_xml(&copy_result).unwrap_or_default()
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .header("ETag", etag)
                .body(Body::from(xml))
                .unwrap()
        } else {
            Response::builder()
                .status(StatusCode::OK)
                .header("ETag", etag)
                .body(Body::empty())
                .unwrap()
        };
    }

    // EC mode: encode data with erasure coding
    // For large files, split into multiple stripes (each shard <= MAX_SHARD_SIZE)
    let total_shards = (ec_k + ec_m) as usize;

    // Calculate max stripe data size: each encoded shard must fit in MAX_SHARD_SIZE
    // shard_size = stripe_data_size / ec_k (approximately)
    // So max_stripe_data_size = MAX_SHARD_SIZE * ec_k
    let max_stripe_data_size = MAX_SHARD_SIZE * ec_k as usize;
    let num_stripes = (body.len() + max_stripe_data_size - 1) / max_stripe_data_size;

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
                let shard_size = (stripe_data.len() + ec_k as usize - 1) / ec_k as usize;
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
                    // Still add the location with placeholder data for tracking
                    shard_locs.push(ShardLocation {
                        position: pos,
                        node_id: placement_node.node_id.clone(),
                        disk_id: placement_node.disk_id.clone(),
                        offset: 0,
                        shard_type: placement_node.shard_type,
                        local_group: placement_node.local_group,
                    });
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
        version_id: String::new(),
        storage_class: "STANDARD".to_string(),
        is_delete_marker: false,
    };

    // Get primary OSD (position 0 in placement)
    let primary_osd = &placement.nodes[0];

    // Store metadata on primary OSD
    if let Err(e) =
        put_object_meta_to_osd(&state.osd_pool, primary_osd, &bucket, &key, object_meta).await
    {
        error!("Failed to store object metadata on OSD: {}", e);
        return S3Error::xml_response(
            "InternalError",
            &format!("Failed to store object metadata: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    // Note: Object metadata is stored on primary OSD only (no meta service index)
    // ListObjects uses scatter-gather to query all OSDs directly

    info!(
        "Created object: {}/{}, size={}, stripes={}, shards_written={}, primary_osd={}, is_copy={}",
        bucket,
        key,
        original_size,
        num_stripes,
        total_shards_written,
        primary_osd.node_address,
        is_copy
    );

    // Return CopyObjectResult XML for copy operations, empty body for regular PUT
    if is_copy {
        let copy_result = CopyObjectResult {
            etag: etag.clone(),
            last_modified: timestamp_to_iso(timestamp),
        };
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
            to_xml(&copy_result).unwrap_or_default()
        );
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/xml")
            .header("ETag", etag)
            .body(Body::from(xml))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::OK)
            .header("ETag", etag)
            .body(Body::empty())
            .unwrap()
    }
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

    // Determine which stripes to fetch (skip non-overlapping stripes for range requests)
    let stripe_plan: Vec<(usize, u64)> = if let Some(ref range) = resolved_range {
        overlapping_stripes(&object.stripes, total_size, range)
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
                let node_placement = objectio_proto::metadata::NodePlacement {
                    position: shard_loc.position,
                    node_id: shard_loc.node_id.clone(),
                    node_address: get_node_address_from_meta(&mut meta_client, &shard_loc.node_id)
                        .await
                        .unwrap_or_else(|| "http://localhost:9002".to_string()),
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
                        if let Some(ref range) = resolved_range {
                            let stripe_end = stripe_byte_offset + stripe_data_size as u64;
                            let slice_start =
                                range.start.saturating_sub(stripe_byte_offset) as usize;
                            let slice_end = std::cmp::min(range.end + 1, stripe_end)
                                .saturating_sub(stripe_byte_offset)
                                as usize;
                            all_data.extend_from_slice(&actual_data[slice_start..slice_end]);
                        } else {
                            all_data.extend(actual_data);
                        }
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

        // Try to read k data shards first (positions 0 to k-1)
        for pos in 0..ec_k {
            if read_count >= ec_k {
                break;
            }

            if let Some(shard_loc) = shard_map.get(&(pos as u32)) {
                let node_placement = objectio_proto::metadata::NodePlacement {
                    position: shard_loc.position,
                    node_id: shard_loc.node_id.clone(),
                    node_address: get_node_address_from_meta(&mut meta_client, &shard_loc.node_id)
                        .await
                        .unwrap_or_else(|| "http://localhost:9002".to_string()),
                    disk_id: shard_loc.disk_id.clone(),
                    shard_type: shard_loc.shard_type,
                    local_group: shard_loc.local_group,
                };

                match read_shard_from_osd(
                    &state.osd_pool,
                    &node_placement,
                    ec_shard_object_id,
                    stripe.stripe_id,
                    pos as u32,
                )
                .await
                {
                    Ok(data) => {
                        debug!("Read data shard {} ({} bytes)", pos, data.len());
                        shards[pos] = Some(data);
                        read_count += 1;
                    }
                    Err(e) => {
                        warn!("Failed to read shard {}: {}", pos, e);
                    }
                }
            }
        }

        // If we don't have enough data shards, try parity shards
        if read_count < ec_k {
            for pos in ec_k..total_shards {
                if read_count >= ec_k {
                    break;
                }

                if let Some(shard_loc) = shard_map.get(&(pos as u32)) {
                    let node_placement = objectio_proto::metadata::NodePlacement {
                        position: shard_loc.position,
                        node_id: shard_loc.node_id.clone(),
                        node_address: get_node_address_from_meta(
                            &mut meta_client,
                            &shard_loc.node_id,
                        )
                        .await
                        .unwrap_or_else(|| "http://localhost:9002".to_string()),
                        disk_id: shard_loc.disk_id.clone(),
                        shard_type: shard_loc.shard_type,
                        local_group: shard_loc.local_group,
                    };

                    match read_shard_from_osd(
                        &state.osd_pool,
                        &node_placement,
                        ec_shard_object_id,
                        stripe.stripe_id,
                        pos as u32,
                    )
                    .await
                    {
                        Ok(data) => {
                            debug!("Read parity shard {} ({} bytes)", pos, data.len());
                            shards[pos] = Some(data);
                            read_count += 1;
                        }
                        Err(e) => {
                            warn!("Failed to read parity shard {}: {}", pos, e);
                        }
                    }
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

        if let Some(ref range) = resolved_range {
            let stripe_end = stripe_byte_offset + stripe_data_size as u64;
            let slice_start = range.start.saturating_sub(stripe_byte_offset) as usize;
            let slice_end = std::cmp::min(range.end + 1, stripe_end)
                .saturating_sub(stripe_byte_offset) as usize;
            all_data.extend_from_slice(&stripe_data[slice_start..slice_end]);
        } else {
            all_data.extend(stripe_data);
        }
    }

    info!(
        "Read object: {}/{}, size={}, stripes_fetched={}/{}",
        bucket,
        key,
        all_data.len(),
        stripe_plan.len(),
        object.stripes.len()
    );

    // Build response — range requests already have sliced data
    if let Some(ref range) = resolved_range {
        let content_range = format!("bytes {}-{}/{total_size}", range.start, range.end);

        let builder = Response::builder()
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

        let builder = add_metadata_headers(builder, &object.user_metadata);

        builder.body(Body::from(all_data)).unwrap()
    } else {
        let builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, &object.content_type)
            .header(header::CONTENT_LENGTH, all_data.len().to_string())
            .header("ETag", &object.etag)
            .header("Accept-Ranges", "bytes")
            .header(
                header::LAST_MODIFIED,
                timestamp_to_http_date(object.modified_at),
            );

        let builder = add_metadata_headers(builder, &object.user_metadata);

        builder.body(Body::from(all_data)).unwrap()
    }
}

/// Helper to get node address from metadata service
async fn get_node_address_from_meta(
    _meta_client: &mut MetadataServiceClient<Channel>,
    _node_id: &[u8],
) -> Option<String> {
    // TODO: Query metadata service for node address
    // For now, return None and let caller use default
    None
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
            let builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, &obj.content_type)
                .header(header::CONTENT_LENGTH, obj.size.to_string())
                .header("ETag", &obj.etag)
                .header(
                    header::LAST_MODIFIED,
                    timestamp_to_http_date(obj.modified_at),
                );

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
            // S3 returns 204 even for non-existent objects
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

    // Delete object metadata from primary OSD
    let primary_osd = &placement.nodes[0];
    if let Err(e) = delete_object_meta_from_osd(&state.osd_pool, primary_osd, &bucket, &key).await {
        warn!("Failed to delete object metadata from OSD: {}", e);
        // Continue anyway - might not exist
    }

    // Note: Object metadata is stored on primary OSD only (no meta service index)
    // ListObjects uses scatter-gather to query all OSDs directly

    info!("Deleted object: {}/{}", bucket, key);
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
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
                S3Error::xml_response("MalformedPolicy", &e.message(), StatusCode::BAD_REQUEST)
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
    body: Bytes,
) -> Response {
    if params.uploads.is_some() {
        // Initiate multipart upload
        initiate_multipart_upload_internal(state, bucket, key).await
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
) -> Response {
    let mut client = state.meta_client.clone();

    match client
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            content_type: String::new(),
            user_metadata: HashMap::new(),
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
        return upload_part_internal(state, bucket, key, upload_id, part_number, body).await;
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

    // Calculate ETag for this part
    let etag = format!("\"{:x}\"", md5::compute(&body));
    let part_size = body.len() as u64;

    let mut meta_client = state.meta_client.clone();

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
        let num_stripes = (body.len() + stripe_size - 1) / stripe_size;

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
            let stripe_data = &body[stripe_start..stripe_end];
            let stripe_data_size = stripe_data.len() as u64;

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
        let num_stripes = (body.len() + max_stripe_data_size - 1) / max_stripe_data_size;

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
            let stripe_data = &body[stripe_start..stripe_end];
            let stripe_data_size = stripe_data.len() as u64;

            let shards: Vec<Vec<u8>> = match codec.encode(stripe_data) {
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

            Response::builder()
                .status(StatusCode::OK)
                .header("ETag", &etag)
                .body(Body::empty())
                .unwrap()
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

                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/xml")
                    .header("ETag", &object.etag)
                    .body(Body::from(xml))
                    .unwrap()
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
) -> Response {
    // If uploadId is present, this is an abort multipart upload request
    if let Some(upload_id) = params.upload_id {
        return abort_multipart_upload_internal(state, bucket, key, upload_id).await;
    }

    // Otherwise, it's a regular DELETE object
    delete_object(State(state), Path((bucket, key)), auth).await
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

/// Check if request is from admin user, returns error response if not
fn check_admin_access(auth: Option<Extension<AuthResult>>) -> Result<(), Response> {
    match auth {
        Some(Extension(ref auth_result)) => {
            if is_admin_user(auth_result) {
                Ok(())
            } else {
                warn!("Admin API access denied for user: {}", auth_result.user_arn);
                Err(admin_forbidden_response())
            }
        }
        None => {
            warn!("Admin API access attempted without authentication");
            Err(admin_auth_required_response())
        }
    }
}

/// List users (GET /_admin/users)
pub async fn admin_list_users(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
) -> Response {
    // Check if user is admin
    if let Err(response) = check_admin_access(auth) {
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
                    .map(|u| AdminUserResponse {
                        user_id: u.user_id,
                        display_name: u.display_name,
                        arn: u.arn,
                        status: format!("{:?}", u.status),
                        created_at: u.created_at,
                        email: u.email,
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
    body: Bytes,
) -> Response {
    // Check if user is admin
    if let Err(response) = check_admin_access(auth) {
        return response;
    }

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

    let mut client = state.meta_client.clone();

    match client
        .create_user(CreateUserRequest {
            display_name: params.display_name,
            email: params.email,
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

/// Delete user (DELETE /_admin/users/{user_id})
pub async fn admin_delete_user(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthResult>>,
    Path(user_id): Path<String>,
) -> Response {
    // Check if user is admin
    if let Err(response) = check_admin_access(auth) {
        return response;
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
    Path(user_id): Path<String>,
) -> Response {
    // Check if user is admin
    if let Err(response) = check_admin_access(auth) {
        return response;
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
    Path(user_id): Path<String>,
) -> Response {
    // Check if user is admin
    if let Err(response) = check_admin_access(auth) {
        return response;
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
    Path(access_key_id): Path<String>,
) -> Response {
    // Check if user is admin
    if let Err(response) = check_admin_access(auth) {
        return response;
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
