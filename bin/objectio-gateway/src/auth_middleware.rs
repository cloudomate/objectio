//! Authentication middleware for the S3 gateway
//!
//! This module provides axum middleware for AWS Signature V4 and V2 authentication.
//! Credentials are fetched from the metadata service for persistence.

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use objectio_auth::AuthResult;
use objectio_proto::metadata::{
    metadata_service_client::MetadataServiceClient, GetAccessKeyForAuthRequest,
};
use parking_lot::RwLock;
use regex::Regex;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, warn};

type HmacSha256 = Hmac<Sha256>;
type HmacSha1 = Hmac<Sha1>;

/// Cached credential for SigV4 verification
#[derive(Clone)]
pub struct CachedCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub user_id: String,
    pub user_arn: String,
    pub cached_at: std::time::Instant,
}

/// Authentication state shared across requests
pub struct AuthState {
    /// Metadata service client for credential lookup
    pub meta_client: MetadataServiceClient<Channel>,
    /// Credential cache (access_key_id -> credential)
    pub credential_cache: RwLock<HashMap<String, CachedCredential>>,
    /// Cache TTL in seconds
    pub cache_ttl_secs: u64,
    /// AWS region for SigV4 verification
    pub region: String,
}

impl AuthState {
    /// Create a new auth state
    pub fn new(meta_client: MetadataServiceClient<Channel>, region: impl Into<String>) -> Self {
        Self {
            meta_client,
            credential_cache: RwLock::new(HashMap::new()),
            cache_ttl_secs: 300, // 5 minutes
            region: region.into(),
        }
    }

    /// Look up credentials from cache or metadata service
    pub async fn lookup_credential(&self, access_key_id: &str) -> Result<CachedCredential, AuthError> {
        // Check cache first
        {
            let cache = self.credential_cache.read();
            if let Some(cred) = cache.get(access_key_id) {
                if cred.cached_at.elapsed().as_secs() < self.cache_ttl_secs {
                    return Ok(cred.clone());
                }
            }
        }

        // Fetch from metadata service
        let mut client = self.meta_client.clone();
        let response = client
            .get_access_key_for_auth(GetAccessKeyForAuthRequest {
                access_key_id: access_key_id.to_string(),
            })
            .await
            .map_err(|e| {
                warn!("Failed to fetch credential from metadata service: {}", e);
                AuthError::AccessDenied(format!("credential lookup failed: {}", e))
            })?;

        let inner = response.into_inner();
        let access_key = inner.access_key.ok_or_else(|| {
            AuthError::AccessDenied("access key not found".to_string())
        })?;
        let user = inner.user.ok_or_else(|| {
            AuthError::AccessDenied("user not found".to_string())
        })?;

        let cred = CachedCredential {
            access_key_id: access_key.access_key_id.clone(),
            secret_access_key: access_key.secret_access_key,
            user_id: user.user_id.clone(),
            user_arn: user.arn,
            cached_at: std::time::Instant::now(),
        };

        // Update cache
        self.credential_cache.write().insert(access_key.access_key_id, cred.clone());

        Ok(cred)
    }
}

/// Authentication middleware layer
pub async fn auth_layer(
    State(auth_state): State<Arc<AuthState>>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AuthError> {
    let path = request.uri().path();

    // Skip auth for health checks and metrics
    if path == "/health" || path == "/metrics" || path == "/_status" {
        return Ok(next.run(request).await);
    }

    // Parse authorization header to get access key ID
    let auth_header = request
        .headers()
        .get("authorization")
        .ok_or(AuthError::AccessDenied("missing authorization header".to_string()))?
        .to_str()
        .map_err(|_| AuthError::AccessDenied("invalid authorization header".to_string()))?;

    let parsed = parse_authorization_header(auth_header)?;

    // Fetch credentials from metadata service
    let cred = auth_state.lookup_credential(parsed.access_key_id()).await?;

    // Verify the signature based on auth version
    let auth_result = match &parsed {
        ParsedAuth::V4 { signed_headers, signature, .. } => {
            verify_request_v4(&request, signed_headers, signature, &cred, &auth_state.region)?
        }
        ParsedAuth::V2 { signature, .. } => {
            verify_request_v2(&request, signature, &cred)?
        }
    };

    debug!(
        "Authenticated user: {} (key: {})",
        auth_result.user_id, auth_result.access_key_id
    );

    // Store auth result in request extensions for handlers to access
    request.extensions_mut().insert(auth_result);
    Ok(next.run(request).await)
}

/// Parsed authorization header (supports both V4 and V2)
enum ParsedAuth {
    V4 {
        access_key_id: String,
        signed_headers: Vec<String>,
        signature: String,
    },
    V2 {
        access_key_id: String,
        signature: String,
    },
}

impl ParsedAuth {
    fn access_key_id(&self) -> &str {
        match self {
            ParsedAuth::V4 { access_key_id, .. } => access_key_id,
            ParsedAuth::V2 { access_key_id, .. } => access_key_id,
        }
    }
}

/// Parse the Authorization header (supports both SigV4 and SigV2)
fn parse_authorization_header(header: &str) -> Result<ParsedAuth, AuthError> {
    if header.starts_with("AWS4-HMAC-SHA256") {
        // SigV4 format: AWS4-HMAC-SHA256 Credential=AKID/date/region/service/aws4_request,
        //               SignedHeaders=host;x-amz-date, Signature=xxx
        let re = Regex::new(
            r"AWS4-HMAC-SHA256\s+Credential=([^/]+)/[^,]+,\s*SignedHeaders=([^,]+),\s*Signature=(\w+)"
        ).unwrap();

        let captures = re.captures(header).ok_or_else(|| {
            AuthError::AccessDenied("invalid authorization header format".to_string())
        })?;

        Ok(ParsedAuth::V4 {
            access_key_id: captures.get(1).unwrap().as_str().to_string(),
            signed_headers: captures
                .get(2)
                .unwrap()
                .as_str()
                .split(';')
                .map(|s| s.to_lowercase())
                .collect(),
            signature: captures.get(3).unwrap().as_str().to_string(),
        })
    } else if header.starts_with("AWS ") {
        // SigV2 format: AWS AccessKeyId:Signature
        let credentials = &header[4..]; // Skip "AWS "
        let parts: Vec<&str> = credentials.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(AuthError::AccessDenied("invalid SigV2 authorization header".to_string()));
        }

        debug!("Using SigV2 authentication (legacy)");
        Ok(ParsedAuth::V2 {
            access_key_id: parts[0].to_string(),
            signature: parts[1].to_string(),
        })
    } else {
        Err(AuthError::AccessDenied("unsupported signature version".to_string()))
    }
}

/// Verify SigV4 request signature
fn verify_request_v4<B>(
    request: &Request<B>,
    signed_headers: &[String],
    signature: &str,
    cred: &CachedCredential,
    region: &str,
) -> Result<AuthResult, AuthError> {
    // Get the request date
    let date_str = get_request_date(request)?;
    let date = parse_date_v4(&date_str)?;

    // Check if request is not too old (allow 15 minutes)
    let now = Utc::now();
    let diff = now.signed_duration_since(date);
    if diff.num_minutes().abs() > 15 {
        return Err(AuthError::RequestTimeTooSkewed);
    }

    // Build canonical request
    let canonical_request = build_canonical_request(request, signed_headers)?;

    // Build string to sign
    let date_stamp = date.format("%Y%m%d").to_string();
    let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, region);
    let string_to_sign = build_string_to_sign(&canonical_request, &date_str, &credential_scope);

    // Calculate signature
    let signing_key = derive_signing_key(&cred.secret_access_key, &date_stamp, region, "s3");
    let calculated_signature = calculate_signature_v4(&signing_key, &string_to_sign);

    // Compare signatures using constant-time comparison
    if !constant_time_eq(&calculated_signature, signature) {
        debug!(
            "SigV4 mismatch:\n  Canonical Request:\n{}\n  String to Sign:\n{}\n  Calculated: {}\n  Provided: {}",
            canonical_request, string_to_sign, calculated_signature, signature
        );
        return Err(AuthError::SignatureDoesNotMatch);
    }

    Ok(AuthResult {
        user_id: cred.user_id.clone(),
        user_arn: cred.user_arn.clone(),
        access_key_id: cred.access_key_id.clone(),
    })
}

/// Sub-resources that should be included in the canonical resource for SigV2
const SIGV2_SUB_RESOURCES: &[&str] = &[
    "acl", "cors", "delete", "lifecycle", "location", "logging", "notification",
    "partNumber", "policy", "requestPayment", "response-cache-control",
    "response-content-disposition", "response-content-encoding", "response-content-language",
    "response-content-type", "response-expires", "restore", "tagging", "torrent",
    "uploadId", "uploads", "versionId", "versioning", "versions", "website",
];

/// Verify SigV2 request signature
fn verify_request_v2<B>(
    request: &Request<B>,
    signature: &str,
    cred: &CachedCredential,
) -> Result<AuthResult, AuthError> {
    // Get the request date
    let date_str = get_request_date(request)?;

    // Try to parse and validate date
    if let Ok(date) = parse_date_v2(&date_str) {
        let now = Utc::now();
        let diff = now.signed_duration_since(date);
        if diff.num_minutes().abs() > 15 {
            return Err(AuthError::RequestTimeTooSkewed);
        }
    }

    // Build string to sign for SigV2
    let string_to_sign = build_string_to_sign_v2(request, &date_str)?;

    // Calculate signature using HMAC-SHA1
    let calculated_signature = calculate_signature_v2(&cred.secret_access_key, &string_to_sign);

    // Compare signatures
    if !constant_time_eq(&calculated_signature, signature) {
        debug!(
            "SigV2 mismatch:\n  String to Sign:\n{}\n  Calculated: {}\n  Provided: {}",
            string_to_sign, calculated_signature, signature
        );
        return Err(AuthError::SignatureDoesNotMatch);
    }

    Ok(AuthResult {
        user_id: cred.user_id.clone(),
        user_arn: cred.user_arn.clone(),
        access_key_id: cred.access_key_id.clone(),
    })
}

/// Build string to sign for SigV2
fn build_string_to_sign_v2<B>(request: &Request<B>, date_str: &str) -> Result<String, AuthError> {
    let method = request.method().as_str();

    // Get Content-MD5 header (empty if not present)
    let content_md5 = request
        .headers()
        .get("content-md5")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Get Content-Type header (empty if not present)
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Use x-amz-date if present, otherwise use Date header value
    let date_field = if request.headers().contains_key("x-amz-date") {
        ""
    } else {
        date_str
    };

    // Build canonicalized AMZ headers
    let canonicalized_amz_headers = build_canonicalized_amz_headers(request);

    // Build canonicalized resource
    let canonicalized_resource = build_canonicalized_resource_v2(request);

    Ok(format!(
        "{}\n{}\n{}\n{}\n{}{}",
        method,
        content_md5,
        content_type,
        date_field,
        canonicalized_amz_headers,
        canonicalized_resource
    ))
}

/// Build canonicalized AMZ headers for SigV2
fn build_canonicalized_amz_headers<B>(request: &Request<B>) -> String {
    let mut amz_headers: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (name, value) in request.headers().iter() {
        let name_lower = name.as_str().to_lowercase();
        if name_lower.starts_with("x-amz-") {
            if let Ok(value_str) = value.to_str() {
                let trimmed = value_str.split_whitespace().collect::<Vec<_>>().join(" ");
                amz_headers.entry(name_lower).or_default().push(trimmed);
            }
        }
    }

    let mut result = String::new();
    for (name, values) in amz_headers {
        result.push_str(&format!("{}:{}\n", name, values.join(",")));
    }
    result
}

/// Build canonicalized resource for SigV2
fn build_canonicalized_resource_v2<B>(request: &Request<B>) -> String {
    let uri = request.uri();
    let path = uri.path();

    let mut resource = if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    };

    // Add sub-resources if present in query string
    if let Some(query) = uri.query() {
        let mut sub_resources: Vec<(String, Option<String>)> = Vec::new();

        for param in query.split('&') {
            let mut parts = param.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next();

            if SIGV2_SUB_RESOURCES.contains(&key) {
                sub_resources.push((key.to_string(), value.map(|s| s.to_string())));
            }
        }

        if !sub_resources.is_empty() {
            sub_resources.sort_by(|a, b| a.0.cmp(&b.0));

            let sub_resource_str: Vec<String> = sub_resources
                .into_iter()
                .map(|(k, v)| {
                    if let Some(val) = v {
                        format!("{}={}", k, val)
                    } else {
                        k
                    }
                })
                .collect();

            resource.push('?');
            resource.push_str(&sub_resource_str.join("&"));
        }
    }

    resource
}

/// Calculate SigV2 signature using HMAC-SHA1
fn calculate_signature_v2(secret_key: &str, string_to_sign: &str) -> String {
    let mut mac = HmacSha1::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(string_to_sign.as_bytes());
    let result = mac.finalize().into_bytes();
    BASE64.encode(result)
}

/// Parse date for SigV2 (supports multiple formats)
fn parse_date_v2(date_str: &str) -> Result<DateTime<Utc>, AuthError> {
    // Try RFC 2822 format first
    if let Ok(dt) = DateTime::parse_from_rfc2822(date_str) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try ISO 8601 format
    if let Ok(dt) = NaiveDateTime::parse_from_str(date_str, "%Y%m%dT%H%M%SZ") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }

    // Try common HTTP date format
    if let Ok(dt) = NaiveDateTime::parse_from_str(date_str, "%a, %d %b %Y %H:%M:%S GMT") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }

    Err(AuthError::AccessDenied("invalid date format".to_string()))
}

/// Get the request date from headers
fn get_request_date<B>(request: &Request<B>) -> Result<String, AuthError> {
    if let Some(date) = request.headers().get("x-amz-date") {
        return date
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| AuthError::AccessDenied("invalid date format".to_string()));
    }
    if let Some(date) = request.headers().get("date") {
        return date
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| AuthError::AccessDenied("invalid date format".to_string()));
    }
    Err(AuthError::AccessDenied("missing date header".to_string()))
}

/// Parse ISO8601 date format for SigV4
fn parse_date_v4(date_str: &str) -> Result<DateTime<Utc>, AuthError> {
    NaiveDateTime::parse_from_str(date_str, "%Y%m%dT%H%M%SZ")
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        .map_err(|_| AuthError::AccessDenied("invalid date format".to_string()))
}

/// Build the canonical request string
fn build_canonical_request<B>(
    request: &Request<B>,
    signed_headers: &[String],
) -> Result<String, AuthError> {
    let method = request.method().as_str();
    let uri = request.uri();
    let path = uri.path();

    let canonical_uri = if path.is_empty() { "/" } else { path };
    let canonical_query = build_canonical_query_string(uri.query().unwrap_or(""));

    let mut headers_map: BTreeMap<String, String> = BTreeMap::new();
    for header_name in signed_headers {
        let value = request
            .headers()
            .get(header_name.as_str())
            .ok_or_else(|| AuthError::AccessDenied(format!("missing signed header: {}", header_name)))?
            .to_str()
            .map_err(|_| AuthError::AccessDenied("invalid header value".to_string()))?
            .trim()
            .to_string();
        headers_map.insert(header_name.clone(), value);
    }

    let canonical_headers: String = headers_map
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v))
        .collect();

    let signed_headers_str = signed_headers.join(";");

    let payload_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD")
        .to_string();

    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers_str, payload_hash
    ))
}

/// Build canonical query string
fn build_canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter_map(|param| {
            let mut parts = param.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            let decoded_key = url_decode(key);
            let decoded_value = url_decode(value);
            Some((url_encode(&decoded_key), url_encode(&decoded_value)))
        })
        .collect();

    params.sort_by(|a, b| a.0.cmp(&b.0));

    params
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// Build the string to sign
fn build_string_to_sign(canonical_request: &str, date_str: &str, credential_scope: &str) -> String {
    let canonical_request_hash = hex_sha256(canonical_request.as_bytes());
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date_str, credential_scope, canonical_request_hash
    )
}

/// Derive the signing key
fn derive_signing_key(secret_key: &str, date_stamp: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Calculate the SigV4 signature
fn calculate_signature_v4(signing_key: &[u8], string_to_sign: &str) -> String {
    hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

/// Calculate HMAC-SHA256
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Calculate SHA256 and return hex string
fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// URL encode a string (AWS style)
fn url_encode(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

/// URL decode a string
fn url_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            result.push('%');
            result.push_str(&hex);
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}

/// Constant-time string comparison to prevent timing attacks
fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result: u8 = 0;
    for (x, y) in a.bytes().zip(b.bytes()) {
        result |= x ^ y;
    }
    result == 0
}

/// Authentication error response
#[derive(Debug)]
pub enum AuthError {
    /// Missing or invalid credentials
    AccessDenied(String),
    /// Signature does not match
    SignatureDoesNotMatch,
    /// Request has expired
    RequestTimeTooSkewed,
    /// Internal error
    InternalError,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_code, message) = match self {
            AuthError::AccessDenied(msg) => (StatusCode::FORBIDDEN, "AccessDenied", msg),
            AuthError::SignatureDoesNotMatch => (
                StatusCode::FORBIDDEN,
                "SignatureDoesNotMatch",
                "The request signature we calculated does not match the signature you provided.".to_string(),
            ),
            AuthError::RequestTimeTooSkewed => (
                StatusCode::FORBIDDEN,
                "RequestTimeTooSkewed",
                "The difference between the request time and the server's time is too large.".to_string(),
            ),
            AuthError::InternalError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "We encountered an internal error. Please try again.".to_string(),
            ),
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
    <RequestId>unknown</RequestId>
</Error>"#,
            error_code, message
        );

        Response::builder()
            .status(status)
            .header("Content-Type", "application/xml")
            .body(Body::from(xml))
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap()
            })
    }
}

/// Extension trait for getting auth result from request
pub trait AuthExt {
    /// Get the authenticated user's result from request extensions
    fn auth_result(&self) -> Option<&AuthResult>;
}

impl<B> AuthExt for Request<B> {
    fn auth_result(&self) -> Option<&AuthResult> {
        self.extensions().get::<AuthResult>()
    }
}
