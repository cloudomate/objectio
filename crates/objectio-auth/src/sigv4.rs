//! AWS Signature V4 verification
//!
//! Implements AWS Signature Version 4 for authenticating S3 API requests.
//! Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html

use crate::error::AuthError;
use crate::store::UserStore;
use crate::user::AuthResult;
use chrono::{DateTime, NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use http::Request;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

/// AWS Signature V4 verifier
pub struct SigV4Verifier {
    user_store: Arc<UserStore>,
    /// Service name (usually "s3")
    service: String,
    /// AWS region (e.g., "us-east-1")
    region: String,
}

impl SigV4Verifier {
    /// Create a new SigV4 verifier
    pub fn new(user_store: Arc<UserStore>, region: impl Into<String>) -> Self {
        Self {
            user_store,
            service: "s3".to_string(),
            region: region.into(),
        }
    }

    /// Verify an incoming HTTP request
    pub fn verify<B>(&self, request: &Request<B>) -> Result<AuthResult, AuthError> {
        // Check for Authorization header
        let auth_header = request
            .headers()
            .get("authorization")
            .ok_or(AuthError::MissingAuthHeader)?
            .to_str()
            .map_err(|_| AuthError::InvalidAuthHeader)?;

        // Parse Authorization header
        let parsed = self.parse_authorization_header(auth_header)?;

        // Get the request date
        let date_str = self.get_request_date(request)?;
        let date = self.parse_date(&date_str)?;

        // Check if request is not too old (allow 15 minutes)
        let now = Utc::now();
        let diff = now.signed_duration_since(date);
        if diff.num_minutes().abs() > 15 {
            return Err(AuthError::RequestExpired);
        }

        // Look up the access key and user
        let (access_key, user) = self.user_store.lookup_for_auth(&parsed.access_key_id)?;

        // Build canonical request
        let canonical_request = self.build_canonical_request(request, &parsed.signed_headers)?;

        // Build string to sign
        let date_stamp = date.format("%Y%m%d").to_string();
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            date_stamp, self.region, self.service
        );
        let string_to_sign =
            self.build_string_to_sign(&canonical_request, &date_str, &credential_scope);

        // Calculate signature
        let signing_key = self.derive_signing_key(
            &access_key.secret_access_key,
            &date_stamp,
            &self.region,
            &self.service,
        );
        let calculated_signature = self.calculate_signature(&signing_key, &string_to_sign);

        // Compare signatures using constant-time comparison
        if !constant_time_eq(&calculated_signature, &parsed.signature) {
            tracing::debug!(
                "Signature mismatch:\n  Canonical Request:\n{}\n  String to Sign:\n{}\n  Calculated: {}\n  Provided: {}",
                canonical_request,
                string_to_sign,
                calculated_signature,
                parsed.signature
            );
            return Err(AuthError::SignatureMismatch);
        }

        Ok(AuthResult {
            user_id: user.user_id,
            user_arn: user.arn,
            access_key_id: access_key.access_key_id,
        })
    }

    /// Parse the Authorization header
    fn parse_authorization_header(&self, header: &str) -> Result<ParsedAuth, AuthError> {
        // Format: AWS4-HMAC-SHA256 Credential=AKID/date/region/service/aws4_request,
        //         SignedHeaders=host;x-amz-date, Signature=xxx

        if !header.starts_with("AWS4-HMAC-SHA256") {
            return Err(AuthError::InvalidSignatureVersion);
        }

        let re = Regex::new(
            r"AWS4-HMAC-SHA256\s+Credential=([^/]+)/[^,]+,\s*SignedHeaders=([^,]+),\s*Signature=(\w+)"
        ).unwrap();

        let captures = re.captures(header).ok_or(AuthError::InvalidAuthHeader)?;

        Ok(ParsedAuth {
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
    }

    /// Get the request date from headers
    fn get_request_date<B>(&self, request: &Request<B>) -> Result<String, AuthError> {
        // Try x-amz-date first, then Date header
        if let Some(date) = request.headers().get("x-amz-date") {
            return date
                .to_str()
                .map(|s| s.to_string())
                .map_err(|_| AuthError::InvalidDateFormat);
        }

        if let Some(date) = request.headers().get("date") {
            return date
                .to_str()
                .map(|s| s.to_string())
                .map_err(|_| AuthError::InvalidDateFormat);
        }

        Err(AuthError::MissingDateHeader)
    }

    /// Parse ISO8601 date format
    fn parse_date(&self, date_str: &str) -> Result<DateTime<Utc>, AuthError> {
        // Format: 20130524T000000Z
        NaiveDateTime::parse_from_str(date_str, "%Y%m%dT%H%M%SZ")
            .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
            .map_err(|_| AuthError::InvalidDateFormat)
    }

    /// Build the canonical request string
    fn build_canonical_request<B>(
        &self,
        request: &Request<B>,
        signed_headers: &[String],
    ) -> Result<String, AuthError> {
        let method = request.method().as_str();
        let uri = request.uri();
        let path = uri.path();

        // Canonical URI (URL-encoded path)
        let canonical_uri = if path.is_empty() { "/" } else { path };

        // Canonical query string (sorted by parameter name)
        let canonical_query = self.build_canonical_query_string(uri.query().unwrap_or(""));

        // Canonical headers (lowercase, sorted, trimmed values)
        let mut headers_map: BTreeMap<String, String> = BTreeMap::new();
        for header_name in signed_headers {
            let value = request
                .headers()
                .get(header_name.as_str())
                .ok_or_else(|| AuthError::MissingSignedHeader(header_name.clone()))?
                .to_str()
                .map_err(|_| AuthError::InvalidAuthHeader)?
                .trim()
                .to_string();
            headers_map.insert(header_name.clone(), value);
        }

        let canonical_headers: String = headers_map
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v))
            .collect();

        let signed_headers_str = signed_headers.join(";");

        // Payload hash (from x-amz-content-sha256 header or UNSIGNED-PAYLOAD)
        let payload_hash = request
            .headers()
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD")
            .to_string();

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers_str,
            payload_hash
        );

        Ok(canonical_request)
    }

    /// Build canonical query string (sorted parameters)
    ///
    /// The incoming query string is already URL-encoded from the HTTP request.
    /// We need to decode it first, then re-encode using AWS's URI encoding rules.
    fn build_canonical_query_string(&self, query: &str) -> String {
        if query.is_empty() {
            return String::new();
        }

        let mut params: Vec<(String, String)> = query
            .split('&')
            .filter_map(|param| {
                let mut parts = param.splitn(2, '=');
                let key = parts.next()?;
                let value = parts.next().unwrap_or("");
                // Decode first (in case already encoded), then re-encode
                let decoded_key = url_decode(key);
                let decoded_value = url_decode(value);
                Some((
                    url_encode(&decoded_key),
                    url_encode(&decoded_value),
                ))
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
    fn build_string_to_sign(
        &self,
        canonical_request: &str,
        date_str: &str,
        credential_scope: &str,
    ) -> String {
        let canonical_request_hash = hex_sha256(canonical_request.as_bytes());

        format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            date_str, credential_scope, canonical_request_hash
        )
    }

    /// Derive the signing key
    fn derive_signing_key(
        &self,
        secret_key: &str,
        date_stamp: &str,
        region: &str,
        service: &str,
    ) -> Vec<u8> {
        let k_secret = format!("AWS4{}", secret_key);
        let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service.as_bytes());
        hmac_sha256(&k_service, b"aws4_request")
    }

    /// Calculate the signature
    fn calculate_signature(&self, signing_key: &[u8], string_to_sign: &str) -> String {
        hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
    }
}

/// Parsed authorization header
struct ParsedAuth {
    access_key_id: String,
    signed_headers: Vec<String>,
    signature: String,
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
            // Try to parse the next two chars as hex
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            // If parsing failed, just keep the original
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("hello"), "hello");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a/b"), "a%2Fb");
    }

    #[test]
    fn test_hex_sha256() {
        let hash = hex_sha256(b"hello");
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq("hello", "hello"));
        assert!(!constant_time_eq("hello", "world"));
        assert!(!constant_time_eq("hello", "hello!"));
    }

    #[test]
    fn test_derive_signing_key() {
        let verifier = SigV4Verifier::new(Arc::new(UserStore::new()), "us-east-1");
        let key = verifier.derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        // The key should be 32 bytes (SHA256 output)
        assert_eq!(key.len(), 32);
    }
}
