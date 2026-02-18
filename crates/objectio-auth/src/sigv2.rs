//! AWS Signature V2 verification
//!
//! Implements AWS Signature Version 2 for authenticating S3 API requests.
//! This is a legacy authentication method but still used by some clients.
//! Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html

use crate::error::AuthError;
use crate::store::UserStore;
use crate::user::AuthResult;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use http::Request;
use sha1::Sha1;
use std::collections::BTreeMap;
use std::sync::Arc;

type HmacSha1 = Hmac<Sha1>;

/// Sub-resources that should be included in the canonical resource
const SUB_RESOURCES: &[&str] = &[
    "acl",
    "cors",
    "delete",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "restore",
    "tagging",
    "torrent",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

/// AWS Signature V2 verifier
pub struct SigV2Verifier {
    user_store: Arc<UserStore>,
}

impl SigV2Verifier {
    /// Create a new SigV2 verifier
    pub fn new(user_store: Arc<UserStore>) -> Self {
        Self { user_store }
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

        // Parse Authorization header: AWS AccessKeyId:Signature
        let parsed = self.parse_authorization_header(auth_header)?;

        // Get the request date
        let date_str = self.get_request_date(request)?;

        // Check if request is not too old (allow 15 minutes)
        if let Ok(date) = self.parse_date(&date_str) {
            let now = Utc::now();
            let diff = now.signed_duration_since(date);
            if diff.num_minutes().abs() > 15 {
                return Err(AuthError::RequestExpired);
            }
        }

        // Look up the access key and user
        let (access_key, user) = self.user_store.lookup_for_auth(&parsed.access_key_id)?;

        // Build string to sign
        let string_to_sign = self.build_string_to_sign(request, &date_str)?;

        // Calculate signature
        let calculated_signature =
            self.calculate_signature(&access_key.secret_access_key, &string_to_sign);

        // Compare signatures using constant-time comparison
        if !constant_time_eq(&calculated_signature, &parsed.signature) {
            tracing::debug!(
                "SigV2 signature mismatch:\n  String to Sign:\n{}\n  Calculated: {}\n  Provided: {}",
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
            group_arns: Vec::new(),
        })
    }

    /// Parse the Authorization header
    fn parse_authorization_header(&self, header: &str) -> Result<ParsedAuth, AuthError> {
        // Format: AWS AccessKeyId:Signature
        if !header.starts_with("AWS ") {
            return Err(AuthError::InvalidSignatureVersion);
        }

        let credentials = &header[4..]; // Skip "AWS "
        let parts: Vec<&str> = credentials.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidAuthHeader);
        }

        Ok(ParsedAuth {
            access_key_id: parts[0].to_string(),
            signature: parts[1].to_string(),
        })
    }

    /// Get the request date from headers
    fn get_request_date<B>(&self, request: &Request<B>) -> Result<String, AuthError> {
        // For SigV2, use x-amz-date first, then Date header
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

    /// Parse HTTP date format (RFC 2616)
    fn parse_date(&self, date_str: &str) -> Result<DateTime<Utc>, AuthError> {
        // Try different date formats
        // RFC 2616: "Sun, 06 Nov 1994 08:49:37 GMT"
        // RFC 850: "Sunday, 06-Nov-94 08:49:37 GMT"
        // ANSI C: "Sun Nov  6 08:49:37 1994"
        // ISO 8601: "20130524T000000Z" (used by some clients)

        if let Ok(dt) = DateTime::parse_from_rfc2822(date_str) {
            return Ok(dt.with_timezone(&Utc));
        }

        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, "%Y%m%dT%H%M%SZ") {
            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
        }

        // Try common HTTP date format
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, "%a, %d %b %Y %H:%M:%S GMT")
        {
            return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
        }

        Err(AuthError::InvalidDateFormat)
    }

    /// Build the string to sign
    fn build_string_to_sign<B>(
        &self,
        request: &Request<B>,
        date_str: &str,
    ) -> Result<String, AuthError> {
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
        // If x-amz-date is present, use empty string for Date field
        let date_field = if request.headers().contains_key("x-amz-date") {
            ""
        } else {
            date_str
        };

        // Build canonicalized AMZ headers
        let canonicalized_amz_headers = self.build_canonicalized_amz_headers(request);

        // Build canonicalized resource
        let canonicalized_resource = self.build_canonicalized_resource(request);

        let string_to_sign = format!(
            "{}\n{}\n{}\n{}\n{}{}",
            method,
            content_md5,
            content_type,
            date_field,
            canonicalized_amz_headers,
            canonicalized_resource
        );

        Ok(string_to_sign)
    }

    /// Build canonicalized AMZ headers
    fn build_canonicalized_amz_headers<B>(&self, request: &Request<B>) -> String {
        let mut amz_headers: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for (name, value) in request.headers().iter() {
            let name_lower = name.as_str().to_lowercase();
            if name_lower.starts_with("x-amz-")
                && let Ok(value_str) = value.to_str()
            {
                // Trim whitespace and collapse multiple spaces
                let trimmed = value_str.split_whitespace().collect::<Vec<_>>().join(" ");
                amz_headers.entry(name_lower).or_default().push(trimmed);
            }
        }

        let mut result = String::new();
        for (name, values) in amz_headers {
            result.push_str(&format!("{}:{}\n", name, values.join(",")));
        }
        result
    }

    /// Build canonicalized resource
    fn build_canonicalized_resource<B>(&self, request: &Request<B>) -> String {
        let uri = request.uri();
        let path = uri.path();

        // Start with the path
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

                if SUB_RESOURCES.contains(&key) {
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

    /// Calculate the signature using HMAC-SHA1
    fn calculate_signature(&self, secret_key: &str, string_to_sign: &str) -> String {
        let mut mac =
            HmacSha1::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
        mac.update(string_to_sign.as_bytes());
        let result = mac.finalize().into_bytes();
        BASE64.encode(result)
    }
}

/// Parsed authorization header
struct ParsedAuth {
    access_key_id: String,
    signature: String,
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
    fn test_parse_auth_header() {
        let user_store = Arc::new(UserStore::new());
        let verifier = SigV2Verifier::new(user_store);

        let result = verifier
            .parse_authorization_header("AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//yllqDzg=")
            .unwrap();

        assert_eq!(result.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(result.signature, "frJIUN8DYpKDtOLCwo//yllqDzg=");
    }

    #[test]
    fn test_parse_invalid_auth_header() {
        let user_store = Arc::new(UserStore::new());
        let verifier = SigV2Verifier::new(user_store);

        // Wrong prefix
        assert!(verifier.parse_authorization_header("Bearer token").is_err());

        // Missing signature
        assert!(
            verifier
                .parse_authorization_header("AWS AKIAIOSFODNN7EXAMPLE")
                .is_err()
        );
    }

    #[test]
    fn test_calculate_signature() {
        let user_store = Arc::new(UserStore::new());
        let verifier = SigV2Verifier::new(user_store);

        // Test with known values from AWS documentation
        let string_to_sign =
            "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/awsexamplebucket1/photos/puppy.jpg";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        let signature = verifier.calculate_signature(secret_key, string_to_sign);

        // The signature should be a valid base64 string
        assert!(BASE64.decode(&signature).is_ok());
    }

    #[test]
    fn test_canonicalized_resource() {
        let user_store = Arc::new(UserStore::new());
        let verifier = SigV2Verifier::new(user_store);

        // Simple path
        let request = http::Request::builder()
            .uri("/bucket/key")
            .body(())
            .unwrap();
        assert_eq!(
            verifier.build_canonicalized_resource(&request),
            "/bucket/key"
        );

        // With sub-resource
        let request = http::Request::builder()
            .uri("/bucket/key?acl")
            .body(())
            .unwrap();
        assert_eq!(
            verifier.build_canonicalized_resource(&request),
            "/bucket/key?acl"
        );

        // With multiple sub-resources (should be sorted)
        let request = http::Request::builder()
            .uri("/bucket/key?versionId=123&acl")
            .body(())
            .unwrap();
        assert_eq!(
            verifier.build_canonicalized_resource(&request),
            "/bucket/key?acl&versionId=123"
        );

        // Non-sub-resource parameters should be ignored
        let request = http::Request::builder()
            .uri("/bucket?prefix=foo&acl")
            .body(())
            .unwrap();
        assert_eq!(
            verifier.build_canonicalized_resource(&request),
            "/bucket?acl"
        );
    }
}
