//! AWS SigV4 presigned URL generation
//!
//! Generates pre-signed GET URLs for S3-compatible object storage.
//! The generated URLs are verified by the existing SigV4 auth middleware.
//!
//! Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html

use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

/// Percent-encode a string for use in a URL query string value or path segment.
/// Leaves unreserved characters (A-Z, a-z, 0-9, `-`, `_`, `.`, `~`) unchanged.
fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(
                    char::from_digit(u32::from(b >> 4), 16)
                        .unwrap()
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit(u32::from(b & 0xf), 16)
                        .unwrap()
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

/// Generate a presigned S3 GET URL.
///
/// # Arguments
/// - `endpoint` — base URL of the gateway (e.g. `http://localhost:9000`)
/// - `region` — AWS region string used in the credential scope (e.g. `"us-east-1"`)
/// - `access_key_id` — access key ID credential
/// - `secret_access_key` — secret access key used to sign
/// - `bucket` — bucket name
/// - `key` — object key (path inside bucket)
/// - `expires_in` — how long the URL should be valid
///
/// The returned URL can be fetched with a plain HTTP GET without any additional headers.
pub fn presign_get(
    endpoint: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
    bucket: &str,
    key: &str,
    expires_in: Duration,
) -> String {
    let now = Utc::now();
    let date_str = now.format("%Y%m%d").to_string();
    let datetime_str = now.format("%Y%m%dT%H%M%SZ").to_string();
    let expires_secs = expires_in.as_secs();

    let service = "s3";
    let credential_scope = format!("{date_str}/{region}/{service}/aws4_request");
    let credential = format!("{access_key_id}/{credential_scope}");

    // The host header value (stripped of scheme)
    let host = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://");

    // Canonical query string (parameters must be sorted)
    let canonical_qs = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256\
         &X-Amz-Credential={cred}\
         &X-Amz-Date={dt}\
         &X-Amz-Expires={exp}\
         &X-Amz-SignedHeaders=host",
        cred = uri_encode(&credential),
        dt = datetime_str,
        exp = expires_secs,
    );

    // Canonical URI: /{bucket}/{key} (each segment percent-encoded except '/')
    let canonical_uri = format!(
        "/{}/{}",
        uri_encode(bucket),
        key.split('/').map(uri_encode).collect::<Vec<_>>().join("/")
    );

    // Canonical headers and signed headers
    let canonical_headers = format!("host:{host}\n");
    let signed_headers = "host";

    // Canonical request (payload is UNSIGNED for presigned GETs)
    let canonical_request = format!(
        "GET\n{uri}\n{qs}\n{headers}\n{signed_hdr}\nUNSIGNED-PAYLOAD",
        uri = canonical_uri,
        qs = canonical_qs,
        headers = canonical_headers,
        signed_hdr = signed_headers,
    );

    // String to sign
    let cr_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{dt}\n{scope}\n{hash}",
        dt = datetime_str,
        scope = credential_scope,
        hash = cr_hash,
    );

    // Derive signing key
    let signing_key = derive_signing_key(secret_access_key, &date_str, region, service);

    // Compute signature
    let mut mac = HmacSha256::new_from_slice(&signing_key).expect("HMAC accepts any key length");
    mac.update(string_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    // Build final URL
    format!(
        "{endpoint}{uri}?{qs}&X-Amz-Signature={sig}",
        endpoint = endpoint.trim_end_matches('/'),
        uri = canonical_uri,
        qs = canonical_qs,
        sig = signature,
    )
}

/// Derive the SigV4 signing key from the secret access key and scope components.
fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_presign_produces_valid_url() {
        let url = presign_get(
            "http://localhost:9000",
            "us-east-1",
            "AKID",
            "secret",
            "my-bucket",
            "path/to/file.parquet",
            Duration::from_secs(3600),
        );
        assert!(url.starts_with("http://localhost:9000/my-bucket/path/to/file.parquet"));
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Signature="));
        assert!(url.contains("X-Amz-Expires=3600"));
    }
}
