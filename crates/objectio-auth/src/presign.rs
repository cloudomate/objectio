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

/// Generate a presigned S3 ListObjectsV2 GET URL.
///
/// Returns a URL that hits `GET /{bucket}/?list-type=2&prefix=<prefix>&...`
/// (continuation token + max-keys honored). The body is XML in the standard
/// S3 `ListBucketResult` format.
///
/// # Arguments
/// - `endpoint` — base URL of the gateway
/// - `region`, `access_key_id`, `secret_access_key` — `SigV4` credentials
/// - `bucket` — bucket name
/// - `prefix` — `prefix` query value (may be empty)
/// - `continuation_token` — `continuation-token` query value, if paginating
/// - `max_keys` — `max-keys` query value (capped server-side)
/// - `expires_in` — URL validity window
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn presign_list_objects_v2(
    endpoint: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
    bucket: &str,
    prefix: &str,
    continuation_token: Option<&str>,
    max_keys: Option<u32>,
    expires_in: Duration,
) -> String {
    let now = Utc::now();
    let date_str = now.format("%Y%m%d").to_string();
    let datetime_str = now.format("%Y%m%dT%H%M%SZ").to_string();
    let expires_secs = expires_in.as_secs();

    let service = "s3";
    let credential_scope = format!("{date_str}/{region}/{service}/aws4_request");
    let credential = format!("{access_key_id}/{credential_scope}");
    let host = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://");

    // SigV4 requires the canonical query string parameters to be sorted by name.
    // Build (name, value) pairs first, then sort, then encode + join.
    let cred_enc = uri_encode(&credential);
    let exp_str = expires_secs.to_string();
    let max_keys_str = max_keys.map(|v| v.to_string());

    let mut pairs: Vec<(&str, &str)> = vec![
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", &cred_enc),
        ("X-Amz-Date", &datetime_str),
        ("X-Amz-Expires", &exp_str),
        ("X-Amz-SignedHeaders", "host"),
        ("list-type", "2"),
    ];
    if !prefix.is_empty() {
        pairs.push(("prefix", prefix));
    }
    if let Some(ref s) = max_keys_str {
        pairs.push(("max-keys", s));
    }
    if let Some(token) = continuation_token {
        pairs.push(("continuation-token", token));
    }
    pairs.sort_by(|a, b| a.0.cmp(b.0));

    let canonical_qs = pairs
        .iter()
        .map(|(k, v)| {
            // X-Amz-Credential is already encoded; everything else needs encoding.
            // We include the literal value when it's already pre-encoded above.
            if *k == "X-Amz-Credential" {
                format!("{k}={v}")
            } else {
                format!("{}={}", k, uri_encode(v))
            }
        })
        .collect::<Vec<_>>()
        .join("&");

    // Bucket-level URI is /{bucket}/ — no key segment.
    let canonical_uri = format!("/{}/", uri_encode(bucket));
    let canonical_headers = format!("host:{host}\n");
    let signed_headers = "host";
    let canonical_request = format!(
        "GET\n{canonical_uri}\n{canonical_qs}\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD",
    );

    let cr_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{datetime_str}\n{credential_scope}\n{cr_hash}",);
    let signing_key = derive_signing_key(secret_access_key, &date_str, region, service);
    let mut mac = HmacSha256::new_from_slice(&signing_key).expect("HMAC accepts any key length");
    mac.update(string_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    format!(
        "{endpoint}{canonical_uri}?{canonical_qs}&X-Amz-Signature={signature}",
        endpoint = endpoint.trim_end_matches('/'),
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

    #[test]
    fn test_presign_list_objects_v2_includes_required_params() {
        let url = presign_list_objects_v2(
            "http://localhost:9000",
            "us-east-1",
            "AKID",
            "secret",
            "lake",
            "warehouse/sales/events/_delta_log/",
            None,
            Some(1000),
            Duration::from_secs(900),
        );
        assert!(url.starts_with("http://localhost:9000/lake/?"));
        assert!(url.contains("list-type=2"));
        assert!(url.contains("max-keys=1000"));
        assert!(url.contains("prefix=warehouse%2Fsales%2Fevents%2F_delta_log%2F"));
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Signature="));
    }

    #[test]
    fn test_presign_list_objects_v2_paginates_with_continuation_token() {
        let url = presign_list_objects_v2(
            "http://localhost:9000",
            "us-east-1",
            "AKID",
            "secret",
            "lake",
            "p/",
            Some("opaqueToken=="),
            None,
            Duration::from_secs(60),
        );
        assert!(url.contains("continuation-token=opaqueToken%3D%3D"));
    }
}
