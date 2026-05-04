//! STS (Security Token Service) — temporary credential issuance and validation.
//!
//! Issues short-lived AWS-style credentials that the gateway uses for vended
//! Iceberg / Unity Catalog data access. The credentials look like real AWS
//! STS creds (`ASIA…` access key + secret + session token) and are used by
//! standard SigV4 clients (PyIceberg, Spark, Trino, boto3) without any
//! special-casing.
//!
//! ## Token shape
//!
//! The session token is an HMAC-signed payload of the form:
//!
//! ```text
//! sts|{user_arn}|{scope}|{operation}|{expires_at}
//! ```
//!
//! Where `scope` is an `s3://bucket/prefix/` URI and `operation` is `R` (read
//! only) or `RW` (read + write). The signing key never leaves the gateway,
//! so the token is the only proof needed.
//!
//! ## Secret derivation
//!
//! The secret access key is **deterministically derived** from the access
//! key id and the master signing key:
//!
//! ```text
//! secret = hex(HMAC-SHA256(signing_key, access_key_id))
//! ```
//!
//! This means the gateway can recover the secret on every incoming request
//! without storing per-cred state, and a real SigV4 signature check can run
//! against the derived secret. Without this, the auth middleware would have
//! to skip signature verification entirely.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Default session duration: 1 hour
const DEFAULT_DURATION_SECS: u64 = 3600;

/// What the holder of a vended credential is allowed to do against the
/// scoped prefix. Mutating methods (PUT/POST/DELETE/PATCH) are rejected
/// when the operation is `Read`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Read,
    ReadWrite,
}

impl Operation {
    fn as_token_str(self) -> &'static str {
        match self {
            Operation::Read => "R",
            Operation::ReadWrite => "RW",
        }
    }

    fn from_token_str(s: &str) -> Option<Self> {
        match s {
            "R" => Some(Operation::Read),
            "RW" => Some(Operation::ReadWrite),
            _ => None,
        }
    }
}

/// Temporary S3 credentials with session token
#[derive(Debug, Clone)]
pub struct TemporaryCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expires_at: u64,
}

/// Parsed session info from a validated token
#[derive(Debug, Clone)]
pub struct StsSessionInfo {
    pub user_arn: String,
    /// `s3://bucket/prefix/` URI the holder is scoped to.
    pub scope: String,
    pub operation: Operation,
    pub parent_access_key: String,
    pub expires_at: u64,
}

/// STS credential provider — issues and validates temporary credentials.
#[derive(Clone)]
pub struct StsProvider {
    signing_key: Vec<u8>,
    default_duration: Duration,
}

impl StsProvider {
    /// Create a new STS provider with the given signing key.
    pub fn new(signing_key: &[u8]) -> Self {
        Self {
            signing_key: signing_key.to_vec(),
            default_duration: Duration::from_secs(DEFAULT_DURATION_SECS),
        }
    }

    /// Issue temporary credentials for a user, scope and operation.
    ///
    /// `scope` should be an `s3://bucket/prefix/` URI — only requests
    /// against that bucket+prefix will be accepted by the auth middleware.
    pub fn issue(&self, user_arn: &str, scope: &str, operation: Operation) -> TemporaryCredentials {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let expires_at = now + self.default_duration.as_secs();

        // ASIA prefix marks this as a temporary credential per AWS convention.
        let access_key_id = format!("ASIA{}", random_alphanum(16));
        // Secret is derived from (signing_key, access_key_id) so it can be
        // recovered on validation without storing per-cred state.
        let secret_access_key = self.derive_secret(&access_key_id);

        let payload = format!(
            "sts|{}|{}|{}|{}",
            user_arn,
            scope,
            operation.as_token_str(),
            expires_at
        );
        let signature = self.sign(&payload);
        let token_raw = format!("{}|{}", payload, signature);
        let session_token = base64_encode(&token_raw);

        TemporaryCredentials {
            access_key_id,
            secret_access_key,
            session_token,
            expires_at,
        }
    }

    /// Recover the secret access key for a previously-issued ASIA*
    /// access key. Only meaningful for keys this provider produced.
    pub fn derive_secret(&self, access_key_id: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(&self.signing_key).expect("HMAC accepts any key length");
        mac.update(access_key_id.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Validate a session token. Returns session info if the HMAC matches,
    /// the payload parses, and the token has not expired.
    pub fn validate(&self, session_token: &str) -> Option<StsSessionInfo> {
        let token_raw = base64_decode(session_token)?;

        // Split on the LAST `|` so user_arn/scope can themselves contain `|`.
        let (payload, signature) = token_raw.rsplit_once('|')?;

        let expected = self.sign(payload);
        if signature != expected {
            return None;
        }

        // Payload: "sts|{user_arn}|{scope}|{operation}|{expires_at}"
        let parts: Vec<&str> = payload.splitn(5, '|').collect();
        if parts.len() != 5 || parts[0] != "sts" {
            return None;
        }

        let user_arn = parts[1].to_string();
        let scope = parts[2].to_string();
        let operation = Operation::from_token_str(parts[3])?;
        let expires_at: u64 = parts[4].parse().ok()?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now > expires_at {
            return None;
        }

        Some(StsSessionInfo {
            user_arn,
            scope,
            operation,
            parent_access_key: String::new(),
            expires_at,
        })
    }

    fn sign(&self, payload: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.signing_key).expect("HMAC key");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

/// Returns true if the request `bucket`/`key` falls within the holder's
/// scope. `scope` is an `s3://bucket/prefix/` URI; an empty path component
/// after the bucket means the entire bucket is in scope.
///
/// `key` is the object key without a leading slash (S3-style).
pub fn scope_allows(scope: &str, bucket: &str, key: &str) -> bool {
    let Some(rest) = scope.strip_prefix("s3://") else {
        return false;
    };
    let (scope_bucket, scope_prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p),
        None => (rest, ""),
    };
    if scope_bucket != bucket {
        return false;
    }
    // Empty prefix → whole bucket in scope.
    key.starts_with(scope_prefix)
}

/// Returns true if `method` writes to S3 (would require `ReadWrite`).
pub fn is_mutating_method(method: &str) -> bool {
    matches!(
        method.to_ascii_uppercase().as_str(),
        "PUT" | "POST" | "DELETE" | "PATCH"
    )
}

fn random_alphanum(len: usize) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| {
            let idx: u8 = rng.gen_range(0..36);
            if idx < 10 {
                (b'0' + idx) as char
            } else {
                (b'A' + idx - 10) as char
            }
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_issue_and_validate_read() {
        let provider = StsProvider::new(b"test-key-12345");
        let creds = provider.issue(
            "arn:objectio:iam::user/alice",
            "s3://warehouse/db/table/",
            Operation::Read,
        );

        assert!(creds.access_key_id.starts_with("ASIA"));
        assert_eq!(creds.secret_access_key.len(), 64); // hex of 32-byte HMAC
        assert!(!creds.session_token.is_empty());

        let info = provider.validate(&creds.session_token).unwrap();
        assert_eq!(info.user_arn, "arn:objectio:iam::user/alice");
        assert_eq!(info.scope, "s3://warehouse/db/table/");
        assert_eq!(info.operation, Operation::Read);
    }

    #[test]
    fn test_issue_and_validate_read_write() {
        let provider = StsProvider::new(b"test-key-12345");
        let creds = provider.issue("arn:test", "s3://b/p/", Operation::ReadWrite);
        let info = provider.validate(&creds.session_token).unwrap();
        assert_eq!(info.operation, Operation::ReadWrite);
    }

    #[test]
    fn test_secret_is_recoverable() {
        let provider = StsProvider::new(b"k");
        let creds = provider.issue("u", "s3://b/", Operation::Read);
        // The secret returned at issue time matches what derive_secret
        // produces — proving the middleware can recover it from just the
        // access_key_id.
        assert_eq!(
            provider.derive_secret(&creds.access_key_id),
            creds.secret_access_key
        );
    }

    #[test]
    fn test_invalid_token() {
        let provider = StsProvider::new(b"test-key-12345");
        assert!(provider.validate("invalid-token").is_none());
    }

    #[test]
    fn test_wrong_key() {
        let provider1 = StsProvider::new(b"key-1");
        let provider2 = StsProvider::new(b"key-2");
        let creds = provider1.issue("arn:test", "s3://b/", Operation::Read);
        assert!(provider2.validate(&creds.session_token).is_none());
    }

    #[test]
    fn test_scope_allows_in_prefix() {
        assert!(scope_allows(
            "s3://warehouse/db/t/",
            "warehouse",
            "db/t/data.parquet"
        ));
    }

    #[test]
    fn test_scope_allows_exact_prefix() {
        assert!(scope_allows("s3://warehouse/db/t/", "warehouse", "db/t/"));
    }

    #[test]
    fn test_scope_rejects_wrong_bucket() {
        assert!(!scope_allows(
            "s3://warehouse/db/t/",
            "other",
            "db/t/data.parquet"
        ));
    }

    #[test]
    fn test_scope_rejects_wrong_prefix() {
        assert!(!scope_allows(
            "s3://warehouse/db/t/",
            "warehouse",
            "db/other/data.parquet"
        ));
    }

    #[test]
    fn test_scope_rejects_prefix_traversal_via_sibling() {
        // "db/table" must not satisfy a "db/t/" scope — naive substring
        // checks would let "db/t" prefix-match "db/table".
        assert!(!scope_allows(
            "s3://warehouse/db/t/",
            "warehouse",
            "db/table/x"
        ));
    }

    #[test]
    fn test_scope_whole_bucket() {
        assert!(scope_allows(
            "s3://warehouse",
            "warehouse",
            "anything/at/all"
        ));
        assert!(scope_allows("s3://warehouse/", "warehouse", "anything"));
    }

    #[test]
    fn test_is_mutating_method() {
        for m in &["GET", "HEAD", "OPTIONS", "get", "head"] {
            assert!(!is_mutating_method(m), "{m} should be read-only");
        }
        for m in &["PUT", "POST", "DELETE", "PATCH", "put", "post"] {
            assert!(is_mutating_method(m), "{m} should be mutating");
        }
    }
}
