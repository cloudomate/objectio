//! STS (Security Token Service) — temporary credential issuance and validation.
//!
//! Used to provide vended S3 credentials in Iceberg LoadTable responses,
//! allowing Spark/Trino to read table data without permanent keys.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Default session duration: 1 hour
const DEFAULT_DURATION_SECS: u64 = 3600;

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
    pub scope: String,
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

    /// Issue temporary credentials for a user and scope (e.g., table data path).
    pub fn issue(&self, user_arn: &str, scope: &str) -> TemporaryCredentials {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let expires_at = now + self.default_duration.as_secs();

        // Generate random access key ID (ASIA prefix = temporary, per AWS convention)
        let access_key_id = format!("ASIA{}", random_alphanum(16));

        // Generate random secret access key
        let secret_access_key = random_alphanum(40);

        // Session token = base64(payload|hmac_signature)
        // Payload contains: sts|user_arn|scope|expires_at
        let payload = format!("sts|{}|{}|{}", user_arn, scope, expires_at);
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

    /// Validate a session token. Returns session info if valid and not expired.
    pub fn validate(&self, session_token: &str) -> Option<StsSessionInfo> {
        let token_raw = base64_decode(session_token)?;

        // Split into payload and signature (last | separator)
        let (payload, signature) = token_raw.rsplit_once('|')?;

        // Verify HMAC
        let expected = self.sign(payload);
        if signature != expected {
            return None;
        }

        // Parse payload: "sts|{user_arn}|{scope}|{expires_at}"
        let parts: Vec<&str> = payload.splitn(4, '|').collect();
        if parts.len() != 4 || parts[0] != "sts" {
            return None;
        }

        let user_arn = parts[1].to_string();
        let scope = parts[2].to_string();
        let expires_at: u64 = parts[3].parse().ok()?;

        // Check expiry
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
    fn test_issue_and_validate() {
        let provider = StsProvider::new(b"test-key-12345");
        let creds = provider.issue("arn:objectio:iam::user/alice", "s3://warehouse/db/table");

        assert!(creds.access_key_id.starts_with("ASIA"));
        assert_eq!(creds.secret_access_key.len(), 40);
        assert!(!creds.session_token.is_empty());

        let info = provider.validate(&creds.session_token).unwrap();
        assert_eq!(info.user_arn, "arn:objectio:iam::user/alice");
        assert_eq!(info.scope, "s3://warehouse/db/table");
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
        let creds = provider1.issue("arn:test", "scope");
        assert!(provider2.validate(&creds.session_token).is_none());
    }
}
