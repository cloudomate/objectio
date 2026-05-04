//! User and access key types

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// User status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserStatus {
    #[default]
    Active,
    Suspended,
    Deleted,
}

/// Access key status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyStatus {
    #[default]
    Active,
    Inactive,
}

/// A user account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Unique user identifier
    pub user_id: String,
    /// Display name
    pub display_name: String,
    /// Email address (optional)
    pub email: Option<String>,
    /// Unix timestamp of creation
    pub created_at: u64,
    /// User status
    pub status: UserStatus,
    /// ARN for this user (e.g., "arn:obio:iam::objectio:user/username")
    pub arn: String,
}

impl User {
    /// Create a new user with generated ID
    pub fn new(display_name: impl Into<String>) -> Self {
        let user_id = Uuid::new_v4().to_string();
        let display_name = display_name.into();
        let arn = format!("arn:obio:iam::objectio:user/{}", display_name);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            user_id,
            display_name,
            email: None,
            created_at,
            status: UserStatus::Active,
            arn,
        }
    }

    /// Create a new user with specific ID
    pub fn with_id(user_id: impl Into<String>, display_name: impl Into<String>) -> Self {
        let user_id = user_id.into();
        let display_name = display_name.into();
        let arn = format!("arn:obio:iam::objectio:user/{}", display_name);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            user_id,
            display_name,
            email: None,
            created_at,
            status: UserStatus::Active,
            arn,
        }
    }

    /// Check if user is active
    pub fn is_active(&self) -> bool {
        self.status == UserStatus::Active
    }
}

/// An access key for API authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessKey {
    /// Access key ID (20 chars, e.g., "AKIAIOSFODNN7EXAMPLE")
    pub access_key_id: String,
    /// Secret access key (40 chars)
    pub secret_access_key: String,
    /// Associated user ID
    pub user_id: String,
    /// Unix timestamp of creation
    pub created_at: u64,
    /// Key status
    pub status: KeyStatus,
}

impl AccessKey {
    /// Generate a new access key for a user
    pub fn generate(user_id: impl Into<String>) -> Self {
        let access_key_id = generate_access_key_id();
        let secret_access_key = generate_secret_key();
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            access_key_id,
            secret_access_key,
            user_id: user_id.into(),
            created_at,
            status: KeyStatus::Active,
        }
    }

    /// Check if key is active
    pub fn is_active(&self) -> bool {
        self.status == KeyStatus::Active
    }
}

/// Generate an access key ID (20 uppercase alphanumeric characters starting with AKIA)
fn generate_access_key_id() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();

    let random_part: String = (0..16)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    format!("AKIA{}", random_part)
}

/// Generate a secret access key (40 alphanumeric + special characters)
fn generate_secret_key() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::thread_rng();

    (0..40)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// How the caller authenticated. Surfaced into policy evaluation as
/// the `obio:CredentialType` condition variable so bucket and Unity
/// policies can deny direct (permanent-key) access while still allowing
/// STS-vended sessions through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMode {
    /// Permanent IAM access key (`AKIA…`).
    #[default]
    Permanent,
    /// STS-vended temporary session (`ASIA…` + X-Amz-Security-Token).
    Sts,
    /// `--no-auth` mode or pre-auth public route.
    Anonymous,
}

impl AuthMode {
    /// Wire/policy-condition representation. Matches the value engines
    /// see in the `obio:CredentialType` policy variable.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Permanent => "Permanent",
            Self::Sts => "STS",
            Self::Anonymous => "Anonymous",
        }
    }
}

/// Authentication result after successful verification
#[derive(Debug, Clone, Default)]
pub struct AuthResult {
    /// Authenticated user ID
    pub user_id: String,
    /// User ARN
    pub user_arn: String,
    /// Access key ID used
    pub access_key_id: String,
    /// Group ARNs the user belongs to (populated by gateway). Includes both
    /// IAM groups (`arn:obio:iam::objectio:group/<name>`) and any
    /// federation-derived groups (e.g. OIDC: `arn:obio:iam::oidc:group/<g>`).
    pub group_arns: Vec<String>,
    /// Resolved IAM group_ids the user belongs to. Populated when the auth
    /// layer can map federation group names back to local IAM groups (e.g.
    /// the OIDC bridge looks up IAM groups by name and stitches their IDs
    /// here). Policy evaluators iterate this list to apply group-attached
    /// IAM policies. Empty = no IAM-group resolution.
    pub group_ids: Vec<String>,
    /// Tenant the user belongs to (empty = system admin)
    pub tenant: String,
    /// How the caller authenticated. Drives `obio:CredentialType` in
    /// policy evaluation.
    pub auth_mode: AuthMode,
}

impl AuthResult {
    /// Get the user ARN
    pub fn user_arn(&self) -> &str {
        &self.user_arn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_access_key_id() {
        let key_id = generate_access_key_id();
        assert_eq!(key_id.len(), 20);
        assert!(key_id.starts_with("AKIA"));
    }

    #[test]
    fn test_generate_secret_key() {
        let secret = generate_secret_key();
        assert_eq!(secret.len(), 40);
    }

    #[test]
    fn test_user_creation() {
        let user = User::new("testuser");
        assert!(user.is_active());
        assert!(user.arn.contains("testuser"));
    }

    #[test]
    fn test_access_key_generation() {
        let key = AccessKey::generate("user123");
        assert!(key.is_active());
        assert!(key.access_key_id.starts_with("AKIA"));
        assert_eq!(key.secret_access_key.len(), 40);
        assert_eq!(key.user_id, "user123");
    }
}
