//! `ObjectIO` license — signed tier attestation loaded by the gateway to unlock
//! Enterprise features. See `LICENSE.md` in the sibling `objectio-docs`
//! repo for the threat model and wire format.
//!
//! Two tiers exist: [`Tier::Community`] (default, no license) and
//! [`Tier::Enterprise`] (requires a valid, unexpired, signature-verified
//! license). There is no online phone-home; verification is purely local
//! against a baked-in Ed25519 public key.
//!
//! A license is a JSON object:
//! ```json
//! {
//!   "payload": {
//!     "tier": "enterprise",
//!     "licensee": "acme-corp",
//!     "issued_at": 1_700_000_000,
//!     "expires_at": 1_800_000_000,
//!     "features": ["iceberg", "delta_sharing", "kms", "multi_tenancy", "oidc", "lrc"]
//!   },
//!   "signature": "base64(ed25519(canonical_json(payload)))"
//! }
//! ```
//!
//! The payload is re-serialized in canonical form (sorted keys) before
//! signing/verification so minor formatting differences in delivered JSON
//! do not invalidate the signature.

use serde::{Deserialize, Serialize};
use std::fmt;

mod pubkey;

pub use pubkey::LICENSE_PUBLIC_KEY;

/// Errors produced while loading, parsing, or verifying a license.
#[derive(Debug, thiserror::Error)]
pub enum LicenseError {
    #[error("license file is malformed: {0}")]
    Malformed(String),
    #[error("license signature verification failed")]
    BadSignature,
    #[error("license expired at {0}")]
    Expired(u64),
    #[error("license is not yet valid (issued_at {0} > now {1})")]
    NotYetValid(u64, u64),
    #[error("unknown tier: {0}")]
    UnknownTier(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("public key is not configured (build-time placeholder still in place)")]
    MissingPublicKey,
}

/// Enterprise tier. Community is the implicit default when no license is
/// installed — it never appears in a license payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    #[default]
    Community,
    Enterprise,
}

impl Tier {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Community => "community",
            Self::Enterprise => "enterprise",
        }
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Feature flags an Enterprise license can unlock. Keeping features explicit
/// (rather than "all Enterprise features implied by tier") leaves room for
/// per-licensee carve-outs later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feature {
    Iceberg,
    DeltaSharing,
    Kms,
    MultiTenancy,
    Oidc,
    Lrc,
}

impl Feature {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Iceberg => "iceberg",
            Self::DeltaSharing => "delta_sharing",
            Self::Kms => "kms",
            Self::MultiTenancy => "multi_tenancy",
            Self::Oidc => "oidc",
            Self::Lrc => "lrc",
        }
    }

    #[must_use]
    pub const fn all() -> [Self; 6] {
        [
            Self::Iceberg,
            Self::DeltaSharing,
            Self::Kms,
            Self::MultiTenancy,
            Self::Oidc,
            Self::Lrc,
        ]
    }
}

/// Payload of a license. This is exactly what gets signed and verified.
///
/// New capacity fields (`max_nodes`, `max_raw_capacity_bytes`) default to
/// `0` (= unlimited) when absent from the JSON, so licenses signed before
/// those fields existed still verify. Canonical JSON still sorts by key
/// name so the same payload produces the same signature regardless of
/// which signer wrote it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LicensePayload {
    pub tier: Tier,
    pub licensee: String,
    pub issued_at: u64,
    pub expires_at: u64,
    #[serde(default)]
    pub features: Vec<String>,
    /// Cap on the number of storage nodes (distinct OSD addresses) the
    /// cluster may have. `0` means no cap. Enforced on the admin
    /// `create_pool` path as a scale-up block.
    ///
    /// Skipped from serialization when `0` so older licenses (issued before
    /// this field existed) continue to verify: the canonical form they were
    /// signed with didn't include this key, and `0` means "same as absent".
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub max_nodes: u64,
    /// Cap on aggregate raw disk capacity across all OSDs in bytes (the
    /// sum of each disk's reported `total_capacity`, pre-erasure coding).
    /// `0` means no cap. Enforced on `create_bucket` as a scale-up block.
    ///
    /// Skipped from serialization when `0` for the same back-compat reason
    /// as `max_nodes` above.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub max_raw_capacity_bytes: u64,
}

// Signature dictated by serde's `skip_serializing_if` — `&T`, not `T`. The
// function is also called on each serialization so keep it trivially const.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

/// Wire format: `{ payload, signature }`. `signature` is base64-encoded
/// raw Ed25519 bytes over the canonical-JSON serialization of `payload`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseFile {
    pub payload: LicensePayload,
    pub signature: String,
}

/// A validated, loaded license.
///
/// Only constructable via [`License::load_from_bytes`] or
/// [`License::community`]; any `License` you hold has already been
/// signature- and expiry-verified (or is the unsigned Community default).
#[derive(Debug, Clone)]
pub struct License {
    pub tier: Tier,
    pub licensee: String,
    pub issued_at: u64,
    pub expires_at: u64,
    pub features: Vec<String>,
    pub max_nodes: u64,
    pub max_raw_capacity_bytes: u64,
}

impl License {
    /// Default license state when no file is installed. Unlocks nothing
    /// beyond Community-tier features.
    #[must_use]
    pub fn community() -> Self {
        Self {
            tier: Tier::Community,
            licensee: "community".to_string(),
            issued_at: 0,
            expires_at: 0,
            features: Vec::new(),
            max_nodes: 0,
            max_raw_capacity_bytes: 0,
        }
    }

    /// Parse + signature-verify + expiry-check a license given as raw bytes.
    /// Pass `now_unix_secs` from the caller so tests can control time.
    ///
    /// # Errors
    /// Returns [`LicenseError::Malformed`] if the bytes aren't valid JSON in
    /// the `LicenseFile` wire format, and any error from [`Self::from_file`].
    pub fn load_from_bytes(bytes: &[u8], now_unix_secs: u64) -> Result<Self, LicenseError> {
        let file: LicenseFile = serde_json::from_slice(bytes)
            .map_err(|e| LicenseError::Malformed(e.to_string()))?;
        Self::from_file(file, now_unix_secs)
    }

    /// Verify a pre-parsed `LicenseFile`. Checks, in order:
    /// 1. public key is configured (build-time placeholder not in place)
    /// 2. Ed25519 signature over canonical(payload)
    /// 3. expiry / not-before windows
    ///
    /// # Errors
    /// [`LicenseError::MissingPublicKey`] if the build-time placeholder
    /// pubkey is still in place, [`LicenseError::BadSignature`] if the
    /// signature fails to verify, [`LicenseError::Expired`] /
    /// [`LicenseError::NotYetValid`] if outside the validity window.
    pub fn from_file(file: LicenseFile, now_unix_secs: u64) -> Result<Self, LicenseError> {
        verify_signature(&file.payload, &file.signature)?;
        if file.payload.issued_at > now_unix_secs {
            return Err(LicenseError::NotYetValid(
                file.payload.issued_at,
                now_unix_secs,
            ));
        }
        if file.payload.expires_at != 0 && file.payload.expires_at <= now_unix_secs {
            return Err(LicenseError::Expired(file.payload.expires_at));
        }
        Ok(Self {
            tier: file.payload.tier,
            licensee: file.payload.licensee,
            issued_at: file.payload.issued_at,
            expires_at: file.payload.expires_at,
            features: file.payload.features,
            max_nodes: file.payload.max_nodes,
            max_raw_capacity_bytes: file.payload.max_raw_capacity_bytes,
        })
    }

    /// True iff the license tier is Enterprise and the named feature is
    /// listed (or no feature list is present — treat as "all features").
    #[must_use]
    pub fn allows(&self, feature: Feature) -> bool {
        if self.tier != Tier::Enterprise {
            return false;
        }
        if self.features.is_empty() {
            return true; // blanket Enterprise → all features enabled
        }
        self.features.iter().any(|f| f == feature.as_str())
    }

    #[must_use]
    pub const fn is_enterprise(&self) -> bool {
        matches!(self.tier, Tier::Enterprise)
    }
}

/// Serialize a payload to canonical JSON — sorted keys, no whitespace. This
/// must match the sign-side serializer byte-for-byte, so both sides use
/// `serde_json` with a `BTreeMap` shim underneath.
fn canonical_payload(payload: &LicensePayload) -> Result<Vec<u8>, LicenseError> {
    // Re-route through Value → BTreeMap-ordered serialization. serde_json
    // already emits keys in the struct's declared order, but to make the
    // canonical form robust against future field reordering we sort
    // explicitly.
    let v = serde_json::to_value(payload)?;
    let sorted = sort_keys(v);
    Ok(serde_json::to_vec(&sorted)?)
}

fn sort_keys(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut sorted: std::collections::BTreeMap<String, serde_json::Value> =
                std::collections::BTreeMap::new();
            for (k, val) in map {
                sorted.insert(k, sort_keys(val));
            }
            serde_json::to_value(sorted).unwrap_or(serde_json::Value::Null)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(sort_keys).collect())
        }
        other => other,
    }
}

fn verify_signature(payload: &LicensePayload, signature_b64: &str) -> Result<(), LicenseError> {
    verify_signature_with_pubkey(payload, signature_b64, &pubkey::LICENSE_PUBLIC_KEY)
}

fn verify_signature_with_pubkey(
    payload: &LicensePayload,
    signature_b64: &str,
    pubkey_bytes: &[u8; 32],
) -> Result<(), LicenseError> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    // A placeholder key is an all-zero 32-byte array — reject outright.
    if pubkey_bytes.iter().all(|b| *b == 0) {
        return Err(LicenseError::MissingPublicKey);
    }
    let vk = VerifyingKey::from_bytes(pubkey_bytes).map_err(|_| LicenseError::BadSignature)?;

    let sig_bytes = STANDARD.decode(signature_b64)?;
    let sig = Signature::from_slice(&sig_bytes).map_err(|_| LicenseError::BadSignature)?;

    let msg = canonical_payload(payload)?;
    vk.verify(&msg, &sig).map_err(|_| LicenseError::BadSignature)
}

/// Sign a payload using a raw 32-byte Ed25519 private key. Exposed so the
/// `objectio-license-gen` binary can produce signed license files; never
/// called inside the gateway.
///
/// # Errors
/// [`LicenseError::Json`] if `payload` fails to serialize for signing.
pub fn sign_payload(
    payload: &LicensePayload,
    private_key: &[u8; 32],
) -> Result<String, LicenseError> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use ed25519_dalek::{Signer, SigningKey};

    let sk = SigningKey::from_bytes(private_key);
    let msg = canonical_payload(payload)?;
    let sig = sk.sign(&msg);
    Ok(STANDARD.encode(sig.to_bytes()))
}

/// Construct a [`LicenseFile`] from a payload + private key, ready to
/// serialize to disk.
///
/// # Errors
/// Any error returned by [`sign_payload`].
pub fn issue_license(
    payload: LicensePayload,
    private_key: &[u8; 32],
) -> Result<LicenseFile, LicenseError> {
    let signature = sign_payload(&payload, private_key)?;
    Ok(LicenseFile { payload, signature })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    fn keypair() -> ([u8; 32], [u8; 32]) {
        let sk = SigningKey::generate(&mut OsRng);
        let pk = sk.verifying_key();
        (sk.to_bytes(), pk.to_bytes())
    }

    /// Test-only verification path: feeds a caller-provided pubkey so we
    /// don't depend on the baked-in production key (which is a placeholder
    /// at development time).
    fn from_file_with_pubkey(
        file: LicenseFile,
        now: u64,
        pubkey: &[u8; 32],
    ) -> Result<License, LicenseError> {
        verify_signature_with_pubkey(&file.payload, &file.signature, pubkey)?;
        if file.payload.issued_at > now {
            return Err(LicenseError::NotYetValid(file.payload.issued_at, now));
        }
        if file.payload.expires_at != 0 && file.payload.expires_at <= now {
            return Err(LicenseError::Expired(file.payload.expires_at));
        }
        Ok(License {
            tier: file.payload.tier,
            licensee: file.payload.licensee,
            issued_at: file.payload.issued_at,
            expires_at: file.payload.expires_at,
            features: file.payload.features,
            max_nodes: file.payload.max_nodes,
            max_raw_capacity_bytes: file.payload.max_raw_capacity_bytes,
        })
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let (sk, pk) = keypair();
        let payload = LicensePayload {
            tier: Tier::Enterprise,
            licensee: "acme".into(),
            issued_at: 10,
            expires_at: 100,
            features: vec!["iceberg".into(), "kms".into()],
            ..Default::default()
        };
        let file = issue_license(payload, &sk).unwrap();
        let license = from_file_with_pubkey(file, 50, &pk).unwrap();
        assert!(license.is_enterprise());
        assert!(license.allows(Feature::Iceberg));
        assert!(license.allows(Feature::Kms));
        assert!(!license.allows(Feature::Lrc));
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let (sk, pk) = keypair();
        let payload = LicensePayload {
            tier: Tier::Enterprise,
            licensee: "acme".into(),
            issued_at: 10,
            expires_at: 100,
            ..Default::default()
        };
        let mut file = issue_license(payload, &sk).unwrap();
        file.payload.licensee = "attacker".into();
        assert!(matches!(
            from_file_with_pubkey(file, 50, &pk),
            Err(LicenseError::BadSignature)
        ));
    }

    #[test]
    fn expired_license_is_rejected() {
        let (sk, pk) = keypair();
        let payload = LicensePayload {
            tier: Tier::Enterprise,
            licensee: "acme".into(),
            issued_at: 10,
            expires_at: 100,
            ..Default::default()
        };
        let file = issue_license(payload, &sk).unwrap();
        assert!(matches!(
            from_file_with_pubkey(file, 1000, &pk),
            Err(LicenseError::Expired(_))
        ));
    }

    #[test]
    fn blanket_enterprise_allows_everything() {
        let l = License {
            tier: Tier::Enterprise,
            licensee: "acme".into(),
            issued_at: 0,
            expires_at: 0,
            features: vec![],
            max_nodes: 0,
            max_raw_capacity_bytes: 0,
        };
        for f in Feature::all() {
            assert!(l.allows(f), "{f:?} should be allowed");
        }
    }

    #[test]
    fn community_allows_nothing() {
        let l = License::community();
        for f in Feature::all() {
            assert!(!l.allows(f));
        }
    }

    #[test]
    fn placeholder_pubkey_is_rejected() {
        // An all-zero pubkey must not verify anything — even a well-formed
        // license file. Guards against shipping binaries without running the
        // keygen step.
        let placeholder = [0u8; 32];
        let (sk, _pk) = keypair();
        let payload = LicensePayload {
            tier: Tier::Enterprise,
            licensee: "acme".into(),
            issued_at: 10,
            expires_at: 100,
            ..Default::default()
        };
        let file = issue_license(payload, &sk).unwrap();
        assert!(matches!(
            from_file_with_pubkey(file, 50, &placeholder),
            Err(LicenseError::MissingPublicKey)
        ));
    }
}
