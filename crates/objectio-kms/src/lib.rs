//! Key material management and AES primitives for server-side encryption.
//!
//! Scope (phase 2):
//! - [`MasterKey`] — a 256-bit AES-GCM wrap key held by the gateway.
//!   Used to wrap per-object DEKs. Loaded from a base64 env var.
//! - [`generate_dek`] / [`generate_iv`] — OS-RNG-backed randomness.
//! - [`encrypt_in_place`] / [`decrypt_in_place`] — AES-256-CTR byte-range
//!   encryption. CTR is seekable, so `decrypt_in_place` takes a byte
//!   offset and correctly handles range GETs.
//!
//! Out of scope for now: pluggable KMS providers (Vault, AWS KMS).
//! The `MasterKey` here stands in for an "SSE-S3 service master key" —
//! the single key the service uses to wrap per-object DEKs. SSE-KMS
//! will add a `KmsProvider` trait later that can back per-bucket keys.

use aes::Aes256;
use aes::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use base64::Engine;
use rand::RngCore;

/// Size of a Data Encryption Key (AES-256-CTR).
pub const DEK_LEN: usize = 32;

/// Size of the base IV for AES-256-CTR. CTR produces the keystream for
/// each block by incrementing this counter; 16 bytes = one AES block.
pub const IV_LEN: usize = 16;

/// Size of the service master key (AES-256-GCM wrap key).
pub const MASTER_KEY_LEN: usize = 32;

/// AES-GCM nonce size (matches aes-gcm's default).
const GCM_NONCE_LEN: usize = 12;
/// AES-GCM tag size appended to ciphertext.
const GCM_TAG_LEN: usize = 16;

/// Length of a wrapped DEK: 12-byte nonce || 32-byte ciphertext || 16-byte tag.
pub const WRAPPED_DEK_LEN: usize = GCM_NONCE_LEN + DEK_LEN + GCM_TAG_LEN;

type Aes256Ctr = ctr::Ctr128BE<Aes256>;

/// The service master key. Used to wrap per-object DEKs.
///
/// The bytes live in memory for the lifetime of the process. The
/// master key is never persisted to disk or sent over the wire by
/// this crate — the expectation is the operator injects it via env
/// var and manages rotation externally.
#[derive(Clone)]
pub struct MasterKey([u8; MASTER_KEY_LEN]);

impl MasterKey {
    /// Build a `MasterKey` from a 32-byte slice.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; MASTER_KEY_LEN]) -> Self {
        Self(bytes)
    }

    /// Load a base64-encoded master key from the given environment
    /// variable. The decoded value must be exactly 32 bytes.
    ///
    /// # Errors
    /// Returns [`KmsError::MasterKeyMissing`] if the env var is unset,
    /// or [`KmsError::MasterKeyInvalid`] on a decode/length failure.
    pub fn from_env(var: &str) -> Result<Self, KmsError> {
        let s = std::env::var(var).map_err(|_| KmsError::MasterKeyMissing(var.to_string()))?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s.trim())
            .map_err(|e| KmsError::MasterKeyInvalid(e.to_string()))?;
        if bytes.len() != MASTER_KEY_LEN {
            return Err(KmsError::MasterKeyInvalid(format!(
                "expected {MASTER_KEY_LEN} bytes after base64 decode, got {}",
                bytes.len()
            )));
        }
        let mut k = [0u8; MASTER_KEY_LEN];
        k.copy_from_slice(&bytes);
        Ok(Self(k))
    }

    /// Generate a fresh random master key. Intended for dev/test only —
    /// callers in production must load a stable key via [`Self::from_env`]
    /// so that objects remain readable across gateway restarts.
    #[must_use]
    pub fn generate_random() -> Self {
        let mut k = [0u8; MASTER_KEY_LEN];
        rand::rngs::OsRng.fill_bytes(&mut k);
        Self(k)
    }

    /// Return the key as a base64 string (for logging in dev mode only).
    #[must_use]
    pub fn to_base64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.0)
    }

    /// Wrap a DEK. Returns a [`WRAPPED_DEK_LEN`]-byte blob (nonce + ciphertext + tag)
    /// suitable for persistence in `ObjectMeta.encrypted_dek`.
    ///
    /// # Panics
    /// Panics only if the underlying AES-GCM primitive fails to encrypt a
    /// fixed-length 32-byte input — this cannot happen with sound inputs.
    #[must_use]
    pub fn wrap_dek(&self, dek: &[u8; DEK_LEN]) -> Vec<u8> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.0));
        let mut nonce_bytes = [0u8; GCM_NONCE_LEN];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        // AES-GCM encryption of a fixed-length input cannot fail under normal use.
        let ct = cipher
            .encrypt(nonce, dek.as_ref())
            .expect("AES-GCM encryption failed for DEK");
        let mut out = Vec::with_capacity(WRAPPED_DEK_LEN);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ct);
        out
    }

    /// Unwrap a DEK produced by [`Self::wrap_dek`].
    ///
    /// # Errors
    /// Returns [`KmsError::InvalidWrap`] if the blob is malformed or the
    /// GCM tag fails verification (wrong master key, corruption, or tampering).
    pub fn unwrap_dek(&self, wrapped: &[u8]) -> Result<[u8; DEK_LEN], KmsError> {
        if wrapped.len() != WRAPPED_DEK_LEN {
            return Err(KmsError::InvalidWrap(format!(
                "wrapped DEK length {} != expected {WRAPPED_DEK_LEN}",
                wrapped.len()
            )));
        }
        let (nonce_bytes, ct) = wrapped.split_at(GCM_NONCE_LEN);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.0));
        let nonce = Nonce::from_slice(nonce_bytes);
        let pt = cipher
            .decrypt(nonce, ct)
            .map_err(|e| KmsError::InvalidWrap(e.to_string()))?;
        if pt.len() != DEK_LEN {
            return Err(KmsError::InvalidWrap(format!(
                "unwrapped DEK length {} != {DEK_LEN}",
                pt.len()
            )));
        }
        let mut dek = [0u8; DEK_LEN];
        dek.copy_from_slice(&pt);
        Ok(dek)
    }
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print raw bytes.
        f.write_str("MasterKey(***)")
    }
}

/// Generate a fresh 256-bit DEK.
#[must_use]
pub fn generate_dek() -> [u8; DEK_LEN] {
    let mut dek = [0u8; DEK_LEN];
    rand::rngs::OsRng.fill_bytes(&mut dek);
    dek
}

/// Generate a fresh 128-bit CTR IV.
#[must_use]
pub fn generate_iv() -> [u8; IV_LEN] {
    let mut iv = [0u8; IV_LEN];
    rand::rngs::OsRng.fill_bytes(&mut iv);
    iv
}

/// Encrypt `buf` in place with AES-256-CTR, starting at byte offset 0.
pub fn encrypt_in_place(dek: &[u8; DEK_LEN], iv: &[u8; IV_LEN], buf: &mut [u8]) {
    let mut cipher = Aes256Ctr::new(dek.into(), iv.into());
    cipher.apply_keystream(buf);
}

/// Decrypt `buf` in place with AES-256-CTR, assuming `buf` begins at
/// byte `offset` of the original plaintext stream. CTR is seekable, so
/// this is the correct primitive for byte-range GETs.
pub fn decrypt_in_place(dek: &[u8; DEK_LEN], iv: &[u8; IV_LEN], offset: u64, buf: &mut [u8]) {
    let mut cipher = Aes256Ctr::new(dek.into(), iv.into());
    cipher.seek(offset);
    cipher.apply_keystream(buf);
}

#[derive(Debug, thiserror::Error)]
pub enum KmsError {
    #[error("master key env var {0} not set")]
    MasterKeyMissing(String),
    #[error("master key invalid: {0}")]
    MasterKeyInvalid(String),
    #[error("wrapped DEK invalid: {0}")]
    InvalidWrap(String),
    #[error("KMS key not found: {0}")]
    KeyNotFound(String),
    #[error("KMS key disabled: {0}")]
    KeyDisabled(String),
    #[error("KMS provider error: {0}")]
    ProviderError(String),
}

/// Result of a `KmsProvider::generate_data_key` call.
///
/// `plaintext_dek` is used immediately by the caller to encrypt the object
/// body; it must not be persisted. `wrapped_dek` is the DEK wrapped by the
/// KMS key identified by `key_id`; this is what gets stored in `ObjectMeta`.
pub struct GeneratedDataKey {
    pub plaintext_dek: [u8; DEK_LEN],
    pub wrapped_dek: Vec<u8>,
}

/// A backend that brokers wrap/unwrap operations against named KMS keys.
///
/// Implementations range from `LocalKmsProvider` (keys stored by our
/// meta service, wrapped by the gateway's service [`MasterKey`]) to
/// external providers (Vault, AWS KMS) in later phases.
///
/// `encryption_context` is the AWS-compatible authenticated-data map —
/// backends should bind it into the wrap so decrypt only succeeds with
/// the same context. The local implementation in this crate uses AES-GCM
/// with the serialized context as AAD.
#[async_trait::async_trait]
pub trait KmsProvider: Send + Sync {
    /// Generate a fresh DEK and return it both in plaintext (for the caller
    /// to encrypt with) and wrapped by the specified KMS key (for storage).
    async fn generate_data_key(
        &self,
        key_id: &str,
        encryption_context: &std::collections::HashMap<String, String>,
    ) -> Result<GeneratedDataKey, KmsError>;

    /// Recover the plaintext DEK from a previously-wrapped blob. The
    /// `encryption_context` must match whatever was passed at generate time.
    async fn decrypt(
        &self,
        key_id: &str,
        wrapped_dek: &[u8],
        encryption_context: &std::collections::HashMap<String, String>,
    ) -> Result<[u8; DEK_LEN], KmsError>;

    /// Cheap check that the key is known to the backend and enabled.
    async fn key_exists(&self, key_id: &str) -> Result<bool, KmsError>;
}

/// Canonicalize an encryption context for use as AEAD associated data.
/// Keys are sorted lexicographically so encrypt and decrypt callers
/// produce identical AAD regardless of `HashMap` iteration order.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn canonical_context_aad(
    context: &std::collections::HashMap<String, String>,
) -> Vec<u8> {
    let mut pairs: Vec<(&String, &String)> = context.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let mut out = Vec::new();
    for (k, v) in pairs {
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v.as_bytes());
        out.push(b'\x1f'); // ASCII unit separator — unambiguous record terminator
    }
    out
}

/// Wrap a DEK with the given 32-byte KEK, binding the AEAD to `context`.
/// Used by [`LocalKmsProvider`] and by tests.
///
/// # Panics
/// Panics only if AES-GCM fails on a fixed-length 32-byte input, which
/// doesn't happen with valid inputs.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn wrap_dek_with_context(
    kek: &[u8; MASTER_KEY_LEN],
    dek: &[u8; DEK_LEN],
    context: &std::collections::HashMap<String, String>,
) -> Vec<u8> {
    use aes_gcm::aead::Payload;
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(kek));
    let mut nonce_bytes = [0u8; GCM_NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let aad = canonical_context_aad(context);
    let ct = cipher
        .encrypt(
            nonce,
            Payload {
                msg: dek,
                aad: &aad,
            },
        )
        .expect("AES-GCM encryption failed for DEK");
    let mut out = Vec::with_capacity(WRAPPED_DEK_LEN);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    out
}

/// Unwrap a DEK produced by [`wrap_dek_with_context`]. Fails if the
/// `context` doesn't match what was used at wrap time.
///
/// # Errors
/// Returns [`KmsError::InvalidWrap`] on length mismatch, tag mismatch,
/// or context mismatch.
#[allow(clippy::implicit_hasher)]
pub fn unwrap_dek_with_context(
    kek: &[u8; MASTER_KEY_LEN],
    wrapped: &[u8],
    context: &std::collections::HashMap<String, String>,
) -> Result<[u8; DEK_LEN], KmsError> {
    use aes_gcm::aead::Payload;
    if wrapped.len() != WRAPPED_DEK_LEN {
        return Err(KmsError::InvalidWrap(format!(
            "wrapped DEK length {} != expected {WRAPPED_DEK_LEN}",
            wrapped.len()
        )));
    }
    let (nonce_bytes, ct) = wrapped.split_at(GCM_NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(kek));
    let nonce = Nonce::from_slice(nonce_bytes);
    let aad = canonical_context_aad(context);
    let pt = cipher
        .decrypt(
            nonce,
            Payload {
                msg: ct,
                aad: &aad,
            },
        )
        .map_err(|e| KmsError::InvalidWrap(e.to_string()))?;
    if pt.len() != DEK_LEN {
        return Err(KmsError::InvalidWrap(format!(
            "unwrapped DEK length {} != {DEK_LEN}",
            pt.len()
        )));
    }
    let mut dek = [0u8; DEK_LEN];
    dek.copy_from_slice(&pt);
    Ok(dek)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_unwrap_roundtrips() {
        let mk = MasterKey::generate_random();
        let dek = generate_dek();
        let wrapped = mk.wrap_dek(&dek);
        assert_eq!(wrapped.len(), WRAPPED_DEK_LEN);
        let unwrapped = mk.unwrap_dek(&wrapped).unwrap();
        assert_eq!(unwrapped, dek);
    }

    #[test]
    fn wrap_is_nondeterministic() {
        let mk = MasterKey::generate_random();
        let dek = generate_dek();
        assert_ne!(mk.wrap_dek(&dek), mk.wrap_dek(&dek));
    }

    #[test]
    fn unwrap_rejects_wrong_master_key() {
        let mk1 = MasterKey::generate_random();
        let mk2 = MasterKey::generate_random();
        let dek = generate_dek();
        let wrapped = mk1.wrap_dek(&dek);
        assert!(mk2.unwrap_dek(&wrapped).is_err());
    }

    #[test]
    fn unwrap_rejects_corrupted_ciphertext() {
        let mk = MasterKey::generate_random();
        let dek = generate_dek();
        let mut wrapped = mk.wrap_dek(&dek);
        wrapped[GCM_NONCE_LEN + 5] ^= 0x01;
        assert!(mk.unwrap_dek(&wrapped).is_err());
    }

    #[test]
    fn ctr_roundtrips_whole_buffer() {
        let dek = generate_dek();
        let iv = generate_iv();
        let plaintext = b"the quick brown fox jumps over the lazy dog".to_vec();
        let mut buf = plaintext.clone();
        encrypt_in_place(&dek, &iv, &mut buf);
        assert_ne!(buf, plaintext);
        decrypt_in_place(&dek, &iv, 0, &mut buf);
        assert_eq!(buf, plaintext);
    }

    #[test]
    fn ctr_range_decrypt_matches_whole() {
        // Verify that decrypting a slice at offset N produces the same bytes
        // as decrypting the whole buffer and then slicing. This is the
        // invariant that makes range GETs correct.
        let dek = generate_dek();
        let iv = generate_iv();
        let plaintext: Vec<u8> = (0..=255u8).cycle().take(1024).collect();
        let mut ciphertext = plaintext.clone();
        encrypt_in_place(&dek, &iv, &mut ciphertext);

        let mut whole = ciphertext.clone();
        decrypt_in_place(&dek, &iv, 0, &mut whole);
        assert_eq!(whole, plaintext);

        for (offset, len) in [(0, 17), (17, 60), (100, 100), (500, 400), (900, 124)] {
            let mut slice = ciphertext[offset..offset + len].to_vec();
            decrypt_in_place(&dek, &iv, offset as u64, &mut slice);
            assert_eq!(slice, &plaintext[offset..offset + len]);
        }
    }

    #[test]
    fn master_key_debug_never_leaks_bytes() {
        let mk = MasterKey::generate_random();
        let s = format!("{mk:?}");
        assert_eq!(s, "MasterKey(***)");
    }

    #[test]
    fn wrap_with_context_roundtrips() {
        let kek = generate_dek();
        let dek = generate_dek();
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("bucket".to_string(), "mybucket".to_string());
        ctx.insert("key".to_string(), "path/to/obj.txt".to_string());
        let wrapped = wrap_dek_with_context(&kek, &dek, &ctx);
        assert_eq!(wrapped.len(), WRAPPED_DEK_LEN);
        let unwrapped = unwrap_dek_with_context(&kek, &wrapped, &ctx).unwrap();
        assert_eq!(unwrapped, dek);
    }

    #[test]
    fn unwrap_with_wrong_context_fails() {
        let kek = generate_dek();
        let dek = generate_dek();
        let mut ctx1 = std::collections::HashMap::new();
        ctx1.insert("bucket".to_string(), "mybucket".to_string());
        let wrapped = wrap_dek_with_context(&kek, &dek, &ctx1);
        let mut ctx2 = std::collections::HashMap::new();
        ctx2.insert("bucket".to_string(), "otherbucket".to_string());
        assert!(unwrap_dek_with_context(&kek, &wrapped, &ctx2).is_err());
    }

    #[test]
    fn canonical_context_aad_is_stable_regardless_of_insertion_order() {
        let mut a = std::collections::HashMap::new();
        a.insert("z".to_string(), "last".to_string());
        a.insert("a".to_string(), "first".to_string());
        a.insert("m".to_string(), "middle".to_string());
        let mut b = std::collections::HashMap::new();
        b.insert("a".to_string(), "first".to_string());
        b.insert("m".to_string(), "middle".to_string());
        b.insert("z".to_string(), "last".to_string());
        assert_eq!(canonical_context_aad(&a), canonical_context_aad(&b));
    }

    #[test]
    fn unwrap_with_empty_context_when_wrapped_without_works() {
        let kek = generate_dek();
        let dek = generate_dek();
        let empty = std::collections::HashMap::new();
        let wrapped = wrap_dek_with_context(&kek, &dek, &empty);
        let unwrapped = unwrap_dek_with_context(&kek, &wrapped, &empty).unwrap();
        assert_eq!(unwrapped, dek);
    }
}
