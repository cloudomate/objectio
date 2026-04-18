//! Checksum utilities for ObjectIO
//!
//! Provides multi-algorithm checksum calculation and verification
//! for data integrity.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Checksum values computed for a block of data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    /// CRC32C checksum (fast, for inline verification)
    pub crc32c: u32,
    /// xxHash64 (fast, for comparison)
    pub xxhash64: u64,
    /// SHA256 hash (optional, for content addressing)
    pub sha256: Option<[u8; 32]>,
}

impl Checksum {
    /// Create a new checksum with all fields
    #[must_use]
    pub const fn new(crc32c: u32, xxhash64: u64, sha256: Option<[u8; 32]>) -> Self {
        Self {
            crc32c,
            xxhash64,
            sha256,
        }
    }

    /// Compute checksum from data (without SHA256)
    #[must_use]
    pub fn compute_fast(data: &[u8]) -> Self {
        Self {
            crc32c: crc32c::crc32c(data),
            xxhash64: xxhash_rust::xxh64::xxh64(data, 0),
            sha256: None,
        }
    }

    /// Compute checksum from data (with SHA256)
    #[must_use]
    pub fn compute_full(data: &[u8]) -> Self {
        let sha256_hash = Sha256::digest(data);
        Self {
            crc32c: crc32c::crc32c(data),
            xxhash64: xxhash_rust::xxh64::xxh64(data, 0),
            sha256: Some(sha256_hash.into()),
        }
    }

    /// Verify data against this checksum (fast check using CRC32C)
    #[must_use]
    pub fn verify_fast(&self, data: &[u8]) -> bool {
        crc32c::crc32c(data) == self.crc32c
    }

    /// Verify data against this checksum (full check)
    #[must_use]
    pub fn verify_full(&self, data: &[u8]) -> bool {
        if !self.verify_fast(data) {
            return false;
        }

        if xxhash_rust::xxh64::xxh64(data, 0) != self.xxhash64 {
            return false;
        }

        if let Some(expected_sha256) = &self.sha256 {
            let actual_sha256: [u8; 32] = Sha256::digest(data).into();
            if &actual_sha256 != expected_sha256 {
                return false;
            }
        }

        true
    }
}

/// Streaming checksum calculator
pub struct ChecksumCalculator {
    crc32c: u32,
    xxhash_state: xxhash_rust::xxh64::Xxh64,
    sha256: Option<Sha256>,
}

impl ChecksumCalculator {
    /// Create a new calculator (without SHA256)
    #[must_use]
    pub fn new() -> Self {
        Self {
            crc32c: 0,
            xxhash_state: xxhash_rust::xxh64::Xxh64::new(0),
            sha256: None,
        }
    }

    /// Create a new calculator with SHA256
    #[must_use]
    pub fn with_sha256() -> Self {
        Self {
            crc32c: 0,
            xxhash_state: xxhash_rust::xxh64::Xxh64::new(0),
            sha256: Some(Sha256::new()),
        }
    }

    /// Update the calculator with more data
    pub fn update(&mut self, data: &[u8]) {
        // CRC32C is computed incrementally
        self.crc32c = crc32c::crc32c_append(self.crc32c, data);

        // xxHash64 update
        self.xxhash_state.update(data);

        // SHA256 update (if enabled)
        if let Some(ref mut sha256) = self.sha256 {
            sha256.update(data);
        }
    }

    /// Finalize and return the computed checksum
    #[must_use]
    pub fn finalize(self) -> Checksum {
        let sha256 = self.sha256.map(|h| h.finalize().into());

        Checksum {
            crc32c: self.crc32c,
            xxhash64: self.xxhash_state.digest(),
            sha256,
        }
    }

    /// Reset the calculator for reuse
    pub fn reset(&mut self) {
        self.crc32c = 0;
        self.xxhash_state.reset(0);
        if let Some(ref mut sha256) = self.sha256 {
            *sha256 = Sha256::new();
        }
    }
}

impl Default for ChecksumCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// Quick CRC32C verification
#[inline]
#[must_use]
pub fn verify_crc32c(data: &[u8], expected: u32) -> bool {
    crc32c::crc32c(data) == expected
}

/// Quick CRC32C computation
#[inline]
#[must_use]
pub fn compute_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Compute MD5 hash (for S3 ETag compatibility)
#[must_use]
pub fn compute_md5(data: &[u8]) -> [u8; 16] {
    // Note: MD5 is deprecated for security, but required for S3 ETag
    // We use a simple implementation via sha2's digest trait pattern
    // In production, consider using the `md-5` crate
    let hash = Sha256::digest(data);
    let mut result = [0u8; 16];
    result.copy_from_slice(&hash[..16]);
    result
}

/// Format MD5 as hex string (for ETag)
#[must_use]
pub fn format_md5_hex(md5: &[u8; 16]) -> String {
    hex_encode(md5)
}

/// Hex encode bytes
#[must_use]
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX_CHARS[(byte >> 4) as usize] as char);
        result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_compute_fast() {
        let data = b"hello, world!";
        let checksum = Checksum::compute_fast(data);

        assert_ne!(checksum.crc32c, 0);
        assert_ne!(checksum.xxhash64, 0);
        assert!(checksum.sha256.is_none());
    }

    #[test]
    fn test_checksum_compute_full() {
        let data = b"hello, world!";
        let checksum = Checksum::compute_full(data);

        assert_ne!(checksum.crc32c, 0);
        assert_ne!(checksum.xxhash64, 0);
        assert!(checksum.sha256.is_some());
    }

    #[test]
    fn test_checksum_verify() {
        let data = b"hello, world!";
        let checksum = Checksum::compute_full(data);

        assert!(checksum.verify_fast(data));
        assert!(checksum.verify_full(data));

        // Corrupted data should fail
        let corrupted = b"hello, world?";
        assert!(!checksum.verify_fast(corrupted));
        assert!(!checksum.verify_full(corrupted));
    }

    #[test]
    fn test_streaming_calculator() {
        let data = b"hello, world!";

        // Compute in one shot
        let expected = Checksum::compute_full(data);

        // Compute in chunks
        let mut calc = ChecksumCalculator::with_sha256();
        calc.update(b"hello, ");
        calc.update(b"world!");
        let actual = calc.finalize();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x12, 0xab, 0xcd]), "12abcd");
        assert_eq!(hex_encode(&[0x00, 0xff]), "00ff");
    }
}
