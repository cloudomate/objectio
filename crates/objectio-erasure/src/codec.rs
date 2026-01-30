//! Reed-Solomon encoder/decoder
//!
//! This module provides both a high-level `ErasureCodec` API and low-level
//! backend access via the `backend` module.
//!
//! # High-Level API
//!
//! ```
//! use objectio_erasure::ErasureCodec;
//! use objectio_common::ErasureConfig;
//!
//! let codec = ErasureCodec::new(ErasureConfig::new(4, 2)).unwrap();
//! let data = b"Hello, World!";
//! let shards = codec.encode(data).unwrap();
//! ```
//!
//! # Backend API
//!
//! For more control, use the backend directly:
//!
//! ```
//! use objectio_erasure::backend::{BackendConfig, BackendFactory};
//!
//! let config = BackendConfig::mds(4, 2);
//! let backend = BackendFactory::create_mds(&config).unwrap();
//! ```

use crate::backend::{BackendConfig, BackendFactory, ErasureBackend, LrcBackend};
use objectio_common::{ErasureConfig, ErasureType, Error as CommonError, Result};
use std::sync::Arc;
use thiserror::Error;

/// Errors specific to erasure coding operations
#[derive(Debug, Error)]
pub enum ErasureError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("encoding failed: {0}")]
    EncodingFailed(String),

    #[error("decoding failed: {0}")]
    DecodingFailed(String),

    #[error("insufficient shards: have {available}, need {required}")]
    InsufficientShards { available: usize, required: usize },

    #[error("shard size mismatch")]
    ShardSizeMismatch,

    #[error("LRC configuration required")]
    LrcConfigRequired,
}

impl From<ErasureError> for CommonError {
    fn from(e: ErasureError) -> Self {
        CommonError::ErasureCoding(e.to_string())
    }
}

/// Backend wrapper for unified handling of MDS and LRC
enum CodecBackend {
    Mds(Arc<dyn ErasureBackend>),
    Lrc(Arc<dyn LrcBackend>),
}

/// Erasure codec supporting both MDS Reed-Solomon and LRC
///
/// This is the high-level API for erasure coding. It automatically selects
/// the best backend based on the configuration and platform.
///
/// # MDS Mode (default)
///
/// Standard Reed-Solomon coding where any k shards can reconstruct the data.
///
/// # LRC Mode
///
/// Locally Repairable Codes with local parity groups for faster single-shard
/// recovery with reduced bandwidth.
pub struct ErasureCodec {
    config: ErasureConfig,
    backend: CodecBackend,
}

impl ErasureCodec {
    /// Create a new erasure codec with the given configuration
    ///
    /// Automatically selects the best backend based on the configuration
    /// and platform (ISA-L on x86 if available, rust_simd otherwise).
    pub fn new(config: ErasureConfig) -> Result<Self> {
        if config.data_shards == 0 {
            return Err(ErasureError::InvalidConfig("data_shards must be > 0".into()).into());
        }
        if config.parity_shards == 0 {
            return Err(ErasureError::InvalidConfig("parity_shards must be > 0".into()).into());
        }

        let backend = match config.ec_type {
            ErasureType::Mds => {
                let backend_config = BackendConfig::mds(config.data_shards, config.parity_shards);
                let mds_backend = BackendFactory::create_mds(&backend_config)?;
                CodecBackend::Mds(mds_backend)
            }
            ErasureType::Lrc {
                local_parity,
                global_parity,
            } => {
                let lrc_config =
                    crate::backend::LrcConfig::new(config.data_shards, local_parity, global_parity);
                let backend_config = BackendConfig::lrc(lrc_config);
                let lrc_backend = BackendFactory::create_lrc(&backend_config)?;
                CodecBackend::Lrc(lrc_backend)
            }
        };

        Ok(Self { config, backend })
    }

    /// Get the configuration
    #[must_use]
    pub const fn config(&self) -> ErasureConfig {
        self.config
    }

    /// Get the number of data shards
    #[must_use]
    pub fn data_shards(&self) -> usize {
        match &self.backend {
            CodecBackend::Mds(b) => b.data_shards(),
            CodecBackend::Lrc(b) => b.data_shards(),
        }
    }

    /// Get the number of parity shards
    #[must_use]
    pub fn parity_shards(&self) -> usize {
        match &self.backend {
            CodecBackend::Mds(b) => b.parity_shards(),
            CodecBackend::Lrc(b) => b.parity_shards(),
        }
    }

    /// Get the total number of shards (data + parity)
    #[must_use]
    pub fn total_shards(&self) -> usize {
        self.data_shards() + self.parity_shards()
    }

    /// Check if this codec uses LRC
    #[must_use]
    pub fn is_lrc(&self) -> bool {
        matches!(self.backend, CodecBackend::Lrc(_))
    }

    /// Encode data into k data shards and m parity shards
    ///
    /// The input data is split into k equal-sized chunks, then m parity
    /// shards are computed. Returns a vector of k+m shards.
    ///
    /// For LRC mode, this includes both local and global parity shards.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let k = self.data_shards();

        // Calculate shard size (pad to multiple of k, minimum 64 bytes for SIMD)
        let shard_size = ((data.len() + k - 1) / k).max(64);
        let padded_size = shard_size * k;

        // Create padded data
        let mut padded = vec![0u8; padded_size];
        padded[..data.len()].copy_from_slice(data);

        // Split into data shards
        let data_shards: Vec<&[u8]> = (0..k)
            .map(|i| &padded[i * shard_size..(i + 1) * shard_size])
            .collect();

        // Encode using backend
        let shards = match &self.backend {
            CodecBackend::Mds(backend) => backend
                .encode(&data_shards, shard_size)
                .map_err(|e| ErasureError::EncodingFailed(e.to_string()))?,
            CodecBackend::Lrc(backend) => {
                let encoded = backend
                    .encode_lrc(&data_shards, shard_size)
                    .map_err(|e| ErasureError::EncodingFailed(e.to_string()))?;
                encoded.all_shards()
            }
        };

        Ok(shards)
    }

    /// Decode shards back to original data
    ///
    /// Takes a vector of Option<Vec<u8>> where None represents missing shards.
    /// At least k shards must be present to reconstruct the data.
    ///
    /// For LRC mode, this will attempt local recovery first (using only the
    /// local parity group) before falling back to global recovery.
    pub fn decode(&self, shards: &mut [Option<Vec<u8>>], original_size: usize) -> Result<Vec<u8>> {
        let k = self.data_shards();

        // Count available shards
        let available = shards.iter().filter(|s| s.is_some()).count();
        if available < k {
            return Err(ErasureError::InsufficientShards {
                available,
                required: k,
            }
            .into());
        }

        // Get shard size from first available shard
        let shard_size = shards
            .iter()
            .find_map(|s| s.as_ref().map(|v| v.len()))
            .ok_or_else(|| ErasureError::InsufficientShards {
                available: 0,
                required: k,
            })?;

        // If all data shards are present, just concatenate them
        let data_shards_ok = shards[..k].iter().all(|s| s.is_some());
        if data_shards_ok {
            let mut result = Vec::with_capacity(k * shard_size);
            for shard in shards.iter().take(k) {
                if let Some(data) = shard {
                    result.extend_from_slice(data);
                }
            }
            result.truncate(original_size);
            return Ok(result);
        }

        // Find missing indices
        let missing_indices: Vec<usize> = shards
            .iter()
            .enumerate()
            .filter_map(|(i, s)| if s.is_none() { Some(i) } else { None })
            .collect();

        // Decode using backend
        let decoded = match &self.backend {
            CodecBackend::Mds(backend) => {
                let shard_refs: Vec<Option<&[u8]>> = shards
                    .iter()
                    .map(|s| s.as_ref().map(|v| v.as_slice()))
                    .collect();
                backend
                    .decode(&shard_refs, shard_size, &missing_indices)
                    .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?
            }
            CodecBackend::Lrc(backend) => {
                // For LRC, try local recovery for single missing shards first
                // We'll store recovered shards separately to avoid borrow issues
                let mut recovered_shards: Vec<Option<Vec<u8>>> = vec![None; self.total_shards()];
                let mut remaining_missing = missing_indices.clone();

                // First pass: try local recovery
                for &idx in &missing_indices {
                    if idx < k {
                        // Build shard refs including any previously recovered shards
                        let shard_refs: Vec<Option<&[u8]>> = (0..self.total_shards())
                            .map(|i| {
                                if let Some(ref rec) = recovered_shards[i] {
                                    Some(rec.as_slice())
                                } else {
                                    shards[i].as_ref().map(|v| v.as_slice())
                                }
                            })
                            .collect();

                        // Try local recovery for data shard
                        if let Ok(Some(recovered)) =
                            backend.decode_local(&shard_refs, shard_size, idx)
                        {
                            recovered_shards[idx] = Some(recovered);
                            remaining_missing.retain(|&i| i != idx);
                        }
                    }
                }

                // Build final shard refs with recovered shards
                let final_shard_refs: Vec<Option<&[u8]>> = (0..self.total_shards())
                    .map(|i| {
                        if let Some(ref rec) = recovered_shards[i] {
                            Some(rec.as_slice())
                        } else {
                            shards[i].as_ref().map(|v| v.as_slice())
                        }
                    })
                    .collect();

                // If still have missing data shards, use global recovery
                if remaining_missing.iter().any(|&i| i < k) {
                    backend
                        .decode(&final_shard_refs, shard_size, &remaining_missing)
                        .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?
                } else {
                    // All data shards recovered, build result
                    (0..self.total_shards())
                        .map(|i| {
                            if let Some(rec) = recovered_shards[i].take() {
                                rec
                            } else if let Some(ref orig) = shards[i] {
                                orig.clone()
                            } else {
                                vec![0u8; shard_size]
                            }
                        })
                        .collect()
                }
            }
        };

        // Build output from data shards
        let mut output = Vec::with_capacity(k * shard_size);
        for i in 0..k {
            output.extend_from_slice(&decoded[i]);
        }

        output.truncate(original_size);
        Ok(output)
    }

    /// Verify that shards are consistent
    ///
    /// Re-encodes the data shards and compares parity to verify integrity.
    pub fn verify(&self, shards: &[Vec<u8>]) -> Result<bool> {
        let total = self.total_shards();

        if shards.len() != total {
            return Ok(false);
        }

        // Check all shards have same size
        if let Some(first_len) = shards.first().map(Vec::len) {
            if !shards.iter().all(|s| s.len() == first_len) {
                return Ok(false);
            }
        }

        // Convert to slice references
        let shard_refs: Vec<&[u8]> = shards.iter().map(|s| s.as_slice()).collect();

        // Verify using backend
        match &self.backend {
            CodecBackend::Mds(backend) => backend
                .verify(&shard_refs)
                .map_err(|e| ErasureError::EncodingFailed(e.to_string()).into()),
            CodecBackend::Lrc(backend) => backend
                .verify(&shard_refs)
                .map_err(|e| ErasureError::EncodingFailed(e.to_string()).into()),
        }
    }

    /// Try to recover a single missing shard using local parity (LRC only)
    ///
    /// Returns `None` if local recovery is not possible (e.g., MDS mode or
    /// multiple failures in the local group).
    pub fn try_local_recovery(
        &self,
        shards: &[Option<&[u8]>],
        shard_size: usize,
        missing_index: usize,
    ) -> Result<Option<Vec<u8>>> {
        match &self.backend {
            CodecBackend::Mds(_) => Ok(None),
            CodecBackend::Lrc(backend) => backend
                .decode_local(shards, shard_size, missing_index)
                .map_err(|e| ErasureError::DecodingFailed(e.to_string()).into()),
        }
    }

    /// Check if a missing shard can be recovered using local parity only
    ///
    /// This is useful for repair scheduling to prioritize local repairs.
    pub fn can_recover_locally(&self, available: &[bool], missing_index: usize) -> bool {
        match &self.backend {
            CodecBackend::Mds(_) => false,
            CodecBackend::Lrc(backend) => backend.can_recover_locally(available, missing_index),
        }
    }
}

impl Default for ErasureCodec {
    fn default() -> Self {
        Self::new(ErasureConfig::default()).expect("default config is valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_mds() {
        let codec = ErasureCodec::new(ErasureConfig::new(4, 2)).unwrap();
        let data = b"Hello, World! This is a test of erasure coding.";

        let shards = codec.encode(data).unwrap();
        assert_eq!(shards.len(), 6); // 4 data + 2 parity

        // All shards should be same size
        let shard_size = shards[0].len();
        assert!(shards.iter().all(|s| s.len() == shard_size));

        // Decode with all shards present
        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let decoded = codec.decode(&mut shard_opts, data.len()).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_decode_with_missing_mds() {
        let codec = ErasureCodec::new(ErasureConfig::new(4, 2)).unwrap();
        let data = b"Hello, World! This is a test of erasure coding with recovery.";

        let shards = codec.encode(data).unwrap();
        assert_eq!(shards.len(), 6);

        // Remove 2 shards (within parity tolerance)
        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shard_opts[1] = None; // Remove one data shard
        shard_opts[4] = None; // Remove one parity shard

        let decoded = codec.decode(&mut shard_opts, data.len()).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_insufficient_shards() {
        let codec = ErasureCodec::new(ErasureConfig::new(4, 2)).unwrap();

        // Only 3 shards when we need 4
        let mut shards: Vec<Option<Vec<u8>>> = vec![
            Some(vec![0u8; 64]),
            Some(vec![0u8; 64]),
            Some(vec![0u8; 64]),
            None,
            None,
            None,
        ];

        let result = codec.decode(&mut shards, 64);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_decode_lrc() {
        // LRC with 6 data shards, 2 local parity (groups of 3), 2 global parity
        let config = ErasureConfig::lrc(6, 2, 2);
        let codec = ErasureCodec::new(config).unwrap();

        assert!(codec.is_lrc());
        assert_eq!(codec.data_shards(), 6);
        assert_eq!(codec.parity_shards(), 4); // 2 local + 2 global

        let data = b"Testing LRC erasure coding with local and global parity shards.";

        let shards = codec.encode(data).unwrap();
        assert_eq!(shards.len(), 10); // 6 data + 2 local + 2 global

        // Decode with all shards present
        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let decoded = codec.decode(&mut shard_opts, data.len()).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_lrc_local_recovery() {
        // LRC with 6 data shards, 2 local parity (groups of 3), 2 global parity
        let config = ErasureConfig::lrc(6, 2, 2);
        let codec = ErasureCodec::new(config).unwrap();

        let data = b"Testing LRC local recovery - should only need local group shards.";

        let shards = codec.encode(data).unwrap();

        // Remove one shard from first group (indices 0-2 are group 0)
        let mut shard_opts: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        shard_opts[1] = None; // Remove data shard 1

        // Should be able to recover using local parity
        let decoded = codec.decode(&mut shard_opts, data.len()).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_verify_mds() {
        let codec = ErasureCodec::new(ErasureConfig::new(4, 2)).unwrap();
        let data = b"Data to verify with erasure coding.";

        let shards = codec.encode(data).unwrap();
        assert!(codec.verify(&shards).unwrap());

        // Corrupt a shard and verify should fail
        let mut corrupted = shards.clone();
        corrupted[0][0] ^= 0xFF;
        assert!(!codec.verify(&corrupted).unwrap());
    }

    #[test]
    fn test_codec_helper_methods() {
        let mds_codec = ErasureCodec::new(ErasureConfig::new(4, 2)).unwrap();
        assert!(!mds_codec.is_lrc());
        assert_eq!(mds_codec.data_shards(), 4);
        assert_eq!(mds_codec.parity_shards(), 2);
        assert_eq!(mds_codec.total_shards(), 6);

        let lrc_codec = ErasureCodec::new(ErasureConfig::lrc(6, 2, 2)).unwrap();
        assert!(lrc_codec.is_lrc());
        assert_eq!(lrc_codec.data_shards(), 6);
        assert_eq!(lrc_codec.parity_shards(), 4);
        assert_eq!(lrc_codec.total_shards(), 10);
    }
}
