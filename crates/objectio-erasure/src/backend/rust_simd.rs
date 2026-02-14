//! Pure Rust SIMD backend using reed-solomon-simd
//!
//! This backend provides portable erasure coding using the `reed-solomon-simd`
//! crate, which automatically uses SIMD instructions where available (SSE, AVX,
//! NEON).

use super::{
    BackendCapabilities, BackendResult, ErasureBackend, LocalGroup, LrcBackend, LrcConfig,
    LrcEncodedData,
};
use crate::ErasureError;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

/// MDS Reed-Solomon backend using reed-solomon-simd
///
/// This provides standard Maximum Distance Separable erasure coding.
/// Any k shards can reconstruct the original data.
pub struct RustSimdBackend {
    data_shards: u8,
    parity_shards: u8,
}

impl RustSimdBackend {
    /// Create a new MDS backend
    pub fn new(data_shards: u8, parity_shards: u8) -> Result<Self, ErasureError> {
        if data_shards == 0 {
            return Err(ErasureError::InvalidConfig(
                "data_shards must be > 0".into(),
            ));
        }
        if parity_shards == 0 {
            return Err(ErasureError::InvalidConfig(
                "parity_shards must be > 0".into(),
            ));
        }
        if data_shards as usize + parity_shards as usize > 255 {
            return Err(ErasureError::InvalidConfig(
                "total shards must be <= 255".into(),
            ));
        }
        Ok(Self {
            data_shards,
            parity_shards,
        })
    }
}

impl ErasureBackend for RustSimdBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            name: "rust_simd",
            supports_lrc: false,
            supports_simd: true,
            max_data_shards: 255,
            max_parity_shards: 255,
        }
    }

    fn data_shards(&self) -> usize {
        self.data_shards as usize
    }

    fn parity_shards(&self) -> usize {
        self.parity_shards as usize
    }

    fn encode(&self, data_shards: &[&[u8]], shard_size: usize) -> BackendResult<Vec<Vec<u8>>> {
        let k = self.data_shards as usize;
        let m = self.parity_shards as usize;

        if data_shards.len() != k {
            return Err(ErasureError::InvalidConfig(format!(
                "expected {} data shards, got {}",
                k,
                data_shards.len()
            )));
        }

        // Verify all shards are same size
        for (i, shard) in data_shards.iter().enumerate() {
            if shard.len() != shard_size {
                return Err(ErasureError::InvalidConfig(format!(
                    "shard {} has size {}, expected {}",
                    i,
                    shard.len(),
                    shard_size
                )));
            }
        }

        let mut encoder = ReedSolomonEncoder::new(k, m, shard_size)
            .map_err(|e| ErasureError::InvalidConfig(e.to_string()))?;

        for shard in data_shards {
            encoder
                .add_original_shard(shard)
                .map_err(|e| ErasureError::EncodingFailed(e.to_string()))?;
        }

        let result = encoder
            .encode()
            .map_err(|e| ErasureError::EncodingFailed(e.to_string()))?;

        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(k + m);

        // Add data shards
        for shard in data_shards {
            shards.push(shard.to_vec());
        }

        // Add parity shards
        for parity in result.recovery_iter() {
            shards.push(parity.to_vec());
        }

        Ok(shards)
    }

    fn decode(
        &self,
        shards: &[Option<&[u8]>],
        shard_size: usize,
        _missing_indices: &[usize],
    ) -> BackendResult<Vec<Vec<u8>>> {
        let k = self.data_shards as usize;
        let m = self.parity_shards as usize;

        let available = shards.iter().filter(|s| s.is_some()).count();
        if available < k {
            return Err(ErasureError::InsufficientShards {
                available,
                required: k,
            });
        }

        if shards.len() != k + m {
            return Err(ErasureError::InvalidConfig(format!(
                "expected {} shards, got {}",
                k + m,
                shards.len()
            )));
        }

        let mut decoder = ReedSolomonDecoder::new(k, m, shard_size)
            .map_err(|e| ErasureError::InvalidConfig(e.to_string()))?;

        // Add data shards (indices 0 to k-1) using add_original_shard
        for (i, shard) in shards.iter().enumerate().take(k) {
            if let Some(data) = shard {
                decoder
                    .add_original_shard(i, data)
                    .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?;
            }
        }

        // Add parity shards (indices k to k+m-1) using add_recovery_shard
        for i in 0..m {
            if let Some(data) = shards[k + i] {
                decoder
                    .add_recovery_shard(i, data)
                    .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?;
            }
        }

        let result = decoder
            .decode()
            .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?;

        // Build full shard list, filling in reconstructed shards
        let mut all_shards: Vec<Vec<u8>> = Vec::with_capacity(k + m);

        // Add data shards (original or reconstructed)
        for (i, shard) in shards.iter().enumerate().take(k) {
            if let Some(data) = shard {
                all_shards.push(data.to_vec());
            } else if let Some(restored) = result.restored_original(i) {
                all_shards.push(restored.to_vec());
            } else {
                return Err(ErasureError::DecodingFailed(format!(
                    "failed to restore data shard {}",
                    i
                )));
            }
        }

        // Add parity shards (original or placeholder)
        // Note: reed-solomon-simd focuses on restoring original shards
        // Parity reconstruction would require re-encoding
        for i in 0..m {
            if let Some(data) = shards[k + i] {
                all_shards.push(data.to_vec());
            } else {
                // For missing parity, we need to re-encode from data shards
                // For now, add placeholder - actual parity recovery would need encode
                all_shards.push(vec![0u8; shard_size]);
            }
        }

        Ok(all_shards)
    }

    fn verify(&self, shards: &[&[u8]]) -> BackendResult<bool> {
        let k = self.data_shards as usize;
        let m = self.parity_shards as usize;

        if shards.len() != k + m {
            return Ok(false);
        }

        // Check all shards have same size
        if let Some(first_len) = shards.first().map(|s| s.len()) {
            if !shards.iter().all(|s| s.len() == first_len) {
                return Ok(false);
            }

            // Re-encode data shards and compare parity
            let data_shards: Vec<&[u8]> = shards[..k].to_vec();
            let encoded = self.encode(&data_shards, first_len)?;

            // Compare parity shards
            for i in 0..m {
                if encoded[k + i] != shards[k + i] {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

/// LRC backend using reed-solomon-simd with layered encoding
///
/// This implements Locally Repairable Codes where:
/// - Data shards are divided into local groups
/// - Each group has a local parity (XOR of group)
/// - Global parity shards are Reed-Solomon over all data
pub struct RustSimdLrcBackend {
    config: LrcConfig,
}

impl RustSimdLrcBackend {
    /// Create a new LRC backend
    pub fn new(config: LrcConfig) -> Result<Self, ErasureError> {
        // Validate config
        if config.data_shards == 0 {
            return Err(ErasureError::InvalidConfig(
                "data_shards must be > 0".into(),
            ));
        }
        if config.local_parity_shards == 0 {
            return Err(ErasureError::InvalidConfig(
                "local_parity_shards must be > 0".into(),
            ));
        }
        if !config
            .data_shards
            .is_multiple_of(config.local_parity_shards)
        {
            return Err(ErasureError::InvalidConfig(
                "data_shards must be divisible by local_parity_shards".into(),
            ));
        }

        Ok(Self { config })
    }

    /// XOR all slices together
    fn xor_slices(slices: &[&[u8]], shard_size: usize) -> Vec<u8> {
        let mut result = vec![0u8; shard_size];
        for slice in slices {
            for (i, byte) in slice.iter().enumerate() {
                if i < shard_size {
                    result[i] ^= byte;
                }
            }
        }
        result
    }
}

impl ErasureBackend for RustSimdLrcBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            name: "rust_simd_lrc",
            supports_lrc: true,
            supports_simd: true,
            max_data_shards: 255,
            max_parity_shards: 255,
        }
    }

    fn data_shards(&self) -> usize {
        self.config.data_shards as usize
    }

    fn parity_shards(&self) -> usize {
        self.config.total_parity() as usize
    }

    fn encode(&self, data_shards: &[&[u8]], shard_size: usize) -> BackendResult<Vec<Vec<u8>>> {
        let result = self.encode_lrc(data_shards, shard_size)?;
        Ok(result.all_shards())
    }

    fn decode(
        &self,
        shards: &[Option<&[u8]>],
        shard_size: usize,
        missing_indices: &[usize],
    ) -> BackendResult<Vec<Vec<u8>>> {
        let k = self.config.data_shards as usize;
        let l = self.config.local_parity_shards as usize;
        let g = self.config.global_parity_shards as usize;
        let total = k + l + g;

        if shards.len() != total {
            return Err(ErasureError::InvalidConfig(format!(
                "expected {} shards, got {}",
                total,
                shards.len()
            )));
        }

        let mut reconstructed = Vec::with_capacity(missing_indices.len());
        let mut remaining_missing: Vec<usize> = missing_indices.to_vec();

        // Try local recovery first for each missing shard
        for &idx in missing_indices {
            if let Some(recovered) = self.decode_local(shards, shard_size, idx)? {
                reconstructed.push(recovered);
                remaining_missing.retain(|&x| x != idx);
            }
        }

        if remaining_missing.is_empty() {
            return Ok(reconstructed);
        }

        // Fall back to global RS recovery for remaining shards
        // Use all data + local parity as effective data for RS
        let effective_k = k + l;
        let available = shards[..effective_k].iter().filter(|s| s.is_some()).count();

        if available < k {
            return Err(ErasureError::InsufficientShards {
                available,
                required: k,
            });
        }

        // For global recovery, we need to use RS decoder
        let mut decoder = ReedSolomonDecoder::new(k, g, shard_size)
            .map_err(|e| ErasureError::InvalidConfig(e.to_string()))?;

        // Add available data shards
        for (i, shard) in shards.iter().enumerate().take(k) {
            if let Some(data) = shard {
                decoder
                    .add_original_shard(i, data)
                    .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?;
            }
        }

        // Add global parity shards
        for (i, shard) in shards.iter().enumerate().skip(k + l).take(g) {
            if let Some(data) = shard {
                decoder
                    .add_original_shard(i - l, data) // Adjust index for decoder
                    .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?;
            }
        }

        let result = decoder
            .decode()
            .map_err(|e| ErasureError::DecodingFailed(e.to_string()))?;

        // Get remaining missing shards from global recovery
        for idx in remaining_missing {
            if idx < k {
                if let Some(restored) = result.restored_original(idx) {
                    reconstructed.push(restored.to_vec());
                } else {
                    return Err(ErasureError::DecodingFailed(format!(
                        "failed to restore shard {}",
                        idx
                    )));
                }
            } else {
                return Err(ErasureError::DecodingFailed(format!(
                    "cannot restore non-data shard {} via global recovery",
                    idx
                )));
            }
        }

        Ok(reconstructed)
    }

    fn verify(&self, shards: &[&[u8]]) -> BackendResult<bool> {
        let k = self.config.data_shards as usize;
        let l = self.config.local_parity_shards as usize;
        let g = self.config.global_parity_shards as usize;

        if shards.len() != k + l + g {
            return Ok(false);
        }

        // Check all shards have same size
        if let Some(first_len) = shards.first().map(|s| s.len()) {
            if !shards.iter().all(|s| s.len() == first_len) {
                return Ok(false);
            }

            // Verify local parity groups
            let group_size = self.config.local_group_size as usize;
            for group_idx in 0..l {
                let start = group_idx * group_size;
                let group_data: Vec<&[u8]> = shards[start..start + group_size].to_vec();
                let expected_local = Self::xor_slices(&group_data, first_len);
                if expected_local != shards[k + group_idx] {
                    return Ok(false);
                }
            }

            // Verify global parity
            let data_shards: Vec<&[u8]> = shards[..k].to_vec();
            let mds_backend = RustSimdBackend::new(k as u8, g as u8)?;
            let encoded = mds_backend.encode(&data_shards, first_len)?;
            for i in 0..g {
                if encoded[k + i] != shards[k + l + i] {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

impl LrcBackend for RustSimdLrcBackend {
    fn lrc_config(&self) -> &LrcConfig {
        &self.config
    }

    fn encode_lrc(
        &self,
        data_shards: &[&[u8]],
        shard_size: usize,
    ) -> BackendResult<LrcEncodedData> {
        let k = self.config.data_shards as usize;
        let l = self.config.local_parity_shards as usize;
        let g = self.config.global_parity_shards as usize;
        let group_size = self.config.local_group_size as usize;

        if data_shards.len() != k {
            return Err(ErasureError::InvalidConfig(format!(
                "expected {} data shards, got {}",
                k,
                data_shards.len()
            )));
        }

        // Verify all shards are same size
        for (i, shard) in data_shards.iter().enumerate() {
            if shard.len() != shard_size {
                return Err(ErasureError::InvalidConfig(format!(
                    "shard {} has size {}, expected {}",
                    i,
                    shard.len(),
                    shard_size
                )));
            }
        }

        // Step 1: Compute local parity for each group (XOR)
        let mut local_parity_shards = Vec::with_capacity(l);
        for group_idx in 0..l {
            let start = group_idx * group_size;
            let end = start + group_size;
            let group_data: Vec<&[u8]> = data_shards[start..end].to_vec();
            let local_parity = Self::xor_slices(&group_data, shard_size);
            local_parity_shards.push(local_parity);
        }

        // Step 2: Compute global parity using Reed-Solomon over all data shards
        let mut encoder = ReedSolomonEncoder::new(k, g, shard_size)
            .map_err(|e| ErasureError::EncodingFailed(e.to_string()))?;

        for shard in data_shards {
            encoder
                .add_original_shard(shard)
                .map_err(|e| ErasureError::EncodingFailed(e.to_string()))?;
        }

        let result = encoder
            .encode()
            .map_err(|e| ErasureError::EncodingFailed(e.to_string()))?;

        let global_parity_shards: Vec<Vec<u8>> =
            result.recovery_iter().map(|p| p.to_vec()).collect();

        Ok(LrcEncodedData {
            data_shards: data_shards.iter().map(|s| s.to_vec()).collect(),
            local_parity_shards,
            global_parity_shards,
        })
    }

    fn decode_local(
        &self,
        shards: &[Option<&[u8]>],
        shard_size: usize,
        missing_index: usize,
    ) -> BackendResult<Option<Vec<u8>>> {
        // Check if missing shard is in a local group
        if let Some(group) = self.local_group(missing_index) {
            // Check if all other shards in the group are available
            let mut all_available = true;

            for &idx in &group.data_shard_indices {
                if idx != missing_index && shards.get(idx).is_none_or(|s| s.is_none()) {
                    all_available = false;
                    break;
                }
            }

            // Check local parity is available
            if shards
                .get(group.local_parity_index)
                .is_none_or(|s| s.is_none())
            {
                all_available = false;
            }

            if all_available {
                // XOR all available shards to recover the missing one
                let mut recovered = vec![0u8; shard_size];

                // XOR data shards in group (except missing)
                for &idx in &group.data_shard_indices {
                    if idx != missing_index
                        && let Some(Some(data)) = shards.get(idx)
                    {
                        for (i, byte) in data.iter().enumerate() {
                            if i < shard_size {
                                recovered[i] ^= byte;
                            }
                        }
                    }
                }

                // XOR with local parity
                if let Some(Some(parity)) = shards.get(group.local_parity_index) {
                    for (i, byte) in parity.iter().enumerate() {
                        if i < shard_size {
                            recovered[i] ^= byte;
                        }
                    }
                }

                return Ok(Some(recovered));
            }
        }

        Ok(None)
    }

    fn local_group(&self, shard_index: usize) -> Option<LocalGroup> {
        let k = self.config.data_shards as usize;
        let l = self.config.local_parity_shards as usize;
        let group_size = self.config.local_group_size as usize;

        if shard_index >= k + l {
            // Global parity shard - no local group
            return None;
        }

        let group_index = if shard_index < k {
            // Data shard
            shard_index / group_size
        } else {
            // Local parity shard
            shard_index - k
        };

        let start = group_index * group_size;
        let data_shard_indices: Vec<usize> = (start..start + group_size).collect();
        let local_parity_index = k + group_index;

        Some(LocalGroup {
            group_index: group_index as u8,
            data_shard_indices,
            local_parity_index,
        })
    }

    fn can_recover_locally(&self, available: &[bool], missing_index: usize) -> bool {
        if let Some(group) = self.local_group(missing_index) {
            // Check all other members of the local group are available
            for &idx in &group.data_shard_indices {
                if idx != missing_index && !available.get(idx).copied().unwrap_or(false) {
                    return false;
                }
            }
            // Check local parity is available
            available
                .get(group.local_parity_index)
                .copied()
                .unwrap_or(false)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mds_encode_decode() {
        let backend = RustSimdBackend::new(4, 2).unwrap();

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8; 1024]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|d| d.as_slice()).collect();

        let shards = backend.encode(&data_refs, 1024).unwrap();
        assert_eq!(shards.len(), 6);

        // All shards present - decode should return all 6 shards
        let shard_refs: Vec<Option<&[u8]>> = shards.iter().map(|s| Some(s.as_slice())).collect();
        let decoded = backend.decode(&shard_refs, 1024, &[]).unwrap();
        assert_eq!(decoded.len(), 6);
        // Data shards should match original
        for i in 0..4 {
            assert_eq!(decoded[i], data[i]);
        }
    }

    #[test]
    fn test_mds_decode_with_missing() {
        let backend = RustSimdBackend::new(4, 2).unwrap();

        let data: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8; 1024]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|d| d.as_slice()).collect();

        let shards = backend.encode(&data_refs, 1024).unwrap();

        // Simulate missing shard 0
        let mut shard_refs: Vec<Option<&[u8]>> =
            shards.iter().map(|s| Some(s.as_slice())).collect();
        shard_refs[0] = None;

        let decoded = backend.decode(&shard_refs, 1024, &[0]).unwrap();
        // decode returns all shards, not just reconstructed
        assert_eq!(decoded.len(), 6);
        // Reconstructed shard should match original
        assert_eq!(decoded[0], data[0]);
    }

    #[test]
    fn test_lrc_encode() {
        let config = LrcConfig::new(6, 2, 2);
        let backend = RustSimdLrcBackend::new(config).unwrap();

        let data: Vec<Vec<u8>> = (0..6).map(|i| vec![i as u8; 1024]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|d| d.as_slice()).collect();

        let encoded = backend.encode_lrc(&data_refs, 1024).unwrap();

        assert_eq!(encoded.data_shards.len(), 6);
        assert_eq!(encoded.local_parity_shards.len(), 2);
        assert_eq!(encoded.global_parity_shards.len(), 2);
    }

    #[test]
    fn test_lrc_local_recovery() {
        let config = LrcConfig::new(6, 2, 2);
        let backend = RustSimdLrcBackend::new(config).unwrap();

        let data: Vec<Vec<u8>> = (0..6).map(|i| vec![i as u8; 1024]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|d| d.as_slice()).collect();

        let all_shards = backend.encode(&data_refs, 1024).unwrap();

        // Simulate missing shard 1 (in group 0)
        let mut shard_refs: Vec<Option<&[u8]>> =
            all_shards.iter().map(|s| Some(s.as_slice())).collect();
        shard_refs[1] = None;

        // Should be able to recover locally
        let available: Vec<bool> = shard_refs.iter().map(|s| s.is_some()).collect();
        assert!(backend.can_recover_locally(&available, 1));

        let recovered = backend.decode_local(&shard_refs, 1024, 1).unwrap();
        assert!(recovered.is_some());
        assert_eq!(recovered.unwrap(), data[1]);
    }

    #[test]
    fn test_lrc_verify() {
        let config = LrcConfig::new(6, 2, 2);
        let backend = RustSimdLrcBackend::new(config).unwrap();

        let data: Vec<Vec<u8>> = (0..6).map(|i| vec![i as u8; 1024]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|d| d.as_slice()).collect();

        let all_shards = backend.encode(&data_refs, 1024).unwrap();
        let shard_refs: Vec<&[u8]> = all_shards.iter().map(|s| s.as_slice()).collect();

        assert!(backend.verify(&shard_refs).unwrap());
    }
}
