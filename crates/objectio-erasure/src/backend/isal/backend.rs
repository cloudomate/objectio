//! ISA-L backend implementation
//!
//! Uses the `erasure-isa-l` crate for hardware-accelerated erasure coding.
//! The crate bundles ISA-L source and compiles it during build, so no system
//! library installation is required (but nasm, autoconf, automake, libtool are needed).

use crate::ErasureError;
use crate::backend::{
    BackendCapabilities, BackendResult, ErasureBackend, LocalGroup, LrcBackend, LrcConfig,
    LrcEncodedData,
};
use erasure_isa_l::erasure::ErasureCode;
use std::num::NonZeroUsize;

/// ISA-L MDS Reed-Solomon backend
///
/// Uses Intel ISA-L for hardware-accelerated erasure coding on x86/x86_64.
/// Works on both Intel and AMD processors (uses standard AVX/AVX2/AVX-512).
pub struct IsalBackend {
    k: usize,
    m: usize,
    encoder: ErasureCode,
}

impl IsalBackend {
    /// Create a new ISA-L backend
    ///
    /// # Arguments
    /// * `data_shards` - Number of data shards (k)
    /// * `parity_shards` - Number of parity shards (m)
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

        let k = data_shards as usize;
        let m = parity_shards as usize;

        if k + m > 255 {
            return Err(ErasureError::InvalidConfig("k + m must be <= 255".into()));
        }

        // Use Reed-Solomon (Vandermonde) matrix - more reliable than Cauchy
        // in some ISA-L versions
        let k_nz = NonZeroUsize::new(k).unwrap(); // Safe: we checked k > 0 above
        let m_nz = NonZeroUsize::new(m).unwrap(); // Safe: we checked m > 0 above
        let encoder = ErasureCode::with_reed_solomon(k_nz, m_nz).map_err(|e| {
            ErasureError::InvalidConfig(format!("failed to create ISA-L encoder: {}", e))
        })?;

        Ok(Self { k, m, encoder })
    }
}

impl ErasureBackend for IsalBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            name: "isal",
            supports_lrc: false,
            supports_simd: true,
            max_data_shards: 255,
            max_parity_shards: 255,
        }
    }

    fn data_shards(&self) -> usize {
        self.k
    }

    fn parity_shards(&self) -> usize {
        self.m
    }

    fn encode(&self, data_shards: &[&[u8]], shard_size: usize) -> BackendResult<Vec<Vec<u8>>> {
        if data_shards.len() != self.k {
            return Err(ErasureError::InvalidConfig(format!(
                "expected {} data shards, got {}",
                self.k,
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

        // Prepare mutable parity buffers
        let mut parity: Vec<Vec<u8>> = (0..self.m).map(|_| vec![0u8; shard_size]).collect();

        // Encode: data is read-only, parity is written
        self.encoder
            .encode(data_shards, &mut parity)
            .map_err(|e| ErasureError::EncodingFailed(format!("ISA-L encode failed: {}", e)))?;

        // Build result: data shards + parity shards
        let mut result = Vec::with_capacity(self.k + self.m);
        for shard in data_shards {
            result.push(shard.to_vec());
        }
        result.extend(parity);

        Ok(result)
    }

    fn decode(
        &self,
        shards: &[Option<&[u8]>],
        shard_size: usize,
        missing_indices: &[usize],
    ) -> BackendResult<Vec<Vec<u8>>> {
        if shards.len() != self.k + self.m {
            return Err(ErasureError::InvalidConfig(format!(
                "expected {} shards, got {}",
                self.k + self.m,
                shards.len()
            )));
        }

        let available = shards.iter().filter(|s| s.is_some()).count();
        if available < self.k {
            return Err(ErasureError::InsufficientShards {
                available,
                required: self.k,
            });
        }

        // If all data shards are present, just copy them and re-encode parity
        let data_all_present = (0..self.k).all(|i| shards[i].is_some());
        if data_all_present {
            let mut result = Vec::with_capacity(self.k + self.m);
            for i in 0..self.k {
                result.push(shards[i].unwrap().to_vec());
            }
            // Re-encode parity
            let data_refs: Vec<&[u8]> = result.iter().map(|s| s.as_slice()).collect();
            let encoded = self.encode(&data_refs, shard_size)?;
            for i in self.k..self.k + self.m {
                result.push(encoded[i].clone());
            }
            return Ok(result);
        }

        // Need to reconstruct missing data shards
        // Prepare data and parity arrays
        let mut data: Vec<Vec<u8>> = Vec::with_capacity(self.k);
        let mut parity: Vec<Vec<u8>> = Vec::with_capacity(self.m);

        for i in 0..self.k {
            if let Some(shard) = shards[i] {
                data.push(shard.to_vec());
            } else {
                data.push(vec![0u8; shard_size]);
            }
        }

        for i in self.k..self.k + self.m {
            if let Some(shard) = shards[i] {
                parity.push(shard.to_vec());
            } else {
                parity.push(vec![0u8; shard_size]);
            }
        }

        // Build erasure list (indices of missing shards in the combined array)
        let erasures: Vec<usize> = missing_indices.to_vec();

        // Decode
        self.encoder
            .decode(&mut data, &mut parity, erasures)
            .map_err(|e| ErasureError::DecodingFailed(format!("ISA-L decode failed: {}", e)))?;

        // Build result
        let mut result = Vec::with_capacity(self.k + self.m);
        result.extend(data);
        result.extend(parity);

        Ok(result)
    }

    fn verify(&self, shards: &[&[u8]]) -> BackendResult<bool> {
        if shards.len() != self.k + self.m {
            return Ok(false);
        }

        // Check all shards have same size
        if let Some(first_len) = shards.first().map(|s| s.len()) {
            if !shards.iter().all(|s| s.len() == first_len) {
                return Ok(false);
            }

            // Re-encode and compare parity
            let data_shards: Vec<&[u8]> = shards[..self.k].to_vec();
            let encoded = self.encode(&data_shards, first_len)?;

            for i in 0..self.m {
                if encoded[self.k + i] != shards[self.k + i] {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

// Safety: IsalBackend contains owned data and ErasureCode which is Send+Sync
unsafe impl Send for IsalBackend {}
unsafe impl Sync for IsalBackend {}

/// ISA-L LRC backend
///
/// Combines ISA-L for global parity with XOR for local parity.
pub struct IsalLrcBackend {
    config: LrcConfig,
    global_encoder: IsalBackend,
}

impl IsalLrcBackend {
    /// Create a new ISA-L LRC backend
    pub fn new(config: LrcConfig) -> Result<Self, ErasureError> {
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
        if config.data_shards % config.local_parity_shards != 0 {
            return Err(ErasureError::InvalidConfig(
                "data_shards must be divisible by local_parity_shards".into(),
            ));
        }

        // Create ISA-L backend for global parity
        let global_encoder = IsalBackend::new(config.data_shards, config.global_parity_shards)?;

        Ok(Self {
            config,
            global_encoder,
        })
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

impl ErasureBackend for IsalLrcBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            name: "isal_lrc",
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
        // Use rust_simd LRC backend for decode (shared logic)
        let rust_lrc = crate::backend::RustSimdLrcBackend::new(self.config)?;
        rust_lrc.decode(shards, shard_size, missing_indices)
    }

    fn verify(&self, shards: &[&[u8]]) -> BackendResult<bool> {
        let rust_lrc = crate::backend::RustSimdLrcBackend::new(self.config)?;
        rust_lrc.verify(shards)
    }
}

impl LrcBackend for IsalLrcBackend {
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
        let group_size = self.config.local_group_size as usize;

        if data_shards.len() != k {
            return Err(ErasureError::InvalidConfig(format!(
                "expected {} data shards, got {}",
                k,
                data_shards.len()
            )));
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

        // Step 2: Compute global parity using ISA-L
        let encoded = self.global_encoder.encode(data_shards, shard_size)?;
        let global_parity_shards: Vec<Vec<u8>> = encoded[k..].to_vec();

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
        // Local decode is pure XOR, same as rust_simd
        let rust_lrc = crate::backend::RustSimdLrcBackend::new(self.config)?;
        rust_lrc.decode_local(shards, shard_size, missing_index)
    }

    fn local_group(&self, shard_index: usize) -> Option<LocalGroup> {
        let rust_lrc = crate::backend::RustSimdLrcBackend::new(self.config).ok()?;
        rust_lrc.local_group(shard_index)
    }

    fn can_recover_locally(&self, available: &[bool], missing_index: usize) -> bool {
        if let Ok(rust_lrc) = crate::backend::RustSimdLrcBackend::new(self.config) {
            rust_lrc.can_recover_locally(available, missing_index)
        } else {
            false
        }
    }
}

// Safety: IsalLrcBackend contains owned Vecs and IsalBackend
unsafe impl Send for IsalLrcBackend {}
unsafe impl Sync for IsalLrcBackend {}
