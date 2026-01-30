//! Backend factory for creating erasure coding implementations
//!
//! Provides automatic backend selection based on platform capabilities
//! and feature flags.

use super::{
    BackendCapabilities, ErasureBackend, LrcBackend, LrcConfig, RustSimdBackend,
    RustSimdLrcBackend,
};
use crate::ErasureError;
use std::sync::Arc;

/// Backend type selection
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BackendType {
    /// Intel ISA-L (x86/x86_64 only, requires `isal` feature)
    IsaL,
    /// Pure Rust with SIMD (portable, default)
    #[default]
    RustSimd,
    /// Auto-select best available backend for this platform
    Auto,
}

impl BackendType {
    /// Get the backend type name
    pub const fn name(&self) -> &'static str {
        match self {
            Self::IsaL => "isal",
            Self::RustSimd => "rust_simd",
            Self::Auto => "auto",
        }
    }
}

impl std::fmt::Display for BackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::str::FromStr for BackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "isal" | "isa-l" | "intel" => Ok(Self::IsaL),
            "rust_simd" | "rust-simd" | "rust" | "simd" => Ok(Self::RustSimd),
            "auto" | "" => Ok(Self::Auto),
            _ => Err(format!("unknown backend type: {}", s)),
        }
    }
}

/// Configuration for creating a backend
#[derive(Clone, Debug)]
pub struct BackendConfig {
    /// Data shards (k)
    pub data_shards: u8,
    /// Parity shards (m) for MDS, or (l + g) for LRC
    pub parity_shards: u8,
    /// Backend type preference
    pub backend_type: BackendType,
    /// LRC configuration (if using LRC mode)
    pub lrc_config: Option<LrcConfig>,
}

impl BackendConfig {
    /// Create a new MDS backend configuration
    pub fn mds(data_shards: u8, parity_shards: u8) -> Self {
        Self {
            data_shards,
            parity_shards,
            backend_type: BackendType::Auto,
            lrc_config: None,
        }
    }

    /// Create a new LRC backend configuration
    pub fn lrc(config: LrcConfig) -> Self {
        Self {
            data_shards: config.data_shards,
            parity_shards: config.total_parity(),
            backend_type: BackendType::Auto,
            lrc_config: Some(config),
        }
    }

    /// Set the backend type
    pub fn with_backend(mut self, backend_type: BackendType) -> Self {
        self.backend_type = backend_type;
        self
    }
}

/// Factory for creating erasure coding backends
pub struct BackendFactory;

impl BackendFactory {
    /// Create a standard MDS erasure coding backend
    ///
    /// # Arguments
    /// * `config` - Backend configuration
    ///
    /// # Returns
    /// Arc-wrapped backend implementing `ErasureBackend`
    pub fn create_mds(config: &BackendConfig) -> Result<Arc<dyn ErasureBackend>, ErasureError> {
        let backend_type = match config.backend_type {
            BackendType::Auto => Self::detect_best_backend(),
            other => other,
        };

        match backend_type {
            BackendType::IsaL => {
                #[cfg(all(feature = "isal", any(target_arch = "x86", target_arch = "x86_64")))]
                {
                    Ok(Arc::new(super::isal::IsalBackend::new(
                        config.data_shards,
                        config.parity_shards,
                    )?))
                }
                #[cfg(not(all(
                    feature = "isal",
                    any(target_arch = "x86", target_arch = "x86_64")
                )))]
                {
                    Err(ErasureError::InvalidConfig(
                        "ISA-L backend not available on this platform".into(),
                    ))
                }
            }
            BackendType::RustSimd | BackendType::Auto => Ok(Arc::new(RustSimdBackend::new(
                config.data_shards,
                config.parity_shards,
            )?)),
        }
    }

    /// Create an LRC-capable backend
    ///
    /// # Arguments
    /// * `config` - Backend configuration (must have `lrc_config` set)
    ///
    /// # Returns
    /// Arc-wrapped backend implementing both `ErasureBackend` and `LrcBackend`
    pub fn create_lrc(config: &BackendConfig) -> Result<Arc<dyn LrcBackend>, ErasureError> {
        let lrc_config = config.lrc_config.ok_or_else(|| {
            ErasureError::InvalidConfig("LRC config required for LRC backend".into())
        })?;

        let backend_type = match config.backend_type {
            BackendType::Auto => Self::detect_best_backend(),
            other => other,
        };

        match backend_type {
            BackendType::IsaL => {
                #[cfg(all(feature = "isal", any(target_arch = "x86", target_arch = "x86_64")))]
                {
                    Ok(Arc::new(super::isal::IsalLrcBackend::new(lrc_config)?))
                }
                #[cfg(not(all(
                    feature = "isal",
                    any(target_arch = "x86", target_arch = "x86_64")
                )))]
                {
                    Err(ErasureError::InvalidConfig(
                        "ISA-L LRC backend not available on this platform".into(),
                    ))
                }
            }
            BackendType::RustSimd | BackendType::Auto => {
                Ok(Arc::new(RustSimdLrcBackend::new(lrc_config)?))
            }
        }
    }

    /// Detect the best available backend for this platform
    pub fn detect_best_backend() -> BackendType {
        #[cfg(all(feature = "isal", any(target_arch = "x86", target_arch = "x86_64")))]
        {
            // ISA-L is generally faster on x86 when available
            if Self::is_isal_available() {
                return BackendType::IsaL;
            }
        }
        BackendType::RustSimd
    }

    /// Check if ISA-L is available at runtime
    #[cfg(all(feature = "isal", any(target_arch = "x86", target_arch = "x86_64")))]
    fn is_isal_available() -> bool {
        // Could add runtime CPU feature detection here
        // For now, if compiled with feature, assume available
        true
    }

    /// Get capabilities of all available backends
    pub fn available_backends() -> Vec<BackendCapabilities> {
        #[allow(unused_mut)]
        let mut caps = vec![BackendCapabilities {
            name: "rust_simd",
            supports_lrc: true,
            supports_simd: true,
            max_data_shards: 255,
            max_parity_shards: 255,
        }];

        #[cfg(all(feature = "isal", any(target_arch = "x86", target_arch = "x86_64")))]
        {
            caps.push(BackendCapabilities {
                name: "isal",
                supports_lrc: true,
                supports_simd: true,
                max_data_shards: 255,
                max_parity_shards: 255,
            });
        }

        caps
    }

    /// Get the currently selected backend type
    pub fn current_backend() -> BackendType {
        Self::detect_best_backend()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mds_backend() {
        let config = BackendConfig::mds(4, 2);
        let backend = BackendFactory::create_mds(&config).unwrap();

        assert_eq!(backend.data_shards(), 4);
        assert_eq!(backend.parity_shards(), 2);
        assert_eq!(backend.total_shards(), 6);
    }

    #[test]
    fn test_create_lrc_backend() {
        let lrc_config = LrcConfig::new(6, 2, 2);
        let config = BackendConfig::lrc(lrc_config);
        let backend = BackendFactory::create_lrc(&config).unwrap();

        assert_eq!(backend.data_shards(), 6);
        assert_eq!(backend.parity_shards(), 4); // 2 local + 2 global
        assert_eq!(backend.total_shards(), 10);
    }

    #[test]
    fn test_backend_type_parse() {
        assert_eq!(
            "rust_simd".parse::<BackendType>().unwrap(),
            BackendType::RustSimd
        );
        assert_eq!("auto".parse::<BackendType>().unwrap(), BackendType::Auto);
        assert_eq!("isal".parse::<BackendType>().unwrap(), BackendType::IsaL);
    }

    #[test]
    fn test_available_backends() {
        let backends = BackendFactory::available_backends();
        assert!(!backends.is_empty());
        assert!(backends.iter().any(|b| b.name == "rust_simd"));
    }
}
