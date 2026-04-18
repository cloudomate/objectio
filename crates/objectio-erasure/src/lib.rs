//! ObjectIO Erasure Coding - Pluggable erasure coding backends
//!
//! This crate provides erasure coding functionality with support for:
//! - MDS (Maximum Distance Separable) Reed-Solomon codes
//! - LRC (Locally Repairable Codes) for reduced repair bandwidth
//! - Multiple backends (ISA-L for x86, rust-simd for portable)
//!
//! # Backends
//!
//! - **rust_simd** (default): Pure Rust using `reed-solomon-simd`, portable
//! - **isal** (feature-gated): Intel ISA-L via FFI, x86/x86_64 only
//!
//! # Code Types
//!
//! - **MDS**: Standard Reed-Solomon, any k shards reconstruct data
//! - **LRC**: Local parity groups enable faster single-shard repairs
//!
//! # Example
//!
//! ```
//! use objectio_erasure::{ErasureCodec, backend::{BackendConfig, BackendFactory}};
//! use objectio_common::ErasureConfig;
//!
//! // High-level API (uses rust_simd by default)
//! let codec = ErasureCodec::new(ErasureConfig::new(4, 2)).unwrap();
//! let data = b"Hello, World!";
//! let shards = codec.encode(data).unwrap();
//!
//! // Low-level backend API for more control
//! let config = BackendConfig::mds(4, 2);
//! let backend = BackendFactory::create_mds(&config).unwrap();
//! ```

pub mod backend;
pub mod codec;
pub mod shard;

// Re-exports from codec
pub use codec::{ErasureCodec, ErasureError};
pub use shard::Shard;

// Re-exports from backend for convenience
pub use backend::{
    BackendCapabilities, BackendConfig, BackendFactory, BackendType, ErasureBackend, LocalGroup,
    LrcBackend, LrcConfig, LrcEncodedData, RustSimdBackend, RustSimdLrcBackend,
};

/// Prelude for common imports
pub mod prelude {
    pub use super::{
        BackendConfig, BackendFactory, BackendType, ErasureBackend, ErasureCodec, ErasureError,
        LrcBackend, LrcConfig,
    };
}
