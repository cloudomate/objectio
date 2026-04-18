//! Intel ISA-L backend for high-performance erasure coding on x86/x86_64
//!
//! This module uses the `erasure-isa-l` crate which provides safe Rust bindings
//! to Intel's Intelligent Storage Acceleration Library (ISA-L) for hardware-accelerated
//! erasure coding using AVX/AVX2/AVX-512.
//!
//! The `erasure-isa-l` crate bundles the ISA-L source code and compiles it during build,
//! so no system library installation is required. However, the following build tools
//! are needed: nasm, autoconf, automake, libtool, libclang-dev.
//!
//! # Features
//!
//! This module is only available when the `isal` feature is enabled.
//! ISA-L works on both Intel and AMD x86/x86_64 processors.
//!
//! # Performance
//!
//! ISA-L typically provides 2-5x speedup over pure Rust implementations on
//! supported hardware due to hand-optimized assembly for Galois field operations.

#[cfg(feature = "isal")]
mod backend;

#[cfg(feature = "isal")]
pub use backend::{IsalBackend, IsalLrcBackend};

/// Check if ISA-L is available at runtime
///
/// Returns `true` if the ISA-L feature is enabled and compiled in.
pub fn is_available() -> bool {
    cfg!(feature = "isal")
}

/// Get the ISA-L library version string
///
/// Returns `None` if ISA-L is not available.
pub fn version() -> Option<&'static str> {
    #[cfg(feature = "isal")]
    {
        // erasure-isa-l bundles ISA-L 2.30.0+
        Some("bundled")
    }
    #[cfg(not(feature = "isal"))]
    {
        None
    }
}
