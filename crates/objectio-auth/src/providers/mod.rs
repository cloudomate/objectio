//! Identity provider implementations
//!
//! This module contains implementations of the `IdentityProvider` trait:
//! - `builtin`: SigV4 authentication using local UserStore
//! - `oidc`: OIDC/JWT authentication (optional feature)

pub mod builtin;

#[cfg(feature = "oidc")]
pub mod oidc;

pub use builtin::BuiltinSigV4Provider;

#[cfg(feature = "oidc")]
pub use oidc::OidcProvider;
