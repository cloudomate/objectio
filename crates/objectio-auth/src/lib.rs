//! ObjectIO Authentication and Authorization
//!
//! This crate provides:
//! - User and access key management
//! - AWS Signature V4 verification
//! - Bucket policy evaluation
//! - Pluggable identity providers (builtin SigV4, OIDC)
//! - External policy evaluators (OpenFGA)
//!
//! # Features
//!
//! - `builtin` (default): Builtin SigV4 authentication
//! - `oidc`: OIDC/JWT authentication support
//! - `openfga`: OpenFGA policy engine support
//! - `full`: All features enabled
//!
//! # Example
//!
//! ```rust,ignore
//! use objectio_auth::{UserStore, IdentityProviderChain, BuiltinSigV4Provider};
//! use std::sync::Arc;
//!
//! // Create user store and provider chain
//! let user_store = Arc::new(UserStore::new());
//! let mut chain = IdentityProviderChain::new();
//! chain.add(BuiltinSigV4Provider::new(user_store, "us-east-1"));
//!
//! // Authenticate requests using the chain
//! // let identity = chain.authenticate(&auth_request).await?;
//! ```

// Core modules (always available)
pub mod error;
pub mod policy;
pub mod sigv2;
pub mod sigv4;
pub mod store;
pub mod user;

// Pluggable auth modules
pub mod chain;
pub mod external_policy;
pub mod provider;
pub mod providers;

// External policy evaluators (feature-gated)
pub mod evaluators;

// Re-export core types
pub use error::AuthError;
pub use policy::{BucketPolicy, Effect, PolicyDecision, PolicyEvaluator, PolicyStatement, Principal};
pub use sigv2::SigV2Verifier;
pub use sigv4::SigV4Verifier;
pub use store::UserStore;
pub use user::{AccessKey, AuthResult, KeyStatus, User, UserStatus};

// Re-export pluggable auth types
pub use chain::{AllowAllEvaluator, DenyAllEvaluator, IdentityProviderChain, PolicyEvaluatorChain};
pub use external_policy::{
    ExternalPolicyDecision, ExternalPolicyError, ExternalPolicyEvaluator, ExternalPolicyRequest,
    S3Action,
};
pub use provider::{AuthProviderError, AuthRequest, AuthenticatedIdentity, IdentityProvider};
pub use providers::BuiltinSigV4Provider;

// Re-export OpenFGA types (feature-gated)
#[cfg(feature = "openfga")]
pub use evaluators::{OpenFgaConfig, OpenFgaEvaluator, OpenFgaTupleManager, TupleKey};
