//! External policy evaluator implementations
//!
//! This module contains implementations of the `ExternalPolicyEvaluator` trait:
//! - `openfga`: OpenFGA ReBAC policy engine (optional feature)

#[cfg(feature = "openfga")]
pub mod openfga;

#[cfg(feature = "openfga")]
pub use openfga::{OpenFgaConfig, OpenFgaEvaluator, OpenFgaTupleManager, TupleKey};
