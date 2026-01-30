//! Provider and evaluator chains
//!
//! This module provides chaining mechanisms for identity providers
//! and policy evaluators, allowing fallback and multi-provider support.

use async_trait::async_trait;
use std::sync::Arc;

use crate::external_policy::{
    ExternalPolicyDecision, ExternalPolicyError, ExternalPolicyEvaluator, ExternalPolicyRequest,
};
use crate::provider::{AuthProviderError, AuthRequest, AuthenticatedIdentity, IdentityProvider};

/// Chain of identity providers (first match wins)
pub struct IdentityProviderChain {
    providers: Vec<Arc<dyn IdentityProvider>>,
}

impl IdentityProviderChain {
    /// Create a new empty provider chain
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    /// Add a provider to the chain
    pub fn add<P: IdentityProvider + 'static>(&mut self, provider: P) -> &mut Self {
        self.providers.push(Arc::new(provider));
        self
    }

    /// Add a provider wrapped in Arc
    pub fn add_arc(&mut self, provider: Arc<dyn IdentityProvider>) -> &mut Self {
        self.providers.push(provider);
        self
    }

    /// Check if chain is empty
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// Get the number of providers
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// Authenticate using the first provider that can handle the request
    pub async fn authenticate(
        &self,
        request: &AuthRequest<'_>,
    ) -> Result<AuthenticatedIdentity, AuthProviderError> {
        for provider in &self.providers {
            if provider.can_handle(request) {
                tracing::debug!("Using identity provider: {}", provider.name());
                return provider.authenticate(request).await;
            }
        }

        // No provider could handle the request
        if self.providers.is_empty() {
            return Err(AuthProviderError::ConfigurationError(
                "No identity providers configured".to_string(),
            ));
        }

        Err(AuthProviderError::UnsupportedAuthMethod)
    }
}

impl Default for IdentityProviderChain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IdentityProvider for IdentityProviderChain {
    fn name(&self) -> &str {
        "chain"
    }

    fn can_handle(&self, request: &AuthRequest<'_>) -> bool {
        self.providers.iter().any(|p| p.can_handle(request))
    }

    async fn authenticate(
        &self,
        request: &AuthRequest<'_>,
    ) -> Result<AuthenticatedIdentity, AuthProviderError> {
        IdentityProviderChain::authenticate(self, request).await
    }
}

/// Chain of policy evaluators with fallback
pub struct PolicyEvaluatorChain {
    /// Primary evaluator
    primary: Arc<dyn ExternalPolicyEvaluator>,
    /// Fallback evaluator (used if primary fails)
    fallback: Option<Arc<dyn ExternalPolicyEvaluator>>,
    /// Deny on error (fail-closed)
    deny_on_error: bool,
}

impl PolicyEvaluatorChain {
    /// Create a new policy evaluator chain with a primary evaluator
    pub fn new<E: ExternalPolicyEvaluator + 'static>(primary: E) -> Self {
        Self {
            primary: Arc::new(primary),
            fallback: None,
            deny_on_error: true,
        }
    }

    /// Create a new policy evaluator chain with an Arc-wrapped primary evaluator
    pub fn with_arc(primary: Arc<dyn ExternalPolicyEvaluator>) -> Self {
        Self {
            primary,
            fallback: None,
            deny_on_error: true,
        }
    }

    /// Set the fallback evaluator
    pub fn with_fallback<E: ExternalPolicyEvaluator + 'static>(mut self, fallback: E) -> Self {
        self.fallback = Some(Arc::new(fallback));
        self
    }

    /// Set the fallback evaluator (Arc-wrapped)
    pub fn with_fallback_arc(mut self, fallback: Arc<dyn ExternalPolicyEvaluator>) -> Self {
        self.fallback = Some(fallback);
        self
    }

    /// Configure fail-closed behavior (deny on error)
    pub fn deny_on_error(mut self, deny: bool) -> Self {
        self.deny_on_error = deny;
        self
    }

    /// Evaluate policy, falling back if primary fails
    pub async fn evaluate(
        &self,
        request: &ExternalPolicyRequest,
    ) -> ExternalPolicyDecision {
        match self.primary.evaluate(request).await {
            Ok(decision) => decision,
            Err(e) => {
                tracing::warn!(
                    "Primary policy evaluator {} failed: {}",
                    self.primary.name(),
                    e
                );

                // Try fallback
                if let Some(ref fallback) = self.fallback {
                    match fallback.evaluate(request).await {
                        Ok(decision) => return decision,
                        Err(e) => {
                            tracing::error!(
                                "Fallback policy evaluator {} failed: {}",
                                fallback.name(),
                                e
                            );
                        }
                    }
                }

                // Default behavior on error
                if self.deny_on_error {
                    ExternalPolicyDecision::Deny
                } else {
                    // Allow on error (fail-open) - not recommended for production
                    ExternalPolicyDecision::Allow
                }
            }
        }
    }

    /// Check health of all evaluators
    pub async fn health_check(&self) -> bool {
        let primary_healthy = self.primary.health_check().await;

        if let Some(ref fallback) = self.fallback {
            let fallback_healthy = fallback.health_check().await;
            primary_healthy || fallback_healthy
        } else {
            primary_healthy
        }
    }
}

#[async_trait]
impl ExternalPolicyEvaluator for PolicyEvaluatorChain {
    fn name(&self) -> &str {
        "chain"
    }

    async fn evaluate(
        &self,
        request: &ExternalPolicyRequest,
    ) -> Result<ExternalPolicyDecision, ExternalPolicyError> {
        Ok(PolicyEvaluatorChain::evaluate(self, request).await)
    }

    async fn health_check(&self) -> bool {
        PolicyEvaluatorChain::health_check(self).await
    }
}

/// A simple allow-all policy evaluator for testing or when external policy is disabled
pub struct AllowAllEvaluator;

#[async_trait]
impl ExternalPolicyEvaluator for AllowAllEvaluator {
    fn name(&self) -> &str {
        "allow-all"
    }

    async fn evaluate(
        &self,
        _request: &ExternalPolicyRequest,
    ) -> Result<ExternalPolicyDecision, ExternalPolicyError> {
        Ok(ExternalPolicyDecision::Allow)
    }
}

/// A simple deny-all policy evaluator for testing
pub struct DenyAllEvaluator;

#[async_trait]
impl ExternalPolicyEvaluator for DenyAllEvaluator {
    fn name(&self) -> &str {
        "deny-all"
    }

    async fn evaluate(
        &self,
        _request: &ExternalPolicyRequest,
    ) -> Result<ExternalPolicyDecision, ExternalPolicyError> {
        Ok(ExternalPolicyDecision::Deny)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::AuthenticatedIdentity;
    use crate::external_policy::S3Action;

    #[tokio::test]
    async fn test_allow_all_evaluator() {
        let evaluator = AllowAllEvaluator;
        let identity = AuthenticatedIdentity::new("user1", "test", "arn:test");
        let request = ExternalPolicyRequest::new(identity, S3Action::GetObject, "bucket");

        let decision = evaluator.evaluate(&request).await.unwrap();
        assert_eq!(decision, ExternalPolicyDecision::Allow);
    }

    #[tokio::test]
    async fn test_deny_all_evaluator() {
        let evaluator = DenyAllEvaluator;
        let identity = AuthenticatedIdentity::new("user1", "test", "arn:test");
        let request = ExternalPolicyRequest::new(identity, S3Action::GetObject, "bucket");

        let decision = evaluator.evaluate(&request).await.unwrap();
        assert_eq!(decision, ExternalPolicyDecision::Deny);
    }

    #[tokio::test]
    async fn test_policy_chain_primary() {
        let chain = PolicyEvaluatorChain::new(AllowAllEvaluator);
        let identity = AuthenticatedIdentity::new("user1", "test", "arn:test");
        let request = ExternalPolicyRequest::new(identity, S3Action::GetObject, "bucket");

        let decision = chain.evaluate(&request).await;
        assert_eq!(decision, ExternalPolicyDecision::Allow);
    }
}
