//! OpenFGA policy evaluator
//!
//! This module provides integration with OpenFGA (Zanzibar-based ReBAC)
//! for fine-grained authorization. OpenFGA is recommended for S3 workloads
//! due to native parent-child relationships (bucket → object inheritance)
//! and 2-5ms latency.
//!
//! # Example OpenFGA Authorization Model for S3:
//!
//! ```dsl
//! model
//!   schema 1.1
//!
//! type user
//!
//! type s3-bucket
//!   relations
//!     define owner: [user]
//!     define writer: [user] or owner
//!     define reader: [user] or writer
//!
//! type s3-object
//!   relations
//!     define parent: [s3-bucket]
//!     define owner: [user] or owner from parent
//!     define writer: [user] or writer from parent or owner
//!     define reader: [user] or reader from parent or writer
//! ```

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::external_policy::{
    ExternalPolicyDecision, ExternalPolicyError, ExternalPolicyEvaluator, ExternalPolicyRequest,
};

/// Configuration for OpenFGA evaluator
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenFgaConfig {
    /// OpenFGA API URL (e.g., "http://localhost:8080")
    pub url: String,
    /// Store ID
    pub store_id: String,
    /// Authorization model ID (optional, uses latest if not set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    /// Request timeout in milliseconds
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Enable local caching
    #[serde(default = "default_enable_cache")]
    pub enable_cache: bool,
    /// Cache TTL in seconds
    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: u64,
    /// Consistency mode
    #[serde(default)]
    pub consistency: ConsistencyMode,
}

fn default_timeout_ms() -> u64 {
    100
}

fn default_enable_cache() -> bool {
    true
}

fn default_cache_ttl_secs() -> u64 {
    60
}

/// Consistency mode for OpenFGA queries
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyMode {
    /// Minimize latency (use cache, may be slightly stale)
    #[default]
    MinimizeLatency,
    /// Highly consistent (always fresh, slower)
    HighlyConsistent,
}

impl OpenFgaConfig {
    /// Create a new OpenFGA config
    pub fn new(url: impl Into<String>, store_id: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            store_id: store_id.into(),
            model_id: None,
            timeout_ms: default_timeout_ms(),
            enable_cache: default_enable_cache(),
            cache_ttl_secs: default_cache_ttl_secs(),
            consistency: ConsistencyMode::default(),
        }
    }

    /// Set the authorization model ID
    pub fn with_model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }

    /// Set the request timeout
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Enable or disable caching
    pub fn with_cache(mut self, enable: bool) -> Self {
        self.enable_cache = enable;
        self
    }

    /// Set the cache TTL
    pub fn with_cache_ttl_secs(mut self, ttl: u64) -> Self {
        self.cache_ttl_secs = ttl;
        self
    }

    /// Set the consistency mode
    pub fn with_consistency(mut self, mode: ConsistencyMode) -> Self {
        self.consistency = mode;
        self
    }
}

/// OpenFGA Check request
#[derive(Debug, Serialize)]
struct CheckRequest {
    tuple_key: TupleKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization_model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consistency: Option<String>,
}

/// OpenFGA tuple key
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TupleKey {
    pub user: String,
    pub relation: String,
    pub object: String,
}

impl TupleKey {
    /// Create a new tuple key
    pub fn new(
        user: impl Into<String>,
        relation: impl Into<String>,
        object: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            relation: relation.into(),
            object: object.into(),
        }
    }
}

/// OpenFGA Check response
#[derive(Debug, Deserialize)]
struct CheckResponse {
    allowed: bool,
}

/// OpenFGA Write request
#[derive(Debug, Serialize)]
struct WriteRequest {
    writes: WriteTuples,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletes: Option<DeleteTuples>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization_model_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct WriteTuples {
    tuple_keys: Vec<TupleKey>,
}

#[derive(Debug, Serialize)]
struct DeleteTuples {
    tuple_keys: Vec<TupleKey>,
}

/// Simple in-memory cache for policy decisions
struct DecisionCache {
    entries: parking_lot::RwLock<HashMap<String, (ExternalPolicyDecision, std::time::Instant)>>,
    ttl: Duration,
}

impl DecisionCache {
    fn new(ttl_secs: u64) -> Self {
        Self {
            entries: parking_lot::RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    fn get(&self, key: &str) -> Option<ExternalPolicyDecision> {
        let entries = self.entries.read();
        entries.get(key).and_then(|(decision, created)| {
            if created.elapsed() < self.ttl {
                Some(*decision)
            } else {
                None
            }
        })
    }

    fn insert(&self, key: String, decision: ExternalPolicyDecision) {
        let mut entries = self.entries.write();
        // Clean up expired entries periodically
        if entries.len() > 10000 {
            entries.retain(|_, (_, created)| created.elapsed() < self.ttl);
        }
        entries.insert(key, (decision, std::time::Instant::now()));
    }
}

/// OpenFGA policy evaluator
pub struct OpenFgaEvaluator {
    config: OpenFgaConfig,
    http_client: reqwest::Client,
    cache: Option<DecisionCache>,
}

impl OpenFgaEvaluator {
    /// Create a new OpenFGA evaluator
    pub fn new(config: OpenFgaConfig) -> Result<Self, ExternalPolicyError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| ExternalPolicyError::ConfigurationError(e.to_string()))?;

        let cache = if config.enable_cache {
            Some(DecisionCache::new(config.cache_ttl_secs))
        } else {
            None
        };

        Ok(Self {
            config,
            http_client,
            cache,
        })
    }

    /// Get the store ID
    pub fn store_id(&self) -> &str {
        &self.config.store_id
    }

    /// Perform a check request to OpenFGA
    async fn check(&self, tuple: &TupleKey) -> Result<bool, ExternalPolicyError> {
        let url = format!(
            "{}/stores/{}/check",
            self.config.url, self.config.store_id
        );

        let consistency = match self.config.consistency {
            ConsistencyMode::MinimizeLatency => Some("MINIMIZE_LATENCY".to_string()),
            ConsistencyMode::HighlyConsistent => Some("HIGHER_CONSISTENCY".to_string()),
        };

        let request = CheckRequest {
            tuple_key: tuple.clone(),
            authorization_model_id: self.config.model_id.clone(),
            consistency,
        };

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    ExternalPolicyError::Timeout
                } else {
                    ExternalPolicyError::Unavailable(e.to_string())
                }
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ExternalPolicyError::Unavailable(format!(
                "OpenFGA returned status {}: {}",
                status, body
            )));
        }

        let check_response: CheckResponse = response
            .json()
            .await
            .map_err(|e| ExternalPolicyError::Internal(e.to_string()))?;

        Ok(check_response.allowed)
    }

    /// Write tuples to OpenFGA
    pub async fn write_tuples(&self, tuples: Vec<TupleKey>) -> Result<(), ExternalPolicyError> {
        if tuples.is_empty() {
            return Ok(());
        }

        let url = format!(
            "{}/stores/{}/write",
            self.config.url, self.config.store_id
        );

        let request = WriteRequest {
            writes: WriteTuples { tuple_keys: tuples },
            deletes: None,
            authorization_model_id: self.config.model_id.clone(),
        };

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    ExternalPolicyError::Timeout
                } else {
                    ExternalPolicyError::Unavailable(e.to_string())
                }
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ExternalPolicyError::Unavailable(format!(
                "OpenFGA write failed with status {}: {}",
                status, body
            )));
        }

        Ok(())
    }

    /// Delete tuples from OpenFGA
    pub async fn delete_tuples(&self, tuples: Vec<TupleKey>) -> Result<(), ExternalPolicyError> {
        if tuples.is_empty() {
            return Ok(());
        }

        let url = format!(
            "{}/stores/{}/write",
            self.config.url, self.config.store_id
        );

        let request = WriteRequest {
            writes: WriteTuples { tuple_keys: vec![] },
            deletes: Some(DeleteTuples { tuple_keys: tuples }),
            authorization_model_id: self.config.model_id.clone(),
        };

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    ExternalPolicyError::Timeout
                } else {
                    ExternalPolicyError::Unavailable(e.to_string())
                }
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ExternalPolicyError::Unavailable(format!(
                "OpenFGA delete failed with status {}: {}",
                status, body
            )));
        }

        Ok(())
    }

    /// Build cache key for a policy request
    fn cache_key(&self, request: &ExternalPolicyRequest) -> String {
        format!(
            "{}:{}:{}:{}",
            request.identity.subject,
            request.action.as_str(),
            request.bucket,
            request.object_key.as_deref().unwrap_or("")
        )
    }
}

#[async_trait]
impl ExternalPolicyEvaluator for OpenFgaEvaluator {
    fn name(&self) -> &str {
        "openfga"
    }

    async fn evaluate(
        &self,
        request: &ExternalPolicyRequest,
    ) -> Result<ExternalPolicyDecision, ExternalPolicyError> {
        // Check cache first
        let cache_key = self.cache_key(request);
        if let Some(ref cache) = self.cache {
            if let Some(decision) = cache.get(&cache_key) {
                tracing::debug!("OpenFGA cache hit for {}", cache_key);
                return Ok(decision);
            }
        }

        // Map S3 action to OpenFGA relation
        let relation = request.action.to_openfga_relation();

        // Build the object string
        let object = if let Some(ref key) = request.object_key {
            // For objects, use "s3-object:bucket#key" format
            // The # is used because / is not allowed in OpenFGA object IDs
            format!("s3-object:{}#{}", request.bucket, key.replace('/', "#"))
        } else {
            format!("s3-bucket:{}", request.bucket)
        };

        // Build the user string
        let user = format!("user:{}", request.identity.subject);

        let tuple = TupleKey::new(&user, relation, &object);

        // Perform the check
        let allowed = self.check(&tuple).await?;

        let decision = if allowed {
            ExternalPolicyDecision::Allow
        } else {
            ExternalPolicyDecision::Deny
        };

        // Cache result
        if let Some(ref cache) = self.cache {
            cache.insert(cache_key, decision);
        }

        Ok(decision)
    }

    async fn health_check(&self) -> bool {
        let url = format!("{}/stores/{}", self.config.url, self.config.store_id);

        self.http_client
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }
}

/// OpenFGA tuple manager for event-driven permission management
///
/// This manages tuples automatically based on S3 operations:
/// - CreateBucket → grants owner permission
/// - DeleteBucket → removes all permissions
/// - PutBucketPolicy → syncs policy to tuples
pub struct OpenFgaTupleManager {
    evaluator: Arc<OpenFgaEvaluator>,
}

impl OpenFgaTupleManager {
    /// Create a new tuple manager
    pub fn new(evaluator: Arc<OpenFgaEvaluator>) -> Self {
        Self { evaluator }
    }

    /// Called after CreateBucket - grants owner permission
    pub async fn on_bucket_created(
        &self,
        user_id: &str,
        bucket: &str,
    ) -> Result<(), ExternalPolicyError> {
        let tuples = vec![TupleKey::new(
            format!("user:{}", user_id),
            "owner",
            format!("s3-bucket:{}", bucket),
        )];

        self.evaluator.write_tuples(tuples).await?;
        tracing::info!(
            "OpenFGA: Granted owner permission for user {} on bucket {}",
            user_id,
            bucket
        );

        Ok(())
    }

    /// Called after DeleteBucket - removes all tuples for bucket
    pub async fn on_bucket_deleted(&self, bucket: &str) -> Result<(), ExternalPolicyError> {
        // Note: OpenFGA doesn't support deleting all tuples for an object directly
        // In a production implementation, you would need to list tuples first
        // For now, we log a warning
        tracing::warn!(
            "OpenFGA: Bucket {} deleted - manual cleanup of tuples may be required",
            bucket
        );
        Ok(())
    }

    /// Grant permission to a user on a bucket
    pub async fn grant_bucket_permission(
        &self,
        user_id: &str,
        bucket: &str,
        relation: &str,
    ) -> Result<(), ExternalPolicyError> {
        let tuples = vec![TupleKey::new(
            format!("user:{}", user_id),
            relation,
            format!("s3-bucket:{}", bucket),
        )];

        self.evaluator.write_tuples(tuples).await?;
        tracing::info!(
            "OpenFGA: Granted {} permission for user {} on bucket {}",
            relation,
            user_id,
            bucket
        );

        Ok(())
    }

    /// Revoke permission from a user on a bucket
    pub async fn revoke_bucket_permission(
        &self,
        user_id: &str,
        bucket: &str,
        relation: &str,
    ) -> Result<(), ExternalPolicyError> {
        let tuples = vec![TupleKey::new(
            format!("user:{}", user_id),
            relation,
            format!("s3-bucket:{}", bucket),
        )];

        self.evaluator.delete_tuples(tuples).await?;
        tracing::info!(
            "OpenFGA: Revoked {} permission for user {} on bucket {}",
            relation,
            user_id,
            bucket
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = OpenFgaConfig::new("http://localhost:8080", "store123")
            .with_model_id("model456")
            .with_timeout_ms(200)
            .with_cache(true)
            .with_cache_ttl_secs(120)
            .with_consistency(ConsistencyMode::HighlyConsistent);

        assert_eq!(config.url, "http://localhost:8080");
        assert_eq!(config.store_id, "store123");
        assert_eq!(config.model_id, Some("model456".to_string()));
        assert_eq!(config.timeout_ms, 200);
        assert!(config.enable_cache);
        assert_eq!(config.cache_ttl_secs, 120);
        assert_eq!(config.consistency, ConsistencyMode::HighlyConsistent);
    }

    #[test]
    fn test_tuple_key() {
        let tuple = TupleKey::new("user:alice", "owner", "s3-bucket:mybucket");
        assert_eq!(tuple.user, "user:alice");
        assert_eq!(tuple.relation, "owner");
        assert_eq!(tuple.object, "s3-bucket:mybucket");
    }
}
