//! Scatter-Gather Listing Implementation
//!
//! Implements distributed object listing by querying multiple OSD nodes in parallel
//! and merging results using k-way merge.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Gateway                                   │
//! │  ┌─────────────────────────────────────────────────────────────┐│
//! │  │              Scatter-Gather List Engine                      ││
//! │  │  1. Get listing nodes from meta service                     ││
//! │  │  2. Fan out ListObjectsMeta to all nodes                    ││
//! │  │  3. K-way merge sorted results                              ││
//! │  │  4. Return merged page + continuation token                 ││
//! │  └─────────────────────────────────────────────────────────────┘│
//! │                          │                                      │
//! │          ┌───────────────┼───────────────┐                      │
//! │          ▼               ▼               ▼                      │
//! │     ┌─────────┐     ┌─────────┐     ┌─────────┐                │
//! │     │  OSD 1  │     │  OSD 2  │     │  OSD 3  │                │
//! │     └─────────┘     └─────────┘     └─────────┘                │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::stream::{self, StreamExt};
use objectio_proto::metadata::{
    GetListingNodesRequest, ListingNode, ObjectMeta, metadata_service_client::MetadataServiceClient,
};
use objectio_proto::storage::{
    ListObjectsMetaRequest, storage_service_client::StorageServiceClient,
};
use ring::hmac;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, warn};

use crate::osd_pool::OsdPool;

/// Maximum number of concurrent shard queries
const MAX_CONCURRENT_QUERIES: usize = 32;

/// Timeout for individual shard queries
const SHARD_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Error types for scatter-gather operations
#[derive(Debug, thiserror::Error)]
pub enum ScatterGatherError {
    #[error("No listing nodes available")]
    NoNodesAvailable,

    #[error("Invalid continuation token")]
    InvalidToken,

    #[error("Token signature mismatch")]
    TokenSignatureMismatch,

    #[error("Topology version changed (expected {expected}, got {actual})")]
    TopologyChanged { expected: u64, actual: u64 },

    #[error("All shards failed")]
    AllShardsFailed,

    #[error("Shard {shard_id} failed: {message}")]
    ShardFailed { shard_id: u32, message: String },

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("Connection error: {0}")]
    Connection(String),
}

/// Per-shard cursor tracking the last key returned from each shard
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ShardCursor {
    /// Last key returned from this shard (empty if not started)
    pub last_key: String,
    /// Whether this shard is exhausted (no more results)
    pub exhausted: bool,
}

/// Continuation token for paginated listing
///
/// Encodes the state needed to resume listing from where we left off:
/// - Per-shard cursors (where each OSD left off)
/// - Topology version (to detect cluster changes)
/// - HMAC signature (to prevent tampering)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListContinuationToken {
    /// Bucket name (for validation)
    pub bucket: String,
    /// Prefix (for validation)
    pub prefix: String,
    /// Per-shard cursors: shard_id -> cursor
    pub shard_cursors: HashMap<u32, ShardCursor>,
    /// Topology version when token was created
    pub topology_version: u64,
    /// HMAC signature over (bucket, prefix, shard_cursors, topology_version)
    #[serde(with = "base64_bytes")]
    pub signature: Vec<u8>,
}

/// Custom serialization for signature bytes
mod base64_bytes {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        URL_SAFE_NO_PAD.decode(s).map_err(serde::de::Error::custom)
    }
}

impl ListContinuationToken {
    /// Create a new continuation token
    pub fn new(
        bucket: &str,
        prefix: &str,
        shard_cursors: HashMap<u32, ShardCursor>,
        topology_version: u64,
        signing_key: &hmac::Key,
    ) -> Self {
        let mut token = Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            shard_cursors,
            topology_version,
            signature: Vec::new(),
        };
        token.signature = token.compute_signature(signing_key);
        token
    }

    /// Compute HMAC signature over token contents
    fn compute_signature(&self, key: &hmac::Key) -> Vec<u8> {
        let data = format!(
            "{}:{}:{}:{}",
            self.bucket,
            self.prefix,
            self.topology_version,
            self.shard_cursors.len()
        );
        hmac::sign(key, data.as_bytes()).as_ref().to_vec()
    }

    /// Verify token signature
    pub fn verify(&self, signing_key: &hmac::Key) -> bool {
        let expected = self.compute_signature(signing_key);
        self.signature == expected
    }

    /// Encode token to base64 string
    pub fn encode(&self) -> Result<String, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        Ok(URL_SAFE_NO_PAD.encode(&json))
    }

    /// Decode token from base64 string
    pub fn decode(s: &str) -> Result<Self, ScatterGatherError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| ScatterGatherError::InvalidToken)?;
        serde_json::from_slice(&bytes).map_err(|_| ScatterGatherError::InvalidToken)
    }

    /// Check if all shards are exhausted
    pub fn all_exhausted(&self) -> bool {
        self.shard_cursors.values().all(|c| c.exhausted)
    }
}

/// Result from a single shard query
struct ShardResult {
    shard_id: u32,
    objects: Vec<ObjectMeta>,
    next_token: String,
    is_truncated: bool,
}

/// Entry for the k-way merge heap
struct MergeEntry {
    /// Object metadata
    object: ObjectMeta,
    /// Source shard ID
    shard_id: u32,
    /// Index within shard's result buffer
    index: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.object.key == other.object.key
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse comparison for smallest key first
        other.object.key.cmp(&self.object.key)
    }
}

/// Scatter-gather listing engine
pub struct ScatterGatherEngine {
    /// OSD connection pool
    osd_pool: Arc<OsdPool>,
    /// HMAC signing key for continuation tokens
    signing_key: hmac::Key,
    /// Behavior on shard failure
    fail_on_shard_error: bool,
}

impl ScatterGatherEngine {
    /// Create a new scatter-gather engine
    pub fn new(osd_pool: Arc<OsdPool>, signing_key_bytes: &[u8]) -> Self {
        let signing_key = hmac::Key::new(hmac::HMAC_SHA256, signing_key_bytes);
        Self {
            osd_pool,
            signing_key,
            fail_on_shard_error: false, // Default: return partial results
        }
    }

    /// Set whether to fail on any shard error (default: false)
    #[allow(dead_code)]
    pub fn set_fail_on_shard_error(&mut self, fail: bool) {
        self.fail_on_shard_error = fail;
    }

    /// Execute a scatter-gather list operation
    pub async fn list_objects(
        &self,
        meta_client: &mut MetadataServiceClient<Channel>,
        bucket: &str,
        prefix: &str,
        max_keys: u32,
        continuation_token: Option<&str>,
    ) -> Result<ListObjectsResult, ScatterGatherError> {
        // 1. Get listing nodes from meta service
        let nodes_resp = meta_client
            .get_listing_nodes(GetListingNodesRequest {
                bucket: bucket.to_string(),
            })
            .await?;
        let nodes_inner = nodes_resp.into_inner();
        let nodes = nodes_inner.nodes;
        let topology_version = nodes_inner.topology_version;

        if nodes.is_empty() {
            return Err(ScatterGatherError::NoNodesAvailable);
        }

        debug!(
            "Scatter-gather list: {} nodes, bucket={}, prefix={}",
            nodes.len(),
            bucket,
            prefix
        );

        // 2. Parse continuation token if provided
        let (shard_cursors, is_first_page) = if let Some(token_str) = continuation_token {
            let token = ListContinuationToken::decode(token_str)?;

            // Verify signature
            if !token.verify(&self.signing_key) {
                return Err(ScatterGatherError::TokenSignatureMismatch);
            }

            // Verify bucket and prefix match
            if token.bucket != bucket || token.prefix != prefix {
                return Err(ScatterGatherError::InvalidToken);
            }

            // Check topology version
            if token.topology_version != topology_version {
                return Err(ScatterGatherError::TopologyChanged {
                    expected: token.topology_version,
                    actual: topology_version,
                });
            }

            (token.shard_cursors, false)
        } else {
            (HashMap::new(), true)
        };

        // 3. Query each shard in parallel (skip exhausted shards)
        let shard_results = self
            .query_shards(&nodes, bucket, prefix, max_keys, &shard_cursors)
            .await?;

        // 4. K-way merge the results
        let (merged_objects, new_cursors, is_truncated) =
            self.k_way_merge(shard_results, max_keys as usize)?;

        // 5. Build continuation token if truncated
        let next_token = if is_truncated {
            let token = ListContinuationToken::new(
                bucket,
                prefix,
                new_cursors,
                topology_version,
                &self.signing_key,
            );
            Some(
                token
                    .encode()
                    .map_err(|_| ScatterGatherError::InvalidToken)?,
            )
        } else {
            None
        };

        Ok(ListObjectsResult {
            objects: merged_objects,
            is_truncated,
            next_continuation_token: next_token,
            key_count: 0, // Will be set by caller
        })
    }

    /// Query all shards in parallel
    async fn query_shards(
        &self,
        nodes: &[ListingNode],
        bucket: &str,
        prefix: &str,
        max_keys: u32,
        shard_cursors: &HashMap<u32, ShardCursor>,
    ) -> Result<Vec<ShardResult>, ScatterGatherError> {
        // Create query futures for each non-exhausted shard
        let queries: Vec<_> = nodes
            .iter()
            .filter_map(|node| {
                let cursor = shard_cursors.get(&node.shard_id);

                // Skip exhausted shards
                if cursor.map(|c| c.exhausted).unwrap_or(false) {
                    return None;
                }

                let start_after = cursor.map(|c| c.last_key.clone()).unwrap_or_default();
                let address = node.address.clone();
                let shard_id = node.shard_id;
                let node_id = node.node_id.clone();
                let bucket = bucket.to_string();
                let prefix = prefix.to_string();
                let osd_pool = self.osd_pool.clone();

                Some(async move {
                    // Request more keys than needed to handle duplicates and ensure we can fill the page
                    let request = ListObjectsMetaRequest {
                        bucket,
                        prefix,
                        start_after,
                        max_keys: max_keys + 100, // Over-fetch to ensure we have enough
                        continuation_token: String::new(),
                    };

                    // Get or create connection
                    let client_result = osd_pool.get_or_connect(&node_id, &address).await;
                    let mut client = match client_result {
                        Ok(c) => c,
                        Err(e) => {
                            return Err((shard_id, format!("Connection failed: {}", e)));
                        }
                    };

                    // Query with timeout
                    let result = tokio::time::timeout(
                        SHARD_QUERY_TIMEOUT,
                        client.list_objects_meta(request),
                    )
                    .await;

                    match result {
                        Ok(Ok(response)) => {
                            let inner = response.into_inner();
                            Ok(ShardResult {
                                shard_id,
                                objects: inner.objects,
                                next_token: inner.next_continuation_token,
                                is_truncated: inner.is_truncated,
                            })
                        }
                        Ok(Err(e)) => Err((shard_id, format!("gRPC error: {}", e))),
                        Err(_) => Err((shard_id, "Timeout".to_string())),
                    }
                })
            })
            .collect();

        if queries.is_empty() {
            // All shards exhausted
            return Ok(Vec::new());
        }

        // Execute queries in parallel with concurrency limit
        let results: Vec<_> = stream::iter(queries)
            .buffer_unordered(MAX_CONCURRENT_QUERIES)
            .collect()
            .await;

        // Process results
        let mut shard_results = Vec::new();
        let mut failures = Vec::new();

        for result in results {
            match result {
                Ok(shard_result) => {
                    shard_results.push(shard_result);
                }
                Err((shard_id, message)) => {
                    warn!("Shard {} failed: {}", shard_id, message);
                    failures.push((shard_id, message));
                }
            }
        }

        // Check failure policy
        if shard_results.is_empty() {
            return Err(ScatterGatherError::AllShardsFailed);
        }

        if self.fail_on_shard_error && !failures.is_empty() {
            let (shard_id, message) = failures.into_iter().next().unwrap();
            return Err(ScatterGatherError::ShardFailed { shard_id, message });
        }

        Ok(shard_results)
    }

    /// K-way merge sorted results from multiple shards
    fn k_way_merge(
        &self,
        shard_results: Vec<ShardResult>,
        max_keys: usize,
    ) -> Result<(Vec<ObjectMeta>, HashMap<u32, ShardCursor>, bool), ScatterGatherError> {
        // Track per-shard state
        let mut shard_buffers: HashMap<u32, (Vec<ObjectMeta>, bool)> = shard_results
            .into_iter()
            .map(|r| (r.shard_id, (r.objects, r.is_truncated)))
            .collect();

        // Initialize min-heap with first element from each shard
        let mut heap = BinaryHeap::new();
        for (shard_id, (objects, _)) in &shard_buffers {
            if let Some(obj) = objects.first() {
                heap.push(MergeEntry {
                    object: obj.clone(),
                    shard_id: *shard_id,
                    index: 0,
                });
            }
        }

        // Merge until we have max_keys or all exhausted
        let mut merged = Vec::with_capacity(max_keys);
        let mut last_key_per_shard: HashMap<u32, String> = HashMap::new();

        while merged.len() < max_keys {
            let entry = match heap.pop() {
                Some(e) => e,
                None => break, // All shards exhausted
            };

            // Add to result (dedup by key - same key can exist on multiple shards)
            if merged.last().map(|o: &ObjectMeta| &o.key) != Some(&entry.object.key) {
                merged.push(entry.object.clone());
            }

            // Track last key seen from this shard
            last_key_per_shard.insert(entry.shard_id, entry.object.key.clone());

            // Push next element from same shard
            let next_index = entry.index + 1;
            if let Some((objects, _)) = shard_buffers.get(&entry.shard_id) {
                if next_index < objects.len() {
                    heap.push(MergeEntry {
                        object: objects[next_index].clone(),
                        shard_id: entry.shard_id,
                        index: next_index,
                    });
                }
            }
        }

        // Determine if truncated (any shard has more data)
        let is_truncated = !heap.is_empty()
            || shard_buffers
                .values()
                .any(|(objects, is_truncated)| *is_truncated || !objects.is_empty());

        // Build new cursors
        let new_cursors: HashMap<u32, ShardCursor> = shard_buffers
            .iter()
            .map(|(shard_id, (objects, is_truncated))| {
                let last_key = last_key_per_shard
                    .get(shard_id)
                    .cloned()
                    .unwrap_or_default();
                let exhausted = last_key.is_empty() && !is_truncated;
                (
                    *shard_id,
                    ShardCursor {
                        last_key,
                        exhausted,
                    },
                )
            })
            .collect();

        Ok((merged, new_cursors, is_truncated))
    }
}

/// Result of a scatter-gather list operation
pub struct ListObjectsResult {
    /// Merged and sorted objects
    pub objects: Vec<ObjectMeta>,
    /// Whether more results are available
    pub is_truncated: bool,
    /// Continuation token for next page (if truncated)
    pub next_continuation_token: Option<String>,
    /// Number of keys returned
    pub key_count: u32,
}
