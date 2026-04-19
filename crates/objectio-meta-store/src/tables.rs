//! Redb table definitions for persistent metadata storage.

use redb::TableDefinition;

// S3 metadata
pub const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
pub const BUCKET_POLICIES: TableDefinition<&str, &str> = TableDefinition::new("bucket_policies");
pub const MULTIPART_UPLOADS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("multipart_uploads");

// Cluster
pub const OSD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("osd_nodes");
pub const CLUSTER_TOPOLOGY: TableDefinition<&str, &[u8]> = TableDefinition::new("cluster_topology");

// IAM
pub const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
pub const ACCESS_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("access_keys");
pub const GROUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("groups");
pub const GROUP_MEMBERS: TableDefinition<&str, &[u8]> = TableDefinition::new("group_members");

// Block storage
pub const VOLUMES: TableDefinition<&str, &[u8]> = TableDefinition::new("volumes");
pub const SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");
pub const VOLUME_CHUNKS: TableDefinition<&str, &[u8]> = TableDefinition::new("volume_chunks");

// Iceberg catalog
// Key: namespace path (e.g. "db1" or "db1\x00schema1"), Value: prost-encoded properties
pub const ICEBERG_NAMESPACES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("iceberg_namespaces");
// Key: "ns1\x00ns2\x00table_name", Value: prost-encoded IcebergTableEntry
pub const ICEBERG_TABLES: TableDefinition<&str, &[u8]> = TableDefinition::new("iceberg_tables");
// Key: filter_id, Value: bincode-encoded StoredDataFilter
pub const DATA_FILTERS: TableDefinition<&str, &[u8]> = TableDefinition::new("data_filters");

// Delta Sharing
// Key: share name, Value: prost-encoded DeltaShareEntry
pub const DELTA_SHARES: TableDefinition<&str, &[u8]> = TableDefinition::new("delta_shares");
// Key: "{share}\x00{schema}\x00{table_name}", Value: prost-encoded DeltaShareTableEntry
pub const DELTA_TABLES: TableDefinition<&str, &[u8]> = TableDefinition::new("delta_tables");
// Key: recipient name, Value: prost-encoded DeltaRecipientEntry
pub const DELTA_RECIPIENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("delta_recipients");

// Cluster configuration
// Key: hierarchical config path (e.g. "identity/openid/keycloak"), Value: prost-encoded ConfigEntry
pub const CONFIG: TableDefinition<&str, &[u8]> = TableDefinition::new("config");

// ---- Raft consensus tables ----
// One row per log index → JSON-encoded openraft::Entry<MetaTypeConfig>.
// Indexes are contiguous; the low watermark moves forward via purge.
pub const RAFT_LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");
// Single-row table; the row holds the serialized current vote (JSON).
pub const RAFT_VOTE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_vote");
// Single-row table: last applied log id + stored membership + last purged
// log id, JSON-encoded. Written in the same transaction as apply() so
// recovery after crash never re-applies committed commands.
pub const RAFT_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_state");

// Server pools
// Key: pool name, Value: prost-encoded PoolConfig
pub const POOLS: TableDefinition<&str, &[u8]> = TableDefinition::new("pools");

// Tenants
// Key: tenant name, Value: prost-encoded TenantConfig
pub const TENANTS: TableDefinition<&str, &[u8]> = TableDefinition::new("tenants");

// Named IAM policies
// Key: policy name, Value: prost-encoded PolicyObject
pub const IAM_POLICIES: TableDefinition<&str, &[u8]> = TableDefinition::new("iam_policies");
// Key: "user:{user_id}" or "group:{group_id}", Value: comma-separated policy names
pub const POLICY_ATTACHMENTS: TableDefinition<&str, &str> =
    TableDefinition::new("policy_attachments");

// Iceberg warehouses
// Key: warehouse name, Value: prost-encoded IcebergWarehouse
pub const ICEBERG_WAREHOUSES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("iceberg_warehouses");

// Object lock configurations
// Key: bucket name, Value: prost-encoded ObjectLockConfiguration
pub const OBJECT_LOCK_CONFIGS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("object_lock_configs");

// Lifecycle configurations
// Key: bucket name, Value: prost-encoded LifecycleConfiguration
pub const LIFECYCLE_CONFIGS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("lifecycle_configs");

// Bucket default SSE configurations
// Key: bucket name, Value: prost-encoded BucketSseConfiguration
pub const BUCKET_ENCRYPTION_CONFIGS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("bucket_encryption_configs");

// KMS keys (service-master-key-wrapped key material)
// Key: key_id, Value: prost-encoded KmsKey
pub const KMS_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("kms_keys");

// Object listing index. Strongly-consistent "does (bucket,key[,version])
// exist in this cluster" source of truth, maintained by the gateway's
// S3 PUT/DELETE path via Raft MultiCas against CasTable::ObjectListings.
// Key format: "{bucket}\0{key}\0{version_id}" — null-byte separators so
// redb's range scan over a bucket prefix ("foo\0") reliably stops at the
// next bucket. Value: prost-encoded ObjectListingEntry.
pub const OBJECT_LISTINGS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("object_listings");
