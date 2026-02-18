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
