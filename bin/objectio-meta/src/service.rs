//! Metadata gRPC service implementation

use objectio_common::{NodeId, NodeStatus};
use objectio_meta_store::{
    EcConfig, MetaStore, MultipartUploadState, OsdNode, PartState, StoredAccessKey,
    StoredDataFilter, StoredGroup, StoredUser,
};
use objectio_placement::{
    Crush2, PlacementTemplate, ShardRole,
    topology::{ClusterTopology, DiskInfo, FailureDomainInfo, NodeInfo},
};
use sha2::{Digest, Sha256};

use objectio_proto::metadata::{
    AbortMultipartUploadRequest,
    AbortMultipartUploadResponse,
    // IAM types
    AccessKeyMeta,
    AddUserToGroupRequest,
    AddUserToGroupResponse,
    // Named IAM policy types
    AttachPolicyRequest,
    AttachPolicyResponse,
    BucketMeta,
    CompleteMultipartUploadRequest,
    CompleteMultipartUploadResponse,
    // Config types
    ConfigEntry,
    CreateAccessKeyRequest,
    CreateAccessKeyResponse,
    CreateBucketRequest,
    CreateBucketResponse,
    CreateDataFilterRequest,
    CreateDataFilterResponse,
    CreateGroupRequest,
    CreateGroupResponse,
    CreateMultipartUploadRequest,
    CreateMultipartUploadResponse,
    CreateObjectRequest,
    CreateObjectResponse,
    CreatePolicyRequest,
    CreatePolicyResponse,
    // Pool types
    CreatePoolRequest,
    CreatePoolResponse,
    // Tenant types
    CreateTenantRequest,
    CreateTenantResponse,
    CreateUserRequest,
    CreateUserResponse,
    DeleteAccessKeyRequest,
    DeleteAccessKeyResponse,
    // Bucket SSE types
    BucketSseConfiguration,
    CreateKmsKeyRequest,
    CreateKmsKeyResponse,
    DeleteBucketEncryptionRequest,
    DeleteBucketEncryptionResponse,
    DeleteKmsKeyRequest,
    DeleteKmsKeyResponse,
    // Lifecycle types
    DeleteBucketLifecycleRequest,
    DeleteBucketLifecycleResponse,
    DeleteBucketPolicyRequest,
    DeleteBucketPolicyResponse,
    DeleteBucketRequest,
    DeleteBucketResponse,
    DeleteConfigRequest,
    DeleteConfigResponse,
    DeleteDataFilterRequest,
    DeleteDataFilterResponse,
    DeleteGroupRequest,
    DeleteGroupResponse,
    DeleteObjectRequest,
    DeleteObjectResponse,
    DeletePolicyRequest,
    DeletePolicyResponse,
    DeletePoolRequest,
    DeletePoolResponse,
    DeleteTenantRequest,
    DeleteTenantResponse,
    DeleteUserRequest,
    DeleteUserResponse,
    // Delta Sharing types
    DeltaAddTableRequest,
    DeltaAddTableResponse,
    DeltaCreateRecipientRequest,
    DeltaCreateRecipientResponse,
    DeltaCreateShareRequest,
    DeltaCreateShareResponse,
    DeltaDropRecipientRequest,
    DeltaDropRecipientResponse,
    DeltaDropShareRequest,
    DeltaDropShareResponse,
    DeltaGetRecipientByTokenRequest,
    DeltaGetRecipientByTokenResponse,
    DeltaGetShareRequest,
    DeltaGetShareResponse,
    DeltaListRecipientsRequest,
    DeltaListRecipientsResponse,
    DeltaListSharesRequest,
    DeltaListSharesResponse,
    DeltaListTablesRequest,
    DeltaListTablesResponse,
    DeltaRecipientEntry,
    DeltaRemoveTableRequest,
    DeltaRemoveTableResponse,
    DeltaShareEntry,
    DeltaShareTableEntry,
    DetachPolicyRequest,
    DetachPolicyResponse,
    ErasureType,
    GetAccessKeyForAuthRequest,
    GetAccessKeyForAuthResponse,
    GetBucketEncryptionRequest,
    GetBucketEncryptionResponse,
    GetBucketLifecycleRequest,
    GetBucketLifecycleResponse,
    GetBucketPolicyRequest,
    GetBucketPolicyResponse,
    GetBucketRequest,
    GetBucketResponse,
    // Versioning types
    GetBucketVersioningRequest,
    GetBucketVersioningResponse,
    GetConfigRequest,
    GetConfigResponse,
    GetDataFiltersForPrincipalRequest,
    GetListingNodesRequest,
    GetListingNodesResponse,
    GetKmsKeyRequest,
    GetKmsKeyResponse,
    GetMultipartUploadRequest,
    GetMultipartUploadResponse,
    // Object lock types
    GetObjectLockConfigRequest,
    GetObjectLockConfigResponse,
    GetObjectRequest,
    GetObjectResponse,
    GetPlacementRequest,
    GetPlacementResponse,
    GetPolicyRequest,
    GetPolicyResponse,
    GetPoolRequest,
    GetPoolResponse,
    GetTenantRequest,
    GetTenantResponse,
    GetUserGroupsRequest,
    GetUserGroupsResponse,
    GetUserRequest,
    GetUserResponse,
    GroupMeta,
    // Iceberg types
    IcebergCommitTableRequest,
    IcebergCommitTableResponse,
    IcebergCreateNamespaceRequest,
    IcebergCreateNamespaceResponse,
    IcebergCreateTableRequest,
    IcebergCreateTableResponse,
    IcebergCreateWarehouseRequest,
    IcebergCreateWarehouseResponse,
    IcebergDataFilter,
    IcebergDeleteWarehouseRequest,
    IcebergDeleteWarehouseResponse,
    IcebergDropNamespaceRequest,
    IcebergDropNamespaceResponse,
    IcebergDropTableRequest,
    IcebergDropTableResponse,
    IcebergGetTablePolicyRequest,
    IcebergGetTablePolicyResponse,
    IcebergListNamespacesRequest,
    IcebergListNamespacesResponse,
    IcebergListTablesRequest,
    IcebergListTablesResponse,
    IcebergListWarehousesRequest,
    IcebergListWarehousesResponse,
    IcebergLoadNamespaceRequest,
    IcebergLoadNamespaceResponse,
    IcebergLoadTableRequest,
    IcebergLoadTableResponse,
    IcebergNamespace,
    IcebergNamespaceExistsRequest,
    IcebergNamespaceExistsResponse,
    IcebergRenameTableRequest,
    IcebergRenameTableResponse,
    IcebergSetTablePolicyRequest,
    IcebergSetTablePolicyResponse,
    IcebergTableEntry,
    IcebergTableExistsRequest,
    IcebergTableExistsResponse,
    IcebergTableIdentifier,
    IcebergUpdateNamespacePropertiesRequest,
    IcebergUpdateNamespacePropertiesResponse,
    IcebergWarehouse,
    KeyStatus,
    KmsKey,
    LifecycleConfiguration,
    ListKmsKeysRequest,
    ListKmsKeysResponse,
    ListAccessKeysRequest,
    ListAccessKeysResponse,
    ListAttachedPoliciesRequest,
    ListAttachedPoliciesResponse,
    ListBucketsRequest,
    ListBucketsResponse,
    ListConfigRequest,
    ListConfigResponse,
    ListDataFiltersRequest,
    ListDataFiltersResponse,
    ListGroupsRequest,
    ListGroupsResponse,
    ListMultipartUploadsRequest,
    ListMultipartUploadsResponse,
    ListObjectsRequest,
    ListObjectsResponse,
    ListPartsRequest,
    ListPartsResponse,
    ListPoliciesRequest,
    ListPoliciesResponse,
    ListPoolsRequest,
    ListPoolsResponse,
    ListTenantsRequest,
    ListTenantsResponse,
    ListUsersRequest,
    ListUsersResponse,
    ListingNode,
    MultipartUpload,
    NodePlacement,
    ObjectLockConfiguration,
    ObjectMeta,
    PartMeta,
    PolicyObject,
    PoolConfig,
    PutBucketEncryptionRequest,
    PutBucketEncryptionResponse,
    PutBucketLifecycleRequest,
    PutBucketLifecycleResponse,
    PutBucketVersioningRequest,
    PutBucketVersioningResponse,
    PutObjectLockConfigRequest,
    PutObjectLockConfigResponse,
    RegisterOsdRequest,
    RegisterOsdResponse,
    RegisterPartRequest,
    RegisterPartResponse,
    RemoveUserFromGroupRequest,
    RemoveUserFromGroupResponse,
    SetBucketPolicyRequest,
    SetBucketPolicyResponse,
    SetConfigRequest,
    SetConfigResponse,
    ShardType,
    TenantConfig,
    UpdatePoolRequest,
    UpdatePoolResponse,
    UpdateTenantRequest,
    UpdateTenantResponse,
    UserMeta,
    UserStatus,
    VersioningState,
    metadata_service_server::MetadataService,
};
use parking_lot::RwLock;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Metadata service state
///
/// Note: Object metadata is stored on OSDs (primary OSD for each object).
/// The meta service only stores cluster configuration (buckets, topology, policies).
/// ListObjects uses scatter-gather to query OSDs directly.
pub struct MetaService {
    /// Bucket metadata: name -> BucketMeta
    buckets: RwLock<HashMap<String, BucketMeta>>,
    /// Bucket policies: bucket_name -> policy_json
    bucket_policies: RwLock<HashMap<String, String>>,
    /// In-progress multipart uploads: upload_id -> MultipartUploadState
    multipart_uploads: RwLock<HashMap<String, MultipartUploadState>>,
    /// Registered OSD nodes (legacy)
    osd_nodes: RwLock<Vec<OsdNode>>,
    /// Cluster topology for CRUSH 2.0
    topology: RwLock<ClusterTopology>,
    /// CRUSH 2.0 placement engine
    crush: RwLock<Crush2>,
    /// Default erasure coding configuration
    default_ec: EcConfig,
    /// Default erasure coding parameters (for backward compat)
    default_ec_k: u32,
    default_ec_m: u32,
    /// IAM: Users indexed by user_id
    users: RwLock<HashMap<String, StoredUser>>,
    /// IAM: Access keys indexed by access_key_id
    access_keys: RwLock<HashMap<String, StoredAccessKey>>,
    /// IAM: Map from user_id to their access_key_ids
    user_keys: RwLock<HashMap<String, Vec<String>>>,
    /// IAM: Groups indexed by group_id
    groups: RwLock<HashMap<String, StoredGroup>>,
    /// Iceberg: data filters indexed by filter_id
    data_filters: RwLock<HashMap<String, StoredDataFilter>>,
    /// Iceberg: namespace key -> properties (prost-encoded bytes in store, HashMap in memory)
    iceberg_namespaces: RwLock<HashMap<String, HashMap<String, String>>>,
    /// Iceberg: table key ("ns\0table") -> IcebergTableEntry
    iceberg_tables: RwLock<HashMap<String, IcebergTableEntry>>,
    /// Delta Sharing: share name -> DeltaShareEntry
    delta_shares: RwLock<HashMap<String, DeltaShareEntry>>,
    /// Delta Sharing: "{share}\x00{schema}\x00{table}" -> DeltaShareTableEntry
    delta_tables: RwLock<HashMap<String, DeltaShareTableEntry>>,
    /// Delta Sharing: recipient name -> DeltaRecipientEntry
    delta_recipients: RwLock<HashMap<String, DeltaRecipientEntry>>,
    /// Delta Sharing: token_hash -> recipient name (reverse index for auth lookups)
    delta_token_index: RwLock<HashMap<String, String>>,
    /// Cluster configuration: key -> ConfigEntry (prost-encoded)
    config: RwLock<HashMap<String, ConfigEntry>>,
    /// Config version counter (monotonically increasing)
    config_version: std::sync::atomic::AtomicU64,
    /// Server pools: name -> PoolConfig
    pools: RwLock<HashMap<String, PoolConfig>>,
    /// Tenants: name -> TenantConfig
    tenants: RwLock<HashMap<String, TenantConfig>>,
    /// IAM policies: name -> PolicyObject
    iam_policies: RwLock<HashMap<String, PolicyObject>>,
    /// Policy attachments: "user:{id}" or "group:{id}" -> Vec<policy_name>
    policy_attachments: RwLock<HashMap<String, Vec<String>>>,
    /// Iceberg warehouses: warehouse_name -> IcebergWarehouse
    iceberg_warehouses: RwLock<HashMap<String, IcebergWarehouse>>,
    /// Object lock configurations: bucket_name -> ObjectLockConfiguration
    object_lock_configs: RwLock<HashMap<String, ObjectLockConfiguration>>,
    /// Lifecycle configurations: bucket_name -> LifecycleConfiguration
    lifecycle_configs: RwLock<HashMap<String, LifecycleConfiguration>>,
    /// Bucket default SSE configurations: bucket_name -> BucketSseConfiguration
    bucket_encryption_configs: RwLock<HashMap<String, BucketSseConfiguration>>,
    /// KMS keys (material already wrapped by gateway's service master key):
    /// key_id -> KmsKey
    kms_keys: RwLock<HashMap<String, KmsKey>>,
    /// Active license — drives node-count + raw-capacity caps enforced on
    /// `register_osd`. Default is Community (`0`/`0`, i.e. unlimited) so an
    /// unconfigured meta never refuses registrations. Reload happens via
    /// `set_config` when the `license/active` key changes.
    license: RwLock<Arc<objectio_license::License>>,
    /// Persistent store (None = in-memory only)
    store: Option<Arc<MetaStore>>,
}

/// Statistics for the metadata service
#[derive(Debug, Clone, Default)]
pub struct MetaStats {
    pub bucket_count: u64,
    pub object_count: u64,
    pub osd_count: u64,
    pub user_count: u64,
}

impl MetaService {
    /// Create a new metadata service with default MDS 4+2 configuration
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::with_ec_config(EcConfig::default())
    }

    /// Get statistics for metrics
    pub fn stats(&self) -> MetaStats {
        let bucket_count = self.buckets.read().len() as u64;
        // Note: Object counts are tracked per-OSD, not in metadata service
        // This would need to aggregate from OSD health reports in a full implementation
        let object_count = 0u64;
        let osd_count = self.topology.read().all_nodes().count() as u64;
        let user_count = self.users.read().len() as u64;

        MetaStats {
            bucket_count,
            object_count,
            osd_count,
            user_count,
        }
    }

    /// Create a new metadata service with custom EC configuration
    pub fn with_ec_config(ec_config: EcConfig) -> Self {
        let topology = ClusterTopology::new();
        let crush = Crush2::new(topology.clone(), 64); // 64 stripe groups

        // For replication mode: k=1 (full data), m=0 (no parity)
        // The gateway will skip EC encoding entirely
        let (default_ec_k, default_ec_m) = match &ec_config {
            EcConfig::Mds { k, m } => (*k as u32, *m as u32),
            EcConfig::Lrc { k, l, g } => (*k as u32, (*l + *g) as u32),
            EcConfig::Replication { count: _ } => (1, 0), // No EC, just raw data
        };

        Self {
            buckets: RwLock::new(HashMap::new()),
            bucket_policies: RwLock::new(HashMap::new()),
            multipart_uploads: RwLock::new(HashMap::new()),
            osd_nodes: RwLock::new(Vec::new()),
            topology: RwLock::new(topology),
            crush: RwLock::new(crush),
            default_ec: ec_config,
            default_ec_k,
            default_ec_m,
            users: RwLock::new(HashMap::new()),
            access_keys: RwLock::new(HashMap::new()),
            user_keys: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            data_filters: RwLock::new(HashMap::new()),
            iceberg_namespaces: RwLock::new(HashMap::new()),
            iceberg_tables: RwLock::new(HashMap::new()),
            delta_shares: RwLock::new(HashMap::new()),
            delta_tables: RwLock::new(HashMap::new()),
            delta_recipients: RwLock::new(HashMap::new()),
            delta_token_index: RwLock::new(HashMap::new()),
            config: RwLock::new(HashMap::new()),
            config_version: std::sync::atomic::AtomicU64::new(0),
            pools: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            iam_policies: RwLock::new(HashMap::new()),
            policy_attachments: RwLock::new(HashMap::new()),
            iceberg_warehouses: RwLock::new(HashMap::new()),
            object_lock_configs: RwLock::new(HashMap::new()),
            lifecycle_configs: RwLock::new(HashMap::new()),
            bucket_encryption_configs: RwLock::new(HashMap::new()),
            kms_keys: RwLock::new(HashMap::new()),
            license: RwLock::new(Arc::new(objectio_license::License::community())),
            store: None,
        }
    }

    /// Create a metadata service backed by persistent storage.
    /// Loads all existing data from the store on startup.
    pub fn with_store(ec_config: EcConfig, store: Arc<MetaStore>) -> Self {
        let mut svc = Self::with_ec_config(ec_config);
        svc.store = Some(store);
        svc.load_from_store();
        svc
    }

    /// Returns true if the store already has OSD nodes persisted.
    pub fn has_persisted_osds(&self) -> bool {
        !self.osd_nodes.read().is_empty()
    }

    /// Load all data from the persistent store into in-memory maps.
    fn load_from_store(&self) {
        let Some(store) = &self.store else { return };

        // Buckets
        match store.load_buckets() {
            Ok(buckets) => {
                let mut map = self.buckets.write();
                for (name, bucket) in buckets {
                    map.insert(name, bucket);
                }
                info!("Loaded {} buckets from store", map.len());
            }
            Err(e) => error!("Failed to load buckets: {}", e),
        }

        // Bucket policies
        match store.load_bucket_policies() {
            Ok(policies) => {
                let mut map = self.bucket_policies.write();
                for (name, policy) in policies {
                    map.insert(name, policy);
                }
                info!("Loaded {} bucket policies from store", map.len());
            }
            Err(e) => error!("Failed to load bucket policies: {}", e),
        }

        // Multipart uploads
        match store.load_multipart_uploads() {
            Ok(uploads) => {
                let mut map = self.multipart_uploads.write();
                for (id, state) in uploads {
                    map.insert(id, state);
                }
                info!("Loaded {} multipart uploads from store", map.len());
            }
            Err(e) => error!("Failed to load multipart uploads: {}", e),
        }

        // OSD nodes + topology rebuild
        match store.load_osd_nodes() {
            Ok(nodes) => {
                let count = nodes.len();
                // Dedupe by address on load: if the persistent store still
                // holds pre-cleanup duplicates from older binaries, keep
                // only the newest entry per address (last-wins). This
                // one-shot cleanup prevents ghost OSDs from surviving
                // a gateway rolling upgrade.
                let mut by_address: std::collections::HashMap<String, OsdNode> =
                    std::collections::HashMap::new();
                for (_hex_id, node) in nodes {
                    by_address.insert(node.address.clone(), node);
                }
                let deduped: Vec<OsdNode> = by_address.into_values().collect();
                let evicted = count - deduped.len();
                if evicted > 0 {
                    warn!(
                        "Deduped {} stale OSD entries on startup (same address, stale node_id)",
                        evicted
                    );
                }
                let mut osd_nodes = self.osd_nodes.write();
                for node in deduped {
                    osd_nodes.push(node);
                }
                info!("Loaded {} OSD nodes from store", osd_nodes.len());
            }
            Err(e) => error!("Failed to load OSD nodes: {}", e),
        }

        // Topology
        match store.load_topology() {
            Ok(Some(topology)) => {
                info!(
                    "Loaded cluster topology from store (version={})",
                    topology.version
                );
                *self.topology.write() = topology.clone();
                self.crush.write().update_topology(topology);
            }
            Ok(None) => {
                // Rebuild topology from OSD nodes if no stored topology
                let nodes = self.osd_nodes.read().clone();
                for node in &nodes {
                    self.update_topology_with_node(node);
                }
            }
            Err(e) => error!("Failed to load topology: {}", e),
        }

        // Users
        match store.load_users() {
            Ok(users) => {
                let mut user_map = self.users.write();
                let mut user_keys = self.user_keys.write();
                for (id, user) in users {
                    user_keys.entry(id.clone()).or_default();
                    user_map.insert(id, user);
                }
                info!("Loaded {} users from store", user_map.len());
            }
            Err(e) => error!("Failed to load users: {}", e),
        }

        // Access keys (rebuild user_keys index)
        match store.load_access_keys() {
            Ok(keys) => {
                let mut key_map = self.access_keys.write();
                let mut user_keys = self.user_keys.write();
                for (id, key) in keys {
                    user_keys
                        .entry(key.user_id.clone())
                        .or_default()
                        .push(id.clone());
                    key_map.insert(id, key);
                }
                info!("Loaded {} access keys from store", key_map.len());
            }
            Err(e) => error!("Failed to load access keys: {}", e),
        }

        // Groups
        match store.load_groups() {
            Ok(groups) => {
                let mut group_map = self.groups.write();
                for (id, group) in groups {
                    group_map.insert(id, group);
                }
                info!("Loaded {} groups from store", group_map.len());
            }
            Err(e) => error!("Failed to load groups: {}", e),
        }

        // Data filters
        match store.load_data_filters() {
            Ok(filters) => {
                let mut filter_map = self.data_filters.write();
                for (id, filter) in filters {
                    filter_map.insert(id, filter);
                }
                info!("Loaded {} data filters from store", filter_map.len());
            }
            Err(e) => error!("Failed to load data filters: {}", e),
        }

        // Iceberg namespaces
        match store.list_iceberg_namespaces("") {
            Ok(entries) => {
                let mut ns_map = self.iceberg_namespaces.write();
                for (key, bytes) in entries {
                    match IcebergCreateNamespaceResponse::decode(bytes.as_slice()) {
                        Ok(resp) => {
                            ns_map.insert(key, resp.properties);
                        }
                        Err(e) => error!("Failed to decode iceberg namespace '{}': {}", key, e),
                    }
                }
                info!("Loaded {} iceberg namespaces from store", ns_map.len());
            }
            Err(e) => error!("Failed to load iceberg namespaces: {}", e),
        }

        // Iceberg tables
        match store.list_iceberg_tables("") {
            Ok(entries) => {
                let mut tbl_map = self.iceberg_tables.write();
                for (key, bytes) in entries {
                    match IcebergTableEntry::decode(bytes.as_slice()) {
                        Ok(entry) => {
                            if entry.metadata_json.is_empty() {
                                warn!(
                                    "Iceberg table '{}' has no inline metadata \
                                     (created by older catalog version); \
                                     load-table will fail until it is dropped and re-created",
                                    key
                                );
                            }
                            tbl_map.insert(key, entry);
                        }
                        Err(e) => error!("Failed to decode iceberg table '{}': {}", key, e),
                    }
                }
                info!("Loaded {} iceberg tables from store", tbl_map.len());
            }
            Err(e) => error!("Failed to load iceberg tables: {}", e),
        }

        // Delta Sharing: shares
        match store.load_delta_shares() {
            Ok(entries) => {
                let mut map = self.delta_shares.write();
                for (key, bytes) in entries {
                    match DeltaShareEntry::decode(bytes.as_slice()) {
                        Ok(entry) => {
                            map.insert(key, entry);
                        }
                        Err(e) => error!("Failed to decode delta share '{}': {}", key, e),
                    }
                }
                info!("Loaded {} delta shares from store", map.len());
            }
            Err(e) => error!("Failed to load delta shares: {}", e),
        }

        // Delta Sharing: tables
        match store.load_delta_tables() {
            Ok(entries) => {
                let mut map = self.delta_tables.write();
                for (key, bytes) in entries {
                    match DeltaShareTableEntry::decode(bytes.as_slice()) {
                        Ok(entry) => {
                            map.insert(key, entry);
                        }
                        Err(e) => error!("Failed to decode delta table '{}': {}", key, e),
                    }
                }
                info!("Loaded {} delta tables from store", map.len());
            }
            Err(e) => error!("Failed to load delta tables: {}", e),
        }

        // Delta Sharing: recipients (rebuild token index)
        match store.load_delta_recipients() {
            Ok(entries) => {
                let mut map = self.delta_recipients.write();
                let mut token_index = self.delta_token_index.write();
                for (key, bytes) in entries {
                    match DeltaRecipientEntry::decode(bytes.as_slice()) {
                        Ok(entry) => {
                            token_index.insert(entry.token_hash.clone(), key.clone());
                            map.insert(key, entry);
                        }
                        Err(e) => error!("Failed to decode delta recipient '{}': {}", key, e),
                    }
                }
                info!("Loaded {} delta recipients from store", map.len());
            }
            Err(e) => error!("Failed to load delta recipients: {}", e),
        }

        // Cluster config
        {
            let entries = store.load_all_config();
            let mut map = self.config.write();
            let mut max_version = 0u64;
            for (key, bytes) in entries {
                match ConfigEntry::decode(bytes.as_slice()) {
                    Ok(entry) => {
                        max_version = max_version.max(entry.version);
                        map.insert(key, entry);
                    }
                    Err(e) => error!("Failed to decode config entry: {}", e),
                }
            }
            self.config_version
                .store(max_version, std::sync::atomic::Ordering::SeqCst);
            info!("Loaded {} config entries from store", map.len());
            // Re-hydrate the license from `license/active` so caps are
            // enforced from the first `register_osd` after restart. Stay
            // on Community if the entry is missing, malformed, or expired
            // — never hard-fail meta startup on a broken license.
            if let Some(entry) = map.get("license/active") {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                match objectio_license::License::load_from_bytes(&entry.value, now) {
                    Ok(l) => {
                        info!(
                            "meta license loaded: tier={} licensee={} max_nodes={} max_raw_capacity_bytes={}",
                            l.tier, l.licensee, l.max_nodes, l.max_raw_capacity_bytes
                        );
                        *self.license.write() = Arc::new(l);
                    }
                    Err(e) => warn!("license/active rejected at meta startup: {}", e),
                }
            }
        }

        // Server pools
        {
            let entries = store.load_all_pools();
            let mut map = self.pools.write();
            for (key, bytes) in entries {
                match PoolConfig::decode(bytes.as_slice()) {
                    Ok(pool) => {
                        map.insert(key, pool);
                    }
                    Err(e) => error!("Failed to decode pool: {}", e),
                }
            }
            info!("Loaded {} server pools from store", map.len());
        }

        // Tenants
        {
            let entries = store.load_all_tenants();
            let mut map = self.tenants.write();
            for (key, bytes) in entries {
                match TenantConfig::decode(bytes.as_slice()) {
                    Ok(tenant) => {
                        map.insert(key, tenant);
                    }
                    Err(e) => error!("Failed to decode tenant: {}", e),
                }
            }
            info!("Loaded {} tenants from store", map.len());
        }

        // Iceberg warehouses
        {
            let entries = store.load_all_warehouses();
            let mut map = self.iceberg_warehouses.write();
            for (key, bytes) in entries {
                match IcebergWarehouse::decode(bytes.as_slice()) {
                    Ok(wh) => {
                        map.insert(key, wh);
                    }
                    Err(e) => error!("Failed to decode warehouse: {}", e),
                }
            }
            info!("Loaded {} iceberg warehouses from store", map.len());
        }

        // Object lock configs
        {
            let entries = store.load_all_object_lock_configs();
            let mut map = self.object_lock_configs.write();
            for (key, bytes) in entries {
                match ObjectLockConfiguration::decode(bytes.as_slice()) {
                    Ok(config) => {
                        map.insert(key, config);
                    }
                    Err(e) => error!("Failed to decode object lock config: {}", e),
                }
            }
            info!("Loaded {} object lock configs from store", map.len());
        }

        // Lifecycle configs
        {
            let entries = store.load_all_lifecycle_configs();
            let mut map = self.lifecycle_configs.write();
            for (key, bytes) in entries {
                match LifecycleConfiguration::decode(bytes.as_slice()) {
                    Ok(config) => {
                        map.insert(key, config);
                    }
                    Err(e) => error!("Failed to decode lifecycle config: {}", e),
                }
            }
            info!("Loaded {} lifecycle configs from store", map.len());
        }

        // Bucket default SSE configs
        {
            let entries = store.load_all_bucket_encryption_configs();
            let mut map = self.bucket_encryption_configs.write();
            for (key, bytes) in entries {
                match BucketSseConfiguration::decode(bytes.as_slice()) {
                    Ok(config) => {
                        map.insert(key, config);
                    }
                    Err(e) => error!("Failed to decode bucket encryption config: {}", e),
                }
            }
            info!("Loaded {} bucket encryption configs from store", map.len());
        }

        // KMS keys
        {
            let entries = store.load_all_kms_keys();
            let mut map = self.kms_keys.write();
            for (key_id, bytes) in entries {
                match KmsKey::decode(bytes.as_slice()) {
                    Ok(k) => {
                        map.insert(key_id, k);
                    }
                    Err(e) => error!("Failed to decode KMS key: {}", e),
                }
            }
            info!("Loaded {} KMS keys from store", map.len());
        }

        // IAM policies
        {
            let entries = store.load_all_iam_policies();
            let mut map = self.iam_policies.write();
            for (name, bytes) in entries {
                match PolicyObject::decode(bytes.as_slice()) {
                    Ok(policy) => {
                        map.insert(name, policy);
                    }
                    Err(e) => error!("Failed to decode IAM policy: {}", e),
                }
            }
            info!("Loaded {} IAM policies from store", map.len());

            // Insert built-in policies if they don't already exist
            let builtins = [
                (
                    "readonly",
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:GetBucketLocation"],"Resource":["arn:obio:s3:::*/*"]}]}"#,
                ),
                (
                    "readwrite",
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:obio:s3:::*","arn:obio:s3:::*/*"]}]}"#,
                ),
                (
                    "writeonly",
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:obio:s3:::*/*"]}]}"#,
                ),
                (
                    "consoleAdmin",
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*","admin:*"],"Resource":["*"]}]}"#,
                ),
            ];
            let now = Self::current_timestamp();
            for (name, json) in builtins {
                if !map.contains_key(name) {
                    let policy = PolicyObject {
                        name: name.to_string(),
                        policy_json: json.to_string(),
                        created_at: now,
                        updated_at: now,
                    };
                    store.put_iam_policy(name, &policy.encode_to_vec());
                    map.insert(name.to_string(), policy);
                }
            }
        }

        // Policy attachments
        {
            let entries = store.load_all_policy_attachments();
            let mut map = self.policy_attachments.write();
            for (key, csv) in entries {
                let policies: Vec<String> = csv
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
                map.insert(key, policies);
            }
            info!("Loaded {} policy attachments from store", map.len());
        }
    }

    /// Create admin user if no users exist
    pub fn ensure_admin(&self, admin_name: &str) -> Option<(String, String)> {
        let users = self.users.read();
        if !users.is_empty() {
            // Users exist, check if admin already has keys
            drop(users);
            let user = self
                .users
                .read()
                .values()
                .find(|u| u.display_name == admin_name)
                .cloned();
            if let Some(admin_user) = user {
                let keys = self.user_keys.read();
                if let Some(key_ids) = keys.get(&admin_user.user_id)
                    && let Some(first_key_id) = key_ids.first()
                    && let Some(key) = self.access_keys.read().get(first_key_id)
                {
                    return Some((key.access_key_id.clone(), key.secret_access_key.clone()));
                }
            }
            return None;
        }
        drop(users);

        // Create admin user
        let user_id = Uuid::new_v4().to_string();
        let now = Self::current_timestamp();
        let user = StoredUser {
            user_id: user_id.clone(),
            display_name: admin_name.to_string(),
            arn: format!("arn:objectio:iam::user/{}", admin_name),
            status: UserStatus::UserActive as i32,
            created_at: now,
            email: String::new(),
            tenant: String::new(), // system admin has no tenant
        };

        self.users.write().insert(user_id.clone(), user.clone());
        self.user_keys.write().insert(user_id.clone(), Vec::new());

        // Create access key
        let access_key_id = Self::generate_access_key_id();
        let secret_access_key = Self::generate_secret_access_key();

        let key = StoredAccessKey {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            user_id: user_id.clone(),
            status: KeyStatus::KeyActive as i32,
            created_at: now,
            tenant: String::new(),
        };

        self.access_keys
            .write()
            .insert(access_key_id.clone(), key.clone());
        self.user_keys
            .write()
            .entry(user_id)
            .or_default()
            .push(access_key_id.clone());

        // Persist admin user + key atomically
        if let Some(store) = &self.store {
            store.put_user_and_key(&user, &key);
        }

        info!(
            "Created admin user '{}' with access key {}",
            admin_name, access_key_id
        );

        Some((access_key_id, secret_access_key))
    }

    /// Generate AWS-style access key ID (20 chars, starts with AKIA)
    fn generate_access_key_id() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".chars().collect();
        let suffix: String = (0..16)
            .map(|_| chars[rng.gen_range(0..chars.len())])
            .collect();
        format!("AKIA{}", suffix)
    }

    /// Generate AWS-style secret access key (40 chars, base64-like)
    fn generate_secret_access_key() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            .chars()
            .collect();
        (0..40)
            .map(|_| chars[rng.gen_range(0..chars.len())])
            .collect()
    }

    /// Register an OSD node for placement (legacy method)
    pub fn register_osd(&self, node: OsdNode) {
        info!(
            "Registering OSD node: {} at {}",
            hex::encode(node.node_id),
            node.address
        );
        self.osd_nodes.write().push(node.clone());

        // Also update the CRUSH topology
        self.update_topology_with_node(&node);

        // Persist OSD + topology atomically
        if let Some(store) = &self.store {
            let topology = self.topology.read().clone();
            store.put_osd_and_topology(&hex::encode(node.node_id), &node, &topology);
        }
    }

    /// Update CRUSH topology with a new OSD node
    fn update_topology_with_node(&self, osd_node: &OsdNode) {
        // Prefer the 5-level `topology` when present; fall back to the
        // legacy 3-tuple for OsdNodes persisted before zone/host existed.
        let (region, zone, dc, rack, host) = osd_node
            .topology
            .clone()
            .or_else(|| {
                osd_node
                    .failure_domain
                    .clone()
                    .map(|(r, dc, rack)| (r, String::new(), dc, rack, String::new()))
            })
            .unwrap_or_else(|| {
                (
                    "default".to_string(),
                    String::new(),
                    "dc1".to_string(),
                    "rack1".to_string(),
                    String::new(),
                )
            });

        let node_id = NodeId::from_bytes(osd_node.node_id);

        let disks: Vec<DiskInfo> = osd_node
            .disk_ids
            .iter()
            .map(|disk_id| {
                DiskInfo {
                    id: objectio_common::DiskId::from_bytes(*disk_id),
                    path: String::new(),
                    total_capacity: 1_000_000_000_000, // 1TB default
                    used_capacity: 0,
                    status: objectio_common::DiskStatus::Healthy,
                    weight: 1.0,
                }
            })
            .collect();

        let node_info = NodeInfo {
            id: node_id,
            name: hex::encode(&osd_node.node_id[..4]),
            address: osd_node
                .address
                .parse()
                .unwrap_or_else(|_| "0.0.0.0:9200".parse().unwrap()),
            failure_domain: FailureDomainInfo::new_full(&region, &zone, &dc, &rack, &host),
            status: NodeStatus::Active,
            disks,
            weight: 1.0,
            last_heartbeat: Self::current_timestamp(),
        };

        // Update topology and rebuild CRUSH
        {
            let mut topology = self.topology.write();
            topology.upsert_node(node_info);
        }

        // Rebuild CRUSH with updated topology
        {
            let topology = self.topology.read().clone();
            let mut crush = self.crush.write();
            crush.update_topology(topology);
        }

        debug!(
            "Updated CRUSH topology with node {}",
            hex::encode(osd_node.node_id)
        );
    }

    /// Encode namespace levels into a store key, scoped by warehouse.
    /// Format: "warehouse\x01ns1\x00ns2" or "ns1\x00ns2" (if warehouse is empty)
    fn iceberg_ns_key_wh(warehouse: &str, levels: &[String]) -> String {
        let ns = levels.join("\x00");
        if warehouse.is_empty() {
            ns
        } else {
            format!("{warehouse}\x01{ns}")
        }
    }

    /// Encode namespace + table name into a store key, scoped by warehouse.
    fn iceberg_table_key_wh(warehouse: &str, ns_levels: &[String], table_name: &str) -> String {
        let ns = Self::iceberg_ns_key_wh(warehouse, ns_levels);
        format!("{ns}\x00{table_name}")
    }

    /// Warehouse prefix for scanning all namespaces in a warehouse.
    fn iceberg_warehouse_prefix(warehouse: &str) -> String {
        if warehouse.is_empty() {
            String::new()
        } else {
            format!("{warehouse}\x01")
        }
    }

    /// Legacy helpers (no warehouse scope) — kept for backward compat
    fn iceberg_ns_key(levels: &[String]) -> String {
        levels.join("\x00")
    }

    fn iceberg_table_key(ns_levels: &[String], table_name: &str) -> String {
        let ns = Self::iceberg_ns_key(ns_levels);
        format!("{ns}\x00{table_name}")
    }

    /// Generate object key for internal storage
    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Generate a short stable id for a new KMS key: `kms-<first 12 hex of UUID>`.
    fn generate_kms_key_id() -> String {
        let uuid = Uuid::new_v4().simple().to_string();
        format!("kms-{}", &uuid[..12])
    }

    /// Legacy placement algorithm (fallback when no CRUSH topology)
    async fn get_placement_legacy(
        &self,
        req: &GetPlacementRequest,
    ) -> Result<Response<GetPlacementResponse>, Status> {
        let nodes = self.osd_nodes.read();

        // Determine number of shards and EC type based on config
        let (total_shards, ec_type, replication_count) = match &self.default_ec {
            EcConfig::Mds { k, m } => ((*k + *m) as usize, ErasureType::ErasureMds, 0u32),
            EcConfig::Lrc { k, l, g } => ((*k + *l + *g) as usize, ErasureType::ErasureLrc, 0u32),
            EcConfig::Replication { count } => (
                *count as usize,
                ErasureType::ErasureReplication,
                *count as u32,
            ),
        };

        if nodes.is_empty() {
            return Err(Status::unavailable("no storage nodes available"));
        }

        // Collect all available disk placements (node, disk pairs)
        let mut all_disks: Vec<(&OsdNode, &[u8; 16])> = nodes
            .iter()
            .flat_map(|node| node.disk_ids.iter().map(move |disk_id| (node, disk_id)))
            .collect();

        // Use object key hash for deterministic placement
        let hash_seed = {
            let key_bytes = format!("{}/{}", req.bucket, req.key);
            key_bytes
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_add(b as u64))
        };

        // Rotate the disk list based on hash for distribution
        if !all_disks.is_empty() {
            let rotation = (hash_seed as usize) % all_disks.len();
            all_disks.rotate_left(rotation);
        }

        // Select disks for each shard position, spreading across nodes
        let mut placements: Vec<NodePlacement> = Vec::with_capacity(total_shards);
        let mut used_nodes: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();

        // First pass: try to use different nodes for each shard
        for pos in 0..total_shards {
            if placements.len() >= total_shards {
                break;
            }

            let disk_opt = all_disks
                .iter()
                .find(|(node, _)| !used_nodes.contains(&node.node_id));

            if let Some((node, disk_id)) = disk_opt {
                let pos_u32 = pos as u32;
                placements.push(NodePlacement {
                    position: pos_u32,
                    node_id: node.node_id.to_vec(),
                    node_address: node.address.clone(),
                    disk_id: disk_id.to_vec(),
                    shard_type: if pos_u32 < self.default_ec_k {
                        ShardType::ShardData.into()
                    } else {
                        ShardType::ShardGlobalParity.into()
                    },
                    local_group: 0,
                });
                used_nodes.insert(node.node_id);
            }
        }

        // Second pass: reuse nodes with different disks if needed
        if placements.len() < total_shards {
            for (node, disk_id) in all_disks.iter() {
                if placements.len() >= total_shards {
                    break;
                }
                let disk_used = placements.iter().any(|p| p.disk_id == disk_id.to_vec());
                if !disk_used {
                    let pos = placements.len() as u32;
                    placements.push(NodePlacement {
                        position: pos,
                        node_id: node.node_id.to_vec(),
                        node_address: node.address.clone(),
                        disk_id: disk_id.to_vec(),
                        shard_type: if pos < self.default_ec_k {
                            ShardType::ShardData.into()
                        } else {
                            ShardType::ShardGlobalParity.into()
                        },
                        local_group: 0,
                    });
                }
            }
        }

        // Third pass: allow disk reuse for single disk mode
        while placements.len() < total_shards {
            let idx = placements.len() % all_disks.len().max(1);
            if let Some((node, disk_id)) = all_disks.get(idx) {
                let pos = placements.len() as u32;
                placements.push(NodePlacement {
                    position: pos,
                    node_id: node.node_id.to_vec(),
                    node_address: node.address.clone(),
                    disk_id: disk_id.to_vec(),
                    shard_type: if pos < self.default_ec_k {
                        ShardType::ShardData.into()
                    } else {
                        ShardType::ShardGlobalParity.into()
                    },
                    local_group: 0,
                });
            } else {
                break;
            }
        }

        debug!(
            "Legacy placement for {}/{}: {} shards across {} nodes",
            req.bucket,
            req.key,
            placements.len(),
            used_nodes.len()
        );

        Ok(Response::new(GetPlacementResponse {
            storage_class: "STANDARD".to_string(),
            ec_k: self.default_ec_k,
            ec_m: self.default_ec_m,
            nodes: placements,
            ec_type: ec_type.into(),
            ec_local_parity: 0,
            ec_global_parity: self.default_ec_m,
            local_group_size: 0,
            replication_count,
        }))
    }
}

#[tonic::async_trait]
impl MetadataService for MetaService {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("bucket name is required"));
        }

        // Check if bucket already exists
        if self.buckets.read().contains_key(&req.name) {
            return Err(Status::already_exists("bucket already exists"));
        }

        // Validate tenant exists if specified
        let tenant = req.tenant.clone();
        if !tenant.is_empty() && !self.tenants.read().contains_key(&tenant) {
            return Err(Status::not_found(format!("tenant '{}' not found", tenant)));
        }

        // Enforce tenant bucket quota
        if !tenant.is_empty()
            && let Some(tc) = self.tenants.read().get(&tenant)
            && tc.quota_buckets > 0
        {
            let count = self
                .buckets
                .read()
                .values()
                .filter(|b| b.tenant == tenant)
                .count() as u64;
            if count >= tc.quota_buckets {
                return Err(Status::resource_exhausted(format!(
                    "tenant '{}' bucket quota exceeded ({}/{})",
                    tenant, count, tc.quota_buckets
                )));
            }
        }

        let bucket = BucketMeta {
            name: req.name.clone(),
            owner: req.owner,
            created_at: Self::current_timestamp(),
            storage_class: if req.storage_class.is_empty() {
                "STANDARD".to_string()
            } else {
                req.storage_class
            },
            versioning: VersioningState::VersioningDisabled.into(),
            pool: String::new(),
            tenant,
            quota_bytes: 0,
            quota_objects: 0,
            object_lock: None,
        };

        self.buckets
            .write()
            .insert(req.name.clone(), bucket.clone());

        if let Some(store) = &self.store {
            store.put_bucket(&req.name, &bucket);
        }

        info!("Created bucket: {}", req.name);

        Ok(Response::new(CreateBucketResponse {
            bucket: Some(bucket),
        }))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let req = request.into_inner();

        // Check if bucket exists
        if !self.buckets.read().contains_key(&req.name) {
            return Err(Status::not_found("bucket not found"));
        }

        // Note: The check for whether bucket is empty should be done by the Gateway
        // using scatter-gather before calling delete_bucket. Object metadata is stored
        // on OSDs, so we can't check emptiness from the meta service.

        self.buckets.write().remove(&req.name);

        if let Some(store) = &self.store {
            store.delete_bucket(&req.name);
        }

        info!("Deleted bucket: {}", req.name);

        Ok(Response::new(DeleteBucketResponse { success: true }))
    }

    async fn get_bucket(
        &self,
        request: Request<GetBucketRequest>,
    ) -> Result<Response<GetBucketResponse>, Status> {
        let req = request.into_inner();

        let bucket = self
            .buckets
            .read()
            .get(&req.name)
            .cloned()
            .ok_or_else(|| Status::not_found("bucket not found"))?;

        Ok(Response::new(GetBucketResponse {
            bucket: Some(bucket),
        }))
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        let req = request.into_inner();

        let buckets: Vec<BucketMeta> = self
            .buckets
            .read()
            .values()
            .filter(|b| req.owner.is_empty() || b.owner == req.owner)
            .filter(|b| req.tenant.is_empty() || b.tenant == req.tenant)
            .cloned()
            .collect();

        Ok(Response::new(ListBucketsResponse { buckets }))
    }

    /// DEPRECATED: Object metadata is now stored on primary OSD
    /// This RPC is kept for backward compatibility but does nothing
    async fn create_object(
        &self,
        request: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "create_object called (deprecated): {}/{}. Object metadata should be stored on primary OSD.",
            req.bucket, req.key
        );
        // Return success but don't store anything - OSD is source of truth
        Ok(Response::new(CreateObjectResponse { object: None }))
    }

    /// DEPRECATED: Object metadata is now stored on primary OSD
    /// This RPC is kept for backward compatibility but does nothing
    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "delete_object called (deprecated): {}/{}. Object metadata should be deleted from primary OSD.",
            req.bucket, req.key
        );
        // Return success - OSD is source of truth
        Ok(Response::new(DeleteObjectResponse {
            success: true,
            version_id: String::new(),
        }))
    }

    /// DEPRECATED: Object metadata is now stored on primary OSD
    /// Use scatter-gather to query OSDs directly
    async fn get_object(
        &self,
        _request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        // Object metadata is stored on primary OSD, not in meta service
        Err(Status::unimplemented(
            "Object metadata is stored on primary OSD. Use GetObjectMeta RPC on OSD.",
        ))
    }

    /// DEPRECATED: Use scatter-gather to query OSDs directly
    /// The meta service no longer maintains an object index
    async fn list_objects(
        &self,
        _request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        // ListObjects should use scatter-gather to query OSDs directly
        Err(Status::unimplemented(
            "ListObjects should use scatter-gather via Gateway. Use GetListingNodes + ListObjectsMeta on OSDs.",
        ))
    }

    async fn get_placement(
        &self,
        request: Request<GetPlacementRequest>,
    ) -> Result<Response<GetPlacementResponse>, Status> {
        let req = request.into_inner();

        // Check if we have any nodes in the topology
        let active_node_count = {
            let topology = self.topology.read();
            topology.active_nodes().count()
        };

        if active_node_count == 0 {
            // Fall back to legacy placement if no CRUSH topology
            return self.get_placement_legacy(&req).await;
        }

        // Create object ID from bucket/key for deterministic placement
        let object_id = {
            let key_str = format!("{}/{}", req.bucket, req.key);
            let hash = xxhash_rust::xxh64::xxh64(key_str.as_bytes(), 0);
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&hash.to_le_bytes());
            bytes[8..16].copy_from_slice(&hash.to_be_bytes());
            objectio_common::ObjectId::from_uuid(Uuid::from_bytes(bytes))
        };

        // Resolve pool for this bucket — use pool-specific EC config if available
        let pool_name = {
            let buckets = self.buckets.read();
            buckets
                .get(&req.bucket)
                .map(|b| b.pool.clone())
                .unwrap_or_default()
        };
        let pool_ec = if !pool_name.is_empty() {
            self.pools.read().get(&pool_name).map(|p| {
                (
                    p.ec_type(),
                    p.ec_k,
                    p.ec_m,
                    p.ec_local_parity,
                    p.ec_global_parity,
                    p.replication_count,
                )
            })
        } else {
            None
        };

        // Select placement template based on pool EC config or global default
        let (
            template,
            ec_type,
            ec_k,
            ec_local_parity,
            ec_global_parity,
            local_group_size,
            replication_count,
        ) = if let Some((p_ec_type, p_k, p_m, p_lp, p_gp, p_rep)) = pool_ec {
            match p_ec_type {
                ErasureType::ErasureLrc => (
                    PlacementTemplate::lrc(p_k as u8, p_lp as u8, p_gp as u8),
                    ErasureType::ErasureLrc,
                    p_k,
                    p_lp,
                    p_gp,
                    if p_lp > 0 { p_k / p_lp } else { 0 },
                    0u32,
                ),
                ErasureType::ErasureReplication => (
                    PlacementTemplate::mds(p_rep as u8, 0),
                    ErasureType::ErasureReplication,
                    1u32,
                    0u32,
                    0u32,
                    0u32,
                    p_rep,
                ),
                _ => (
                    PlacementTemplate::mds(p_k as u8, p_m as u8),
                    ErasureType::ErasureMds,
                    p_k,
                    0u32,
                    p_m,
                    0u32,
                    0u32,
                ),
            }
        } else {
            // Fall back to global default EC config
            match &self.default_ec {
                EcConfig::Mds { k, m } => (
                    PlacementTemplate::mds(*k, *m),
                    ErasureType::ErasureMds,
                    *k as u32,
                    0u32,
                    *m as u32,
                    0u32,
                    0u32,
                ),
                EcConfig::Lrc { k, l, g } => (
                    PlacementTemplate::lrc(*k, *l, *g),
                    ErasureType::ErasureLrc,
                    *k as u32,
                    *l as u32,
                    *g as u32,
                    (*k / *l) as u32,
                    0u32,
                ),
                EcConfig::Replication { count } => (
                    PlacementTemplate::mds(*count, 0),
                    ErasureType::ErasureReplication,
                    1u32,
                    0u32,
                    0u32,
                    0u32,
                    *count as u32,
                ),
            }
        };

        // Use CRUSH 2.0 for placement
        let crush = self.crush.read();
        let hrw_placements = crush.select_placement(&object_id, &template);
        drop(crush);

        // Convert HRW placements to NodePlacement responses
        let nodes = self.osd_nodes.read();
        let placements: Vec<NodePlacement> = hrw_placements
            .iter()
            .map(|hrw| {
                // Find the OSD node by NodeId
                let node = nodes
                    .iter()
                    .find(|n| NodeId::from_bytes(n.node_id) == hrw.node_id);

                let (node_address, disk_id) = match node {
                    Some(n) => {
                        let disk = n
                            .disk_ids
                            .first()
                            .map(|d| d.to_vec())
                            .unwrap_or_else(|| vec![0u8; 16]);
                        (n.address.clone(), disk)
                    }
                    None => {
                        // Node not found in legacy list, use placeholder
                        warn!("Node {} not found in OSD list", hrw.node_id);
                        (String::new(), hrw.node_id.as_bytes().to_vec())
                    }
                };

                let shard_type = match hrw.role {
                    ShardRole::Data => ShardType::ShardData.into(),
                    ShardRole::LocalParity => ShardType::ShardLocalParity.into(),
                    ShardRole::GlobalParity => ShardType::ShardGlobalParity.into(),
                };

                NodePlacement {
                    position: hrw.position as u32,
                    node_id: hrw.node_id.as_bytes().to_vec(),
                    node_address,
                    disk_id,
                    shard_type,
                    local_group: hrw.local_group.unwrap_or(0) as u32,
                }
            })
            .collect();

        debug!(
            "CRUSH 2.0 placement for {}/{}: {} shards using {:?}",
            req.bucket,
            req.key,
            placements.len(),
            ec_type
        );

        Ok(Response::new(GetPlacementResponse {
            storage_class: req.storage_class.clone(),
            ec_k,
            ec_m: ec_local_parity + ec_global_parity,
            nodes: placements,
            ec_type: ec_type.into(),
            ec_local_parity,
            ec_global_parity,
            local_group_size,
            replication_count,
        }))
    }

    async fn create_multipart_upload(
        &self,
        request: Request<CreateMultipartUploadRequest>,
    ) -> Result<Response<CreateMultipartUploadResponse>, Status> {
        let req = request.into_inner();

        // Check if bucket exists
        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found("bucket not found"));
        }

        let upload_id = Uuid::new_v4().to_string();
        let now = Self::current_timestamp();

        // Store the multipart upload state
        let state = MultipartUploadState {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            upload_id: upload_id.clone(),
            content_type: req.content_type.clone(),
            user_metadata: req.user_metadata.clone(),
            initiated: now,
            parts: HashMap::new(),
            encryption_algorithm: req.encryption_algorithm,
            kms_key_id: req.kms_key_id.clone(),
            encrypted_dek: req.encrypted_dek.clone(),
            customer_key_md5: req.customer_key_md5.clone(),
            encryption_context: req.encryption_context.clone(),
        };
        self.multipart_uploads
            .write()
            .insert(upload_id.clone(), state.clone());

        if let Some(store) = &self.store {
            store.put_multipart_upload(&upload_id, &state);
        }

        info!(
            "Created multipart upload: bucket={}, key={}, upload_id={}, sse_algo={}",
            req.bucket, req.key, upload_id, req.encryption_algorithm
        );

        Ok(Response::new(CreateMultipartUploadResponse {
            upload_id,
            bucket: req.bucket,
            key: req.key,
            encryption_algorithm: req.encryption_algorithm,
            kms_key_id: req.kms_key_id,
        }))
    }

    async fn get_multipart_upload(
        &self,
        request: Request<GetMultipartUploadRequest>,
    ) -> Result<Response<GetMultipartUploadResponse>, Status> {
        let req = request.into_inner();
        let uploads = self.multipart_uploads.read();
        let Some(upload) = uploads.get(&req.upload_id) else {
            return Ok(Response::new(GetMultipartUploadResponse {
                found: false,
                ..Default::default()
            }));
        };
        if upload.bucket != req.bucket || upload.key != req.key {
            return Ok(Response::new(GetMultipartUploadResponse {
                found: false,
                ..Default::default()
            }));
        }
        Ok(Response::new(GetMultipartUploadResponse {
            found: true,
            content_type: upload.content_type.clone(),
            user_metadata: upload.user_metadata.clone(),
            initiated: upload.initiated,
            encryption_algorithm: upload.encryption_algorithm,
            kms_key_id: upload.kms_key_id.clone(),
            encrypted_dek: upload.encrypted_dek.clone(),
            customer_key_md5: upload.customer_key_md5.clone(),
            encryption_context: upload.encryption_context.clone(),
        }))
    }

    async fn register_part(
        &self,
        request: Request<RegisterPartRequest>,
    ) -> Result<Response<RegisterPartResponse>, Status> {
        let req = request.into_inner();

        // Validate part number (S3 allows 1-10,000)
        if req.part_number == 0 || req.part_number > 10000 {
            return Err(Status::invalid_argument(
                "part number must be between 1 and 10000",
            ));
        }

        let now = Self::current_timestamp();

        // Find and update the multipart upload
        let mut uploads = self.multipart_uploads.write();
        let upload = uploads.get_mut(&req.upload_id).ok_or_else(|| {
            Status::not_found(format!("multipart upload not found: {}", req.upload_id))
        })?;

        // Verify bucket/key match
        if upload.bucket != req.bucket || upload.key != req.key {
            return Err(Status::invalid_argument(
                "bucket/key mismatch for upload_id",
            ));
        }

        // Register the part (overwrites if same part_number uploaded again)
        let part_state = PartState {
            part_number: req.part_number,
            etag: req.etag.clone(),
            size: req.size,
            last_modified: now,
            stripes: req.stripes, // Multiple stripes for large parts
        };
        upload.parts.insert(req.part_number, part_state);

        // Persist entire upload state (includes new part)
        if let Some(store) = &self.store {
            store.put_multipart_upload(&req.upload_id, upload);
        }

        debug!(
            "Registered part {} for upload {}: size={}, etag={}",
            req.part_number, req.upload_id, req.size, req.etag
        );

        Ok(Response::new(RegisterPartResponse {
            success: true,
            etag: req.etag,
        }))
    }

    async fn list_parts(
        &self,
        request: Request<ListPartsRequest>,
    ) -> Result<Response<ListPartsResponse>, Status> {
        let req = request.into_inner();

        let uploads = self.multipart_uploads.read();
        let upload = uploads.get(&req.upload_id).ok_or_else(|| {
            Status::not_found(format!("multipart upload not found: {}", req.upload_id))
        })?;

        // Verify bucket/key match
        if upload.bucket != req.bucket || upload.key != req.key {
            return Err(Status::invalid_argument(
                "bucket/key mismatch for upload_id",
            ));
        }

        // Get parts sorted by part number, starting after marker
        let max_parts = if req.max_parts == 0 {
            1000
        } else {
            req.max_parts.min(1000)
        };
        let marker = req.part_number_marker;

        let mut parts: Vec<PartMeta> = upload
            .parts
            .values()
            .filter(|p| p.part_number > marker)
            .map(|p| PartMeta {
                part_number: p.part_number,
                etag: p.etag.clone(),
                size: p.size,
                last_modified: p.last_modified,
                stripes: p.stripes.clone(), // Multiple stripes for large parts
            })
            .collect();

        parts.sort_by_key(|p| p.part_number);

        let is_truncated = parts.len() > max_parts as usize;
        let parts: Vec<PartMeta> = parts.into_iter().take(max_parts as usize).collect();
        let next_marker = parts.last().map(|p| p.part_number).unwrap_or(0);

        Ok(Response::new(ListPartsResponse {
            parts,
            is_truncated,
            next_part_number_marker: next_marker,
            bucket: upload.bucket.clone(),
            key: upload.key.clone(),
            upload_id: upload.upload_id.clone(),
        }))
    }

    /// Complete multipart upload
    /// Validates parts and builds final object metadata with all stripes
    async fn complete_multipart_upload(
        &self,
        request: Request<CompleteMultipartUploadRequest>,
    ) -> Result<Response<CompleteMultipartUploadResponse>, Status> {
        let req = request.into_inner();

        // Get the upload state
        let upload = {
            let uploads = self.multipart_uploads.read();
            uploads.get(&req.upload_id).cloned().ok_or_else(|| {
                Status::not_found(format!("multipart upload not found: {}", req.upload_id))
            })?
        };

        // Verify bucket/key match
        if upload.bucket != req.bucket || upload.key != req.key {
            return Err(Status::invalid_argument(
                "bucket/key mismatch for upload_id",
            ));
        }

        // Validate that all requested parts exist and ETags match
        let mut stripes = Vec::new();
        let mut total_size = 0u64;

        for part_info in &req.parts {
            let stored_part = upload.parts.get(&part_info.part_number).ok_or_else(|| {
                Status::invalid_argument(format!("part {} not found", part_info.part_number))
            })?;

            // Verify ETag matches (normalize by removing quotes)
            let req_etag = part_info.etag.trim_matches('"');
            let stored_etag = stored_part.etag.trim_matches('"');
            if req_etag != stored_etag {
                return Err(Status::invalid_argument(format!(
                    "ETag mismatch for part {}: expected {}, got {}",
                    part_info.part_number, stored_etag, req_etag
                )));
            }

            total_size += stored_part.size;

            // Add all stripes for this part (large parts may have multiple stripes)
            stripes.extend(stored_part.stripes.clone());
        }

        // Calculate multipart ETag: MD5 of concatenated part MD5s + "-" + part count
        let final_etag = {
            let mut concatenated_hashes = Vec::new();
            for part_info in &req.parts {
                if let Some(stored_part) = upload.parts.get(&part_info.part_number) {
                    // Decode hex ETag and add to concatenated bytes
                    let etag_clean = stored_part.etag.trim_matches('"');
                    if let Ok(bytes) = hex::decode(etag_clean) {
                        concatenated_hashes.extend(bytes);
                    }
                }
            }
            let hash = md5::compute(&concatenated_hashes);
            format!("\"{:x}-{}\"", hash, req.parts.len())
        };

        let object_id = *Uuid::new_v4().as_bytes();
        let now = Self::current_timestamp();

        let object = ObjectMeta {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            object_id: object_id.to_vec(),
            size: total_size,
            etag: final_etag,
            content_type: upload.content_type.clone(),
            created_at: upload.initiated,
            modified_at: now,
            storage_class: "STANDARD".to_string(),
            user_metadata: upload.user_metadata.clone(),
            version_id: String::new(),
            is_delete_marker: false,
            stripes,
            retention: None,
            legal_hold: None,
            // Multipart SSE: each stripe carries its own IV; the object-level
            // fields just record the algorithm + wrapped DEK so GET knows
            // how to unwrap and which algorithm to advertise on responses.
            encryption_algorithm: upload.encryption_algorithm,
            kms_key_id: upload.kms_key_id.clone(),
            encrypted_dek: upload.encrypted_dek.clone(),
            encryption_iv: Vec::new(),
            ..Default::default()
        };

        // Remove the completed upload from state
        self.multipart_uploads.write().remove(&req.upload_id);

        if let Some(store) = &self.store {
            store.delete_multipart_upload(&req.upload_id);
        }

        info!(
            "Completed multipart upload: bucket={}, key={}, upload_id={}, size={}, parts={}",
            req.bucket,
            req.key,
            req.upload_id,
            total_size,
            req.parts.len()
        );

        Ok(Response::new(CompleteMultipartUploadResponse {
            object: Some(object),
        }))
    }

    async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartUploadRequest>,
    ) -> Result<Response<AbortMultipartUploadResponse>, Status> {
        let req = request.into_inner();

        // Remove the upload from state
        let removed = self.multipart_uploads.write().remove(&req.upload_id);

        if removed.is_some() {
            if let Some(store) = &self.store {
                store.delete_multipart_upload(&req.upload_id);
            }
            info!(
                "Aborted multipart upload: bucket={}, key={}, upload_id={}",
                req.bucket, req.key, req.upload_id
            );
        } else {
            debug!(
                "Abort for unknown upload_id={} (may already be completed)",
                req.upload_id
            );
        }

        // Note: Part data on OSDs should be cleaned up by background garbage collection
        // using the __mpu/{upload_id}/* prefix

        Ok(Response::new(AbortMultipartUploadResponse {
            success: true,
        }))
    }

    async fn list_multipart_uploads(
        &self,
        request: Request<ListMultipartUploadsRequest>,
    ) -> Result<Response<ListMultipartUploadsResponse>, Status> {
        let req = request.into_inner();

        // Check if bucket exists
        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found("bucket not found"));
        }

        let max_uploads = if req.max_uploads == 0 {
            1000
        } else {
            req.max_uploads.min(1000)
        };

        // Filter and collect uploads for this bucket
        let uploads_lock = self.multipart_uploads.read();
        let mut uploads: Vec<MultipartUpload> = uploads_lock
            .values()
            .filter(|u| u.bucket == req.bucket)
            .filter(|u| req.prefix.is_empty() || u.key.starts_with(&req.prefix))
            .filter(|u| {
                if req.key_marker.is_empty() || u.key > req.key_marker {
                    true
                } else if u.key == req.key_marker && !req.upload_id_marker.is_empty() {
                    u.upload_id > req.upload_id_marker
                } else {
                    false
                }
            })
            .map(|u| MultipartUpload {
                key: u.key.clone(),
                upload_id: u.upload_id.clone(),
                initiated: u.initiated,
                storage_class: "STANDARD".to_string(),
            })
            .collect();

        // Sort by key, then upload_id
        uploads.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| a.upload_id.cmp(&b.upload_id))
        });

        let is_truncated = uploads.len() > max_uploads as usize;
        let uploads: Vec<MultipartUpload> =
            uploads.into_iter().take(max_uploads as usize).collect();

        let (next_key_marker, next_upload_id_marker) = uploads
            .last()
            .map(|u| (u.key.clone(), u.upload_id.clone()))
            .unwrap_or_default();

        Ok(Response::new(ListMultipartUploadsResponse {
            uploads,
            next_key_marker,
            next_upload_id_marker,
            is_truncated,
        }))
    }

    async fn set_bucket_policy(
        &self,
        request: Request<SetBucketPolicyRequest>,
    ) -> Result<Response<SetBucketPolicyResponse>, Status> {
        let req = request.into_inner();

        // Check if bucket exists
        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found("bucket not found"));
        }

        // Validate that the policy is valid JSON
        if serde_json::from_str::<serde_json::Value>(&req.policy_json).is_err() {
            return Err(Status::invalid_argument("invalid policy JSON"));
        }

        self.bucket_policies
            .write()
            .insert(req.bucket.clone(), req.policy_json.clone());

        if let Some(store) = &self.store {
            store.put_bucket_policy(&req.bucket, &req.policy_json);
        }

        info!("Set bucket policy for: {}", req.bucket);

        Ok(Response::new(SetBucketPolicyResponse { success: true }))
    }

    async fn get_bucket_policy(
        &self,
        request: Request<GetBucketPolicyRequest>,
    ) -> Result<Response<GetBucketPolicyResponse>, Status> {
        let req = request.into_inner();

        // Check if bucket exists
        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found("bucket not found"));
        }

        let policies = self.bucket_policies.read();
        let (policy_json, has_policy) = match policies.get(&req.bucket) {
            Some(policy) => (policy.clone(), true),
            None => (String::new(), false),
        };

        Ok(Response::new(GetBucketPolicyResponse {
            policy_json,
            has_policy,
        }))
    }

    async fn delete_bucket_policy(
        &self,
        request: Request<DeleteBucketPolicyRequest>,
    ) -> Result<Response<DeleteBucketPolicyResponse>, Status> {
        let req = request.into_inner();

        // Check if bucket exists
        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found("bucket not found"));
        }

        self.bucket_policies.write().remove(&req.bucket);

        if let Some(store) = &self.store {
            store.delete_bucket_policy(&req.bucket);
        }

        info!("Deleted bucket policy for: {}", req.bucket);

        Ok(Response::new(DeleteBucketPolicyResponse { success: true }))
    }

    async fn register_osd(
        &self,
        request: Request<RegisterOsdRequest>,
    ) -> Result<Response<RegisterOsdResponse>, Status> {
        let req = request.into_inner();

        // Validate node_id is 16 bytes
        if req.node_id.len() != 16 {
            return Err(Status::invalid_argument("node_id must be 16 bytes"));
        }

        let mut node_id = [0u8; 16];
        node_id.copy_from_slice(&req.node_id);

        // Parse disk IDs
        let mut disk_ids = Vec::new();
        for disk_id in &req.disk_ids {
            if disk_id.len() != 16 {
                return Err(Status::invalid_argument("disk_id must be 16 bytes"));
            }
            let mut id = [0u8; 16];
            id.copy_from_slice(disk_id);
            disk_ids.push(id);
        }

        // Capacity hint must be index-aligned with disk_ids. Tolerate the
        // older single-field protocol (empty capacities) by treating the
        // caller as reporting 0 bytes — under-reports, never blocks
        // re-registration of already-known hardware.
        let disk_capacity_bytes: Vec<u64> = if req.disk_capacity_bytes.is_empty() {
            vec![0; disk_ids.len()]
        } else if req.disk_capacity_bytes.len() == disk_ids.len() {
            req.disk_capacity_bytes.clone()
        } else {
            return Err(Status::invalid_argument(
                "disk_capacity_bytes length must match disk_ids",
            ));
        };

        // License caps. Re-registrations of an already-known node_id never
        // add to capacity accounting, so we only enforce for a truly new OSD.
        let existing_node = self
            .osd_nodes
            .read()
            .iter()
            .find(|n| n.node_id == node_id)
            .cloned();
        if existing_node.is_none() {
            let license = self.license.read().clone();
            if license.max_nodes != 0 || license.max_raw_capacity_bytes != 0 {
                let snapshot = self.osd_nodes.read();
                let current_nodes = snapshot.len() as u64;
                let current_capacity: u64 = snapshot
                    .iter()
                    .flat_map(|n| n.disk_capacity_bytes.iter().copied())
                    .sum();
                let new_capacity: u64 = disk_capacity_bytes.iter().copied().sum();
                drop(snapshot);

                if license.max_nodes != 0 && current_nodes >= license.max_nodes {
                    warn!(
                        "register_osd refused: node cap {} reached (current={})",
                        license.max_nodes, current_nodes
                    );
                    return Err(Status::resource_exhausted(format!(
                        "license node cap reached: {} / {} OSDs already registered. Install a license with a higher max_nodes.",
                        current_nodes, license.max_nodes
                    )));
                }
                if license.max_raw_capacity_bytes != 0
                    && current_capacity.saturating_add(new_capacity) > license.max_raw_capacity_bytes
                {
                    warn!(
                        "register_osd refused: capacity cap {} B exceeded (current={} + new={})",
                        license.max_raw_capacity_bytes, current_capacity, new_capacity
                    );
                    return Err(Status::resource_exhausted(format!(
                        "license raw-capacity cap would be exceeded: current {} B + new OSD {} B > cap {} B. Install a license with a higher max_raw_capacity_bytes.",
                        current_capacity, new_capacity, license.max_raw_capacity_bytes
                    )));
                }
            }
        }

        // Register the OSD. Pull failure-domain fields from the request
        // and persist both the legacy 3-tuple (back-compat) and the full
        // 5-level `topology`, so newer meta readers see zone/host and
        // older code paths still find region/dc/rack.
        let topology_tuple = req.failure_domain.as_ref().map(|fd| {
            (
                fd.region.clone(),
                fd.zone.clone(),
                fd.datacenter.clone(),
                fd.rack.clone(),
                fd.host.clone(),
            )
        });
        let legacy_fd = req
            .failure_domain
            .as_ref()
            .map(|fd| (fd.region.clone(), fd.datacenter.clone(), fd.rack.clone()));
        let num_disks = disk_ids.len();
        let node = OsdNode {
            node_id,
            address: req.address.clone(),
            disk_ids,
            failure_domain: legacy_fd,
            topology: topology_tuple,
            disk_capacity_bytes,
        };

        // Check if node already exists and update, or add new. We dedupe
        // on node_id AND on address — an OSD that loses its persistent
        // state gets a new node_id on restart, but still advertises the
        // same hostname. Treat "same address, different node_id" as a
        // replacement so the topology doesn't accumulate ghosts.
        let mut nodes = self.osd_nodes.write();
        let mut evicted_ids: Vec<[u8; 16]> = Vec::new();
        if let Some(existing) = nodes.iter_mut().find(|n| n.node_id == node_id) {
            existing.address = node.address.clone();
            existing.disk_ids = node.disk_ids.clone();
            existing.disk_capacity_bytes = node.disk_capacity_bytes.clone();
            existing.failure_domain = node.failure_domain.clone();
            existing.topology = node.topology.clone();
            info!(
                "Updated OSD registration: {} at {}",
                hex::encode(node_id),
                req.address
            );
        } else {
            // Evict any existing entry at the same address (stale node_id
            // from a prior OSD process) so the tree shows live nodes only.
            nodes.retain(|n| {
                if n.address == req.address && n.node_id != node_id {
                    evicted_ids.push(n.node_id);
                    false
                } else {
                    true
                }
            });
            if !evicted_ids.is_empty() {
                info!(
                    "Evicted {} stale OSD entry/entries at address {} (node_id changed)",
                    evicted_ids.len(),
                    req.address
                );
            }
            info!(
                "Registered new OSD: {} at {} with {} disks",
                hex::encode(node_id),
                req.address,
                num_disks
            );
            nodes.push(node.clone());
        }
        drop(nodes);

        // Also drop the stale node_ids from the CRUSH topology so listings
        // and placement see a clean view.
        if !evicted_ids.is_empty() {
            let mut topology = self.topology.write();
            for id in &evicted_ids {
                let stale = NodeId::from_bytes(*id);
                topology.remove_node(stale);
            }
        }

        // Persist OSD + topology
        if let Some(store) = &self.store {
            let topology = self.topology.read().clone();
            store.put_osd_and_topology(&hex::encode(node_id), &node, &topology);
        }

        // Get current topology version
        let topology_version = self.topology.read().version;

        Ok(Response::new(RegisterOsdResponse {
            success: true,
            topology_version,
        }))
    }

    /// Get all active nodes for scatter-gather listing operations
    async fn get_listing_nodes(
        &self,
        _request: Request<GetListingNodesRequest>,
    ) -> Result<Response<GetListingNodesResponse>, Status> {
        let topology = self.topology.read();
        let osd_nodes = self.osd_nodes.read();

        // Collect all active nodes from topology, carrying their failure
        // domain so gateways can render a topology tree and do locality
        // routing (Phase 2).
        let mut nodes: Vec<ListingNode> = topology
            .active_nodes()
            .enumerate()
            .map(|(idx, node)| ListingNode {
                node_id: node.id.as_bytes().to_vec(),
                address: format!("http://{}", node.address),
                shard_id: idx as u32, // Assign logical shard IDs in order
                failure_domain: Some(objectio_proto::metadata::FailureDomainInfo {
                    region: node.failure_domain.region.clone(),
                    datacenter: node.failure_domain.datacenter.clone(),
                    rack: node.failure_domain.rack.clone(),
                    zone: node.failure_domain.zone.clone(),
                    host: node.failure_domain.host.clone(),
                }),
            })
            .collect();

        // Also include legacy OSD nodes if no topology nodes exist
        if nodes.is_empty() {
            nodes = osd_nodes
                .iter()
                .enumerate()
                .map(|(idx, node)| {
                    let fd = node.topology.as_ref().map(|t| {
                        objectio_proto::metadata::FailureDomainInfo {
                            region: t.0.clone(),
                            zone: t.1.clone(),
                            datacenter: t.2.clone(),
                            rack: t.3.clone(),
                            host: t.4.clone(),
                        }
                    });
                    ListingNode {
                        node_id: node.node_id.to_vec(),
                        address: node.address.clone(),
                        shard_id: idx as u32,
                        failure_domain: fd,
                    }
                })
                .collect();
        }

        debug!(
            "GetListingNodes: returning {} nodes (topology_version={})",
            nodes.len(),
            topology.version
        );

        Ok(Response::new(GetListingNodesResponse {
            nodes,
            topology_version: topology.version,
        }))
    }

    // =========== IAM Operations ===========

    async fn create_user(
        &self,
        request: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        let req = request.into_inner();

        if req.display_name.is_empty() {
            return Err(Status::invalid_argument("display_name is required"));
        }

        // Check if user with same name exists
        if self
            .users
            .read()
            .values()
            .any(|u| u.display_name == req.display_name)
        {
            return Err(Status::already_exists("user with this name already exists"));
        }

        let user_id = Uuid::new_v4().to_string();
        let now = Self::current_timestamp();

        // Validate tenant if specified
        let tenant = req.tenant.clone();
        if !tenant.is_empty() && !self.tenants.read().contains_key(&tenant) {
            return Err(Status::not_found(format!("tenant '{}' not found", tenant)));
        }

        // Include tenant in ARN if tenant-scoped
        let arn = if tenant.is_empty() {
            format!("arn:objectio:iam::user/{}", req.display_name)
        } else {
            format!("arn:objectio:iam::{}:user/{}", tenant, req.display_name)
        };

        let user = StoredUser {
            user_id: user_id.clone(),
            display_name: req.display_name.clone(),
            arn,
            status: UserStatus::UserActive as i32,
            created_at: now,
            email: req.email.clone(),
            tenant: tenant.clone(),
        };

        self.users.write().insert(user_id.clone(), user.clone());
        self.user_keys.write().insert(user_id.clone(), Vec::new());

        if let Some(store) = &self.store {
            store.put_user(&user_id, &user);
        }

        info!(
            "Created user: {} (tenant={})",
            req.display_name,
            if tenant.is_empty() { "system" } else { &tenant }
        );

        Ok(Response::new(CreateUserResponse {
            user: Some(UserMeta {
                user_id: user.user_id,
                display_name: user.display_name,
                arn: user.arn,
                status: user.status,
                created_at: user.created_at,
                email: user.email,
                tenant,
            }),
        }))
    }

    async fn get_user(
        &self,
        request: Request<GetUserRequest>,
    ) -> Result<Response<GetUserResponse>, Status> {
        let req = request.into_inner();

        let user = self
            .users
            .read()
            .get(&req.user_id)
            .cloned()
            .ok_or_else(|| Status::not_found("user not found"))?;

        Ok(Response::new(GetUserResponse {
            user: Some(UserMeta {
                user_id: user.user_id.clone(),
                display_name: user.display_name.clone(),
                arn: user.arn.clone(),
                status: user.status,
                created_at: user.created_at,
                email: user.email.clone(),
                tenant: user.tenant.clone(),
            }),
        }))
    }

    async fn list_users(
        &self,
        request: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        let req = request.into_inner();
        let max_results = if req.max_results == 0 {
            100
        } else {
            req.max_results.min(1000)
        };

        let users: Vec<UserMeta> = self
            .users
            .read()
            .values()
            .filter(|u| req.marker.is_empty() || u.user_id > req.marker)
            .filter(|u| u.status != UserStatus::UserDeleted as i32)
            .take(max_results as usize + 1)
            .map(|u| UserMeta {
                user_id: u.user_id.clone(),
                display_name: u.display_name.clone(),
                arn: u.arn.clone(),
                status: u.status,
                created_at: u.created_at,
                email: u.email.clone(),
                tenant: u.tenant.clone(),
            })
            .collect();

        let is_truncated = users.len() > max_results as usize;
        let users: Vec<UserMeta> = users.into_iter().take(max_results as usize).collect();
        let next_marker = users.last().map(|u| u.user_id.clone()).unwrap_or_default();

        Ok(Response::new(ListUsersResponse {
            users,
            next_marker: if is_truncated {
                next_marker
            } else {
                String::new()
            },
            is_truncated,
        }))
    }

    async fn delete_user(
        &self,
        request: Request<DeleteUserRequest>,
    ) -> Result<Response<DeleteUserResponse>, Status> {
        let req = request.into_inner();

        // Mark user as deleted (don't remove for audit trail)
        let mut users = self.users.write();
        let user = users
            .get_mut(&req.user_id)
            .ok_or_else(|| Status::not_found("user not found"))?;
        user.status = UserStatus::UserDeleted as i32;
        let user_snapshot = user.clone();

        // Deactivate all user's keys
        let key_ids: Vec<String> = self
            .user_keys
            .read()
            .get(&req.user_id)
            .cloned()
            .unwrap_or_default();

        let mut keys = self.access_keys.write();
        for key_id in &key_ids {
            if let Some(key) = keys.get_mut(key_id) {
                key.status = KeyStatus::KeyInactive as i32;
            }
        }

        // Persist updated user and deactivated keys
        if let Some(store) = &self.store {
            store.put_user(&req.user_id, &user_snapshot);
            for key_id in &key_ids {
                if let Some(key) = keys.get(key_id) {
                    store.put_access_key(key_id, key);
                }
            }
        }

        info!("Deleted user: {}", req.user_id);

        Ok(Response::new(DeleteUserResponse { success: true }))
    }

    async fn create_access_key(
        &self,
        request: Request<CreateAccessKeyRequest>,
    ) -> Result<Response<CreateAccessKeyResponse>, Status> {
        let req = request.into_inner();

        // Verify user exists and is active
        let user = self
            .users
            .read()
            .get(&req.user_id)
            .cloned()
            .ok_or_else(|| Status::not_found("user not found"))?;

        if user.status != UserStatus::UserActive as i32 {
            return Err(Status::failed_precondition("user is not active"));
        }

        let now = Self::current_timestamp();
        let access_key_id = Self::generate_access_key_id();
        let secret_access_key = Self::generate_secret_access_key();

        let key = StoredAccessKey {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            user_id: req.user_id.clone(),
            status: KeyStatus::KeyActive as i32,
            created_at: now,
            tenant: user.tenant.clone(),
        };

        self.access_keys
            .write()
            .insert(access_key_id.clone(), key.clone());
        self.user_keys
            .write()
            .entry(req.user_id.clone())
            .or_default()
            .push(access_key_id.clone());

        if let Some(store) = &self.store {
            store.put_access_key(&access_key_id, &key);
        }

        info!(
            "Created access key {} for user {}",
            access_key_id, req.user_id
        );

        Ok(Response::new(CreateAccessKeyResponse {
            access_key: Some(AccessKeyMeta {
                access_key_id: key.access_key_id,
                secret_access_key: key.secret_access_key, // Only returned on creation
                user_id: key.user_id,
                status: key.status,
                created_at: key.created_at,
                tenant: key.tenant,
            }),
        }))
    }

    async fn list_access_keys(
        &self,
        request: Request<ListAccessKeysRequest>,
    ) -> Result<Response<ListAccessKeysResponse>, Status> {
        let req = request.into_inner();

        let key_ids = self
            .user_keys
            .read()
            .get(&req.user_id)
            .cloned()
            .unwrap_or_default();

        let keys = self.access_keys.read();
        let access_keys: Vec<AccessKeyMeta> = key_ids
            .iter()
            .filter_map(|id| keys.get(id))
            .map(|k| AccessKeyMeta {
                access_key_id: k.access_key_id.clone(),
                secret_access_key: String::new(), // Don't return secret in list
                user_id: k.user_id.clone(),
                status: k.status,
                created_at: k.created_at,
                tenant: k.tenant.clone(),
            })
            .collect();

        Ok(Response::new(ListAccessKeysResponse { access_keys }))
    }

    async fn delete_access_key(
        &self,
        request: Request<DeleteAccessKeyRequest>,
    ) -> Result<Response<DeleteAccessKeyResponse>, Status> {
        let req = request.into_inner();

        let key = self.access_keys.write().remove(&req.access_key_id);
        if let Some(key) = key {
            // Remove from user_keys
            if let Some(keys) = self.user_keys.write().get_mut(&key.user_id) {
                keys.retain(|id| id != &req.access_key_id);
            }

            if let Some(store) = &self.store {
                store.delete_access_key(&req.access_key_id);
            }

            info!("Deleted access key: {}", req.access_key_id);
            Ok(Response::new(DeleteAccessKeyResponse { success: true }))
        } else {
            Err(Status::not_found("access key not found"))
        }
    }

    async fn get_access_key_for_auth(
        &self,
        request: Request<GetAccessKeyForAuthRequest>,
    ) -> Result<Response<GetAccessKeyForAuthResponse>, Status> {
        let req = request.into_inner();

        let key = self
            .access_keys
            .read()
            .get(&req.access_key_id)
            .cloned()
            .ok_or_else(|| Status::not_found("access key not found"))?;

        if key.status != KeyStatus::KeyActive as i32 {
            return Err(Status::permission_denied("access key is inactive"));
        }

        let user = self
            .users
            .read()
            .get(&key.user_id)
            .cloned()
            .ok_or_else(|| Status::not_found("user not found"))?;

        if user.status != UserStatus::UserActive as i32 {
            return Err(Status::permission_denied("user is not active"));
        }

        Ok(Response::new(GetAccessKeyForAuthResponse {
            access_key: Some(AccessKeyMeta {
                access_key_id: key.access_key_id,
                secret_access_key: key.secret_access_key, // Include for auth verification
                user_id: key.user_id,
                status: key.status,
                created_at: key.created_at,
                tenant: key.tenant,
            }),
            user: Some(UserMeta {
                user_id: user.user_id,
                display_name: user.display_name,
                arn: user.arn,
                status: user.status,
                created_at: user.created_at,
                email: user.email,
                tenant: user.tenant,
            }),
        }))
    }

    // =========== Iceberg Catalog Operations ===========

    async fn iceberg_create_namespace(
        &self,
        request: Request<IcebergCreateNamespaceRequest>,
    ) -> Result<Response<IcebergCreateNamespaceResponse>, Status> {
        let req = request.into_inner();

        if req.namespace_levels.is_empty() {
            return Err(Status::invalid_argument("namespace levels cannot be empty"));
        }

        let ns_key = Self::iceberg_ns_key_wh(&req.warehouse, &req.namespace_levels);

        if self.iceberg_namespaces.read().contains_key(&ns_key) {
            return Err(Status::already_exists("namespace already exists"));
        }

        // If multi-level, verify parent exists
        if req.namespace_levels.len() > 1 {
            let parent_key = Self::iceberg_ns_key_wh(
                &req.warehouse,
                &req.namespace_levels[..req.namespace_levels.len() - 1],
            );
            if !self.iceberg_namespaces.read().contains_key(&parent_key) {
                return Err(Status::not_found("parent namespace does not exist"));
            }
        }

        let properties = req.properties.clone();
        self.iceberg_namespaces
            .write()
            .insert(ns_key.clone(), properties.clone());

        if let Some(store) = &self.store {
            let resp = IcebergCreateNamespaceResponse {
                namespace_levels: req.namespace_levels.clone(),
                properties: properties.clone(),
            };
            store.put_iceberg_namespace(&ns_key, &resp.encode_to_vec());
        }

        info!("Created iceberg namespace: {:?}", req.namespace_levels);

        Ok(Response::new(IcebergCreateNamespaceResponse {
            namespace_levels: req.namespace_levels,
            properties,
        }))
    }

    async fn iceberg_load_namespace(
        &self,
        request: Request<IcebergLoadNamespaceRequest>,
    ) -> Result<Response<IcebergLoadNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key_wh(&req.warehouse, &req.namespace_levels);

        let properties = self
            .iceberg_namespaces
            .read()
            .get(&ns_key)
            .cloned()
            .ok_or_else(|| Status::not_found("namespace not found"))?;

        Ok(Response::new(IcebergLoadNamespaceResponse {
            namespace_levels: req.namespace_levels,
            properties,
        }))
    }

    async fn iceberg_drop_namespace(
        &self,
        request: Request<IcebergDropNamespaceRequest>,
    ) -> Result<Response<IcebergDropNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key_wh(&req.warehouse, &req.namespace_levels);

        // Check namespace exists
        if !self.iceberg_namespaces.read().contains_key(&ns_key) {
            return Err(Status::not_found("namespace not found"));
        }

        // Check for tables in namespace
        let table_prefix = format!("{ns_key}\x00");
        let has_tables = self
            .iceberg_tables
            .read()
            .keys()
            .any(|k| k.starts_with(&table_prefix));
        if has_tables {
            return Err(Status::failed_precondition(
                "namespace is not empty (contains tables)",
            ));
        }

        // Check for child namespaces
        let child_prefix = format!("{ns_key}\x00");
        let has_children = self
            .iceberg_namespaces
            .read()
            .keys()
            .any(|k| k.starts_with(&child_prefix));
        if has_children {
            return Err(Status::failed_precondition(
                "namespace is not empty (contains child namespaces)",
            ));
        }

        self.iceberg_namespaces.write().remove(&ns_key);

        if let Some(store) = &self.store {
            store.delete_iceberg_namespace(&ns_key);
        }

        info!("Dropped iceberg namespace: {:?}", req.namespace_levels);

        Ok(Response::new(IcebergDropNamespaceResponse {
            success: true,
        }))
    }

    async fn iceberg_list_namespaces(
        &self,
        request: Request<IcebergListNamespacesRequest>,
    ) -> Result<Response<IcebergListNamespacesResponse>, Status> {
        let req = request.into_inner();

        let wh_prefix = Self::iceberg_warehouse_prefix(&req.warehouse);

        let parent_key = if req.parent_levels.is_empty() {
            String::new()
        } else {
            Self::iceberg_ns_key_wh(&req.warehouse, &req.parent_levels)
        };

        // If parent specified, verify it exists
        if !parent_key.is_empty() && !self.iceberg_namespaces.read().contains_key(&parent_key) {
            return Err(Status::not_found("parent namespace not found"));
        }

        let prefix = if parent_key.is_empty() {
            wh_prefix.clone()
        } else {
            format!("{parent_key}\x00")
        };

        let page_size = if req.page_size == 0 {
            100
        } else {
            req.page_size.min(1000)
        } as usize;

        let mut keys: Vec<String> = self
            .iceberg_namespaces
            .read()
            .keys()
            .filter(|k| {
                if prefix.is_empty() {
                    // No warehouse, no parent: top-level namespaces without warehouse prefix
                    !k.contains('\x00') && !k.contains('\x01')
                } else if parent_key.is_empty() && !wh_prefix.is_empty() {
                    // Warehouse set but no parent: top-level namespaces in this warehouse
                    k.starts_with(&prefix) && !k[prefix.len()..].contains('\x00')
                } else {
                    k.starts_with(&prefix) && !k[prefix.len()..].contains('\x00')
                }
            })
            .cloned()
            .collect();
        keys.sort();

        // Skip past page_token
        if !req.page_token.is_empty() {
            keys.retain(|k| k.as_str() > req.page_token.as_str());
        }

        let has_more = keys.len() > page_size;
        let keys: Vec<String> = keys.into_iter().take(page_size).collect();

        let next_page_token = if has_more {
            keys.last().cloned().unwrap_or_default()
        } else {
            String::new()
        };

        let namespaces: Vec<IcebergNamespace> = keys
            .iter()
            .map(|k| {
                // Strip warehouse prefix (warehouse\x01) if present
                let ns_part = if let Some(pos) = k.find('\x01') {
                    &k[pos + 1..]
                } else {
                    k.as_str()
                };
                let levels: Vec<String> = ns_part.split('\x00').map(String::from).collect();
                IcebergNamespace { levels }
            })
            .collect();

        Ok(Response::new(IcebergListNamespacesResponse {
            namespaces,
            next_page_token,
        }))
    }

    async fn iceberg_update_namespace_properties(
        &self,
        request: Request<IcebergUpdateNamespacePropertiesRequest>,
    ) -> Result<Response<IcebergUpdateNamespacePropertiesResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key_wh(&req.warehouse, &req.namespace_levels);

        let mut ns_map = self.iceberg_namespaces.write();
        let properties = ns_map
            .get_mut(&ns_key)
            .ok_or_else(|| Status::not_found("namespace not found"))?;

        let mut updated = Vec::new();
        let mut removed = Vec::new();
        let mut missing = Vec::new();

        // Apply removals
        for key in &req.removals {
            if properties.remove(key).is_some() {
                removed.push(key.clone());
            } else {
                missing.push(key.clone());
            }
        }

        // Apply updates
        for (key, value) in &req.updates {
            properties.insert(key.clone(), value.clone());
            updated.push(key.clone());
        }

        // Persist
        if let Some(store) = &self.store {
            let resp = IcebergCreateNamespaceResponse {
                namespace_levels: req.namespace_levels.clone(),
                properties: properties.clone(),
            };
            store.put_iceberg_namespace(&ns_key, &resp.encode_to_vec());
        }

        Ok(Response::new(IcebergUpdateNamespacePropertiesResponse {
            updated,
            removed,
            missing,
        }))
    }

    async fn iceberg_namespace_exists(
        &self,
        request: Request<IcebergNamespaceExistsRequest>,
    ) -> Result<Response<IcebergNamespaceExistsResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key_wh(&req.warehouse, &req.namespace_levels);
        let exists = self.iceberg_namespaces.read().contains_key(&ns_key);
        Ok(Response::new(IcebergNamespaceExistsResponse { exists }))
    }

    async fn iceberg_create_table(
        &self,
        request: Request<IcebergCreateTableRequest>,
    ) -> Result<Response<IcebergCreateTableResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key_wh(&req.warehouse, &req.namespace_levels);

        // Verify namespace exists
        if !self.iceberg_namespaces.read().contains_key(&ns_key) {
            return Err(Status::not_found("namespace not found"));
        }

        let table_key =
            Self::iceberg_table_key_wh(&req.warehouse, &req.namespace_levels, &req.table_name);

        if self.iceberg_tables.read().contains_key(&table_key) {
            return Err(Status::already_exists("table already exists"));
        }

        let now = Self::current_timestamp();
        let entry = IcebergTableEntry {
            metadata_location: req.metadata_location.clone(),
            created_at: now,
            updated_at: now,
            metadata_json: req.metadata_json.clone(),
            policy_json: Vec::new(),
        };

        self.iceberg_tables
            .write()
            .insert(table_key.clone(), entry.clone());

        if let Some(store) = &self.store {
            store.put_iceberg_table(&table_key, &entry.encode_to_vec());
        }

        info!(
            "Created iceberg table: {:?}.{}",
            req.namespace_levels, req.table_name
        );

        Ok(Response::new(IcebergCreateTableResponse {
            metadata_location: req.metadata_location,
            metadata_json: req.metadata_json,
        }))
    }

    async fn iceberg_load_table(
        &self,
        request: Request<IcebergLoadTableRequest>,
    ) -> Result<Response<IcebergLoadTableResponse>, Status> {
        let req = request.into_inner();
        let table_key =
            Self::iceberg_table_key_wh(&req.warehouse, &req.namespace_levels, &req.table_name);

        let entry = self
            .iceberg_tables
            .read()
            .get(&table_key)
            .cloned()
            .ok_or_else(|| Status::not_found("table not found"))?;

        Ok(Response::new(IcebergLoadTableResponse {
            metadata_location: entry.metadata_location,
            metadata_json: entry.metadata_json,
        }))
    }

    async fn iceberg_commit_table(
        &self,
        request: Request<IcebergCommitTableRequest>,
    ) -> Result<Response<IcebergCommitTableResponse>, Status> {
        let req = request.into_inner();
        let table_key =
            Self::iceberg_table_key_wh(&req.warehouse, &req.namespace_levels, &req.table_name);

        let mut tables = self.iceberg_tables.write();
        let entry = tables
            .get(&table_key)
            .ok_or_else(|| Status::not_found("table not found"))?;

        // CAS: verify current metadata location matches expected
        if entry.metadata_location != req.current_metadata_location {
            return Err(Status::failed_precondition(format!(
                "metadata location mismatch: expected '{}', actual '{}'",
                req.current_metadata_location, entry.metadata_location
            )));
        }

        let now = Self::current_timestamp();
        let new_entry = IcebergTableEntry {
            metadata_location: req.new_metadata_location.clone(),
            created_at: entry.created_at,
            updated_at: now,
            metadata_json: req.new_metadata_json.clone(),
            policy_json: entry.policy_json.clone(),
        };

        // Persist with CAS in store
        if let Some(store) = &self.store {
            let old_bytes = entry.encode_to_vec();
            let new_bytes = new_entry.encode_to_vec();
            match store.cas_iceberg_table(&table_key, &old_bytes, &new_bytes) {
                Ok(true) => {}
                Ok(false) => {
                    return Err(Status::failed_precondition(
                        "concurrent metadata update detected",
                    ));
                }
                Err(e) => {
                    error!("Failed to CAS iceberg table '{}': {}", table_key, e);
                    return Err(Status::internal("failed to commit table update"));
                }
            }
        }

        tables.insert(table_key, new_entry);

        debug!(
            "Committed iceberg table {:?}.{}: {} -> {}",
            req.namespace_levels,
            req.table_name,
            req.current_metadata_location,
            req.new_metadata_location
        );

        Ok(Response::new(IcebergCommitTableResponse {
            metadata_location: req.new_metadata_location,
            metadata_json: req.new_metadata_json,
        }))
    }

    async fn iceberg_drop_table(
        &self,
        request: Request<IcebergDropTableRequest>,
    ) -> Result<Response<IcebergDropTableResponse>, Status> {
        let req = request.into_inner();
        let table_key =
            Self::iceberg_table_key_wh(&req.warehouse, &req.namespace_levels, &req.table_name);

        let removed = self.iceberg_tables.write().remove(&table_key);
        if removed.is_none() {
            return Err(Status::not_found("table not found"));
        }

        if let Some(store) = &self.store {
            store.delete_iceberg_table(&table_key);
        }

        info!(
            "Dropped iceberg table: {:?}.{} (purge={})",
            req.namespace_levels, req.table_name, req.purge
        );

        Ok(Response::new(IcebergDropTableResponse { success: true }))
    }

    async fn iceberg_rename_table(
        &self,
        request: Request<IcebergRenameTableRequest>,
    ) -> Result<Response<IcebergRenameTableResponse>, Status> {
        let req = request.into_inner();
        let source = req
            .source
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("source is required"))?;
        let dest = req
            .destination
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("destination is required"))?;

        let src_key = Self::iceberg_table_key(&source.namespace_levels, &source.name);
        let dst_key = Self::iceberg_table_key(&dest.namespace_levels, &dest.name);

        // Verify destination namespace exists
        let dst_ns_key = Self::iceberg_ns_key(&dest.namespace_levels);
        if !self.iceberg_namespaces.read().contains_key(&dst_ns_key) {
            return Err(Status::not_found("destination namespace not found"));
        }

        let mut tables = self.iceberg_tables.write();

        // Check source exists
        let entry = tables
            .remove(&src_key)
            .ok_or_else(|| Status::not_found("source table not found"))?;

        // Check destination doesn't exist
        if tables.contains_key(&dst_key) {
            // Put source back
            tables.insert(src_key, entry);
            return Err(Status::already_exists("destination table already exists"));
        }

        tables.insert(dst_key.clone(), entry.clone());

        if let Some(store) = &self.store {
            store.delete_iceberg_table(&src_key);
            store.put_iceberg_table(&dst_key, &entry.encode_to_vec());
        }

        info!(
            "Renamed iceberg table: {:?}.{} -> {:?}.{}",
            source.namespace_levels, source.name, dest.namespace_levels, dest.name
        );

        Ok(Response::new(IcebergRenameTableResponse { success: true }))
    }

    async fn iceberg_list_tables(
        &self,
        request: Request<IcebergListTablesRequest>,
    ) -> Result<Response<IcebergListTablesResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key_wh(&req.warehouse, &req.namespace_levels);

        // Verify namespace exists
        if !self.iceberg_namespaces.read().contains_key(&ns_key) {
            return Err(Status::not_found("namespace not found"));
        }

        let prefix = format!("{ns_key}\x00");

        let page_size = if req.page_size == 0 {
            100
        } else {
            req.page_size.min(1000)
        } as usize;

        let mut table_names: Vec<String> = self
            .iceberg_tables
            .read()
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .filter_map(|k| {
                let table_name = &k[prefix.len()..];
                if table_name.contains('\x00') {
                    None
                } else {
                    Some(table_name.to_string())
                }
            })
            .collect();
        table_names.sort();

        // Skip past page_token
        if !req.page_token.is_empty() {
            table_names.retain(|n| n.as_str() > req.page_token.as_str());
        }

        let has_more = table_names.len() > page_size;
        let table_names: Vec<String> = table_names.into_iter().take(page_size).collect();

        let next_page_token = if has_more {
            table_names.last().cloned().unwrap_or_default()
        } else {
            String::new()
        };

        let identifiers: Vec<IcebergTableIdentifier> = table_names
            .iter()
            .map(|name| IcebergTableIdentifier {
                namespace_levels: req.namespace_levels.clone(),
                name: name.clone(),
            })
            .collect();

        Ok(Response::new(IcebergListTablesResponse {
            identifiers,
            next_page_token,
        }))
    }

    async fn iceberg_table_exists(
        &self,
        request: Request<IcebergTableExistsRequest>,
    ) -> Result<Response<IcebergTableExistsResponse>, Status> {
        let req = request.into_inner();
        let table_key =
            Self::iceberg_table_key_wh(&req.warehouse, &req.namespace_levels, &req.table_name);
        let exists = self.iceberg_tables.read().contains_key(&table_key);
        Ok(Response::new(IcebergTableExistsResponse { exists }))
    }

    async fn iceberg_set_table_policy(
        &self,
        request: Request<IcebergSetTablePolicyRequest>,
    ) -> Result<Response<IcebergSetTablePolicyResponse>, Status> {
        let req = request.into_inner();
        let table_key = Self::iceberg_table_key(&req.namespace_levels, &req.table_name);

        let mut tables = self.iceberg_tables.write();
        let entry = tables
            .get_mut(&table_key)
            .ok_or_else(|| Status::not_found("table not found"))?;

        entry.policy_json = req.policy_json;

        if let Some(store) = &self.store {
            store.put_iceberg_table(&table_key, &entry.encode_to_vec());
        }

        info!(
            "Set policy on iceberg table: {:?}.{}",
            req.namespace_levels, req.table_name
        );

        Ok(Response::new(IcebergSetTablePolicyResponse {
            success: true,
        }))
    }

    async fn iceberg_get_table_policy(
        &self,
        request: Request<IcebergGetTablePolicyRequest>,
    ) -> Result<Response<IcebergGetTablePolicyResponse>, Status> {
        let req = request.into_inner();
        let table_key = Self::iceberg_table_key(&req.namespace_levels, &req.table_name);

        let entry = self
            .iceberg_tables
            .read()
            .get(&table_key)
            .cloned()
            .ok_or_else(|| Status::not_found("table not found"))?;

        Ok(Response::new(IcebergGetTablePolicyResponse {
            policy_json: entry.policy_json,
        }))
    }

    // ---- Group management ----

    async fn create_group(
        &self,
        request: Request<CreateGroupRequest>,
    ) -> Result<Response<CreateGroupResponse>, Status> {
        let req = request.into_inner();

        if req.group_name.is_empty() {
            return Err(Status::invalid_argument("group_name is required"));
        }

        // Check uniqueness
        if self
            .groups
            .read()
            .values()
            .any(|g| g.group_name == req.group_name)
        {
            return Err(Status::already_exists(
                "group with this name already exists",
            ));
        }

        let group_id = Uuid::new_v4().to_string();
        let now = Self::current_timestamp();

        let group = StoredGroup {
            group_id: group_id.clone(),
            group_name: req.group_name.clone(),
            arn: format!("arn:obio:iam::objectio:group/{}", req.group_name),
            member_user_ids: Vec::new(),
            created_at: now,
        };

        self.groups.write().insert(group_id.clone(), group.clone());

        if let Some(store) = &self.store {
            store.put_group(&group_id, &group);
        }

        info!("Created group: {}", req.group_name);

        Ok(Response::new(CreateGroupResponse {
            group: Some(GroupMeta {
                group_id: group.group_id,
                group_name: group.group_name,
                arn: group.arn,
                member_user_ids: group.member_user_ids,
                created_at: group.created_at,
            }),
        }))
    }

    async fn delete_group(
        &self,
        request: Request<DeleteGroupRequest>,
    ) -> Result<Response<DeleteGroupResponse>, Status> {
        let req = request.into_inner();

        let removed = self.groups.write().remove(&req.group_id);
        if removed.is_none() {
            return Err(Status::not_found("group not found"));
        }

        if let Some(store) = &self.store {
            store.delete_group(&req.group_id);
        }

        info!("Deleted group: {}", req.group_id);

        Ok(Response::new(DeleteGroupResponse { success: true }))
    }

    async fn list_groups(
        &self,
        request: Request<ListGroupsRequest>,
    ) -> Result<Response<ListGroupsResponse>, Status> {
        let req = request.into_inner();
        let max_results = if req.max_results == 0 {
            100
        } else {
            req.max_results.min(1000)
        };

        let groups: Vec<GroupMeta> = self
            .groups
            .read()
            .values()
            .filter(|g| req.marker.is_empty() || g.group_id > req.marker)
            .take(max_results as usize + 1)
            .map(|g| GroupMeta {
                group_id: g.group_id.clone(),
                group_name: g.group_name.clone(),
                arn: g.arn.clone(),
                member_user_ids: g.member_user_ids.clone(),
                created_at: g.created_at,
            })
            .collect();

        let is_truncated = groups.len() > max_results as usize;
        let groups: Vec<GroupMeta> = groups.into_iter().take(max_results as usize).collect();
        let next_marker = groups
            .last()
            .map(|g| g.group_id.clone())
            .unwrap_or_default();

        Ok(Response::new(ListGroupsResponse {
            groups,
            next_marker: if is_truncated {
                next_marker
            } else {
                String::new()
            },
            is_truncated,
        }))
    }

    async fn add_user_to_group(
        &self,
        request: Request<AddUserToGroupRequest>,
    ) -> Result<Response<AddUserToGroupResponse>, Status> {
        let req = request.into_inner();

        // Validate user exists
        if !self.users.read().contains_key(&req.user_id) {
            return Err(Status::not_found("user not found"));
        }

        let mut groups = self.groups.write();
        let group = groups
            .get_mut(&req.group_id)
            .ok_or_else(|| Status::not_found("group not found"))?;

        if group.member_user_ids.contains(&req.user_id) {
            return Err(Status::already_exists("user already in group"));
        }

        group.member_user_ids.push(req.user_id.clone());
        let group_snapshot = group.clone();

        if let Some(store) = &self.store {
            store.put_group(&req.group_id, &group_snapshot);
        }

        info!("Added user {} to group {}", req.user_id, req.group_id);

        Ok(Response::new(AddUserToGroupResponse { success: true }))
    }

    async fn remove_user_from_group(
        &self,
        request: Request<RemoveUserFromGroupRequest>,
    ) -> Result<Response<RemoveUserFromGroupResponse>, Status> {
        let req = request.into_inner();

        let mut groups = self.groups.write();
        let group = groups
            .get_mut(&req.group_id)
            .ok_or_else(|| Status::not_found("group not found"))?;

        let before_len = group.member_user_ids.len();
        group.member_user_ids.retain(|id| id != &req.user_id);

        if group.member_user_ids.len() == before_len {
            return Err(Status::not_found("user not in group"));
        }

        let group_snapshot = group.clone();

        if let Some(store) = &self.store {
            store.put_group(&req.group_id, &group_snapshot);
        }

        info!("Removed user {} from group {}", req.user_id, req.group_id);

        Ok(Response::new(RemoveUserFromGroupResponse { success: true }))
    }

    async fn get_user_groups(
        &self,
        request: Request<GetUserGroupsRequest>,
    ) -> Result<Response<GetUserGroupsResponse>, Status> {
        let req = request.into_inner();

        // Validate user exists
        if !self.users.read().contains_key(&req.user_id) {
            return Err(Status::not_found("user not found"));
        }

        let groups: Vec<GroupMeta> = self
            .groups
            .read()
            .values()
            .filter(|g| g.member_user_ids.contains(&req.user_id))
            .map(|g| GroupMeta {
                group_id: g.group_id.clone(),
                group_name: g.group_name.clone(),
                arn: g.arn.clone(),
                member_user_ids: g.member_user_ids.clone(),
                created_at: g.created_at,
            })
            .collect();

        Ok(Response::new(GetUserGroupsResponse { groups }))
    }

    // ---- Data filter RPCs ----

    async fn create_data_filter(
        &self,
        request: Request<CreateDataFilterRequest>,
    ) -> Result<Response<CreateDataFilterResponse>, Status> {
        let req = request.into_inner();
        let filter_id = Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let filter = StoredDataFilter {
            filter_id: filter_id.clone(),
            filter_name: req.filter_name.clone(),
            namespace_levels: req.namespace_levels.clone(),
            table_name: req.table_name.clone(),
            principal_arns: req.principal_arns.clone(),
            allowed_columns: req.allowed_columns.clone(),
            excluded_columns: req.excluded_columns.clone(),
            row_filter_expression: req.row_filter_expression.clone(),
            created_at: now,
            updated_at: now,
        };

        if let Some(store) = &self.store {
            store.put_data_filter(&filter_id, &filter);
        }
        self.data_filters.write().insert(filter_id, filter.clone());

        info!(
            filter_id = %filter.filter_id,
            filter_name = %filter.filter_name,
            table = %filter.table_name,
            "data_filter.created"
        );

        Ok(Response::new(CreateDataFilterResponse {
            filter: Some(IcebergDataFilter {
                filter_id: filter.filter_id,
                filter_name: filter.filter_name,
                namespace_levels: filter.namespace_levels,
                table_name: filter.table_name,
                principal_arns: filter.principal_arns,
                allowed_columns: filter.allowed_columns,
                excluded_columns: filter.excluded_columns,
                row_filter_expression: filter.row_filter_expression,
                created_at: filter.created_at,
                updated_at: filter.updated_at,
            }),
        }))
    }

    async fn list_data_filters(
        &self,
        request: Request<ListDataFiltersRequest>,
    ) -> Result<Response<ListDataFiltersResponse>, Status> {
        let req = request.into_inner();
        let ns_key = req.namespace_levels.join("\x00");

        let filters: Vec<IcebergDataFilter> = self
            .data_filters
            .read()
            .values()
            .filter(|f| f.namespace_levels.join("\x00") == ns_key && f.table_name == req.table_name)
            .map(|f| IcebergDataFilter {
                filter_id: f.filter_id.clone(),
                filter_name: f.filter_name.clone(),
                namespace_levels: f.namespace_levels.clone(),
                table_name: f.table_name.clone(),
                principal_arns: f.principal_arns.clone(),
                allowed_columns: f.allowed_columns.clone(),
                excluded_columns: f.excluded_columns.clone(),
                row_filter_expression: f.row_filter_expression.clone(),
                created_at: f.created_at,
                updated_at: f.updated_at,
            })
            .collect();

        Ok(Response::new(ListDataFiltersResponse { filters }))
    }

    async fn delete_data_filter(
        &self,
        request: Request<DeleteDataFilterRequest>,
    ) -> Result<Response<DeleteDataFilterResponse>, Status> {
        let req = request.into_inner();

        let removed = self.data_filters.write().remove(&req.filter_id).is_some();
        if removed && let Some(store) = &self.store {
            store.delete_data_filter(&req.filter_id);
        }

        Ok(Response::new(DeleteDataFilterResponse { success: removed }))
    }

    async fn get_data_filters_for_principal(
        &self,
        request: Request<GetDataFiltersForPrincipalRequest>,
    ) -> Result<Response<ListDataFiltersResponse>, Status> {
        let req = request.into_inner();
        let ns_key = req.namespace_levels.join("\x00");

        let all_arns: Vec<&str> = std::iter::once(req.principal_arn.as_str())
            .chain(req.group_arns.iter().map(String::as_str))
            .collect();

        let filters: Vec<IcebergDataFilter> = self
            .data_filters
            .read()
            .values()
            .filter(|f| {
                f.namespace_levels.join("\x00") == ns_key
                    && f.table_name == req.table_name
                    && f.principal_arns
                        .iter()
                        .any(|p| p == "*" || all_arns.iter().any(|a| a == p))
            })
            .map(|f| IcebergDataFilter {
                filter_id: f.filter_id.clone(),
                filter_name: f.filter_name.clone(),
                namespace_levels: f.namespace_levels.clone(),
                table_name: f.table_name.clone(),
                principal_arns: f.principal_arns.clone(),
                allowed_columns: f.allowed_columns.clone(),
                excluded_columns: f.excluded_columns.clone(),
                row_filter_expression: f.row_filter_expression.clone(),
                created_at: f.created_at,
                updated_at: f.updated_at,
            })
            .collect();

        Ok(Response::new(ListDataFiltersResponse { filters }))
    }

    // ============================================================
    // Delta Sharing Protocol
    // ============================================================

    async fn delta_create_share(
        &self,
        request: Request<DeltaCreateShareRequest>,
    ) -> Result<Response<DeltaCreateShareResponse>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("share name is required"));
        }
        let now = Self::current_timestamp();
        let entry = DeltaShareEntry {
            name: req.name.clone(),
            comment: req.comment,
            created_at: now as i64,
            tenant: req.tenant,
        };
        {
            let mut shares = self.delta_shares.write();
            if shares.contains_key(&req.name) {
                return Err(Status::already_exists("share already exists"));
            }
            shares.insert(req.name.clone(), entry.clone());
        }
        if let Some(store) = &self.store {
            store.put_delta_share(&req.name, &entry.encode_to_vec());
        }
        info!("Created Delta share: {}", req.name);
        Ok(Response::new(DeltaCreateShareResponse {
            share: Some(entry),
        }))
    }

    async fn delta_get_share(
        &self,
        request: Request<DeltaGetShareRequest>,
    ) -> Result<Response<DeltaGetShareResponse>, Status> {
        let req = request.into_inner();
        let entry = self
            .delta_shares
            .read()
            .get(&req.name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("share '{}' not found", req.name)))?;
        Ok(Response::new(DeltaGetShareResponse { share: Some(entry) }))
    }

    async fn delta_list_shares(
        &self,
        request: Request<DeltaListSharesRequest>,
    ) -> Result<Response<DeltaListSharesResponse>, Status> {
        let req = request.into_inner();
        let shares: Vec<DeltaShareEntry> = self
            .delta_shares
            .read()
            .values()
            .filter(|s| req.tenant.is_empty() || s.tenant == req.tenant)
            .cloned()
            .collect();
        Ok(Response::new(DeltaListSharesResponse {
            shares,
            next_page_token: String::new(),
        }))
    }

    async fn delta_drop_share(
        &self,
        request: Request<DeltaDropShareRequest>,
    ) -> Result<Response<DeltaDropShareResponse>, Status> {
        let req = request.into_inner();
        let removed = self.delta_shares.write().remove(&req.name).is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_delta_share(&req.name);
            }
            // Remove all tables in this share
            self.delta_tables
                .write()
                .retain(|k, _| !k.starts_with(&format!("{}\x00", req.name)));
            info!("Dropped Delta share: {}", req.name);
        }
        Ok(Response::new(DeltaDropShareResponse { success: removed }))
    }

    async fn delta_add_table(
        &self,
        request: Request<DeltaAddTableRequest>,
    ) -> Result<Response<DeltaAddTableResponse>, Status> {
        let req = request.into_inner();
        if !self.delta_shares.read().contains_key(&req.share) {
            return Err(Status::not_found(format!(
                "share '{}' not found",
                req.share
            )));
        }
        let table_key = format!("{}\x00{}\x00{}", req.share, req.schema, req.table_name);
        let share_id = Uuid::new_v4().to_string();
        let entry = DeltaShareTableEntry {
            share: req.share.clone(),
            schema: req.schema.clone(),
            table_name: req.table_name.clone(),
            share_id: share_id.clone(),
            table_type: req.table_type.clone(),
            bucket: req.bucket.clone(),
            path: req.path.clone(),
            warehouse: req.warehouse.clone(),
            namespace: req.namespace.clone(),
        };
        self.delta_tables
            .write()
            .insert(table_key.clone(), entry.clone());
        if let Some(store) = &self.store {
            store.put_delta_table(&table_key, &entry.encode_to_vec());
        }
        info!(
            "Added table {}.{} to Delta share {}",
            req.schema, req.table_name, req.share
        );
        Ok(Response::new(DeltaAddTableResponse { table: Some(entry) }))
    }

    async fn delta_remove_table(
        &self,
        request: Request<DeltaRemoveTableRequest>,
    ) -> Result<Response<DeltaRemoveTableResponse>, Status> {
        let req = request.into_inner();
        let table_key = format!("{}\x00{}\x00{}", req.share, req.schema, req.table_name);
        let removed = self.delta_tables.write().remove(&table_key).is_some();
        if removed && let Some(store) = &self.store {
            store.delete_delta_table(&table_key);
        }
        Ok(Response::new(DeltaRemoveTableResponse { success: removed }))
    }

    async fn delta_list_tables(
        &self,
        request: Request<DeltaListTablesRequest>,
    ) -> Result<Response<DeltaListTablesResponse>, Status> {
        let req = request.into_inner();
        let tables: Vec<DeltaShareTableEntry> = self
            .delta_tables
            .read()
            .iter()
            .filter(|(k, _)| {
                k.starts_with(&format!("{}\x00", req.share))
                    && (req.schema.is_empty()
                        || k.starts_with(&format!("{}\x00{}\x00", req.share, req.schema)))
            })
            .map(|(_, v)| v.clone())
            .collect();
        Ok(Response::new(DeltaListTablesResponse {
            tables,
            next_page_token: String::new(),
        }))
    }

    async fn delta_create_recipient(
        &self,
        request: Request<DeltaCreateRecipientRequest>,
    ) -> Result<Response<DeltaCreateRecipientResponse>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("recipient name is required"));
        }
        // Generate a cryptographically random bearer token (32 bytes → 64 hex chars)
        let raw_token = {
            use rand::RngCore;
            let mut bytes = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut bytes);
            hex::encode(bytes)
        };
        // Store SHA-256 hash of the token (never store raw)
        let token_hash = hex::encode(Sha256::digest(raw_token.as_bytes()));

        let now = Self::current_timestamp();
        let entry = DeltaRecipientEntry {
            name: req.name.clone(),
            token_hash: token_hash.clone(),
            shares: req.shares,
            created_at: now as i64,
        };

        {
            let mut recipients = self.delta_recipients.write();
            if recipients.contains_key(&req.name) {
                return Err(Status::already_exists("recipient already exists"));
            }
            recipients.insert(req.name.clone(), entry.clone());
        }
        self.delta_token_index
            .write()
            .insert(token_hash.clone(), req.name.clone());

        if let Some(store) = &self.store {
            store.put_delta_recipient(&req.name, &entry.encode_to_vec());
        }
        info!("Created Delta recipient: {}", req.name);
        Ok(Response::new(DeltaCreateRecipientResponse {
            recipient: Some(entry),
            raw_token,
        }))
    }

    async fn delta_get_recipient_by_token(
        &self,
        request: Request<DeltaGetRecipientByTokenRequest>,
    ) -> Result<Response<DeltaGetRecipientByTokenResponse>, Status> {
        let req = request.into_inner();
        let token_hash = hex::encode(Sha256::digest(req.raw_token.as_bytes()));
        let recipient_name = self.delta_token_index.read().get(&token_hash).cloned();
        match recipient_name {
            Some(name) => {
                let entry = self.delta_recipients.read().get(&name).cloned();
                Ok(Response::new(DeltaGetRecipientByTokenResponse {
                    recipient: entry,
                    found: true,
                }))
            }
            None => Ok(Response::new(DeltaGetRecipientByTokenResponse {
                recipient: None,
                found: false,
            })),
        }
    }

    async fn delta_list_recipients(
        &self,
        _request: Request<DeltaListRecipientsRequest>,
    ) -> Result<Response<DeltaListRecipientsResponse>, Status> {
        let recipients: Vec<DeltaRecipientEntry> =
            self.delta_recipients.read().values().cloned().collect();
        Ok(Response::new(DeltaListRecipientsResponse {
            recipients,
            next_page_token: String::new(),
        }))
    }

    async fn delta_drop_recipient(
        &self,
        request: Request<DeltaDropRecipientRequest>,
    ) -> Result<Response<DeltaDropRecipientResponse>, Status> {
        let req = request.into_inner();
        let entry = self.delta_recipients.write().remove(&req.name);
        let removed = entry.is_some();
        if let Some(e) = entry {
            // Remove from token index
            self.delta_token_index.write().remove(&e.token_hash);
            if let Some(store) = &self.store {
                store.delete_delta_recipient(&req.name);
            }
            info!("Dropped Delta recipient: {}", req.name);
        }
        Ok(Response::new(DeltaDropRecipientResponse {
            success: removed,
        }))
    }

    // ============ Cluster Configuration ============

    async fn get_config(
        &self,
        request: Request<GetConfigRequest>,
    ) -> Result<Response<GetConfigResponse>, Status> {
        let req = request.into_inner();
        let config = self.config.read();
        match config.get(&req.key) {
            Some(entry) => Ok(Response::new(GetConfigResponse {
                entry: Some(entry.clone()),
                found: true,
            })),
            None => Ok(Response::new(GetConfigResponse {
                entry: None,
                found: false,
            })),
        }
    }

    async fn set_config(
        &self,
        request: Request<SetConfigRequest>,
    ) -> Result<Response<SetConfigResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("config key is required"));
        }

        let version = self
            .config_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let now = Self::current_timestamp();

        let entry = ConfigEntry {
            key: req.key.clone(),
            value: req.value,
            updated_at: now,
            updated_by: req.updated_by,
            version,
        };

        // Persist
        if let Some(store) = &self.store {
            store.put_config(&req.key, &entry.encode_to_vec());
        }

        // Update in-memory
        self.config.write().insert(req.key.clone(), entry.clone());

        // License hot-swap. When the admin installs a new license via
        // `PUT /_admin/license`, the gateway forwards that to `set_config`
        // with key `license/active`. Meta verifies the signature + expiry
        // here so `register_osd` picks up the new caps immediately.
        if req.key == "license/active" {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            match objectio_license::License::load_from_bytes(&entry.value, now) {
                Ok(l) => {
                    info!(
                        "meta license reloaded: tier={} licensee={} max_nodes={} max_raw_capacity_bytes={}",
                        l.tier, l.licensee, l.max_nodes, l.max_raw_capacity_bytes
                    );
                    *self.license.write() = Arc::new(l);
                }
                Err(e) => warn!("license/active rejected on set_config: {}", e),
            }
        }

        info!("Config set: key={}, version={}", req.key, version);

        Ok(Response::new(SetConfigResponse { entry: Some(entry) }))
    }

    async fn delete_config(
        &self,
        request: Request<DeleteConfigRequest>,
    ) -> Result<Response<DeleteConfigResponse>, Status> {
        let req = request.into_inner();
        let removed = self.config.write().remove(&req.key).is_some();

        if removed {
            if let Some(store) = &self.store {
                store.delete_config(&req.key);
            }
            if req.key == "license/active" {
                info!("meta license removed — reverting to Community tier");
                *self.license.write() = Arc::new(objectio_license::License::community());
            }
            info!("Config deleted: key={}", req.key);
        }

        Ok(Response::new(DeleteConfigResponse { success: removed }))
    }

    async fn list_config(
        &self,
        request: Request<ListConfigRequest>,
    ) -> Result<Response<ListConfigResponse>, Status> {
        let req = request.into_inner();
        let config = self.config.read();

        let entries: Vec<ConfigEntry> = if req.prefix.is_empty() {
            config.values().cloned().collect()
        } else {
            config
                .iter()
                .filter(|(k, _)| k.starts_with(&req.prefix))
                .map(|(_, v)| v.clone())
                .collect()
        };

        Ok(Response::new(ListConfigResponse { entries }))
    }

    // ============ Server Pools ============

    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<Response<CreatePoolResponse>, Status> {
        let pool = request
            .into_inner()
            .pool
            .ok_or_else(|| Status::invalid_argument("missing pool"))?;
        if pool.name.is_empty() {
            return Err(Status::invalid_argument("pool name is required"));
        }
        let mut pools = self.pools.write();
        if pools.contains_key(&pool.name) {
            return Err(Status::already_exists(format!(
                "pool '{}' already exists",
                pool.name
            )));
        }
        let mut pool = pool;
        pool.created_at = Self::current_timestamp();
        pool.updated_at = pool.created_at;
        if let Some(store) = &self.store {
            store.put_pool(&pool.name, &pool.encode_to_vec());
        }
        pools.insert(pool.name.clone(), pool.clone());
        info!("Created pool: {}", pool.name);
        Ok(Response::new(CreatePoolResponse { pool: Some(pool) }))
    }

    async fn get_pool(
        &self,
        request: Request<GetPoolRequest>,
    ) -> Result<Response<GetPoolResponse>, Status> {
        let name = request.into_inner().name;
        let pools = self.pools.read();
        match pools.get(&name) {
            Some(pool) => Ok(Response::new(GetPoolResponse {
                pool: Some(pool.clone()),
                found: true,
            })),
            None => Ok(Response::new(GetPoolResponse {
                pool: None,
                found: false,
            })),
        }
    }

    async fn list_pools(
        &self,
        _request: Request<ListPoolsRequest>,
    ) -> Result<Response<ListPoolsResponse>, Status> {
        let pools = self.pools.read();
        Ok(Response::new(ListPoolsResponse {
            pools: pools.values().cloned().collect(),
        }))
    }

    async fn update_pool(
        &self,
        request: Request<UpdatePoolRequest>,
    ) -> Result<Response<UpdatePoolResponse>, Status> {
        let pool = request
            .into_inner()
            .pool
            .ok_or_else(|| Status::invalid_argument("missing pool"))?;
        let mut pools = self.pools.write();
        if !pools.contains_key(&pool.name) {
            return Err(Status::not_found(format!("pool '{}' not found", pool.name)));
        }
        let mut pool = pool;
        pool.updated_at = Self::current_timestamp();
        if let Some(store) = &self.store {
            store.put_pool(&pool.name, &pool.encode_to_vec());
        }
        pools.insert(pool.name.clone(), pool.clone());
        info!("Updated pool: {}", pool.name);
        Ok(Response::new(UpdatePoolResponse { pool: Some(pool) }))
    }

    async fn delete_pool(
        &self,
        request: Request<DeletePoolRequest>,
    ) -> Result<Response<DeletePoolResponse>, Status> {
        let name = request.into_inner().name;
        if name == "default" {
            return Err(Status::invalid_argument("cannot delete the default pool"));
        }
        let removed = self.pools.write().remove(&name).is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_pool(&name);
            }
            info!("Deleted pool: {}", name);
        }
        Ok(Response::new(DeletePoolResponse { success: removed }))
    }

    // ============ Tenants ============

    async fn create_tenant(
        &self,
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<CreateTenantResponse>, Status> {
        let tenant = request
            .into_inner()
            .tenant
            .ok_or_else(|| Status::invalid_argument("missing tenant"))?;
        if tenant.name.is_empty() {
            return Err(Status::invalid_argument("tenant name is required"));
        }
        let mut tenants = self.tenants.write();
        if tenants.contains_key(&tenant.name) {
            return Err(Status::already_exists(format!(
                "tenant '{}' already exists",
                tenant.name
            )));
        }
        let mut tenant = tenant;
        tenant.created_at = Self::current_timestamp();
        tenant.updated_at = tenant.created_at;
        if let Some(store) = &self.store {
            store.put_tenant(&tenant.name, &tenant.encode_to_vec());
        }
        tenants.insert(tenant.name.clone(), tenant.clone());
        info!("Created tenant: {}", tenant.name);
        Ok(Response::new(CreateTenantResponse {
            tenant: Some(tenant),
        }))
    }

    async fn get_tenant(
        &self,
        request: Request<GetTenantRequest>,
    ) -> Result<Response<GetTenantResponse>, Status> {
        let name = request.into_inner().name;
        let tenants = self.tenants.read();
        match tenants.get(&name) {
            Some(t) => Ok(Response::new(GetTenantResponse {
                tenant: Some(t.clone()),
                found: true,
            })),
            None => Ok(Response::new(GetTenantResponse {
                tenant: None,
                found: false,
            })),
        }
    }

    async fn list_tenants(
        &self,
        _request: Request<ListTenantsRequest>,
    ) -> Result<Response<ListTenantsResponse>, Status> {
        let tenants = self.tenants.read();
        Ok(Response::new(ListTenantsResponse {
            tenants: tenants.values().cloned().collect(),
        }))
    }

    async fn update_tenant(
        &self,
        request: Request<UpdateTenantRequest>,
    ) -> Result<Response<UpdateTenantResponse>, Status> {
        let tenant = request
            .into_inner()
            .tenant
            .ok_or_else(|| Status::invalid_argument("missing tenant"))?;
        let mut tenants = self.tenants.write();
        if !tenants.contains_key(&tenant.name) {
            return Err(Status::not_found(format!(
                "tenant '{}' not found",
                tenant.name
            )));
        }
        let mut tenant = tenant;
        tenant.updated_at = Self::current_timestamp();
        if let Some(store) = &self.store {
            store.put_tenant(&tenant.name, &tenant.encode_to_vec());
        }
        tenants.insert(tenant.name.clone(), tenant.clone());
        info!("Updated tenant: {}", tenant.name);
        Ok(Response::new(UpdateTenantResponse {
            tenant: Some(tenant),
        }))
    }

    async fn delete_tenant(
        &self,
        request: Request<DeleteTenantRequest>,
    ) -> Result<Response<DeleteTenantResponse>, Status> {
        let name = request.into_inner().name;
        // Check no buckets belong to this tenant
        let has_buckets = self.buckets.read().values().any(|b| b.tenant == name);
        if has_buckets {
            return Err(Status::failed_precondition(format!(
                "tenant '{}' still has buckets — delete them first",
                name
            )));
        }
        let removed = self.tenants.write().remove(&name).is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_tenant(&name);
            }
            info!("Deleted tenant: {}", name);
        }
        Ok(Response::new(DeleteTenantResponse { success: removed }))
    }

    // ============================================================
    // ============================================================
    // Iceberg Warehouse Management
    // ============================================================

    async fn iceberg_create_warehouse(
        &self,
        request: Request<IcebergCreateWarehouseRequest>,
    ) -> Result<Response<IcebergCreateWarehouseResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("warehouse name is required"));
        }

        // Check if warehouse already exists
        if self.iceberg_warehouses.read().contains_key(&req.name) {
            return Err(Status::already_exists(format!(
                "warehouse '{}' already exists",
                req.name
            )));
        }

        let bucket_name = format!("iceberg-{}", req.name);
        let location = format!("s3://{}", bucket_name);
        let now = Self::current_timestamp();

        // Create the backing bucket
        if self.buckets.read().contains_key(&bucket_name) {
            return Err(Status::already_exists(format!(
                "bucket '{}' already exists",
                bucket_name
            )));
        }

        let bucket = BucketMeta {
            name: bucket_name.clone(),
            owner: "system".to_string(),
            created_at: now,
            storage_class: "STANDARD".to_string(),
            versioning: VersioningState::VersioningDisabled.into(),
            pool: String::new(),
            tenant: req.tenant.clone(),
            quota_bytes: 0,
            quota_objects: 0,
            object_lock: None,
        };
        self.buckets
            .write()
            .insert(bucket_name.clone(), bucket.clone());
        if let Some(store) = &self.store {
            store.put_bucket(&bucket_name, &bucket);
        }

        // Create warehouse entry
        let warehouse = IcebergWarehouse {
            name: req.name.clone(),
            bucket: bucket_name,
            location,
            tenant: req.tenant,
            created_at: now,
            properties: req.properties,
        };

        let bytes = warehouse.encode_to_vec();
        self.iceberg_warehouses
            .write()
            .insert(req.name.clone(), warehouse.clone());
        if let Some(store) = &self.store {
            store.put_warehouse(&req.name, &bytes);
        }

        info!(
            "Created warehouse: {} (bucket: {})",
            req.name, warehouse.bucket
        );

        Ok(Response::new(IcebergCreateWarehouseResponse {
            warehouse: Some(warehouse),
        }))
    }

    async fn iceberg_list_warehouses(
        &self,
        request: Request<IcebergListWarehousesRequest>,
    ) -> Result<Response<IcebergListWarehousesResponse>, Status> {
        let req = request.into_inner();
        let warehouses: Vec<IcebergWarehouse> = self
            .iceberg_warehouses
            .read()
            .values()
            .filter(|w| req.tenant.is_empty() || w.tenant == req.tenant)
            .cloned()
            .collect();
        Ok(Response::new(IcebergListWarehousesResponse { warehouses }))
    }

    async fn iceberg_delete_warehouse(
        &self,
        request: Request<IcebergDeleteWarehouseRequest>,
    ) -> Result<Response<IcebergDeleteWarehouseResponse>, Status> {
        let name = request.into_inner().name;
        let removed = self.iceberg_warehouses.write().remove(&name);
        if let Some(wh) = removed {
            // Also remove the backing bucket
            self.buckets.write().remove(&wh.bucket);
            if let Some(store) = &self.store {
                store.delete_warehouse(&name);
                store.delete_bucket(&wh.bucket);
            }
            info!("Deleted warehouse: {} (bucket: {})", name, wh.bucket);
            Ok(Response::new(IcebergDeleteWarehouseResponse {
                success: true,
            }))
        } else {
            Err(Status::not_found(format!("warehouse '{}' not found", name)))
        }
    }

    // Bucket Versioning
    // ============================================================

    async fn put_bucket_versioning(
        &self,
        request: Request<PutBucketVersioningRequest>,
    ) -> Result<Response<PutBucketVersioningResponse>, Status> {
        let req = request.into_inner();
        let mut buckets = self.buckets.write();
        let bucket = buckets
            .get_mut(&req.bucket)
            .ok_or_else(|| Status::not_found(format!("bucket '{}' not found", req.bucket)))?;

        // Object-locked buckets cannot have versioning suspended
        if req.state() == VersioningState::VersioningSuspended {
            let lock_configs = self.object_lock_configs.read();
            if lock_configs.get(&req.bucket).is_some_and(|c| c.enabled) {
                return Err(Status::failed_precondition(
                    "cannot suspend versioning on object-locked bucket",
                ));
            }
        }

        bucket.versioning = req.state;
        if let Some(store) = &self.store {
            store.put_bucket(&req.bucket, bucket);
        }
        info!(
            "Set versioning for bucket '{}' to {:?}",
            req.bucket,
            req.state()
        );
        Ok(Response::new(PutBucketVersioningResponse { success: true }))
    }

    async fn get_bucket_versioning(
        &self,
        request: Request<GetBucketVersioningRequest>,
    ) -> Result<Response<GetBucketVersioningResponse>, Status> {
        let bucket_name = request.into_inner().bucket;
        let buckets = self.buckets.read();
        let bucket = buckets
            .get(&bucket_name)
            .ok_or_else(|| Status::not_found(format!("bucket '{}' not found", bucket_name)))?;
        Ok(Response::new(GetBucketVersioningResponse {
            state: bucket.versioning,
        }))
    }

    // ============================================================
    // Object Lock Configuration
    // ============================================================

    async fn put_object_lock_configuration(
        &self,
        request: Request<PutObjectLockConfigRequest>,
    ) -> Result<Response<PutObjectLockConfigResponse>, Status> {
        let req = request.into_inner();
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("missing object lock configuration"))?;

        // Verify bucket exists
        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found(format!(
                "bucket '{}' not found",
                req.bucket
            )));
        }

        let bytes = config.encode_to_vec();
        self.object_lock_configs
            .write()
            .insert(req.bucket.clone(), config);
        if let Some(store) = &self.store {
            store.put_object_lock_config(&req.bucket, &bytes);
        }
        info!("Set object lock config for bucket '{}'", req.bucket);
        Ok(Response::new(PutObjectLockConfigResponse { success: true }))
    }

    async fn get_object_lock_configuration(
        &self,
        request: Request<GetObjectLockConfigRequest>,
    ) -> Result<Response<GetObjectLockConfigResponse>, Status> {
        let bucket = request.into_inner().bucket;
        let configs = self.object_lock_configs.read();
        match configs.get(&bucket) {
            Some(config) => Ok(Response::new(GetObjectLockConfigResponse {
                config: Some(*config),
                found: true,
            })),
            None => Ok(Response::new(GetObjectLockConfigResponse {
                config: None,
                found: false,
            })),
        }
    }

    // ============================================================
    // Lifecycle Configuration
    // ============================================================

    async fn put_bucket_lifecycle(
        &self,
        request: Request<PutBucketLifecycleRequest>,
    ) -> Result<Response<PutBucketLifecycleResponse>, Status> {
        let req = request.into_inner();
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("missing lifecycle configuration"))?;

        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found(format!(
                "bucket '{}' not found",
                req.bucket
            )));
        }

        let bytes = config.encode_to_vec();
        self.lifecycle_configs
            .write()
            .insert(req.bucket.clone(), config);
        if let Some(store) = &self.store {
            store.put_lifecycle_config(&req.bucket, &bytes);
        }
        info!(
            "Set lifecycle config for bucket '{}' ({} rules)",
            req.bucket,
            bytes.len()
        );
        Ok(Response::new(PutBucketLifecycleResponse { success: true }))
    }

    async fn get_bucket_lifecycle(
        &self,
        request: Request<GetBucketLifecycleRequest>,
    ) -> Result<Response<GetBucketLifecycleResponse>, Status> {
        let bucket = request.into_inner().bucket;
        let configs = self.lifecycle_configs.read();
        match configs.get(&bucket) {
            Some(config) => Ok(Response::new(GetBucketLifecycleResponse {
                config: Some(config.clone()),
                found: true,
            })),
            None => Ok(Response::new(GetBucketLifecycleResponse {
                config: None,
                found: false,
            })),
        }
    }

    async fn delete_bucket_lifecycle(
        &self,
        request: Request<DeleteBucketLifecycleRequest>,
    ) -> Result<Response<DeleteBucketLifecycleResponse>, Status> {
        let bucket = request.into_inner().bucket;
        let removed = self.lifecycle_configs.write().remove(&bucket).is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_lifecycle_config(&bucket);
            }
            info!("Deleted lifecycle config for bucket '{}'", bucket);
        }
        Ok(Response::new(DeleteBucketLifecycleResponse {
            success: removed,
        }))
    }

    // ============================================================
    // Bucket Default SSE Configuration
    // ============================================================

    async fn put_bucket_encryption(
        &self,
        request: Request<PutBucketEncryptionRequest>,
    ) -> Result<Response<PutBucketEncryptionResponse>, Status> {
        let req = request.into_inner();
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("missing bucket encryption configuration"))?;

        if !self.buckets.read().contains_key(&req.bucket) {
            return Err(Status::not_found(format!(
                "bucket '{}' not found",
                req.bucket
            )));
        }

        let bytes = config.encode_to_vec();
        self.bucket_encryption_configs
            .write()
            .insert(req.bucket.clone(), config);
        if let Some(store) = &self.store {
            store.put_bucket_encryption_config(&req.bucket, &bytes);
        }
        info!("Set bucket encryption config for bucket '{}'", req.bucket);
        Ok(Response::new(PutBucketEncryptionResponse { success: true }))
    }

    async fn get_bucket_encryption(
        &self,
        request: Request<GetBucketEncryptionRequest>,
    ) -> Result<Response<GetBucketEncryptionResponse>, Status> {
        let bucket = request.into_inner().bucket;
        let configs = self.bucket_encryption_configs.read();
        match configs.get(&bucket) {
            Some(config) => Ok(Response::new(GetBucketEncryptionResponse {
                config: Some(config.clone()),
                found: true,
            })),
            None => Ok(Response::new(GetBucketEncryptionResponse {
                config: None,
                found: false,
            })),
        }
    }

    async fn delete_bucket_encryption(
        &self,
        request: Request<DeleteBucketEncryptionRequest>,
    ) -> Result<Response<DeleteBucketEncryptionResponse>, Status> {
        let bucket = request.into_inner().bucket;
        let removed = self
            .bucket_encryption_configs
            .write()
            .remove(&bucket)
            .is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_bucket_encryption_config(&bucket);
            }
            info!("Deleted bucket encryption config for bucket '{}'", bucket);
        }
        Ok(Response::new(DeleteBucketEncryptionResponse {
            success: removed,
        }))
    }

    // ============================================================
    // KMS keys
    // ============================================================

    async fn create_kms_key(
        &self,
        request: Request<CreateKmsKeyRequest>,
    ) -> Result<Response<CreateKmsKeyResponse>, Status> {
        let req = request.into_inner();
        if req.wrapped_key_material.is_empty() {
            return Err(Status::invalid_argument(
                "wrapped_key_material is required — gateway wraps the raw key before sending",
            ));
        }
        let key_id = if req.key_id.trim().is_empty() {
            Self::generate_kms_key_id()
        } else {
            req.key_id.trim().to_string()
        };
        let now = Self::current_timestamp();
        let mut map = self.kms_keys.write();
        if map.contains_key(&key_id) {
            return Err(Status::already_exists(format!(
                "KMS key '{key_id}' already exists"
            )));
        }
        let key = KmsKey {
            key_id: key_id.clone(),
            arn: format!("arn:obio:kms:::{key_id}"),
            description: req.description,
            wrapped_key_material: req.wrapped_key_material,
            status: 0, // KMS_KEY_ENABLED
            created_at: now,
            updated_at: now,
            created_by: req.created_by,
        };
        if let Some(store) = &self.store {
            store.put_kms_key(&key_id, &key.encode_to_vec());
        }
        map.insert(key_id.clone(), key.clone());
        info!("Created KMS key '{key_id}'");
        Ok(Response::new(CreateKmsKeyResponse { key: Some(key) }))
    }

    async fn get_kms_key(
        &self,
        request: Request<GetKmsKeyRequest>,
    ) -> Result<Response<GetKmsKeyResponse>, Status> {
        let key_id = request.into_inner().key_id;
        let map = self.kms_keys.read();
        match map.get(&key_id) {
            Some(k) => Ok(Response::new(GetKmsKeyResponse {
                key: Some(k.clone()),
                found: true,
            })),
            None => Ok(Response::new(GetKmsKeyResponse {
                key: None,
                found: false,
            })),
        }
    }

    async fn list_kms_keys(
        &self,
        request: Request<ListKmsKeysRequest>,
    ) -> Result<Response<ListKmsKeysResponse>, Status> {
        let req = request.into_inner();
        let max = if req.max_results == 0 {
            1000
        } else {
            req.max_results as usize
        };
        let map = self.kms_keys.read();
        let mut keys: Vec<KmsKey> = map.values().cloned().collect();
        keys.sort_by(|a, b| a.key_id.cmp(&b.key_id));
        // Simple pagination: treat page_token as the last key_id returned.
        if !req.page_token.is_empty() {
            keys.retain(|k| k.key_id > req.page_token);
        }
        let next_token = if keys.len() > max {
            keys[max - 1].key_id.clone()
        } else {
            String::new()
        };
        keys.truncate(max);
        Ok(Response::new(ListKmsKeysResponse {
            keys,
            next_page_token: next_token,
        }))
    }

    async fn delete_kms_key(
        &self,
        request: Request<DeleteKmsKeyRequest>,
    ) -> Result<Response<DeleteKmsKeyResponse>, Status> {
        let key_id = request.into_inner().key_id;
        let removed = self.kms_keys.write().remove(&key_id).is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_kms_key(&key_id);
            }
            info!("Deleted KMS key '{key_id}'");
        }
        Ok(Response::new(DeleteKmsKeyResponse { success: removed }))
    }

    // ---- Named IAM Policies (PBAC) ----

    async fn create_policy(
        &self,
        request: Request<CreatePolicyRequest>,
    ) -> Result<Response<CreatePolicyResponse>, Status> {
        let req = request.into_inner();
        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Err(Status::invalid_argument("Policy name is required"));
        }
        if req.policy_json.trim().is_empty() {
            return Err(Status::invalid_argument("Policy JSON is required"));
        }
        // Validate that policy_json is valid JSON
        if serde_json::from_str::<serde_json::Value>(&req.policy_json).is_err() {
            return Err(Status::invalid_argument("Invalid JSON in policy document"));
        }

        let mut map = self.iam_policies.write();
        if map.contains_key(&name) {
            return Err(Status::already_exists(format!(
                "Policy '{}' already exists",
                name
            )));
        }
        let now = Self::current_timestamp();
        let policy = PolicyObject {
            name: name.clone(),
            policy_json: req.policy_json,
            created_at: now,
            updated_at: now,
        };
        if let Some(store) = &self.store {
            store.put_iam_policy(&name, &policy.encode_to_vec());
        }
        map.insert(name.clone(), policy.clone());
        info!("Created IAM policy '{}'", name);
        Ok(Response::new(CreatePolicyResponse {
            policy: Some(policy),
        }))
    }

    async fn get_policy(
        &self,
        request: Request<GetPolicyRequest>,
    ) -> Result<Response<GetPolicyResponse>, Status> {
        let name = request.into_inner().name;
        let map = self.iam_policies.read();
        match map.get(&name) {
            Some(policy) => Ok(Response::new(GetPolicyResponse {
                policy: Some(policy.clone()),
                found: true,
            })),
            None => Ok(Response::new(GetPolicyResponse {
                policy: None,
                found: false,
            })),
        }
    }

    async fn list_policies(
        &self,
        _request: Request<ListPoliciesRequest>,
    ) -> Result<Response<ListPoliciesResponse>, Status> {
        let map = self.iam_policies.read();
        let policies: Vec<PolicyObject> = map.values().cloned().collect();
        Ok(Response::new(ListPoliciesResponse { policies }))
    }

    async fn delete_policy(
        &self,
        request: Request<DeletePolicyRequest>,
    ) -> Result<Response<DeletePolicyResponse>, Status> {
        let name = request.into_inner().name;
        let removed = self.iam_policies.write().remove(&name).is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_iam_policy(&name);
            }
            // Remove from all attachments
            let mut attachments = self.policy_attachments.write();
            let mut keys_to_update = Vec::new();
            for (key, policies) in attachments.iter_mut() {
                if policies.contains(&name) {
                    policies.retain(|p| p != &name);
                    keys_to_update.push((key.clone(), policies.clone()));
                }
            }
            // Persist updated attachments
            if let Some(store) = &self.store {
                for (key, policies) in &keys_to_update {
                    if policies.is_empty() {
                        store.delete_policy_attachment(key);
                    } else {
                        store.put_policy_attachment(key, &policies.join(","));
                    }
                }
            }
            // Remove empty attachment entries
            attachments.retain(|_, v| !v.is_empty());
            info!("Deleted IAM policy '{}'", name);
        }
        Ok(Response::new(DeletePolicyResponse { success: removed }))
    }

    async fn attach_policy(
        &self,
        request: Request<AttachPolicyRequest>,
    ) -> Result<Response<AttachPolicyResponse>, Status> {
        let req = request.into_inner();
        let policy_name = req.policy_name;

        // Validate the policy exists
        if !self.iam_policies.read().contains_key(&policy_name) {
            return Err(Status::not_found(format!(
                "Policy '{}' not found",
                policy_name
            )));
        }

        let key = if !req.user_id.is_empty() {
            format!("user:{}", req.user_id)
        } else if !req.group_id.is_empty() {
            format!("group:{}", req.group_id)
        } else {
            return Err(Status::invalid_argument(
                "Either user_id or group_id is required",
            ));
        };

        let mut attachments = self.policy_attachments.write();
        let policies = attachments.entry(key.clone()).or_default();
        if !policies.contains(&policy_name) {
            policies.push(policy_name.clone());
            if let Some(store) = &self.store {
                store.put_policy_attachment(&key, &policies.join(","));
            }
            info!("Attached policy '{}' to '{}'", policy_name, key);
        }
        Ok(Response::new(AttachPolicyResponse { success: true }))
    }

    async fn detach_policy(
        &self,
        request: Request<DetachPolicyRequest>,
    ) -> Result<Response<DetachPolicyResponse>, Status> {
        let req = request.into_inner();
        let policy_name = req.policy_name;

        let key = if !req.user_id.is_empty() {
            format!("user:{}", req.user_id)
        } else if !req.group_id.is_empty() {
            format!("group:{}", req.group_id)
        } else {
            return Err(Status::invalid_argument(
                "Either user_id or group_id is required",
            ));
        };

        let mut attachments = self.policy_attachments.write();
        let removed = if let Some(policies) = attachments.get_mut(&key) {
            let before = policies.len();
            policies.retain(|p| p != &policy_name);
            let did_remove = policies.len() < before;
            if did_remove {
                if let Some(store) = &self.store {
                    if policies.is_empty() {
                        store.delete_policy_attachment(&key);
                    } else {
                        store.put_policy_attachment(&key, &policies.join(","));
                    }
                }
                info!("Detached policy '{}' from '{}'", policy_name, key);
            }
            // Clean up empty entries
            if policies.is_empty() {
                attachments.remove(&key);
            }
            did_remove
        } else {
            false
        };
        Ok(Response::new(DetachPolicyResponse { success: removed }))
    }

    async fn list_attached_policies(
        &self,
        request: Request<ListAttachedPoliciesRequest>,
    ) -> Result<Response<ListAttachedPoliciesResponse>, Status> {
        let req = request.into_inner();
        let key = if !req.user_id.is_empty() {
            format!("user:{}", req.user_id)
        } else if !req.group_id.is_empty() {
            format!("group:{}", req.group_id)
        } else {
            return Err(Status::invalid_argument(
                "Either user_id or group_id is required",
            ));
        };

        let attachments = self.policy_attachments.read();
        let policy_names = attachments.get(&key).cloned().unwrap_or_default();
        Ok(Response::new(ListAttachedPoliciesResponse { policy_names }))
    }
}
