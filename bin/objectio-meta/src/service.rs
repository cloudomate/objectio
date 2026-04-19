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
    ObjectListingEntry,
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
    IcebergCommitTransactionRequest,
    IcebergCommitTransactionResponse,
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
    GetPlacementGroupRequest,
    GetPlacementGroupResponse,
    ListPlacementGroupsRequest,
    ListPlacementGroupsResponse,
    PlacementGroup,
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
    SetOsdAdminStateRequest,
    SetOsdAdminStateResponse,
    GetDrainStatusRequest,
    GetDrainStatusResponse,
    DrainStatus as ProtoDrainStatus,
    GetRebalanceStatusRequest,
    GetRebalanceStatusResponse,
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

/// Map an openraft `client_write` error to a tonic status the gateway /
/// CLI can interpret. ForwardToLeader becomes `FailedPrecondition` with
/// a message the caller can parse for the leader id; all other errors
/// become `Internal`. Concrete to the types used by `MetaTypeConfig`.
fn raft_write_to_status(
    err: &openraft::error::RaftError<
        u64,
        openraft::error::ClientWriteError<u64, openraft::BasicNode>,
    >,
) -> tonic::Status {
    use openraft::error::{ClientWriteError, RaftError};
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
            let leader = f
                .leader_id
                .map_or_else(|| "unknown".to_string(), |id| id.to_string());
            let addr = f
                .leader_node
                .as_ref()
                .map_or_else(String::new, |n| format!(" at {}", n.addr));
            tonic::Status::failed_precondition(format!(
                "not the raft leader — forward to node {leader}{addr}"
            ))
        }
        other => tonic::Status::internal(format!("raft client_write: {other}")),
    }
}

/// Classify a shard slot's role under MDS/LRC/Replication layouts the
/// PG-allocation path uses. For MDS and Replication the layout is
/// [data... parity...]; for LRC it is [data... local_parities...
/// global_parities...] ordered by `template.lrc(k, l, g)` in the
/// placement crate.
fn pg_position_shard_type(
    ec_type: ErasureType,
    position: usize,
    ec_k: usize,
    ec_local_parity: usize,
    _local_group_size: usize,
) -> ShardType {
    match ec_type {
        ErasureType::ErasureLrc => {
            if position < ec_k {
                ShardType::ShardData
            } else if position < ec_k + ec_local_parity {
                ShardType::ShardLocalParity
            } else {
                ShardType::ShardGlobalParity
            }
        }
        ErasureType::ErasureReplication => ShardType::ShardData,
        _ => {
            if position < ec_k {
                ShardType::ShardData
            } else {
                ShardType::ShardGlobalParity
            }
        }
    }
}

/// For LRC, shard positions within a local-parity group share a
/// `local_group` id. MDS and Replication return 0.
fn pg_position_local_group(
    ec_type: ErasureType,
    position: usize,
    ec_k: usize,
    ec_local_parity: usize,
    local_group_size: usize,
) -> u32 {
    if ec_type != ErasureType::ErasureLrc || local_group_size == 0 {
        return 0;
    }
    if position < ec_k {
        (position / local_group_size) as u32
    } else if position < ec_k + ec_local_parity {
        ((position - ec_k).min(ec_local_parity.saturating_sub(1))) as u32
    } else {
        0
    }
}

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
    /// Placement groups: (pool, pg_id) -> PlacementGroup. Refreshed by
    /// the apply-listener so every replica has an up-to-date cache and
    /// GetPlacement on the leader is a sub-millisecond in-memory lookup.
    placement_groups: RwLock<HashMap<(String, u32), PlacementGroup>>,
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
    /// Raft handle — set by main.rs after `Raft::new()` succeeds. Config
    /// mutations route through `client_write`; other mutations still
    /// write to redb directly pending their R2+ migration. Behind a
    /// `RwLock` so the service can be constructed before Raft (the
    /// existing startup order needs meta_service to register OSDs first).
    raft: RwLock<Option<Arc<openraft::Raft<objectio_meta_store::MetaTypeConfig>>>>,
    /// Per-OSD drain progress — populated by the Phase 3b migrator
    /// while an OSD is Draining, cleared when the OSD flips to Out or
    /// back to In. Keyed by 16-byte node_id.
    ///
    /// Exposed to the admin layer via `drain_statuses()` and consumed
    /// by `GET /_admin/drain-status`. Lives in memory only — losing
    /// progress across leader failover is OK, the next sweep
    /// reconstructs it from current shard counts.
    drain_statuses: RwLock<HashMap<[u8; 16], DrainProgress>>,
    /// Cluster-wide rebalance progress (one instance, not per-OSD).
    rebalance_progress: RwLock<RebalanceProgress>,
}

/// Cluster-wide rebalance progress — exposed to the admin UI.
///
/// The rebalancer scans every primary-held ObjectMeta in the cluster
/// once per scan cycle and migrates misplaced shards at the same rate
/// as the drain migrator. One instance lives in `MetaService`;
/// populated on the Raft leader, read by anyone.
#[derive(Clone, Debug, Default)]
pub struct RebalanceProgress {
    /// True iff the reconciler sweep touched at least one OSD since
    /// process start. Lets the console distinguish "not yet started"
    /// from "finished clean".
    pub started: bool,
    /// Admin-toggled: when true, reconciler skips its sweep. Stored
    /// in the `rebalance/paused` config key; mirrored here for fast
    /// reads without a config lookup.
    pub paused: bool,
    /// Last time a sweep completed (unix seconds). 0 before first.
    pub last_sweep_at: u64,
    /// Number of ObjectMetas scanned so far in the current pass.
    /// Resets each time we complete a full loop through the cluster.
    pub scanned_this_pass: u64,
    /// Number of drifted shards observed in the current pass.
    pub drifts_seen_this_pass: u64,
    /// Cumulative count of shards successfully migrated since process
    /// start. Monotonic — doesn't reset per pass.
    pub shards_rebalanced_total: u64,
    /// Last non-empty error (transient OSD outage, etc.). Empty when
    /// the last sweep succeeded. Surfaces real failures to operators.
    pub last_error: String,
}

/// Live progress for one Draining OSD.
#[derive(Clone, Debug, Default)]
pub struct DrainProgress {
    /// shard_count reported by the OSD at the last sweep.
    pub shards_remaining: u64,
    /// Initial shard_count observed when the OSD first entered
    /// Draining in this leader's lifetime. Used for "X of Y migrated"
    /// display — `shards_migrated = initial - remaining`.
    pub initial_shards: u64,
    /// Unix seconds of the last sweep update.
    pub updated_at: u64,
    /// Last non-transient error observed by the migrator for this
    /// OSD. Empty when the last sweep succeeded. Surfaced to the
    /// admin UI so operators see real failures instead of silent
    /// stalls.
    pub last_error: String,
    /// Number of shards the migrator successfully moved so far (distinct
    /// from the derived `initial - remaining` because OSD shard_count
    /// is eventually consistent and may lag slightly).
    pub shards_migrated: u64,
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
            placement_groups: RwLock::new(HashMap::new()),
            object_lock_configs: RwLock::new(HashMap::new()),
            lifecycle_configs: RwLock::new(HashMap::new()),
            bucket_encryption_configs: RwLock::new(HashMap::new()),
            kms_keys: RwLock::new(HashMap::new()),
            drain_statuses: RwLock::new(HashMap::new()),
            rebalance_progress: RwLock::new(RebalanceProgress::default()),
            license: RwLock::new(Arc::new(objectio_license::License::community())),
            store: None,
            raft: RwLock::new(None),
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

    /// Borrow the underlying persistent store, if the service is
    /// persistence-backed. Raft wiring needs this so `MetaRaftStorage`
    /// can share the same redb database handle.
    #[must_use]
    pub fn store(&self) -> Option<Arc<MetaStore>> {
        self.store.clone()
    }

    /// Install the Raft handle. Called from `main.rs` once the cluster
    /// scaffolding is live, before the service is wrapped in `Arc`.
    /// Config mutations routed through Raft become linearizable; without
    /// a handle set, they fall back to the legacy direct-redb path
    /// (useful for in-memory tests).
    pub fn set_raft(
        &self,
        raft: Arc<openraft::Raft<objectio_meta_store::MetaTypeConfig>>,
    ) {
        *self.raft.write() = Some(raft);
    }

    /// Current Raft handle, if any.
    fn raft_handle(
        &self,
    ) -> Option<Arc<openraft::Raft<objectio_meta_store::MetaTypeConfig>>> {
        self.raft.read().clone()
    }

    /// Spawn the apply-listener task. Consumes `ApplyEvent`s emitted by
    /// the Raft state machine after every committed MultiCas and
    /// refreshes the matching in-memory cache. Runs on every replica —
    /// not just the leader — so on failover a freshly-promoted pod
    /// serves reads against a cache that was kept live all along,
    /// instead of the pre-Raft snapshot plus whatever's been applied
    /// since startup.
    pub fn spawn_apply_listener(
        self: &Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<objectio_meta_store::ApplyEvent>,
    ) {
        let svc = Arc::clone(self);
        tokio::spawn(async move {
            use objectio_meta_store::{ApplyEvent, CasTable};
            while let Some(ev) = rx.recv().await {
                match ev {
                    ApplyEvent::MultiCasOp {
                        table,
                        key,
                        new_value,
                    } => match table {
                        CasTable::Buckets => svc.apply_bucket_event(&key, new_value.as_deref()),
                        CasTable::BucketPolicies => {
                            svc.apply_bucket_policy_event(&key, new_value.as_deref());
                        }
                        CasTable::Users => svc.apply_user_event(&key, new_value.as_deref()),
                        CasTable::AccessKeys => {
                            svc.apply_access_key_event(&key, new_value.as_deref());
                        }
                        CasTable::IcebergTables => {
                            svc.apply_iceberg_table_event(&key, new_value.as_deref());
                        }
                        CasTable::IcebergNamespaces => {
                            svc.apply_iceberg_namespace_event(&key, new_value.as_deref());
                        }
                        CasTable::IcebergWarehouses => {
                            svc.apply_iceberg_warehouse_event(&key, new_value.as_deref());
                        }
                        CasTable::PlacementGroups => {
                            svc.apply_placement_group_event(&key, new_value.as_deref());
                        }
                        // Tables not yet covered by a cache refresh:
                        // writers are responsible for mirroring their
                        // own writes on the leader, and followers still
                        // rebuild from redb on promote (load_from_store).
                        _ => {}
                    },
                }
            }
        });
    }

    fn apply_bucket_event(&self, key: &str, new_value: Option<&[u8]>) {
        use prost::Message;
        let mut buckets = self.buckets.write();
        match new_value {
            Some(bytes) => match BucketMeta::decode(bytes) {
                Ok(b) => {
                    buckets.insert(key.to_string(), b);
                }
                Err(e) => warn!("apply: decode BucketMeta('{key}') failed: {e}"),
            },
            None => {
                buckets.remove(key);
            }
        }
    }

    fn apply_bucket_policy_event(&self, key: &str, new_value: Option<&[u8]>) {
        let mut m = self.bucket_policies.write();
        match new_value {
            Some(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => {
                    m.insert(key.to_string(), s.to_string());
                }
                Err(e) => warn!("apply: bucket_policy('{key}') not utf-8: {e}"),
            },
            None => {
                m.remove(key);
            }
        }
    }

    fn apply_user_event(&self, key: &str, new_value: Option<&[u8]>) {
        let mut m = self.users.write();
        match new_value {
            Some(bytes) => match bincode::deserialize::<StoredUser>(bytes) {
                Ok(u) => {
                    m.insert(key.to_string(), u);
                }
                Err(e) => warn!("apply: decode StoredUser('{key}') failed: {e}"),
            },
            None => {
                m.remove(key);
            }
        }
    }

    fn apply_access_key_event(&self, key: &str, new_value: Option<&[u8]>) {
        let mut m = self.access_keys.write();
        match new_value {
            Some(bytes) => match bincode::deserialize::<StoredAccessKey>(bytes) {
                Ok(k) => {
                    // Keep user_keys index consistent: insert the
                    // access_key_id under the owning user if absent.
                    let user_id = k.user_id.clone();
                    m.insert(key.to_string(), k);
                    drop(m);
                    let mut idx = self.user_keys.write();
                    let ids = idx.entry(user_id).or_default();
                    if !ids.iter().any(|k2| k2 == key) {
                        ids.push(key.to_string());
                    }
                }
                Err(e) => warn!("apply: decode StoredAccessKey('{key}') failed: {e}"),
            },
            None => {
                let removed = m.remove(key);
                drop(m);
                if let Some(k) = removed {
                    let mut idx = self.user_keys.write();
                    if let Some(ids) = idx.get_mut(&k.user_id) {
                        ids.retain(|k2| k2 != key);
                    }
                }
            }
        }
    }

    fn apply_iceberg_table_event(&self, key: &str, new_value: Option<&[u8]>) {
        use prost::Message;
        let mut tables = self.iceberg_tables.write();
        match new_value {
            Some(bytes) => match IcebergTableEntry::decode(bytes) {
                Ok(e) => {
                    tables.insert(key.to_string(), e);
                }
                Err(e) => warn!("apply: decode IcebergTableEntry('{key}') failed: {e}"),
            },
            None => {
                tables.remove(key);
            }
        }
    }

    fn apply_iceberg_namespace_event(&self, key: &str, new_value: Option<&[u8]>) {
        use prost::Message;
        let mut ns = self.iceberg_namespaces.write();
        match new_value {
            Some(bytes) => match IcebergCreateNamespaceResponse::decode(bytes) {
                Ok(r) => {
                    ns.insert(key.to_string(), r.properties);
                }
                Err(e) => warn!("apply: decode IcebergNamespace('{key}') failed: {e}"),
            },
            None => {
                ns.remove(key);
            }
        }
    }

    /// Apply a committed PlacementGroup mutation to the in-memory
    /// cache. Key format is "{pool}\0{pg_id:010}" — same as the redb
    /// key produced by `objectio_meta_store::MetaStore::pg_key`. A
    /// delete (`new_value = None`) removes the entry; a put decodes
    /// the prost bytes and upserts.
    fn apply_placement_group_event(&self, key: &str, new_value: Option<&[u8]>) {
        use prost::Message;
        let Some((pool, pg_id)) = Self::parse_pg_key(key) else {
            warn!("apply: malformed placement_group key '{key}'");
            return;
        };
        let mut map = self.placement_groups.write();
        match new_value {
            Some(bytes) => match PlacementGroup::decode(bytes) {
                Ok(pg) => {
                    map.insert((pool, pg_id), pg);
                }
                Err(e) => warn!("apply: decode PlacementGroup('{key}') failed: {e}"),
            },
            None => {
                map.remove(&(pool, pg_id));
            }
        }
    }

    /// Inverse of `objectio_meta_store::MetaStore::pg_key`. Returns
    /// (pool, pg_id) from "{pool}\0{pg_id:010}".
    fn parse_pg_key(key: &str) -> Option<(String, u32)> {
        let (pool, tail) = key.split_once('\0')?;
        let pg_id: u32 = tail.parse().ok()?;
        Some((pool.to_string(), pg_id))
    }

    /// Look up a placement group from the in-memory cache.
    pub fn placement_group(&self, pool: &str, pg_id: u32) -> Option<PlacementGroup> {
        self.placement_groups
            .read()
            .get(&(pool.to_string(), pg_id))
            .cloned()
    }

    /// Pre-allocate `pool.pg_count` placement groups using the
    /// copyset allocator. Called from `create_pool` once the pool
    /// row has been committed. Returns Ok(()) when every PG is
    /// written (or on the non-Raft fallback path). Commits in
    /// MultiCas batches of ≤128 ops to stay under the storage
    /// limit (see `raft_storage::MAX_OPS = 256`).
    async fn preallocate_placement_groups(&self, pool: &PoolConfig) -> Result<(), Status> {
        use objectio_common::FailureDomain;
        use objectio_placement::CopysetPool;

        let copy_count = match pool.ec_type() {
            ErasureType::ErasureMds => (pool.ec_k + pool.ec_m) as usize,
            ErasureType::ErasureLrc => {
                (pool.ec_k + pool.ec_local_parity + pool.ec_global_parity) as usize
            }
            ErasureType::ErasureReplication => pool.replication_count as usize,
        };
        if copy_count == 0 {
            return Err(Status::invalid_argument(
                "pool has zero shards per PG — check ec_k / ec_m / replication_count",
            ));
        }

        let fd_level = match pool.failure_domain.as_str() {
            "host" | "" => FailureDomain::Host,
            "node" => FailureDomain::Node,
            "rack" => FailureDomain::Rack,
            "datacenter" => FailureDomain::Datacenter,
            "zone" => FailureDomain::Zone,
            "region" => FailureDomain::Region,
            "disk" => FailureDomain::Disk,
            other => {
                return Err(Status::invalid_argument(format!(
                    "pool.failure_domain '{other}' not recognised"
                )));
            }
        };

        let topology = self.topology.read().clone();
        // Seed = topology.version × pg_count so concurrent pool creates
        // with the same topology get distinct pools.
        let seed = topology
            .version
            .wrapping_mul(1_000_003)
            .wrapping_add(u64::from(pool.pg_count));
        let cs_pool = CopysetPool::build(&topology, fd_level, copy_count, 10, seed).map_err(
            |e| Status::failed_precondition(format!("copyset pool build failed: {e}")),
        )?;
        if cs_pool.sets.is_empty() {
            return Err(Status::failed_precondition(
                "no feasible copysets for current topology",
            ));
        }

        let now = Self::current_timestamp();
        let mut pgs: Vec<PlacementGroup> = Vec::with_capacity(pool.pg_count as usize);
        for pg_id in 0..pool.pg_count {
            let cs = &cs_pool.sets[pg_id as usize % cs_pool.sets.len()];
            pgs.push(PlacementGroup {
                pool: pool.name.clone(),
                pg_id,
                osd_ids: cs.osds.iter().map(|n| n.as_bytes().to_vec()).collect(),
                version: 1,
                updated_at: now,
                migrating_to_osd_ids: Vec::new(),
                migration_started_at: 0,
                pending_moves_count: 0,
            });
        }

        // Commit in batches. MAX_OPS in raft_storage is 256 — stay
        // well under to leave headroom for other MultiCas calls that
        // land in the same raft entry.
        const CHUNK: usize = 128;
        let raft = self.raft_handle();
        for chunk in pgs.chunks(CHUNK) {
            if let Some(raft) = raft.clone() {
                use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
                let ops: Vec<CasOp> = chunk
                    .iter()
                    .map(|pg| CasOp {
                        table: CasTable::PlacementGroups,
                        key: MetaStore::pg_key(&pg.pool, pg.pg_id),
                        expected: None,
                        new_value: Some(pg.encode_to_vec()),
                    })
                    .collect();
                let cmd = MetaCommand::MultiCas {
                    ops,
                    requested_by: "create-pool:init-pgs".into(),
                };
                match raft.client_write(cmd).await {
                    Ok(r) => match r.data {
                        MetaResponse::MultiCasOk => {}
                        MetaResponse::MultiCasConflict { .. } => {
                            return Err(Status::aborted("PG conflict during pre-allocation"));
                        }
                        other => {
                            error!("unexpected raft response during PG pre-alloc: {other:?}");
                            return Err(Status::internal("raft commit wrong variant"));
                        }
                    },
                    Err(e) => return Err(raft_write_to_status(&e)),
                }
            } else if let Some(store) = &self.store {
                for pg in chunk {
                    store.put_placement_group(&pg.pool, pg.pg_id, &pg.encode_to_vec());
                }
            }
        }

        info!(
            "pool '{}' pre-allocated {} PGs (copy_count={}, fd={}, pool.size={})",
            pool.name,
            pool.pg_count,
            copy_count,
            fd_level,
            cs_pool.sets.len(),
        );
        Ok(())
    }

    fn apply_iceberg_warehouse_event(&self, key: &str, new_value: Option<&[u8]>) {
        use prost::Message;
        let mut wh = self.iceberg_warehouses.write();
        match new_value {
            Some(bytes) => match IcebergWarehouse::decode(bytes) {
                Ok(w) => {
                    wh.insert(key.to_string(), w);
                }
                Err(e) => warn!("apply: decode IcebergWarehouse('{key}') failed: {e}"),
            },
            None => {
                wh.remove(key);
            }
        }
    }

    /// True iff this replica is the current Raft leader. Used by
    /// leader-only background tasks (drain observer, future migrator)
    /// so every replica can run the task definition while only one
    /// actually mutates state.
    ///
    /// Returns `false` when Raft isn't wired (in-memory tests), which
    /// is the correct answer — there's no leader to issue writes to.
    #[must_use]
    pub fn is_raft_leader(&self) -> bool {
        self.raft_handle()
            .map(|r| {
                let m = r.metrics().borrow().clone();
                m.current_leader == Some(m.id)
            })
            .unwrap_or(false)
    }

    /// Read-only snapshot of the registered OSD list. Exposed for
    /// internal background tasks (drain observer) that need to
    /// iterate without acquiring the lock for an async scope.
    pub fn osd_nodes_read(&self) -> parking_lot::RwLockReadGuard<'_, Vec<OsdNode>> {
        self.osd_nodes.read()
    }

    /// Clone-snapshot of all pools. Background tasks use this instead
    /// of holding the lock across awaits.
    pub fn pools_snapshot(&self) -> Vec<PoolConfig> {
        self.pools.read().values().cloned().collect()
    }

    /// Clone-snapshot of all placement groups for a given pool.
    pub fn placement_groups_for_pool(&self, pool: &str) -> Vec<PlacementGroup> {
        let map = self.placement_groups.read();
        let mut pgs: Vec<PlacementGroup> = map
            .iter()
            .filter(|((p, _), _)| p == pool)
            .map(|(_, pg)| pg.clone())
            .collect();
        pgs.sort_by_key(|p| p.pg_id);
        pgs
    }

    /// Clone-snapshot of the cluster topology.
    pub fn topology_snapshot(&self) -> ClusterTopology {
        self.topology.read().clone()
    }

    /// Snapshot of all OSD drain progresses — node_id → progress.
    /// Consumed by the gateway's `/_admin/drain-status` endpoint.
    pub fn drain_statuses_snapshot(&self) -> HashMap<[u8; 16], DrainProgress> {
        self.drain_statuses.read().clone()
    }

    /// Mutate a single OSD's drain progress. Creates a default entry
    /// if absent. Background-task entrypoint — not exposed over gRPC.
    pub fn update_drain_progress<F: FnOnce(&mut DrainProgress)>(
        &self,
        node_id: [u8; 16],
        f: F,
    ) {
        let mut statuses = self.drain_statuses.write();
        let entry = statuses.entry(node_id).or_default();
        f(entry);
    }

    /// Remove a node from the drain-progress map — called once an OSD
    /// leaves the Draining state (either finalised to Out or rolled
    /// back to In).
    pub fn clear_drain_progress(&self, node_id: &[u8; 16]) {
        self.drain_statuses.write().remove(node_id);
    }

    /// Snapshot of the cluster-wide rebalance progress — consumed by
    /// `/_admin/rebalance-status`.
    pub fn rebalance_progress_snapshot(&self) -> RebalanceProgress {
        self.rebalance_progress.read().clone()
    }

    /// Mutate rebalance progress. Used by the reconciler sweep.
    pub fn update_rebalance_progress<F: FnOnce(&mut RebalanceProgress)>(
        &self,
        f: F,
    ) {
        let mut p = self.rebalance_progress.write();
        f(&mut p);
    }

    /// Is the rebalancer currently paused via the `rebalance/paused`
    /// config key? Checked on every reconciler sweep so paused state
    /// stays hot-swappable without restarting the process.
    pub fn is_rebalance_paused(&self) -> bool {
        // The config map mirrors what `set_config` has committed via
        // Raft; a bool cast from the stored "true"/"false" byte
        // string. Any parse error is treated as not-paused so a
        // garbled config can't lock the cluster into a no-rebalance
        // state.
        let cfg = self.config.read();
        cfg.get("rebalance/paused")
            .and_then(|e| std::str::from_utf8(&e.value).ok())
            .map(|s| s.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Look up the address of an OSD by its node_id. Used by the drain
    /// migrator to open client channels.
    pub fn osd_address_by_id(&self, node_id: &[u8; 16]) -> Option<String> {
        self.osd_nodes
            .read()
            .iter()
            .find(|n| &n.node_id == node_id)
            .map(|n| n.address.clone())
    }

    /// List addresses of every registered OSD (any admin_state).
    /// Drain migrator uses this to fan out the
    /// `FindObjectsReferencingNode` scan.
    pub fn all_osd_addresses(&self) -> Vec<(String, [u8; 16])> {
        self.osd_nodes
            .read()
            .iter()
            .map(|n| (n.address.clone(), n.node_id))
            .collect()
    }

    /// CRUSH's canonical top-pick for `(object_id, position)`, no
    /// exclusion applied. Used by the **rebalancer** to answer the
    /// single question that matters: *is the current owner the one
    /// CRUSH would pick today?* Comparing current == ideal is the
    /// drift check; if they differ, `ideal` is the migration target.
    ///
    /// Unlike `pick_migration_target`, we don't filter by "anything
    /// but X" — if we did, every successful rebalance would look like
    /// drift on the next sweep (current became the previous target,
    /// `exclude=current` would then pick a different node, and so on).
    ///
    /// Returns `None` if CRUSH can't place this position at all
    /// (cluster too small) or its pick isn't currently registered.
    pub fn crush_ideal_for_position(
        &self,
        object_id: &[u8; 16],
        position: u32,
    ) -> Option<[u8; 16]> {
        use objectio_placement::crush2::PlacementTemplate;

        let template =
            PlacementTemplate::mds(self.default_ec_k as u8, self.default_ec_m as u8);

        let crush = self.crush.read();
        let obj_id =
            objectio_common::ObjectId::from_uuid(uuid::Uuid::from_bytes(*object_id));
        let placements = crush.select_placement(&obj_id, &template);
        drop(crush);

        let registered: std::collections::HashSet<[u8; 16]> = self
            .osd_nodes
            .read()
            .iter()
            .map(|n| n.node_id)
            .collect();

        for p in &placements {
            if p.position as u32 == position {
                let cand = *p.node_id.as_bytes();
                if registered.contains(&cand) {
                    return Some(cand);
                }
            }
        }
        None
    }

    /// Compute a CRUSH replacement for a single shard at `position` in
    /// the placement set for `object_id`, excluding the `exclude` node
    /// (typically the draining / current-owner OSD). Only returns
    /// candidates that are currently **registered** on this meta —
    /// a stale topology entry for an unregistered node is skipped
    /// rather than returned, so the caller can rely on
    /// `osd_address_by_id(target)` succeeding.
    ///
    /// Returns `None` if CRUSH can't find a valid alternative (too few
    /// eligible OSDs, or every CRUSH-picked node is unregistered /
    /// the excluded one). Caller logs and retries next sweep.
    pub fn pick_migration_target(
        &self,
        object_id: &[u8; 16],
        position: u32,
        exclude: &[u8; 16],
    ) -> Option<[u8; 16]> {
        use objectio_placement::crush2::PlacementTemplate;

        let template =
            PlacementTemplate::mds(self.default_ec_k as u8, self.default_ec_m as u8);

        let crush = self.crush.read();
        let obj_id =
            objectio_common::ObjectId::from_uuid(uuid::Uuid::from_bytes(*object_id));
        let placements = crush.select_placement(&obj_id, &template);
        drop(crush);

        let registered: std::collections::HashSet<[u8; 16]> = self
            .osd_nodes
            .read()
            .iter()
            .map(|n| n.node_id)
            .collect();

        let eligible = |cand: &[u8; 16]| cand != exclude && registered.contains(cand);

        // Prefer the CRUSH pick for the exact stripe position. This
        // preserves the intended role (Data vs Parity) and keeps
        // placement deterministic for the other shards in the stripe.
        for p in &placements {
            if p.position as u32 == position {
                let cand = *p.node_id.as_bytes();
                if eligible(&cand) {
                    return Some(cand);
                }
            }
        }
        // Fallback: any CRUSH-eligible node in the returned set that
        // isn't excluded. Role becomes a soft hint — better than
        // failing the migration outright.
        placements
            .iter()
            .map(|p| *p.node_id.as_bytes())
            .find(eligible)
    }

    /// Invoke `SetOsdAdminState` from internal code (background tasks,
    /// not from an incoming RPC). Same Raft-routed path as the public
    /// gRPC handler; just skips the request-parsing / authz layer and
    /// always supplies `requested_by` so the audit log shows which
    /// subsystem triggered the change.
    ///
    /// # Errors
    /// Propagates any Raft `client_write` error (leader loss, timeout,
    /// shutdown).
    pub async fn internal_set_osd_admin_state(
        &self,
        node_id: [u8; 16],
        state: objectio_common::OsdAdminState,
        requested_by: String,
    ) -> anyhow::Result<()> {
        let raft = self
            .raft_handle()
            .ok_or_else(|| anyhow::anyhow!("raft handle unavailable"))?;
        raft.client_write(objectio_meta_store::MetaCommand::SetOsdAdminState {
            node_id,
            state,
            requested_by,
        })
        .await
        .map_err(|e| anyhow::anyhow!("client_write: {e}"))?;

        // Mirror into in-memory osd_nodes so the next get_listing_nodes
        // response reflects the new state immediately on this leader.
        let mut nodes = self.osd_nodes.write();
        if let Some(n) = nodes.iter_mut().find(|n| n.node_id == node_id) {
            n.admin_state = state;
        }
        // Rebuild placement topology to apply the change to CRUSH
        // without waiting for the next registration.
        let snapshot = nodes.clone();
        drop(nodes);
        for osd in &snapshot {
            self.update_topology_with_node(osd);
        }
        Ok(())
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
                // only the newest entry per address (last-wins). Also drop
                // known-bad placeholder addresses (e.g. http://0.0.0.0:9200)
                // that come from OSDs that never heartbeated a real
                // address — they pollute CRUSH and cause empty-address
                // write failures.
                let mut by_address: std::collections::HashMap<String, OsdNode> =
                    std::collections::HashMap::new();
                let mut bad_placeholder = 0usize;
                for (_hex_id, node) in nodes {
                    if node.address.is_empty()
                        || node.address.contains("://0.0.0.0")
                        || node.address.contains("://[::]")
                    {
                        bad_placeholder += 1;
                        continue;
                    }
                    by_address.insert(node.address.clone(), node);
                }
                let deduped: Vec<OsdNode> = by_address.into_values().collect();
                let evicted = count - deduped.len() - bad_placeholder;
                if evicted > 0 {
                    warn!(
                        "Deduped {} stale OSD entries on startup (same address, stale node_id)",
                        evicted
                    );
                }
                if bad_placeholder > 0 {
                    warn!(
                        "Dropped {} OSD entries with placeholder addresses on startup",
                        bad_placeholder
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

        // Topology — rebuild from the post-dedup OSD list so any ghost
        // node_ids that lingered in the stored topology (from pre-cleanup
        // binaries) don't come back into CRUSH. The stored topology is
        // an optimization; the authoritative source is the live OSD list.
        {
            let nodes = self.osd_nodes.read().clone();
            *self.topology.write() = Default::default();
            for node in &nodes {
                self.update_topology_with_node(node);
            }
            info!("Rebuilt topology from {} live OSD nodes", nodes.len());
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

        // Placement groups. Re-hydrate every PG across every pool so
        // the first GetPlacement doesn't do a redb read. The store API
        // is per-pool, so scan pools first; pools load above.
        {
            let pool_names: Vec<String> = self.pools.read().keys().cloned().collect();
            let mut map = self.placement_groups.write();
            let mut loaded = 0usize;
            for pool in &pool_names {
                let mut next_pg: u32 = 0;
                loop {
                    let rows = match store.list_placement_groups(pool, next_pg, 1000) {
                        Ok(r) => r,
                        Err(e) => {
                            error!("list placement_groups({pool}) failed: {e}");
                            break;
                        }
                    };
                    if rows.is_empty() {
                        break;
                    }
                    let mut highest = next_pg;
                    for bytes in rows {
                        match PlacementGroup::decode(bytes.as_slice()) {
                            Ok(pg) => {
                                highest = highest.max(pg.pg_id);
                                map.insert((pg.pool.clone(), pg.pg_id), pg);
                                loaded += 1;
                            }
                            Err(e) => error!("decode PlacementGroup: {e}"),
                        }
                    }
                    // list_placement_groups already advances past
                    // start_after_pg_id; step one past the highest we
                    // saw to paginate forward.
                    next_pg = highest.saturating_add(1);
                }
            }
            info!("Loaded {} placement groups from store", loaded);
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

        // Merge operator intent (admin_state) with observed status. An
        // OSD that's In is Active; Draining / Out map to the matching
        // NodeStatus so placement's `active_nodes()` filter skips them.
        let status = match osd_node.admin_state {
            objectio_common::OsdAdminState::In => NodeStatus::Active,
            objectio_common::OsdAdminState::Draining => NodeStatus::Draining,
            objectio_common::OsdAdminState::Out => NodeStatus::Decommissioning,
        };

        let node_info = NodeInfo {
            id: node_id,
            name: hex::encode(&osd_node.node_id[..4]),
            address: osd_node
                .address
                .parse()
                .unwrap_or_else(|_| "0.0.0.0:9200".parse().unwrap()),
            failure_domain: FailureDomainInfo::new_full(&region, &zone, &dc, &rack, &host),
            status,
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
            // Legacy path: no PG, pool blank. Phase 3 fills these.
            pg_id: 0,
            pg_version: 0,
            pool: String::new(),
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

        // Replicate through Raft so followers see the new bucket at the
        // same log position. Single-op MultiCas with expected=None enforces
        // "must-not-exist" at the state machine — if a concurrent proposal
        // on another pod raced us, the CAS fails and we surface it as
        // AlreadyExists (same error the in-memory precheck above returns).
        let bucket_bytes = bucket.encode_to_vec();
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Buckets,
                    key: req.name.clone(),
                    expected: None,
                    new_value: Some(bucket_bytes),
                }],
                requested_by: "create-bucket".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("bucket already exists"));
                    }
                    other => {
                        error!("unexpected raft response for create_bucket: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_bucket(&req.name, &bucket);
        }

        self.buckets
            .write()
            .insert(req.name.clone(), bucket.clone());

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

        // Read current bucket bytes so the CAS can detect a concurrent
        // mutation between now and commit.
        let current = {
            let b = self.buckets.read();
            b.get(&req.name)
                .cloned()
                .ok_or_else(|| Status::not_found("bucket not found"))?
        };
        let expected_bytes = current.encode_to_vec();

        // Note: The check for whether bucket is empty should be done by
        // the Gateway using scatter-gather before calling delete_bucket.

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Buckets,
                    key: req.name.clone(),
                    expected: Some(expected_bytes),
                    new_value: None, // delete
                }],
                requested_by: "delete-bucket".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "bucket changed since read; retry delete",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for delete_bucket: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_bucket(&req.name);
        }

        self.buckets.write().remove(&req.name);

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
    /// Register a PUT in the Meta-backed OBJECT_LISTINGS index. Every
    /// S3 PUT that succeeds at the data layer calls this to make the
    /// object visible via ListObjects. Routed through Raft MultiCas so
    /// followers see the commit at the same log position.
    async fn create_object(
        &self,
        request: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>, Status> {
        let req = request.into_inner();
        if req.bucket.is_empty() || req.key.is_empty() {
            return Err(Status::invalid_argument("bucket and key required"));
        }

        // Build the listing entry. primary_osd_id is optional (the
        // first shard in the first stripe, as a routing hint).
        let primary_osd_id = req
            .stripes
            .first()
            .and_then(|s| s.shards.first())
            .map(|s| s.node_id.clone())
            .unwrap_or_default();
        let now = Self::current_timestamp();
        let entry = ObjectListingEntry {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            size: req.size,
            etag: req.etag.clone(),
            content_type: req.content_type.clone(),
            created_at: now,
            modified_at: now,
            version_id: String::new(),
            is_delete_marker: false,
            storage_class: "STANDARD".into(),
            user_metadata: req.user_metadata.clone(),
            primary_osd_id,
            // Gateway carried these from its GetPlacement call. With
            // pg_id set, ListObjects + GET can resolve osd_ids via a
            // single PG lookup; without, we fall back to the legacy
            // per-object CRUSH path.
            pg_id: req.pg_id,
            pool: req.pool.clone(),
        };
        let listing_key = format!("{}\0{}\0", req.bucket, req.key);
        let new_bytes = entry.encode_to_vec();

        // Idempotent overwrite: PUT on an existing key replaces. Read
        // current (if any) so the MultiCas doesn't spuriously fail.
        let expected_bytes = self
            .store
            .as_ref()
            .and_then(|s| s.read_object_listing(&listing_key));

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::ObjectListings,
                    key: listing_key.clone(),
                    expected: expected_bytes,
                    new_value: Some(new_bytes),
                }],
                requested_by: "create-object".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "listing changed during PUT; client should retry",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for create_object: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_object_listing(&listing_key, &entry.encode_to_vec());
        }

        Ok(Response::new(CreateObjectResponse { object: None }))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let req = request.into_inner();
        let listing_key = format!("{}\0{}\0{}", req.bucket, req.key, req.version_id);
        let expected_bytes = self
            .store
            .as_ref()
            .and_then(|s| s.read_object_listing(&listing_key));
        if expected_bytes.is_none() {
            // Nothing to remove — return success idempotently.
            return Ok(Response::new(DeleteObjectResponse {
                success: true,
                version_id: req.version_id,
            }));
        }

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::ObjectListings,
                    key: listing_key,
                    expected: expected_bytes,
                    new_value: None,
                }],
                requested_by: "delete-object".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("listing changed during DELETE; retry"));
                    }
                    other => {
                        error!("unexpected raft response for delete_object: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            // Legacy non-raft path: direct redb delete.
            store.delete_object_listing(&format!("{}\0{}\0{}", req.bucket, req.key, req.version_id));
        }

        Ok(Response::new(DeleteObjectResponse {
            success: true,
            version_id: req.version_id,
        }))
    }

    /// Single-object read — not in the common path (gateway goes to
    /// OSDs for ObjectMeta), kept so admin tools can look up metadata
    /// by (bucket, key).
    async fn get_object(
        &self,
        _request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        Err(Status::unimplemented(
            "Object metadata lives on OSDs — use GetObjectMeta. ObjectListings only stores the listing hint.",
        ))
    }

    /// Linearizable listing via a B-tree scan of OBJECT_LISTINGS in
    /// Meta's redb. Replaces the old scatter-gather-then-merge path
    /// on the gateway. Continuation token is the bucket-relative
    /// form of the last key returned.
    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let req = request.into_inner();
        if req.bucket.is_empty() {
            return Err(Status::invalid_argument("bucket required"));
        }
        let max_keys = if req.max_keys == 0 {
            1000
        } else {
            req.max_keys.min(1000) as usize
        };
        let start_after = if !req.continuation_token.is_empty() {
            req.continuation_token.clone()
        } else {
            req.start_after.clone()
        };

        let Some(store) = &self.store else {
            // No persistent store = no Raft backend — return empty.
            return Ok(Response::new(ListObjectsResponse::default()));
        };
        let (rows, is_truncated, next_token) = store
            .list_object_listings(&req.bucket, &req.prefix, &start_after, max_keys)
            .map_err(|e| {
                error!("list_object_listings failed: {e}");
                Status::internal(format!("list failed: {e}"))
            })?;

        let mut entries = Vec::with_capacity(rows.len());
        for (_k, bytes) in rows {
            match <ObjectListingEntry as prost::Message>::decode(bytes.as_slice()) {
                Ok(e) => entries.push(e),
                Err(err) => {
                    warn!("decode ObjectListingEntry failed: {err}");
                }
            }
        }

        // Common prefixes (delimiter handling) — keep the existing
        // shape the gateway expects. Our store scan returns fully
        // expanded keys; applying the delimiter here keeps the client
        // contract stable across the migration.
        let mut common_prefixes: Vec<String> = Vec::new();
        if !req.delimiter.is_empty() {
            use std::collections::BTreeSet;
            let mut prefixes: BTreeSet<String> = BTreeSet::new();
            entries.retain(|e| {
                let key = &e.key;
                if let Some(tail) = key.strip_prefix(&req.prefix)
                    && let Some(idx) = tail.find(&req.delimiter)
                {
                    let end = req.prefix.len() + idx + req.delimiter.len();
                    prefixes.insert(key[..end].to_string());
                    return false;
                }
                true
            });
            common_prefixes = prefixes.into_iter().collect();
        }

        let key_count = entries.len() as u32 + common_prefixes.len() as u32;
        Ok(Response::new(ListObjectsResponse {
            objects: Vec::new(),
            common_prefixes,
            next_continuation_token: next_token,
            is_truncated,
            key_count,
            entries,
        }))
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
        let (pool_ec, pool_pg_count) = if !pool_name.is_empty() {
            self.pools
                .read()
                .get(&pool_name)
                .map(|p| {
                    (
                        Some((
                            p.ec_type(),
                            p.ec_k,
                            p.ec_m,
                            p.ec_local_parity,
                            p.ec_global_parity,
                            p.replication_count,
                        )),
                        p.pg_count,
                    )
                })
                .unwrap_or((None, 0))
        } else {
            (None, 0)
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

        // Placement-group fast path. When the bucket's pool has a
        // non-zero pg_count we route object_id -> pg_id via jump
        // consistent hash and read the PG's committed osd_ids in one
        // in-memory lookup. Falls through to CRUSH2 if the PG row is
        // missing (pre-allocation still in progress on a fresh pool)
        // or if the PG's shard count disagrees with the current EC
        // config (topology mid-reconfigure).
        if pool_pg_count > 0 && !pool_name.is_empty() {
            let key_str = format!("{}/{}", req.bucket, req.key);
            let key_hash = xxhash_rust::xxh64::xxh64(key_str.as_bytes(), 0);
            let pg_id = objectio_placement::jump_consistent_hash(key_hash, pool_pg_count as i32)
                as u32;
            if let Some(pg) = self.placement_group(&pool_name, pg_id) {
                let expected_shards = match ec_type {
                    ErasureType::ErasureMds => ec_k as usize + ec_global_parity as usize,
                    ErasureType::ErasureLrc => {
                        ec_k as usize + ec_local_parity as usize + ec_global_parity as usize
                    }
                    ErasureType::ErasureReplication => replication_count as usize,
                };
                if pg.osd_ids.len() == expected_shards && expected_shards > 0 {
                    let nodes_snap = self.osd_nodes.read();
                    let placements: Vec<NodePlacement> = pg
                        .osd_ids
                        .iter()
                        .enumerate()
                        .map(|(pos, osd_bytes)| {
                            let node = nodes_snap.iter().find(|n| n.node_id.as_slice() == osd_bytes.as_slice());
                            let (node_address, disk_id) = match node {
                                Some(n) => (
                                    n.address.clone(),
                                    n.disk_ids
                                        .first()
                                        .map(|d| d.to_vec())
                                        .unwrap_or_else(|| vec![0u8; 16]),
                                ),
                                None => (String::new(), vec![0u8; 16]),
                            };
                            let shard_type = pg_position_shard_type(
                                ec_type,
                                pos,
                                ec_k as usize,
                                ec_local_parity as usize,
                                local_group_size as usize,
                            );
                            let local_group = pg_position_local_group(
                                ec_type,
                                pos,
                                ec_k as usize,
                                ec_local_parity as usize,
                                local_group_size as usize,
                            );
                            NodePlacement {
                                position: pos as u32,
                                node_id: osd_bytes.clone(),
                                node_address,
                                disk_id,
                                shard_type: shard_type.into(),
                                local_group,
                            }
                        })
                        .collect();
                    debug!(
                        "PG placement for {}/{}: pool={}, pg_id={}, {} shards",
                        req.bucket,
                        req.key,
                        pool_name,
                        pg_id,
                        placements.len()
                    );
                    return Ok(Response::new(GetPlacementResponse {
                        storage_class: req.storage_class.clone(),
                        ec_k,
                        ec_m: ec_local_parity + ec_global_parity,
                        nodes: placements,
                        ec_type: ec_type.into(),
                        ec_local_parity,
                        ec_global_parity,
                        local_group_size,
                        replication_count,
                        pg_id,
                        pg_version: pg.version,
                        pool: pool_name.clone(),
                    }));
                }
                warn!(
                    "PG {}/{}: osd_ids={} doesn't match expected shards={}; falling back to CRUSH",
                    pool_name,
                    pg_id,
                    pg.osd_ids.len(),
                    expected_shards
                );
            }
        }

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
            // Filled by Phase 3 once the PG lookup replaces
            // per-object CRUSH. Leaving zeros keeps pre-migration
            // clients safe (gateway treats 0 as legacy).
            pg_id: 0,
            pg_version: 0,
            pool: String::new(),
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

        // CAS against whatever is currently stored so a concurrent update
        // from another pod doesn't silently overwrite. Racing admin
        // operations retry from the handler.
        let expected = self
            .bucket_policies
            .read()
            .get(&req.bucket)
            .map(|v| v.as_bytes().to_vec());
        let new_value = Some(req.policy_json.as_bytes().to_vec());

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::BucketPolicies,
                    key: req.bucket.clone(),
                    expected,
                    new_value,
                }],
                requested_by: "set-bucket-policy".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "bucket policy changed since read; retry",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for set_bucket_policy: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_bucket_policy(&req.bucket, &req.policy_json);
        }

        self.bucket_policies
            .write()
            .insert(req.bucket.clone(), req.policy_json.clone());

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

        let expected = self
            .bucket_policies
            .read()
            .get(&req.bucket)
            .map(|v| v.as_bytes().to_vec());

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::BucketPolicies,
                    key: req.bucket.clone(),
                    expected,
                    new_value: None, // delete
                }],
                requested_by: "delete-bucket-policy".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "bucket policy changed since read; retry",
                        ));
                    }
                    other => {
                        error!(
                            "unexpected raft response for delete_bucket_policy: {:?}",
                            other
                        );
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_bucket_policy(&req.bucket);
        }

        self.bucket_policies.write().remove(&req.bucket);

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
        // Preserve operator intent across re-registrations: if the OSD
        // was marked Out or Draining and the same node_id (or address)
        // re-registers, keep it out of placement until an admin
        // explicitly flips it back to In.
        let prev_admin_state = {
            let nodes = self.osd_nodes.read();
            nodes
                .iter()
                .find(|n| n.node_id == node_id || n.address == req.address)
                .map(|n| n.admin_state)
                .unwrap_or_default()
        };
        let node = OsdNode {
            node_id,
            address: req.address.clone(),
            disk_ids,
            failure_domain: legacy_fd,
            topology: topology_tuple,
            disk_capacity_bytes,
            admin_state: prev_admin_state,
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

        // Add (or refresh) THIS OSD in the topology. Without this, a
        // freshly-registered OSD whose state PVC was wiped — so it comes
        // back with a new node_id — never joins the CRUSH placement set,
        // because the old node_id was evicted but the new one was never
        // inserted. Symptom on the cluster: writes and rebalance both
        // skip the OSD forever, its shard count stays at 0. Update the
        // topology now so `active_nodes()` sees the new node_id right
        // away; the CRUSH engine gets rebuilt inside
        // `update_topology_with_node`.
        self.update_topology_with_node(&node);

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
        request: Request<GetListingNodesRequest>,
    ) -> Result<Response<GetListingNodesResponse>, Status> {
        let req = request.into_inner();
        let topology = self.topology.read();
        let osd_nodes = self.osd_nodes.read();

        // Helper — map internal admin state → proto enum value.
        let admin_state_proto = |s: objectio_common::OsdAdminState| -> i32 {
            match s {
                objectio_common::OsdAdminState::In => {
                    objectio_proto::metadata::OsdAdminState::OsdAdminIn as i32
                }
                objectio_common::OsdAdminState::Out => {
                    objectio_proto::metadata::OsdAdminState::OsdAdminOut as i32
                }
                objectio_common::OsdAdminState::Draining => {
                    objectio_proto::metadata::OsdAdminState::OsdAdminDraining as i32
                }
            }
        };

        // When include_all_states is true, admin UI wants EVERY OSD
        // including Draining / Out / Decommissioning. Scatter-gather
        // callers (the default) only want Active ones.
        let topology_iter: Box<dyn Iterator<Item = &objectio_placement::topology::NodeInfo>> =
            if req.include_all_states {
                Box::new(topology.all_nodes())
            } else {
                Box::new(topology.active_nodes())
            };

        // Build lookups keyed by node_id:
        //   admin_state_by_id — operator intent (In/Out/Draining)
        //   address_by_id    — the real OSD endpoint string. The placement
        //                       topology's `address` field is a SocketAddr
        //                       which can't represent DNS names (e.g.
        //                       "http://objectio-osd-3.objectio-osd-headless:9200")
        //                       and falls back to "0.0.0.0:9200". If we
        //                       used that, every node would collapse to
        //                       the same address and the gateway's
        //                       address-based dedup would reduce the
        //                       whole cluster to a single row.
        let admin_state_by_id: std::collections::HashMap<[u8; 16], objectio_common::OsdAdminState> =
            osd_nodes
                .iter()
                .map(|n| (n.node_id, n.admin_state))
                .collect();
        let address_by_id: std::collections::HashMap<[u8; 16], String> = osd_nodes
            .iter()
            .map(|n| (n.node_id, n.address.clone()))
            .collect();

        let mut nodes: Vec<ListingNode> = topology_iter
            .enumerate()
            .map(|(idx, node)| {
                let id_bytes = *node.id.as_bytes();
                let admin_state = admin_state_by_id
                    .get(&id_bytes)
                    .copied()
                    .unwrap_or_default();
                // Prefer the registered DNS form; fall back to the
                // topology's parsed SocketAddr only when the OSD isn't
                // in osd_nodes (shouldn't happen in practice).
                let address = address_by_id
                    .get(&id_bytes)
                    .cloned()
                    .unwrap_or_else(|| format!("http://{}", node.address));
                ListingNode {
                    node_id: id_bytes.to_vec(),
                    address,
                    shard_id: idx as u32, // Assign logical shard IDs in order
                    failure_domain: Some(objectio_proto::metadata::FailureDomainInfo {
                        region: node.failure_domain.region.clone(),
                        datacenter: node.failure_domain.datacenter.clone(),
                        rack: node.failure_domain.rack.clone(),
                        zone: node.failure_domain.zone.clone(),
                        host: node.failure_domain.host.clone(),
                    }),
                    admin_state: admin_state_proto(admin_state),
                }
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
                        admin_state: admin_state_proto(node.admin_state),
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

        // Replicate through Raft. expected=None ensures the user_id
        // hasn't collided with a concurrent create (cryptographically
        // unlikely for UUIDs, but tested correctly by the state machine).
        let user_bytes = bincode::serialize(&user)
            .map_err(|e| Status::internal(format!("user encode: {e}")))?;
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Users,
                    key: user_id.clone(),
                    expected: None,
                    new_value: Some(user_bytes),
                }],
                requested_by: "create-user".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists(
                            "user_id collision (retry with fresh id)",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for create_user: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_user(&user_id, &user);
        }

        self.users.write().insert(user_id.clone(), user.clone());
        self.user_keys.write().insert(user_id.clone(), Vec::new());

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

        // Snapshot current user + access-key state under read locks so we
        // can build the CAS batch atomically. The user's status flips to
        // Deleted; every owned access key flips to Inactive — all in one
        // MultiCas so followers see the compound change at the same log
        // position (can't observe "user deleted but keys still active").
        let (old_user_bytes, new_user_bytes, user_snapshot) = {
            let users = self.users.read();
            let user = users
                .get(&req.user_id)
                .ok_or_else(|| Status::not_found("user not found"))?;
            let mut new_user = user.clone();
            new_user.status = UserStatus::UserDeleted as i32;
            let old_bytes = bincode::serialize(user)
                .map_err(|e| Status::internal(format!("user encode: {e}")))?;
            let new_bytes = bincode::serialize(&new_user)
                .map_err(|e| Status::internal(format!("user encode: {e}")))?;
            (old_bytes, new_bytes, new_user)
        };

        let key_ids: Vec<String> = self
            .user_keys
            .read()
            .get(&req.user_id)
            .cloned()
            .unwrap_or_default();

        // Build per-key (old_bytes, new_bytes) transitions. Keys that
        // aren't found in the access_keys map are silently skipped (stale
        // entry in user_keys index).
        let mut key_transitions: Vec<(String, Vec<u8>, Vec<u8>, StoredAccessKey)> =
            Vec::with_capacity(key_ids.len());
        {
            let keys = self.access_keys.read();
            for key_id in &key_ids {
                if let Some(key) = keys.get(key_id) {
                    let mut new_key = key.clone();
                    new_key.status = KeyStatus::KeyInactive as i32;
                    let old_b = bincode::serialize(key)
                        .map_err(|e| Status::internal(format!("key encode: {e}")))?;
                    let new_b = bincode::serialize(&new_key)
                        .map_err(|e| Status::internal(format!("key encode: {e}")))?;
                    key_transitions.push((key_id.clone(), old_b, new_b, new_key));
                }
            }
        }

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let mut ops = Vec::with_capacity(1 + key_transitions.len());
            ops.push(CasOp {
                table: CasTable::Users,
                key: req.user_id.clone(),
                expected: Some(old_user_bytes),
                new_value: Some(new_user_bytes),
            });
            for (kid, old_b, new_b, _) in &key_transitions {
                ops.push(CasOp {
                    table: CasTable::AccessKeys,
                    key: kid.clone(),
                    expected: Some(old_b.clone()),
                    new_value: Some(new_b.clone()),
                });
            }
            let cmd = MetaCommand::MultiCas {
                ops,
                requested_by: "delete-user".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { failed_indices } => {
                        return Err(Status::aborted(format!(
                            "user or access-key changed mid-delete; retry (conflicts at ops {failed_indices:?})"
                        )));
                    }
                    other => {
                        error!("unexpected raft response for delete_user: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_user(&req.user_id, &user_snapshot);
            for (kid, _, _, new_key) in &key_transitions {
                store.put_access_key(kid, new_key);
            }
        }

        // Mirror into in-memory caches after the quorum commit.
        self.users
            .write()
            .insert(req.user_id.clone(), user_snapshot);
        {
            let mut keys = self.access_keys.write();
            for (kid, _, _, new_key) in key_transitions {
                keys.insert(kid, new_key);
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

        let key_bytes = bincode::serialize(&key)
            .map_err(|e| Status::internal(format!("access key encode: {e}")))?;
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    // No enum variant yet for access_keys — use Named
                    // escape hatch. Switching to a dedicated CasTable
                    // variant later is additive.
                    table: CasTable::AccessKeys,
                    key: access_key_id.clone(),
                    expected: None,
                    new_value: Some(key_bytes),
                }],
                requested_by: "create-access-key".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists(
                            "access key id collision (retry with fresh id)",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for create_access_key: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_access_key(&access_key_id, &key);
        }

        self.access_keys
            .write()
            .insert(access_key_id.clone(), key.clone());
        self.user_keys
            .write()
            .entry(req.user_id.clone())
            .or_default()
            .push(access_key_id.clone());

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

        let current = self
            .access_keys
            .read()
            .get(&req.access_key_id)
            .cloned()
            .ok_or_else(|| Status::not_found("access key not found"))?;
        let expected_bytes = bincode::serialize(&current)
            .map_err(|e| Status::internal(format!("access key encode: {e}")))?;

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::AccessKeys,
                    key: req.access_key_id.clone(),
                    expected: Some(expected_bytes),
                    new_value: None, // delete
                }],
                requested_by: "delete-access-key".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "access key changed since read; retry delete",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for delete_access_key: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_access_key(&req.access_key_id);
        }

        self.access_keys.write().remove(&req.access_key_id);
        if let Some(keys) = self.user_keys.write().get_mut(&current.user_id) {
            keys.retain(|id| id != &req.access_key_id);
        }

        info!("Deleted access key: {}", req.access_key_id);
        Ok(Response::new(DeleteAccessKeyResponse { success: true }))
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
        let resp = IcebergCreateNamespaceResponse {
            namespace_levels: req.namespace_levels.clone(),
            properties: properties.clone(),
        };
        let new_bytes = resp.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergNamespaces,
                    key: ns_key.clone(),
                    expected: None,
                    new_value: Some(new_bytes),
                }],
                requested_by: "iceberg-create-namespace".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("namespace already exists"));
                    }
                    other => {
                        error!("unexpected raft response for create_namespace: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_iceberg_namespace(&ns_key, &resp.encode_to_vec());
        }

        self.iceberg_namespaces
            .write()
            .insert(ns_key.clone(), properties.clone());

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

        // Reconstruct the expected stored bytes from the in-memory
        // properties (prost is deterministic on the same struct shape).
        let expected_bytes = {
            let ns_map = self.iceberg_namespaces.read();
            let properties = ns_map
                .get(&ns_key)
                .cloned()
                .ok_or_else(|| Status::not_found("namespace not found"))?;
            let stored = IcebergCreateNamespaceResponse {
                namespace_levels: req.namespace_levels.clone(),
                properties,
            };
            stored.encode_to_vec()
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergNamespaces,
                    key: ns_key.clone(),
                    expected: Some(expected_bytes),
                    new_value: None,
                }],
                requested_by: "iceberg-drop-namespace".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "namespace changed since read; retry drop",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for drop_namespace: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_iceberg_namespace(&ns_key);
        }

        self.iceberg_namespaces.write().remove(&ns_key);

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

        // Compute the before/after snapshot under a read lock so the
        // CAS can roll back cleanly on a concurrent mutation.
        let (expected_bytes, new_bytes, new_properties, updated, removed, missing) = {
            let ns_map = self.iceberg_namespaces.read();
            let current = ns_map
                .get(&ns_key)
                .cloned()
                .ok_or_else(|| Status::not_found("namespace not found"))?;

            let expected = IcebergCreateNamespaceResponse {
                namespace_levels: req.namespace_levels.clone(),
                properties: current.clone(),
            }
            .encode_to_vec();

            let mut new_props = current;
            let mut updated = Vec::new();
            let mut removed = Vec::new();
            let mut missing = Vec::new();
            for key in &req.removals {
                if new_props.remove(key).is_some() {
                    removed.push(key.clone());
                } else {
                    missing.push(key.clone());
                }
            }
            for (key, value) in &req.updates {
                new_props.insert(key.clone(), value.clone());
                updated.push(key.clone());
            }
            let new_bytes = IcebergCreateNamespaceResponse {
                namespace_levels: req.namespace_levels.clone(),
                properties: new_props.clone(),
            }
            .encode_to_vec();
            (expected, new_bytes, new_props, updated, removed, missing)
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergNamespaces,
                    key: ns_key.clone(),
                    expected: Some(expected_bytes),
                    new_value: Some(new_bytes),
                }],
                requested_by: "iceberg-update-namespace-properties".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "namespace properties changed since read; retry",
                        ));
                    }
                    other => {
                        error!(
                            "unexpected raft response for update_namespace_properties: {:?}",
                            other
                        );
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            let resp = IcebergCreateNamespaceResponse {
                namespace_levels: req.namespace_levels.clone(),
                properties: new_properties.clone(),
            };
            store.put_iceberg_namespace(&ns_key, &resp.encode_to_vec());
        }

        self.iceberg_namespaces
            .write()
            .insert(ns_key.clone(), new_properties);

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
        let new_bytes = entry.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergTables,
                    key: table_key.clone(),
                    expected: None,
                    new_value: Some(new_bytes),
                }],
                requested_by: "iceberg-create-table".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("table already exists"));
                    }
                    other => {
                        error!("unexpected raft response for create_table: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_iceberg_table(&table_key, &entry.encode_to_vec());
        }

        self.iceberg_tables
            .write()
            .insert(table_key.clone(), entry.clone());

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

        // Stage 1 — read the current entry, validate the expected metadata
        // location, and encode the proposed new entry. Held behind a read
        // lock so concurrent non-conflicting RPCs on other tables aren't
        // serialized against this one.
        let (old_bytes, new_entry, new_bytes) = {
            let tables = self.iceberg_tables.read();
            let entry = tables
                .get(&table_key)
                .ok_or_else(|| Status::not_found("table not found"))?;

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
            let old_bytes = entry.encode_to_vec();
            let new_bytes = new_entry.encode_to_vec();
            (old_bytes, new_entry, new_bytes)
        };

        // Stage 2 — replicate the CAS through Raft so followers observe
        // the same commit. Non-leader pods return a Forwarding error that
        // `raft_write_to_status` turns into a leader-hint Status; the
        // iceberg REST handler retries against the leader.
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergTables,
                    key: table_key.clone(),
                    expected: Some(old_bytes),
                    new_value: Some(new_bytes),
                }],
                requested_by: "iceberg-commit".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::failed_precondition(
                            "concurrent metadata update detected",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for iceberg commit: {:?}", other);
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            // Legacy non-Raft path (tests, pre-Raft deployments). Keep the
            // old direct-redb CAS so unit tests without a raft handle
            // still work.
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

        // Stage 3 — mirror into the in-memory cache on this leader so
        // local reads see the update without waiting for a redb hit.
        // Followers' caches are rebuilt from redb on next leader promote
        // (the state machine apply already landed the bytes on disk).
        self.iceberg_tables
            .write()
            .insert(table_key.clone(), new_entry);

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

    async fn iceberg_commit_transaction(
        &self,
        request: Request<IcebergCommitTransactionRequest>,
    ) -> Result<Response<IcebergCommitTransactionResponse>, Status> {
        let req = request.into_inner();
        if req.table_changes.is_empty() {
            return Err(Status::invalid_argument("table_changes is empty"));
        }

        // Stage 1 — validate every change's expected location and encode the
        // new entries. Held behind a read-lock so concurrent single-table
        // commits on other tables aren't serialized behind this one. A
        // failed expected location aborts the whole transaction before
        // any Raft round-trip.
        struct Prepared {
            table_key: String,
            old_bytes: Vec<u8>,
            new_entry: IcebergTableEntry,
            new_bytes: Vec<u8>,
            resp: IcebergCommitTableResponse,
        }
        let prepared: Vec<Prepared> = {
            let tables = self.iceberg_tables.read();
            let now = Self::current_timestamp();
            let mut prepared = Vec::with_capacity(req.table_changes.len());
            for (i, ch) in req.table_changes.iter().enumerate() {
                let table_key = Self::iceberg_table_key_wh(
                    &req.warehouse,
                    &ch.namespace_levels,
                    &ch.table_name,
                );
                let entry = tables
                    .get(&table_key)
                    .ok_or_else(|| Status::not_found(format!("table_changes[{i}]: not found")))?;
                if entry.metadata_location != ch.current_metadata_location {
                    return Err(Status::failed_precondition(format!(
                        "table_changes[{i}]: metadata location mismatch: expected '{}', actual '{}'",
                        ch.current_metadata_location, entry.metadata_location
                    )));
                }
                let new_entry = IcebergTableEntry {
                    metadata_location: ch.new_metadata_location.clone(),
                    created_at: entry.created_at,
                    updated_at: now,
                    metadata_json: ch.new_metadata_json.clone(),
                    policy_json: entry.policy_json.clone(),
                };
                let old_bytes = entry.encode_to_vec();
                let new_bytes = new_entry.encode_to_vec();
                let resp = IcebergCommitTableResponse {
                    metadata_location: ch.new_metadata_location.clone(),
                    metadata_json: ch.new_metadata_json.clone(),
                };
                prepared.push(Prepared {
                    table_key,
                    old_bytes,
                    new_entry,
                    new_bytes,
                    resp,
                });
            }
            prepared
        };

        // Stage 2 — one Raft MultiCas for the whole batch. All ops land
        // atomically or none do; a stale expected on any row rolls the
        // whole transaction back.
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let ops: Vec<CasOp> = prepared
                .iter()
                .map(|p| CasOp {
                    table: CasTable::IcebergTables,
                    key: p.table_key.clone(),
                    expected: Some(p.old_bytes.clone()),
                    new_value: Some(p.new_bytes.clone()),
                })
                .collect();
            let cmd = MetaCommand::MultiCas {
                ops,
                requested_by: "iceberg-transaction".into(),
            };
            match raft.client_write(cmd).await {
                Ok(resp) => match resp.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { failed_indices } => {
                        return Err(Status::failed_precondition(format!(
                            "concurrent metadata update detected on table_changes {failed_indices:?}"
                        )));
                    }
                    other => {
                        error!(
                            "unexpected raft response for iceberg transaction: {:?}",
                            other
                        );
                        return Err(Status::internal("raft commit returned wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            // Non-Raft test path: emulate atomicity via the store's
            // multi-key CAS helper (one redb write-txn across all ops).
            // Not replicated; production deployments always take the
            // Raft branch above.
            let ops: Vec<(String, Vec<u8>, Vec<u8>)> = prepared
                .iter()
                .map(|p| (p.table_key.clone(), p.old_bytes.clone(), p.new_bytes.clone()))
                .collect();
            match store.cas_iceberg_tables_multi(&ops) {
                Ok(failed) if failed.is_empty() => {}
                Ok(failed) => {
                    return Err(Status::failed_precondition(format!(
                        "concurrent metadata update detected on table_changes {failed:?}"
                    )));
                }
                Err(e) => {
                    error!("cas_iceberg_tables_multi failed: {e}");
                    return Err(Status::internal("failed to commit transaction"));
                }
            }
        }

        // Stage 3 — mirror every successful commit into the in-memory
        // cache on this leader. `committed` vector mirrors the request
        // `table_changes` order so the caller can match results 1:1.
        let mut committed = Vec::with_capacity(prepared.len());
        {
            let mut tables = self.iceberg_tables.write();
            for p in prepared {
                tables.insert(p.table_key.clone(), p.new_entry);
                committed.push(p.resp);
            }
        }

        debug!(
            "Committed iceberg transaction: warehouse={} changes={}",
            req.warehouse,
            committed.len()
        );

        Ok(Response::new(IcebergCommitTransactionResponse { committed }))
    }

    async fn iceberg_drop_table(
        &self,
        request: Request<IcebergDropTableRequest>,
    ) -> Result<Response<IcebergDropTableResponse>, Status> {
        let req = request.into_inner();
        let table_key =
            Self::iceberg_table_key_wh(&req.warehouse, &req.namespace_levels, &req.table_name);

        let expected_bytes = {
            let tables = self.iceberg_tables.read();
            tables
                .get(&table_key)
                .cloned()
                .ok_or_else(|| Status::not_found("table not found"))?
                .encode_to_vec()
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IcebergTables,
                    key: table_key.clone(),
                    expected: Some(expected_bytes),
                    new_value: None,
                }],
                requested_by: "iceberg-drop-table".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "table changed since read; retry drop",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for drop_table: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_iceberg_table(&table_key);
        }

        self.iceberg_tables.write().remove(&table_key);

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

        // Rename is two ops in one atomic MultiCas: delete src + insert
        // dst. If either side conflicts the whole rename aborts.
        let (expected_src_bytes, entry_bytes, entry) = {
            let tables = self.iceberg_tables.read();
            let entry = tables
                .get(&src_key)
                .cloned()
                .ok_or_else(|| Status::not_found("source table not found"))?;
            if tables.contains_key(&dst_key) {
                return Err(Status::already_exists("destination table already exists"));
            }
            let bytes = entry.encode_to_vec();
            (bytes.clone(), bytes, entry)
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![
                    CasOp {
                        table: CasTable::IcebergTables,
                        key: src_key.clone(),
                        expected: Some(expected_src_bytes),
                        new_value: None,
                    },
                    CasOp {
                        table: CasTable::IcebergTables,
                        key: dst_key.clone(),
                        expected: None,
                        new_value: Some(entry_bytes),
                    },
                ],
                requested_by: "iceberg-rename-table".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { failed_indices } => {
                        return Err(Status::aborted(format!(
                            "rename conflict at ops {failed_indices:?}; retry"
                        )));
                    }
                    other => {
                        error!("unexpected raft response for rename_table: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_iceberg_table(&src_key);
            store.put_iceberg_table(&dst_key, &entry.encode_to_vec());
        }

        {
            let mut tables = self.iceberg_tables.write();
            tables.remove(&src_key);
            tables.insert(dst_key.clone(), entry);
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
        let group_bytes = bincode::serialize(&group)
            .map_err(|e| Status::internal(format!("group encode: {e}")))?;

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Groups,
                    key: group_id.clone(),
                    expected: None,
                    new_value: Some(group_bytes),
                }],
                requested_by: "create-group".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists(
                            "group_id collision (retry with fresh id)",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for create_group: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_group(&group_id, &group);
        }

        self.groups.write().insert(group_id.clone(), group.clone());
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
        let expected_bytes = {
            let groups = self.groups.read();
            let g = groups
                .get(&req.group_id)
                .ok_or_else(|| Status::not_found("group not found"))?;
            bincode::serialize(g)
                .map_err(|e| Status::internal(format!("group encode: {e}")))?
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Groups,
                    key: req.group_id.clone(),
                    expected: Some(expected_bytes),
                    new_value: None,
                }],
                requested_by: "delete-group".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("group changed since read; retry delete"));
                    }
                    other => {
                        error!("unexpected raft response for delete_group: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_group(&req.group_id);
        }

        self.groups.write().remove(&req.group_id);
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

        if !self.users.read().contains_key(&req.user_id) {
            return Err(Status::not_found("user not found"));
        }

        let (expected_bytes, new_group) = {
            let groups = self.groups.read();
            let current = groups
                .get(&req.group_id)
                .cloned()
                .ok_or_else(|| Status::not_found("group not found"))?;
            if current.member_user_ids.contains(&req.user_id) {
                return Err(Status::already_exists("user already in group"));
            }
            let expected = bincode::serialize(&current)
                .map_err(|e| Status::internal(format!("group encode: {e}")))?;
            let mut new_group = current;
            new_group.member_user_ids.push(req.user_id.clone());
            (expected, new_group)
        };
        let new_bytes = bincode::serialize(&new_group)
            .map_err(|e| Status::internal(format!("group encode: {e}")))?;

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Groups,
                    key: req.group_id.clone(),
                    expected: Some(expected_bytes),
                    new_value: Some(new_bytes),
                }],
                requested_by: "add-user-to-group".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("group changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for add_user_to_group: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_group(&req.group_id, &new_group);
        }

        self.groups
            .write()
            .insert(req.group_id.clone(), new_group);
        info!("Added user {} to group {}", req.user_id, req.group_id);
        Ok(Response::new(AddUserToGroupResponse { success: true }))
    }

    async fn remove_user_from_group(
        &self,
        request: Request<RemoveUserFromGroupRequest>,
    ) -> Result<Response<RemoveUserFromGroupResponse>, Status> {
        let req = request.into_inner();

        let (expected_bytes, new_group) = {
            let groups = self.groups.read();
            let current = groups
                .get(&req.group_id)
                .cloned()
                .ok_or_else(|| Status::not_found("group not found"))?;
            if !current.member_user_ids.contains(&req.user_id) {
                return Err(Status::not_found("user not in group"));
            }
            let expected = bincode::serialize(&current)
                .map_err(|e| Status::internal(format!("group encode: {e}")))?;
            let mut new_group = current;
            new_group.member_user_ids.retain(|id| id != &req.user_id);
            (expected, new_group)
        };
        let new_bytes = bincode::serialize(&new_group)
            .map_err(|e| Status::internal(format!("group encode: {e}")))?;

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Groups,
                    key: req.group_id.clone(),
                    expected: Some(expected_bytes),
                    new_value: Some(new_bytes),
                }],
                requested_by: "remove-user-from-group".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("group changed since read; retry"));
                    }
                    other => {
                        error!(
                            "unexpected raft response for remove_user_from_group: {:?}",
                            other
                        );
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_group(&req.group_id, &new_group);
        }

        self.groups
            .write()
            .insert(req.group_id.clone(), new_group);
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

        let bytes = bincode::serialize(&filter)
            .map_err(|e| Status::internal(format!("data_filter encode: {e}")))?;
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::DataFilters,
                    key: filter_id.clone(),
                    expected: None,
                    new_value: Some(bytes),
                }],
                requested_by: "create-data-filter".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("filter_id collision"));
                    }
                    other => {
                        error!("unexpected raft response for create_data_filter: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_data_filter(&filter_id, &filter);
        }
        self.data_filters
            .write()
            .insert(filter_id.clone(), filter.clone());

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
        let expected = {
            let filters = self.data_filters.read();
            let Some(f) = filters.get(&req.filter_id) else {
                return Ok(Response::new(DeleteDataFilterResponse { success: false }));
            };
            bincode::serialize(f)
                .map_err(|e| Status::internal(format!("data_filter encode: {e}")))?
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::DataFilters,
                    key: req.filter_id.clone(),
                    expected: Some(expected),
                    new_value: None,
                }],
                requested_by: "delete-data-filter".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("filter changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for delete_data_filter: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_data_filter(&req.filter_id);
        }

        self.data_filters.write().remove(&req.filter_id);
        Ok(Response::new(DeleteDataFilterResponse { success: true }))
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
        if self.delta_shares.read().contains_key(&req.name) {
            return Err(Status::already_exists("share already exists"));
        }
        let now = Self::current_timestamp();
        let entry = DeltaShareEntry {
            name: req.name.clone(),
            comment: req.comment,
            created_at: now as i64,
            tenant: req.tenant,
        };
        let bytes = entry.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::DeltaShares,
                    key: req.name.clone(),
                    expected: None,
                    new_value: Some(bytes),
                }],
                requested_by: "delta-create-share".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("share already exists"));
                    }
                    other => {
                        error!("unexpected raft response for delta_create_share: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_delta_share(&req.name, &entry.encode_to_vec());
        }

        self.delta_shares
            .write()
            .insert(req.name.clone(), entry.clone());
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
        // Multi-op MultiCas: the share row + every DeltaShareTableEntry
        // whose key prefix matches. All removed atomically — no orphan
        // table rows pointing at a dropped share.
        let share_prefix = format!("{}\x00", req.name);
        let (expected_share_bytes, table_deletes) = {
            let shares = self.delta_shares.read();
            let Some(share) = shares.get(&req.name) else {
                return Ok(Response::new(DeltaDropShareResponse { success: false }));
            };
            let share_bytes = share.encode_to_vec();
            let tables = self.delta_tables.read();
            let deletes: Vec<(String, Vec<u8>)> = tables
                .iter()
                .filter(|(k, _)| k.starts_with(&share_prefix))
                .map(|(k, v)| (k.clone(), v.encode_to_vec()))
                .collect();
            (share_bytes, deletes)
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let mut ops = Vec::with_capacity(1 + table_deletes.len());
            ops.push(CasOp {
                table: CasTable::DeltaShares,
                key: req.name.clone(),
                expected: Some(expected_share_bytes),
                new_value: None,
            });
            for (tk, tb) in &table_deletes {
                ops.push(CasOp {
                    table: CasTable::DeltaTables,
                    key: tk.clone(),
                    expected: Some(tb.clone()),
                    new_value: None,
                });
            }
            let cmd = MetaCommand::MultiCas {
                ops,
                requested_by: "delta-drop-share".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { failed_indices } => {
                        return Err(Status::aborted(format!(
                            "share or table changed mid-drop; retry (conflicts at {failed_indices:?})"
                        )));
                    }
                    other => {
                        error!("unexpected raft response for delta_drop_share: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_delta_share(&req.name);
            for (tk, _) in &table_deletes {
                store.delete_delta_table(tk);
            }
        }

        self.delta_shares.write().remove(&req.name);
        self.delta_tables
            .write()
            .retain(|k, _| !k.starts_with(&share_prefix));
        info!("Dropped Delta share: {}", req.name);
        Ok(Response::new(DeltaDropShareResponse { success: true }))
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
        let bytes = entry.encode_to_vec();
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::DeltaTables,
                    key: table_key.clone(),
                    expected: None,
                    new_value: Some(bytes),
                }],
                requested_by: "delta-add-table".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("table already in share"));
                    }
                    other => {
                        error!("unexpected raft response for delta_add_table: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_delta_table(&table_key, &entry.encode_to_vec());
        }

        self.delta_tables
            .write()
            .insert(table_key.clone(), entry.clone());
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
        let expected = self
            .delta_tables
            .read()
            .get(&table_key)
            .map(|v| v.encode_to_vec());
        if expected.is_none() {
            return Ok(Response::new(DeltaRemoveTableResponse { success: false }));
        }

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::DeltaTables,
                    key: table_key.clone(),
                    expected,
                    new_value: None,
                }],
                requested_by: "delta-remove-table".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("table changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for delta_remove_table: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_delta_table(&table_key);
        }

        self.delta_tables.write().remove(&table_key);
        Ok(Response::new(DeltaRemoveTableResponse { success: true }))
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

        if self.delta_recipients.read().contains_key(&req.name) {
            return Err(Status::already_exists("recipient already exists"));
        }
        let bytes = entry.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::DeltaRecipients,
                    key: req.name.clone(),
                    expected: None,
                    new_value: Some(bytes),
                }],
                requested_by: "delta-create-recipient".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("recipient already exists"));
                    }
                    other => {
                        error!("unexpected raft response for delta_create_recipient: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_delta_recipient(&req.name, &entry.encode_to_vec());
        }

        self.delta_recipients
            .write()
            .insert(req.name.clone(), entry.clone());
        self.delta_token_index
            .write()
            .insert(token_hash.clone(), req.name.clone());
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
        let (expected, token_hash) = {
            let recipients = self.delta_recipients.read();
            let Some(entry) = recipients.get(&req.name) else {
                return Ok(Response::new(DeltaDropRecipientResponse { success: false }));
            };
            (entry.encode_to_vec(), entry.token_hash.clone())
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::DeltaRecipients,
                    key: req.name.clone(),
                    expected: Some(expected),
                    new_value: None,
                }],
                requested_by: "delta-drop-recipient".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("recipient changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for delta_drop_recipient: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_delta_recipient(&req.name);
        }

        self.delta_recipients.write().remove(&req.name);
        self.delta_token_index.write().remove(&token_hash);
        info!("Dropped Delta recipient: {}", req.name);
        Ok(Response::new(DeltaDropRecipientResponse { success: true }))
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

        // Consensus path: if Raft is wired, every config write has to
        // commit through the log. Non-leader nodes reject with a leader
        // hint so the client (gateway) can retry against the right pod.
        if let Some(raft) = self.raft_handle() {
            match raft
                .client_write(objectio_meta_store::MetaCommand::SetConfig {
                    key: req.key.clone(),
                    value: req.value.clone(),
                    updated_by: req.updated_by.clone(),
                })
                .await
            {
                Ok(resp) => {
                    // The state machine wrote to the CONFIG redb table
                    // with a monotonic version inside apply. Mirror that
                    // entry into the in-memory map on the leader so
                    // local reads see the new value without a redb hit.
                    // Followers pick it up via apply on their own copy,
                    // but their in-memory map is not updated until R1's
                    // follow-up adds an apply listener.
                    let version = match resp.data {
                        objectio_meta_store::MetaResponse::ConfigSet { version } => version,
                        _ => 0,
                    };
                    let now = Self::current_timestamp();
                    let entry = ConfigEntry {
                        key: req.key.clone(),
                        value: req.value.clone(),
                        updated_at: now,
                        updated_by: req.updated_by.clone(),
                        version,
                    };
                    self.config.write().insert(req.key.clone(), entry.clone());
                    self.config_version
                        .store(version, std::sync::atomic::Ordering::SeqCst);

                    // License hot-swap mirrors the pre-Raft behavior:
                    // applying a new `license/active` value refreshes the
                    // in-memory license used by `register_osd` caps.
                    if req.key == "license/active" {
                        let now_secs = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        match objectio_license::License::load_from_bytes(
                            &entry.value,
                            now_secs,
                        ) {
                            Ok(l) => {
                                info!(
                                    "meta license reloaded: tier={} licensee={} max_nodes={} max_raw_capacity_bytes={}",
                                    l.tier,
                                    l.licensee,
                                    l.max_nodes,
                                    l.max_raw_capacity_bytes
                                );
                                *self.license.write() = Arc::new(l);
                            }
                            Err(e) => {
                                warn!("license/active rejected on set_config: {}", e);
                            }
                        }
                    }

                    info!(
                        "Config set via Raft: key={} version={} log_id={:?}",
                        req.key, version, resp.log_id
                    );
                    return Ok(Response::new(SetConfigResponse { entry: Some(entry) }));
                }
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        }

        // Legacy direct-redb path — tests without a Raft handle fall
        // through here; production deployments always have Raft set.
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

        if let Some(store) = &self.store {
            store.put_config(&req.key, &entry.encode_to_vec());
        }
        self.config.write().insert(req.key.clone(), entry.clone());

        if req.key == "license/active" {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            match objectio_license::License::load_from_bytes(&entry.value, now_secs) {
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
        info!("Config set (legacy path): key={}, version={}", req.key, version);
        Ok(Response::new(SetConfigResponse { entry: Some(entry) }))
    }

    async fn delete_config(
        &self,
        request: Request<DeleteConfigRequest>,
    ) -> Result<Response<DeleteConfigResponse>, Status> {
        let req = request.into_inner();

        if let Some(raft) = self.raft_handle() {
            match raft
                .client_write(objectio_meta_store::MetaCommand::DeleteConfig {
                    key: req.key.clone(),
                })
                .await
            {
                Ok(resp) => {
                    let existed = matches!(
                        resp.data,
                        objectio_meta_store::MetaResponse::ConfigDeleted { existed: true }
                    );
                    if existed {
                        self.config.write().remove(&req.key);
                        if req.key == "license/active" {
                            info!("meta license removed — reverting to Community tier");
                            *self.license.write() =
                                Arc::new(objectio_license::License::community());
                        }
                        info!(
                            "Config deleted via Raft: key={} log_id={:?}",
                            req.key, resp.log_id
                        );
                    }
                    return Ok(Response::new(DeleteConfigResponse { success: existed }));
                }
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        }

        // Legacy direct path.
        let removed = self.config.write().remove(&req.key).is_some();
        if removed {
            if let Some(store) = &self.store {
                store.delete_config(&req.key);
            }
            if req.key == "license/active" {
                info!("meta license removed — reverting to Community tier");
                *self.license.write() = Arc::new(objectio_license::License::community());
            }
            info!("Config deleted (legacy path): key={}", req.key);
        }

        Ok(Response::new(DeleteConfigResponse { success: removed }))
    }

    async fn set_osd_admin_state(
        &self,
        request: Request<SetOsdAdminStateRequest>,
    ) -> Result<Response<SetOsdAdminStateResponse>, Status> {
        let req = request.into_inner();

        // node_id must be 16 bytes (UUID).
        let node_id: [u8; 16] = req.node_id.as_slice().try_into().map_err(|_| {
            Status::invalid_argument("node_id must be 16 bytes")
        })?;

        // Map wire enum → internal enum. Proto's numeric `i32` reaches us
        // here; rely on the generated accessor to handle unknown values.
        // prost prefixes the proto enum variants with the enum name; map
        // them back to our internal OsdAdminState.
        let state = match objectio_proto::metadata::OsdAdminState::try_from(req.state) {
            Ok(objectio_proto::metadata::OsdAdminState::OsdAdminIn) => {
                objectio_common::OsdAdminState::In
            }
            Ok(objectio_proto::metadata::OsdAdminState::OsdAdminOut) => {
                objectio_common::OsdAdminState::Out
            }
            Ok(objectio_proto::metadata::OsdAdminState::OsdAdminDraining) => {
                objectio_common::OsdAdminState::Draining
            }
            Err(_) => {
                return Err(Status::invalid_argument(format!(
                    "unknown OsdAdminState: {}",
                    req.state
                )));
            }
        };

        let requested_by = if req.requested_by.is_empty() {
            "meta".to_string()
        } else {
            req.requested_by.clone()
        };

        // Raft is the only write path. set_osd_admin_state persists to
        // OSD_NODES inside apply, which every follower also observes.
        let raft = self.raft_handle().ok_or_else(|| {
            Status::failed_precondition(
                "raft is not initialized — run POST /init on meta admin port",
            )
        })?;

        let resp = raft
            .client_write(objectio_meta_store::MetaCommand::SetOsdAdminState {
                node_id,
                state,
                requested_by,
            })
            .await
            .map_err(|e| raft_write_to_status(&e))?;

        let (found, changed) = match resp.data {
            objectio_meta_store::MetaResponse::OsdAdminStateSet { found, changed } => {
                (found, changed)
            }
            _ => (false, false),
        };

        // Mirror the change into the in-memory OsdNode list so this
        // process's topology rebuild sees the new state without a redb
        // re-read. (Followers do this via their own apply — deferred
        // until the apply-listener lands in a later phase.)
        if found && changed {
            let mut nodes = self.osd_nodes.write();
            if let Some(n) = nodes.iter_mut().find(|n| n.node_id == node_id) {
                n.admin_state = state;
            }
        }

        // Rebuild the placement topology so the next `place_object`
        // call respects the new state immediately.
        if found && changed {
            let snapshot = self.osd_nodes.read().clone();
            for osd in &snapshot {
                self.update_topology_with_node(osd);
            }
            info!(
                "OSD {} admin_state → {} (via Raft, log_id={:?})",
                hex::encode(node_id),
                state.as_str(),
                resp.log_id
            );
        } else if !found {
            warn!(
                "set_osd_admin_state: no OSD with node_id={}",
                hex::encode(node_id)
            );
        }

        Ok(Response::new(SetOsdAdminStateResponse {
            found,
            changed,
            effective: req.state,
        }))
    }

    async fn get_drain_status(
        &self,
        _request: Request<GetDrainStatusRequest>,
    ) -> Result<Response<GetDrainStatusResponse>, Status> {
        let snapshot = self.drain_statuses_snapshot();
        let drains: Vec<ProtoDrainStatus> = snapshot
            .into_iter()
            .map(|(node_id, p)| ProtoDrainStatus {
                node_id: node_id.to_vec(),
                shards_remaining: p.shards_remaining,
                initial_shards: p.initial_shards,
                shards_migrated: p.shards_migrated,
                updated_at: p.updated_at,
                last_error: p.last_error,
            })
            .collect();
        Ok(Response::new(GetDrainStatusResponse { drains }))
    }

    async fn get_rebalance_status(
        &self,
        _request: Request<GetRebalanceStatusRequest>,
    ) -> Result<Response<GetRebalanceStatusResponse>, Status> {
        let p = self.rebalance_progress_snapshot();
        // `paused` is sourced from the live config each request; the
        // cached field is kept for the reconciler's fast path.
        let paused = self.is_rebalance_paused();
        Ok(Response::new(GetRebalanceStatusResponse {
            started: p.started,
            paused,
            last_sweep_at: p.last_sweep_at,
            scanned_this_pass: p.scanned_this_pass,
            drifts_seen_this_pass: p.drifts_seen_this_pass,
            shards_rebalanced_total: p.shards_rebalanced_total,
            last_error: p.last_error,
        }))
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
        if self.pools.read().contains_key(&pool.name) {
            return Err(Status::already_exists(format!(
                "pool '{}' already exists",
                pool.name
            )));
        }
        let mut pool = pool;
        pool.created_at = Self::current_timestamp();
        pool.updated_at = pool.created_at;
        let bytes = pool.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Pools,
                    key: pool.name.clone(),
                    expected: None,
                    new_value: Some(bytes),
                }],
                requested_by: "create-pool".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("pool already exists"));
                    }
                    other => {
                        error!("unexpected raft response for create_pool: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_pool(&pool.name, &pool.encode_to_vec());
        }

        self.pools.write().insert(pool.name.clone(), pool.clone());
        info!("Created pool: {}", pool.name);

        // Pre-allocate placement groups if the pool opted in. Done
        // after the pool row is committed so a partial failure here
        // leaves a pool with pg_count>0 but no PGs — the balancer
        // (Phase 4) will detect that and regenerate. Fatal errors
        // from allocation surface as status; gateway retries.
        if pool.pg_count > 0
            && let Err(e) = self.preallocate_placement_groups(&pool).await
        {
            warn!(
                "pool '{}' created but PG pre-allocation failed: {}",
                pool.name, e
            );
        }

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
        let mut pool = request
            .into_inner()
            .pool
            .ok_or_else(|| Status::invalid_argument("missing pool"))?;
        let expected_bytes = {
            let pools = self.pools.read();
            pools
                .get(&pool.name)
                .ok_or_else(|| Status::not_found(format!("pool '{}' not found", pool.name)))?
                .encode_to_vec()
        };
        pool.updated_at = Self::current_timestamp();
        let new_bytes = pool.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Pools,
                    key: pool.name.clone(),
                    expected: Some(expected_bytes),
                    new_value: Some(new_bytes),
                }],
                requested_by: "update-pool".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("pool changed since read; retry update"));
                    }
                    other => {
                        error!("unexpected raft response for update_pool: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_pool(&pool.name, &pool.encode_to_vec());
        }

        self.pools.write().insert(pool.name.clone(), pool.clone());
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
        let expected_bytes = self.pools.read().get(&name).map(|p| p.encode_to_vec());
        if expected_bytes.is_none() {
            return Ok(Response::new(DeletePoolResponse { success: false }));
        }

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Pools,
                    key: name.clone(),
                    expected: expected_bytes,
                    new_value: None,
                }],
                requested_by: "delete-pool".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("pool changed since read; retry delete"));
                    }
                    other => {
                        error!("unexpected raft response for delete_pool: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_pool(&name);
        }

        self.pools.write().remove(&name);
        info!("Deleted pool: {}", name);
        Ok(Response::new(DeletePoolResponse { success: true }))
    }

    // ============ Placement groups ============
    //
    // Balancer owns writes (via CasTable::PlacementGroups MultiCas).
    // Gateway only reads, so only Get + List are exposed for now.

    async fn get_placement_group(
        &self,
        request: Request<GetPlacementGroupRequest>,
    ) -> Result<Response<GetPlacementGroupResponse>, Status> {
        let req = request.into_inner();
        let pg = self.placement_group(&req.pool, req.pg_id);
        let found = pg.is_some();
        Ok(Response::new(GetPlacementGroupResponse { pg, found }))
    }

    async fn list_placement_groups(
        &self,
        request: Request<ListPlacementGroupsRequest>,
    ) -> Result<Response<ListPlacementGroupsResponse>, Status> {
        let req = request.into_inner();
        let max = if req.max_results == 0 {
            1000usize
        } else {
            (req.max_results as usize).min(10_000)
        };
        let map = self.placement_groups.read();
        let mut pgs: Vec<PlacementGroup> = map
            .iter()
            .filter(|((p, id), _)| p == &req.pool && *id > req.start_after_pg_id)
            .map(|(_, v)| v.clone())
            .collect();
        pgs.sort_by_key(|p| p.pg_id);
        let truncated = pgs.len() > max;
        pgs.truncate(max);
        let next_pg_id = if truncated {
            pgs.last().map(|p| p.pg_id).unwrap_or(0)
        } else {
            0
        };
        Ok(Response::new(ListPlacementGroupsResponse { pgs, next_pg_id }))
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
        if self.tenants.read().contains_key(&tenant.name) {
            return Err(Status::already_exists(format!(
                "tenant '{}' already exists",
                tenant.name
            )));
        }
        let mut tenant = tenant;
        tenant.created_at = Self::current_timestamp();
        tenant.updated_at = tenant.created_at;
        let bytes = tenant.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Tenants,
                    key: tenant.name.clone(),
                    expected: None,
                    new_value: Some(bytes),
                }],
                requested_by: "create-tenant".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("tenant already exists"));
                    }
                    other => {
                        error!("unexpected raft response for create_tenant: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_tenant(&tenant.name, &tenant.encode_to_vec());
        }

        self.tenants
            .write()
            .insert(tenant.name.clone(), tenant.clone());
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
        let mut tenant = request
            .into_inner()
            .tenant
            .ok_or_else(|| Status::invalid_argument("missing tenant"))?;
        let expected_bytes = {
            let tenants = self.tenants.read();
            tenants
                .get(&tenant.name)
                .ok_or_else(|| {
                    Status::not_found(format!("tenant '{}' not found", tenant.name))
                })?
                .encode_to_vec()
        };
        tenant.updated_at = Self::current_timestamp();
        let new_bytes = tenant.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Tenants,
                    key: tenant.name.clone(),
                    expected: Some(expected_bytes),
                    new_value: Some(new_bytes),
                }],
                requested_by: "update-tenant".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("tenant changed since read; retry update"));
                    }
                    other => {
                        error!("unexpected raft response for update_tenant: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_tenant(&tenant.name, &tenant.encode_to_vec());
        }

        self.tenants
            .write()
            .insert(tenant.name.clone(), tenant.clone());
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
        let has_buckets = self.buckets.read().values().any(|b| b.tenant == name);
        if has_buckets {
            return Err(Status::failed_precondition(format!(
                "tenant '{}' still has buckets — delete them first",
                name
            )));
        }
        let expected_bytes = self
            .tenants
            .read()
            .get(&name)
            .map(|t| t.encode_to_vec());
        if expected_bytes.is_none() {
            return Ok(Response::new(DeleteTenantResponse { success: false }));
        }

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Tenants,
                    key: name.clone(),
                    expected: expected_bytes,
                    new_value: None,
                }],
                requested_by: "delete-tenant".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("tenant changed since read; retry delete"));
                    }
                    other => {
                        error!("unexpected raft response for delete_tenant: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_tenant(&name);
        }

        self.tenants.write().remove(&name);
        info!("Deleted tenant: {}", name);
        Ok(Response::new(DeleteTenantResponse { success: true }))
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
        let bucket_bytes = bucket.encode_to_vec();

        let warehouse = IcebergWarehouse {
            name: req.name.clone(),
            bucket: bucket_name.clone(),
            location,
            tenant: req.tenant.clone(),
            created_at: now,
            properties: req.properties.clone(),
        };
        let warehouse_bytes = warehouse.encode_to_vec();

        // Two-op atomic MultiCas: warehouse row + backing bucket. If
        // either side conflicts the whole creation aborts, avoiding
        // the half-state where a warehouse exists without its bucket
        // (or a lingering orphan bucket from a failed warehouse create).
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![
                    CasOp {
                        table: CasTable::IcebergWarehouses,
                        key: req.name.clone(),
                        expected: None,
                        new_value: Some(warehouse_bytes),
                    },
                    CasOp {
                        table: CasTable::Buckets,
                        key: bucket_name.clone(),
                        expected: None,
                        new_value: Some(bucket_bytes),
                    },
                ],
                requested_by: "iceberg-create-warehouse".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { failed_indices } => {
                        return Err(Status::already_exists(format!(
                            "warehouse or backing bucket already exists (conflicts at {failed_indices:?})"
                        )));
                    }
                    other => {
                        error!("unexpected raft response for create_warehouse: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_bucket(&bucket_name, &bucket);
            store.put_warehouse(&req.name, &warehouse_bytes);
        }

        self.buckets
            .write()
            .insert(bucket_name.clone(), bucket.clone());
        self.iceberg_warehouses
            .write()
            .insert(req.name.clone(), warehouse.clone());

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

        let (wh, wh_bytes, bucket_bytes) = {
            let warehouses = self.iceberg_warehouses.read();
            let wh = warehouses
                .get(&name)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("warehouse '{}' not found", name)))?;
            let wh_bytes = wh.encode_to_vec();
            let bucket_bytes = self
                .buckets
                .read()
                .get(&wh.bucket)
                .map(|b| b.encode_to_vec());
            (wh, wh_bytes, bucket_bytes)
        };

        // Atomic dual delete: warehouse row + backing bucket. If the
        // bucket has already been removed separately, skip its op so
        // we don't spuriously fail on expected=Some but actual=None.
        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let mut ops = vec![CasOp {
                table: CasTable::IcebergWarehouses,
                key: name.clone(),
                expected: Some(wh_bytes),
                new_value: None,
            }];
            if let Some(bucket_bytes) = bucket_bytes {
                ops.push(CasOp {
                    table: CasTable::Buckets,
                    key: wh.bucket.clone(),
                    expected: Some(bucket_bytes),
                    new_value: None,
                });
            }
            let cmd = MetaCommand::MultiCas {
                ops,
                requested_by: "iceberg-delete-warehouse".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted(
                            "warehouse or bucket changed since read; retry delete",
                        ));
                    }
                    other => {
                        error!("unexpected raft response for delete_warehouse: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_warehouse(&name);
            store.delete_bucket(&wh.bucket);
        }

        self.iceberg_warehouses.write().remove(&name);
        self.buckets.write().remove(&wh.bucket);

        info!("Deleted warehouse: {} (bucket: {})", name, wh.bucket);
        Ok(Response::new(IcebergDeleteWarehouseResponse {
            success: true,
        }))
    }

    // Bucket Versioning
    // ============================================================

    async fn put_bucket_versioning(
        &self,
        request: Request<PutBucketVersioningRequest>,
    ) -> Result<Response<PutBucketVersioningResponse>, Status> {
        let req = request.into_inner();

        // Object-locked buckets cannot have versioning suspended.
        if req.state() == VersioningState::VersioningSuspended {
            let lock_configs = self.object_lock_configs.read();
            if lock_configs.get(&req.bucket).is_some_and(|c| c.enabled) {
                return Err(Status::failed_precondition(
                    "cannot suspend versioning on object-locked bucket",
                ));
            }
        }

        let (expected_bytes, new_bucket, new_bytes) = {
            let buckets = self.buckets.read();
            let current = buckets
                .get(&req.bucket)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("bucket '{}' not found", req.bucket)))?;
            let expected = current.encode_to_vec();
            let mut new_bucket = current;
            new_bucket.versioning = req.state;
            let new_bytes = new_bucket.encode_to_vec();
            (expected, new_bucket, new_bytes)
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Buckets,
                    key: req.bucket.clone(),
                    expected: Some(expected_bytes),
                    new_value: Some(new_bytes),
                }],
                requested_by: "put-bucket-versioning".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("bucket changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for put_bucket_versioning: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_bucket(&req.bucket, &new_bucket);
        }

        self.buckets.write().insert(req.bucket.clone(), new_bucket);
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
        let expected = self
            .object_lock_configs
            .read()
            .get(&req.bucket)
            .map(|c| c.encode_to_vec());

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Named("object_lock_configs".into()),
                    key: req.bucket.clone(),
                    expected,
                    new_value: Some(bytes.clone()),
                }],
                requested_by: "put-object-lock-config".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("object lock config changed; retry"));
                    }
                    other => {
                        error!("unexpected raft response for put_object_lock_config: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_object_lock_config(&req.bucket, &bytes);
        }

        self.object_lock_configs
            .write()
            .insert(req.bucket.clone(), config);
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
        let expected = self
            .lifecycle_configs
            .read()
            .get(&req.bucket)
            .map(|c| c.encode_to_vec());

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Named("lifecycle_configs".into()),
                    key: req.bucket.clone(),
                    expected,
                    new_value: Some(bytes.clone()),
                }],
                requested_by: "put-bucket-lifecycle".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("lifecycle config changed; retry"));
                    }
                    other => {
                        error!("unexpected raft response for put_bucket_lifecycle: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_lifecycle_config(&req.bucket, &bytes);
        }

        self.lifecycle_configs
            .write()
            .insert(req.bucket.clone(), config);
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
        let expected = self
            .lifecycle_configs
            .read()
            .get(&bucket)
            .map(|c| c.encode_to_vec());
        if expected.is_none() {
            return Ok(Response::new(DeleteBucketLifecycleResponse {
                success: false,
            }));
        }

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Named("lifecycle_configs".into()),
                    key: bucket.clone(),
                    expected,
                    new_value: None,
                }],
                requested_by: "delete-bucket-lifecycle".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("lifecycle changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for delete_bucket_lifecycle: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_lifecycle_config(&bucket);
        }

        self.lifecycle_configs.write().remove(&bucket);
        info!("Deleted lifecycle config for bucket '{}'", bucket);
        Ok(Response::new(DeleteBucketLifecycleResponse { success: true }))
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
        let expected = self
            .bucket_encryption_configs
            .read()
            .get(&req.bucket)
            .map(|c| c.encode_to_vec());

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Named("bucket_encryption_configs".into()),
                    key: req.bucket.clone(),
                    expected,
                    new_value: Some(bytes.clone()),
                }],
                requested_by: "put-bucket-encryption".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("encryption config changed; retry"));
                    }
                    other => {
                        error!("unexpected raft response for put_bucket_encryption: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_bucket_encryption_config(&req.bucket, &bytes);
        }

        self.bucket_encryption_configs
            .write()
            .insert(req.bucket.clone(), config);
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
        let expected = self
            .bucket_encryption_configs
            .read()
            .get(&bucket)
            .map(|c| c.encode_to_vec());
        if expected.is_none() {
            return Ok(Response::new(DeleteBucketEncryptionResponse {
                success: false,
            }));
        }

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::Named("bucket_encryption_configs".into()),
                    key: bucket.clone(),
                    expected,
                    new_value: None,
                }],
                requested_by: "delete-bucket-encryption".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("encryption config changed; retry"));
                    }
                    other => {
                        error!("unexpected raft response for delete_bucket_encryption: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_bucket_encryption_config(&bucket);
        }

        self.bucket_encryption_configs.write().remove(&bucket);
        info!("Deleted bucket encryption config for bucket '{}'", bucket);
        Ok(Response::new(DeleteBucketEncryptionResponse { success: true }))
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

        if self.iam_policies.read().contains_key(&name) {
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
        let bytes = policy.encode_to_vec();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::IamPolicies,
                    key: name.clone(),
                    expected: None,
                    new_value: Some(bytes),
                }],
                requested_by: "create-iam-policy".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::already_exists("policy already exists"));
                    }
                    other => {
                        error!("unexpected raft response for create_policy: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_iam_policy(&name, &policy.encode_to_vec());
        }

        self.iam_policies
            .write()
            .insert(name.clone(), policy.clone());
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
        // Snapshot current state: policy row + every attachment row that
        // references this policy. The whole mutation lands as one atomic
        // MultiCas — a crash mid-delete can't leave orphan attachments.
        let (expected_policy_bytes, attachment_transitions) = {
            let policies = self.iam_policies.read();
            let Some(current) = policies.get(&name) else {
                return Ok(Response::new(DeletePolicyResponse { success: false }));
            };
            let expected = current.encode_to_vec();
            let mut transitions: Vec<(String, Vec<u8>, Option<Vec<u8>>, Vec<String>)> = Vec::new();
            let atts = self.policy_attachments.read();
            for (key, policy_names) in atts.iter() {
                if policy_names.contains(&name) {
                    let before = policy_names.clone();
                    let after: Vec<String> = policy_names
                        .iter()
                        .filter(|p| *p != &name)
                        .cloned()
                        .collect();
                    let old_bytes = before.join(",").into_bytes();
                    let new_bytes = if after.is_empty() {
                        None
                    } else {
                        Some(after.join(",").into_bytes())
                    };
                    transitions.push((key.clone(), old_bytes, new_bytes, after));
                }
            }
            (expected, transitions)
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let mut ops = Vec::with_capacity(1 + attachment_transitions.len());
            ops.push(CasOp {
                table: CasTable::IamPolicies,
                key: name.clone(),
                expected: Some(expected_policy_bytes),
                new_value: None,
            });
            for (key, old, new, _) in &attachment_transitions {
                ops.push(CasOp {
                    table: CasTable::PolicyAttachments,
                    key: key.clone(),
                    expected: Some(old.clone()),
                    new_value: new.clone(),
                });
            }
            let cmd = MetaCommand::MultiCas {
                ops,
                requested_by: "delete-iam-policy".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { failed_indices } => {
                        return Err(Status::aborted(format!(
                            "policy or attachment changed mid-delete; retry (conflicts at ops {failed_indices:?})"
                        )));
                    }
                    other => {
                        error!("unexpected raft response for delete_policy: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.delete_iam_policy(&name);
            for (key, _, new, after) in &attachment_transitions {
                if new.is_some() {
                    store.put_policy_attachment(key, &after.join(","));
                } else {
                    store.delete_policy_attachment(key);
                }
            }
        }

        // Mirror into in-memory caches.
        self.iam_policies.write().remove(&name);
        {
            let mut atts = self.policy_attachments.write();
            for (key, _, _, after) in attachment_transitions {
                if after.is_empty() {
                    atts.remove(&key);
                } else {
                    atts.insert(key, after);
                }
            }
        }
        info!("Deleted IAM policy '{}'", name);
        Ok(Response::new(DeletePolicyResponse { success: true }))
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

        // Snapshot current attachments under a read lock, compute the
        // transition, then CAS. Idempotent: if the policy is already
        // attached, no-op returns success without a Raft round-trip.
        let (expected_bytes, new_policies_vec) = {
            let atts = self.policy_attachments.read();
            let current: Vec<String> = atts.get(&key).cloned().unwrap_or_default();
            if current.contains(&policy_name) {
                return Ok(Response::new(AttachPolicyResponse { success: true }));
            }
            let old_bytes = if current.is_empty() {
                None
            } else {
                Some(current.join(",").into_bytes())
            };
            let mut after = current;
            after.push(policy_name.clone());
            (old_bytes, after)
        };
        let new_bytes = new_policies_vec.join(",").into_bytes();

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::PolicyAttachments,
                    key: key.clone(),
                    expected: expected_bytes,
                    new_value: Some(new_bytes),
                }],
                requested_by: "attach-policy".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("attachment changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for attach_policy: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            store.put_policy_attachment(&key, &new_policies_vec.join(","));
        }

        self.policy_attachments
            .write()
            .insert(key.clone(), new_policies_vec);
        info!("Attached policy '{}' to '{}'", policy_name, key);
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

        // Compute the transition under a read lock.
        let (expected_bytes, new_after) = {
            let atts = self.policy_attachments.read();
            let Some(current) = atts.get(&key).cloned() else {
                return Ok(Response::new(DetachPolicyResponse { success: false }));
            };
            if !current.contains(&policy_name) {
                return Ok(Response::new(DetachPolicyResponse { success: false }));
            }
            let old_bytes = current.join(",").into_bytes();
            let after: Vec<String> = current
                .into_iter()
                .filter(|p| p != &policy_name)
                .collect();
            (old_bytes, after)
        };

        if let Some(raft) = self.raft_handle() {
            use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse};
            let new_value = if new_after.is_empty() {
                None
            } else {
                Some(new_after.join(",").into_bytes())
            };
            let cmd = MetaCommand::MultiCas {
                ops: vec![CasOp {
                    table: CasTable::PolicyAttachments,
                    key: key.clone(),
                    expected: Some(expected_bytes),
                    new_value,
                }],
                requested_by: "detach-policy".into(),
            };
            match raft.client_write(cmd).await {
                Ok(r) => match r.data {
                    MetaResponse::MultiCasOk => {}
                    MetaResponse::MultiCasConflict { .. } => {
                        return Err(Status::aborted("attachment changed since read; retry"));
                    }
                    other => {
                        error!("unexpected raft response for detach_policy: {:?}", other);
                        return Err(Status::internal("raft commit wrong variant"));
                    }
                },
                Err(e) => return Err(raft_write_to_status(&e)),
            }
        } else if let Some(store) = &self.store {
            if new_after.is_empty() {
                store.delete_policy_attachment(&key);
            } else {
                store.put_policy_attachment(&key, &new_after.join(","));
            }
        }

        let mut attachments = self.policy_attachments.write();
        if new_after.is_empty() {
            attachments.remove(&key);
        } else {
            attachments.insert(key.clone(), new_after);
        }
        info!("Detached policy '{}' from '{}'", policy_name, key);
        let removed = true;
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
