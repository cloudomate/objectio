//! Metadata gRPC service implementation

use objectio_common::{NodeId, NodeStatus};
use objectio_meta_store::{
    EcConfig, MetaStore, MultipartUploadState, OsdNode, PartState, StoredAccessKey, StoredUser,
};
use objectio_placement::{
    Crush2, PlacementTemplate, ShardRole,
    topology::{ClusterTopology, DiskInfo, FailureDomainInfo, NodeInfo},
};
use objectio_proto::metadata::{
    AbortMultipartUploadRequest,
    AbortMultipartUploadResponse,
    // IAM types
    AccessKeyMeta,
    BucketMeta,
    CompleteMultipartUploadRequest,
    CompleteMultipartUploadResponse,
    CreateAccessKeyRequest,
    CreateAccessKeyResponse,
    CreateBucketRequest,
    CreateBucketResponse,
    CreateMultipartUploadRequest,
    CreateMultipartUploadResponse,
    CreateObjectRequest,
    CreateObjectResponse,
    CreateUserRequest,
    CreateUserResponse,
    DeleteAccessKeyRequest,
    DeleteAccessKeyResponse,
    DeleteBucketPolicyRequest,
    DeleteBucketPolicyResponse,
    DeleteBucketRequest,
    DeleteBucketResponse,
    DeleteObjectRequest,
    DeleteObjectResponse,
    DeleteUserRequest,
    DeleteUserResponse,
    ErasureType,
    GetAccessKeyForAuthRequest,
    GetAccessKeyForAuthResponse,
    GetBucketPolicyRequest,
    GetBucketPolicyResponse,
    GetBucketRequest,
    GetBucketResponse,
    GetListingNodesRequest,
    GetListingNodesResponse,
    GetObjectRequest,
    GetObjectResponse,
    GetPlacementRequest,
    GetPlacementResponse,
    GetUserRequest,
    GetUserResponse,
    // Iceberg types
    IcebergCommitTableRequest,
    IcebergCommitTableResponse,
    IcebergCreateNamespaceRequest,
    IcebergCreateNamespaceResponse,
    IcebergCreateTableRequest,
    IcebergCreateTableResponse,
    IcebergDropNamespaceRequest,
    IcebergDropNamespaceResponse,
    IcebergDropTableRequest,
    IcebergDropTableResponse,
    IcebergListNamespacesRequest,
    IcebergListNamespacesResponse,
    IcebergListTablesRequest,
    IcebergListTablesResponse,
    IcebergLoadNamespaceRequest,
    IcebergLoadNamespaceResponse,
    IcebergLoadTableRequest,
    IcebergLoadTableResponse,
    IcebergNamespace,
    IcebergNamespaceExistsRequest,
    IcebergNamespaceExistsResponse,
    IcebergRenameTableRequest,
    IcebergRenameTableResponse,
    IcebergTableEntry,
    IcebergTableExistsRequest,
    IcebergTableExistsResponse,
    IcebergTableIdentifier,
    IcebergUpdateNamespacePropertiesRequest,
    IcebergUpdateNamespacePropertiesResponse,
    KeyStatus,
    ListAccessKeysRequest,
    ListAccessKeysResponse,
    ListBucketsRequest,
    ListBucketsResponse,
    ListMultipartUploadsRequest,
    ListMultipartUploadsResponse,
    ListObjectsRequest,
    ListObjectsResponse,
    ListPartsRequest,
    ListPartsResponse,
    ListUsersRequest,
    ListUsersResponse,
    ListingNode,
    MultipartUpload,
    NodePlacement,
    ObjectMeta,
    PartMeta,
    RegisterOsdRequest,
    RegisterOsdResponse,
    RegisterPartRequest,
    RegisterPartResponse,
    SetBucketPolicyRequest,
    SetBucketPolicyResponse,
    ShardType,
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
    /// Iceberg: namespace key -> properties (prost-encoded bytes in store, HashMap in memory)
    iceberg_namespaces: RwLock<HashMap<String, HashMap<String, String>>>,
    /// Iceberg: table key ("ns\0table") -> IcebergTableEntry
    iceberg_tables: RwLock<HashMap<String, IcebergTableEntry>>,
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
            iceberg_namespaces: RwLock::new(HashMap::new()),
            iceberg_tables: RwLock::new(HashMap::new()),
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
                let mut osd_nodes = self.osd_nodes.write();
                for (_hex_id, node) in nodes {
                    osd_nodes.push(node);
                }
                info!("Loaded {} OSD nodes from store", count);
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
                            tbl_map.insert(key, entry);
                        }
                        Err(e) => error!("Failed to decode iceberg table '{}': {}", key, e),
                    }
                }
                info!("Loaded {} iceberg tables from store", tbl_map.len());
            }
            Err(e) => error!("Failed to load iceberg tables: {}", e),
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
        let (region, dc, rack) = osd_node.failure_domain.clone().unwrap_or_else(|| {
            (
                "default".to_string(),
                "dc1".to_string(),
                "rack1".to_string(),
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
            failure_domain: FailureDomainInfo::new(&region, &dc, &rack),
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

    /// Encode namespace levels into a store key using null byte separator.
    fn iceberg_ns_key(levels: &[String]) -> String {
        levels.join("\x00")
    }

    /// Encode namespace + table name into a store key.
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

        // Select placement template based on EC configuration
        let (
            template,
            ec_type,
            ec_k,
            ec_local_parity,
            ec_global_parity,
            local_group_size,
            replication_count,
        ) = match &self.default_ec {
            EcConfig::Mds { k, m } => (
                PlacementTemplate::mds(*k, *m),
                ErasureType::ErasureMds,
                *k as u32,
                0u32,
                *m as u32,
                0u32,
                0u32, // Not replication mode
            ),
            EcConfig::Lrc { k, l, g } => (
                PlacementTemplate::lrc(*k, *l, *g),
                ErasureType::ErasureLrc,
                *k as u32,
                *l as u32,
                *g as u32,
                (*k / *l) as u32,
                0u32, // Not replication mode
            ),
            EcConfig::Replication { count } => (
                // For replication, we need 'count' replicas
                // Use MDS template with k=count (each is a full copy) and m=0
                PlacementTemplate::mds(*count, 0),
                ErasureType::ErasureReplication,
                1u32,          // ec_k=1: single data shard (full data)
                0u32,          // No local parity
                0u32,          // No global parity
                0u32,          // No local groups
                *count as u32, // Replication count
            ),
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
        };
        self.multipart_uploads
            .write()
            .insert(upload_id.clone(), state.clone());

        if let Some(store) = &self.store {
            store.put_multipart_upload(&upload_id, &state);
        }

        info!(
            "Created multipart upload: bucket={}, key={}, upload_id={}",
            req.bucket, req.key, upload_id
        );

        Ok(Response::new(CreateMultipartUploadResponse {
            upload_id,
            bucket: req.bucket,
            key: req.key,
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

        // Register the OSD
        let num_disks = disk_ids.len();
        let node = OsdNode {
            node_id,
            address: req.address.clone(),
            disk_ids,
            failure_domain: None,
        };

        // Check if node already exists and update, or add new
        let mut nodes = self.osd_nodes.write();
        if let Some(existing) = nodes.iter_mut().find(|n| n.node_id == node_id) {
            existing.address = node.address.clone();
            existing.disk_ids = node.disk_ids.clone();
            info!(
                "Updated OSD registration: {} at {}",
                hex::encode(node_id),
                req.address
            );
        } else {
            info!(
                "Registered new OSD: {} at {} with {} disks",
                hex::encode(node_id),
                req.address,
                num_disks
            );
            nodes.push(node.clone());
        }
        drop(nodes);

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

        // Collect all active nodes from topology
        let mut nodes: Vec<ListingNode> = topology
            .active_nodes()
            .enumerate()
            .map(|(idx, node)| ListingNode {
                node_id: node.id.as_bytes().to_vec(),
                address: format!("http://{}", node.address),
                shard_id: idx as u32, // Assign logical shard IDs in order
            })
            .collect();

        // Also include legacy OSD nodes if no topology nodes exist
        if nodes.is_empty() {
            nodes = osd_nodes
                .iter()
                .enumerate()
                .map(|(idx, node)| ListingNode {
                    node_id: node.node_id.to_vec(),
                    address: node.address.clone(),
                    shard_id: idx as u32,
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

        let user = StoredUser {
            user_id: user_id.clone(),
            display_name: req.display_name.clone(),
            arn: format!("arn:objectio:iam::user/{}", req.display_name),
            status: UserStatus::UserActive as i32,
            created_at: now,
            email: req.email.clone(),
        };

        self.users.write().insert(user_id.clone(), user.clone());
        self.user_keys.write().insert(user_id.clone(), Vec::new());

        if let Some(store) = &self.store {
            store.put_user(&user_id, &user);
        }

        info!("Created user: {}", req.display_name);

        Ok(Response::new(CreateUserResponse {
            user: Some(UserMeta {
                user_id: user.user_id,
                display_name: user.display_name,
                arn: user.arn,
                status: user.status,
                created_at: user.created_at,
                email: user.email,
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
                user_id: user.user_id,
                display_name: user.display_name,
                arn: user.arn,
                status: user.status,
                created_at: user.created_at,
                email: user.email,
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
            }),
            user: Some(UserMeta {
                user_id: user.user_id,
                display_name: user.display_name,
                arn: user.arn,
                status: user.status,
                created_at: user.created_at,
                email: user.email,
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

        let ns_key = Self::iceberg_ns_key(&req.namespace_levels);

        if self.iceberg_namespaces.read().contains_key(&ns_key) {
            return Err(Status::already_exists("namespace already exists"));
        }

        // If multi-level, verify parent exists
        if req.namespace_levels.len() > 1 {
            let parent_key =
                Self::iceberg_ns_key(&req.namespace_levels[..req.namespace_levels.len() - 1]);
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
        let ns_key = Self::iceberg_ns_key(&req.namespace_levels);

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
        let ns_key = Self::iceberg_ns_key(&req.namespace_levels);

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

        let parent_key = if req.parent_levels.is_empty() {
            String::new()
        } else {
            Self::iceberg_ns_key(&req.parent_levels)
        };

        // If parent specified, verify it exists
        if !parent_key.is_empty() && !self.iceberg_namespaces.read().contains_key(&parent_key) {
            return Err(Status::not_found("parent namespace not found"));
        }

        let prefix = if parent_key.is_empty() {
            String::new()
        } else {
            format!("{parent_key}\x00")
        };

        let namespaces: Vec<IcebergNamespace> = self
            .iceberg_namespaces
            .read()
            .keys()
            .filter(|k| {
                if prefix.is_empty() {
                    // Top-level: keys with no null byte separator
                    !k.contains('\x00')
                } else {
                    // Direct children: starts with prefix and no additional null bytes after prefix
                    k.starts_with(&prefix) && !k[prefix.len()..].contains('\x00')
                }
            })
            .map(|k| {
                let levels: Vec<String> = k.split('\x00').map(String::from).collect();
                IcebergNamespace { levels }
            })
            .collect();

        Ok(Response::new(IcebergListNamespacesResponse { namespaces }))
    }

    async fn iceberg_update_namespace_properties(
        &self,
        request: Request<IcebergUpdateNamespacePropertiesRequest>,
    ) -> Result<Response<IcebergUpdateNamespacePropertiesResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key(&req.namespace_levels);

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
        let ns_key = Self::iceberg_ns_key(&req.namespace_levels);
        let exists = self.iceberg_namespaces.read().contains_key(&ns_key);
        Ok(Response::new(IcebergNamespaceExistsResponse { exists }))
    }

    async fn iceberg_create_table(
        &self,
        request: Request<IcebergCreateTableRequest>,
    ) -> Result<Response<IcebergCreateTableResponse>, Status> {
        let req = request.into_inner();
        let ns_key = Self::iceberg_ns_key(&req.namespace_levels);

        // Verify namespace exists
        if !self.iceberg_namespaces.read().contains_key(&ns_key) {
            return Err(Status::not_found("namespace not found"));
        }

        let table_key = Self::iceberg_table_key(&req.namespace_levels, &req.table_name);

        if self.iceberg_tables.read().contains_key(&table_key) {
            return Err(Status::already_exists("table already exists"));
        }

        let now = Self::current_timestamp();
        let entry = IcebergTableEntry {
            metadata_location: req.metadata_location.clone(),
            created_at: now,
            updated_at: now,
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
        }))
    }

    async fn iceberg_load_table(
        &self,
        request: Request<IcebergLoadTableRequest>,
    ) -> Result<Response<IcebergLoadTableResponse>, Status> {
        let req = request.into_inner();
        let table_key = Self::iceberg_table_key(&req.namespace_levels, &req.table_name);

        let entry = self
            .iceberg_tables
            .read()
            .get(&table_key)
            .cloned()
            .ok_or_else(|| Status::not_found("table not found"))?;

        Ok(Response::new(IcebergLoadTableResponse {
            metadata_location: entry.metadata_location,
        }))
    }

    async fn iceberg_commit_table(
        &self,
        request: Request<IcebergCommitTableRequest>,
    ) -> Result<Response<IcebergCommitTableResponse>, Status> {
        let req = request.into_inner();
        let table_key = Self::iceberg_table_key(&req.namespace_levels, &req.table_name);

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
        }))
    }

    async fn iceberg_drop_table(
        &self,
        request: Request<IcebergDropTableRequest>,
    ) -> Result<Response<IcebergDropTableResponse>, Status> {
        let req = request.into_inner();
        let table_key = Self::iceberg_table_key(&req.namespace_levels, &req.table_name);

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
        let ns_key = Self::iceberg_ns_key(&req.namespace_levels);

        // Verify namespace exists
        if !self.iceberg_namespaces.read().contains_key(&ns_key) {
            return Err(Status::not_found("namespace not found"));
        }

        let prefix = format!("{ns_key}\x00");

        let identifiers: Vec<IcebergTableIdentifier> = self
            .iceberg_tables
            .read()
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .filter_map(|k| {
                // Table key is "ns1\x00ns2\x00table_name"
                // Strip the ns prefix to get table name
                let table_name = &k[prefix.len()..];
                // Only include direct tables (no additional null bytes)
                if table_name.contains('\x00') {
                    None
                } else {
                    Some(IcebergTableIdentifier {
                        namespace_levels: req.namespace_levels.clone(),
                        name: table_name.to_string(),
                    })
                }
            })
            .collect();

        Ok(Response::new(IcebergListTablesResponse { identifiers }))
    }

    async fn iceberg_table_exists(
        &self,
        request: Request<IcebergTableExistsRequest>,
    ) -> Result<Response<IcebergTableExistsResponse>, Status> {
        let req = request.into_inner();
        let table_key = Self::iceberg_table_key(&req.namespace_levels, &req.table_name);
        let exists = self.iceberg_tables.read().contains_key(&table_key);
        Ok(Response::new(IcebergTableExistsResponse { exists }))
    }
}
