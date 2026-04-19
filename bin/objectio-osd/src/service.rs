//! OSD gRPC service implementation

use futures::stream::Stream;
use objectio_proto::metadata::ObjectMeta;
use objectio_proto::storage::{
    BlockLocation,
    Checksum,
    CopyObjectMetaRequest,
    CopyObjectMetaResponse,
    DeleteObjectMetaRequest,
    DeleteObjectMetaResponse,
    DeleteShardRequest,
    DeleteShardResponse,
    DiskStatus,
    GetObjectMetaRequest,
    GetObjectMetaResponse,
    GetShardMetaRequest,
    GetShardMetaResponse,
    GetStatusRequest,
    GetStatusResponse,
    HealthCheckRequest,
    HealthCheckResponse,
    ListObjectVersionsMetaRequest,
    ListObjectVersionsMetaResponse,
    ListObjectsMetaChunk,
    ListObjectsMetaRequest,
    ListObjectsMetaResponse,
    ListShardsRequest,
    ListShardsResponse,
    FindObjectsReferencingNodeRequest,
    FindObjectsReferencingNodeResponse,
    AffectedObject,
    AffectedShardRef,
    // Object metadata RPCs
    PutObjectMetaRequest,
    PutObjectMetaResponse,
    ReadShardRequest,
    ReadShardResponse,
    WriteShardRequest,
    WriteShardResponse,
    health_check_response::Status as HealthStatus,
    storage_service_server::StorageService,
};
use objectio_storage::DiskManager;
use objectio_storage::metadata::{MetadataKey, MetadataStore, MetadataStoreConfig};
use parking_lot::RwLock;
use prost::Message;
use std::collections::HashMap;
use std::fmt::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// gRPC method metrics
#[derive(Debug, Default)]
pub struct GrpcMethodMetrics {
    pub requests_total: AtomicU64,
    pub requests_success: AtomicU64,
    pub requests_error: AtomicU64,
    pub latency_sum_us: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
}

impl GrpcMethodMetrics {
    pub fn record(&self, success: bool, latency_us: u64, bytes_in: u64, bytes_out: u64) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        if success {
            self.requests_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.requests_error.fetch_add(1, Ordering::Relaxed);
        }
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes_out, Ordering::Relaxed);
    }
}

/// gRPC metrics collector for OSD
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct GrpcMetrics {
    pub write_shard: GrpcMethodMetrics,
    pub read_shard: GrpcMethodMetrics,
    pub delete_shard: GrpcMethodMetrics,
    pub get_shard_meta: GrpcMethodMetrics,
    pub list_shards: GrpcMethodMetrics,
    pub put_object_meta: GrpcMethodMetrics,
    pub get_object_meta: GrpcMethodMetrics,
    pub delete_object_meta: GrpcMethodMetrics,
    pub list_objects_meta: GrpcMethodMetrics,
    pub copy_object_meta: GrpcMethodMetrics,
    pub stream_list_objects_meta: GrpcMethodMetrics,
    pub health_check: GrpcMethodMetrics,
    pub get_status: GrpcMethodMetrics,
}

impl GrpcMetrics {
    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self, osd_id: &str) -> String {
        let mut output = String::with_capacity(4 * 1024);

        // Requests total by method and status
        writeln!(
            output,
            "# HELP objectio_osd_grpc_requests_total Total gRPC requests by method and status"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_osd_grpc_requests_total counter").unwrap();

        let methods = [
            ("WriteShard", &self.write_shard),
            ("ReadShard", &self.read_shard),
            ("DeleteShard", &self.delete_shard),
            ("GetShardMeta", &self.get_shard_meta),
            ("ListShards", &self.list_shards),
            ("PutObjectMeta", &self.put_object_meta),
            ("GetObjectMeta", &self.get_object_meta),
            ("DeleteObjectMeta", &self.delete_object_meta),
            ("ListObjectsMeta", &self.list_objects_meta),
            ("HealthCheck", &self.health_check),
            ("GetStatus", &self.get_status),
        ];

        for (method, metrics) in methods.iter() {
            let success = metrics.requests_success.load(Ordering::Relaxed);
            let error = metrics.requests_error.load(Ordering::Relaxed);
            writeln!(
                output,
                "objectio_osd_grpc_requests_total{{osd_id=\"{}\",method=\"{}\",status=\"success\"}} {}",
                osd_id, method, success
            ).unwrap();
            writeln!(
                output,
                "objectio_osd_grpc_requests_total{{osd_id=\"{}\",method=\"{}\",status=\"error\"}} {}",
                osd_id, method, error
            ).unwrap();
        }

        // Latency sum (for calculating average)
        writeln!(
            output,
            "# HELP objectio_osd_grpc_latency_seconds_sum Sum of gRPC request latencies"
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE objectio_osd_grpc_latency_seconds_sum counter"
        )
        .unwrap();
        for (method, metrics) in methods.iter() {
            let sum_us = metrics.latency_sum_us.load(Ordering::Relaxed);
            writeln!(
                output,
                "objectio_osd_grpc_latency_seconds_sum{{osd_id=\"{}\",method=\"{}\"}} {}",
                osd_id,
                method,
                sum_us as f64 / 1_000_000.0
            )
            .unwrap();
        }

        // Bytes sent/received
        writeln!(
            output,
            "# HELP objectio_osd_grpc_bytes_received_total Total bytes received via gRPC"
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE objectio_osd_grpc_bytes_received_total counter"
        )
        .unwrap();
        for (method, metrics) in methods.iter() {
            let bytes = metrics.bytes_received.load(Ordering::Relaxed);
            if bytes > 0 {
                writeln!(
                    output,
                    "objectio_osd_grpc_bytes_received_total{{osd_id=\"{}\",method=\"{}\"}} {}",
                    osd_id, method, bytes
                )
                .unwrap();
            }
        }

        writeln!(
            output,
            "# HELP objectio_osd_grpc_bytes_sent_total Total bytes sent via gRPC"
        )
        .unwrap();
        writeln!(output, "# TYPE objectio_osd_grpc_bytes_sent_total counter").unwrap();
        for (method, metrics) in methods.iter() {
            let bytes = metrics.bytes_sent.load(Ordering::Relaxed);
            if bytes > 0 {
                writeln!(
                    output,
                    "objectio_osd_grpc_bytes_sent_total{{osd_id=\"{}\",method=\"{}\"}} {}",
                    osd_id, method, bytes
                )
                .unwrap();
            }
        }

        output
    }
}

/// Disk status information for metrics
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct DiskStatusInfo {
    pub path: String,
    pub capacity: u64,
    pub used: u64,
    pub shard_count: u64,
    pub status: String,
    pub read_errors: u64,
    pub write_errors: u64,
}

/// OSD status information for metrics
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct OsdStatus {
    pub disks: Vec<DiskStatusInfo>,
    pub total_capacity: u64,
    pub total_used: u64,
    pub total_shards: u64,
    pub uptime_secs: u64,
}

/// Shard location stored in memory (backed by MetadataStore for persistence)
#[derive(Clone, Debug)]
struct ShardLocation {
    disk_idx: usize,
    block_num: u64,
    size: u32,
    crc32c: u32,
    created_at: u64,
}

/// OSD service state
pub struct OsdService {
    node_id: [u8; 16],
    disks: Vec<DiskManager>,
    disk_ids: Vec<[u8; 16]>,
    /// Shard index: object_id:stripe_id:position -> location (in-memory cache)
    shard_index: RwLock<HashMap<String, ShardLocation>>,
    /// Persistent metadata store (WAL + B-tree + ARC cache)
    meta_store: Arc<MetadataStore>,
    start_time: Instant,
    /// Round-robin disk selection for writes
    next_disk: RwLock<usize>,
    /// Per-disk atomic block counter for allocation (no race conditions)
    next_block: Vec<std::sync::atomic::AtomicU64>,
    /// gRPC metrics collector
    grpc_metrics: Arc<GrpcMetrics>,
}

impl OsdService {
    /// Create a new OSD service with the given disks
    ///
    /// The block_size parameter configures the storage block size for new disks.
    /// Existing disks will use their existing block size from the superblock.
    /// A larger block size allows larger erasure-coded shards without chunking.
    pub fn new(
        disk_paths: Vec<String>,
        block_size: u32,
        data_dir: PathBuf,
    ) -> Result<Self, String> {
        // Node identity: Ceph/Rook pattern is "disk is source of truth".
        // We follow a three-level cascade:
        //   1. Any data disk's superblock — if one has a non-nil osd_node_id,
        //      use it (and reject the mount if disks disagree).
        //   2. data_dir/node_id — state PVC fallback for clusters running
        //      without disk-level identity (pre-migration, dev setups).
        //   3. Fresh uuid — only on true first boot; persisted to the
        //      state PVC immediately so a restart finds it.
        //
        // Superblock wiring lives in a follow-up patch; this commit lands
        // the state-PVC fallback so pod restarts stop orphaning shards.
        let id_path = data_dir.join("node_id");
        let node_id: [u8; 16] = match std::fs::read(&id_path) {
            Ok(bytes) if bytes.len() == 16 => {
                let mut id = [0u8; 16];
                id.copy_from_slice(&bytes);
                info!(
                    "Loaded persistent OSD node_id from {}: {}",
                    id_path.display(),
                    hex::encode(id)
                );
                id
            }
            _ => {
                // First boot (or corrupt file) — generate and persist.
                if let Err(e) = std::fs::create_dir_all(&data_dir) {
                    return Err(format!(
                        "Failed to create OSD data directory {}: {e}",
                        data_dir.display()
                    ));
                }
                let id = *Uuid::new_v4().as_bytes();
                if let Err(e) = std::fs::write(&id_path, id) {
                    warn!(
                        "Generated OSD node_id but failed to persist to {}: {e} — \
                         restarts will orphan data",
                        id_path.display()
                    );
                } else {
                    info!(
                        "Generated persistent OSD node_id at {}: {}",
                        id_path.display(),
                        hex::encode(id)
                    );
                }
                id
            }
        };
        let mut disks = Vec::new();
        let mut disk_ids = Vec::new();

        for path in &disk_paths {
            info!("Initializing disk: {}", path);

            // Try to open existing disk or initialize new one
            let disk = match DiskManager::open(path) {
                Ok(d) => {
                    info!(
                        "Opened existing disk: {} (block_size={})",
                        path,
                        d.block_size()
                    );
                    d
                }
                Err(_) => {
                    // Get device/file size - for block devices we need to check
                    let size = if std::path::Path::new(path).exists() {
                        // Use raw_io to get size
                        let rf = objectio_storage::RawFile::open(path, true)
                            .map_err(|e| format!("Failed to check {}: {}", path, e))?;
                        rf.size()
                    } else {
                        // Default to 10GB for new files
                        10 * 1024 * 1024 * 1024
                    };

                    info!(
                        "Initializing new disk: {} with size {} bytes, block_size {} bytes",
                        path, size, block_size
                    );
                    DiskManager::init(path, size, Some(block_size))
                        .map_err(|e| format!("Failed to init disk {}: {}", path, e))?
                }
            };

            disk_ids.push(*disk.id().as_bytes());
            disks.push(disk);
        }

        if disks.is_empty() {
            return Err("No disks configured".into());
        }

        // Initialize metadata store for persistent object metadata
        let meta_config = MetadataStoreConfig::with_data_dir(&data_dir);
        let meta_store = MetadataStore::open_or_create(meta_config)
            .map_err(|e| format!("Failed to open metadata store: {}", e))?;

        info!(
            "OSD initialized with {} disks, metadata at {:?}",
            disks.len(),
            data_dir
        );

        let num_disks = disks.len();
        Ok(Self {
            node_id,
            disks,
            disk_ids,
            shard_index: RwLock::new(HashMap::new()),
            meta_store: Arc::new(meta_store),
            start_time: Instant::now(),
            next_disk: RwLock::new(0),
            next_block: (0..num_disks)
                .map(|_| std::sync::atomic::AtomicU64::new(0))
                .collect(),
            grpc_metrics: Arc::new(GrpcMetrics::default()),
        })
    }

    /// Get gRPC metrics
    pub fn grpc_metrics(&self) -> &Arc<GrpcMetrics> {
        &self.grpc_metrics
    }

    /// Create OSD service with default metadata directory
    #[allow(dead_code)]
    pub fn new_default(disk_paths: Vec<String>, block_size: u32) -> Result<Self, String> {
        let data_dir = PathBuf::from("./osd-metadata");
        Self::new(disk_paths, block_size, data_dir)
    }

    /// Get node ID as bytes
    pub fn node_id(&self) -> &[u8; 16] {
        &self.node_id
    }

    /// Get disk IDs
    pub fn disk_ids(&self) -> &[[u8; 16]] {
        &self.disk_ids
    }

    /// Raw capacity of each managed disk, index-aligned with `disk_ids()`.
    /// Used at registration time so meta can sum capacity across OSDs and
    /// enforce the license's `max_raw_capacity_bytes` cap.
    pub fn disk_capacities(&self) -> Vec<u64> {
        self.disks.iter().map(|d| d.capacity()).collect()
    }

    /// Get disk count
    #[allow(dead_code)]
    pub fn disk_count(&self) -> usize {
        self.disks.len()
    }

    /// Get OSD status for metrics
    pub fn status(&self) -> OsdStatus {
        let mut disks = Vec::new();
        let mut total_capacity = 0u64;
        let mut total_used = 0u64;
        let mut total_shards = 0u64;

        for (i, disk) in self.disks.iter().enumerate() {
            let stats = disk.stats();
            let capacity = disk.capacity();
            let free = disk.free_space();
            let used = capacity.saturating_sub(free);
            let shard_count = self
                .shard_index
                .read()
                .values()
                .filter(|loc| loc.disk_idx == i)
                .count() as u64;

            total_capacity += capacity;
            total_used += used;
            total_shards += shard_count;

            disks.push(DiskStatusInfo {
                path: disk.path().to_string(),
                capacity,
                used,
                shard_count,
                status: "healthy".to_string(), // TODO: Check actual health
                read_errors: stats.read_errors.load(std::sync::atomic::Ordering::Relaxed),
                write_errors: stats
                    .write_errors
                    .load(std::sync::atomic::Ordering::Relaxed),
            });
        }

        OsdStatus {
            disks,
            total_capacity,
            total_used,
            total_shards,
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }

    /// Select disk for write (round-robin)
    fn select_disk_for_write(&self) -> usize {
        let mut next = self.next_disk.write();
        let disk_idx = *next;
        *next = (*next + 1) % self.disks.len();
        disk_idx
    }

    /// Generate shard key for index
    fn shard_key(object_id: &[u8], stripe_id: u64, position: u32) -> String {
        format!("{}:{}:{}", hex::encode(object_id), stripe_id, position)
    }

    /// Allocate a block for writing
    #[allow(clippy::result_large_err)]
    fn allocate_block(&self, disk_idx: usize) -> Result<u64, Status> {
        // Atomic increment ensures no two concurrent writes get the same block
        Ok(self.next_block[disk_idx].fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }

    /// Get current timestamp
    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

#[tonic::async_trait]
impl StorageService for OsdService {
    async fn write_shard(
        &self,
        request: Request<WriteShardRequest>,
    ) -> Result<Response<WriteShardResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let bytes_in = req.data.len() as u64;
        let shard_id = req.shard_id.ok_or_else(|| {
            self.grpc_metrics
                .write_shard
                .record(false, start.elapsed().as_micros() as u64, 0, 0);
            Status::invalid_argument("missing shard_id")
        })?;

        debug!(
            "WriteShard: object={}, stripe={}, pos={}, size={}",
            hex::encode(&shard_id.object_id),
            shard_id.stripe_id,
            shard_id.position,
            req.data.len()
        );

        // Select disk and allocate block
        let disk_idx = self.select_disk_for_write();
        let block_num = self.allocate_block(disk_idx)?;

        let disk = &self.disks[disk_idx];

        // Prepare object_id as fixed array
        let mut object_id = [0u8; 16];
        let copy_len = shard_id.object_id.len().min(16);
        object_id[..copy_len].copy_from_slice(&shard_id.object_id[..copy_len]);

        // Write block
        disk.write_block(block_num, object_id, shard_id.stripe_id, &req.data)
            .map_err(|e| Status::internal(format!("write failed: {}", e)))?;

        disk.sync()
            .map_err(|e| Status::internal(format!("sync failed: {}", e)))?;

        // Calculate checksum
        let crc32c = crc32c::crc32c(&req.data);

        // Store location in index
        let key = Self::shard_key(&shard_id.object_id, shard_id.stripe_id, shard_id.position);
        let timestamp = Self::current_timestamp();

        self.shard_index.write().insert(
            key,
            ShardLocation {
                disk_idx,
                block_num,
                size: req.data.len() as u32,
                crc32c,
                created_at: timestamp,
            },
        );

        info!(
            "Wrote shard: disk={}, block={}, size={}, crc32c={:08x}",
            disk_idx,
            block_num,
            req.data.len(),
            crc32c
        );

        let resp = WriteShardResponse {
            location: Some(BlockLocation {
                node_id: self.node_id.to_vec(),
                disk_id: self.disk_ids[disk_idx].to_vec(),
                offset: block_num * disk.block_size() as u64,
                size: req.data.len() as u32,
            }),
            timestamp,
        };
        let bytes_out = resp.encoded_len() as u64;
        self.grpc_metrics.write_shard.record(
            true,
            start.elapsed().as_micros() as u64,
            bytes_in,
            bytes_out,
        );

        Ok(Response::new(resp))
    }

    async fn read_shard(
        &self,
        request: Request<ReadShardRequest>,
    ) -> Result<Response<ReadShardResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let bytes_in = req.encoded_len() as u64;
        let shard_id = req.shard_id.ok_or_else(|| {
            self.grpc_metrics.read_shard.record(
                false,
                start.elapsed().as_micros() as u64,
                bytes_in,
                0,
            );
            Status::invalid_argument("missing shard_id")
        })?;

        let key = Self::shard_key(&shard_id.object_id, shard_id.stripe_id, shard_id.position);

        let location = self.shard_index.read().get(&key).cloned().ok_or_else(|| {
            self.grpc_metrics.read_shard.record(
                false,
                start.elapsed().as_micros() as u64,
                bytes_in,
                0,
            );
            Status::not_found("shard not found")
        })?;

        let disk = &self.disks[location.disk_idx];

        let (_header, data) = disk.read_block(location.block_num).map_err(|e| {
            self.grpc_metrics.read_shard.record(
                false,
                start.elapsed().as_micros() as u64,
                bytes_in,
                0,
            );
            Status::internal(format!("read failed: {}", e))
        })?;

        debug!(
            "ReadShard: object={}, stripe={}, pos={}, size={}",
            hex::encode(&shard_id.object_id),
            shard_id.stripe_id,
            shard_id.position,
            data.len()
        );

        let timestamp = Self::current_timestamp();

        let resp = ReadShardResponse {
            data,
            checksum: Some(Checksum {
                crc32c: location.crc32c,
                xxhash64: 0,
                sha256: vec![],
            }),
            timestamp,
        };
        let bytes_out = resp.data.len() as u64;
        self.grpc_metrics.read_shard.record(
            true,
            start.elapsed().as_micros() as u64,
            bytes_in,
            bytes_out,
        );

        Ok(Response::new(resp))
    }

    async fn delete_shard(
        &self,
        request: Request<DeleteShardRequest>,
    ) -> Result<Response<DeleteShardResponse>, Status> {
        let req = request.into_inner();
        let shard_id = req
            .shard_id
            .ok_or_else(|| Status::invalid_argument("missing shard_id"))?;

        let key = Self::shard_key(&shard_id.object_id, shard_id.stripe_id, shard_id.position);

        let removed = self.shard_index.write().remove(&key).is_some();

        // Note: actual block space is not reclaimed in this simple implementation
        // A real implementation would mark the block as free in the bitmap

        Ok(Response::new(DeleteShardResponse { success: removed }))
    }

    async fn get_shard_meta(
        &self,
        request: Request<GetShardMetaRequest>,
    ) -> Result<Response<GetShardMetaResponse>, Status> {
        let req = request.into_inner();
        let shard_id = req
            .shard_id
            .ok_or_else(|| Status::invalid_argument("missing shard_id"))?;

        let key = Self::shard_key(&shard_id.object_id, shard_id.stripe_id, shard_id.position);

        let location = self
            .shard_index
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| Status::not_found("shard not found"))?;

        let disk = &self.disks[location.disk_idx];

        Ok(Response::new(GetShardMetaResponse {
            shard_id: Some(shard_id),
            location: Some(BlockLocation {
                node_id: self.node_id.to_vec(),
                disk_id: self.disk_ids[location.disk_idx].to_vec(),
                offset: location.block_num * disk.block_size() as u64,
                size: location.size,
            }),
            size: location.size,
            checksum: Some(Checksum {
                crc32c: location.crc32c,
                xxhash64: 0,
                sha256: vec![],
            }),
            created_at: location.created_at,
        }))
    }

    async fn list_shards(
        &self,
        request: Request<ListShardsRequest>,
    ) -> Result<Response<ListShardsResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit as usize
        };

        let index = self.shard_index.read();
        let mut shards: Vec<GetShardMetaResponse> = Vec::new();

        for (key, location) in index.iter().take(limit) {
            // Parse key back to shard_id
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() != 3 {
                continue;
            }

            let object_id = hex::decode(parts[0]).unwrap_or_default();
            let stripe_id: u64 = parts[1].parse().unwrap_or_default();
            let position: u32 = parts[2].parse().unwrap_or_default();

            // Filter by object_id if specified
            if !req.object_id.is_empty() && object_id != req.object_id {
                continue;
            }

            let disk = &self.disks[location.disk_idx];

            shards.push(GetShardMetaResponse {
                shard_id: Some(objectio_proto::storage::ShardId {
                    object_id,
                    stripe_id,
                    position,
                }),
                location: Some(BlockLocation {
                    node_id: self.node_id.to_vec(),
                    disk_id: self.disk_ids[location.disk_idx].to_vec(),
                    offset: location.block_num * disk.block_size() as u64,
                    size: location.size,
                }),
                size: location.size,
                checksum: Some(Checksum {
                    crc32c: location.crc32c,
                    xxhash64: 0,
                    sha256: vec![],
                }),
                created_at: location.created_at,
            });
        }

        Ok(Response::new(ListShardsResponse {
            shards,
            next_token: vec![],
        }))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        // Check if all disks are accessible
        let all_healthy = self.disks.iter().all(|d| d.verify_block(0).is_ok());

        let (status, message) = if all_healthy {
            (HealthStatus::Healthy, "All disks healthy".to_string())
        } else {
            (HealthStatus::Degraded, "Some disks have issues".to_string())
        };

        Ok(Response::new(HealthCheckResponse {
            status: status.into(),
            message,
        }))
    }

    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let mut total_capacity = 0u64;
        let mut used_capacity = 0u64;
        let mut disk_statuses = Vec::new();

        for (idx, disk) in self.disks.iter().enumerate() {
            let cap = disk.capacity();
            let free = disk.free_space();
            let used = cap - free;

            total_capacity += cap;
            used_capacity += used;

            let shard_count = self
                .shard_index
                .read()
                .values()
                .filter(|loc| loc.disk_idx == idx)
                .count() as u64;

            disk_statuses.push(DiskStatus {
                disk_id: self.disk_ids[idx].to_vec(),
                path: disk.path().to_string(),
                total_capacity: cap,
                used_capacity: used,
                status: "healthy".to_string(),
                shard_count,
            });
        }

        let shard_count = self.shard_index.read().len() as u64;
        let uptime = self.start_time.elapsed().as_secs();

        // Gather host/environment info
        let kubernetes_node = std::env::var("NODE_NAME").unwrap_or_default();
        let pod_name = std::env::var("POD_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_default();
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let os_info = sys_info::os_type()
            .map(|t| {
                let rel = sys_info::os_release().unwrap_or_default();
                format!("{t} {rel}")
            })
            .unwrap_or_default();
        let cpu_cores = sys_info::cpu_num().unwrap_or(0) as u64;
        let memory_bytes = sys_info::mem_info()
            .map(|m| m.total * 1024) // mem_info returns KB
            .unwrap_or(0);

        Ok(Response::new(GetStatusResponse {
            node_id: self.node_id.to_vec(),
            node_name: if pod_name.is_empty() {
                format!("osd-{}", hex::encode(&self.node_id[..4]))
            } else {
                pod_name.clone()
            },
            disks: disk_statuses,
            total_capacity,
            used_capacity,
            shard_count,
            uptime_seconds: uptime,
            kubernetes_node,
            pod_name,
            os_info,
            hostname,
            cpu_cores,
            memory_bytes,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    // ============================================================
    // Object Metadata Operations (stored on primary OSD)
    // ============================================================

    async fn put_object_meta(
        &self,
        request: Request<PutObjectMetaRequest>,
    ) -> Result<Response<PutObjectMetaResponse>, Status> {
        let req = request.into_inner();

        let object = req
            .object
            .ok_or_else(|| Status::invalid_argument("missing object"))?;

        // Serialize ObjectMeta to bytes using protobuf
        let value = object.encode_to_vec();

        // Always store as current version at m:{bucket}\0{key}
        let key = MetadataKey::object_meta(&req.bucket, &req.key);
        self.meta_store
            .put(key, value.clone())
            .map_err(|e| Status::internal(format!("failed to store object metadata: {}", e)))?;

        // If versioning is enabled and version_id is set, also store version entry
        if req.versioning_enabled && !object.version_id.is_empty() {
            let version_key =
                MetadataKey::object_version(&req.bucket, &req.key, &object.version_id);
            self.meta_store
                .put(version_key, value)
                .map_err(|e| Status::internal(format!("failed to store version entry: {}", e)))?;
        }

        let timestamp = Self::current_timestamp();

        info!(
            "Stored object metadata: {}/{} ({} bytes, version={})",
            req.bucket, req.key, object.size, object.version_id
        );

        Ok(Response::new(PutObjectMetaResponse {
            success: true,
            timestamp,
        }))
    }

    async fn get_object_meta(
        &self,
        request: Request<GetObjectMetaRequest>,
    ) -> Result<Response<GetObjectMetaResponse>, Status> {
        let req = request.into_inner();

        // If version_id specified, look up specific version; otherwise get current
        let key = if req.version_id.is_empty() {
            MetadataKey::object_meta(&req.bucket, &req.key)
        } else {
            MetadataKey::object_version(&req.bucket, &req.key, &req.version_id)
        };

        // Lookup in metadata store
        match self.meta_store.get(&key) {
            Some(value) => {
                // Deserialize ObjectMeta from protobuf
                let object = ObjectMeta::decode(&value[..]).map_err(|e| {
                    Status::internal(format!("failed to decode object metadata: {}", e))
                })?;

                debug!("Found object metadata: {}/{}", req.bucket, req.key);

                Ok(Response::new(GetObjectMetaResponse {
                    object: Some(object),
                    found: true,
                }))
            }
            None => {
                debug!("Object metadata not found: {}/{}", req.bucket, req.key);

                Ok(Response::new(GetObjectMetaResponse {
                    object: None,
                    found: false,
                }))
            }
        }
    }

    async fn delete_object_meta(
        &self,
        request: Request<DeleteObjectMetaRequest>,
    ) -> Result<Response<DeleteObjectMetaResponse>, Status> {
        let req = request.into_inner();

        if req.version_id.is_empty() {
            // Delete current version entry
            let key = MetadataKey::object_meta(&req.bucket, &req.key);
            self.meta_store.delete(&key).map_err(|e| {
                Status::internal(format!("failed to delete object metadata: {}", e))
            })?;
            info!("Deleted object metadata: {}/{}", req.bucket, req.key);
        } else {
            // Delete specific version entry
            let version_key = MetadataKey::object_version(&req.bucket, &req.key, &req.version_id);
            self.meta_store
                .delete(&version_key)
                .map_err(|e| Status::internal(format!("failed to delete version entry: {}", e)))?;
            info!(
                "Deleted version: {}/{} (version={})",
                req.bucket, req.key, req.version_id
            );
        }

        Ok(Response::new(DeleteObjectMetaResponse { success: true }))
    }

    async fn list_objects_meta(
        &self,
        request: Request<ListObjectsMetaRequest>,
    ) -> Result<Response<ListObjectsMetaResponse>, Status> {
        let req = request.into_inner();
        let max_keys = if req.max_keys == 0 {
            1000
        } else {
            req.max_keys as usize
        };

        // Empty bucket means "scan every primary-held ObjectMeta on
        // this OSD". Used by the cluster rebalancer to enumerate
        // candidates without driving per-bucket fan-out. Regular
        // bucket-scoped callers keep their existing semantics.
        let prefix = if req.bucket.is_empty() {
            MetadataKey::all_object_meta_prefix()
        } else {
            MetadataKey::object_meta_prefix(&req.bucket)
        };

        // Scan all objects in bucket (or cluster-wide when bucket="")
        let entries = self.meta_store.scan_prefix(&prefix);

        let mut objects = Vec::new();
        let mut count = 0;
        let mut last_key = String::new();

        // Cluster-wide pagination: object keys can repeat across
        // buckets (bucketA/file.txt vs bucketB/file.txt), so the
        // single `key` string isn't a total order. When bucket is
        // empty we cursor on `{bucket}\0{key}` instead — that matches
        // the underlying meta_store key order.
        let cluster_wide = req.bucket.is_empty();
        for (meta_key, value) in entries {
            if let Some((bucket_of, key)) = meta_key.parse_object_meta() {
                let cursor = if cluster_wide {
                    format!("{bucket_of}\0{key}")
                } else {
                    key.clone()
                };

                // Skip if before start_after
                if !req.start_after.is_empty() && cursor <= req.start_after {
                    continue;
                }

                // Apply prefix filter (only meaningful within a bucket)
                if !req.prefix.is_empty() && !key.starts_with(&req.prefix) {
                    continue;
                }

                // Skip if before continuation token
                if !req.continuation_token.is_empty()
                    && cursor <= req.continuation_token
                {
                    continue;
                }

                // Check limit
                if count >= max_keys {
                    break;
                }

                // Decode object metadata
                if let Ok(object) = ObjectMeta::decode(&value[..]) {
                    last_key = cursor;
                    objects.push(object);
                    count += 1;
                }
            }
        }

        let is_truncated = count >= max_keys;
        let next_token = if is_truncated {
            last_key
        } else {
            String::new()
        };

        Ok(Response::new(ListObjectsMetaResponse {
            objects,
            next_continuation_token: next_token,
            is_truncated,
            key_count: count as u32,
        }))
    }

    async fn find_objects_referencing_node(
        &self,
        request: Request<FindObjectsReferencingNodeRequest>,
    ) -> Result<Response<FindObjectsReferencingNodeResponse>, Status> {
        let req = request.into_inner();

        // 16-byte UUID validation. Unknown lengths are almost always a
        // client bug; fail fast rather than "no matches" which would be
        // misleading under a real drain.
        if req.draining_node_id.len() != 16 {
            return Err(Status::invalid_argument(
                "draining_node_id must be 16 bytes",
            ));
        }
        let needle = req.draining_node_id.as_slice();
        let limit = if req.limit == 0 {
            usize::MAX
        } else {
            req.limit as usize
        };

        // Scan every object_meta on this OSD (across all buckets) and
        // collect the ones whose any stripe has a ShardLocation on the
        // draining node. O(total objects on this OSD) — only runs when
        // an operator-triggered drain is actively sweeping, so the
        // linear scan is acceptable.
        let prefix = MetadataKey::all_object_meta_prefix();
        let entries = self.meta_store.scan_prefix(&prefix);
        let mut out: Vec<AffectedObject> = Vec::new();
        let mut truncated = false;

        for (meta_key, value) in entries {
            if out.len() >= limit {
                truncated = true;
                break;
            }
            // `m:` prefix also matches any future `m*`-rooted key we
            // might add. parse_object_meta is the canonical check —
            // skip anything that isn't a plain object_meta record.
            let Some((bucket, key)) = meta_key.parse_object_meta() else {
                continue;
            };
            let Ok(object) = objectio_proto::metadata::ObjectMeta::decode(&value[..])
            else {
                continue;
            };

            let mut shards: Vec<AffectedShardRef> = Vec::new();
            for stripe in &object.stripes {
                for shard in &stripe.shards {
                    if shard.node_id == needle {
                        shards.push(AffectedShardRef {
                            stripe_id: stripe.stripe_id,
                            position: shard.position,
                        });
                    }
                }
            }

            if !shards.is_empty() {
                out.push(AffectedObject {
                    bucket,
                    key,
                    object_id: object.object_id.clone(),
                    shards,
                });
            }
        }

        debug!(
            "find_objects_referencing_node({}): {} objects affected (truncated={})",
            hex::encode(&req.draining_node_id),
            out.len(),
            truncated
        );

        Ok(Response::new(FindObjectsReferencingNodeResponse {
            objects: out,
            truncated,
        }))
    }

    async fn copy_object_meta(
        &self,
        request: Request<CopyObjectMetaRequest>,
    ) -> Result<Response<CopyObjectMetaResponse>, Status> {
        let req = request.into_inner();

        // Read source ObjectMeta from local store
        let src_key = MetadataKey::object_meta(&req.source_bucket, &req.source_key);
        let value = self
            .meta_store
            .get(&src_key)
            .ok_or_else(|| Status::not_found("source object not found on this OSD"))?;

        let mut object = ObjectMeta::decode(&value[..]).map_err(|e| {
            Status::internal(format!("failed to decode source object metadata: {e}"))
        })?;

        // Update metadata fields for the destination key
        let now = Self::current_timestamp();
        object.bucket = req.dest_bucket.clone();
        object.key = req.dest_key.clone();
        object.created_at = now;
        object.modified_at = now;
        // Generate a new ETag based on object_id + timestamp so dest has its own identity
        object.etag = format!("{:x}", Uuid::new_v4().as_u128());

        // Write dest ObjectMeta
        let dst_key = MetadataKey::object_meta(&req.dest_bucket, &req.dest_key);
        let dest_bytes = object.encode_to_vec();
        self.meta_store
            .put(dst_key, dest_bytes)
            .map_err(|e| Status::internal(format!("failed to store dest object metadata: {e}")))?;

        info!(
            "Copied object metadata: {}/{} -> {}/{}",
            req.source_bucket, req.source_key, req.dest_bucket, req.dest_key
        );

        Ok(Response::new(CopyObjectMetaResponse {
            object: Some(object),
        }))
    }

    type StreamListObjectsMetaStream =
        Pin<Box<dyn Stream<Item = Result<ListObjectsMetaChunk, Status>> + Send + 'static>>;

    async fn stream_list_objects_meta(
        &self,
        request: Request<ListObjectsMetaRequest>,
    ) -> Result<Response<Self::StreamListObjectsMetaStream>, Status> {
        const CHUNK_SIZE: usize = 500;

        let req = request.into_inner();
        let prefix = MetadataKey::object_meta_prefix(&req.bucket);
        let entries = self.meta_store.scan_prefix(&prefix);

        let mut chunks: Vec<ListObjectsMetaChunk> = Vec::new();
        let mut batch: Vec<ObjectMeta> = Vec::with_capacity(CHUNK_SIZE);

        for (meta_key, value) in entries {
            if let Some((_bucket, key)) = meta_key.parse_object_meta() {
                // Apply start_after / continuation_token cursor
                if !req.start_after.is_empty() && key <= req.start_after {
                    continue;
                }
                if !req.continuation_token.is_empty() && key <= req.continuation_token {
                    continue;
                }
                // Apply prefix filter
                if !req.prefix.is_empty() && !key.starts_with(&req.prefix) {
                    continue;
                }

                if let Ok(object) = ObjectMeta::decode(&value[..]) {
                    batch.push(object);

                    if batch.len() >= CHUNK_SIZE {
                        let cursor = key.clone();
                        chunks.push(ListObjectsMetaChunk {
                            objects: std::mem::take(&mut batch),
                            next_start_after: cursor,
                            is_last: false,
                        });
                    }
                }
            }
        }

        // Emit the final (possibly partial) batch
        chunks.push(ListObjectsMetaChunk {
            objects: batch,
            next_start_after: String::new(),
            is_last: true,
        });

        let stream = futures::stream::iter(chunks.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_object_versions_meta(
        &self,
        request: Request<ListObjectVersionsMetaRequest>,
    ) -> Result<Response<ListObjectVersionsMetaResponse>, Status> {
        let req = request.into_inner();
        let max_keys = if req.max_keys == 0 {
            1000
        } else {
            req.max_keys as usize
        };

        // Scan version entries (v:{bucket}\0...)
        let prefix = MetadataKey::object_version_bucket_prefix(&req.bucket);
        let entries = self.meta_store.scan_prefix(&prefix);

        let mut versions = Vec::new();
        let mut count = 0;
        let mut last_key = String::new();
        let mut last_version_id = String::new();

        for (meta_key, value) in entries {
            if let Some((_bucket, key, version_id)) = meta_key.parse_object_version() {
                // Apply prefix filter
                if !req.prefix.is_empty() && !key.starts_with(&req.prefix) {
                    continue;
                }

                // Apply key_marker: skip entries at or before key_marker
                if !req.key_marker.is_empty() {
                    if key < req.key_marker {
                        continue;
                    }
                    if key == req.key_marker
                        && !req.version_id_marker.is_empty()
                        && version_id <= req.version_id_marker
                    {
                        continue;
                    }
                }

                if count >= max_keys {
                    break;
                }

                if let Ok(object) = ObjectMeta::decode(&value[..]) {
                    last_key = key;
                    last_version_id = version_id;
                    versions.push(object);
                    count += 1;
                }
            }
        }

        let is_truncated = count >= max_keys;

        Ok(Response::new(ListObjectVersionsMetaResponse {
            versions,
            next_key_marker: if is_truncated {
                last_key
            } else {
                String::new()
            },
            next_version_id_marker: if is_truncated {
                last_version_id
            } else {
                String::new()
            },
            is_truncated,
        }))
    }
}
