//! Block storage gRPC service implementation
//!
//! Provides volume and snapshot management for block storage.

use objectio_proto::block::{
    block_service_server::BlockService,
    // Volume operations
    CreateVolumeRequest, CreateVolumeResponse, DeleteVolumeRequest, DeleteVolumeResponse,
    GetVolumeRequest, GetVolumeResponse, ListVolumesRequest, ListVolumesResponse,
    ResizeVolumeRequest, ResizeVolumeResponse, Volume, VolumeState, VolumeQos,
    UpdateVolumeQosRequest, UpdateVolumeQosResponse,
    GetVolumeStatsRequest, GetVolumeStatsResponse,
    // Snapshot operations
    CreateSnapshotRequest, CreateSnapshotResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    GetSnapshotRequest, GetSnapshotResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    CloneVolumeRequest, CloneVolumeResponse, Snapshot, SnapshotState,
    // Attachment operations
    AttachVolumeRequest, AttachVolumeResponse, DetachVolumeRequest, DetachVolumeResponse,
    ListAttachmentsRequest, ListAttachmentsResponse, Attachment, TargetType,
    // I/O operations
    ReadRequest, ReadResponse, WriteRequest, WriteResponse,
    FlushRequest, FlushResponse, TrimRequest, TrimResponse,
    // Metrics operations
    GetOsdMetricsRequest, GetOsdMetricsResponse,
    ListOsdMetricsRequest, ListOsdMetricsResponse,
    GetClusterMetricsRequest, GetClusterMetricsResponse,
    GetIoTraceRequest, GetIoTraceResponse,
};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Default chunk size: 4MB
const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;

/// Stored volume metadata
#[derive(Clone, Debug)]
pub struct StoredVolume {
    pub volume_id: String,
    pub name: String,
    pub size_bytes: u64,
    pub used_bytes: u64,
    pub pool: String,
    pub state: i32,
    pub created_at: u64,
    pub updated_at: u64,
    pub parent_snapshot_id: String,
    pub chunk_size_bytes: u32,
    pub metadata: HashMap<String, String>,
    pub qos: Option<VolumeQos>,
}

/// Stored snapshot metadata
#[derive(Clone, Debug)]
pub struct StoredSnapshot {
    pub snapshot_id: String,
    pub volume_id: String,
    pub name: String,
    pub size_bytes: u64,
    pub unique_bytes: u64,
    pub state: i32,
    pub created_at: u64,
    pub metadata: HashMap<String, String>,
    /// Chunk references at snapshot time
    pub chunk_refs: HashMap<u64, StoredChunkRef>,
}

/// Stored chunk reference
#[derive(Clone, Debug)]
pub struct StoredChunkRef {
    pub chunk_id: u64,
    pub object_key: String,
    pub etag: String,
    pub size_bytes: u64,
}

/// Stored attachment information
#[derive(Clone, Debug)]
pub struct StoredAttachment {
    pub volume_id: String,
    pub target_type: i32,
    pub target_address: String,
    pub initiator: String,
    pub attached_at: u64,
    pub read_only: bool,
}

/// Block metadata service state
pub struct BlockMetaService {
    /// Volumes indexed by volume_id
    volumes: RwLock<HashMap<String, StoredVolume>>,
    /// Volume names to IDs
    volume_names: RwLock<HashMap<String, String>>,
    /// Snapshots indexed by snapshot_id
    snapshots: RwLock<HashMap<String, StoredSnapshot>>,
    /// Snapshots by volume ID
    volume_snapshots: RwLock<HashMap<String, HashSet<String>>>,
    /// Chunk references per volume (sparse)
    volume_chunks: RwLock<HashMap<String, HashMap<u64, StoredChunkRef>>>,
    /// Volume attachments
    attachments: RwLock<HashMap<String, StoredAttachment>>,
}

/// Per-volume statistics for metrics export
#[derive(Debug, Clone)]
pub struct VolumeStatEntry {
    pub volume_id: String,
    pub name: String,
    pub pool: String,
    pub size_bytes: u64,
    pub used_bytes: u64,
    pub qos: Option<VolumeQos>,
}

/// Per-snapshot statistics for metrics export
#[derive(Debug, Clone)]
pub struct SnapshotStatEntry {
    pub snapshot_id: String,
    pub volume_id: String,
    pub name: String,
    pub size_bytes: u64,
}

/// Statistics for the block metadata service
#[derive(Debug, Clone, Default)]
pub struct BlockMetaStats {
    pub volume_count: u64,
    pub snapshot_count: u64,
    pub attachment_count: u64,
    pub volumes_by_state: HashMap<String, u64>,
    pub volumes_provisioned_bytes: u64,
    pub volumes_used_bytes: u64,
    pub volumes: Vec<VolumeStatEntry>,
    pub snapshots: Vec<SnapshotStatEntry>,
    pub snapshots_space_bytes: u64,
    pub attachments_by_type: HashMap<String, u64>,
}

impl BlockMetaService {
    /// Create a new block metadata service
    pub fn new() -> Self {
        Self {
            volumes: RwLock::new(HashMap::new()),
            volume_names: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
            volume_snapshots: RwLock::new(HashMap::new()),
            volume_chunks: RwLock::new(HashMap::new()),
            attachments: RwLock::new(HashMap::new()),
        }
    }

    /// Get statistics for metrics
    pub fn stats(&self) -> BlockMetaStats {
        let volumes = self.volumes.read();
        let snapshots = self.snapshots.read();
        let attachments = self.attachments.read();

        // Volumes by state
        let mut volumes_by_state: HashMap<String, u64> = HashMap::new();
        let mut volumes_provisioned_bytes = 0u64;
        let mut volumes_used_bytes = 0u64;
        let mut volume_entries = Vec::with_capacity(volumes.len());

        for vol in volumes.values() {
            let state_name = match vol.state {
                s if s == VolumeState::Creating as i32 => "creating",
                s if s == VolumeState::Available as i32 => "available",
                s if s == VolumeState::Attached as i32 => "attached",
                s if s == VolumeState::Error as i32 => "error",
                s if s == VolumeState::Deleting as i32 => "deleting",
                _ => "unknown",
            };
            *volumes_by_state.entry(state_name.to_string()).or_default() += 1;
            volumes_provisioned_bytes += vol.size_bytes;
            volumes_used_bytes += vol.used_bytes;
            volume_entries.push(VolumeStatEntry {
                volume_id: vol.volume_id.clone(),
                name: vol.name.clone(),
                pool: vol.pool.clone(),
                size_bytes: vol.size_bytes,
                used_bytes: vol.used_bytes,
                qos: vol.qos,
            });
        }

        // Snapshots
        let mut snapshots_space_bytes = 0u64;
        let mut snapshot_entries = Vec::with_capacity(snapshots.len());
        for snap in snapshots.values() {
            snapshots_space_bytes += snap.size_bytes;
            snapshot_entries.push(SnapshotStatEntry {
                snapshot_id: snap.snapshot_id.clone(),
                volume_id: snap.volume_id.clone(),
                name: snap.name.clone(),
                size_bytes: snap.size_bytes,
            });
        }

        // Attachments by type
        let mut attachments_by_type: HashMap<String, u64> = HashMap::new();
        for att in attachments.values() {
            let type_name = match att.target_type {
                t if t == TargetType::Iscsi as i32 => "iscsi",
                t if t == TargetType::Nvmeof as i32 => "nvmeof",
                t if t == TargetType::Nbd as i32 => "nbd",
                _ => "unknown",
            };
            *attachments_by_type.entry(type_name.to_string()).or_default() += 1;
        }

        BlockMetaStats {
            volume_count: volumes.len() as u64,
            snapshot_count: snapshots.len() as u64,
            attachment_count: attachments.len() as u64,
            volumes_by_state,
            volumes_provisioned_bytes,
            volumes_used_bytes,
            volumes: volume_entries,
            snapshots: snapshot_entries,
            snapshots_space_bytes,
            attachments_by_type,
        }
    }

    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn volume_to_proto(vol: &StoredVolume) -> Volume {
        Volume {
            volume_id: vol.volume_id.clone(),
            name: vol.name.clone(),
            size_bytes: vol.size_bytes,
            used_bytes: vol.used_bytes,
            pool: vol.pool.clone(),
            state: vol.state,
            created_at: vol.created_at,
            updated_at: vol.updated_at,
            parent_snapshot_id: vol.parent_snapshot_id.clone(),
            chunk_size_bytes: vol.chunk_size_bytes,
            metadata: vol.metadata.clone(),
            qos: vol.qos.clone(),
        }
    }

    fn snapshot_to_proto(snap: &StoredSnapshot) -> Snapshot {
        Snapshot {
            snapshot_id: snap.snapshot_id.clone(),
            volume_id: snap.volume_id.clone(),
            name: snap.name.clone(),
            size_bytes: snap.size_bytes,
            unique_bytes: snap.unique_bytes,
            state: snap.state,
            created_at: snap.created_at,
            metadata: snap.metadata.clone(),
        }
    }

    fn attachment_to_proto(att: &StoredAttachment) -> Attachment {
        Attachment {
            volume_id: att.volume_id.clone(),
            target_type: att.target_type,
            target_address: att.target_address.clone(),
            initiator: att.initiator.clone(),
            attached_at: att.attached_at,
            read_only: att.read_only,
        }
    }

    /// Get chunk reference for a volume
    pub fn get_chunk(&self, volume_id: &str, chunk_id: u64) -> Option<StoredChunkRef> {
        self.volume_chunks
            .read()
            .get(volume_id)
            .and_then(|chunks| chunks.get(&chunk_id).cloned())
    }

    /// Set chunk reference for a volume
    pub fn set_chunk(&self, volume_id: &str, chunk_id: u64, chunk_ref: StoredChunkRef) {
        if let Some(chunks) = self.volume_chunks.write().get_mut(volume_id) {
            // Update used_bytes on new chunk
            let is_new = !chunks.contains_key(&chunk_id);
            chunks.insert(chunk_id, chunk_ref.clone());

            if is_new {
                if let Some(vol) = self.volumes.write().get_mut(volume_id) {
                    vol.used_bytes += chunk_ref.size_bytes;
                    vol.updated_at = Self::current_timestamp();
                }
            }
        }
    }
}

impl Default for BlockMetaService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl BlockService for BlockMetaService {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("volume name is required"));
        }

        if req.size_bytes == 0 {
            return Err(Status::invalid_argument("size must be positive"));
        }

        // Check for duplicate name
        if self.volume_names.read().contains_key(&req.name) {
            return Err(Status::already_exists("volume with this name already exists"));
        }

        let volume_id = Uuid::new_v4().to_string();
        let now = Self::current_timestamp();
        let chunk_size = if req.chunk_size_bytes > 0 {
            req.chunk_size_bytes
        } else {
            DEFAULT_CHUNK_SIZE
        };

        let volume = StoredVolume {
            volume_id: volume_id.clone(),
            name: req.name.clone(),
            size_bytes: req.size_bytes,
            used_bytes: 0,
            pool: if req.pool.is_empty() { "default".to_string() } else { req.pool },
            state: VolumeState::Available as i32,
            created_at: now,
            updated_at: now,
            parent_snapshot_id: String::new(),
            chunk_size_bytes: chunk_size,
            metadata: req.metadata,
            qos: req.qos,
        };

        self.volumes.write().insert(volume_id.clone(), volume.clone());
        self.volume_names.write().insert(req.name.clone(), volume_id.clone());
        self.volume_chunks.write().insert(volume_id.clone(), HashMap::new());
        self.volume_snapshots.write().insert(volume_id.clone(), HashSet::new());

        info!("Created volume: {} ({})", req.name, volume_id);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Self::volume_to_proto(&volume)),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();

        let volume = self.volumes.read()
            .get(&req.volume_id)
            .cloned()
            .ok_or_else(|| Status::not_found("volume not found"))?;

        // Check if attached
        if volume.state == VolumeState::Attached as i32 && !req.force {
            return Err(Status::failed_precondition("volume is attached"));
        }

        // Check for snapshots
        let has_snapshots = self.volume_snapshots.read()
            .get(&req.volume_id)
            .map(|s| !s.is_empty())
            .unwrap_or(false);

        if has_snapshots && !req.force {
            return Err(Status::failed_precondition("volume has snapshots"));
        }

        // Remove volume
        self.volumes.write().remove(&req.volume_id);
        self.volume_names.write().remove(&volume.name);
        self.volume_chunks.write().remove(&req.volume_id);
        self.volume_snapshots.write().remove(&req.volume_id);
        self.attachments.write().remove(&req.volume_id);

        info!("Deleted volume: {} ({})", volume.name, req.volume_id);

        Ok(Response::new(DeleteVolumeResponse { success: true }))
    }

    async fn get_volume(
        &self,
        request: Request<GetVolumeRequest>,
    ) -> Result<Response<GetVolumeResponse>, Status> {
        let req = request.into_inner();

        let volume = self.volumes.read()
            .get(&req.volume_id)
            .cloned()
            .ok_or_else(|| Status::not_found("volume not found"))?;

        Ok(Response::new(GetVolumeResponse {
            volume: Some(Self::volume_to_proto(&volume)),
        }))
    }

    async fn list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let req = request.into_inner();
        let max_results = if req.max_results == 0 { 100 } else { req.max_results.min(1000) };

        let volumes: Vec<Volume> = self.volumes.read()
            .values()
            .filter(|v| req.pool.is_empty() || v.pool == req.pool)
            .filter(|v| req.marker.is_empty() || v.volume_id > req.marker)
            .take(max_results as usize + 1)
            .map(Self::volume_to_proto)
            .collect();

        let is_truncated = volumes.len() > max_results as usize;
        let volumes: Vec<Volume> = volumes.into_iter().take(max_results as usize).collect();
        let next_marker = volumes.last().map(|v| v.volume_id.clone()).unwrap_or_default();

        Ok(Response::new(ListVolumesResponse {
            volumes,
            next_marker: if is_truncated { next_marker } else { String::new() },
            is_truncated,
        }))
    }

    async fn resize_volume(
        &self,
        request: Request<ResizeVolumeRequest>,
    ) -> Result<Response<ResizeVolumeResponse>, Status> {
        let req = request.into_inner();

        let mut volumes = self.volumes.write();
        let volume = volumes.get_mut(&req.volume_id)
            .ok_or_else(|| Status::not_found("volume not found"))?;

        if volume.state == VolumeState::Attached as i32 {
            return Err(Status::failed_precondition("cannot resize attached volume"));
        }

        if req.new_size_bytes < volume.size_bytes {
            return Err(Status::invalid_argument("cannot shrink volume"));
        }

        volume.size_bytes = req.new_size_bytes;
        volume.updated_at = Self::current_timestamp();

        info!("Resized volume {} to {} bytes", req.volume_id, req.new_size_bytes);

        Ok(Response::new(ResizeVolumeResponse {
            volume: Some(Self::volume_to_proto(volume)),
        }))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();

        let volume = self.volumes.read()
            .get(&req.volume_id)
            .cloned()
            .ok_or_else(|| Status::not_found("volume not found"))?;

        // Copy current chunk refs
        let chunk_refs = self.volume_chunks.read()
            .get(&req.volume_id)
            .cloned()
            .unwrap_or_default();

        let snapshot_id = Uuid::new_v4().to_string();
        let now = Self::current_timestamp();

        let snapshot = StoredSnapshot {
            snapshot_id: snapshot_id.clone(),
            volume_id: req.volume_id.clone(),
            name: req.name.clone(),
            size_bytes: volume.size_bytes,
            unique_bytes: 0,
            state: SnapshotState::Available as i32,
            created_at: now,
            metadata: req.metadata,
            chunk_refs,
        };

        self.snapshots.write().insert(snapshot_id.clone(), snapshot.clone());
        if let Some(snaps) = self.volume_snapshots.write().get_mut(&req.volume_id) {
            snaps.insert(snapshot_id.clone());
        }

        info!("Created snapshot {} for volume {}", req.name, req.volume_id);

        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(Self::snapshot_to_proto(&snapshot)),
        }))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();

        let snapshot = self.snapshots.write().remove(&req.snapshot_id)
            .ok_or_else(|| Status::not_found("snapshot not found"))?;

        // Remove from volume's snapshot list
        if let Some(snaps) = self.volume_snapshots.write().get_mut(&snapshot.volume_id) {
            snaps.remove(&req.snapshot_id);
        }

        info!("Deleted snapshot {}", req.snapshot_id);

        Ok(Response::new(DeleteSnapshotResponse { success: true }))
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let req = request.into_inner();

        let snapshot = self.snapshots.read()
            .get(&req.snapshot_id)
            .cloned()
            .ok_or_else(|| Status::not_found("snapshot not found"))?;

        Ok(Response::new(GetSnapshotResponse {
            snapshot: Some(Self::snapshot_to_proto(&snapshot)),
        }))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let req = request.into_inner();
        let max_results = if req.max_results == 0 { 100 } else { req.max_results.min(1000) };

        let snapshots: Vec<Snapshot> = self.snapshots.read()
            .values()
            .filter(|s| req.volume_id.is_empty() || s.volume_id == req.volume_id)
            .filter(|s| req.marker.is_empty() || s.snapshot_id > req.marker)
            .take(max_results as usize + 1)
            .map(Self::snapshot_to_proto)
            .collect();

        let is_truncated = snapshots.len() > max_results as usize;
        let snapshots: Vec<Snapshot> = snapshots.into_iter().take(max_results as usize).collect();
        let next_marker = snapshots.last().map(|s| s.snapshot_id.clone()).unwrap_or_default();

        Ok(Response::new(ListSnapshotsResponse {
            snapshots,
            next_marker: if is_truncated { next_marker } else { String::new() },
            is_truncated,
        }))
    }

    async fn clone_volume(
        &self,
        request: Request<CloneVolumeRequest>,
    ) -> Result<Response<CloneVolumeResponse>, Status> {
        let req = request.into_inner();

        let snapshot = self.snapshots.read()
            .get(&req.snapshot_id)
            .cloned()
            .ok_or_else(|| Status::not_found("snapshot not found"))?;

        // Check for duplicate name
        if self.volume_names.read().contains_key(&req.name) {
            return Err(Status::already_exists("volume with this name already exists"));
        }

        let volume_id = Uuid::new_v4().to_string();
        let now = Self::current_timestamp();

        // Get parent volume's chunk size
        let chunk_size = self.volumes.read()
            .get(&snapshot.volume_id)
            .map(|v| v.chunk_size_bytes)
            .unwrap_or(DEFAULT_CHUNK_SIZE);

        // Get parent volume's QoS settings
        let parent_qos = self.volumes.read()
            .get(&snapshot.volume_id)
            .and_then(|v| v.qos.clone());

        let volume = StoredVolume {
            volume_id: volume_id.clone(),
            name: req.name.clone(),
            size_bytes: snapshot.size_bytes,
            used_bytes: snapshot.unique_bytes,
            pool: "default".to_string(),
            state: VolumeState::Available as i32,
            created_at: now,
            updated_at: now,
            parent_snapshot_id: req.snapshot_id.clone(),
            chunk_size_bytes: chunk_size,
            metadata: req.metadata,
            qos: parent_qos,
        };

        // Copy snapshot's chunk refs to new volume
        self.volumes.write().insert(volume_id.clone(), volume.clone());
        self.volume_names.write().insert(req.name.clone(), volume_id.clone());
        self.volume_chunks.write().insert(volume_id.clone(), snapshot.chunk_refs);
        self.volume_snapshots.write().insert(volume_id.clone(), HashSet::new());

        info!("Cloned volume {} from snapshot {}", req.name, req.snapshot_id);

        Ok(Response::new(CloneVolumeResponse {
            volume: Some(Self::volume_to_proto(&volume)),
        }))
    }

    async fn attach_volume(
        &self,
        request: Request<AttachVolumeRequest>,
    ) -> Result<Response<AttachVolumeResponse>, Status> {
        let req = request.into_inner();

        // Update volume state
        {
            let mut volumes = self.volumes.write();
            let volume = volumes.get_mut(&req.volume_id)
                .ok_or_else(|| Status::not_found("volume not found"))?;

            if volume.state == VolumeState::Attached as i32 {
                return Err(Status::failed_precondition("volume already attached"));
            }

            volume.state = VolumeState::Attached as i32;
            volume.updated_at = Self::current_timestamp();
        }

        // Generate target address based on type
        let target_address = match req.target_type {
            t if t == TargetType::Iscsi as i32 => {
                format!("iqn.2024-01.io.objectio:{}", req.volume_id)
            }
            t if t == TargetType::Nvmeof as i32 => {
                format!("nqn.2024-01.io.objectio:{}:uuid:{}", req.volume_id, Uuid::new_v4())
            }
            t if t == TargetType::Nbd as i32 => {
                format!("/dev/nbd/{}", req.volume_id)
            }
            _ => {
                return Err(Status::invalid_argument("unknown target type"));
            }
        };

        let attachment = StoredAttachment {
            volume_id: req.volume_id.clone(),
            target_type: req.target_type,
            target_address: target_address.clone(),
            initiator: req.initiator,
            attached_at: Self::current_timestamp(),
            read_only: req.read_only,
        };

        self.attachments.write().insert(req.volume_id.clone(), attachment.clone());

        info!("Attached volume {} as {}", req.volume_id, target_address);

        Ok(Response::new(AttachVolumeResponse {
            attachment: Some(Self::attachment_to_proto(&attachment)),
        }))
    }

    async fn detach_volume(
        &self,
        request: Request<DetachVolumeRequest>,
    ) -> Result<Response<DetachVolumeResponse>, Status> {
        let req = request.into_inner();

        // Update volume state
        {
            let mut volumes = self.volumes.write();
            let volume = volumes.get_mut(&req.volume_id)
                .ok_or_else(|| Status::not_found("volume not found"))?;

            if volume.state != VolumeState::Attached as i32 && !req.force {
                return Err(Status::failed_precondition("volume not attached"));
            }

            volume.state = VolumeState::Available as i32;
            volume.updated_at = Self::current_timestamp();
        }

        self.attachments.write().remove(&req.volume_id);

        info!("Detached volume {}", req.volume_id);

        Ok(Response::new(DetachVolumeResponse { success: true }))
    }

    async fn list_attachments(
        &self,
        request: Request<ListAttachmentsRequest>,
    ) -> Result<Response<ListAttachmentsResponse>, Status> {
        let req = request.into_inner();

        let attachments: Vec<Attachment> = self.attachments.read()
            .values()
            .filter(|a| req.volume_id.is_empty() || a.volume_id == req.volume_id)
            .map(Self::attachment_to_proto)
            .collect();

        Ok(Response::new(ListAttachmentsResponse { attachments }))
    }

    // I/O operations - these would typically be handled by a block gateway, not metadata service
    // But we provide stubs for completeness

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        warn!("Read called on metadata service - should use block gateway: volume={}", req.volume_id);

        // This should be handled by block gateway, not metadata service
        Err(Status::unimplemented(
            "I/O operations should be performed via block gateway, not metadata service"
        ))
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        warn!("Write called on metadata service - should use block gateway: volume={}", req.volume_id);

        Err(Status::unimplemented(
            "I/O operations should be performed via block gateway, not metadata service"
        ))
    }

    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let req = request.into_inner();
        debug!("Flush requested for volume {}", req.volume_id);

        // Metadata service doesn't cache I/O - this is a no-op
        Ok(Response::new(FlushResponse { success: true }))
    }

    async fn trim(
        &self,
        request: Request<TrimRequest>,
    ) -> Result<Response<TrimResponse>, Status> {
        let req = request.into_inner();

        // Remove chunk refs in the trimmed range
        let chunk_size = self.volumes.read()
            .get(&req.volume_id)
            .map(|v| v.chunk_size_bytes as u64)
            .unwrap_or(DEFAULT_CHUNK_SIZE as u64);

        let start_chunk = req.offset_bytes / chunk_size;
        let end_chunk = (req.offset_bytes + req.length_bytes + chunk_size - 1) / chunk_size;

        if let Some(chunks) = self.volume_chunks.write().get_mut(&req.volume_id) {
            for chunk_id in start_chunk..end_chunk {
                chunks.remove(&chunk_id);
            }
        }

        debug!("Trimmed volume {} range {}+{}", req.volume_id, req.offset_bytes, req.length_bytes);

        Ok(Response::new(TrimResponse { success: true }))
    }

    // ============ QoS Operations ============

    async fn update_volume_qos(
        &self,
        request: Request<UpdateVolumeQosRequest>,
    ) -> Result<Response<UpdateVolumeQosResponse>, Status> {
        let req = request.into_inner();

        let mut volumes = self.volumes.write();
        let volume = volumes.get_mut(&req.volume_id)
            .ok_or_else(|| Status::not_found("volume not found"))?;

        volume.qos = req.qos;
        volume.updated_at = Self::current_timestamp();

        info!("Updated QoS for volume {}", req.volume_id);

        Ok(Response::new(UpdateVolumeQosResponse {
            volume: Some(Self::volume_to_proto(volume)),
        }))
    }

    async fn get_volume_stats(
        &self,
        request: Request<GetVolumeStatsRequest>,
    ) -> Result<Response<GetVolumeStatsResponse>, Status> {
        let req = request.into_inner();

        // Verify volume exists
        if !self.volumes.read().contains_key(&req.volume_id) {
            return Err(Status::not_found("volume not found"));
        }

        // Volume stats are tracked by OSDs, not metadata service
        // Return empty stats - the block gateway should aggregate from OSDs
        warn!("GetVolumeStats called on metadata service - stats should be aggregated from OSDs");

        Ok(Response::new(GetVolumeStatsResponse {
            stats: None,
        }))
    }

    // ============ Metrics Operations ============
    // These are typically handled by OSDs or a dedicated metrics aggregator
    // The metadata service provides stubs for completeness

    async fn get_osd_metrics(
        &self,
        request: Request<GetOsdMetricsRequest>,
    ) -> Result<Response<GetOsdMetricsResponse>, Status> {
        let req = request.into_inner();
        warn!("GetOsdMetrics called on metadata service - should query OSD {} directly", req.osd_id);

        // Metadata service doesn't track OSD metrics
        Err(Status::unimplemented(
            "OSD metrics should be queried from the OSD directly, not the metadata service"
        ))
    }

    async fn list_osd_metrics(
        &self,
        _request: Request<ListOsdMetricsRequest>,
    ) -> Result<Response<ListOsdMetricsResponse>, Status> {
        warn!("ListOsdMetrics called on metadata service - should query OSDs directly");

        // Metadata service doesn't track OSD metrics
        Err(Status::unimplemented(
            "OSD metrics should be queried from OSDs directly, not the metadata service"
        ))
    }

    async fn get_cluster_metrics(
        &self,
        _request: Request<GetClusterMetricsRequest>,
    ) -> Result<Response<GetClusterMetricsResponse>, Status> {
        warn!("GetClusterMetrics called on metadata service - should use metrics aggregator");

        // Return basic cluster metrics that metadata service can provide
        let volumes = self.volumes.read();
        let snapshots = self.snapshots.read();
        let attachments = self.attachments.read();

        let total_volumes = volumes.len() as u32;
        let attached_volumes = attachments.len() as u32;
        let total_snapshots = snapshots.len() as u32;
        let total_provisioned: u64 = volumes.values().map(|v| v.size_bytes).sum();

        use objectio_proto::block::{ClusterMetrics, HealthStatus};

        Ok(Response::new(GetClusterMetricsResponse {
            metrics: Some(ClusterMetrics {
                timestamp: Self::current_timestamp() * 1000, // Convert to milliseconds
                overall_status: HealthStatus::Healthy as i32,
                total_osds: 0, // Would need OSD registry
                healthy_osds: 0,
                warning_osds: 0,
                error_osds: 0,
                total_capacity_bytes: 0,
                used_capacity_bytes: 0,
                available_capacity_bytes: 0,
                capacity_utilization: 0.0,
                total_read_iops: 0,
                total_write_iops: 0,
                total_read_bandwidth_bps: 0,
                total_write_bandwidth_bps: 0,
                cluster_read_latency: None,
                cluster_write_latency: None,
                total_volumes,
                attached_volumes,
                total_provisioned_bytes: total_provisioned,
                total_throttled_ios: 0,
                total_reserved_iops: 0,
                total_available_iops: 0,
                degraded_objects: 0,
                recovering_objects: 0,
                recovery_rate_bps: 0,
                total_snapshots,
                snapshot_space_bytes: 0,
            }),
        }))
    }

    async fn get_io_trace(
        &self,
        request: Request<GetIoTraceRequest>,
    ) -> Result<Response<GetIoTraceResponse>, Status> {
        let req = request.into_inner();
        warn!("GetIoTrace called on metadata service - trace_id={:?} volume_id={:?}",
            if req.trace_id.is_empty() { None } else { Some(&req.trace_id) },
            if req.volume_id.is_empty() { None } else { Some(&req.volume_id) }
        );

        // I/O traces are tracked by the block gateway/OSDs, not metadata service
        Err(Status::unimplemented(
            "I/O traces should be queried from the block gateway or OSDs"
        ))
    }
}
