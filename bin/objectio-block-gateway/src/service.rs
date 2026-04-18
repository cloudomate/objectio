//! gRPC BlockService implementation

use std::sync::Arc;

use objectio_block::volume::VolumeState;
use objectio_block::{VolumeManager, WriteCache};
use objectio_proto::block::block_service_server::BlockService;
use objectio_proto::block::{
    AttachVolumeRequest, AttachVolumeResponse, Attachment, CloneVolumeRequest, CloneVolumeResponse,
    CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse,
    DeleteSnapshotRequest, DeleteSnapshotResponse, DeleteVolumeRequest, DeleteVolumeResponse,
    DetachVolumeRequest, DetachVolumeResponse, FlushRequest, FlushResponse,
    GetClusterMetricsRequest, GetClusterMetricsResponse, GetIoTraceRequest, GetIoTraceResponse,
    GetOsdMetricsRequest, GetOsdMetricsResponse, GetSnapshotRequest, GetSnapshotResponse,
    GetVolumeRequest, GetVolumeResponse, GetVolumeStatsRequest, GetVolumeStatsResponse,
    ListAttachmentsRequest, ListAttachmentsResponse, ListOsdMetricsRequest, ListOsdMetricsResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, ListVolumesRequest, ListVolumesResponse,
    ReadRequest, ReadResponse, ResizeVolumeRequest, ResizeVolumeResponse,
    Snapshot as ProtoSnapshot, TargetType, TrimRequest, TrimResponse, UpdateVolumeQosRequest,
    UpdateVolumeQosResponse, Volume as ProtoVolume, VolumeStats, WriteRequest, WriteResponse,
};
use objectio_proto::metadata::metadata_service_client::MetadataServiceClient;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Channel};
use tracing::{info, warn};

use crate::ec_io::read_chunk;
use crate::flush::flush_volume_all;
use crate::nbd::NbdServer;
use crate::osd_pool::OsdPool;
use crate::store::BlockStore;

// ── Shared state ──────────────────────────────────────────────────────────────

pub struct BlockGatewayState {
    pub meta_client: Arc<Mutex<MetadataServiceClient<Channel>>>,
    pub osd_pool: Arc<OsdPool>,
    pub cache: Arc<WriteCache>,
    pub store: Arc<BlockStore>,
    pub volume_manager: Arc<VolumeManager>,
    pub nbd_server: Arc<NbdServer>,
    pub advertise_host: String,
    pub nbd_port: u16,
    pub ec_k: u32,
    pub ec_m: u32,
}

// ── Service ───────────────────────────────────────────────────────────────────

pub struct BlockGatewayService {
    state: Arc<BlockGatewayState>,
}

impl BlockGatewayService {
    pub fn new(state: Arc<BlockGatewayState>) -> Self {
        Self { state }
    }
}

// ── Conversions ───────────────────────────────────────────────────────────────

fn volume_to_proto(v: &objectio_block::volume::Volume) -> ProtoVolume {
    ProtoVolume {
        volume_id: v.volume_id.clone(),
        name: v.name.clone(),
        size_bytes: v.size_bytes,
        used_bytes: v.used_bytes,
        pool: v.pool.clone(),
        state: i32::from(v.state),
        created_at: v.created_at,
        updated_at: v.updated_at,
        parent_snapshot_id: v.parent_snapshot_id.clone().unwrap_or_default(),
        chunk_size_bytes: v.chunk_size as u32,
        metadata: v.metadata.clone(),
        qos: None,
    }
}

fn snapshot_to_proto(s: &objectio_block::volume::Snapshot) -> ProtoSnapshot {
    ProtoSnapshot {
        snapshot_id: s.snapshot_id.clone(),
        volume_id: s.volume_id.clone(),
        name: s.name.clone(),
        size_bytes: s.size_bytes,
        unique_bytes: s.unique_bytes,
        state: 2, // SnapshotState::Available
        created_at: s.created_at,
        metadata: s.metadata.clone(),
    }
}

fn block_err_to_status(e: objectio_block::error::BlockError) -> Status {
    use objectio_block::error::BlockError;
    match e {
        BlockError::VolumeNotFound(id) => Status::not_found(format!("volume not found: {id}")),
        BlockError::SnapshotNotFound(id) => Status::not_found(format!("snapshot not found: {id}")),
        BlockError::VolumeExists(name) => {
            Status::already_exists(format!("volume already exists: {name}"))
        }
        BlockError::VolumeAttached(id) => {
            Status::failed_precondition(format!("volume attached: {id}"))
        }
        BlockError::VolumeHasSnapshots(id) => {
            Status::failed_precondition(format!("volume has snapshots: {id}"))
        }
        other => Status::internal(other.to_string()),
    }
}

// ── BlockService impl ─────────────────────────────────────────────────────────

#[tonic::async_trait]
impl BlockService for BlockGatewayService {
    // ── Volume CRUD ───────────────────────────────────────────────────────────

    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        let vm = &self.state.volume_manager;

        let vol = vm
            .create_volume(req.name, req.size_bytes, req.pool)
            .map_err(block_err_to_status)?;

        // Init cache slot
        self.state.cache.init_volume(&vol.volume_id);

        // Persist
        if let Err(e) = self.state.store.save_volume(&vol) {
            warn!("Failed to persist volume {}: {e}", vol.volume_id);
        }

        info!("Created volume {} ({}B)", vol.volume_id, vol.size_bytes);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(volume_to_proto(&vol)),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        let vm = &self.state.volume_manager;

        vm.delete_volume(&req.volume_id, req.force)
            .map_err(block_err_to_status)?;

        self.state.cache.remove_volume(&req.volume_id);

        if let Err(e) = self.state.store.delete_volume(&req.volume_id) {
            warn!("Failed to delete volume record {}: {e}", req.volume_id);
        }

        info!("Deleted volume {}", req.volume_id);

        Ok(Response::new(DeleteVolumeResponse { success: true }))
    }

    async fn get_volume(
        &self,
        request: Request<GetVolumeRequest>,
    ) -> Result<Response<GetVolumeResponse>, Status> {
        let req = request.into_inner();
        let vol = self
            .state
            .volume_manager
            .get_volume(&req.volume_id)
            .map_err(block_err_to_status)?;

        Ok(Response::new(GetVolumeResponse {
            volume: Some(volume_to_proto(&vol)),
        }))
    }

    async fn list_volumes(
        &self,
        _request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let volumes = self
            .state
            .volume_manager
            .list_volumes()
            .iter()
            .map(volume_to_proto)
            .collect();

        Ok(Response::new(ListVolumesResponse {
            volumes,
            next_marker: String::new(),
            is_truncated: false,
        }))
    }

    async fn resize_volume(
        &self,
        request: Request<ResizeVolumeRequest>,
    ) -> Result<Response<ResizeVolumeResponse>, Status> {
        let req = request.into_inner();
        let vol = self
            .state
            .volume_manager
            .resize_volume(&req.volume_id, req.new_size_bytes)
            .map_err(block_err_to_status)?;

        if let Err(e) = self.state.store.save_volume(&vol) {
            warn!("Failed to persist resized volume {}: {e}", vol.volume_id);
        }

        Ok(Response::new(ResizeVolumeResponse {
            volume: Some(volume_to_proto(&vol)),
        }))
    }

    async fn update_volume_qos(
        &self,
        request: Request<UpdateVolumeQosRequest>,
    ) -> Result<Response<UpdateVolumeQosResponse>, Status> {
        let req = request.into_inner();
        let vol = self
            .state
            .volume_manager
            .get_volume(&req.volume_id)
            .map_err(block_err_to_status)?;

        Ok(Response::new(UpdateVolumeQosResponse {
            volume: Some(volume_to_proto(&vol)),
        }))
    }

    async fn get_volume_stats(
        &self,
        request: Request<GetVolumeStatsRequest>,
    ) -> Result<Response<GetVolumeStatsResponse>, Status> {
        let req = request.into_inner();

        // Validate volume exists
        self.state
            .volume_manager
            .get_volume(&req.volume_id)
            .map_err(block_err_to_status)?;

        Ok(Response::new(GetVolumeStatsResponse {
            stats: Some(VolumeStats {
                volume_id: req.volume_id,
                ..Default::default()
            }),
        }))
    }

    // ── Snapshot CRUD ─────────────────────────────────────────────────────────

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        let snap = self
            .state
            .volume_manager
            .create_snapshot(&req.volume_id, req.name)
            .map_err(block_err_to_status)?;

        if let Err(e) = self.state.store.save_snapshot(&snap) {
            warn!("Failed to persist snapshot {}: {e}", snap.snapshot_id);
        }

        info!(
            "Created snapshot {} for volume {}",
            snap.snapshot_id, snap.volume_id
        );

        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(snapshot_to_proto(&snap)),
        }))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();

        self.state
            .volume_manager
            .delete_snapshot(&req.snapshot_id)
            .map_err(block_err_to_status)?;

        if let Err(e) = self.state.store.delete_snapshot(&req.snapshot_id) {
            warn!("Failed to delete snapshot record {}: {e}", req.snapshot_id);
        }

        Ok(Response::new(DeleteSnapshotResponse { success: true }))
    }

    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let req = request.into_inner();
        let snap = self
            .state
            .volume_manager
            .get_snapshot(&req.snapshot_id)
            .map_err(block_err_to_status)?;

        Ok(Response::new(GetSnapshotResponse {
            snapshot: Some(snapshot_to_proto(&snap)),
        }))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let req = request.into_inner();

        // Validate volume exists if filter provided
        if !req.volume_id.is_empty() {
            self.state
                .volume_manager
                .get_volume(&req.volume_id)
                .map_err(block_err_to_status)?;
        }

        let snapshots = self
            .state
            .volume_manager
            .list_snapshots(&req.volume_id)
            .iter()
            .map(snapshot_to_proto)
            .collect();

        Ok(Response::new(ListSnapshotsResponse {
            snapshots,
            next_marker: String::new(),
            is_truncated: false,
        }))
    }

    async fn clone_volume(
        &self,
        request: Request<CloneVolumeRequest>,
    ) -> Result<Response<CloneVolumeResponse>, Status> {
        let req = request.into_inner();
        let vol = self
            .state
            .volume_manager
            .clone_from_snapshot(&req.snapshot_id, req.name, None)
            .map_err(block_err_to_status)?;

        self.state.cache.init_volume(&vol.volume_id);

        if let Err(e) = self.state.store.save_volume(&vol) {
            warn!("Failed to persist cloned volume {}: {e}", vol.volume_id);
        }

        info!(
            "Cloned volume {} from snapshot {}",
            vol.volume_id, req.snapshot_id
        );

        Ok(Response::new(CloneVolumeResponse {
            volume: Some(volume_to_proto(&vol)),
        }))
    }

    // ── Attachment ────────────────────────────────────────────────────────────

    async fn attach_volume(
        &self,
        request: Request<AttachVolumeRequest>,
    ) -> Result<Response<AttachVolumeResponse>, Status> {
        let req = request.into_inner();
        let vol = self
            .state
            .volume_manager
            .get_volume(&req.volume_id)
            .map_err(block_err_to_status)?;

        if !vol.can_attach() {
            return Err(Status::failed_precondition(format!(
                "volume {} is not in Available state",
                req.volume_id
            )));
        }

        let target_type = TargetType::try_from(req.target_type).unwrap_or(TargetType::Nbd);
        let read_only = req.read_only;

        let target_address = if target_type == TargetType::Nbd {
            // Register with NBD server and return connection string
            self.state
                .nbd_server
                .register(&req.volume_id, vol.size_bytes, read_only);
            format!(
                "nbd://{}:{}/{}",
                self.state.advertise_host, self.state.nbd_port, req.volume_id
            )
        } else {
            return Err(Status::unimplemented("only NBD target type is supported"));
        };

        self.state
            .volume_manager
            .set_volume_state(&req.volume_id, VolumeState::Attached)
            .map_err(block_err_to_status)?;

        if let Ok(updated) = self.state.volume_manager.get_volume(&req.volume_id) {
            let _ = self.state.store.save_volume(&updated);
        }

        info!("Attached volume {} → {}", req.volume_id, target_address);

        Ok(Response::new(AttachVolumeResponse {
            attachment: Some(Attachment {
                volume_id: req.volume_id,
                target_type: target_type.into(),
                target_address,
                initiator: req.initiator,
                attached_at: chrono::Utc::now().timestamp_millis() as u64,
                read_only,
            }),
        }))
    }

    async fn detach_volume(
        &self,
        request: Request<DetachVolumeRequest>,
    ) -> Result<Response<DetachVolumeResponse>, Status> {
        let req = request.into_inner();

        // Flush all dirty data before detach
        flush_volume_all(&req.volume_id, &self.state).await;

        // Unregister from NBD
        self.state.nbd_server.unregister(&req.volume_id);

        self.state
            .volume_manager
            .set_volume_state(&req.volume_id, VolumeState::Available)
            .map_err(block_err_to_status)?;

        if let Ok(updated) = self.state.volume_manager.get_volume(&req.volume_id) {
            let _ = self.state.store.save_volume(&updated);
        }

        info!("Detached volume {}", req.volume_id);

        Ok(Response::new(DetachVolumeResponse { success: true }))
    }

    async fn list_attachments(
        &self,
        request: Request<ListAttachmentsRequest>,
    ) -> Result<Response<ListAttachmentsResponse>, Status> {
        let req = request.into_inner();

        let attachments = self.state.nbd_server.list_attachments(&req.volume_id);

        Ok(Response::new(ListAttachmentsResponse { attachments }))
    }

    // ── Direct I/O ────────────────────────────────────────────────────────────

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();

        // Try cache first
        if let Some(data) =
            self.state
                .cache
                .read(&req.volume_id, req.offset_bytes, req.length_bytes as u64)
        {
            return Ok(Response::new(ReadResponse { data }));
        }

        // Cache miss: need to determine which chunk(s) and read from EC
        let chunk_mapper = self.state.volume_manager.chunk_mapper();
        let ranges = chunk_mapper.byte_range_to_chunks(req.offset_bytes, req.length_bytes as u64);

        let mut result = vec![0u8; req.length_bytes as usize];
        let mut result_offset = 0usize;

        for range in &ranges {
            let object_key = self
                .state
                .store
                .get_chunk(&req.volume_id, range.chunk_id)
                .map_err(|e| Status::internal(e.to_string()))?;

            let chunk_data = if let Some(key) = object_key {
                // Read from EC storage
                read_chunk(
                    Arc::clone(&self.state.meta_client),
                    &self.state.osd_pool,
                    &key,
                    self.state.ec_k,
                    self.state.ec_m,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            } else {
                // Chunk never written = sparse zero region
                vec![0u8; chunk_mapper.chunk_size() as usize]
            };

            // Add to clean cache for future reads
            let chunk_size = chunk_mapper.chunk_size();
            self.state.cache.add_clean(
                &req.volume_id,
                range.chunk_id,
                bytes::Bytes::from(chunk_data.clone()),
            );

            // Copy the requested range from this chunk
            let start = range.offset_in_chunk as usize;
            let end = (range.offset_in_chunk + range.length) as usize;
            let src = &chunk_data[start..end.min(chunk_data.len()).min(chunk_size as usize)];
            let dst_end = result_offset + src.len();
            if dst_end <= result.len() {
                result[result_offset..dst_end].copy_from_slice(src);
            }
            result_offset += src.len();
        }

        Ok(Response::new(ReadResponse { data: result }))
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let len = req.data.len() as u32;

        self.state
            .cache
            .write(&req.volume_id, req.offset_bytes, &req.data)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(WriteResponse { bytes_written: len }))
    }

    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let req = request.into_inner();
        flush_volume_all(&req.volume_id, &self.state).await;
        Ok(Response::new(FlushResponse { success: true }))
    }

    async fn trim(&self, request: Request<TrimRequest>) -> Result<Response<TrimResponse>, Status> {
        let req = request.into_inner();

        // Zero-fill the trimmed range in cache
        let zeros = vec![0u8; req.length_bytes as usize];
        if let Err(e) = self
            .state
            .cache
            .write(&req.volume_id, req.offset_bytes, &zeros)
        {
            warn!("Trim write-zero failed for vol {}: {e}", req.volume_id);
        }

        Ok(Response::new(TrimResponse { success: true }))
    }

    // ── Metrics (stubs) ───────────────────────────────────────────────────────

    async fn get_osd_metrics(
        &self,
        _request: Request<GetOsdMetricsRequest>,
    ) -> Result<Response<GetOsdMetricsResponse>, Status> {
        Ok(Response::new(GetOsdMetricsResponse { metrics: None }))
    }

    async fn list_osd_metrics(
        &self,
        _request: Request<ListOsdMetricsRequest>,
    ) -> Result<Response<ListOsdMetricsResponse>, Status> {
        Ok(Response::new(ListOsdMetricsResponse { osds: vec![] }))
    }

    async fn get_cluster_metrics(
        &self,
        _request: Request<GetClusterMetricsRequest>,
    ) -> Result<Response<GetClusterMetricsResponse>, Status> {
        Ok(Response::new(GetClusterMetricsResponse { metrics: None }))
    }

    async fn get_io_trace(
        &self,
        _request: Request<GetIoTraceRequest>,
    ) -> Result<Response<GetIoTraceResponse>, Status> {
        Ok(Response::new(GetIoTraceResponse { traces: vec![] }))
    }
}
