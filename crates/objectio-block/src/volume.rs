//! Volume management for block storage

use crate::DEFAULT_CHUNK_SIZE;
use crate::chunk::{ChunkId, ChunkMapper};
use crate::error::{BlockError, BlockResult};

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

/// Volume state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeState {
    /// Volume is being created
    Creating,
    /// Volume is available for use
    Available,
    /// Volume is attached to a target
    Attached,
    /// Volume is in an error state
    Error,
    /// Volume is being deleted
    Deleting,
}

impl From<i32> for VolumeState {
    fn from(value: i32) -> Self {
        match value {
            1 => VolumeState::Creating,
            2 => VolumeState::Available,
            3 => VolumeState::Attached,
            4 => VolumeState::Error,
            5 => VolumeState::Deleting,
            _ => VolumeState::Available,
        }
    }
}

impl From<VolumeState> for i32 {
    fn from(state: VolumeState) -> i32 {
        match state {
            VolumeState::Creating => 1,
            VolumeState::Available => 2,
            VolumeState::Attached => 3,
            VolumeState::Error => 4,
            VolumeState::Deleting => 5,
        }
    }
}

/// Volume metadata
#[derive(Debug, Clone)]
pub struct Volume {
    /// Unique volume ID
    pub volume_id: String,
    /// Human-readable name
    pub name: String,
    /// Provisioned size in bytes
    pub size_bytes: u64,
    /// Actual used bytes (for thin provisioning)
    pub used_bytes: u64,
    /// Storage pool
    pub pool: String,
    /// Current state
    pub state: VolumeState,
    /// Creation timestamp
    pub created_at: u64,
    /// Last update timestamp
    pub updated_at: u64,
    /// Parent snapshot ID (for clones)
    pub parent_snapshot_id: Option<String>,
    /// Chunk size in bytes
    pub chunk_size: u64,
    /// User-defined metadata
    pub metadata: HashMap<String, String>,
}

impl Volume {
    /// Create a new volume
    pub fn new(name: String, size_bytes: u64, pool: String) -> Self {
        let now = chrono::Utc::now().timestamp() as u64;
        Self {
            volume_id: Uuid::new_v4().to_string(),
            name,
            size_bytes,
            used_bytes: 0,
            pool,
            state: VolumeState::Creating,
            created_at: now,
            updated_at: now,
            parent_snapshot_id: None,
            chunk_size: DEFAULT_CHUNK_SIZE,
            metadata: HashMap::new(),
        }
    }

    /// Get the number of chunks in this volume
    pub fn chunk_count(&self) -> u64 {
        (self.size_bytes + self.chunk_size - 1) / self.chunk_size
    }

    /// Check if the volume can be modified
    pub fn can_modify(&self) -> bool {
        matches!(self.state, VolumeState::Available)
    }

    /// Check if the volume can be attached
    pub fn can_attach(&self) -> bool {
        matches!(self.state, VolumeState::Available)
    }

    /// Check if the volume can be deleted
    pub fn can_delete(&self) -> bool {
        matches!(self.state, VolumeState::Available)
    }
}

/// Snapshot metadata
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Unique snapshot ID
    pub snapshot_id: String,
    /// Source volume ID
    pub volume_id: String,
    /// Human-readable name
    pub name: String,
    /// Logical size (same as source volume at snapshot time)
    pub size_bytes: u64,
    /// Data unique to this snapshot
    pub unique_bytes: u64,
    /// Creation timestamp
    pub created_at: u64,
    /// User-defined metadata
    pub metadata: HashMap<String, String>,
    /// Chunk references at snapshot time
    pub chunk_refs: HashMap<ChunkId, ChunkRef>,
}

/// Reference to a chunk's data
#[derive(Debug, Clone)]
pub struct ChunkRef {
    /// Object key in storage
    pub object_key: String,
    /// ETag for consistency check
    pub etag: String,
    /// Actual data size
    pub size: u64,
}

/// Manages volumes and snapshots
pub struct VolumeManager {
    /// Volumes by ID
    volumes: RwLock<HashMap<String, Volume>>,
    /// Volume names to IDs
    volume_names: RwLock<HashMap<String, String>>,
    /// Snapshots by ID
    snapshots: RwLock<HashMap<String, Snapshot>>,
    /// Snapshots by volume ID
    volume_snapshots: RwLock<HashMap<String, HashSet<String>>>,
    /// Allocated chunks per volume (sparse: only tracks allocated chunks)
    volume_chunks: RwLock<HashMap<String, HashMap<ChunkId, ChunkRef>>>,
    /// Chunk mapper
    chunk_mapper: Arc<ChunkMapper>,
}

impl VolumeManager {
    /// Create a new volume manager
    pub fn new() -> Self {
        Self {
            volumes: RwLock::new(HashMap::new()),
            volume_names: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
            volume_snapshots: RwLock::new(HashMap::new()),
            volume_chunks: RwLock::new(HashMap::new()),
            chunk_mapper: Arc::new(ChunkMapper::default()),
        }
    }

    /// Create a new volume manager with custom chunk size
    pub fn with_chunk_size(chunk_size: u64) -> Self {
        Self {
            volumes: RwLock::new(HashMap::new()),
            volume_names: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
            volume_snapshots: RwLock::new(HashMap::new()),
            volume_chunks: RwLock::new(HashMap::new()),
            chunk_mapper: Arc::new(ChunkMapper::new(chunk_size)),
        }
    }

    /// Get the chunk mapper
    pub fn chunk_mapper(&self) -> Arc<ChunkMapper> {
        self.chunk_mapper.clone()
    }

    /// Create a new volume
    pub fn create_volume(
        &self,
        name: String,
        size_bytes: u64,
        pool: String,
    ) -> BlockResult<Volume> {
        // Validate size
        if size_bytes == 0 {
            return Err(BlockError::InvalidSize("Size must be positive".to_string()));
        }

        // Check for duplicate name
        if self.volume_names.read().contains_key(&name) {
            return Err(BlockError::VolumeExists(name));
        }

        // Create volume
        let mut volume = Volume::new(name.clone(), size_bytes, pool);
        volume.chunk_size = self.chunk_mapper.chunk_size();
        volume.state = VolumeState::Available;

        let volume_id = volume.volume_id.clone();

        // Store volume
        self.volumes
            .write()
            .insert(volume_id.clone(), volume.clone());
        self.volume_names.write().insert(name, volume_id.clone());
        self.volume_chunks
            .write()
            .insert(volume_id.clone(), HashMap::new());
        self.volume_snapshots
            .write()
            .insert(volume_id, HashSet::new());

        Ok(volume)
    }

    /// Get a volume by ID
    pub fn get_volume(&self, volume_id: &str) -> BlockResult<Volume> {
        self.volumes
            .read()
            .get(volume_id)
            .cloned()
            .ok_or_else(|| BlockError::VolumeNotFound(volume_id.to_string()))
    }

    /// Get a volume by name
    pub fn get_volume_by_name(&self, name: &str) -> BlockResult<Volume> {
        let volume_id = self
            .volume_names
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| BlockError::VolumeNotFound(name.to_string()))?;
        self.get_volume(&volume_id)
    }

    /// List all volumes
    pub fn list_volumes(&self) -> Vec<Volume> {
        self.volumes.read().values().cloned().collect()
    }

    /// Delete a volume
    pub fn delete_volume(&self, volume_id: &str, force: bool) -> BlockResult<()> {
        let volume = self.get_volume(volume_id)?;

        // Check if volume is attached
        if volume.state == VolumeState::Attached && !force {
            return Err(BlockError::VolumeAttached(volume_id.to_string()));
        }

        // Check for snapshots
        let has_snapshots = self
            .volume_snapshots
            .read()
            .get(volume_id)
            .map(|s| !s.is_empty())
            .unwrap_or(false);

        if has_snapshots && !force {
            return Err(BlockError::VolumeHasSnapshots(volume_id.to_string()));
        }

        // Remove volume
        self.volumes.write().remove(volume_id);
        self.volume_names.write().remove(&volume.name);
        self.volume_chunks.write().remove(volume_id);
        self.volume_snapshots.write().remove(volume_id);

        Ok(())
    }

    /// Resize a volume (grow only)
    pub fn resize_volume(&self, volume_id: &str, new_size_bytes: u64) -> BlockResult<Volume> {
        let mut volumes = self.volumes.write();
        let volume = volumes
            .get_mut(volume_id)
            .ok_or_else(|| BlockError::VolumeNotFound(volume_id.to_string()))?;

        if !volume.can_modify() {
            return Err(BlockError::VolumeAttached(volume_id.to_string()));
        }

        if new_size_bytes < volume.size_bytes {
            return Err(BlockError::CannotShrink(volume.size_bytes, new_size_bytes));
        }

        volume.size_bytes = new_size_bytes;
        volume.updated_at = chrono::Utc::now().timestamp() as u64;

        Ok(volume.clone())
    }

    /// Set volume state
    pub fn set_volume_state(&self, volume_id: &str, state: VolumeState) -> BlockResult<()> {
        let mut volumes = self.volumes.write();
        let volume = volumes
            .get_mut(volume_id)
            .ok_or_else(|| BlockError::VolumeNotFound(volume_id.to_string()))?;

        volume.state = state;
        volume.updated_at = chrono::Utc::now().timestamp() as u64;

        Ok(())
    }

    /// Get chunk reference for a volume
    pub fn get_chunk(&self, volume_id: &str, chunk_id: ChunkId) -> Option<ChunkRef> {
        self.volume_chunks
            .read()
            .get(volume_id)
            .and_then(|chunks| chunks.get(&chunk_id).cloned())
    }

    /// Set chunk reference for a volume
    pub fn set_chunk(&self, volume_id: &str, chunk_id: ChunkId, chunk_ref: ChunkRef) {
        if let Some(chunks) = self.volume_chunks.write().get_mut(volume_id) {
            chunks.insert(chunk_id, chunk_ref);
        }
    }

    /// Check if a chunk is allocated
    pub fn is_chunk_allocated(&self, volume_id: &str, chunk_id: ChunkId) -> bool {
        self.volume_chunks
            .read()
            .get(volume_id)
            .map(|chunks| chunks.contains_key(&chunk_id))
            .unwrap_or(false)
    }

    /// Get all allocated chunk IDs for a volume
    pub fn get_allocated_chunks(&self, volume_id: &str) -> Vec<ChunkId> {
        self.volume_chunks
            .read()
            .get(volume_id)
            .map(|chunks| chunks.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Create a snapshot
    pub fn create_snapshot(&self, volume_id: &str, name: String) -> BlockResult<Snapshot> {
        let volume = self.get_volume(volume_id)?;

        // Copy current chunk refs
        let chunk_refs = self
            .volume_chunks
            .read()
            .get(volume_id)
            .cloned()
            .unwrap_or_default();

        let snapshot = Snapshot {
            snapshot_id: Uuid::new_v4().to_string(),
            volume_id: volume_id.to_string(),
            name,
            size_bytes: volume.size_bytes,
            unique_bytes: 0, // Will be updated as writes happen
            created_at: chrono::Utc::now().timestamp() as u64,
            metadata: HashMap::new(),
            chunk_refs,
        };

        let snapshot_id = snapshot.snapshot_id.clone();

        // Store snapshot
        self.snapshots
            .write()
            .insert(snapshot_id.clone(), snapshot.clone());
        if let Some(snaps) = self.volume_snapshots.write().get_mut(volume_id) {
            snaps.insert(snapshot_id);
        }

        Ok(snapshot)
    }

    /// Get a snapshot by ID
    pub fn get_snapshot(&self, snapshot_id: &str) -> BlockResult<Snapshot> {
        self.snapshots
            .read()
            .get(snapshot_id)
            .cloned()
            .ok_or_else(|| BlockError::SnapshotNotFound(snapshot_id.to_string()))
    }

    /// List snapshots for a volume
    pub fn list_snapshots(&self, volume_id: &str) -> Vec<Snapshot> {
        let snapshot_ids = self
            .volume_snapshots
            .read()
            .get(volume_id)
            .cloned()
            .unwrap_or_default();

        let snapshots = self.snapshots.read();
        snapshot_ids
            .iter()
            .filter_map(|id| snapshots.get(id).cloned())
            .collect()
    }

    /// Delete a snapshot
    pub fn delete_snapshot(&self, snapshot_id: &str) -> BlockResult<()> {
        let snapshot = self.get_snapshot(snapshot_id)?;

        // Remove from volume's snapshot list
        if let Some(snaps) = self.volume_snapshots.write().get_mut(&snapshot.volume_id) {
            snaps.remove(snapshot_id);
        }

        // Remove snapshot
        self.snapshots.write().remove(snapshot_id);

        Ok(())
    }

    /// Clone a volume from a snapshot
    pub fn clone_from_snapshot(
        &self,
        snapshot_id: &str,
        name: String,
        pool: Option<String>,
    ) -> BlockResult<Volume> {
        let snapshot = self.get_snapshot(snapshot_id)?;

        // Check for duplicate name
        if self.volume_names.read().contains_key(&name) {
            return Err(BlockError::VolumeExists(name));
        }

        // Create new volume with snapshot's chunks
        let now = chrono::Utc::now().timestamp() as u64;
        let volume = Volume {
            volume_id: Uuid::new_v4().to_string(),
            name: name.clone(),
            size_bytes: snapshot.size_bytes,
            used_bytes: snapshot.unique_bytes,
            pool: pool.unwrap_or_else(|| "default".to_string()),
            state: VolumeState::Available,
            created_at: now,
            updated_at: now,
            parent_snapshot_id: Some(snapshot_id.to_string()),
            chunk_size: self.chunk_mapper.chunk_size(),
            metadata: HashMap::new(),
        };

        let volume_id = volume.volume_id.clone();

        // Store volume with snapshot's chunk refs
        self.volumes
            .write()
            .insert(volume_id.clone(), volume.clone());
        self.volume_names.write().insert(name, volume_id.clone());
        self.volume_chunks
            .write()
            .insert(volume_id.clone(), snapshot.chunk_refs.clone());
        self.volume_snapshots
            .write()
            .insert(volume_id, HashSet::new());

        Ok(volume)
    }
}

impl Default for VolumeManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_volume() {
        let manager = VolumeManager::new();
        let volume = manager
            .create_volume(
                "test-vol".to_string(),
                100 * 1024 * 1024 * 1024,
                "default".to_string(),
            )
            .unwrap();

        assert_eq!(volume.name, "test-vol");
        assert_eq!(volume.size_bytes, 100 * 1024 * 1024 * 1024);
        assert_eq!(volume.state, VolumeState::Available);
    }

    #[test]
    fn test_duplicate_volume_name() {
        let manager = VolumeManager::new();
        manager
            .create_volume("test-vol".to_string(), 1024 * 1024, "default".to_string())
            .unwrap();

        let result =
            manager.create_volume("test-vol".to_string(), 1024 * 1024, "default".to_string());
        assert!(matches!(result, Err(BlockError::VolumeExists(_))));
    }

    #[test]
    fn test_resize_volume() {
        let manager = VolumeManager::new();
        let volume = manager
            .create_volume("test-vol".to_string(), 1024 * 1024, "default".to_string())
            .unwrap();

        let resized = manager
            .resize_volume(&volume.volume_id, 2 * 1024 * 1024)
            .unwrap();
        assert_eq!(resized.size_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn test_cannot_shrink_volume() {
        let manager = VolumeManager::new();
        let volume = manager
            .create_volume(
                "test-vol".to_string(),
                2 * 1024 * 1024,
                "default".to_string(),
            )
            .unwrap();

        let result = manager.resize_volume(&volume.volume_id, 1024 * 1024);
        assert!(matches!(result, Err(BlockError::CannotShrink(_, _))));
    }

    #[test]
    fn test_snapshot_and_clone() {
        let manager = VolumeManager::new();
        let volume = manager
            .create_volume("test-vol".to_string(), 1024 * 1024, "default".to_string())
            .unwrap();

        // Create snapshot
        let snapshot = manager
            .create_snapshot(&volume.volume_id, "snap1".to_string())
            .unwrap();
        assert_eq!(snapshot.volume_id, volume.volume_id);
        assert_eq!(snapshot.size_bytes, volume.size_bytes);

        // Clone from snapshot
        let clone = manager
            .clone_from_snapshot(&snapshot.snapshot_id, "clone-vol".to_string(), None)
            .unwrap();
        assert_eq!(clone.size_bytes, volume.size_bytes);
        assert_eq!(clone.parent_snapshot_id, Some(snapshot.snapshot_id));
    }
}
