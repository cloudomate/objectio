//! Metadata types for OSD storage

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Key for metadata entries
///
/// Keys are designed for efficient prefix scanning:
/// - Shard keys: `s:{object_id}:{shard_pos}`
/// - Object keys: `o:{object_id}`
/// - Block keys: `b:{block_num}`
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MetadataKey(pub Vec<u8>);

impl MetadataKey {
    /// Create a shard metadata key
    pub fn shard(object_id: &[u8; 16], shard_position: u8) -> Self {
        let mut key = Vec::with_capacity(18);
        key.push(b's');
        key.extend_from_slice(object_id);
        key.push(shard_position);
        Self(key)
    }

    /// Create an object metadata key
    pub fn object(object_id: &[u8; 16]) -> Self {
        let mut key = Vec::with_capacity(17);
        key.push(b'o');
        key.extend_from_slice(object_id);
        Self(key)
    }

    /// Create a block metadata key
    pub fn block(block_num: u64) -> Self {
        let mut key = Vec::with_capacity(9);
        key.push(b'b');
        key.extend_from_slice(&block_num.to_be_bytes()); // Big-endian for sorting
        Self(key)
    }

    /// Create a disk usage key
    pub fn disk_usage(disk_id: &[u8; 16]) -> Self {
        let mut key = Vec::with_capacity(17);
        key.push(b'd');
        key.extend_from_slice(disk_id);
        Self(key)
    }

    /// Create an object metadata key by bucket/key (for primary OSD storage)
    /// Format: `m:{bucket}\0{key}`
    pub fn object_meta(bucket: &str, key: &str) -> Self {
        let mut data = Vec::with_capacity(2 + bucket.len() + key.len());
        data.push(b'm');
        data.extend_from_slice(bucket.as_bytes());
        data.push(0); // null separator
        data.extend_from_slice(key.as_bytes());
        Self(data)
    }

    /// Create a prefix for scanning object metadata by bucket
    /// Format: `m:{bucket}\0`
    pub fn object_meta_prefix(bucket: &str) -> Self {
        let mut data = Vec::with_capacity(2 + bucket.len());
        data.push(b'm');
        data.extend_from_slice(bucket.as_bytes());
        data.push(0);
        Self(data)
    }

    /// Parse bucket and key from object metadata key
    pub fn parse_object_meta(&self) -> Option<(String, String)> {
        if self.0.first() != Some(&b'm') {
            return None;
        }
        let rest = &self.0[1..];
        let null_pos = rest.iter().position(|&b| b == 0)?;
        let bucket = std::str::from_utf8(&rest[..null_pos]).ok()?;
        let key = std::str::from_utf8(&rest[null_pos + 1..]).ok()?;
        Some((bucket.to_string(), key.to_string()))
    }

    /// Get the key type prefix
    pub fn key_type(&self) -> Option<char> {
        self.0.first().map(|&b| b as char)
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Create from raw bytes
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl Ord for MetadataKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for MetadataKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl AsRef<[u8]> for MetadataKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Shard metadata stored on this OSD
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardMeta {
    /// Object ID this shard belongs to
    pub object_id: [u8; 16],
    /// Shard position in the stripe (0..k+m-1)
    pub shard_position: u8,
    /// Block number where shard data starts
    pub block_num: u64,
    /// Shard data size in bytes
    pub size: u64,
    /// CRC32C checksum of shard data
    pub checksum: u32,
    /// Creation timestamp (unix millis)
    pub created_at: u64,
    /// Last verified timestamp (for scrubbing)
    pub last_verified: u64,
    /// Shard type: 0=Data, 1=LocalParity, 2=GlobalParity
    pub shard_type: u8,
    /// Local group index for LRC (255 = no group / global)
    pub local_group: u8,
}

impl ShardMeta {
    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

/// Generic metadata entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataEntry {
    /// Entry key
    pub key: MetadataKey,
    /// Entry value (serialized)
    pub value: Vec<u8>,
    /// Log Sequence Number (for ordering)
    pub lsn: u64,
    /// Tombstone flag (true = deleted)
    pub deleted: bool,
}

impl MetadataEntry {
    /// Create a new entry
    pub fn new(key: MetadataKey, value: Vec<u8>, lsn: u64) -> Self {
        Self {
            key,
            value,
            lsn,
            deleted: false,
        }
    }

    /// Create a tombstone (deletion marker)
    pub fn tombstone(key: MetadataKey, lsn: u64) -> Self {
        Self {
            key,
            value: vec![],
            lsn,
            deleted: true,
        }
    }

    /// Serialize to bytes for WAL
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

/// Metadata operation type for WAL
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetadataOp {
    /// Insert or update an entry
    Put { key: MetadataKey, value: Vec<u8> },
    /// Delete an entry
    Delete { key: MetadataKey },
    /// Batch of operations (atomic)
    Batch { ops: Vec<MetadataOp> },
}

impl MetadataOp {
    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

/// Snapshot header for B-tree persistence
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Snapshot version
    pub version: u32,
    /// LSN at snapshot time
    pub lsn: u64,
    /// Number of entries in snapshot
    pub entry_count: u64,
    /// CRC32C of snapshot data (excluding header)
    pub checksum: u32,
    /// Timestamp of snapshot creation
    pub created_at: u64,
}

impl SnapshotHeader {
    pub const MAGIC: u32 = 0x4D455441; // "META"
    pub const VERSION: u32 = 1;
    pub const SIZE: usize = 32;

    pub fn new(lsn: u64, entry_count: u64) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            lsn,
            entry_count,
            checksum: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.lsn.to_le_bytes());
        buf[16..24].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[24..28].copy_from_slice(&self.checksum.to_le_bytes());
        buf[28..32].copy_from_slice(&(self.created_at as u32).to_le_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE {
            return None;
        }
        let magic = u32::from_le_bytes(data[0..4].try_into().ok()?);
        if magic != Self::MAGIC {
            return None;
        }
        Some(Self {
            magic,
            version: u32::from_le_bytes(data[4..8].try_into().ok()?),
            lsn: u64::from_le_bytes(data[8..16].try_into().ok()?),
            entry_count: u64::from_le_bytes(data[16..24].try_into().ok()?),
            checksum: u32::from_le_bytes(data[24..28].try_into().ok()?),
            created_at: u32::from_le_bytes(data[28..32].try_into().ok()?) as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_key_ordering() {
        let k1 = MetadataKey::block(1);
        let k2 = MetadataKey::block(2);
        let k3 = MetadataKey::block(100);

        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn test_shard_meta_roundtrip() {
        let meta = ShardMeta {
            object_id: [1u8; 16],
            shard_position: 0,
            block_num: 42,
            size: 1024,
            checksum: 0xDEADBEEF,
            created_at: 1234567890,
            last_verified: 1234567890,
            shard_type: 0,
            local_group: 255,
        };

        let bytes = meta.to_bytes();
        let parsed = ShardMeta::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.object_id, meta.object_id);
        assert_eq!(parsed.block_num, meta.block_num);
        assert_eq!(parsed.checksum, meta.checksum);
    }

    #[test]
    fn test_metadata_entry_roundtrip() {
        let entry = MetadataEntry::new(
            MetadataKey::shard(&[1u8; 16], 0),
            b"test value".to_vec(),
            100,
        );

        let bytes = entry.to_bytes();
        let parsed = MetadataEntry::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.key, entry.key);
        assert_eq!(parsed.value, entry.value);
        assert_eq!(parsed.lsn, entry.lsn);
        assert!(!parsed.deleted);
    }

    #[test]
    fn test_snapshot_header_roundtrip() {
        let header = SnapshotHeader::new(1000, 500);
        let bytes = header.to_bytes();
        let parsed = SnapshotHeader::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.magic, SnapshotHeader::MAGIC);
        assert_eq!(parsed.lsn, 1000);
        assert_eq!(parsed.entry_count, 500);
    }
}
