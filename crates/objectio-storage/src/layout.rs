//! Disk layout definitions
//!
//! Disk Layout:
//! ```text
//! +------------------+  Block 0 (offset 0)
//! |   Superblock     |  4KB - Magic, version, UUID, region offsets
//! +------------------+
//! |   WAL Region     |  Configurable (default 1GB)
//! +------------------+
//! |  Block Bitmap    |  Tracks free/used blocks
//! +------------------+
//! |  Index Region    |  B+ tree for block lookup
//! +------------------+
//! |   Data Region    |  Object data blocks
//! +------------------+
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use objectio_common::{DiskId, Error, Result};
use std::io::Write;
use uuid::Uuid;

/// Magic number for ObjectIO disk format
pub const MAGIC: [u8; 8] = *b"OBJECTIO";

/// Current disk format version
pub const FORMAT_VERSION: u32 = 1;

/// Superblock size (4KB)
pub const SUPERBLOCK_SIZE: u64 = 4096;

/// Default block size (64KB)
pub const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024;

/// Default WAL size (1GB)
pub const DEFAULT_WAL_SIZE: u64 = 1024 * 1024 * 1024;

/// Minimum disk size (1GB)
pub const MIN_DISK_SIZE: u64 = 1024 * 1024 * 1024;

/// Alignment requirement for direct I/O (4KB)
pub const ALIGNMENT: u64 = 4096;

/// Align a value up to the nearest multiple of ALIGNMENT
#[inline]
const fn align_up(value: u64) -> u64 {
    (value + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT
}

/// Superblock stored at the beginning of each disk
#[derive(Clone, Debug)]
pub struct Superblock {
    /// Magic number for format identification
    pub magic: [u8; 8],
    /// Format version
    pub version: u32,
    /// Unique disk identifier
    pub disk_uuid: Uuid,
    /// Disk ID in cluster
    pub disk_id: DiskId,
    /// Total disk size in bytes
    pub disk_size: u64,
    /// Block size in bytes
    pub block_size: u32,
    /// Total number of data blocks
    pub total_blocks: u64,
    /// Number of free blocks
    pub free_blocks: u64,
    /// WAL region offset
    pub wal_offset: u64,
    /// WAL region size
    pub wal_size: u64,
    /// Bitmap region offset
    pub bitmap_offset: u64,
    /// Bitmap region size
    pub bitmap_size: u64,
    /// Index region offset
    pub index_offset: u64,
    /// Index region size
    pub index_size: u64,
    /// Data region offset
    pub data_offset: u64,
    /// Data region size
    pub data_size: u64,
    /// Creation timestamp (Unix epoch)
    pub created_at: u64,
    /// Last mount timestamp
    pub last_mount: u64,
    /// Mount count
    pub mount_count: u64,
    /// Flags
    pub flags: u32,
    /// Reserved for future use
    pub reserved: [u8; 128],
    /// Checksum of superblock (excluding this field)
    pub checksum: u32,
}

impl Superblock {
    /// Create a new superblock for a disk of the given size
    pub fn new(disk_size: u64, block_size: u32) -> Result<Self> {
        if disk_size < MIN_DISK_SIZE {
            return Err(Error::Storage(format!(
                "disk size {} is below minimum {}",
                disk_size, MIN_DISK_SIZE
            )));
        }

        // All offsets and sizes must be aligned to 4KB for direct I/O
        let wal_offset = SUPERBLOCK_SIZE; // Already aligned (4KB)
        let wal_size = align_up(DEFAULT_WAL_SIZE.min(disk_size / 10)); // Max 10% of disk, aligned

        // Calculate bitmap size (1 bit per block)
        let bitmap_offset = align_up(wal_offset + wal_size);
        let available_for_data = disk_size - bitmap_offset;
        let blocks_approx = available_for_data / u64::from(block_size);
        let bitmap_size = align_up((blocks_approx + 7) / 8); // Align to 4KB

        let index_offset = align_up(bitmap_offset + bitmap_size);
        let index_size = align_up((blocks_approx / 100).max(1) * 4096); // ~1% of blocks for index

        let data_offset = align_up(index_offset + index_size);
        let data_size = disk_size - data_offset;
        let total_blocks = data_size / u64::from(block_size);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut sb = Self {
            magic: MAGIC,
            version: FORMAT_VERSION,
            disk_uuid: Uuid::new_v4(),
            disk_id: DiskId::new(),
            disk_size,
            block_size,
            total_blocks,
            free_blocks: total_blocks,
            wal_offset,
            wal_size,
            bitmap_offset,
            bitmap_size,
            index_offset,
            index_size,
            data_offset,
            data_size,
            created_at: now,
            last_mount: now,
            mount_count: 1,
            flags: 0,
            reserved: [0u8; 128],
            checksum: 0,
        };

        sb.checksum = sb.compute_checksum();
        Ok(sb)
    }

    /// Serialize superblock to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(SUPERBLOCK_SIZE as usize);

        buf.put_slice(&self.magic);
        buf.put_u32_le(self.version);
        buf.put_slice(self.disk_uuid.as_bytes());
        buf.put_slice(self.disk_id.as_bytes());
        buf.put_u64_le(self.disk_size);
        buf.put_u32_le(self.block_size);
        buf.put_u64_le(self.total_blocks);
        buf.put_u64_le(self.free_blocks);
        buf.put_u64_le(self.wal_offset);
        buf.put_u64_le(self.wal_size);
        buf.put_u64_le(self.bitmap_offset);
        buf.put_u64_le(self.bitmap_size);
        buf.put_u64_le(self.index_offset);
        buf.put_u64_le(self.index_size);
        buf.put_u64_le(self.data_offset);
        buf.put_u64_le(self.data_size);
        buf.put_u64_le(self.created_at);
        buf.put_u64_le(self.last_mount);
        buf.put_u64_le(self.mount_count);
        buf.put_u32_le(self.flags);
        buf.put_slice(&self.reserved);
        buf.put_u32_le(self.checksum);

        // Pad to SUPERBLOCK_SIZE
        buf.resize(SUPERBLOCK_SIZE as usize, 0);

        buf.freeze()
    }

    /// Parse superblock from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 256 {
            return Err(Error::Storage("superblock too small".into()));
        }

        let mut buf = &data[..];

        let mut magic = [0u8; 8];
        buf.copy_to_slice(&mut magic);

        if magic != MAGIC {
            return Err(Error::Storage("invalid superblock magic".into()));
        }

        let version = buf.get_u32_le();
        if version != FORMAT_VERSION {
            return Err(Error::Storage(format!(
                "unsupported format version: {version}"
            )));
        }

        let mut uuid_bytes = [0u8; 16];
        buf.copy_to_slice(&mut uuid_bytes);
        let disk_uuid = Uuid::from_bytes(uuid_bytes);

        let mut disk_id_bytes = [0u8; 16];
        buf.copy_to_slice(&mut disk_id_bytes);
        let disk_id = DiskId::from_bytes(disk_id_bytes);

        let disk_size = buf.get_u64_le();
        let block_size = buf.get_u32_le();
        let total_blocks = buf.get_u64_le();
        let free_blocks = buf.get_u64_le();
        let wal_offset = buf.get_u64_le();
        let wal_size = buf.get_u64_le();
        let bitmap_offset = buf.get_u64_le();
        let bitmap_size = buf.get_u64_le();
        let index_offset = buf.get_u64_le();
        let index_size = buf.get_u64_le();
        let data_offset = buf.get_u64_le();
        let data_size = buf.get_u64_le();
        let created_at = buf.get_u64_le();
        let last_mount = buf.get_u64_le();
        let mount_count = buf.get_u64_le();
        let flags = buf.get_u32_le();

        let mut reserved = [0u8; 128];
        buf.copy_to_slice(&mut reserved);

        let checksum = buf.get_u32_le();

        let sb = Self {
            magic,
            version,
            disk_uuid,
            disk_id,
            disk_size,
            block_size,
            total_blocks,
            free_blocks,
            wal_offset,
            wal_size,
            bitmap_offset,
            bitmap_size,
            index_offset,
            index_size,
            data_offset,
            data_size,
            created_at,
            last_mount,
            mount_count,
            flags,
            reserved,
            checksum,
        };

        // Verify checksum
        if sb.compute_checksum() != checksum {
            return Err(Error::Storage("superblock checksum mismatch".into()));
        }

        Ok(sb)
    }

    /// Offset of the checksum field within the superblock
    /// This is the sum of all field sizes before the checksum:
    /// magic(8) + version(4) + disk_uuid(16) + disk_id(16) + disk_size(8) +
    /// block_size(4) + total_blocks(8) + free_blocks(8) + wal_offset(8) +
    /// wal_size(8) + bitmap_offset(8) + bitmap_size(8) + index_offset(8) +
    /// index_size(8) + data_offset(8) + data_size(8) + created_at(8) +
    /// last_mount(8) + mount_count(8) + flags(4) + reserved(128) = 280
    const CHECKSUM_OFFSET: usize = 280;

    /// Compute checksum of superblock (CRC32C)
    fn compute_checksum(&self) -> u32 {
        let bytes = self.to_bytes();
        // Checksum everything up to (but not including) the checksum field
        crc32c::crc32c(&bytes[..Self::CHECKSUM_OFFSET])
    }

    /// Update the checksum field after modifying other fields
    pub fn update_checksum(&mut self) {
        self.checksum = self.compute_checksum();
    }

    /// Validate superblock consistency
    pub fn validate(&self) -> Result<()> {
        if self.magic != MAGIC {
            return Err(Error::Storage("invalid magic".into()));
        }
        if self.version != FORMAT_VERSION {
            return Err(Error::Storage("invalid version".into()));
        }
        if self.data_offset + self.data_size > self.disk_size {
            return Err(Error::Storage("data region exceeds disk size".into()));
        }
        if self.total_blocks * u64::from(self.block_size) > self.data_size {
            return Err(Error::Storage("block count exceeds data region".into()));
        }
        Ok(())
    }
}

/// Block header stored at the beginning of each data block
#[derive(Clone, Debug)]
pub struct BlockHeader {
    /// Magic for block identification
    pub magic: u32,
    /// Block sequence number
    pub sequence: u64,
    /// Object ID this block belongs to
    pub object_id: [u8; 16],
    /// Offset within the object
    pub object_offset: u64,
    /// Actual data size in this block
    pub data_size: u32,
    /// Flags (compressed, encrypted, etc.)
    pub flags: u32,
    /// Header checksum
    pub checksum: u32,
}

impl BlockHeader {
    /// Block header magic
    pub const MAGIC: u32 = 0x424C4B48; // "BLKH"

    /// Header size in bytes
    pub const SIZE: usize = 64;

    /// Create a new block header
    pub fn new(sequence: u64, object_id: [u8; 16], object_offset: u64, data_size: u32) -> Self {
        let mut header = Self {
            magic: Self::MAGIC,
            sequence,
            object_id,
            object_offset,
            data_size,
            flags: 0,
            checksum: 0,
        };
        header.checksum = header.compute_checksum();
        header
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let mut cursor = &mut buf[..];

        cursor.write_all(&self.magic.to_le_bytes()).unwrap();
        cursor.write_all(&self.sequence.to_le_bytes()).unwrap();
        cursor.write_all(&self.object_id).unwrap();
        cursor.write_all(&self.object_offset.to_le_bytes()).unwrap();
        cursor.write_all(&self.data_size.to_le_bytes()).unwrap();
        cursor.write_all(&self.flags.to_le_bytes()).unwrap();
        cursor.write_all(&self.checksum.to_le_bytes()).unwrap();

        buf
    }

    /// Parse from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            return Err(Error::Storage("block header too small".into()));
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != Self::MAGIC {
            return Err(Error::Storage("invalid block header magic".into()));
        }

        let sequence = u64::from_le_bytes(data[4..12].try_into().unwrap());
        let mut object_id = [0u8; 16];
        object_id.copy_from_slice(&data[12..28]);
        let object_offset = u64::from_le_bytes(data[28..36].try_into().unwrap());
        let data_size = u32::from_le_bytes(data[36..40].try_into().unwrap());
        let flags = u32::from_le_bytes(data[40..44].try_into().unwrap());
        let checksum = u32::from_le_bytes(data[44..48].try_into().unwrap());

        let header = Self {
            magic,
            sequence,
            object_id,
            object_offset,
            data_size,
            flags,
            checksum,
        };

        if header.compute_checksum() != checksum {
            return Err(Error::Storage("block header checksum mismatch".into()));
        }

        Ok(header)
    }

    fn compute_checksum(&self) -> u32 {
        let bytes = self.to_bytes();
        crc32c::crc32c(&bytes[..44]) // Everything except checksum field
    }
}

/// Block footer stored at the end of each data block
#[derive(Clone, Debug)]
pub struct BlockFooter {
    /// Data checksum (CRC32C of block data)
    pub data_checksum: u32,
    /// Block sequence (must match header)
    pub sequence: u64,
    /// Footer magic
    pub magic: u32,
}

impl BlockFooter {
    /// Footer magic
    pub const MAGIC: u32 = 0x424C4B46; // "BLKF"

    /// Footer size in bytes
    pub const SIZE: usize = 32;

    /// Create a new footer
    pub fn new(data_checksum: u32, sequence: u64) -> Self {
        Self {
            data_checksum,
            sequence,
            magic: Self::MAGIC,
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let mut cursor = &mut buf[..];

        cursor.write_all(&self.data_checksum.to_le_bytes()).unwrap();
        cursor.write_all(&self.sequence.to_le_bytes()).unwrap();
        cursor
            .write_all(&[0u8; Self::SIZE - 16])
            .unwrap(); // Padding
        cursor.write_all(&self.magic.to_le_bytes()).unwrap();

        buf
    }

    /// Parse from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            return Err(Error::Storage("block footer too small".into()));
        }

        let data_checksum = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let sequence = u64::from_le_bytes(data[4..12].try_into().unwrap());
        let magic = u32::from_le_bytes(data[Self::SIZE - 4..Self::SIZE].try_into().unwrap());

        if magic != Self::MAGIC {
            return Err(Error::Storage("invalid block footer magic".into()));
        }

        Ok(Self {
            data_checksum,
            sequence,
            magic,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_superblock_roundtrip() {
        let sb = Superblock::new(10 * 1024 * 1024 * 1024, DEFAULT_BLOCK_SIZE).unwrap();
        let bytes = sb.to_bytes();
        let sb2 = Superblock::from_bytes(&bytes).unwrap();

        assert_eq!(sb.disk_uuid, sb2.disk_uuid);
        assert_eq!(sb.total_blocks, sb2.total_blocks);
        assert_eq!(sb.block_size, sb2.block_size);
    }

    #[test]
    fn test_block_header_roundtrip() {
        let header = BlockHeader::new(42, [1u8; 16], 1024, 4096);
        let bytes = header.to_bytes();
        let header2 = BlockHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.sequence, header2.sequence);
        assert_eq!(header.object_id, header2.object_id);
        assert_eq!(header.data_size, header2.data_size);
    }

    #[test]
    fn test_block_footer_roundtrip() {
        let footer = BlockFooter::new(0x12345678, 42);
        let bytes = footer.to_bytes();
        let footer2 = BlockFooter::from_bytes(&bytes).unwrap();

        assert_eq!(footer.data_checksum, footer2.data_checksum);
        assert_eq!(footer.sequence, footer2.sequence);
    }
}
