//! Disk management and raw I/O
//!
//! Provides high-level disk operations including:
//! - Disk initialization with superblock
//! - Block read/write with checksums
//! - Background scrubbing

use crate::layout::{BlockFooter, BlockHeader, DEFAULT_BLOCK_SIZE, SUPERBLOCK_SIZE, Superblock};
use crate::raw_io::{AlignedBuffer, RawFile};
use objectio_common::{DiskId, Error, Result};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Disk manager for a single raw disk
pub struct DiskManager {
    /// Raw file handle
    file: RawFile,
    /// Superblock (cached)
    superblock: RwLock<Superblock>,
    /// Next block sequence number
    sequence: AtomicU64,
    /// Statistics
    stats: DiskStats,
}

/// Disk statistics
#[derive(Debug, Default)]
pub struct DiskStats {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub read_errors: AtomicU64,
    pub write_errors: AtomicU64,
    pub checksum_errors: AtomicU64,
}

impl DiskManager {
    /// Initialize a new disk with ObjectIO format
    ///
    /// The block_size parameter determines the storage block size.
    /// Use None to use the default (64KB), or specify a custom size.
    /// Larger block sizes support larger erasure-coded shards without chunking.
    pub fn init(path: impl AsRef<Path>, size: u64, block_size: Option<u32>) -> Result<Self> {
        let file = RawFile::create(&path, size)?;

        // Create and write superblock
        let actual_block_size = block_size.unwrap_or(DEFAULT_BLOCK_SIZE);
        let superblock = Superblock::new(size, actual_block_size)?;
        let sb_bytes = superblock.to_bytes();

        let mut buf = AlignedBuffer::new(SUPERBLOCK_SIZE as usize);
        buf.copy_from(&sb_bytes);
        file.write_at(0, buf.as_slice())?;
        file.sync()?;

        // Initialize bitmap region (all zeros = all free)
        let bitmap_size = superblock.bitmap_size as usize;
        let bitmap_buf = AlignedBuffer::new(bitmap_size);
        file.write_at(superblock.bitmap_offset, bitmap_buf.as_slice())?;

        file.sync()?;

        Ok(Self {
            file,
            superblock: RwLock::new(superblock),
            sequence: AtomicU64::new(1),
            stats: DiskStats::default(),
        })
    }

    /// Open an existing disk
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = RawFile::open(&path, false)?;

        // Read and validate superblock
        let mut buf = AlignedBuffer::new(SUPERBLOCK_SIZE as usize);
        file.read_at(0, buf.as_mut_slice())?;

        let superblock = Superblock::from_bytes(buf.as_slice())?;
        superblock.validate()?;

        Ok(Self {
            file,
            superblock: RwLock::new(superblock),
            sequence: AtomicU64::new(1),
            stats: DiskStats::default(),
        })
    }

    /// Get the disk ID
    pub fn id(&self) -> DiskId {
        self.superblock.read().disk_id
    }

    /// Get the disk path
    pub fn path(&self) -> &str {
        self.file.path()
    }

    /// Get total capacity in bytes
    pub fn capacity(&self) -> u64 {
        let sb = self.superblock.read();
        sb.total_blocks * u64::from(sb.block_size)
    }

    /// Get free space in bytes
    pub fn free_space(&self) -> u64 {
        let sb = self.superblock.read();
        sb.free_blocks * u64::from(sb.block_size)
    }

    /// Get block size
    pub fn block_size(&self) -> u32 {
        self.superblock.read().block_size
    }

    /// Get statistics
    pub fn stats(&self) -> &DiskStats {
        &self.stats
    }

    /// Write a block to the disk
    ///
    /// Returns the block number where data was written
    pub fn write_block(
        &self,
        block_num: u64,
        object_id: [u8; 16],
        object_offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let sb = self.superblock.read();
        let block_size = sb.block_size as usize;
        let max_data_size = block_size - BlockHeader::SIZE - BlockFooter::SIZE;

        if data.len() > max_data_size {
            return Err(Error::Storage(format!(
                "data size {} exceeds max block data size {}",
                data.len(),
                max_data_size
            )));
        }

        if block_num >= sb.total_blocks {
            return Err(Error::Storage(format!(
                "block {} exceeds total blocks {}",
                block_num, sb.total_blocks
            )));
        }

        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        // Build block: header + data + padding + footer
        let mut buf = AlignedBuffer::new(block_size);
        let block_buf = buf.as_mut_slice();

        // Write header
        let header = BlockHeader::new(sequence, object_id, object_offset, data.len() as u32);
        block_buf[..BlockHeader::SIZE].copy_from_slice(&header.to_bytes());

        // Write data
        let data_start = BlockHeader::SIZE;
        let data_end = data_start + data.len();
        block_buf[data_start..data_end].copy_from_slice(data);

        // Calculate data checksum
        let data_checksum = crc32c::crc32c(data);

        // Write footer at the end of the block
        let footer = BlockFooter::new(data_checksum, sequence);
        let footer_start = block_size - BlockFooter::SIZE;
        block_buf[footer_start..].copy_from_slice(&footer.to_bytes());

        // Calculate disk offset
        let offset = sb.data_offset + block_num * block_size as u64;
        drop(sb); // Release read lock

        // Write to disk
        self.file.write_at(offset, block_buf)?;

        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_written
            .fetch_add(block_size as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Read a block from the disk
    ///
    /// Returns the data portion of the block (without header/footer)
    pub fn read_block(&self, block_num: u64) -> Result<(BlockHeader, Vec<u8>)> {
        let sb = self.superblock.read();
        let block_size = sb.block_size as usize;

        if block_num >= sb.total_blocks {
            return Err(Error::Storage(format!(
                "block {} exceeds total blocks {}",
                block_num, sb.total_blocks
            )));
        }

        let offset = sb.data_offset + block_num * block_size as u64;
        drop(sb);

        // Read block
        let mut buf = AlignedBuffer::new(block_size);
        self.file.read_at(offset, buf.as_mut_slice())?;

        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_read
            .fetch_add(block_size as u64, Ordering::Relaxed);

        let block_buf = buf.as_slice();

        // Parse header
        let header = BlockHeader::from_bytes(&block_buf[..BlockHeader::SIZE])?;

        // Parse footer
        let footer_start = block_size - BlockFooter::SIZE;
        let footer = BlockFooter::from_bytes(&block_buf[footer_start..])?;

        // Verify sequence numbers match
        if header.sequence != footer.sequence {
            self.stats.checksum_errors.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Storage(format!(
                "block {} sequence mismatch: header={}, footer={}",
                block_num, header.sequence, footer.sequence
            )));
        }

        // Extract and verify data
        let data_start = BlockHeader::SIZE;
        let data_end = data_start + header.data_size as usize;
        let data = &block_buf[data_start..data_end];

        let computed_checksum = crc32c::crc32c(data);
        if computed_checksum != footer.data_checksum {
            self.stats.checksum_errors.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Storage(format!(
                "block {} data checksum mismatch: computed={:08x}, stored={:08x}",
                block_num, computed_checksum, footer.data_checksum
            )));
        }

        Ok((header, data.to_vec()))
    }

    /// Verify a block's integrity without returning data
    pub fn verify_block(&self, block_num: u64) -> Result<bool> {
        match self.read_block(block_num) {
            Ok(_) => Ok(true),
            Err(Error::Storage(msg)) if msg.contains("checksum") || msg.contains("mismatch") => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Sync all pending writes to disk
    pub fn sync(&self) -> Result<()> {
        self.file.sync()
    }

    /// Update superblock on disk
    pub fn update_superblock(&self) -> Result<()> {
        let mut sb = self.superblock.write();
        sb.last_mount = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Recompute checksum after modifying fields
        sb.update_checksum();

        let sb_bytes = sb.to_bytes();
        let mut buf = AlignedBuffer::new(SUPERBLOCK_SIZE as usize);
        buf.copy_from(&sb_bytes);

        self.file.write_at(0, buf.as_slice())?;
        self.file.sync()
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        // Try to sync and update superblock on close
        let _ = self.update_superblock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_disk_init_and_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.disk");

        // Initialize disk
        {
            let disk = DiskManager::init(&path, 2 * 1024 * 1024 * 1024, None).unwrap();
            assert!(disk.capacity() > 0);
            assert_eq!(disk.free_space(), disk.capacity());
        }

        // Reopen disk
        {
            let disk = DiskManager::open(&path).unwrap();
            assert!(disk.capacity() > 0);
        }
    }

    #[test]
    fn test_block_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.disk");

        let disk = DiskManager::init(&path, 2 * 1024 * 1024 * 1024, None).unwrap();

        let object_id = [1u8; 16];
        let data = b"Hello, ObjectIO!";

        // Write block
        disk.write_block(0, object_id, 0, data).unwrap();
        disk.sync().unwrap();

        // Read block back
        let (header, read_data) = disk.read_block(0).unwrap();
        assert_eq!(header.object_id, object_id);
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_block_verify() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.disk");

        let disk = DiskManager::init(&path, 2 * 1024 * 1024 * 1024, None).unwrap();

        let object_id = [2u8; 16];
        let data = b"Test block verification";

        disk.write_block(5, object_id, 1024, data).unwrap();

        assert!(disk.verify_block(5).unwrap());
    }
}
