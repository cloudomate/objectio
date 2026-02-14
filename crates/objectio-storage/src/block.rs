//! Block management and allocation
//!
//! This module provides:
//! - Block data structure for storing object data
//! - Bitmap-based block allocator for managing free space
//! - Extent allocation for contiguous block sequences

use crate::raw_io::{AlignedBuffer, RawFile};
use bytes::Bytes;
use objectio_common::{BlockId, Checksum, Error, Result};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// A data block stored on disk
#[derive(Clone, Debug)]
pub struct Block {
    /// Block identifier
    pub id: BlockId,
    /// Block data
    pub data: Bytes,
    /// Checksum for integrity
    pub checksum: Checksum,
}

impl Block {
    /// Create a new block
    #[must_use]
    pub fn new(data: Bytes) -> Self {
        let checksum = Checksum::compute_fast(&data);
        Self {
            id: BlockId::new(),
            data,
            checksum,
        }
    }

    /// Verify block integrity
    #[must_use]
    pub fn verify(&self) -> bool {
        self.checksum.verify_fast(&self.data)
    }

    /// Get block size
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// An extent represents a contiguous range of blocks
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    /// Starting block number
    pub start: u64,
    /// Number of blocks in the extent
    pub length: u64,
}

impl Extent {
    /// Create a new extent
    #[must_use]
    pub const fn new(start: u64, length: u64) -> Self {
        Self { start, length }
    }

    /// Get the ending block number (exclusive)
    #[must_use]
    pub const fn end(&self) -> u64 {
        self.start + self.length
    }

    /// Check if this extent contains a block
    #[must_use]
    pub const fn contains(&self, block: u64) -> bool {
        block >= self.start && block < self.end()
    }

    /// Check if two extents overlap
    #[must_use]
    pub const fn overlaps(&self, other: &Extent) -> bool {
        self.start < other.end() && other.start < self.end()
    }

    /// Try to merge with another extent
    #[must_use]
    pub fn try_merge(&self, other: &Extent) -> Option<Extent> {
        if self.end() == other.start {
            Some(Extent::new(self.start, self.length + other.length))
        } else if other.end() == self.start {
            Some(Extent::new(other.start, self.length + other.length))
        } else {
            None
        }
    }
}

/// Bitmap for tracking block allocation
///
/// Uses one bit per block: 0 = free, 1 = used
pub struct BlockBitmap {
    /// Bitmap data
    data: RwLock<Vec<u8>>,
    /// Total number of blocks
    total_blocks: u64,
    /// Number of free blocks (cached)
    free_blocks: AtomicU64,
    /// Hint for next free block search
    search_hint: AtomicU64,
}

impl BlockBitmap {
    /// Create a new bitmap for the given number of blocks (all free)
    #[must_use]
    pub fn new(total_blocks: u64) -> Self {
        let bytes_needed = total_blocks.div_ceil(8) as usize;
        Self {
            data: RwLock::new(vec![0u8; bytes_needed]),
            total_blocks,
            free_blocks: AtomicU64::new(total_blocks),
            search_hint: AtomicU64::new(0),
        }
    }

    /// Load bitmap from bytes
    pub fn from_bytes(data: &[u8], total_blocks: u64) -> Self {
        let mut bitmap_data = vec![0u8; total_blocks.div_ceil(8) as usize];
        let copy_len = bitmap_data.len().min(data.len());
        bitmap_data[..copy_len].copy_from_slice(&data[..copy_len]);

        // Count free blocks
        let mut free = 0u64;
        for block in 0..total_blocks {
            if !Self::is_set_in_slice(&bitmap_data, block) {
                free += 1;
            }
        }

        Self {
            data: RwLock::new(bitmap_data),
            total_blocks,
            free_blocks: AtomicU64::new(free),
            search_hint: AtomicU64::new(0),
        }
    }

    /// Get the bitmap data as bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.read().clone()
    }

    /// Check if a block is allocated (used)
    pub fn is_allocated(&self, block: u64) -> bool {
        if block >= self.total_blocks {
            return true; // Out of range blocks are considered allocated
        }
        let data = self.data.read();
        Self::is_set_in_slice(&data, block)
    }

    /// Check if a bit is set in a byte slice
    fn is_set_in_slice(data: &[u8], block: u64) -> bool {
        let byte_idx = (block / 8) as usize;
        let bit_idx = (block % 8) as u8;
        data[byte_idx] & (1 << bit_idx) != 0
    }

    /// Set a bit in a byte slice
    fn set_in_slice(data: &mut [u8], block: u64) {
        let byte_idx = (block / 8) as usize;
        let bit_idx = (block % 8) as u8;
        data[byte_idx] |= 1 << bit_idx;
    }

    /// Clear a bit in a byte slice
    fn clear_in_slice(data: &mut [u8], block: u64) {
        let byte_idx = (block / 8) as usize;
        let bit_idx = (block % 8) as u8;
        data[byte_idx] &= !(1 << bit_idx);
    }

    /// Allocate a single block
    pub fn allocate(&self) -> Option<u64> {
        let mut data = self.data.write();
        let hint = self.search_hint.load(Ordering::Relaxed);

        // Search from hint to end
        if let Some(block) = self.find_free_in_range(&data, hint, self.total_blocks) {
            Self::set_in_slice(&mut data, block);
            self.free_blocks.fetch_sub(1, Ordering::Relaxed);
            self.search_hint.store(block + 1, Ordering::Relaxed);
            return Some(block);
        }

        // Wrap around: search from start to hint
        if hint > 0
            && let Some(block) = self.find_free_in_range(&data, 0, hint)
        {
            Self::set_in_slice(&mut data, block);
            self.free_blocks.fetch_sub(1, Ordering::Relaxed);
            self.search_hint.store(block + 1, Ordering::Relaxed);
            return Some(block);
        }

        None
    }

    /// Allocate a contiguous extent of blocks
    pub fn allocate_extent(&self, count: u64) -> Option<Extent> {
        if count == 0 {
            return None;
        }
        if count == 1 {
            return self.allocate().map(|b| Extent::new(b, 1));
        }

        let mut data = self.data.write();
        let hint = self.search_hint.load(Ordering::Relaxed);

        // Search from hint to end
        if let Some(extent) = self.find_free_extent_in_range(&data, hint, self.total_blocks, count)
        {
            self.mark_extent_used(&mut data, &extent);
            self.free_blocks.fetch_sub(count, Ordering::Relaxed);
            self.search_hint.store(extent.end(), Ordering::Relaxed);
            return Some(extent);
        }

        // Wrap around: search from start to hint
        if hint > 0
            && let Some(extent) = self.find_free_extent_in_range(&data, 0, hint, count)
        {
            self.mark_extent_used(&mut data, &extent);
            self.free_blocks.fetch_sub(count, Ordering::Relaxed);
            self.search_hint.store(extent.end(), Ordering::Relaxed);
            return Some(extent);
        }

        None
    }

    /// Find a free block in the given range
    fn find_free_in_range(&self, data: &[u8], start: u64, end: u64) -> Option<u64> {
        (start..end.min(self.total_blocks)).find(|&block| !Self::is_set_in_slice(data, block))
    }

    /// Find a contiguous extent in the given range
    fn find_free_extent_in_range(
        &self,
        data: &[u8],
        start: u64,
        end: u64,
        count: u64,
    ) -> Option<Extent> {
        let mut run_start = start;
        let mut run_len = 0u64;

        for block in start..end.min(self.total_blocks) {
            if Self::is_set_in_slice(data, block) {
                run_start = block + 1;
                run_len = 0;
            } else {
                run_len += 1;
                if run_len >= count {
                    return Some(Extent::new(run_start, count));
                }
            }
        }

        None
    }

    /// Mark an extent as used
    fn mark_extent_used(&self, data: &mut [u8], extent: &Extent) {
        for block in extent.start..extent.end() {
            Self::set_in_slice(data, block);
        }
    }

    /// Free a single block
    pub fn free(&self, block: u64) -> Result<()> {
        if block >= self.total_blocks {
            return Err(Error::Storage(format!(
                "block {} out of range (max {})",
                block, self.total_blocks
            )));
        }

        let mut data = self.data.write();
        if !Self::is_set_in_slice(&data, block) {
            return Err(Error::Storage(format!("block {} is not allocated", block)));
        }

        Self::clear_in_slice(&mut data, block);
        self.free_blocks.fetch_add(1, Ordering::Relaxed);

        // Update hint if this block is before current hint
        let hint = self.search_hint.load(Ordering::Relaxed);
        if block < hint {
            self.search_hint.store(block, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Free an extent
    pub fn free_extent(&self, extent: &Extent) -> Result<()> {
        if extent.end() > self.total_blocks {
            return Err(Error::Storage(format!(
                "extent end {} out of range (max {})",
                extent.end(),
                self.total_blocks
            )));
        }

        let mut data = self.data.write();

        // Verify all blocks are allocated
        for block in extent.start..extent.end() {
            if !Self::is_set_in_slice(&data, block) {
                return Err(Error::Storage(format!("block {} is not allocated", block)));
            }
        }

        // Free all blocks
        for block in extent.start..extent.end() {
            Self::clear_in_slice(&mut data, block);
        }

        self.free_blocks.fetch_add(extent.length, Ordering::Relaxed);

        // Update hint
        let hint = self.search_hint.load(Ordering::Relaxed);
        if extent.start < hint {
            self.search_hint.store(extent.start, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Get the number of free blocks
    pub fn free_count(&self) -> u64 {
        self.free_blocks.load(Ordering::Relaxed)
    }

    /// Get the total number of blocks
    pub fn total_count(&self) -> u64 {
        self.total_blocks
    }

    /// Get the number of allocated blocks
    pub fn allocated_count(&self) -> u64 {
        self.total_blocks - self.free_count()
    }
}

/// Block allocator that persists to disk
pub struct BlockAllocator {
    /// The bitmap
    bitmap: BlockBitmap,
    /// File for persistence
    file: Option<RawFile>,
    /// Offset in file where bitmap starts
    bitmap_offset: u64,
    /// Size of bitmap on disk (aligned)
    bitmap_size: u64,
    /// Dirty flag (bitmap needs to be written)
    dirty: AtomicU64,
}

impl BlockAllocator {
    /// Create a new block allocator (in-memory only)
    #[must_use]
    pub fn new(total_blocks: u64) -> Self {
        Self {
            bitmap: BlockBitmap::new(total_blocks),
            file: None,
            bitmap_offset: 0,
            bitmap_size: 0,
            dirty: AtomicU64::new(0),
        }
    }

    /// Create a block allocator with disk persistence
    pub fn with_file(
        file: RawFile,
        bitmap_offset: u64,
        bitmap_size: u64,
        total_blocks: u64,
    ) -> Result<Self> {
        // Read existing bitmap from disk
        let mut buf = AlignedBuffer::new(bitmap_size as usize);
        file.read_at(bitmap_offset, buf.as_mut_slice())?;

        let bitmap = BlockBitmap::from_bytes(buf.as_slice(), total_blocks);

        Ok(Self {
            bitmap,
            file: Some(file),
            bitmap_offset,
            bitmap_size,
            dirty: AtomicU64::new(0),
        })
    }

    /// Initialize a new allocator on disk (all blocks free)
    pub fn init(
        file: RawFile,
        bitmap_offset: u64,
        bitmap_size: u64,
        total_blocks: u64,
    ) -> Result<Self> {
        let bitmap = BlockBitmap::new(total_blocks);

        // Write initial bitmap (all zeros = all free)
        let buf = AlignedBuffer::new(bitmap_size as usize);
        file.write_at(bitmap_offset, buf.as_slice())?;

        Ok(Self {
            bitmap,
            file: Some(file),
            bitmap_offset,
            bitmap_size,
            dirty: AtomicU64::new(0),
        })
    }

    /// Allocate a single block
    pub fn allocate(&self) -> Result<u64> {
        self.bitmap.allocate().ok_or(Error::DiskFull).inspect(|_| {
            self.mark_dirty();
        })
    }

    /// Allocate multiple blocks (not necessarily contiguous)
    pub fn allocate_multiple(&self, count: u64) -> Result<Vec<u64>> {
        let mut blocks = Vec::with_capacity(count as usize);
        for _ in 0..count {
            match self.bitmap.allocate() {
                Some(block) => blocks.push(block),
                None => {
                    // Rollback: free allocated blocks
                    for block in blocks {
                        let _ = self.bitmap.free(block);
                    }
                    return Err(Error::DiskFull);
                }
            }
        }
        self.mark_dirty();
        Ok(blocks)
    }

    /// Allocate a contiguous extent
    pub fn allocate_extent(&self, count: u64) -> Result<Extent> {
        self.bitmap
            .allocate_extent(count)
            .ok_or(Error::DiskFull)
            .inspect(|_| {
                self.mark_dirty();
            })
    }

    /// Free a single block
    pub fn free(&self, block: u64) -> Result<()> {
        self.bitmap.free(block)?;
        self.mark_dirty();
        Ok(())
    }

    /// Free an extent
    pub fn free_extent(&self, extent: &Extent) -> Result<()> {
        self.bitmap.free_extent(extent)?;
        self.mark_dirty();
        Ok(())
    }

    /// Free multiple blocks
    pub fn free_multiple(&self, blocks: &[u64]) -> Result<()> {
        for &block in blocks {
            self.bitmap.free(block)?;
        }
        self.mark_dirty();
        Ok(())
    }

    /// Check if a block is allocated
    pub fn is_allocated(&self, block: u64) -> bool {
        self.bitmap.is_allocated(block)
    }

    /// Get the number of free blocks
    pub fn free_count(&self) -> u64 {
        self.bitmap.free_count()
    }

    /// Get the total number of blocks
    pub fn total_count(&self) -> u64 {
        self.bitmap.total_count()
    }

    /// Sync bitmap to disk if dirty
    pub fn sync(&self) -> Result<()> {
        if self.dirty.load(Ordering::Relaxed) == 0 {
            return Ok(());
        }

        if let Some(ref file) = self.file {
            let data = self.bitmap.to_bytes();
            let mut buf = AlignedBuffer::new(self.bitmap_size as usize);
            buf.copy_from(&data);
            file.write_at(self.bitmap_offset, buf.as_slice())?;
            file.sync()?;
            self.dirty.store(0, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Mark the allocator as dirty (needs sync)
    fn mark_dirty(&self) {
        self.dirty.store(1, Ordering::Relaxed);
    }
}

impl Default for BlockAllocator {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extent_basic() {
        let extent = Extent::new(10, 5);
        assert_eq!(extent.start, 10);
        assert_eq!(extent.length, 5);
        assert_eq!(extent.end(), 15);
        assert!(extent.contains(10));
        assert!(extent.contains(14));
        assert!(!extent.contains(15));
    }

    #[test]
    fn test_extent_merge() {
        let e1 = Extent::new(0, 5);
        let e2 = Extent::new(5, 3);
        let merged = e1.try_merge(&e2).unwrap();
        assert_eq!(merged.start, 0);
        assert_eq!(merged.length, 8);

        let e3 = Extent::new(10, 2);
        assert!(e1.try_merge(&e3).is_none());
    }

    #[test]
    fn test_bitmap_allocate() {
        let bitmap = BlockBitmap::new(100);
        assert_eq!(bitmap.free_count(), 100);

        let block1 = bitmap.allocate().unwrap();
        assert_eq!(block1, 0);
        assert_eq!(bitmap.free_count(), 99);
        assert!(bitmap.is_allocated(0));

        let block2 = bitmap.allocate().unwrap();
        assert_eq!(block2, 1);
        assert_eq!(bitmap.free_count(), 98);
    }

    #[test]
    fn test_bitmap_free() {
        let bitmap = BlockBitmap::new(100);

        let block = bitmap.allocate().unwrap();
        assert!(bitmap.is_allocated(block));

        bitmap.free(block).unwrap();
        assert!(!bitmap.is_allocated(block));
        assert_eq!(bitmap.free_count(), 100);
    }

    #[test]
    fn test_bitmap_extent_allocation() {
        let bitmap = BlockBitmap::new(100);

        // Allocate a 10-block extent
        let extent = bitmap.allocate_extent(10).unwrap();
        assert_eq!(extent.start, 0);
        assert_eq!(extent.length, 10);
        assert_eq!(bitmap.free_count(), 90);

        // All blocks in extent should be allocated
        for block in extent.start..extent.end() {
            assert!(bitmap.is_allocated(block));
        }

        // Allocate another extent
        let extent2 = bitmap.allocate_extent(5).unwrap();
        assert_eq!(extent2.start, 10);
        assert_eq!(bitmap.free_count(), 85);
    }

    #[test]
    fn test_bitmap_free_extent() {
        let bitmap = BlockBitmap::new(100);

        let extent = bitmap.allocate_extent(10).unwrap();
        bitmap.free_extent(&extent).unwrap();

        assert_eq!(bitmap.free_count(), 100);
        for block in extent.start..extent.end() {
            assert!(!bitmap.is_allocated(block));
        }
    }

    #[test]
    fn test_bitmap_full() {
        let bitmap = BlockBitmap::new(5);

        for _ in 0..5 {
            assert!(bitmap.allocate().is_some());
        }

        assert!(bitmap.allocate().is_none());
        assert_eq!(bitmap.free_count(), 0);
    }

    #[test]
    fn test_bitmap_serialization() {
        let bitmap = BlockBitmap::new(100);

        // Allocate some blocks
        bitmap.allocate().unwrap(); // 0
        bitmap.allocate().unwrap(); // 1
        bitmap.allocate().unwrap(); // 2

        let bytes = bitmap.to_bytes();

        // Reload
        let bitmap2 = BlockBitmap::from_bytes(&bytes, 100);
        assert_eq!(bitmap2.free_count(), 97);
        assert!(bitmap2.is_allocated(0));
        assert!(bitmap2.is_allocated(1));
        assert!(bitmap2.is_allocated(2));
        assert!(!bitmap2.is_allocated(3));
    }

    #[test]
    fn test_allocator_basic() {
        let allocator = BlockAllocator::new(1000);

        let block = allocator.allocate().unwrap();
        assert_eq!(block, 0);
        assert!(allocator.is_allocated(0));
        assert_eq!(allocator.free_count(), 999);

        allocator.free(block).unwrap();
        assert!(!allocator.is_allocated(0));
        assert_eq!(allocator.free_count(), 1000);
    }

    #[test]
    fn test_allocator_multiple() {
        let allocator = BlockAllocator::new(100);

        let blocks = allocator.allocate_multiple(10).unwrap();
        assert_eq!(blocks.len(), 10);
        assert_eq!(allocator.free_count(), 90);

        for &block in &blocks {
            assert!(allocator.is_allocated(block));
        }

        allocator.free_multiple(&blocks).unwrap();
        assert_eq!(allocator.free_count(), 100);
    }

    #[test]
    fn test_allocator_extent() {
        let allocator = BlockAllocator::new(100);

        let extent = allocator.allocate_extent(20).unwrap();
        assert_eq!(extent.length, 20);
        assert_eq!(allocator.free_count(), 80);

        allocator.free_extent(&extent).unwrap();
        assert_eq!(allocator.free_count(), 100);
    }

    #[test]
    fn test_block_creation() {
        let data = Bytes::from("Hello, ObjectIO!");
        let block = Block::new(data.clone());

        assert_eq!(block.data, data);
        assert!(block.verify());
        assert_eq!(block.size(), 16);
    }
}
