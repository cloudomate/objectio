//! Chunk mapping for block storage
//!
//! Maps logical block addresses (LBAs) to chunk objects stored in ObjectIO.

use crate::{DEFAULT_CHUNK_SIZE, LBA_SIZE};

/// Chunk identifier (index within a volume)
pub type ChunkId = u64;

/// A range of bytes within a chunk
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRange {
    /// Chunk identifier
    pub chunk_id: ChunkId,
    /// Byte offset within the chunk
    pub offset_in_chunk: u64,
    /// Number of bytes in this range
    pub length: u64,
}

/// Maps LBAs to chunks
///
/// Volumes are divided into fixed-size chunks. Each chunk is stored as
/// an object in ObjectIO's object storage layer.
///
/// ```text
/// Volume (100GB)
/// ├── Chunk 0:  bytes 0 - 4MB       → vol_{id}/chunk_00000000
/// ├── Chunk 1:  bytes 4MB - 8MB     → vol_{id}/chunk_00000001
/// ├── Chunk 2:  bytes 8MB - 12MB    → vol_{id}/chunk_00000002
/// │   ...
/// └── Chunk N:  ...                 → vol_{id}/chunk_{N:08x}
/// ```
#[derive(Debug, Clone)]
pub struct ChunkMapper {
    /// Chunk size in bytes
    chunk_size: u64,
}

impl Default for ChunkMapper {
    fn default() -> Self {
        Self::new(DEFAULT_CHUNK_SIZE)
    }
}

impl ChunkMapper {
    /// Create a new chunk mapper with the specified chunk size
    pub fn new(chunk_size: u64) -> Self {
        assert!(chunk_size > 0, "Chunk size must be positive");
        assert!(
            chunk_size.is_power_of_two(),
            "Chunk size should be a power of two"
        );
        Self { chunk_size }
    }

    /// Get the chunk size
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// Get the number of LBAs per chunk
    pub fn lbas_per_chunk(&self) -> u64 {
        self.chunk_size / LBA_SIZE
    }

    /// Convert a byte offset to a chunk ID
    pub fn byte_offset_to_chunk_id(&self, byte_offset: u64) -> ChunkId {
        byte_offset / self.chunk_size
    }

    /// Convert an LBA to a chunk ID
    pub fn lba_to_chunk_id(&self, lba: u64) -> ChunkId {
        let byte_offset = lba * LBA_SIZE;
        self.byte_offset_to_chunk_id(byte_offset)
    }

    /// Convert a byte range to chunk ranges
    ///
    /// Returns a list of chunk ranges that cover the specified byte range.
    /// Handles ranges that span multiple chunks.
    pub fn byte_range_to_chunks(&self, start_byte: u64, length: u64) -> Vec<ChunkRange> {
        if length == 0 {
            return Vec::new();
        }

        let end_byte = start_byte + length;
        let start_chunk = start_byte / self.chunk_size;
        let end_chunk = (end_byte - 1) / self.chunk_size;

        let mut ranges = Vec::with_capacity((end_chunk - start_chunk + 1) as usize);

        for chunk_id in start_chunk..=end_chunk {
            let chunk_start = chunk_id * self.chunk_size;
            let chunk_end = chunk_start + self.chunk_size;

            // Calculate the intersection of [start_byte, end_byte) and [chunk_start, chunk_end)
            let range_start = start_byte.max(chunk_start);
            let range_end = end_byte.min(chunk_end);

            ranges.push(ChunkRange {
                chunk_id,
                offset_in_chunk: range_start - chunk_start,
                length: range_end - range_start,
            });
        }

        ranges
    }

    /// Convert an LBA range to chunk ranges
    pub fn lba_range_to_chunks(&self, start_lba: u64, lba_count: u64) -> Vec<ChunkRange> {
        let start_byte = start_lba * LBA_SIZE;
        let length = lba_count * LBA_SIZE;
        self.byte_range_to_chunks(start_byte, length)
    }

    /// Get the object key for a chunk
    pub fn chunk_key(&self, volume_id: &str, chunk_id: ChunkId) -> String {
        format!("vol_{}/chunk_{:08x}", volume_id, chunk_id)
    }

    /// Get the bucket name for block storage
    pub fn block_bucket() -> &'static str {
        "__block__"
    }

    /// Calculate the number of chunks needed for a volume of the given size
    pub fn chunks_for_size(&self, size_bytes: u64) -> u64 {
        (size_bytes + self.chunk_size - 1) / self.chunk_size
    }

    /// Calculate the chunk-aligned size for a volume
    pub fn aligned_size(&self, size_bytes: u64) -> u64 {
        self.chunks_for_size(size_bytes) * self.chunk_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_mapper_default() {
        let mapper = ChunkMapper::default();
        assert_eq!(mapper.chunk_size(), DEFAULT_CHUNK_SIZE);
        assert_eq!(mapper.lbas_per_chunk(), 8192); // 4MB / 512 bytes
    }

    #[test]
    fn test_byte_offset_to_chunk_id() {
        let mapper = ChunkMapper::new(4 * 1024 * 1024); // 4MB chunks

        assert_eq!(mapper.byte_offset_to_chunk_id(0), 0);
        assert_eq!(mapper.byte_offset_to_chunk_id(4 * 1024 * 1024 - 1), 0);
        assert_eq!(mapper.byte_offset_to_chunk_id(4 * 1024 * 1024), 1);
        assert_eq!(mapper.byte_offset_to_chunk_id(8 * 1024 * 1024), 2);
    }

    #[test]
    fn test_lba_to_chunk_id() {
        let mapper = ChunkMapper::new(4 * 1024 * 1024); // 4MB chunks

        // 4MB = 8192 LBAs (512 bytes each)
        assert_eq!(mapper.lba_to_chunk_id(0), 0);
        assert_eq!(mapper.lba_to_chunk_id(8191), 0);
        assert_eq!(mapper.lba_to_chunk_id(8192), 1);
        assert_eq!(mapper.lba_to_chunk_id(16384), 2);
    }

    #[test]
    fn test_byte_range_single_chunk() {
        let mapper = ChunkMapper::new(4 * 1024 * 1024);

        // Read 1KB from start of chunk 0
        let ranges = mapper.byte_range_to_chunks(0, 1024);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].chunk_id, 0);
        assert_eq!(ranges[0].offset_in_chunk, 0);
        assert_eq!(ranges[0].length, 1024);

        // Read 1KB from middle of chunk 0
        let ranges = mapper.byte_range_to_chunks(1024, 1024);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].chunk_id, 0);
        assert_eq!(ranges[0].offset_in_chunk, 1024);
        assert_eq!(ranges[0].length, 1024);
    }

    #[test]
    fn test_byte_range_spanning_chunks() {
        let mapper = ChunkMapper::new(4 * 1024 * 1024); // 4MB chunks

        // Read 8MB starting at 2MB offset (spans chunks 0 and 1, partially into 2)
        let ranges = mapper.byte_range_to_chunks(2 * 1024 * 1024, 8 * 1024 * 1024);
        assert_eq!(ranges.len(), 3);

        // Chunk 0: bytes 2MB - 4MB (2MB)
        assert_eq!(ranges[0].chunk_id, 0);
        assert_eq!(ranges[0].offset_in_chunk, 2 * 1024 * 1024);
        assert_eq!(ranges[0].length, 2 * 1024 * 1024);

        // Chunk 1: bytes 4MB - 8MB (4MB)
        assert_eq!(ranges[1].chunk_id, 1);
        assert_eq!(ranges[1].offset_in_chunk, 0);
        assert_eq!(ranges[1].length, 4 * 1024 * 1024);

        // Chunk 2: bytes 8MB - 10MB (2MB)
        assert_eq!(ranges[2].chunk_id, 2);
        assert_eq!(ranges[2].offset_in_chunk, 0);
        assert_eq!(ranges[2].length, 2 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_key() {
        let mapper = ChunkMapper::default();
        assert_eq!(mapper.chunk_key("abc123", 0), "vol_abc123/chunk_00000000");
        assert_eq!(mapper.chunk_key("abc123", 255), "vol_abc123/chunk_000000ff");
    }

    #[test]
    fn test_chunks_for_size() {
        let mapper = ChunkMapper::new(4 * 1024 * 1024);

        // Exact multiple
        assert_eq!(mapper.chunks_for_size(4 * 1024 * 1024), 1);
        assert_eq!(mapper.chunks_for_size(8 * 1024 * 1024), 2);

        // Not exact multiple (rounds up)
        assert_eq!(mapper.chunks_for_size(1), 1);
        assert_eq!(mapper.chunks_for_size(4 * 1024 * 1024 + 1), 2);
    }

    #[test]
    fn test_empty_range() {
        let mapper = ChunkMapper::default();
        let ranges = mapper.byte_range_to_chunks(1000, 0);
        assert!(ranges.is_empty());
    }
}
