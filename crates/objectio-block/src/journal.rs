//! Write-ahead journal for block storage durability
//!
//! Provides crash recovery for the write cache by persisting write operations
//! to a journal file before acknowledging them to the client.

use crate::chunk::ChunkId;
use crate::error::{BlockError, BlockResult};

use bytes::Bytes;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

/// Magic number for journal file header
const JOURNAL_MAGIC: u64 = 0x4F424A5F4A524E4C; // "OBJ_JRNL"

/// Journal file version
const JOURNAL_VERSION: u32 = 1;

/// Journal entry type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    /// Write data to a chunk
    Write = 1,
    /// Flush completed for a chunk
    Flush = 2,
    /// Checkpoint (all prior entries can be discarded)
    Checkpoint = 3,
}

impl TryFrom<u8> for EntryType {
    type Error = BlockError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EntryType::Write),
            2 => Ok(EntryType::Flush),
            3 => Ok(EntryType::Checkpoint),
            _ => Err(BlockError::Journal(format!(
                "invalid entry type: {}",
                value
            ))),
        }
    }
}

/// Journal entry header (fixed size for easy reading)
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// Sequence number
    pub sequence: u64,
    /// Entry type
    pub entry_type: EntryType,
    /// Volume ID
    pub volume_id: String,
    /// Chunk ID
    pub chunk_id: ChunkId,
    /// Offset within chunk
    pub offset: u64,
    /// Data (for write entries)
    pub data: Option<Bytes>,
    /// CRC32 checksum
    pub checksum: u32,
}

impl JournalEntry {
    /// Create a write entry
    pub fn write(
        sequence: u64,
        volume_id: String,
        chunk_id: ChunkId,
        offset: u64,
        data: Bytes,
    ) -> Self {
        let mut entry = Self {
            sequence,
            entry_type: EntryType::Write,
            volume_id,
            chunk_id,
            offset,
            data: Some(data),
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Create a flush entry
    pub fn flush(sequence: u64, volume_id: String, chunk_id: ChunkId) -> Self {
        let mut entry = Self {
            sequence,
            entry_type: EntryType::Flush,
            volume_id,
            chunk_id,
            offset: 0,
            data: None,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Create a checkpoint entry
    pub fn checkpoint(sequence: u64) -> Self {
        let mut entry = Self {
            sequence,
            entry_type: EntryType::Checkpoint,
            volume_id: String::new(),
            chunk_id: 0,
            offset: 0,
            data: None,
            checksum: 0,
        };
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// Compute CRC32 checksum
    fn compute_checksum(&self) -> u32 {
        let mut data = Vec::new();
        data.extend_from_slice(&self.sequence.to_le_bytes());
        data.push(self.entry_type as u8);
        data.extend_from_slice(self.volume_id.as_bytes());
        data.extend_from_slice(&self.chunk_id.to_le_bytes());
        data.extend_from_slice(&self.offset.to_le_bytes());
        if let Some(ref d) = self.data {
            data.extend_from_slice(d);
        }
        crc32c::crc32c(&data)
    }

    /// Verify checksum
    pub fn verify(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    /// Serialize to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Entry header
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.push(self.entry_type as u8);

        // Volume ID (length-prefixed)
        let vol_bytes = self.volume_id.as_bytes();
        buf.extend_from_slice(&(vol_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(vol_bytes);

        // Chunk ID and offset
        buf.extend_from_slice(&self.chunk_id.to_le_bytes());
        buf.extend_from_slice(&self.offset.to_le_bytes());

        // Data (length-prefixed)
        if let Some(ref data) = self.data {
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        // Checksum
        buf.extend_from_slice(&self.checksum.to_le_bytes());

        buf
    }

    /// Deserialize from reader
    pub fn deserialize<R: Read>(reader: &mut R) -> BlockResult<Self> {
        // Sequence number
        let mut seq_buf = [0u8; 8];
        reader
            .read_exact(&mut seq_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let sequence = u64::from_le_bytes(seq_buf);

        // Entry type
        let mut type_buf = [0u8; 1];
        reader
            .read_exact(&mut type_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let entry_type = EntryType::try_from(type_buf[0])?;

        // Volume ID
        let mut vol_len_buf = [0u8; 2];
        reader
            .read_exact(&mut vol_len_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let vol_len = u16::from_le_bytes(vol_len_buf) as usize;
        let mut vol_buf = vec![0u8; vol_len];
        reader
            .read_exact(&mut vol_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let volume_id = String::from_utf8(vol_buf)
            .map_err(|e| BlockError::Journal(format!("invalid volume ID: {}", e)))?;

        // Chunk ID and offset
        let mut chunk_buf = [0u8; 8];
        reader
            .read_exact(&mut chunk_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let chunk_id = u64::from_le_bytes(chunk_buf);

        let mut offset_buf = [0u8; 8];
        reader
            .read_exact(&mut offset_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let offset = u64::from_le_bytes(offset_buf);

        // Data
        let mut data_len_buf = [0u8; 4];
        reader
            .read_exact(&mut data_len_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let data_len = u32::from_le_bytes(data_len_buf) as usize;
        let data = if data_len > 0 {
            let mut data_buf = vec![0u8; data_len];
            reader
                .read_exact(&mut data_buf)
                .map_err(|e| BlockError::Journal(e.to_string()))?;
            Some(Bytes::from(data_buf))
        } else {
            None
        };

        // Checksum
        let mut crc_buf = [0u8; 4];
        reader
            .read_exact(&mut crc_buf)
            .map_err(|e| BlockError::Journal(e.to_string()))?;
        let checksum = u32::from_le_bytes(crc_buf);

        Ok(Self {
            sequence,
            entry_type,
            volume_id,
            chunk_id,
            offset,
            data,
            checksum,
        })
    }
}

/// Write-ahead journal for block storage
pub struct WriteJournal {
    /// Journal file path
    path: PathBuf,
    /// Journal file writer
    writer: Mutex<Option<BufWriter<File>>>,
    /// Current sequence number
    sequence: AtomicU64,
    /// Last checkpoint sequence
    last_checkpoint: AtomicU64,
    /// Maximum journal size before rotation
    max_size: u64,
    /// Current journal size
    current_size: AtomicU64,
}

impl WriteJournal {
    /// Create or open a journal at the given path
    pub fn open<P: AsRef<Path>>(path: P, max_size: u64) -> BlockResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BlockError::Journal(format!("failed to create journal dir: {}", e)))?;
        }

        // Open or create journal file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| BlockError::Journal(format!("failed to open journal: {}", e)))?;

        let file_len = file
            .metadata()
            .map_err(|e| BlockError::Journal(format!("failed to stat journal: {}", e)))?
            .len();

        let (sequence, last_checkpoint) = if file_len > 0 {
            // Existing journal - recover state
            let (seq, checkpoint) = Self::read_header(&file)?;
            (seq, checkpoint)
        } else {
            // New journal - write header
            let mut writer = BufWriter::new(file);
            Self::write_header(&mut writer, 0, 0)?;
            let file = writer
                .into_inner()
                .map_err(|e| BlockError::Journal(format!("flush failed: {}", e)))?;
            (0, 0)
        };

        // Reopen for appending
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&path)
            .map_err(|e| BlockError::Journal(format!("failed to reopen journal: {}", e)))?;

        let current_size = file
            .metadata()
            .map_err(|e| BlockError::Journal(format!("failed to stat journal: {}", e)))?
            .len();

        info!(
            "Opened journal at {:?}: seq={}, checkpoint={}, size={}",
            path, sequence, last_checkpoint, current_size
        );

        Ok(Self {
            path,
            writer: Mutex::new(Some(BufWriter::new(file))),
            sequence: AtomicU64::new(sequence),
            last_checkpoint: AtomicU64::new(last_checkpoint),
            max_size,
            current_size: AtomicU64::new(current_size),
        })
    }

    /// Write journal header
    fn write_header<W: Write>(writer: &mut W, sequence: u64, checkpoint: u64) -> BlockResult<()> {
        writer
            .write_all(&JOURNAL_MAGIC.to_le_bytes())
            .map_err(|e| BlockError::Journal(format!("failed to write magic: {}", e)))?;
        writer
            .write_all(&JOURNAL_VERSION.to_le_bytes())
            .map_err(|e| BlockError::Journal(format!("failed to write version: {}", e)))?;
        writer
            .write_all(&sequence.to_le_bytes())
            .map_err(|e| BlockError::Journal(format!("failed to write sequence: {}", e)))?;
        writer
            .write_all(&checkpoint.to_le_bytes())
            .map_err(|e| BlockError::Journal(format!("failed to write checkpoint: {}", e)))?;
        writer
            .flush()
            .map_err(|e| BlockError::Journal(format!("failed to flush header: {}", e)))?;
        Ok(())
    }

    /// Read journal header
    fn read_header(file: &File) -> BlockResult<(u64, u64)> {
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| BlockError::Journal(format!("failed to seek: {}", e)))?;

        let mut magic_buf = [0u8; 8];
        reader
            .read_exact(&mut magic_buf)
            .map_err(|e| BlockError::Journal(format!("failed to read magic: {}", e)))?;
        let magic = u64::from_le_bytes(magic_buf);
        if magic != JOURNAL_MAGIC {
            return Err(BlockError::Journal("invalid journal magic".to_string()));
        }

        let mut version_buf = [0u8; 4];
        reader
            .read_exact(&mut version_buf)
            .map_err(|e| BlockError::Journal(format!("failed to read version: {}", e)))?;
        let version = u32::from_le_bytes(version_buf);
        if version != JOURNAL_VERSION {
            return Err(BlockError::Journal(format!(
                "unsupported journal version: {}",
                version
            )));
        }

        let mut seq_buf = [0u8; 8];
        reader
            .read_exact(&mut seq_buf)
            .map_err(|e| BlockError::Journal(format!("failed to read sequence: {}", e)))?;
        let sequence = u64::from_le_bytes(seq_buf);

        let mut checkpoint_buf = [0u8; 8];
        reader
            .read_exact(&mut checkpoint_buf)
            .map_err(|e| BlockError::Journal(format!("failed to read checkpoint: {}", e)))?;
        let checkpoint = u64::from_le_bytes(checkpoint_buf);

        Ok((sequence, checkpoint))
    }

    /// Append a journal entry
    pub fn append(&self, entry: &JournalEntry) -> BlockResult<u64> {
        let data = entry.serialize();
        let data_len = data.len() as u64;

        let mut writer_guard = self.writer.lock();
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| BlockError::Journal("journal closed".to_string()))?;

        writer
            .write_all(&data)
            .map_err(|e| BlockError::Journal(format!("write failed: {}", e)))?;
        writer
            .flush()
            .map_err(|e| BlockError::Journal(format!("flush failed: {}", e)))?;

        self.current_size.fetch_add(data_len, Ordering::SeqCst);
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);

        Ok(seq)
    }

    /// Log a write operation
    pub fn log_write(
        &self,
        volume_id: &str,
        chunk_id: ChunkId,
        offset: u64,
        data: Bytes,
    ) -> BlockResult<u64> {
        let seq = self.sequence.load(Ordering::SeqCst);
        let entry = JournalEntry::write(seq, volume_id.to_string(), chunk_id, offset, data);
        self.append(&entry)
    }

    /// Log a flush completion
    pub fn log_flush(&self, volume_id: &str, chunk_id: ChunkId) -> BlockResult<u64> {
        let seq = self.sequence.load(Ordering::SeqCst);
        let entry = JournalEntry::flush(seq, volume_id.to_string(), chunk_id);
        self.append(&entry)
    }

    /// Write a checkpoint
    pub fn checkpoint(&self) -> BlockResult<u64> {
        let seq = self.sequence.load(Ordering::SeqCst);
        let entry = JournalEntry::checkpoint(seq);
        let result = self.append(&entry)?;
        self.last_checkpoint.store(seq, Ordering::SeqCst);
        debug!("Journal checkpoint at sequence {}", seq);
        Ok(result)
    }

    /// Recover unflushed writes from journal
    pub fn recover(&self) -> BlockResult<Vec<JournalEntry>> {
        let file = File::open(&self.path)
            .map_err(|e| BlockError::Journal(format!("failed to open for recovery: {}", e)))?;

        let mut reader = BufReader::new(file);

        // Skip header
        reader
            .seek(SeekFrom::Start(28)) // 8 + 4 + 8 + 8 bytes
            .map_err(|e| BlockError::Journal(format!("failed to seek past header: {}", e)))?;

        let mut all_entries = Vec::new();
        let mut last_checkpoint_seq = self.last_checkpoint.load(Ordering::SeqCst);

        // Scan all entries, tracking the last checkpoint seen on disk
        loop {
            match JournalEntry::deserialize(&mut reader) {
                Ok(entry) => {
                    if !entry.verify() {
                        warn!(
                            "Journal entry {} failed checksum, stopping recovery",
                            entry.sequence
                        );
                        break;
                    }
                    if entry.entry_type == EntryType::Checkpoint {
                        last_checkpoint_seq = entry.sequence;
                    } else {
                        all_entries.push(entry);
                    }
                }
                Err(_) => break, // EOF or corruption
            }
        }

        // Only keep write entries after the last checkpoint
        let entries: Vec<_> = all_entries
            .into_iter()
            .filter(|e| e.sequence > last_checkpoint_seq && e.entry_type == EntryType::Write)
            .collect();

        info!("Recovered {} journal entries", entries.len());
        Ok(entries)
    }

    /// Check if journal needs rotation
    pub fn needs_rotation(&self) -> bool {
        self.current_size.load(Ordering::SeqCst) > self.max_size
    }

    /// Rotate journal (create new one, discard old)
    pub fn rotate(&self) -> BlockResult<()> {
        // Close current journal
        {
            let mut writer_guard = self.writer.lock();
            *writer_guard = None;
        }

        // Rename old journal
        let old_path = self.path.with_extension("old");
        std::fs::rename(&self.path, &old_path)
            .map_err(|e| BlockError::Journal(format!("failed to rename old journal: {}", e)))?;

        // Create new journal
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)
            .map_err(|e| BlockError::Journal(format!("failed to create new journal: {}", e)))?;

        let mut writer = BufWriter::new(file);
        let seq = self.sequence.load(Ordering::SeqCst);
        Self::write_header(&mut writer, seq, seq)?;

        {
            let mut writer_guard = self.writer.lock();
            *writer_guard = Some(writer);
        }

        self.last_checkpoint.store(seq, Ordering::SeqCst);
        self.current_size.store(28, Ordering::SeqCst); // Header size

        // Delete old journal
        if let Err(e) = std::fs::remove_file(&old_path) {
            warn!("Failed to remove old journal: {}", e);
        }

        info!("Rotated journal at sequence {}", seq);
        Ok(())
    }

    /// Sync journal to disk
    pub fn sync(&self) -> BlockResult<()> {
        let writer_guard = self.writer.lock();
        if let Some(ref writer) = *writer_guard {
            writer
                .get_ref()
                .sync_all()
                .map_err(|e| BlockError::Journal(format!("sync failed: {}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_journal_entry_serialization() {
        let entry = JournalEntry::write(
            42,
            "vol-123".to_string(),
            5,
            1024,
            Bytes::from(vec![0xAB; 100]),
        );

        assert!(entry.verify());

        let data = entry.serialize();
        let mut reader = std::io::Cursor::new(data);
        let recovered = JournalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(recovered.sequence, 42);
        assert_eq!(recovered.volume_id, "vol-123");
        assert_eq!(recovered.chunk_id, 5);
        assert_eq!(recovered.offset, 1024);
        assert_eq!(recovered.data.as_ref().unwrap().len(), 100);
        assert!(recovered.verify());
    }

    #[test]
    fn test_journal_write_and_recover() {
        let dir = tempdir().unwrap();
        let journal_path = dir.path().join("test.journal");

        // Write some entries
        {
            let journal = WriteJournal::open(&journal_path, 1024 * 1024).unwrap();
            journal
                .log_write("vol1", 0, 0, Bytes::from(vec![1; 100]))
                .unwrap();
            journal
                .log_write("vol1", 1, 0, Bytes::from(vec![2; 100]))
                .unwrap();
            journal.checkpoint().unwrap();
            journal
                .log_write("vol1", 2, 0, Bytes::from(vec![3; 100]))
                .unwrap();
        }

        // Recover
        {
            let journal = WriteJournal::open(&journal_path, 1024 * 1024).unwrap();
            let entries = journal.recover().unwrap();

            // Only entry after checkpoint should be recovered
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].chunk_id, 2);
        }
    }
}
