//! Write-ahead log implementation
//!
//! The WAL provides durability guarantees for storage operations.
//! It supports transactions with begin/commit/abort semantics and
//! can be replayed after a crash to recover consistent state.
//!
//! Record format:
//! ```text
//! +--------+------+--------+--------+------+--------+
//! | Magic  | Type | TxnID  | Length | Data | CRC32C |
//! | 4B     | 1B   | 8B     | 4B     | var  | 4B     |
//! +--------+------+--------+--------+------+--------+
//! ```

use crate::raw_io::{AlignedBuffer, RawFile};
use objectio_common::{Error, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// WAL record magic number
const WAL_RECORD_MAGIC: u32 = 0x57414C52; // "WALR"

/// WAL header magic number
const WAL_HEADER_MAGIC: u32 = 0x57414C48; // "WALH"

/// WAL header size (aligned to 4KB)
const WAL_HEADER_SIZE: u64 = 4096;

/// Minimum record header size (magic + type + txn_id + length)
const RECORD_HEADER_SIZE: usize = 17;

/// Record type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Begin a new transaction
    BeginTxn = 1,
    /// Write data (part of a transaction)
    Write = 2,
    /// Commit a transaction
    Commit = 3,
    /// Abort a transaction
    Abort = 4,
    /// Checkpoint marker
    Checkpoint = 5,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::BeginTxn),
            2 => Some(Self::Write),
            3 => Some(Self::Commit),
            4 => Some(Self::Abort),
            5 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

/// A write operation to be applied
#[derive(Debug, Clone)]
pub struct WriteOp {
    /// Block number
    pub block_num: u64,
    /// Object ID
    pub object_id: [u8; 16],
    /// Offset within object
    pub object_offset: u64,
    /// Data to write
    pub data: Vec<u8>,
}

impl WriteOp {
    /// Serialize a write operation
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32 + self.data.len());
        buf.extend_from_slice(&self.block_num.to_le_bytes());
        buf.extend_from_slice(&self.object_id);
        buf.extend_from_slice(&self.object_offset.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }

    /// Deserialize a write operation
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 36 {
            return Err(Error::Storage("write op data too small".into()));
        }

        let block_num = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let mut object_id = [0u8; 16];
        object_id.copy_from_slice(&data[8..24]);
        let object_offset = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let data_len = u32::from_le_bytes(data[32..36].try_into().unwrap()) as usize;

        if data.len() < 36 + data_len {
            return Err(Error::Storage("write op data truncated".into()));
        }

        Ok(Self {
            block_num,
            object_id,
            object_offset,
            data: data[36..36 + data_len].to_vec(),
        })
    }
}

/// WAL sync mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Sync after every write (safest, slowest)
    Always,
    /// Sync after commit only
    OnCommit,
    /// Don't sync (fastest, risk of data loss)
    Never,
}

/// WAL header stored at the beginning of the WAL file
#[derive(Debug, Clone)]
struct WalHeader {
    magic: u32,
    version: u32,
    /// Offset of the next write position
    write_offset: u64,
    /// Last committed transaction ID
    last_committed_txn: u64,
    /// Last checkpoint offset
    last_checkpoint: u64,
    checksum: u32,
}

impl WalHeader {
    fn new() -> Self {
        Self {
            magic: WAL_HEADER_MAGIC,
            version: 1,
            write_offset: WAL_HEADER_SIZE,
            last_committed_txn: 0,
            last_checkpoint: 0,
            checksum: 0,
        }
    }

    fn to_bytes(&self) -> [u8; 36] {
        let mut buf = [0u8; 36];
        let mut cursor = &mut buf[..];
        cursor.write_all(&self.magic.to_le_bytes()).unwrap();
        cursor.write_all(&self.version.to_le_bytes()).unwrap();
        cursor.write_all(&self.write_offset.to_le_bytes()).unwrap();
        cursor
            .write_all(&self.last_committed_txn.to_le_bytes())
            .unwrap();
        cursor
            .write_all(&self.last_checkpoint.to_le_bytes())
            .unwrap();
        cursor.write_all(&self.checksum.to_le_bytes()).unwrap();
        buf
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 36 {
            return Err(Error::Storage("WAL header too small".into()));
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != WAL_HEADER_MAGIC {
            return Err(Error::Storage("invalid WAL header magic".into()));
        }

        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let write_offset = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let last_committed_txn = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let last_checkpoint = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let checksum = u32::from_le_bytes(data[32..36].try_into().unwrap());

        let header = Self {
            magic,
            version,
            write_offset,
            last_committed_txn,
            last_checkpoint,
            checksum,
        };

        // Verify checksum
        let computed = header.compute_checksum();
        if computed != checksum {
            return Err(Error::Storage("WAL header checksum mismatch".into()));
        }

        Ok(header)
    }

    /// Offset of the checksum field (sum of magic + version + write_offset +
    /// last_committed_txn + last_checkpoint = 4 + 4 + 8 + 8 + 8 = 32)
    const CHECKSUM_OFFSET: usize = 32;

    fn compute_checksum(&self) -> u32 {
        let bytes = self.to_bytes();
        crc32c::crc32c(&bytes[..Self::CHECKSUM_OFFSET]) // Everything before checksum field
    }

    fn update_checksum(&mut self) {
        self.checksum = self.compute_checksum();
    }
}

/// A WAL record
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub record_type: RecordType,
    pub txn_id: u64,
    pub data: Vec<u8>,
}

impl WalRecord {
    /// Create a new record
    pub fn new(record_type: RecordType, txn_id: u64, data: Vec<u8>) -> Self {
        Self {
            record_type,
            txn_id,
            data,
        }
    }

    /// Serialize a record to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let total_len = RECORD_HEADER_SIZE + self.data.len() + 4; // +4 for CRC
        let mut buf = Vec::with_capacity(total_len);

        buf.extend_from_slice(&WAL_RECORD_MAGIC.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&self.txn_id.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.data);

        // Compute CRC over everything before the CRC field
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Parse a record from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < RECORD_HEADER_SIZE + 4 {
            return Err(Error::Storage("WAL record too small".into()));
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != WAL_RECORD_MAGIC {
            return Err(Error::Storage("invalid WAL record magic".into()));
        }

        let record_type = RecordType::from_u8(data[4])
            .ok_or_else(|| Error::Storage("invalid WAL record type".into()))?;
        let txn_id = u64::from_le_bytes(data[5..13].try_into().unwrap());
        let data_len = u32::from_le_bytes(data[13..17].try_into().unwrap()) as usize;

        let total_len = RECORD_HEADER_SIZE + data_len + 4;
        if data.len() < total_len {
            return Err(Error::Storage("WAL record data truncated".into()));
        }

        let record_data = data[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + data_len].to_vec();
        let stored_crc = u32::from_le_bytes(
            data[RECORD_HEADER_SIZE + data_len..RECORD_HEADER_SIZE + data_len + 4]
                .try_into()
                .unwrap(),
        );

        // Verify CRC
        let computed_crc = crc32c::crc32c(&data[..RECORD_HEADER_SIZE + data_len]);
        if computed_crc != stored_crc {
            return Err(Error::Storage("WAL record CRC mismatch".into()));
        }

        Ok(Self {
            record_type,
            txn_id,
            data: record_data,
        })
    }

    /// Get the serialized size of this record
    pub fn serialized_size(&self) -> usize {
        RECORD_HEADER_SIZE + self.data.len() + 4
    }
}

/// Active transaction state
struct ActiveTransaction {
    writes: Vec<WriteOp>,
}

/// Write-ahead log for durability
pub struct WriteAheadLog {
    /// Underlying file
    file: RawFile,
    /// Current header
    header: Mutex<WalHeader>,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Active transactions
    active_txns: Mutex<HashMap<u64, ActiveTransaction>>,
    /// Sync mode
    sync_mode: SyncMode,
    /// WAL size limit
    size_limit: u64,
}

impl WriteAheadLog {
    /// Create a new WAL at the given path
    pub fn create(path: impl AsRef<Path>, size: u64, sync_mode: SyncMode) -> Result<Self> {
        let file = RawFile::create(&path, size)?;

        // Write initial header
        let mut header = WalHeader::new();
        header.update_checksum();

        let mut buf = AlignedBuffer::new(WAL_HEADER_SIZE as usize);
        buf.copy_from(&header.to_bytes());
        file.write_at(0, buf.as_slice())?;
        file.sync()?;

        Ok(Self {
            file,
            header: Mutex::new(header),
            next_txn_id: AtomicU64::new(1),
            active_txns: Mutex::new(HashMap::new()),
            sync_mode,
            size_limit: size,
        })
    }

    /// Open an existing WAL
    pub fn open(path: impl AsRef<Path>, sync_mode: SyncMode) -> Result<Self> {
        let file = RawFile::open(&path, false)?;

        // Read header
        let mut buf = AlignedBuffer::new(WAL_HEADER_SIZE as usize);
        file.read_at(0, buf.as_mut_slice())?;
        let header = WalHeader::from_bytes(buf.as_slice())?;

        let next_txn_id = header.last_committed_txn + 1;
        let size_limit = file.size();

        Ok(Self {
            file,
            header: Mutex::new(header),
            next_txn_id: AtomicU64::new(next_txn_id),
            active_txns: Mutex::new(HashMap::new()),
            sync_mode,
            size_limit,
        })
    }

    /// Begin a new transaction
    pub fn begin_txn(&self) -> Result<u64> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);

        // Write begin record
        let record = WalRecord::new(RecordType::BeginTxn, txn_id, vec![]);
        self.append_record(&record)?;

        // Track active transaction
        let mut active = self.active_txns.lock();
        active.insert(txn_id, ActiveTransaction { writes: vec![] });

        Ok(txn_id)
    }

    /// Add a write operation to a transaction
    pub fn write(&self, txn_id: u64, write_op: WriteOp) -> Result<()> {
        // Verify transaction exists
        {
            let mut active = self.active_txns.lock();
            let txn = active
                .get_mut(&txn_id)
                .ok_or_else(|| Error::Storage(format!("transaction {} not found", txn_id)))?;
            txn.writes.push(write_op.clone());
        }

        // Write record
        let record = WalRecord::new(RecordType::Write, txn_id, write_op.to_bytes());
        self.append_record(&record)?;

        Ok(())
    }

    /// Commit a transaction
    pub fn commit(&self, txn_id: u64) -> Result<Vec<WriteOp>> {
        // Get and remove the transaction
        let writes = {
            let mut active = self.active_txns.lock();
            let txn = active
                .remove(&txn_id)
                .ok_or_else(|| Error::Storage(format!("transaction {} not found", txn_id)))?;
            txn.writes
        };

        // Write commit record
        let record = WalRecord::new(RecordType::Commit, txn_id, vec![]);
        self.append_record(&record)?;

        // Update header
        {
            let mut header = self.header.lock();
            header.last_committed_txn = txn_id;
            header.update_checksum();
        }

        // Sync if required
        if self.sync_mode == SyncMode::Always || self.sync_mode == SyncMode::OnCommit {
            self.sync()?;
            self.flush_header()?;
        }

        Ok(writes)
    }

    /// Abort a transaction
    pub fn abort(&self, txn_id: u64) -> Result<()> {
        // Remove the transaction
        {
            let mut active = self.active_txns.lock();
            active.remove(&txn_id);
        }

        // Write abort record
        let record = WalRecord::new(RecordType::Abort, txn_id, vec![]);
        self.append_record(&record)?;

        Ok(())
    }

    /// Write a checkpoint marker
    pub fn checkpoint(&self) -> Result<u64> {
        let offset = {
            let header = self.header.lock();
            header.write_offset
        };

        // Write checkpoint record
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let record = WalRecord::new(RecordType::Checkpoint, txn_id, vec![]);
        self.append_record(&record)?;

        // Update header
        {
            let mut header = self.header.lock();
            header.last_checkpoint = offset;
            header.update_checksum();
        }

        // Always sync on checkpoint
        self.sync()?;
        self.flush_header()?;

        Ok(offset)
    }

    /// Append a record to the WAL
    fn append_record(&self, record: &WalRecord) -> Result<u64> {
        let record_bytes = record.to_bytes();
        let record_len = record_bytes.len();

        // Align to 4KB boundary for next write
        let aligned_len = record_len.div_ceil(4096) * 4096;

        let offset = {
            let mut header = self.header.lock();
            let offset = header.write_offset;

            // Check if we have space
            if offset + aligned_len as u64 > self.size_limit {
                return Err(Error::Storage("WAL is full".into()));
            }

            header.write_offset = offset + aligned_len as u64;
            offset
        };

        // Write record (with padding to alignment)
        let mut buf = AlignedBuffer::new(aligned_len);
        buf.copy_from(&record_bytes);
        self.file.write_at(offset, buf.as_slice())?;

        if self.sync_mode == SyncMode::Always {
            self.file.sync()?;
        }

        Ok(offset)
    }

    /// Sync WAL to disk
    pub fn sync(&self) -> Result<()> {
        self.file.sync()
    }

    /// Flush header to disk
    fn flush_header(&self) -> Result<()> {
        let header = self.header.lock();
        let mut buf = AlignedBuffer::new(WAL_HEADER_SIZE as usize);
        buf.copy_from(&header.to_bytes());
        self.file.write_at(0, buf.as_slice())?;
        self.file.sync()
    }

    /// Replay the WAL and return committed transactions
    ///
    /// Returns a map of transaction ID -> list of write operations
    pub fn replay(&self) -> Result<HashMap<u64, Vec<WriteOp>>> {
        let mut offset = WAL_HEADER_SIZE;
        let end_offset = self.header.lock().write_offset;

        let mut transactions: HashMap<u64, Vec<WriteOp>> = HashMap::new();
        let mut committed: HashMap<u64, Vec<WriteOp>> = HashMap::new();

        while offset < end_offset {
            // Read a 4KB aligned chunk
            let mut buf = AlignedBuffer::new(4096);
            match self.file.read_at(offset, buf.as_mut_slice()) {
                Ok(_) => {}
                Err(_) => break, // End of valid data
            }

            // Try to parse a record
            let record = match WalRecord::from_bytes(buf.as_slice()) {
                Ok(r) => r,
                Err(_) => break, // Corrupted or end of data
            };

            match record.record_type {
                RecordType::BeginTxn => {
                    transactions.insert(record.txn_id, vec![]);
                }
                RecordType::Write => {
                    if let Some(writes) = transactions.get_mut(&record.txn_id)
                        && let Ok(write_op) = WriteOp::from_bytes(&record.data)
                    {
                        writes.push(write_op);
                    }
                }
                RecordType::Commit => {
                    if let Some(writes) = transactions.remove(&record.txn_id) {
                        committed.insert(record.txn_id, writes);
                    }
                }
                RecordType::Abort => {
                    transactions.remove(&record.txn_id);
                }
                RecordType::Checkpoint => {
                    // Checkpoint just marks a known-good point
                }
            }

            // Move to next aligned position
            let record_size = record.serialized_size();
            let aligned_size = record_size.div_ceil(4096) * 4096;
            offset += aligned_size as u64;
        }

        Ok(committed)
    }

    /// Get current write offset
    pub fn write_offset(&self) -> u64 {
        self.header.lock().write_offset
    }

    /// Get last committed transaction ID
    pub fn last_committed_txn(&self) -> u64 {
        self.header.lock().last_committed_txn
    }

    /// Check if WAL needs rotation (> 80% full)
    pub fn needs_rotation(&self) -> bool {
        let offset = self.header.lock().write_offset;
        offset > self.size_limit * 80 / 100
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_create_and_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Create WAL
        {
            let wal = WriteAheadLog::create(&path, 1024 * 1024, SyncMode::Always).unwrap();
            assert_eq!(wal.last_committed_txn(), 0);
        }

        // Reopen WAL
        {
            let wal = WriteAheadLog::open(&path, SyncMode::Always).unwrap();
            assert_eq!(wal.last_committed_txn(), 0);
        }
    }

    #[test]
    fn test_wal_transaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::create(&path, 1024 * 1024, SyncMode::OnCommit).unwrap();

        // Begin transaction
        let txn_id = wal.begin_txn().unwrap();
        assert_eq!(txn_id, 1);

        // Write some data
        let write_op = WriteOp {
            block_num: 42,
            object_id: [1u8; 16],
            object_offset: 0,
            data: b"Hello, WAL!".to_vec(),
        };
        wal.write(txn_id, write_op).unwrap();

        // Commit
        let writes = wal.commit(txn_id).unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].block_num, 42);

        // Verify last committed
        assert_eq!(wal.last_committed_txn(), 1);
    }

    #[test]
    fn test_wal_abort() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let wal = WriteAheadLog::create(&path, 1024 * 1024, SyncMode::OnCommit).unwrap();

        // Begin and abort transaction
        let txn_id = wal.begin_txn().unwrap();
        let write_op = WriteOp {
            block_num: 1,
            object_id: [2u8; 16],
            object_offset: 0,
            data: b"Aborted data".to_vec(),
        };
        wal.write(txn_id, write_op).unwrap();
        wal.abort(txn_id).unwrap();

        // Should not be committed
        assert_eq!(wal.last_committed_txn(), 0);
    }

    #[test]
    fn test_wal_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Create and write some transactions
        {
            let wal = WriteAheadLog::create(&path, 1024 * 1024, SyncMode::Always).unwrap();

            // Committed transaction
            let txn1 = wal.begin_txn().unwrap();
            wal.write(
                txn1,
                WriteOp {
                    block_num: 1,
                    object_id: [1u8; 16],
                    object_offset: 0,
                    data: b"data1".to_vec(),
                },
            )
            .unwrap();
            wal.commit(txn1).unwrap();

            // Aborted transaction
            let txn2 = wal.begin_txn().unwrap();
            wal.write(
                txn2,
                WriteOp {
                    block_num: 2,
                    object_id: [2u8; 16],
                    object_offset: 0,
                    data: b"data2".to_vec(),
                },
            )
            .unwrap();
            wal.abort(txn2).unwrap();

            // Another committed transaction
            let txn3 = wal.begin_txn().unwrap();
            wal.write(
                txn3,
                WriteOp {
                    block_num: 3,
                    object_id: [3u8; 16],
                    object_offset: 0,
                    data: b"data3".to_vec(),
                },
            )
            .unwrap();
            wal.commit(txn3).unwrap();
        }

        // Replay
        {
            let wal = WriteAheadLog::open(&path, SyncMode::Always).unwrap();
            let committed = wal.replay().unwrap();

            // Should have 2 committed transactions (txn1 and txn3)
            assert_eq!(committed.len(), 2);
            assert!(committed.contains_key(&1));
            assert!(committed.contains_key(&3));
            assert!(!committed.contains_key(&2)); // Aborted
        }
    }

    #[test]
    fn test_record_roundtrip() {
        let record = WalRecord::new(RecordType::Write, 42, b"test data".to_vec());
        let bytes = record.to_bytes();
        let parsed = WalRecord::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.record_type, RecordType::Write);
        assert_eq!(parsed.txn_id, 42);
        assert_eq!(parsed.data, b"test data");
    }

    #[test]
    fn test_write_op_roundtrip() {
        let write_op = WriteOp {
            block_num: 123,
            object_id: [0xAB; 16],
            object_offset: 4096,
            data: b"block data here".to_vec(),
        };

        let bytes = write_op.to_bytes();
        let parsed = WriteOp::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.block_num, 123);
        assert_eq!(parsed.object_id, [0xAB; 16]);
        assert_eq!(parsed.object_offset, 4096);
        assert_eq!(parsed.data, b"block data here");
    }
}
