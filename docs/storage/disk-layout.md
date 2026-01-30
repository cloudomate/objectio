# Disk Layout

ObjectIO uses raw block devices with a custom disk format.

## Overview

```
Raw Block Device Layout
══════════════════════════════════════════════════════════════════

Offset 0                                                     End
├──────────┬──────────────┬─────────────┬────────────────────────┤
│Superblock│   WAL Region │Block Bitmap │     Data Region        │
│  (4 KB)  │   (1-4 GB)   │  (variable) │   (remaining space)    │
└──────────┴──────────────┴─────────────┴────────────────────────┘
```

## Superblock

The superblock is the first 4 KB of the disk.

```rust
#[repr(C, packed)]
pub struct Superblock {
    /// Magic number: "OBJIO001"
    pub magic: [u8; 8],
    /// On-disk format version
    pub version: u32,
    /// Unique disk identifier (UUID)
    pub disk_id: Uuid,
    /// Total disk size in bytes
    pub total_size: u64,
    /// Block size for data (default: 4 MB)
    pub data_block_size: u32,
    /// Block size for metadata (default: 4 KB)
    pub metadata_block_size: u32,

    // Region offsets
    pub wal_offset: u64,
    pub wal_size: u64,
    pub bitmap_offset: u64,
    pub bitmap_size: u64,
    pub data_offset: u64,

    // Timestamps
    pub created_at: u64,
    pub last_mount: u64,

    /// CRC32C of superblock
    pub checksum: u32,

    /// Reserved for future use
    pub _reserved: [u8; 3952],
}
```

### Magic Number

The magic number identifies a valid ObjectIO disk:

```
Bytes 0-7: "OBJIO001" (0x4F424A494F303031)
```

### Version

| Version | Description |
|---------|-------------|
| 1 | Initial format |

## Write-Ahead Log (WAL)

The WAL region provides durability for writes.

### WAL Entry Format

```
+--------+------+--------+------+--------+
| Magic  | LSN  | Length | Data | CRC32C |
| 4B     | 8B   | 4B     | var  | 4B     |
+--------+------+--------+------+--------+

Magic: 0x57414C4F ("WALO")
LSN: Monotonically increasing sequence number
Data: Serialized WalEntry
CRC32C: Checksum of entire record
```

### WAL Entry Types

```rust
pub enum WalEntry {
    /// Begin write transaction
    BeginTxn { txn_id: u64, object_id: ObjectId, timestamp: u64 },

    /// Write a block
    WriteBlock { txn_id: u64, block_id: BlockId, location: BlockLocation, crc: u32 },

    /// Commit transaction (durable)
    Commit { txn_id: u64, timestamp: u64 },

    /// Abort transaction (rollback)
    Abort { txn_id: u64, reason: String },

    /// Delete block
    Delete { txn_id: u64, block_id: BlockId },

    /// Checkpoint (WAL can be truncated before this point)
    Checkpoint { sequence: u64, timestamp: u64 },
}
```

### Recovery

1. Read WAL from last checkpoint
2. Replay committed transactions
3. Rollback uncommitted transactions
4. Truncate WAL at checkpoint

## Block Bitmap

The block bitmap tracks free/used blocks.

### Format

- 1 bit per data block
- 0 = free, 1 = allocated
- Aligned to 4 KB

### Size Calculation

```
bitmap_size = ceil(data_blocks / 8)
data_blocks = (total_size - wal_offset - wal_size - bitmap_size) / block_size
```

## Data Block Format

Each data block (default 4 MB) has a header and footer.

```
Block Layout (4 MB default)
══════════════════════════════════════════════════════════════════

┌────────────────────────────────────────────────────────────────┐
│                      Block Header (64 bytes)                   │
├────────────────────────────────────────────────────────────────┤
│ magic[4]     │ "BLOK"                                          │
│ block_type   │ Data(0), Parity(1), Index(2)                    │
│ flags        │ Compressed, Encrypted, etc.                     │
│ block_id     │ UUID (16 bytes)                                 │
│ object_id    │ UUID (16 bytes)                                 │
│ stripe_id    │ u64 - which stripe this belongs to              │
│ stripe_pos   │ u8 - position within stripe (0..k+m-1)          │
│ ec_k, ec_m   │ Erasure coding parameters                       │
│ data_length  │ u32 - actual data size (may be < block size)    │
│ sequence     │ u64 - for ordering                              │
├────────────────────────────────────────────────────────────────┤
│                      Data Payload                              │
│              (block_size - 64 - 32 bytes)                      │
├────────────────────────────────────────────────────────────────┤
│                      Block Footer (32 bytes)                   │
├────────────────────────────────────────────────────────────────┤
│ crc32c       │ CRC32C of header + data                         │
│ xxhash64     │ xxHash64 for fast comparison                    │
│ sha256[20]   │ First 20 bytes of SHA256 (content addressing)   │
└────────────────────────────────────────────────────────────────┘
```

### Block Types

| Type | Value | Description |
|------|-------|-------------|
| Data | 0 | Data shard from erasure coding |
| Parity | 1 | Parity shard from erasure coding |
| Index | 2 | Index/metadata block |

### Block Flags

| Flag | Bit | Description |
|------|-----|-------------|
| Compressed | 0x01 | Data is compressed |
| Encrypted | 0x02 | Data is encrypted |
| Verified | 0x04 | Recently verified by scrubber |

## Example Disk Layout

For a 1 TB disk with 4 MB blocks:

```
Superblock:     0x0000_0000 - 0x0000_0FFF    (4 KB)
WAL:            0x0000_1000 - 0x4000_0FFF    (1 GB)
Bitmap:         0x4000_1000 - 0x4003_F000    (254 KB for 262,144 blocks)
Data:           0x4004_0000 - 0xFFFF_FFFF    (~959 GB = 245,759 blocks)
```

## Raw Disk Access

### Linux (O_DIRECT)

```rust
let file = OpenOptions::new()
    .read(true)
    .write(true)
    .custom_flags(libc::O_DIRECT | libc::O_SYNC)
    .open(path)?;
```

### macOS (F_NOCACHE)

```rust
let file = OpenOptions::new()
    .read(true)
    .write(true)
    .open(path)?;
unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
```

### Alignment Requirements

- All I/O must be 4 KB aligned (offset and size)
- Use aligned buffers for read/write operations

```rust
pub struct AlignedBuffer {
    data: Vec<u8>,
    alignment: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize, alignment: usize) -> Self {
        let data = unsafe {
            let ptr = std::alloc::alloc(
                std::alloc::Layout::from_size_align(size, alignment).unwrap()
            );
            Vec::from_raw_parts(ptr, size, size)
        };
        Self { data, alignment }
    }
}
```
