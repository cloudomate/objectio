# Data Protection: Erasure Coding & Replication

ObjectIO supports multiple data protection modes for different deployment scenarios.

## Storage Modes

| Mode | Use Case | Overhead | Min Disks |
|------|----------|----------|-----------|
| **Replication (count=1)** | Single-node dev/test | 1x | 1 |
| **Replication (count=3)** | Small clusters, low latency | 3x | 3 |
| **EC 4+2 (MDS)** | Production, balanced | 1.5x | 6 |
| **EC 8+4 (MDS)** | Large clusters, high efficiency | 1.5x | 12 |
| **LRC 12+2+2** | Very large clusters | 1.33x | 16 |

## Replication Mode

Simple replication stores full copies of data on multiple disks/nodes.

### Single-Disk Mode (count=1)

For development and testing with no redundancy:

```bash
# Start metadata service in single-disk mode
objectio-meta --node-id 1 --listen 0.0.0.0:9100 --replication 1

# Start OSD with one disk
objectio-osd --listen 0.0.0.0:9200 --disks /data/disk0.img --meta-endpoint http://localhost:9100
```

**Characteristics:**
- No data redundancy (disk failure = data loss)
- Minimum overhead (1x storage)
- Ideal for local development and testing

### 3-Way Replication (count=3)

For small clusters prioritizing simplicity and read performance:

```bash
# Start metadata service with 3-way replication
objectio-meta --node-id 1 --listen 0.0.0.0:9100 --replication 3
```

**Characteristics:**
- Tolerates 2 disk/node failures
- 3x storage overhead
- Fast reads (any replica can serve)
- Simple recovery (just copy data)

## Erasure Coding (Default)

Erasure coding splits data into `k` data shards and generates `m` parity shards. Any `k` shards can reconstruct the original data.

```
Original Data:  ┌─────────────────────────────────────┐
                │            4 MB chunk               │
                └─────────────────────────────────────┘
                              │
                        RS Encode (4+2)
                              │
                              ▼
Data Shards:    ┌────┐ ┌────┐ ┌────┐ ┌────┐
                │ D0 │ │ D1 │ │ D2 │ │ D3 │  (1 MB each)
                └────┘ └────┘ └────┘ └────┘

Parity Shards:  ┌────┐ ┌────┐
                │ P0 │ │ P1 │  (computed from D0-D3)
                └────┘ └────┘
```

## Striping for Large Objects

ObjectIO uses striping to handle objects larger than a single block. Each stripe is independently erasure-coded and written to OSDs.

### Block and Stripe Size Limits

| Parameter | Value | Description |
|-----------|-------|-------------|
| **Block size** | 4 MB | Maximum raw block on disk |
| **Max shard size** | ~4 MB | Block size minus header/footer overhead |
| **Max stripe data** | `MAX_SHARD_SIZE × k` | e.g., ~16 MB for 4+2 EC |

### How Striping Works

For a 50 MB object with 4+2 EC (max stripe = ~16 MB):

```
┌───────────────────────────────────────────────────────────────────────┐
│                         50 MB Object                                   │
└───────────────────────────────────────────────────────────────────────┘
                              │
                    Split into stripes
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │  Stripe 0    │     │  Stripe 1    │     │  Stripe 2    │
  │  (~16 MB)    │     │  (~16 MB)    │     │  (~18 MB)    │
  └──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
   EC Encode             EC Encode             EC Encode
   (4+2)                 (4+2)                 (4+2)
        │                     │                     │
        ▼                     ▼                     ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │ 6 shards     │     │ 6 shards     │     │ 6 shards     │
  │ (~4 MB each) │     │ (~4 MB each) │     │ (~4.5 MB ea) │
  └──────────────┘     └──────────────┘     └──────────────┘
```

### Stripe Metadata

Each object stores metadata about all its stripes:

```rust
pub struct StripeMeta {
    /// Stripe index (0, 1, 2, ...)
    pub stripe_id: u64,
    /// EC parameters for this stripe
    pub ec_k: u8,
    pub ec_m: u8,
    /// Location of each shard
    pub shards: Vec<ShardLocation>,
    /// Original data size before encoding (for padding removal)
    pub data_size: u64,
    /// EC type (MDS, LRC, Replication)
    pub ec_type: ErasureType,
}
```

### Read Path with Striping

```
1. Fetch object metadata (includes all stripe info)
2. For each stripe in parallel:
   a. Read k available shards from OSDs
   b. Decode to recover stripe data
   c. Trim to data_size (remove padding)
3. Concatenate all stripes
4. Return complete object
```

### Write Path with Striping

```
1. Calculate number of stripes: ceil(object_size / max_stripe_data)
2. For each stripe:
   a. Extract stripe data from object
   b. EC encode into k+m shards
   c. Write shards to OSDs in parallel (wait for quorum)
3. Store object metadata with all stripe locations
```

### Quorum Writes

For durability, writes wait for a quorum of shards before returning success:

| EC Profile | Total Shards | Write Quorum | Rationale |
|------------|--------------|--------------|-----------|
| 4+2 | 6 | 4 (k) | Can reconstruct from any k shards |
| 8+4 | 12 | 8 (k) | Can reconstruct from any k shards |
| Replication 3 | 3 | 1 | At least one copy must succeed |

### Stripe-Level Parallelism

All stripes for an object are processed in parallel:
- **Write**: All stripes encoded and written concurrently
- **Read**: All stripes fetched and decoded concurrently
- **Per-stripe**: All k+m shard operations run in parallel

## Backend Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                     ErasureCodec (High-Level API)                  │
│  • encode(data) → shards                                           │
│  • decode(shards) → data                                           │
│  • supports_lrc() → bool                                           │
└────────────────────────────┬───────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
       ┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
       │  rust_simd  │ │   ISA-L    │ │   Future   │
       │  (default)  │ │   (x86)    │ │  backends  │
       │             │ │            │ │            │
       │ • Portable  │ │ • AVX/AVX2 │ │ • CUDA     │
       │ • ARM NEON  │ │ • AVX-512  │ │ • etc.     │
       │ • Pure Rust │ │ • FFI      │ │            │
       └─────────────┘ └────────────┘ └────────────┘
```

## Backends

### rust-simd (Default)

Pure Rust implementation using the `reed-solomon-simd` crate.

**Features:**
- Portable across all platforms
- SIMD optimizations for x86 (SSE/AVX) and ARM (NEON)
- No external dependencies

**Usage:**
```rust
use objectio_erasure::ReedSolomonBackend;

let backend = ReedSolomonBackend::new(4, 2)?;
let shards = backend.encode(&data)?;
```

### Intel ISA-L (Optional)

Hardware-accelerated implementation using Intel ISA-L.

**Features:**
- 2-5x faster than pure Rust on x86
- Uses AVX, AVX2, or AVX-512
- Bundled with `erasure-isa-l` crate

**Requirements:**
- x86/x86_64 processor
- Build tools: nasm, autoconf, automake, libtool

**Usage:**
```bash
# Enable ISA-L feature
cargo build --features isal
```

```rust
use objectio_erasure::IsalBackend;

let backend = IsalBackend::new(4, 2)?;
let shards = backend.encode(&data)?;
```

## API

### Core Trait

```rust
pub trait ErasureBackend: Send + Sync {
    /// Number of data shards
    fn data_shards(&self) -> u8;

    /// Number of parity shards
    fn parity_shards(&self) -> u8;

    /// Total shards (k + m)
    fn total_shards(&self) -> u8;

    /// Encode data into shards
    fn encode(&self, data: &[&[u8]]) -> Result<Vec<Vec<u8>>, ErasureError>;

    /// Decode shards back to data
    fn decode(&self, shards: &[Option<&[u8]>]) -> Result<Vec<Vec<u8>>, ErasureError>;

    /// Get backend capabilities
    fn capabilities(&self) -> BackendCapabilities;
}
```

### LRC Extension

```rust
pub trait LrcBackend: ErasureBackend {
    /// Get LRC configuration
    fn lrc_config(&self) -> &LrcConfig;

    /// Get local group for a shard
    fn local_group(&self, shard_idx: usize) -> Option<usize>;

    /// Check if local recovery is possible
    fn can_recover_locally(&self, available: &[bool], missing: usize) -> bool;

    /// Try local recovery (faster for single failures)
    fn try_local_recovery(
        &self,
        shards: &mut [Option<Vec<u8>>],
        missing: usize,
    ) -> Result<bool, ErasureError>;
}
```

## Encoding Process

### 1. Split Data

```rust
// Split 4 MB object into 4 x 1 MB data shards
let shard_size = data.len() / k;
let data_shards: Vec<&[u8]> = (0..k)
    .map(|i| &data[i * shard_size..(i + 1) * shard_size])
    .collect();
```

### 2. Generate Parity

```rust
// Generate 2 parity shards using Reed-Solomon
let all_shards = backend.encode(&data_shards)?;
// all_shards contains: [D0, D1, D2, D3, P0, P1]
```

### 3. Distribute Shards

```rust
// Place shards on different failure domains
for (i, shard) in all_shards.iter().enumerate() {
    let location = placement.select_location(object_id, i)?;
    osd_client.write_shard(location, shard).await?;
}
```

## Decoding Process

### 1. Read Available Shards

```rust
// Read shards from OSDs
let mut shards: Vec<Option<&[u8]>> = vec![None; k + m];
for (i, location) in shard_locations.iter().enumerate() {
    match osd_client.read_shard(location).await {
        Ok(data) => shards[i] = Some(data),
        Err(_) => shards[i] = None, // Failed disk/node
    }
}
```

### 2. Check Availability

```rust
let available = shards.iter().filter(|s| s.is_some()).count();
if available < k {
    return Err(Error::InsufficientShards);
}
```

### 3. Reconstruct Data

```rust
// Decode using any k available shards
let recovered = backend.decode(&shards)?;
let original_data: Vec<u8> = recovered[..k].concat();
```

## LRC (Locally Repairable Codes)

### Configuration

```rust
pub struct LrcConfig {
    pub data_shards: u8,      // e.g., 12
    pub local_parity: u8,     // e.g., 2
    pub global_parity: u8,    // e.g., 2
    pub local_group_size: u8, // e.g., 6
}
```

### Local Recovery

```rust
// For single failure within a local group
if backend.can_recover_locally(&availability, missing_shard) {
    // XOR-based recovery (reads 6 shards instead of 12)
    backend.try_local_recovery(&mut shards, missing_shard)?;
} else {
    // Fall back to global Reed-Solomon (reads 12 shards)
    backend.decode(&shards)?;
}
```

## Performance

### Benchmark Results (4+2, 4 MB blocks)

| Backend | Encode | Decode | Platform |
|---------|--------|--------|----------|
| rust-simd | 450 MB/s | 420 MB/s | x86_64 AVX2 |
| ISA-L | 1.2 GB/s | 1.1 GB/s | x86_64 AVX2 |
| ISA-L | 2.4 GB/s | 2.2 GB/s | x86_64 AVX-512 |
| rust-simd | 280 MB/s | 260 MB/s | ARM64 NEON |

### Optimization Tips

1. **Use ISA-L on x86**: Enable the `isal` feature for 2-5x speedup
2. **Batch operations**: Encode multiple objects in parallel
3. **Align buffers**: Use 64-byte aligned buffers for SIMD
4. **Pre-allocate**: Reuse shard buffers to avoid allocation

## Error Handling

```rust
pub enum ErasureError {
    /// Invalid k/m configuration
    InvalidConfig(String),

    /// Not enough shards available for decode
    InsufficientShards { available: usize, required: usize },

    /// Encoding failed
    EncodingFailed(String),

    /// Decoding failed
    DecodingFailed(String),

    /// Shard size mismatch
    ShardSizeMismatch { expected: usize, got: usize },
}
```

## Configuration

```toml
[erasure_coding]
# Default profile
data_shards = 4
parity_shards = 2

# Backend selection (auto, rust_simd, isal)
backend = "auto"

# LRC settings (optional)
[erasure_coding.lrc]
enabled = false
local_parity = 2
global_parity = 2
local_group_size = 6
```
