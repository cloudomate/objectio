# Configuration Reference

ObjectIO components are configured via TOML files or command-line arguments.

## Configuration Files

| Component | Default Path |
|-----------|--------------|
| Gateway | `/etc/objectio/gateway.toml` |
| Metadata | `/etc/objectio/meta.toml` |
| OSD | `/etc/objectio/osd.toml` |

## Gateway Configuration

```toml
# /etc/objectio/gateway.toml

[server]
# Listen address for S3 API
listen = "0.0.0.0:9000"

# TLS configuration (optional)
tls_cert = "/etc/objectio/tls/cert.pem"
tls_key = "/etc/objectio/tls/key.pem"

[metadata]
# Metadata service endpoints (comma-separated)
endpoints = ["http://meta1:9100", "http://meta2:9100", "http://meta3:9100"]

# Connection timeout
timeout_ms = 5000

[auth]
# Enable authentication
enabled = true

# AWS region for SigV4
region = "us-east-1"

# Admin user name (creates on startup if not exists)
admin_user = "admin"

[erasure_coding]
# Default EC profile
data_shards = 4
parity_shards = 2

[logging]
# Log level: trace, debug, info, warn, error
level = "info"

# Log format: json, pretty
format = "pretty"
```

### CLI Arguments

```bash
objectio-gateway \
  --listen 0.0.0.0:9000 \
  --meta-endpoint http://meta1:9100 \
  --ec-k 4 \
  --ec-m 2 \
  --region us-east-1 \
  --log-level info
```

| Arg | Default | Description |
|-----|---------|-------------|
| `--listen` | `0.0.0.0:9000` | Listen address |
| `--meta-endpoint` | `http://localhost:9001` | Metadata service |
| `--ec-k` | `4` | Data shards |
| `--ec-m` | `2` | Parity shards |
| `--no-auth` | - | Disable authentication |
| `--region` | `us-east-1` | AWS region |
| `--log-level` | `info` | Log level |

---

## Metadata Configuration

```toml
# /etc/objectio/meta.toml

[server]
# Listen address for gRPC
listen = "0.0.0.0:9100"

# Raft peer communication port
raft_listen = "0.0.0.0:9101"

[raft]
# Node ID (must be unique in cluster)
node_id = 1

# Cluster peers (comma-separated)
peers = ["meta1:9101", "meta2:9101", "meta3:9101"]

# Data directory for Raft logs and snapshots
data_dir = "/var/lib/objectio/meta"

# Election timeout
election_timeout_ms = 1000

# Heartbeat interval
heartbeat_interval_ms = 100

[storage]
# Metadata database path
db_path = "/var/lib/objectio/meta/metadata.redb"

[logging]
level = "info"
```

### CLI Arguments

```bash
objectio-meta \
  --node-id 1 \
  --listen 0.0.0.0:9100 \
  --peers meta1:9100,meta2:9100,meta3:9100 \
  --log-level info
```

| Arg | Default | Description |
|-----|---------|-------------|
| `--node-id` | - | Raft node ID (required) |
| `--listen` | `0.0.0.0:9001` | gRPC listen address |
| `--peers` | - | Raft peers |
| `--ec-k` | `4` | Erasure coding data shards |
| `--ec-m` | `2` | Erasure coding parity shards |
| `--replication` | - | Replication count (overrides EC settings) |
| `--log-level` | `info` | Log level |

### Storage Mode Examples

```bash
# Erasure coding 4+2 (default, 1.5x overhead, tolerates 2 failures)
objectio-meta --node-id 1 --listen 0.0.0.0:9100

# Erasure coding 8+4 (1.5x overhead, tolerates 4 failures)
objectio-meta --node-id 1 --listen 0.0.0.0:9100 --ec-k 8 --ec-m 4

# Single-disk mode (no redundancy, for dev/test)
objectio-meta --node-id 1 --listen 0.0.0.0:9100 --replication 1

# 3-way replication (3x overhead, tolerates 2 failures)
objectio-meta --node-id 1 --listen 0.0.0.0:9100 --replication 3
```

---

## OSD Configuration

```toml
# /etc/objectio/osd.toml

[osd]
# Listen address for gRPC
listen = "0.0.0.0:9200"

# Advertise address (how other services reach this OSD)
advertise_addr = "osd1:9200"

# Metadata service endpoint
meta_endpoint = "http://meta1:9100"

[storage]
# Disk paths (raw devices or files)
disks = ["/dev/vdb", "/dev/vdc"]

# Block size (default: 4 MB)
block_size = 4194304

[storage.cache]
# Block cache settings
[storage.cache.block_cache]
enabled = true
size_mb = 256
policy = "write_through"  # write_through | write_back | write_around

# Metadata cache settings
[storage.cache.metadata_cache]
enabled = true
size_mb = 64

[storage.wal]
# Sync on every write
sync_on_write = true

# Max WAL size before snapshot
max_size_mb = 64

[logging]
level = "info"
```

### CLI Arguments

```bash
objectio-osd \
  --listen 0.0.0.0:9200 \
  --advertise-addr osd1:9200 \
  --disks /dev/vdb \
  --meta-endpoint http://meta1:9100 \
  --log-level info
```

| Arg | Default | Description |
|-----|---------|-------------|
| `--listen` | - | gRPC listen address |
| `--advertise-addr` | - | Address to register with metadata |
| `--disks` | - | Disk paths (can be repeated) |
| `--meta-endpoint` | - | Metadata service endpoint |
| `--log-level` | `info` | Log level |

---

## Storage Classes

Storage classes are defined in the metadata service configuration.

```toml
# /etc/objectio/storage-classes.toml

[[storage_class]]
name = "standard"
type = "erasure_coding"
data_shards = 4
parity_shards = 2
placement = "rack"

[[storage_class]]
name = "high-durability"
type = "erasure_coding"
data_shards = 8
parity_shards = 4
placement = "rack"

[[storage_class]]
name = "single-node"
type = "erasure_coding"
data_shards = 4
parity_shards = 2
placement = "disk"

[[storage_class]]
name = "geo-replicated"
type = "replication"
replicas = 3
placement = "datacenter"
sync_mode = "async"
```

---

## Environment Variables

All configuration options can be set via environment variables:

| Variable | Component | Description |
|----------|-----------|-------------|
| `OBJECTIO_LISTEN` | All | Listen address |
| `OBJECTIO_META_ENDPOINTS` | Gateway | Metadata endpoints |
| `OBJECTIO_NODE_ID` | Meta | Raft node ID |
| `OBJECTIO_RAFT_PEERS` | Meta | Raft peers |
| `OBJECTIO_DATA_DIR` | Meta | Data directory |
| `OBJECTIO_DISKS` | OSD | Disk paths |
| `OBJECTIO_LOG_LEVEL` | All | Log level |

Example:

```bash
export OBJECTIO_META_ENDPOINTS="meta1:9100,meta2:9100,meta3:9100"
export OBJECTIO_LOG_LEVEL="debug"
objectio-gateway
```

---

## Systemd Units

### Gateway

```ini
# /etc/systemd/system/objectio-gateway.service
[Unit]
Description=ObjectIO S3 Gateway
After=network.target

[Service]
Type=simple
User=objectio
ExecStart=/usr/local/bin/objectio-gateway --config /etc/objectio/gateway.toml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Metadata

```ini
# /etc/systemd/system/objectio-meta.service
[Unit]
Description=ObjectIO Metadata Service
After=network.target

[Service]
Type=simple
User=objectio
ExecStart=/usr/local/bin/objectio-meta --config /etc/objectio/meta.toml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### OSD

```ini
# /etc/systemd/system/objectio-osd.service
[Unit]
Description=ObjectIO Storage Daemon
After=network.target

[Service]
Type=simple
User=root  # Required for raw disk access
ExecStart=/usr/local/bin/objectio-osd --config /etc/objectio/osd.toml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Enable and Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable objectio-gateway objectio-meta objectio-osd
sudo systemctl start objectio-gateway objectio-meta objectio-osd
```
