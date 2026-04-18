# ObjectIO — Feature Reference

ObjectIO is a unified software-defined storage (SDS) platform delivering four storage paradigms on a single erasure-coded backend:

| Paradigm | Protocol | Use Case |
|----------|----------|----------|
| **Object Storage** | S3 API | Unstructured data, backups, media, ML datasets |
| **Data Lakehouse** | Iceberg REST Catalog + Delta Sharing | Analytics warehouses, Spark/Trino queries, cross-org data sharing |
| **File Storage** | NFS / SMB / FUSE *(planned)* | POSIX file access, NAS replacement, shared file systems |
| **Block Storage** | gRPC + NBD | Databases, VMs, thin-provisioned volumes with snapshots |

All four share the same underlying erasure-coded storage layer, IAM, multi-tenancy, and web console.

---

# 1. Object Storage (S3 API)

### Core S3 API
- **Bucket operations**: CreateBucket, DeleteBucket, HeadBucket, ListBuckets
- **Object operations**: PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects (batch)
- **Multipart upload**: InitiateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads
- **Range requests**: HTTP Range header support (`bytes=0-99`, suffix ranges, open-ended ranges)
- **Copy object**: Server-side copy via `x-amz-copy-source` header (metadata-only fast path when on same OSD)
- **User metadata**: `x-amz-meta-*` headers stored and returned
- **S3 error responses**: Standard XML error format with S3 error codes

### Bucket Versioning
- **PUT/GET /{bucket}?versioning** — Enable, suspend, or query versioning state
- **Version-aware PUT**: Generates UUID version ID when versioning enabled, returns `x-amz-version-id` header
- **Version-aware DELETE**: Creates delete markers (new version with `is_delete_marker=true`) when no `?versionId` specified
- **Version-specific DELETE**: Permanently deletes a specific version via `?versionId=`
- **ListObjectVersions**: `GET /{bucket}?versions` returns all versions including delete markers
- **Dual-key OSD storage**: Current version at `m:{bucket}\0{key}`, version history at `v:{bucket}\0{key}\0{version_id}`

### Object Locking (WORM)
- **PUT/GET /{bucket}?object-lock** — Configure object lock with default retention
- **Retention modes**: GOVERNANCE (bypassable with header) and COMPLIANCE (immutable until expiry)
- **PUT/GET /{bucket}/{key}?retention** — Per-object retention (mode + retain-until-date)
- **PUT/GET /{bucket}/{key}?legal-hold** — Per-object legal hold (ON/OFF)
- **Delete enforcement**: Objects under retention or legal hold cannot be deleted (403 Forbidden)
- **Governance bypass**: `x-amz-bypass-governance-retention: true` header allows deletion
- **Lock at creation**: `x-amz-bucket-object-lock-enabled: true` on CreateBucket

### Lifecycle Management
- **PUT/GET/DELETE /{bucket}?lifecycle** — Configure lifecycle rules per bucket
- **Expiration rules**: Delete objects after N days
- **Noncurrent version expiration**: Delete non-current versions after N days
- **Expired delete marker cleanup**: Remove orphaned delete markers
- **Incomplete multipart upload abort**: Clean up stale uploads after N days
- **Prefix filtering**: Apply rules to objects matching a key prefix
- **Background worker**: Gateway runs periodic lifecycle scan (default: every 24 hours)

### Bucket Policies
- **PUT/GET/DELETE /{bucket}?policy** — IAM-style JSON bucket policies
- **Policy evaluation**: Allow/Deny/ImplicitDeny decisions
- **ARN-based resources**: `arn:obio:s3:::{bucket}` and `arn:obio:s3:::{bucket}/{key}`
- **Condition keys**: `s3:x-amz-server-side-encryption`, `s3:x-amz-server-side-encryption-aws-kms-key-id`, `aws:SecureTransport` — populated from PUT/GET/DELETE request headers, evaluated at request time

### Server-Side Encryption (SSE)

All three AWS S3 encryption modes are supported (SSE-S3, SSE-KMS, SSE-C). Encryption always happens **before** the erasure-coding stage so OSD shards only ever see ciphertext. Objects written without SSE remain plaintext; enabling SSE on a bucket does not retroactively encrypt existing objects. Ciphertext correctness is verified end-to-end in tests — grep for any plaintext marker on OSD shards returns zero matches when SSE is enabled.

- **PUT/GET/DELETE /{bucket}?encryption** — Bucket default encryption configuration, AWS-compatible XML (`ServerSideEncryptionConfiguration`)
- **Per-request precedence**: explicit `x-amz-server-side-encryption*` request header > bucket default > plaintext
- **ETag semantics**: MD5 of plaintext for SSE-S3 / SSE-KMS (AWS-compatible). SSE-C uses the AWS multipart ETag format for multipart uploads
- **Response headers**: `x-amz-server-side-encryption`, `x-amz-server-side-encryption-aws-kms-key-id`, `x-amz-server-side-encryption-customer-algorithm`, `x-amz-server-side-encryption-customer-key-md5`

#### SSE-S3 (`AES256`)
- AES-256-CTR with a fresh per-object Data Encryption Key (DEK)
- DEK wrapped with the gateway's service master key via AES-256-GCM and stored in `ObjectMeta.encrypted_dek`
- Service master key loaded from `OBJECTIO_MASTER_KEY` env var (base64 32 bytes); helm chart auto-generates one on first install and preserves it across `helm upgrade`
- Range GETs and multipart uploads fully supported — each part/stripe uses its own IV so stripes decrypt independently

#### SSE-KMS (`aws:kms`) *(Enterprise)*
- Per-object DEK wrapped by a named KMS key (Key Encryption Key, KEK)
- **Local KMS provider**: self-contained backend — KEK material is itself wrapped by the service master key before it reaches meta, so meta stores only encrypted blobs
- **Envelope encryption**: client specifies KMS key via `x-amz-server-side-encryption-aws-kms-key-id` (full ARN or short id)
- **Encryption context**: optional `x-amz-server-side-encryption-context` (base64 JSON) bound to the DEK wrap as AEAD associated data; decrypt fails if context differs
- **Multipart support**: MPU picks the key at CreateMultipartUpload time; per-part stripe IVs

#### SSE-C (customer-provided keys)
- Three headers: `x-amz-server-side-encryption-customer-algorithm: AES256`, `-customer-key` (base64 32 bytes), `-customer-key-md5` (base64 MD5 of the key)
- Key material is **never persisted** — object metadata stores only the algorithm marker + IV
- MD5 binding verified on every PUT/GET/HEAD; wrong MD5 → `InvalidArgument 400`
- **Hard-blocked on warehouse buckets** (`iceberg-*` and the configured warehouse bucket): query engines can't provide the customer key on every read, so writes are rejected at PUT rather than silently producing unreadable objects
- **Multipart upload**: supported end-to-end. `CreateMultipartUpload` stores the customer-key MD5 in the MPU state; every `UploadPart` must resupply the same key, validated MD5-against-MD5 before encryption; `CompleteMultipartUpload` produces a standard AWS multipart ETag. The key itself never touches meta.

#### KMS Administration (`/_admin/kms/*`)
AIStor-compatible KMS action model. Resource ARNs use `arn:obio:kms:::{key_id}`; the wildcard `arn:obio:kms:::*` matches all keys.

| Endpoint | Action | Description |
|---|---|---|
| `GET /_admin/kms/status` | `kms:Status` | Backend state (enabled, local, master-key configured) |
| `GET /_admin/kms/version` | `kms:Version` | Service version + protocol |
| `GET /_admin/kms/api` | `kms:API` | Supported endpoints |
| `POST /_admin/kms/keys` | `kms:CreateKey` | Create a new KMS key |
| `GET /_admin/kms/keys` | `kms:ListKeys` | List KMS keys |
| `GET /_admin/kms/keys/{key_id}` | `kms:KeyStatus` | Get a specific key |
| `DELETE /_admin/kms/keys/{key_id}` | `kms:DeleteKey` | Delete + drop cached KEK |

- **Policy gating**: every endpoint evaluates the caller's attached named IAM policies against `(action, resource)`. The built-in admin user and console sessions bypass policy evaluation.
- **Dual auth on admin endpoints**: SigV4 (for API clients) *or* console session cookie (for web UI). Cookie requests pass through a pre-router layer that validates SigV4 only when an `Authorization` header is present.

#### KMS Backend Selection
The gateway's KMS backend picks how SSE-KMS wrap/unwrap is performed.

**Two configuration sources, meta wins:**
1. **Meta `kms/config`** (console-writable) — the persisted config. Hot-swappable at runtime via `PUT /_admin/kms/config` with no gateway restart.
2. **`--kms-backend` CLI flag + env vars** — fallback used at first boot when meta has no entry. Useful for IaC-pinned deployments.

**Backends:**
- **`local`** — self-contained. Each KMS key's master material is generated on the gateway, wrapped by the service master key, and persisted via meta. Admin endpoints under `/_admin/kms/keys` manage the key lifecycle. Most appropriate for on-prem deployments that don't have an external KMS.
- **`vault`** — HashiCorp Vault's Transit secrets engine. Needs `addr` + `token` + `transit_path` (defaults to `transit`). The gateway calls `transit/datakey/plaintext/{key}` to generate per-object DEKs and `transit/decrypt/{key}` on GET. Per-object encryption context is canonicalized and base64-passed as Vault's context so decrypt fails on context mismatch (AEAD binding at the Vault layer). On external backends the `/_admin/kms/keys` endpoints return `NotImplemented` — manage keys directly in Vault.
- **`disabled`** — SSE-KMS off; SSE-S3 still works if a service master key is set.

**Runtime config API** (all under `/_admin/kms/*`; gated by `kms:Status`/`CreateKey`/`DeleteKey`/`KeyStatus`):
- `GET /_admin/kms/config` — current persisted backend (Vault token redacted; only `token_set: true` exposed).
- `PUT /_admin/kms/config` — persist + hot-swap. Body is `{backend, addr?, transit_path?, token?}`. An empty `token` on update keeps the previously-stored one so admins can edit addr/path without re-entering secrets.
- `DELETE /_admin/kms/config` — remove the persisted entry; next boot falls back to CLI/env.
- `POST /_admin/kms/test` — run a throwaway `generate_data_key` + `decrypt` round-trip against the active backend. Returns `{ok: bool, error: string|null}` so the console can render a green/red indicator on the Save form.

**Console**: top-level **Encryption** page (admin-only) shows master-key status (read-only), backend selector (Disabled / Local / Vault), Vault addr + transit path + token-paste fields (token shown as `••••••••` when already stored), Save & Apply button, and a Test-connection form.

**Safety note**: switching backends does **not** retroactively re-encrypt existing objects. Objects wrapped by the old backend stay readable only while that backend is still reachable; use `CopyObject` (phase 2f) to migrate them.

#### CopyObject with encryption transitions
- **Fast path (no data movement)**: when source and destination share the same SSE algorithm (and same KMS key id for SSE-KMS), CopyObject does a metadata-only copy — shards are shared between source and dest until either is deleted.
- **Slow path (decrypt → re-encrypt)**: when SSE settings differ (plaintext → SSE-S3, SSE-S3 → SSE-KMS, SSE-KMS → plaintext, SSE-KMS with different key, or explicit override via request headers), the gateway reads the source through its normal decrypt path, then re-PUTs through the normal encrypt path so the destination bucket's default — or the copy request's `x-amz-server-side-encryption*` headers — controls the new encryption.
- **SSE-C on copy**: not yet supported; returns `NotImplemented 501` when either side is SSE-C (needs the AWS-specific `x-amz-copy-source-server-side-encryption-customer-*` header set).

#### Not yet supported
- AWS KMS backend (trait seat in `objectio-kms::KmsProvider` is ready; unused until a deployment needs it)
- KMS key rotation + status changes (enable/disable/pending-deletion)
- CopyObject with SSE-C on source or destination
- Streaming re-encrypt on CopyObject (current implementation buffers the object in memory — same as single-part PUT)

---

# 2. Data Protection (Erasure Coding)

### Reed-Solomon (MDS)
- Standard Maximum Distance Separable codes
- Configurable k (data) + m (parity) shards
- Any k shards can reconstruct the data
- Presets: 3+2, 4+2, 8+4

### LRC (Locally Repairable Codes) *(Enterprise)*
- Local parity groups for faster single-shard recovery
- Three parameters: k (data) + local parity + global parity
- Presets: 6+2+1, 10+2+2
- Gated on an Enterprise license — `POST /_admin/pools` with `ec_type=ERASURE_LRC` returns `EnterpriseLicenseRequired` on Community

### Replication
- Simple N-way replication (no erasure coding)
- Configurable replica count (2-way, 3-way)

### Storage Pools
- Independent erasure coding and placement per pool
- Failure domain configuration: OSD, node, rack, datacenter
- OSD tags for hardware-class targeting (e.g., `nvme`, `ssd`)
- Per-pool storage quotas
- Protection presets with automatic feasibility check (greyed out if insufficient nodes)
- Console shows efficiency %, min nodes, fault tolerance for each configuration

---

# 3. Data Lakehouse (Iceberg Tables + Delta Sharing)

> Create analytics warehouses, manage Iceberg tables, query with Spark/Trino, and share data across organizations via the Delta Sharing protocol.

The Iceberg Tables and Table Sharing sections below describe the full data lakehouse capabilities.

---

# 4. Multi-Tenancy *(Enterprise)*

### Tenant Management
- **Create/List/Update/Delete tenants** via `/_admin/tenants` API and console
- **Per-tenant configuration**: Default pool, allowed pools, OIDC provider, admin users
- **Tenant quotas**: Storage bytes, max buckets, max objects
- **Tenant labels**: Arbitrary key-value metadata

### Tenant-Scoped Resources
- **Users**: Created with `tenant` field, ARN includes tenant (`arn:objectio:iam::{tenant}:user/{name}`)
- **Access keys**: Inherit tenant from user
- **Buckets**: Created with tenant scope, ListBuckets filtered by tenant
- **Warehouses**: Tenant-scoped Iceberg warehouses
- **Shares**: Tenant-scoped table shares

### Tenant Isolation
- **Auth middleware**: `AuthResult` carries tenant from access key → user → tenant chain
- **Console session**: Session token includes tenant, admin pages hidden for tenant users
- **API filtering**: All list operations filter by authenticated user's tenant
- **System admin**: Users with empty tenant see all resources across tenants

### Tenant Admin Role
- **`TenantConfig.admin_users`**: List of user IDs (or ARNs) who can admin the tenant without being system admins
- **`POST /_admin/tenants/{name}/admins`** — grant tenant-admin
- **`DELETE /_admin/tenants/{name}/admins/{user}`** — revoke
- **Three-tier access model**: System admin (tenant=""), tenant admin (in `admin_users` for their tenant), regular user
- **Scope**: tenant admins can create/delete users + access keys + buckets + warehouses + shares **within their own tenant**; they cannot touch pools, tenants, nodes, system config, KMS backend, or other tenants
- **Console**: dedicated "Manage admins" panel on each tenant row; picker lists that tenant's users + free-form user-id/ARN input
- **CLI**: `objectio-cli tenant admin add|remove|list <tenant> <user>`

---

# 5. IAM (Identity & Access Management)

### Users & Access Keys
- **Create/List/Delete users** with display name, email, tenant
- **Access key management**: Generate/list/delete access keys per user
- **Key format**: `AKIA*` (permanent) or `ASIA*` (temporary STS)
- **Auto-create key on user creation** (console flow)
- **Admin user**: Auto-created on first meta service startup with credentials logged

### Authentication
- **AWS SigV4**: Full Signature Version 4 authentication
- **AWS SigV2**: Legacy Signature Version 2 support
- **OIDC/OAuth2** *(Enterprise)*: External identity provider integration (Keycloak, Azure AD, Okta, Google). Provider registration at `identity/openid/{name}` is license-gated; login flow using already-registered providers is not
- **Console session cookies**: HMAC-signed session tokens with 24-hour TTL
- **STS temporary credentials**: 1-hour HMAC-signed tokens for Iceberg data access

### Authorization
- **Bucket policies**: IAM-style JSON policies attached to a bucket via `PUT /{bucket}?policy`. Allow/Deny evaluation over Principal, Action, Resource, Condition.
- **Named IAM policies (PBAC)**: `PUT/GET/DELETE /_admin/policies/{name}` + `POST /_admin/policies/attach|detach` attach reusable JSON policies to users or groups. Used by the KMS admin API for `kms:*` action gating.
- **Action namespaces**: `s3:*` (object + bucket ops), `iceberg:*` (catalog ops at warehouse/namespace/table level), `kms:*` (KMS admin — `Status`, `Version`, `API`, `CreateKey`, `ListKeys`, `KeyStatus`, `DeleteKey`).
- **Condition keys** (populated from request at evaluation time):
  - `aws:SourceIp` — requester IP
  - `aws:SecureTransport` — `"true"` when TLS-terminated upstream
  - `aws:username` — the calling user's name
  - `s3:prefix` — object-key prefix (List operations)
  - `s3:x-amz-server-side-encryption` — `AES256` / `aws:kms` when supplied on PUT
  - `s3:x-amz-server-side-encryption-aws-kms-key-id` — KMS key ARN on PUT
  - `obio:CurrentTime` — server time
  - Plus arbitrary user-supplied variables via the `RequestContext` HashMap
- **Policy evaluator**: explicit `Deny` wins, then any `Allow` wins, else implicit deny. Wildcards in actions (`kms:*`) and resources (`arn:obio:kms:::keys-abc-*`) supported.
- **Admin access**: the built-in admin user bypasses policy evaluation; console sessions are treated as admin for admin endpoints.

---

## 3a. Iceberg Tables (REST Catalog) *(Enterprise)*

### Warehouse Management
- **POST/GET/DELETE /_admin/warehouses** — Create, list, delete warehouses
- **Backing bucket**: Each warehouse automatically creates an S3 bucket (`iceberg-{name}`)
- **Tenant-scoped**: Warehouses isolated per tenant
- **Warehouse-scoped namespaces**: Each warehouse has its own namespace/table hierarchy

### Iceberg REST Catalog API
- **GET /iceberg/v1/config** — Catalog configuration (warehouse location)
- **Namespace CRUD**: Create, load, drop, list, update properties, check existence
- **Table CRUD**: Create, load, commit (CAS), drop, list, rename, check existence
- **Multi-level namespaces**: Dot-separated hierarchy (e.g., `db.schema`)
- **Namespace policies**: IAM-style access control per namespace
- **Table policies**: Per-table access control
- **Data filters**: Column/row-level security per principal

### Authentication
- **SigV4**: Same access key / secret key as S3 (industry standard)
- **OIDC/OAuth2**: Bearer token authentication via external provider
- **Console session cookie**: For web console access
- **Unified auth middleware**: Tries SigV4 → OIDC → session cookie → unauthenticated

### Vended Credentials (STS)
- **LoadTable response**: Includes temporary S3 credentials in `config` field
- **Credential format**: `ASIA*` access key + secret + HMAC-signed session token
- **1-hour TTL**: Fresh credentials generated per LoadTable call
- **Path-style access**: `s3.path-style-access=true` for ObjectIO compatibility
- **External endpoint**: Configured via `--external-endpoint` for public access

### Client Compatibility
- **PyIceberg**: Full support with SigV4 signing (`rest.sigv4-enabled=true`)
- **Spark**: REST catalog with SigV4 or OAuth2
- **Trino/Presto**: REST catalog connector

---

## 3b. Table Sharing (Delta Sharing Protocol) *(Enterprise)*

### Share Management
- **Create/List/Delete shares** via admin API and console
- **Tenant-scoped shares**: Filtered by authenticated user's tenant

### Table Types
- **Delta Lake**: Share Delta tables by S3 bucket + path (`table_type: "delta"`)
- **Iceberg UniForm**: Share Iceberg tables via Delta Sharing protocol (`table_type: "uniform"`)
- **Mixed sharing**: Both Delta and Iceberg tables in the same share

### Recipient & Token Management
- **Create recipients**: Name + list of accessible shares
- **Bearer tokens**: One-time display, SHA-256 hashed for storage
- **profile.share**: Standard Delta Sharing credential file format
- **Token authentication**: Recipients authenticate via `Authorization: Bearer <token>`

### Protocol Endpoints
- `GET /delta-sharing/v1/shares` — List accessible shares
- `GET /delta-sharing/v1/shares/{share}/schemas` — List schemas in share
- `GET /delta-sharing/v1/shares/{share}/schemas/{schema}/tables` — List tables
- `GET .../tables/{table}/version` — Current table version (snapshot ID)
- `GET .../tables/{table}/metadata` — Table metadata (NDJSON format)
- `POST .../tables/{table}/query` — Query table data (returns presigned S3 URLs)

### Data Access
- **Presigned URLs**: Server generates SigV4 presigned URLs for Parquet data files
- **1-hour expiry**: Presigned URLs valid for 1 hour
- **No S3 credentials needed**: Recipients access data via presigned URLs only

---

# 6. File Storage (NFS / POSIX)

> **Status: Planned**

File storage provides POSIX-compatible access to ObjectIO buckets via standard file system protocols, enabling applications that require file-level semantics (read/write/seek, directory listing, permissions) without code changes.

### Planned Features
- **NFS v3/v4 Gateway**: Dedicated service translating NFS operations to S3 API calls
- **POSIX semantics**: Open, read, write, seek, rename, delete, directory listing
- **SMB/CIFS Gateway**: Windows-compatible file sharing
- **FUSE mount**: Client-side mount of S3 buckets as local file systems

### Current File-Like Access (via S3)
- **Object Browser** (Console): Navigate bucket → folder → file hierarchy
- **Presigned URLs**: Time-limited download/upload URLs for direct file access
- **Folder semantics**: Delimiter-based folder listing (`?delimiter=/`)
- **Range reads**: HTTP Range header for partial file reads
- **Multipart upload**: Large file upload in parts (up to 10,000 parts)

---

# 7. Block Storage

### gRPC Block Service
- **Thin provisioning**: Storage allocated only on write
- **4 MB chunks**: Logical block addresses mapped to erasure-coded chunks on OSDs
- **512-byte LBA**: Standard sector size
- **Snapshots**: Copy-on-write snapshots and writable clones
- **Write journal**: Crash-consistent writes via write-ahead journal
- **Write cache**: Coalesces small writes before flushing to chunks

### QoS (Quality of Service)
- **Per-volume IOPS limits**: Token bucket rate limiting
- **Per-volume bandwidth limits**: Configurable MB/s caps
- **Priority scheduling**: High/normal/low priority queues

### Protocol Support
- **gRPC BlockService**: Native block I/O over gRPC (port 9300)
- **NBD (Network Block Device)**: Linux kernel NBD attachment (port 10809)
- **iSCSI**: iSCSI target support (planned)
- **NVMe-oF**: NVMe over Fabrics target (planned)

### Block Gateway
- Stateless, horizontally scalable
- Accepts block I/O, buffers in write cache, erasure-encodes 4 MB chunks, flushes to OSDs
- Bridges block storage volumes to the erasure-coded object layer

---

# 8. Web Console

### Authentication
- **Login page**: Access key + secret key authentication
- **Session management**: HMAC-signed HttpOnly cookies, 24-hour TTL
- **Tenant-aware**: Sidebar adapts based on user's tenant (admin pages hidden for tenant users)
- **Logout**: Clears session cookie

### Pages

| Page | Description |
|------|-------------|
| **Dashboard** | Cluster health, stats (buckets, users, namespaces, S3 requests), services status |
| **Buckets** | Create/delete buckets, versioning/lock/lifecycle badges, tenant column |
| **Bucket Detail** | Tabs: Versioning toggle, Object Lock config, Lifecycle rule editor |
| **Object Browser** | Navigate buckets → folders → files, breadcrumb path, folder/file counts |
| **Storage Pools** | Protection presets, EC config (RS/LRC/Replication), failure domain, node-aware constraints |
| **Tenants** | Tenant CRUD, quotas, OIDC provider, pool assignment |
| **Users & Keys** | Create users with tenant selector, auto-create access keys, credential display |
| **Identity** | OIDC provider configuration (Azure, Keycloak, Okta, Google, generic) |
| **Tables** | Warehouse → Namespace → Table hierarchy, table metadata viewer |
| **Table Sharing** | Share/table/recipient CRUD, UniForm type support, token + profile.share generation |
| **Monitoring** | Live Prometheus metrics, charts (line, area, bar), 5s polling |
| **Nodes & Drives** | K8s node grouping → OSD pods → disks, CPU/RAM/OS info, usage bars |
| **Encryption** | SSE master-key status, KMS backend selector (Disabled / Local / Vault), hot-swap + Test button |
| **Topology** | Interactive, collapsible diagram of region → zone → datacenter → rack → host → OSDs with color-coded level pills. Per-pool placement satisfiability strip (green check / amber warning + reason). Distinct counts at every level. |
| **License** | Current tier banner (Community / Enterprise), licensee + expiry, file upload to install, capacity + node usage bars (green/amber/red), per-feature unlock matrix |

### UI Design
- Compact 13px base font (Inter)
- Tailwind CSS styling
- Loading bar animations
- Hover-reveal actions (delete buttons)
- Sticky table headers
- Status badges with colored dots

---

# 9. Infrastructure

### Architecture
- **Gateway** (stateless, horizontally scalable): S3 API, Iceberg REST Catalog, Delta Sharing, Admin API, Console
- **Meta** (Raft cluster): Bucket/object metadata, placement (CRUSH 2.0), IAM, tenant config
- **OSD** (one per disk): Erasure-coded shard storage with Direct I/O, WAL
- **Block Gateway** (optional): Block storage over gRPC + NBD

### Deployment
- **Kubernetes (Helm)**: Production-grade Helm chart with StatefulSets, PVCs, Ingress
- **Docker Compose**: Local development cluster (3 meta + 6 OSD + 1 gateway)
- **Single binary**: All-in-one Docker image with per-service entrypoints

### Observability
- **Prometheus metrics**: Exported at `/metrics` on each service
- **Per-operation counters**: `objectio_s3_requests_total`, `objectio_iceberg_requests_total`
- **Per-operation latency**: Duration histograms
- **OSD status**: Disk capacity, shard counts, SMART health
- **Node info**: K8s node name (downward API), OS version, CPU, RAM, ObjectIO version

### Networking
| Service | API Port | Metrics Port |
|---------|----------|-------------|
| Gateway | 9000 | 9000 `/metrics` |
| Meta | 9100 | 9101 |
| OSD | 9200 | 9201 |
| Block Gateway | 9300 (gRPC) / 10809 (NBD) | — |

### Storage Engine
- **4 MB chunk/stripe size**: Fundamental EC unit
- **64 KB block size**: Internal allocation unit
- **O_DIRECT / F_NOCACHE**: Bypass OS page cache
- **Write-ahead log**: Crash-consistent writes
- **ARC metadata cache**: Adaptive replacement cache
- **CRUSH 2.0 placement**: Consistent hashing with failure domain awareness

### Cluster Topology & Locality
- **5-level failure domain model** on `OsdNode` and `FailureDomainInfo`: region → zone → datacenter → rack → host. Additive proto change, back-compat with 3-level deployments (empty zone/host = "inherit from enclosing")
- **`TopologyDistance` metric** (`objectio-placement::distance`): ranks two peers as `SameHost < SameRack < SameDatacenter < SameZone < SameRegion < Remote < Unknown`. Total order — safe to use as a sort key. Empty-on-both-sides at a level counts as matching so deployments that only use some levels aren't penalized
- **Placement feasibility validation**: `Crush2::validate_placement_feasibility(level, required_count)` returns `InsufficientDomainSpread { required_level, required_count, available_count }` when a pool cannot spread across the configured level. Surfaced via `GET /_admin/placement/validate?pool=NAME`
- **Gateway self-positioning**: `--topology-region/zone/datacenter/rack/host` CLI flags (or `OBJECTIO_TOPOLOGY_*` env vars). Stored on `AppState.self_topology`; visible via `GET /_admin/cluster-info`
- **Locality-aware EC reads**: `get_object` ranks all candidate shard positions by `distance(gateway, shard_host)`, fetches the k closest first. Parity reconstruction only triggers when nearer shards are unhealthy. No protocol change — same `ReadShard` RPCs, different ordering
- **`GET /_admin/topology`** — aggregated region→zone→datacenter→rack→host→OSDs tree + per-level distinct counts
- **`GET /_admin/cluster-info`** — gateway's self-position + every OSD's computed distance + `is_local` flag
- **Prometheus metric**: `objectio_read_locality_bytes_total{locality="same-rack"|"same-zone"|"same-datacenter"|"same-region"|"remote"|"unknown"}` — bytes fetched from OSDs partitioned by topology distance. Multi-DC deployments use this to prove cross-DC bandwidth is being collapsed
- **CLI**: `objectio-cli topology show` (full tree + distinct counts) and `topology validate <pool>` (fails with exit 1 when a pool is unsatisfiable)

### Stable OSD identity & license-cap enforcement
- **OSD registration retry**: `register_with_meta` backs off exponentially (2s → 30s cap) until it succeeds. Stops OSDs from silently staying out of the cluster after a meta-first rolling restart
- **Address-based dedupe**: re-registration at an existing address with a different node_id evicts the stale entry from both `osd_nodes` and the CRUSH topology. One-shot dedupe on meta startup also cleans ghosts left by older binaries
- **`RegisterOsdRequest.disk_capacity_bytes`**: per-disk raw capacity reported at registration so meta can sum across OSDs and enforce the license `max_raw_capacity_bytes` cap
- **License-cap enforcement at registration**: new-OSD registration returns `ResourceExhausted { "license node cap reached" | "license raw-capacity cap would be exceeded" }` when a registration would exceed `max_nodes` or `max_raw_capacity_bytes`. Idempotent re-registration of an already-known `node_id` skips the check so a healthy cluster never refuses to come back up after a restart

---

# 10. Licensing & Tiering

Two tiers — **Community** (free, default) and **Enterprise** (signed license required). No license installed → Community; there is no grace-period state. Verification is local-only (no phone-home) against an Ed25519 public key baked into the gateway + meta binaries.

### Tier split

| Feature | Community | Enterprise |
|---|:-:|:-:|
| S3 API (bucket + object + multipart + versioning + object lock + lifecycle) | ✓ | ✓ |
| IAM (users, access keys, groups, bucket + named policies) | ✓ | ✓ |
| Reed-Solomon MDS erasure coding | ✓ | ✓ |
| Replication | ✓ | ✓ |
| SSE-S3 (builtin KMS) | ✓ | ✓ |
| SSE-C (customer-provided keys) | ✓ | ✓ |
| Console + CLI admin | ✓ | ✓ |
| Topology / locality-aware reads | ✓ | ✓ |
| **Multi-tenancy** (create tenants, tenant admins) | — | ✓ |
| **Iceberg REST Catalog** (tables, warehouses, policies) | — | ✓ |
| **Delta Sharing** (protocol + recipients + admin API) | — | ✓ |
| **SSE-KMS + external KMS** (Vault Transit) | — | ✓ |
| **OIDC / external SSO** (register providers via `identity/openid/*`) | — | ✓ |
| **LRC** (Locally Repairable Codes) | — | ✓ |

### License format

- Ed25519-signed JSON: `{ "payload": { tier, licensee, issued_at, expires_at, features, max_nodes, max_raw_capacity_bytes }, "signature": "base64(ed25519(canonical_json(payload)))" }`
- **Canonical serialization** with sorted keys + `skip_serializing_if` on zero-valued caps so old licenses still verify after additive schema bumps
- **Capacity / node caps**: optional on every license. `max_nodes=0` or `max_raw_capacity_bytes=0` means unlimited. Capacity accepts human-readable suffixes (`100T`, `1PB`, `500G`, `2.5P`) in the signer
- **Blanket vs per-feature**: a license with an empty `features` list unlocks every Enterprise feature; a non-empty list restricts to exactly those (e.g. KMS-only, Iceberg-only)

### License lifecycle

| Endpoint | Action |
|---|---|
| `GET /_admin/license` | Current tier + licensee + expiry + enabled features + live usage (node_count, raw_capacity_bytes) vs limits |
| `PUT /_admin/license` | Install a license file. Verifies signature + expiry before persisting to meta `license/active`. **Hot-swap** — gated routers unlock without a gateway restart |
| `DELETE /_admin/license` | Remove license; cluster reverts to Community immediately |

Loading order at gateway startup: `--license <path>` CLI flag → `$OBJECTIO_LICENSE` env (path or inline JSON) → meta `license/active` config entry → Community default. Any failure (missing file, bad signature, expired) logs a warning and degrades to Community — startup never hard-fails on the license.

### CLI

```sh
objectio-cli license show              # current license (from meta) + verify against built-in pubkey
objectio-cli license install <file>    # local verify → write to meta
objectio-cli license verify <file>     # offline verify against built-in pubkey, never touches the cluster
objectio-cli license remove            # delete from meta; gateway reverts on next restart
```

### Gate enforcement

- **Router-level**: Iceberg and Delta-Sharing routers mount `feature_gate` middleware; every request under `/iceberg/*` or `/delta-sharing/*` returns structured 403 `{ "error": "EnterpriseLicenseRequired", "feature": "iceberg", "tier_required": "enterprise" }` on Community
- **Handler-level**: SSE-KMS `apply_put_sse`, `admin_create_tenant`, Vault KMS backend install in `admin_kms_put_config`, OIDC provider config in `admin_set_config(identity/openid/*)`, and LRC in `admin_create_pool`/`admin_update_pool` each check `state.has_feature(…)` inline
- **Registration-level**: meta's `register_osd` enforces `max_nodes` and `max_raw_capacity_bytes` (see §9 "Stable OSD identity & license-cap enforcement" above). Scale-up blocks at the *real* chokepoint — new hardware joining the cluster — rather than on logical carve-outs like bucket creation

### Repo layout ("open core", CockroachDB-style)

**One binary, one public repo, runtime gates.** Modeled after
CockroachDB: all source — including Enterprise features — is visible on
GitHub. The business moat is (a) a source-available license on the
Enterprise directories, (b) an Ed25519 private signing key that only
ObjectIO holds, and (c) runtime license checks that refuse to serve
Enterprise endpoints without a valid signed license.

**Public repo** (`cloudomate/objectio`, this one):

| Path | License |
|---|---|
| Everything not in `enterprise/` | Apache-2.0 (`./LICENSE`) |
| `enterprise/crates/objectio-iceberg/` | BUSL-1.1 (`enterprise/LICENSE`), converts to Apache-2.0 on 2030-04-18 |
| `enterprise/crates/objectio-delta-sharing/` | BUSL-1.1, same change date |

The BUSL-1.1 clause blocks third parties from offering the Enterprise
features as a competitive managed service. Self-hosting in production is
explicitly permitted by the additional-use grant.

**Private repo** (`cloudomate/objectio-enterprise`, access-restricted):

| Path | Purpose |
|---|---|
| `bin/objectio-license-gen/` | Offline Ed25519 signer (keygen / issue / verify). Proprietary license; never published |

**Outside every repo**:

| Path | Purpose |
|---|---|
| `~/.objectio-license-signing/license-signing.key` | Ed25519 private signing key (mode 0600, offline backups) |

Rotating the signing key is a breaking change for existing licensees:
generate a new keypair with `objectio-license-gen keygen`, paste the new
`LICENSE_PUBLIC_KEY` into `crates/objectio-license/src/pubkey.rs`, and
cut a new release. Coordinate with customers before.
