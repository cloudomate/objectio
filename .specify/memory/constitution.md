# ObjectIO Constitution

## Purpose

This document establishes the governing principles and invariants for the ObjectIO distributed storage system. All architectural decisions, implementation choices, and operational practices must align with these principles.

---

## 1. Distributed Systems Correctness

### 1.1 Metadata Consistency

- Metadata is the source of truth for all object state, location, and ownership.
- Metadata operations MUST be consistent and durable before acknowledging writes to clients.
- The Meta service uses Raft consensus to ensure metadata consistency across nodes.
- No client-facing acknowledgment occurs until metadata updates are committed via Raft and replicated.
- Silent data loss is unacceptable. Every write MUST be traceable to a durable acknowledgment.

### 1.2 No Silent Data Loss

- Object data and metadata MUST remain consistent under all failure scenarios.
- Lost writes MUST be detected, logged, and surfaced via alerting — not silently discarded.
- Checksums MUST be validated on every read to detect silent corruption.
- The system MUST NOT swallow errors or return success when underlying operations fail.

---

## 2. EC (Erasure Coding) Integrity

### 2.1 4MB Chunk Invariant — Sacred

- The fundamental EC data unit is a **4MB chunk**.
- This invariant is non-negotiable and MUST be enforced at all layers of the stack.
- Chunk boundaries MUST be preserved across all EC encode/decode operations.
- Mixing chunk sizes within a single object or placement group is forbidden.

### 2.2 Error Type Segregation — Sacred

- **BlockError** and **objectio_common::Error** are distinct error types and MUST NOT be mixed.**
- BlockError represents storage-layer failures (I/O, checksum, corruption).
- objectio_common::Error represents API, protocol, and application-layer failures.
- Error conversion points MUST be explicit and documented. No silent error type coercion.
- Conflating these error types violates the EC integrity contract and is a critical bug.

---

## 3. Performance by Default

### 3.1 64KB Block Size — Invariant

- The default and preferred block size is **64KB**.
- This size is chosen to balance sequential I/O efficiency with random access granularity.
- Deviations from 64KB require explicit justification and documentation.

### 3.2 O_DIRECT — Invariant

- Storage backends MUST use O_DIRECT for data paths to bypass the page cache.
- This ensures deterministic performance and avoids double-buffering overhead.
- O_DIRECT is not optional in production data paths.

### 3.3 Write-Ahead Log (WAL) — Invariant

- A WAL MUST be used for all metadata and index operations.
- The WAL ensures durability and enables crash recovery without data loss.
- WAL records MUST be fsynced before acknowledgments are sent to clients.

---

## 4. Multi-Service Architecture

### 4.1 Gateway — Stateless

- The Gateway service is purely stateless and serves as the API entry point.
- It handles authentication, rate limiting, request routing, and protocol bridging.
- No object data or metadata is stored or cached in the Gateway beyond request context.
- Gateway failures do not affect data durability or availability.

### 4.2 Meta Service — Raft Consensus with 3+ Nodes

- The Meta service manages all metadata and uses Raft consensus for fault tolerance.
- A minimum of **3 nodes** is required for a functional Raft cluster.
- Quorum is required for all metadata writes — majority failure leads to read-only mode.
- Meta nodes MUST be deployed across failure domains ( racks, availability zones).

### 4.3 OSD (Object Storage Daemon) — Per-Disk Design

- Each OSD manages one or more physical disks independently.
- OSDs handle data placement, replication, EC encoding/decoding, and local I/O.
- Disk failures are isolated to the OSD managing that disk — other OSDs are unaffected.
- OSDs MUST report health and utilization metrics to the control plane.

### 4.4 Block Gateway — Protocol Bridge

- The Block Gateway translates block storage protocols (e.g., iSCSI, NVMe-oF) to ObjectIO's native protocol.
- It provides compatibility with standard block storage clients and ecosystems.
- Block Gateway is stateful for session management but stateless for data paths.

---

## 5. Zero-Trust Authentication

### 5.1 --no-auth Is Development-Only

- Authentication MUST be enabled in all non-local, non-development environments.
- The `--no-auth` flag is provided exclusively for local development and testing.
- Production deployments MUST use mutual TLS (mTLS) or token-based authentication.
- All inter-service communication requires authentication — no implicit trust.

### 5.2 Zero-Trust Model

- Every request, regardless of source, MUST be authenticated and authorized.
- No network location implies trust. Internal services are not exempt from auth.
- Credentials MUST be rotated and revoked without downtime.

---

## 6. Operational Surface — Metrics Requirement

### 6.1 Prometheus Metrics — Mandatory

- **Every feature and subsystem MUST expose Prometheus metrics.**
- Metrics are the primary observability signal for production systems.
- Required metric categories:
  - **Request latency** (histogram) — per operation type
  - **Throughput** (counter) — bytes and operations per second
  - **Error rates** (counter) — categorized by error type
  - **Resource utilization** (gauge) — CPU, memory, disk, network
  - **Health status** (gauge) — node, disk, service availability
  - **EC metrics** — chunk sizes, encode/decode latencies, corruption counts

### 6.2 Alerting and Dashboards

- Metrics MUST be wired to alerting rules for critical failure modes.
- Dashboards MUST be created for each service showing key operational health indicators.
- SLOs MUST be defined and tracked using these metrics.

---

## Summary of Sacred Invariants

| Invariant | Value |
|-----------|-------|
| EC chunk size | 4MB |
| Default block size | 64KB |
| I/O mode | O_DIRECT |
| Metadata consistency | Raft-based |
| Meta cluster size | 3+ nodes |
| Auth in production | Mandatory |
| Metrics per feature | Mandatory |

---

## Revision History

| Version | Date | Description |
|---------|------|-------------|
| 1.0 | 2026-05-14 | Initial constitution |

---

*This constitution is the authoritative source for ObjectIO architectural invariants. All team members MUST read, understand, and adhere to these principles.*