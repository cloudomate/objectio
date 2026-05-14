# 006 - Iceberg and Delta Sharing

## Overview

ObjectIO provides Apache Iceberg REST Catalog API and Delta Sharing protocol for table sharing, both integrated with the IAM-based access control system.

## Iceberg REST Catalog

### Implementation

- **objectio-iceberg** crate: Iceberg REST Catalog server
- Mounted at `/iceberg/v1/*` on Gateway
- Metadata persisted via Meta service (Redb)

### Endpoints

- `GET /iceberg/v1/config` — Catalog configuration
- `GET /iceberg/v1/namespaces` — List namespaces
- `POST /iceberg/v1/namespaces` — Create namespace
- `GET /iceberg/v1/namespaces/{ns}` — Get namespace
- `DELETE /iceberg/v1/namespaces/{ns}` — Delete namespace
- `GET /iceberg/v1/namespaces/{ns}/tables` — List tables
- `POST /iceberg/v1/namespaces/{ns}/tables` — Create table
- `GET /iceberg/v1/namespaces/{ns}/tables/{table}` — Get table
- `DELETE /iceberg/v1/namespaces/{ns}/tables/{table}` — Delete table
- `POST /iceberg/v1/tables/rename` — Rename table

### Access Control

- Policy ARNs: `arn:obio:iceberg:::`
- Actions: `iceberg:LoadTable`, `iceberg:CreateTable`, `iceberg:DropTable`, etc.
- Policy hierarchy: root → namespace → table (deny wins)
- Namespace policies in `__policy` key in properties
- Table policies in proto `policy_json` field

### Policy Management

- `PUT /iceberg/v1/namespaces/{ns}/policy` — Set namespace policy (admin only)
- `PUT /iceberg/v1/namespaces/{ns}/tables/{table}/policy` — Set table policy (admin only)

### Warehouses

- Created via `POST /_admin/warehouses {"name":"X"}`
- Auto-provisions backing bucket `iceberg-X`
- Every request must pass `?warehouse=X` query parameter
- Missing/unknown warehouse → 400 error

### Metrics

- `objectio_iceberg_requests_total{operation,status}` — Request counter
- `objectio_iceberg_request_duration_seconds{operation}` — Latency histogram

## Delta Sharing

### Implementation

- **objectio-delta-sharing** crate: Delta Sharing protocol server
- Axum sub-router at `/delta-sharing/v1/*`
- Mounted OUTSIDE SigV4 auth middleware (bearer token auth)

### Endpoints

- `GET /delta-sharing/v1/share/{share}/schemas/{schema}/tables/{table}` — Table metadata
- `GET /delta-sharing/v1/recipients` — List recipients
- `POST /delta-sharing/v1/recipients` — Create recipient
- `GET /delta-sharing/v1/tokens` — List tokens (admin)
- Admin API at `/_admin/delta-sharing/*` — Share/schema/table/recipient management

### Authentication

- Bearer token authentication (NOT SigV4)
- Token format: UUID or opaque token
- Tokens managed via admin API

### Data Access

- Presigned S3 URLs for data download
- Generated using admin credentials
- Recipients download directly from S3

### Access Control

- Policy ARNs: `arn:obio:delta-sharing:::`
- Actions: `delta-sharing:Receive`, `delta-sharing:Share`, etc.
- Reuses PolicyEvaluator from objectio-auth