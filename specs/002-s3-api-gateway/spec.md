# 002 - S3 API Gateway

## Overview

The ObjectIO Gateway is a stateless, horizontally scalable S3-compatible API gateway that handles authentication, erasure encoding, and routing to OSD storage daemons.

## S3 API Implementation

### Core Components

- **objectio-s3** crate: Axum-based S3 API handlers
- **objectio-auth** crate: AWS SigV4 authentication middleware
- **objectio-erasure** crate: Erasure encoding (4+2 default)
- **objectio-client** crate: gRPC client to OSDs

### S3 Endpoints

#### Bucket Operations

- `PUT /{bucket}` — Create bucket
- `DELETE /{bucket}` — Delete bucket
- `GET /?buckets` or `LIST BUCKETS` — List all buckets
- `HEAD /{bucket}` — Bucket metadata

#### Object Operations

- `PUT /{bucket}/{object}` — Put object
- `GET /{bucket}/{object}` — Get object
- `HEAD /{bucket}/{object}` — Object metadata
- `DELETE /{bucket}/{object}` — Delete object
- `GET /{bucket}/{object}?acl` — Object ACL
- `PUT /{bucket}/{object}?acl` — Set object ACL

#### Multipart Upload

- `POST /{bucket}/{object}?uploads` — Initiate multipart upload
- `PUT /{bucket}/{object}?partNumber=N&uploadId=ID` — Upload part
- `GET /{bucket}/{object}?uploadId=ID` — List parts
- `POST /{bucket}/{object}?uploadId=ID&action=complete` — Complete upload
- `DELETE /{bucket}/{object}?uploadId=ID` — Abort upload

#### Other Operations

- `GET /{bucket}/{object}?policy` — Bucket policy (admin only)
- `PUT /{bucket}/{object}?policy` — Set bucket policy
- `GET /{bucket}/{object}?versioning` — Bucket versioning config
- `PUT /{bucket}/{object}?versioning` — Set bucket versioning

### Authentication

#### SigV4 Authentication

- AWS Signature Version 4 implementation in `objectio-auth`
- Feature flags: `builtin` (default), `oidc`, `openfga`, `full`
- Auth middleware injects `Extension<AuthResult>` into Axum handlers
- `--no-auth` flag bypasses authentication in development

#### Admin API Routes

- `/_admin/*` endpoints for user and key management
- Registered on router before auth middleware (publicly accessible)
- Implementation in `bin/objectio-gateway/src/`

### Request Flow

1. Client sends S3 request with SigV4 signature
2. Auth middleware validates signature and injects `AuthResult`
3. Handler queries Meta service for object placement (CRUSH algorithm)
4. Handler erasure-encodes data (4+2 default, 4MB chunks)
5. Handler writes shards to OSDs in parallel
6. Handler returns S3 response

### Metrics

- `objectio_s3_requests_total{operation,status}` — Request counter
- `objectio_s3_request_duration_seconds{operation}` — Latency histogram
- Exposed at `/metrics` on port 9000