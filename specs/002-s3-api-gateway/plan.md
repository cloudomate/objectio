# 002 - S3 API Gateway: Plan

## Approach

Document the existing S3 API implementation in objectio-s3 crate, focusing on the Axum handlers, SigV4 auth integration, multipart upload flow, and integration with Iceberg/Delta Sharing.

## Technical Approach

### S3 API Surface

1. **Handler Structure**: Document bucket, object, and multipart handlers in `crates/objectio-s3/src/handlers/`
2. **Auth Integration**: Document SigV4 middleware and `AuthResult` extension pattern
3. **Request Flow**: Document how requests flow from Axum through auth, placement lookup, EC encoding, OSD writes
4. **Admin API**: Document `/_admin/*` user/key management endpoints
5. **Metrics**: Document Prometheus metrics exported by S3 handlers

### Source Files to Reference

- `crates/objectio-s3/src/lib.rs` — Module structure and exports
- `crates/objectio-s3/src/handlers.rs` — Top-level handler registration
- `crates/objectio-s3/src/handlers/bucket.rs` — Bucket operations
- `crates/objectio-s3/src/handlers/object.rs` — Object operations
- `crates/objectio-s3/src/handlers/multipart.rs` — Multipart upload
- `crates/objectio-s3/src/auth.rs` — Auth context extraction
- `crates/objectio-s3/src/metrics.rs` — Prometheus metrics
- `crates/objectio-auth/src/lib.rs` — PolicyEvaluator, BucketPolicy
- `bin/objectio-gateway/src/main.rs` — Router setup

### Iceberg/Delta Sharing Integration

- Document that Gateway hosts `/iceberg/v1/*` (objectio-iceberg) and `/delta-sharing/v1/*` (objectio-delta-sharing) alongside S3
- Document policy evaluation reuse between S3 bucket policies and Iceberg/Delta policies

## Deliverables

- spec.md: S3 endpoints, auth, request flow, metrics
- plan.md: This document
- tasks.md: Actionable documentation tasks