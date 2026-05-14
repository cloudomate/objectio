# 006 - Iceberg and Delta Sharing: Plan

## Approach

Document the existing Iceberg REST Catalog and Delta Sharing implementations, focusing on endpoints, access control with IAM policies, warehouse management, and bearer token auth.

## Technical Approach

### Iceberg REST Catalog

1. **Endpoints**: Document all /iceberg/v1/* REST endpoints
2. **Access Control**: Document ARN format, action names, policy hierarchy
3. **Warehouse**: Document warehouse creation and ?warehouse= query parameter
4. **Metrics**: Document request counter and latency histogram

### Delta Sharing

1. **Protocol**: Document Delta Sharing open protocol implementation
2. **Auth**: Document bearer token auth (outside SigV4 middleware)
3. **Data Access**: Document presigned S3 URL generation
4. **Admin API**: Document /_admin/delta-sharing/* management endpoints

### Source Files to Reference

- `enterprise/crates/objectio-iceberg/src/lib.rs` — Module structure
- `enterprise/crates/objectio-iceberg/src/handlers.rs` — REST handlers
- `enterprise/crates/objectio-iceberg/src/catalog.rs` — Catalog operations
- `enterprise/crates/objectio-iceberg/src/access.rs` — Policy evaluation
- `enterprise/crates/objectio-iceberg/src/types.rs` — Type definitions
- `enterprise/crates/objectio-iceberg/src/metadata.rs` — Metadata handling
- `enterprise/crates/objectio-iceberg/src/roles.rs` — Role definitions
- `enterprise/crates/objectio-delta-sharing/src/lib.rs` — Module structure
- `enterprise/crates/objectio-delta-sharing/src/handlers.rs` — REST handlers
- `enterprise/crates/objectio-delta-sharing/src/catalog.rs` — Catalog operations
- `enterprise/crates/objectio-delta-sharing/src/access.rs` — Policy evaluation
- `enterprise/crates/objectio-delta-sharing/src/types.rs` — Type definitions
- `enterprise/crates/objectio-delta-sharing/src/delta_log.rs` — Delta log handling
- `enterprise/crates/objectio-delta-sharing/src/presigned_reader.rs` — Presigned URL generation

## Deliverables

- spec.md: Iceberg REST and Delta Sharing endpoints, auth, access control
- plan.md: This document
- tasks.md: Actionable documentation tasks