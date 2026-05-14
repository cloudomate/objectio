# 006 - Iceberg and Delta Sharing: Tasks

## Documentation Tasks

### Iceberg REST Catalog

- [ ] Document GET /iceberg/v1/config endpoint
- [ ] Document namespace CRUD endpoints (list, create, get, delete)
- [ ] Document table CRUD endpoints (list, create, get, delete)
- [ ] Document POST /iceberg/v1/tables/rename endpoint
- [ ] Document ?warehouse=X query parameter requirement
- [ ] Document warehouse creation via POST /_admin/warehouses
- [ ] Document auto-provisioning of iceberg-{name} bucket

### Iceberg Access Control

- [ ] Document arn:obio:iceberg::: ARN format
- [ ] Document iceberg:LoadTable, iceberg:CreateTable, etc. actions
- [ ] Document policy hierarchy (root → namespace → table, deny wins)
- [ ] Document __policy key in namespace properties
- [ ] Document policy_json field in table proto
- [ ] Document PUT policy endpoints (admin only)

### Iceberg Metrics

- [ ] Document objectio_iceberg_requests_total{operation,status} counter
- [ ] Document objectio_iceberg_request_duration_seconds{operation} histogram

### Delta Sharing Protocol

- [ ] Document /delta-sharing/v1/* endpoints
- [ ] Document bearer token authentication (NOT SigV4)
- [ ] Document token format (UUID or opaque)
- [ ] Document presigned S3 URL generation for data access
- [ ] Document recipient download directly from S3

### Delta Sharing Admin API

- [ ] Document /_admin/delta-sharing/* endpoints
- [ ] Document share management
- [ ] Document schema management
- [ ] Document table management
- [ ] Document recipient management

### Delta Sharing Access Control

- [ ] Document arn:obio:delta-sharing::: ARN format
- [ ] Document delta-sharing:Receive, delta-sharing:Share actions
- [ ] Document PolicyEvaluator reuse from objectio-auth

### Unity Catalog Integration (Enterprise)

- [ ] Document objectio-unity-catalog enterprise crate
- [ ] Document Unity Catalog bridge if implemented