# 002 - S3 API Gateway: Tasks

## Documentation Tasks

### S3 Endpoints

- [ ] Document PUT/DELETE/GET/HEAD bucket operations
- [ ] Document PUT/GET/HEAD/DELETE object operations
- [ ] Document multipart upload lifecycle (initiate, upload parts, complete, abort)
- [ ] Document bucket policy GET/PUT operations
- [ ] Document bucket versioning GET/PUT operations
- [ ] Document object ACL operations

### Authentication

- [ ] Document SigV4 authentication flow
- [ ] Document AuthResult extension injection pattern
- [ ] Document --no-auth development flag
- [ ] Document objectio-auth feature flags (builtin, oidc, openfga, full)
- [ ] Document PolicyEvaluator reuse for bucket policies

### Request Flow

- [ ] Document handler → Meta placement lookup flow
- [ ] Document erasure encoding with objectio-erasure (4+2 default)
- [ ] Document parallel shard write to OSDs via gRPC
- [ ] Document response assembly and S3 response formatting

### Admin API

- [ ] Document /_admin/* user management endpoints
- [ ] Document /_admin/* key management endpoints
- [ ] Document auth bypass for admin routes (registered before auth middleware)

### Metrics

- [ ] Document objectio_s3_requests_total counter with operation/status labels
- [ ] Document objectio_s3_request_duration_seconds histogram
- [ ] Document /metrics endpoint on port 9000

### Integration

- [ ] Document Gateway hosting S3 + Iceberg + Delta Sharing on same port
- [ ] Document iceberg:LoadTable and delta-sharing:Receive actions from PolicyEvaluator