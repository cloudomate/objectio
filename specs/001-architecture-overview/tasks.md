# 001 - Architecture Overview: Tasks

## Documentation Tasks

### Core Architecture

- [ ] Document four-service architecture (Gateway, Meta, OSD, Block Gateway) with responsibilities
- [ ] Document service ports from CLAUDE.md and config examples
- [ ] Document all workspace crates with their purpose and dependencies
- [ ] Document crate dependency flow as ASCII diagram
- [ ] Document all binary targets in bin/ directory

### Key Constants

- [ ] Document 4MB chunk/stripe size constant and its usage in EC encoding
- [ ] Document 512-byte LBA size for block storage sector addressing
- [ ] Document 8192 LBAs per chunk calculation (4MB / 512B)
- [ ] Document 64KB storage block size for internal allocation
- [ ] Document 4KB O_DIRECT alignment requirement
- [ ] Document 4+2 default EC scheme (4 data + 2 parity shards)

### Console SPA

- [ ] Document console as Vite + React 19 + TypeScript sub-project
- [ ] Document ops/tenant bundle split architecture
- [ ] Document `/_console/` route served by gateway with tower_http::ServeDir
- [ ] Document OBJECTIO_CONSOLE_DIR environment variable
- [ ] Document console auth routes: /_console/api/login, /_console/api/session, /_console/api/logout

### Service Details

- [ ] Document Gateway as stateless, horizontally scalable S3 API receiver
- [ ] Document Meta as Raft cluster using OpenRaft + Redb for bucket/object metadata and IAM
- [ ] Document OSD as raw disk storage with O_DIRECT and WAL for durability
- [ ] Document Block Gateway with gRPC BlockService :9300 and NBD :10809

### Gateway Listener Split (Optional)

- [ ] Document --admin-listen, --ops-console-listen, --tenant-console-listen flags
- [ ] Document how control plane splits off :9000 when split listeners configured
- [ ] Document ops/tenant bundle separation rationale