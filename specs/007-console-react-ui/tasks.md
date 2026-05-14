# 007 - Console React UI: Tasks

## Documentation Tasks

### Tech Stack

- [ ] Document Vite build tool configuration
- [ ] Document React 19 with TypeScript
- [ ] Document multi-bundle strategy (ops + tenant)
- [ ] Document SPA fallback for client-side routing

### Directory Structure

- [ ] Document src/apps/ops/ bundle
- [ ] Document src/apps/tenant/ bundle
- [ ] Document src/pages/ shared pages
- [ ] Document src/components/ shared components
- [ ] Document dist/ build output

### Pages

- [ ] Document Dashboard page
- [ ] Document Login page
- [ ] Document Buckets/Objects pages
- [ ] Document Users/Identity/Policies pages
- [ ] Document Monitoring/Cluster/Topology/Drives pages
- [ ] Document Encryption/Tenants/Pools pages
- [ ] Document IcebergCatalog/DeltaSharing pages
- [ ] Document License page
- [ ] Document which pages are in ops vs tenant bundle

### Bundle Split

- [ ] Document ops bundle: full admin capabilities
- [ ] Document tenant bundle: self-service for end users
- [ ] Document --ops-console-listen and --tenant-console-listen flags
- [ ] Document separate port serving when split

### Serving

- [ ] Document /_console/ route on Gateway
- [ ] Document tower_http::ServeDir mounting
- [ ] Document OBJECTIO_CONSOLE_DIR environment variable
- [ ] Document default path /usr/share/objectio/console
- [ ] Document Docker build copy of console/dist/

### Console Auth

- [ ] Document /_console/api/login endpoint
- [ ] Document /_console/api/session endpoint
- [ ] Document /_console/api/logout endpoint
- [ ] Document /_console/api/me/keys endpoint
- [ ] Document OIDC integration: /_console/api/oidc/{enabled,authorize,callback}
- [ ] Document console_auth.rs implementation

### Build Commands

- [ ] Document npm install
- [ ] Document npm run build (tsc -b && vite build)
- [ ] Document npm run dev (Vite dev server with /api proxy)
- [ ] Document npm run lint (eslint)