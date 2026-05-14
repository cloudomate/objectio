# 007 - Console React UI

## Overview

The ObjectIO Console is a React 19 + TypeScript SPA (Single Page Application) for cluster administration and tenant self-service, built with Vite and served by the Gateway.

## Tech Stack

- **Framework**: React 19 with TypeScript
- **Build Tool**: Vite
- **Routing**: React Router (multi-bundle)
- **Styling**: CSS (plain, component-based)
- **Bundle Strategy**: Two bundles — "ops" (admin) and "tenant" (self-service)

## Directory Structure

```
console/
├── src/
│   ├── apps/
│   │   ├── ops/        # Admin bundle
│   │   │   ├── App.tsx
│   │   │   └── main.tsx
│   │   └── tenant/     # Tenant/self-service bundle
│   │       ├── App.tsx
│   │       └── main.tsx
│   ├── pages/          # Shared pages
│   ├── components/     # Shared components
│   └── main.tsx        # Legacy SPA entry
├── dist/               # Build output
├── vite.config.ts      # Multi-bundle build config
└── tsconfig.json
```

## Page Structure

### Shared Pages

- Dashboard
- Login
- Buckets
- Objects
- Users
- Identity
- Policies
- Monitoring
- Cluster
- Topology
- Drives
- Encryption
- Tenants
- Pools
- PoolPlacement
- Balancing
- BucketDetail
- MyAccount
- IcebergCatalog
- DeltaSharing
- UnityCatalog (Enterprise)
- License

### Bundle Split

**Ops Bundle**: Full system administration
- Cluster management
- Topology/placement
- User management
- Policy management
- Monitoring
- Drives/disk health

**Tenant Bundle**: Self-service for end users
- Bucket management (create, delete)
- Object browsing/upload
- My account / keys
- Iceberg tables (namespace browsing)
- Delta Sharing recipients

## Serving

- Gateway serves console at `/_console/` via `tower_http::ServeDir`
- `$OBJECTIO_CONSOLE_DIR` env var (default `/usr/share/objectio/console`)
- SPA fallback to `index.html` for client-side routing
- Docker copies `console/dist/` to console directory during build

## Console Auth

- `/_console/api/login` — Login endpoint
- `/_console/api/session` — Session check
- `/_console/api/logout` — Logout
- `/_console/api/me/keys` — Access key management
- `/_console/api/oidc/{enabled,authorize,callback}` — OIDC integration

Implementation in `bin/objectio-gateway/src/console_auth.rs`

## Gateway Listener Split

When split listeners configured:
- `--ops-console-listen` for ops bundle
- `--tenant-console-listen` for tenant bundle
- Each bundle served on separate port

## Build Commands

```bash
cd console
npm install
npm run build    # tsc -b && vite build → console/dist/
npm run dev      # Vite dev server (proxies /api to gateway)
npm run lint     # eslint
```