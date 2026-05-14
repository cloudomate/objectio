# 007 - Console React UI: Plan

## Approach

Document the existing Console React SPA architecture, bundle split strategy, page structure, auth integration, and serving configuration.

## Technical Approach

### Console Architecture

1. **Tech Stack**: Document Vite + React 19 + TypeScript
2. **Bundle Strategy**: Document ops vs tenant bundle split
3. **Pages**: Document all pages and which bundle they're in
4. **Serving**: Document `/_console/` route, tower_http::ServeDir, OBJECTIO_CONSOLE_DIR
5. **Auth**: Document console auth routes and OIDC integration

### Source Files to Reference

- `console/vite.config.ts` — Multi-bundle build configuration
- `console/src/main.tsx` — Legacy SPA entry
- `console/src/apps/ops/App.tsx` — Ops bundle app
- `console/src/apps/ops/main.tsx` — Ops bundle entry
- `console/src/apps/tenant/App.tsx` — Tenant bundle app
- `console/src/apps/tenant/main.tsx` — Tenant bundle entry
- `console/src/pages/` — Page components
- `console/src/components/` — Shared components
- `bin/objectio-gateway/src/console_auth.rs` — Console auth implementation
- `bin/objectio-gateway/src/main.rs` — ServeDir mounting

### Bundle Split

- Ops: Full admin surface
- Tenant: Self-service surface

## Deliverables

- spec.md: Console architecture, bundle split, pages, auth
- plan.md: This document
- tasks.md: Actionable documentation tasks