# 001 - Architecture Overview: Plan

## Approach

Document the existing four-service architecture from CLAUDE.md, Cargo.toml workspace members, crate source files, and deploy artifacts.

## Technical Approach

### Architecture Documentation

1. **Service Model**: Document Gateway (S3 + Iceberg + Delta Sharing), Meta (Raft + Redb), OSD (raw disk + WAL), Block Gateway (gRPC + NBD)
2. **Crate Graph**: Map all workspace crates and their dependencies from Cargo.toml
3. **Constants**: Extract key constants (4MB chunk, 512B LBA, 64KB block, 4+2 EC) from source code comments and config examples
4. **Ports**: Document default listener ports from CLAUDE.md and config examples
5. **Binary Targets**: Document all binaries from workspace Cargo.toml

### Information Sources

- `/tmp/objectio/CLAUDE.md` — primary architecture reference
- `/tmp/objectio/Cargo.toml` — workspace members and dependency graph
- `/tmp/objectio/crates/*/Cargo.toml` — individual crate dependencies
- `/tmp/objectio/examples/config/` — service configuration examples
- `/tmp/objectio/deploy/helm/objectio/values.yaml` — production configuration

## Deliverables

- spec.md: Architecture overview, service list, crate graph, constants, ports
- plan.md: This document
- tasks.md: Actionable documentation tasks