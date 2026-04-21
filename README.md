# ObjectIO

**Unified software-defined storage in Rust.** One cluster, one binary per
service, four protocols on a shared erasure-coded durability core:

- **S3** — wire-compatible with AWS S3 (SigV4, multipart, policies, SSE)
- **Apache Iceberg REST Catalog** — embedded; warehouse creation
  auto-provisions its backing bucket
- **Delta Sharing** — read-only table sharing over bearer tokens
- **Block** — iSCSI, NVMe-oF, NBD attachment targets with thin
  provisioning, snapshots, clones, per-volume QoS

Everything runs on top of topology-aware, failure-domain-hardened
erasure coding (Reed-Solomon, LRC) with a Raft-consensus metadata
service. No JVM, no Go runtime, no external kv store.

## Features

- **Wire-compatible S3** — AWS CLI, boto3, SDKs, s3cmd all just work
- **Erasure coding** — 4+2 default, configurable up to 20+4; LRC for
  large clusters; ISA-L on x86, pure-Rust elsewhere (identical wire
  format)
- **Topology-aware placement** — 5-level failure domains (region →
  zone → dc → rack → host); hard-enforced, locality-aware reads
- **Iceberg REST Catalog** — `/iceberg/v1/*`; works with Spark, Trino,
  PyIceberg, Flink; IAM-style policies at namespace + table level;
  vended credentials
- **Delta Sharing server** — open protocol, bearer-token auth,
  presigned S3 URLs for recipients
- **Distributed block volumes** — snapshots, writable clones, thin
  provisioning, QoS (IOPS + bandwidth)
- **Raft metadata** — single-pod dev mode or 3+-pod HA; no external
  service dependency
- **Encryption at rest** — SSE-S3, SSE-C, SSE-KMS (local or external
  Vault)
- **Multi-tenancy** — per-tenant users, keys, quotas, buckets,
  warehouses, shares; OIDC SSO (Keycloak, Entra, Okta, Google)
- **Web console** — React SPA at `/_console/`; AK/SK or OIDC login;
  topology viz, tables, monitoring
- **Prometheus metrics** — per-operation histograms on S3, Iceberg,
  OSD, block paths; locality metrics split by topology distance
- **One all-in-one binary** — `objectio-aio` runs meta + OSD + gateway
  in one process for quick tests and appliance builds

## Quickstart

### One-binary monolith (fastest)

```sh
# macOS (arm64) — download the v0.1.0 release binary
curl -L -o objectio-aio \
  https://github.com/cloudomate/objectio/releases/download/v0.1.0/objectio-aio-v0.1.0-darwin-arm64
chmod +x objectio-aio && sudo mv objectio-aio /usr/local/bin/

objectio-aio
```

Banner prints the admin AK/SK and a ready-to-paste `aws ...` command.
Console at `http://localhost:9000/_console/`.

### Docker Compose (3 meta + 6 OSD + 1 gateway, 4+2 EC)

```sh
git clone https://github.com/cloudomate/objectio
cd objectio && make cluster-up
```

### Using it

```sh
# S3
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/

# Iceberg REST (PyIceberg / Spark / Trino point at)
#   http://localhost:9000/iceberg/v1
# Create a warehouse first:
curl -s -u $AK:$SK -X POST -H 'Content-Type: application/json' \
  -d '{"name":"analytics"}' \
  http://localhost:9000/_admin/warehouses
```

### Building from source

```sh
# macOS
brew install nasm autoconf automake libtool llvm protobuf
# Ubuntu / Debian
sudo apt-get install build-essential nasm autoconf automake libtool libclang-dev protobuf-compiler

cargo build --workspace --release --features isal   # omit --features on ARM
```

## Documentation

Everything else — installation topologies, operations, admin API,
Iceberg catalog management, authentication setup, TOGAF-aligned
architecture docs — lives at **[docs.objectio.io](https://docs.objectio.io)**.

Source: [cloudomate/objectio-docs](https://github.com/cloudomate/objectio-docs).

## License

Dual-licensed:

- **Apache 2.0** — everything outside `enterprise/`
- **BUSL 1.1** — `enterprise/crates/objectio-iceberg` and
  `enterprise/crates/objectio-delta-sharing`; converts to Apache-2.0
  on 2030-04-18. BUSL permits reading, self-hosting, and modifying;
  it only restricts offering the Enterprise features as a competing
  paid managed service.

Enterprise features additionally require an Ed25519-signed license at
runtime; without one, those endpoints return `403
EnterpriseLicenseRequired`. See [NOTICE](./NOTICE) for the split and
`docs.objectio.io` for the full licensing + tiering reference.
