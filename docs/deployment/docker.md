# Docker Deployment

ObjectIO provides Docker images for easy container-based deployment.

## Docker Images

| Image | Purpose | Size |
|-------|---------|------|
| `objectio-gateway` | S3 API gateway | ~35 MB |
| `objectio-meta` | Metadata service | ~35 MB |
| `objectio-osd` | Storage node | ~35 MB |
| `objectio-cli` | Admin CLI | ~35 MB |
| `objectio` | All-in-one | ~50 MB |

## Building Images

```bash
# Build all images
make docker

# Build specific images
make docker-gateway
make docker-meta
make docker-osd
make docker-cli

# Build with specific tag
DOCKER_TAG=v1.0.0 make docker
```

## Single-Node Deployment (Simplest)

For development, testing, or single-server production use:

### docker-compose.single.yml

```yaml
services:
  meta:
    image: objectio-meta:latest
    hostname: meta
    command:
      - "--node-id"
      - "1"
      - "--listen"
      - "0.0.0.0:9100"
      - "--replication"
      - "1"
      - "--log-level"
      - "info"
    volumes:
      - meta-data:/var/lib/objectio
    ports:
      - "9100:9100"
    networks:
      - objectio-net
    restart: unless-stopped

  osd:
    image: objectio-osd:latest
    hostname: osd1
    user: root
    command:
      - "--listen"
      - "0.0.0.0:9200"
      - "--advertise-addr"
      - "osd1:9200"
      - "--disks"
      - "/data/objectio/storage/disk0.img"
      - "--meta-endpoint"
      - "http://meta:9100"
    volumes:
      - osd-storage:/data/objectio/storage
      - osd-wal:/var/lib/objectio/wal
    ports:
      - "9200:9200"
    depends_on:
      - meta
    networks:
      - objectio-net
    restart: unless-stopped

  gateway:
    image: objectio-gateway:latest
    hostname: gateway
    command:
      - "--listen"
      - "0.0.0.0:9000"
      - "--meta-endpoint"
      - "http://meta:9100"
      - "--no-auth"
      - "--log-level"
      - "info"
    ports:
      - "9000:9000"
    depends_on:
      - meta
      - osd
    networks:
      - objectio-net
    restart: unless-stopped

networks:
  objectio-net:
    driver: bridge

volumes:
  meta-data:
  osd-storage:
  osd-wal:
```

### Start Single-Node Cluster

```bash
# Start
docker compose -f docker-compose.single.yml up -d

# Test
aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp myfile.txt s3://test/

# Stop
docker compose -f docker-compose.single.yml down
```

## Development Cluster

Start a local development cluster with Docker Compose:

```bash
# Start cluster (3 meta + 6 OSD + 1 gateway)
make cluster-up

# Check status
make cluster-status

# View logs
make cluster-logs

# Stop cluster
make cluster-down

# Stop and remove data
make cluster-clean
```

### docker-compose.cluster.yml

```yaml
version: '3.8'

x-meta-common: &meta-common
  image: objectio-meta:latest
  networks:
    - objectio-net
  restart: unless-stopped

x-osd-common: &osd-common
  image: objectio-osd:latest
  networks:
    - objectio-net
  restart: unless-stopped

services:
  gateway:
    image: objectio-gateway:latest
    ports:
      - "9000:9000"
    command:
      - "--listen"
      - "0.0.0.0:9000"
      - "--meta-endpoint"
      - "http://meta1:9100"
      - "--no-auth"
    depends_on:
      - meta1
      - meta2
      - meta3

  meta1:
    <<: *meta-common
    hostname: meta1
    command:
      - "--node-id"
      - "1"
      - "--listen"
      - "0.0.0.0:9100"
      - "--peers"
      - "meta1:9100,meta2:9100,meta3:9100"
    volumes:
      - meta1-data:/var/lib/objectio

  # ... meta2, meta3, osd1-6 ...

networks:
  objectio-net:
    driver: bridge

volumes:
  meta1-data:
  meta2-data:
  meta3-data:
  osd1-data:
  # ...
```

## Production Deployment

For production with raw disks:

### docker-compose.prod.yml

```yaml
version: '3.8'

x-osd-common: &osd-common
  image: objectio-osd:latest
  restart: unless-stopped
  networks:
    - objectio-net
  depends_on:
    - meta1
  privileged: true
  cap_add:
    - SYS_RAWIO
    - SYS_ADMIN

services:
  gateway:
    image: objectio-gateway:latest
    hostname: gateway
    ports:
      - "9000:9000"
    command:
      - "--listen"
      - "0.0.0.0:9000"
      - "--meta-endpoint"
      - "http://meta1:9100"
    depends_on:
      - meta1
      - meta2
      - meta3
    networks:
      - objectio-net

  meta1:
    image: objectio-meta:latest
    hostname: meta1
    command:
      - "--node-id"
      - "1"
      - "--listen"
      - "0.0.0.0:9100"
      - "--peers"
      - "meta1:9100,meta2:9100,meta3:9100"
    volumes:
      - meta1-data:/var/lib/objectio
    ports:
      - "9100:9100"
    networks:
      - objectio-net

  # ... meta2, meta3 ...

  osd1:
    <<: *osd-common
    hostname: osd1
    command:
      - "--listen"
      - "0.0.0.0:9200"
      - "--advertise-addr"
      - "osd1:9200"
      - "--disks"
      - "/dev/vdb"
      - "--meta-endpoint"
      - "http://meta1:9100"
    devices:
      - /dev/vdb:/dev/vdb
    ports:
      - "9200:9200"

  # ... osd2-7 ...

networks:
  objectio-net:
    driver: bridge

volumes:
  meta1-data:
  meta2-data:
  meta3-data:
```

### Deploying with Raw Disks

```bash
# Start production cluster
docker compose -f docker-compose.prod.yml up -d

# Check logs
docker compose -f docker-compose.prod.yml logs -f

# Stop cluster
docker compose -f docker-compose.prod.yml down
```

## Docker Build Details

### Multi-Stage Dockerfile

```dockerfile
# Stage 1: Build with Rust
FROM rust:1.92-bookworm AS builder

# Install ISA-L build dependencies
RUN apt-get update && apt-get install -y \
    nasm autoconf automake libtool libclang-dev

WORKDIR /build
COPY . .
RUN cargo build --release --features isal

# Stage 2: Runtime image
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates libssl3
RUN groupadd -r objectio && useradd -r -g objectio objectio

# Stage 3: Gateway
FROM runtime AS gateway
COPY --from=builder /build/target/release/objectio-gateway /usr/local/bin/
USER objectio
EXPOSE 9000
ENTRYPOINT ["objectio-gateway"]

# ... meta, osd, cli stages ...
```

### Build Arguments

| Arg | Default | Description |
|-----|---------|-------------|
| `RUST_VERSION` | 1.92 | Rust compiler version |
| `FEATURES` | isal | Cargo features to enable |

```bash
# Build with specific Rust version
docker build --build-arg RUST_VERSION=1.93 -t objectio:latest .

# Build without ISA-L
docker build --build-arg FEATURES="" -t objectio:no-isal .
```

## Health Checks

All images include health checks:

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:9000/health"]
  interval: 30s
  timeout: 5s
  retries: 3
  start_period: 10s
```

## Networking

### Bridge Network (default)

Services communicate via Docker's internal DNS:
- `meta1:9100`, `meta2:9100`, `meta3:9100`
- `osd1:9200`, `osd2:9200`, etc.

### Host Network

For production with less overhead:

```yaml
services:
  osd1:
    network_mode: host
    command:
      - "--listen"
      - "0.0.0.0:9200"
      - "--advertise-addr"
      - "192.168.1.10:9200"
```

## Resource Limits

```yaml
services:
  gateway:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M

  osd1:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '1'
          memory: 2G
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs objectio-gateway-1

# Check container status
docker inspect objectio-gateway-1 --format='{{.State.Status}}'
```

### Port Already in Use

```bash
# Find what's using the port
sudo lsof -i :9000

# Stop conflicting process
sudo kill <PID>
```

### Disk Access Issues

```bash
# Verify disk is accessible
ls -la /dev/vdb

# Check container can access device
docker exec objectio-osd1-1 ls -la /dev/vdb
```
