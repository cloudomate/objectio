# CLAUDE.local.md

Local development context for deploying ObjectIO to datacore.

## Container Registry

Registry: `cr.imys.in/objectio`

All images are pushed to:

- `cr.imys.in/objectio/objectio-gateway:<tag>`
- `cr.imys.in/objectio/objectio-meta:<tag>`
- `cr.imys.in/objectio/objectio-osd:<tag>`
- `cr.imys.in/objectio/objectio-cli:<tag>`
- `cr.imys.in/objectio/objectio:<tag>` (all-in-one)

## CI / CD

GitHub Actions (`.github/workflows/ci.yml`) runs on a **self-hosted runner** (x86_64):

1. **CI job**: Builds the `deps` Dockerfile stage, runs fmt + clippy + test inside Docker containers.
2. **Build & Push job** (main branch only, after CI passes): Builds all Docker targets, tags `:latest` + `:$SHA_SHORT`, pushes to `cr.imys.in/objectio/`.

The self-hosted runner has `REGISTRY_USER` and `REGISTRY_PASSWORD` set as environment variables for `docker login cr.imys.in`.

Push to `main` → CI passes → images built & pushed → pull on datacore.

## Datacore Deployment

- **Host**: datacore / 192.168.4.102 (x86_64)
- **SSH**: `ssh datacore` (see `~/.ssh/config`)
- **Compose file on host**: `/data/objectio/` (uses `deploy/datacore/docker-compose.yml`)
- **EC scheme**: 3+2 (3 data + 2 parity, 5 OSDs)
- **Disks**: Docker volumes, each OSD creates a 10GB `disk.raw` file inside its volume
- **Topology**: 3 meta (Raft) + 5 OSD + 1 gateway
- **Docker network**: `objectio_objectio-net`

### OSD disk notes

The OSD storage engine uses `O_DIRECT` and expects a **file path**, not a directory.
Docker volumes mount as directories, so OSD configs point to a file inside the volume:

```toml
# config/osdN.toml
[storage]
disks = ["/data/disk0/disk.raw"]   # file inside Docker volume, NOT the directory
```

OSDs run as `user: root` (Docker volumes are root-owned).
The OSD auto-creates a 10GB sparse `disk.raw` on first start.

### Endpoints

- **Gateway (S3)**: `http://192.168.4.102:9000`
- **Iceberg REST Catalog**: `http://192.168.4.102:9000/iceberg/v1/`
- **Meta (gRPC)**: `http://192.168.4.102:9100`
- **OSDs**: ports 9200, 9202-9205

### Deploy / update

```bash
# Pull latest images and restart (on datacore)
ssh datacore "cd /data/objectio && docker compose pull && docker compose up -d"
```

Images are built and pushed automatically by CI on push to `main`. Manual build if needed:

```bash
# Build + push from datacore (or any x86_64 host with Docker)
ssh datacore "cd /data/objectio && \
  docker build --target gateway -t cr.imys.in/objectio/objectio-gateway:latest . && \
  docker build --target meta    -t cr.imys.in/objectio/objectio-meta:latest . && \
  docker build --target osd     -t cr.imys.in/objectio/objectio-osd:latest . && \
  docker build --target cli     -t cr.imys.in/objectio/objectio-cli:latest . && \
  docker push cr.imys.in/objectio/objectio-gateway:latest && \
  docker push cr.imys.in/objectio/objectio-meta:latest && \
  docker push cr.imys.in/objectio/objectio-osd:latest && \
  docker push cr.imys.in/objectio/objectio-cli:latest"
```

### Clean restart (wipes all data)

```bash
ssh datacore "cd /data/objectio && docker compose down -v && docker compose up -d"
```

### Test with S3

```bash
# Create bucket
aws --endpoint-url http://192.168.4.102:9000 s3 mb s3://test

# Upload
echo "hello objectio" | aws --endpoint-url http://192.168.4.102:9000 s3 cp - s3://test/hello.txt

# Download
aws --endpoint-url http://192.168.4.102:9000 s3 cp s3://test/hello.txt -

# Range read (first 5 bytes → 206 Partial Content)
curl -r 0-4 http://192.168.4.102:9000/test/hello.txt

# Upload large file (multi-stripe)
dd if=/dev/urandom bs=1M count=20 of=/tmp/bigfile
aws --endpoint-url http://192.168.4.102:9000 s3 cp /tmp/bigfile s3://test/bigfile

# Range read on large file (should only fetch 1 stripe, not all)
curl -r 0-1023 http://192.168.4.102:9000/test/bigfile -o /dev/null -w '%{http_code}\n'  # 206

# List
aws --endpoint-url http://192.168.4.102:9000 s3 ls s3://test/
```

### Test Iceberg REST Catalog

```bash
ENDPOINT=http://192.168.4.102:9000/iceberg/v1

# Get config
curl -s $ENDPOINT/config | jq .

# Create namespace
curl -s -X POST $ENDPOINT/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace":["analytics"],"properties":{"owner":"alice"}}' | jq .

# List namespaces
curl -s $ENDPOINT/namespaces | jq .

# Create table
curl -s -X POST $ENDPOINT/namespaces/analytics/tables \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","schema":{"type":"struct","fields":[{"id":1,"name":"id","type":"long","required":true}]}}' | jq .

# List tables
curl -s $ENDPOINT/namespaces/analytics/tables | jq .

# Load table
curl -s $ENDPOINT/namespaces/analytics/tables/events | jq .

# Set namespace policy (admin only, requires SigV4 auth)
curl -s -X PUT $ENDPOINT/namespaces/analytics/policy \
  -H 'Content-Type: application/json' \
  -d '{"policy":"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"OBIO\":[\"*\"]},\"Action\":[\"iceberg:*\"],\"Resource\":[\"arn:obio:iceberg:::analytics/*\"]}]}"}'
```

### CLI against datacore

```bash
objectio-cli -e http://192.168.4.102:9100 user list
objectio-cli -e http://192.168.4.102:9100 volume list
objectio-cli -e http://192.168.4.102:9100 volume create test-vol --size 10G
objectio-cli -e http://192.168.4.102:9100 snapshot create <vol-id> --name snap1
```

## Monitoring

Prometheus and Grafana run on **datacore** alongside the cluster.

- **Prometheus**: `http://192.168.4.102:9090` (container: `objectio-prometheus`)
- **Grafana**: `http://192.168.4.102:3002` (container: `grafana-dev`, credentials: `admin/admin`)
- **Monitoring config**: `/data/objectio/monitoring/` on datacore
- **Monitoring compose**: `deploy/monitoring/docker-compose.monitoring.yml`
- **Datasource**: `prometheus-objectio` (uid: `cfby2ljdndt6oa`)

Grafana is a shared `grafana-dev` instance (not managed by objectio compose). Dashboards are imported via API.

### Sync monitoring config + restart Prometheus

```bash
# Sync monitoring config to datacore
rsync -az --delete --exclude '.DS_Store' \
  -e "ssh -i ~/.ssh/ifinia" \
  deploy/monitoring/ datacore:/data/objectio/monitoring/

# Restart Prometheus (picks up new scrape config)
ssh datacore "cd /data/objectio/monitoring && \
  docker compose -f docker-compose.monitoring.yml restart objectio-prometheus"
```

### Import/update a Grafana dashboard

```bash
cat deploy/monitoring/grafana/dashboards/<dashboard>.json | \
  python3 -c 'import json,sys; print(json.dumps({"dashboard":json.load(sys.stdin),"overwrite":True,"folderId":0}))' | \
  ssh datacore \
    "curl -s -X POST http://localhost:3002/api/dashboards/db -u admin:admin -H 'Content-Type: application/json' -d @-"
```

### Dashboards

| Dashboard | UID | URL |
|-----------|-----|-----|
| Cluster Overview | `objectio-cluster-overview` | `http://192.168.4.102:3002/d/objectio-cluster-overview` |
| Block Storage | `objectio-block-storage` | `http://192.168.4.102:3002/d/objectio-block-storage` |
| OSD Detail | `objectio-osd-detail` | `http://192.168.4.102:3002/d/objectio-osd-detail` |
| Disk Health | `objectio-disk-health` | `http://192.168.4.102:3002/d/objectio-disk-health` |
| S3 Operations | `objectio-s3-operations` | `http://192.168.4.102:3002/d/objectio-s3-operations` |
