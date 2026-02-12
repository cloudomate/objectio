# CLAUDE.local.md

Local development context for deploying ObjectIO to myvm / datacore.

## Container Registry

Registry: `cr.imys.in/objectio`

All images are pushed to:

- `cr.imys.in/objectio/objectio-gateway:<tag>`
- `cr.imys.in/objectio/objectio-meta:<tag>`
- `cr.imys.in/objectio/objectio-osd:<tag>`
- `cr.imys.in/objectio/objectio-cli:<tag>`
- `cr.imys.in/objectio/objectio:<tag>` (all-in-one)

## Build Host

Build natively on myvm (x86_64) instead of cross-compiling from macOS.

```bash
# SSH access
ssh -i ~/.ssh/onekey ubuntu@myvm.lan

# Sync repo to myvm (from dev machine)
rsync -az --delete --exclude target/ --exclude .git/ --exclude '.DS_Store' \
  -e "ssh -i ~/.ssh/onekey" \
  /Users/ys/coderepo/objectio/ ubuntu@myvm.lan:~/objectio/

# Build all images on myvm (native x86_64, ISA-L enabled)
ssh -i ~/.ssh/onekey ubuntu@myvm.lan "cd ~/objectio && \
  docker build --target gateway -t cr.imys.in/objectio/objectio-gateway:latest . && \
  docker build --target meta -t cr.imys.in/objectio/objectio-meta:latest . && \
  docker build --target osd -t cr.imys.in/objectio/objectio-osd:latest . && \
  docker build --target cli -t cr.imys.in/objectio/objectio-cli:latest ."

# Push all images from myvm
ssh -i ~/.ssh/onekey ubuntu@myvm.lan "\
  docker push cr.imys.in/objectio/objectio-gateway:latest && \
  docker push cr.imys.in/objectio/objectio-meta:latest && \
  docker push cr.imys.in/objectio/objectio-osd:latest && \
  docker push cr.imys.in/objectio/objectio-cli:latest"
```

## Datacore Deployment

- **Host**: datacore / 192.168.4.102 (x86_64)
- **SSH**: `ssh -i ~/.ssh/onekey ys@192.168.4.102`
- **Config/docker compose**: `/data/objectio/`
- **EC scheme**: 3+2 (3 data + 2 parity, 5 OSDs)
- **Disks**: Docker volumes, each OSD creates a 10GB `disk.raw` file inside its volume
- **Topology**: 3 meta (Raft) + 5 OSD + 1 gateway
- **Docker network**: `objectio_objectio-net`

Note: myvm is the **build host only** (compile + push images). The cluster runs on **datacore**.

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
- **Meta (gRPC)**: `http://192.168.4.102:9100`
- **OSDs**: ports 9200, 9202-9205

### Deploy / update

```bash
# 1. Sync + build + push (from dev machine to myvm build host)
rsync -az --delete --exclude target/ --exclude .git/ --exclude '.DS_Store' \
  -e "ssh -i ~/.ssh/onekey" \
  /Users/ys/coderepo/objectio/ ubuntu@myvm.lan:~/objectio/

ssh -i ~/.ssh/onekey ubuntu@myvm.lan "cd ~/objectio && \
  docker build --target gateway -t cr.imys.in/objectio/objectio-gateway:latest . && \
  docker build --target meta -t cr.imys.in/objectio/objectio-meta:latest . && \
  docker build --target osd -t cr.imys.in/objectio/objectio-osd:latest . && \
  docker build --target cli -t cr.imys.in/objectio/objectio-cli:latest . && \
  docker push cr.imys.in/objectio/objectio-gateway:latest && \
  docker push cr.imys.in/objectio/objectio-meta:latest && \
  docker push cr.imys.in/objectio/objectio-osd:latest && \
  docker push cr.imys.in/objectio/objectio-cli:latest"

# 2. Restart cluster (on datacore)
ssh -i ~/.ssh/onekey ys@192.168.4.102 "cd /data/objectio && \
  docker compose pull && docker compose up -d"
```

### Clean restart (wipes all data)

```bash
ssh -i ~/.ssh/onekey ys@192.168.4.102 "cd /data/objectio && \
  docker compose down -v && docker compose up -d"
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
- **Datasource**: `prometheus-objectio` (uid: `cfby2ljdndt6oa`)

Grafana is a shared `grafana-dev` instance (not managed by objectio compose). Dashboards are imported via API.

### Sync monitoring config + restart Prometheus

```bash
# Sync monitoring config to datacore
rsync -az --delete --exclude '.DS_Store' \
  -e "ssh -i ~/.ssh/onekey" \
  /Users/ys/coderepo/objectio/deploy/monitoring/ ys@192.168.4.102:/data/objectio/monitoring/

# Restart Prometheus (picks up new scrape config)
ssh -i ~/.ssh/onekey ys@192.168.4.102 "cd /data/objectio/monitoring && \
  docker compose -f docker-compose.monitoring.yml restart objectio-prometheus"
```

### Import/update a Grafana dashboard

```bash
# Import a dashboard JSON into grafana-dev
cat deploy/monitoring/grafana/dashboards/<dashboard>.json | \
  python3 -c 'import json,sys; print(json.dumps({"dashboard":json.load(sys.stdin),"overwrite":True,"folderId":0}))' | \
  ssh -i ~/.ssh/onekey ys@192.168.4.102 \
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
