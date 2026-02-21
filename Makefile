# ObjectIO Makefile
# Common build, test, and deployment commands
#
# Usage:
#   make build          # Build all binaries locally
#   make docker         # Build all Docker images
#   make cluster-up     # Start local cluster
#   make cluster-down   # Stop local cluster

.PHONY: all build build-release test lint fmt clean \
        docker docker-gateway docker-meta docker-osd docker-cli docker-all \
        docker-multiarch docker-multiarch-gateway docker-multiarch-meta \
        docker-multiarch-osd docker-multiarch-cli docker-multiarch-all \
        buildx-setup \
        cluster-up cluster-down cluster-logs \
        kind-up kind-down kind-load kind-logs kind-status \
        dev push push-multiarch help

# Configuration
DOCKER_REGISTRY ?=
DOCKER_TAG ?= latest
RUST_VERSION ?= 1.93
PLATFORMS ?= linux/amd64,linux/arm64
BUILDX_BUILDER ?= objectio-builder

# Default target
all: build

# =============================================================================
# Local Development
# =============================================================================

## Build all binaries in debug mode
build:
	cargo build --workspace --features isal

## Build all binaries in release mode
build-release:
	cargo build --workspace --release --features isal

## Run all tests
test:
	cargo test --workspace --features isal

## Run clippy linter
lint:
	cargo clippy --workspace --all-targets --features isal -- -D warnings

## Check code formatting
fmt:
	cargo fmt --all -- --check

## Format code
fmt-fix:
	cargo fmt --all

## Clean build artifacts
clean:
	cargo clean
	rm -rf target/

# =============================================================================
# Docker Builds
# =============================================================================

## Build all Docker images
docker: docker-gateway docker-meta docker-osd docker-cli docker-all

## Build gateway image
docker-gateway:
	docker build --target gateway -t $(DOCKER_REGISTRY)objectio-gateway:$(DOCKER_TAG) .

## Build metadata service image
docker-meta:
	docker build --target meta -t $(DOCKER_REGISTRY)objectio-meta:$(DOCKER_TAG) .

## Build OSD image
docker-osd:
	docker build --target osd -t $(DOCKER_REGISTRY)objectio-osd:$(DOCKER_TAG) .

## Build CLI image
docker-cli:
	docker build --target cli -t $(DOCKER_REGISTRY)objectio-cli:$(DOCKER_TAG) .

## Build all-in-one image
docker-all:
	docker build --target all -t $(DOCKER_REGISTRY)objectio:$(DOCKER_TAG) .

## Build with BuildKit cache (faster rebuilds)
docker-cached:
	DOCKER_BUILDKIT=1 docker build \
		--target all \
		--cache-from $(DOCKER_REGISTRY)objectio:cache \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		-t $(DOCKER_REGISTRY)objectio:$(DOCKER_TAG) .

## Push all images to registry
push:
	docker push $(DOCKER_REGISTRY)objectio-gateway:$(DOCKER_TAG)
	docker push $(DOCKER_REGISTRY)objectio-meta:$(DOCKER_TAG)
	docker push $(DOCKER_REGISTRY)objectio-osd:$(DOCKER_TAG)
	docker push $(DOCKER_REGISTRY)objectio-cli:$(DOCKER_TAG)
	docker push $(DOCKER_REGISTRY)objectio:$(DOCKER_TAG)

# =============================================================================
# Multi-Architecture Docker Builds (requires docker buildx)
# =============================================================================

## Setup buildx builder for multi-platform builds
buildx-setup:
	@echo "Setting up Docker buildx builder..."
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		docker buildx create --name $(BUILDX_BUILDER) --driver docker-container --bootstrap
	@docker buildx use $(BUILDX_BUILDER)
	@echo "Builder '$(BUILDX_BUILDER)' is ready"
	@echo "Supported platforms: $$(docker buildx inspect --bootstrap | grep Platforms)"

## Build all multi-arch images and push
docker-multiarch: buildx-setup docker-multiarch-gateway docker-multiarch-meta docker-multiarch-osd docker-multiarch-cli docker-multiarch-all

## Build multi-arch gateway and push
docker-multiarch-gateway: buildx-setup
	docker buildx build --platform $(PLATFORMS) \
		--target gateway \
		-t $(DOCKER_REGISTRY)objectio-gateway:$(DOCKER_TAG) \
		--push .

## Build multi-arch meta and push
docker-multiarch-meta: buildx-setup
	docker buildx build --platform $(PLATFORMS) \
		--target meta \
		-t $(DOCKER_REGISTRY)objectio-meta:$(DOCKER_TAG) \
		--push .

## Build multi-arch OSD and push
docker-multiarch-osd: buildx-setup
	docker buildx build --platform $(PLATFORMS) \
		--target osd \
		-t $(DOCKER_REGISTRY)objectio-osd:$(DOCKER_TAG) \
		--push .

## Build multi-arch CLI and push
docker-multiarch-cli: buildx-setup
	docker buildx build --platform $(PLATFORMS) \
		--target cli \
		-t $(DOCKER_REGISTRY)objectio-cli:$(DOCKER_TAG) \
		--push .

## Build multi-arch all-in-one and push
docker-multiarch-all: buildx-setup
	docker buildx build --platform $(PLATFORMS) \
		--target all \
		-t $(DOCKER_REGISTRY)objectio:$(DOCKER_TAG) \
		--push .

## Build multi-arch images locally (load into Docker)
## Note: Can only load single platform at a time
docker-multiarch-local:
	docker buildx build --platform linux/$(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/') \
		--target all \
		-t $(DOCKER_REGISTRY)objectio:$(DOCKER_TAG) \
		--load .

# =============================================================================
# Docker Compose Development
# =============================================================================

## Start interactive dev shell
dev:
	docker compose run --rm dev

## Build in Docker container
docker-build:
	docker compose run --rm build

## Run tests in Docker container
docker-test:
	docker compose run --rm test

## Run linter in Docker container
docker-lint:
	docker compose run --rm lint

# =============================================================================
# Local Cluster Management
# =============================================================================

## Start local cluster (3 meta + 6 OSD + 1 gateway)
cluster-up: docker
	docker compose -f deploy/local-cluster/docker-compose.yml up -d

## Stop local cluster
cluster-down:
	docker compose -f deploy/local-cluster/docker-compose.yml down

## Stop cluster and remove volumes
cluster-clean:
	docker compose -f deploy/local-cluster/docker-compose.yml down -v

## View cluster logs
cluster-logs:
	docker compose -f deploy/local-cluster/docker-compose.yml logs -f

## View gateway logs
cluster-logs-gateway:
	docker compose -f deploy/local-cluster/docker-compose.yml logs -f gateway

## View meta logs
cluster-logs-meta:
	docker compose -f deploy/local-cluster/docker-compose.yml logs -f meta1 meta2 meta3

## View OSD logs
cluster-logs-osd:
	docker compose -f deploy/local-cluster/docker-compose.yml logs -f osd1 osd2 osd3 osd4 osd5 osd6

## Check cluster status
cluster-status:
	docker compose -f deploy/local-cluster/docker-compose.yml ps

## Run CLI in cluster network
cluster-cli:
	docker compose -f deploy/local-cluster/docker-compose.yml run --rm \
		-e OBJECTIO_META_ENDPOINTS=meta1:9100,meta2:9100,meta3:9100 \
		objectio-cli $(ARGS)

# =============================================================================
# Kind (Kubernetes in Docker) Cluster
# =============================================================================

## Create Kind cluster and deploy full ObjectIO stack (S3 + Iceberg + Delta Sharing)
kind-up:
	bash deploy/kind/setup.sh

## Create Kind cluster and deploy using pre-built GHCR images (no local build)
kind-up-registry:
	bash deploy/kind/setup.sh --registry ghcr.io/cloudomate

## Tear down the Kind cluster
kind-down:
	bash deploy/kind/setup.sh --teardown

## Build all images and load into the running Kind cluster
kind-load:
	docker build --target gateway       -t objectio-gateway:latest .
	docker build --target meta          -t objectio-meta:latest .
	docker build --target osd           -t objectio-osd:latest .
	docker build --target cli           -t objectio-cli:latest .
	docker build --target block-gateway -t objectio-block-gateway:latest .
	kind load docker-image objectio-gateway:latest       --name objectio
	kind load docker-image objectio-meta:latest          --name objectio
	kind load docker-image objectio-osd:latest           --name objectio
	kind load docker-image objectio-cli:latest           --name objectio
	kind load docker-image objectio-block-gateway:latest --name objectio

## Tail logs for all ObjectIO pods in Kind
kind-logs:
	kubectl logs -n objectio -l app.kubernetes.io/name=objectio --all-containers -f --max-log-requests=20

## Show pod status in Kind cluster
kind-status:
	kubectl get pods -n objectio -o wide

# =============================================================================
# CI/CD Helpers
# =============================================================================

## Run full CI pipeline locally
ci: fmt lint test
	@echo "CI pipeline passed!"

## Generate code coverage report
coverage:
	cargo llvm-cov --workspace --features isal --html

# =============================================================================
# Help
# =============================================================================

## Show this help message
help:
	@echo "ObjectIO Build System"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Local Development:"
	@echo "  build          Build all binaries (debug)"
	@echo "  build-release  Build all binaries (release)"
	@echo "  test           Run all tests"
	@echo "  lint           Run clippy linter"
	@echo "  fmt            Check code formatting"
	@echo "  fmt-fix        Fix code formatting"
	@echo "  clean          Remove build artifacts"
	@echo ""
	@echo "Docker (single-arch, native platform):"
	@echo "  docker         Build all Docker images"
	@echo "  docker-gateway Build gateway image"
	@echo "  docker-meta    Build metadata service image"
	@echo "  docker-osd     Build OSD image"
	@echo "  docker-cli     Build CLI image"
	@echo "  push           Push images to registry"
	@echo ""
	@echo "Docker Multi-Architecture (amd64 + arm64):"
	@echo "  buildx-setup           Setup buildx builder"
	@echo "  docker-multiarch       Build & push all images for amd64 + arm64"
	@echo "  docker-multiarch-local Build for current platform and load locally"
	@echo ""
	@echo "  Platform notes:"
	@echo "    - linux/amd64: Uses ISA-L for ~3-5x faster erasure coding"
	@echo "    - linux/arm64: Uses pure Rust SIMD (no ISA-L)"
	@echo ""
	@echo "Local Docker Compose Cluster:"
	@echo "  cluster-up     Start local cluster"
	@echo "  cluster-down   Stop local cluster"
	@echo "  cluster-clean  Stop cluster and remove data"
	@echo "  cluster-logs   View all logs"
	@echo "  cluster-status Check cluster status"
	@echo ""
	@echo "Kind (Kubernetes) Cluster:"
	@echo "  kind-up        Create Kind cluster + deploy full stack"
	@echo "  kind-up-registry  Deploy from GHCR images (no build)"
	@echo "  kind-down      Delete Kind cluster"
	@echo "  kind-load      Build + load images into existing Kind cluster"
	@echo "  kind-logs      Tail all pod logs"
	@echo "  kind-status    Show pod status"
	@echo ""
	@echo "Variables:"
	@echo "  DOCKER_REGISTRY  Registry prefix (default: '')"
	@echo "  DOCKER_TAG       Image tag (default: 'latest')"
	@echo "  PLATFORMS        Build platforms (default: 'linux/amd64,linux/arm64')"
