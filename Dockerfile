# ObjectIO Multi-Stage Dockerfile
# Builds all services with automatic platform detection
#
# Multi-Architecture Support:
#   - linux/amd64 (x86_64): Uses ISA-L for accelerated erasure coding
#   - linux/arm64 (aarch64): Uses pure Rust reed-solomon-simd
#
# Build targets:
#   docker build --target gateway -t objectio-gateway .
#   docker build --target meta -t objectio-meta .
#   docker build --target osd -t objectio-osd .
#   docker build --target cli -t objectio-cli .
#   docker build --target all -t objectio .
#
# Multi-platform build (requires docker buildx):
#   docker buildx build --platform linux/amd64,linux/arm64 --target gateway -t objectio-gateway .
#
# Build args:
#   RUST_VERSION - Rust version (default: 1.92)
#   FEATURES - Override cargo features (default: auto-detect based on arch)

ARG RUST_VERSION=1.93

# =============================================================================
# Stage 1: Builder base with Rust and build dependencies
# =============================================================================
FROM rust:${RUST_VERSION}-bookworm AS builder-base

# Platform args provided by Docker buildx
ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETOS

# Install build dependencies
# NASM is only needed for ISA-L on x86_64, but we install it anyway
# (it's small and simplifies the Dockerfile)
RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    libtool \
    libclang-dev \
    pkg-config \
    protobuf-compiler \
    libprotobuf-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Install NASM only on x86_64 (required for ISA-L assembly)
RUN if [ "$(uname -m)" = "x86_64" ]; then \
        apt-get update && apt-get install -y --no-install-recommends nasm \
        && rm -rf /var/lib/apt/lists/*; \
    fi

# Install rustfmt + clippy (not always bundled in the base image)
RUN rustup component add rustfmt clippy

WORKDIR /build

# =============================================================================
# Stage 2: Dependency caching layer
# =============================================================================
FROM builder-base AS deps

# Copy only Cargo files for dependency caching
COPY Cargo.toml Cargo.lock ./

# Create dummy source files to build dependencies
COPY crates ./crates
COPY bin ./bin

# Build dependencies only (this layer gets cached)
RUN cargo fetch

# =============================================================================
# Stage 3: Build all binaries
# =============================================================================
FROM deps AS builder

# Build arguments - TARGETARCH is provided by Docker buildx
ARG TARGETARCH
ARG TARGETPLATFORM
ARG FEATURES=""

# Determine features based on architecture
# - x86_64/amd64: Enable ISA-L for hardware-accelerated erasure coding
# - arm64/aarch64: Use pure Rust reed-solomon-simd (no ISA-L)
#
# ISA-L provides ~3-5x faster erasure coding on x86_64 via Intel's
# optimized assembly routines. On ARM, we fall back to the pure Rust
# implementation which is still performant via SIMD intrinsics.
RUN set -ex && \
    ARCH="$(uname -m)" && \
    echo "Build platform: ${TARGETPLATFORM:-native}" && \
    echo "Target arch: ${TARGETARCH:-$ARCH}" && \
    echo "Native arch: $ARCH" && \
    # Determine features based on architecture
    CARGO_FEATURES="" && \
    if [ "$TARGETARCH" = "amd64" ] || [ "$ARCH" = "x86_64" ]; then \
        echo "Detected x86_64 - enabling ISA-L acceleration" && \
        CARGO_FEATURES="isal"; \
    elif [ "$TARGETARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then \
        echo "Detected ARM64 - using pure Rust erasure coding" && \
        CARGO_FEATURES=""; \
    else \
        echo "Unknown architecture: $ARCH - using default features"; \
    fi && \
    # Allow override via build arg
    if [ -n "$FEATURES" ]; then \
        echo "Feature override: $FEATURES" && \
        CARGO_FEATURES="$FEATURES"; \
    fi && \
    # Build with determined features
    echo "Final cargo features: ${CARGO_FEATURES:-default}" && \
    if [ -n "$CARGO_FEATURES" ]; then \
        cargo build --release --features "$CARGO_FEATURES"; \
    else \
        cargo build --release; \
    fi

# =============================================================================
# Stage 4: Runtime base image (minimal)
# =============================================================================
FROM debian:bookworm-slim AS runtime-base

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for running services
RUN groupadd -r objectio && useradd -r -g objectio objectio

# Common directories
RUN mkdir -p /etc/objectio /var/lib/objectio /var/log/objectio \
    && chown -R objectio:objectio /var/lib/objectio /var/log/objectio

# Health check script
COPY --chmod=755 <<'EOF' /usr/local/bin/healthcheck.sh
#!/bin/sh
# Generic health check - services should override
exit 0
EOF

# =============================================================================
# Stage 5: Gateway service (S3 API)
# =============================================================================
FROM runtime-base AS gateway

ARG TARGETARCH
ARG TARGETPLATFORM

LABEL org.opencontainers.image.title="ObjectIO Gateway"
LABEL org.opencontainers.image.description="S3-compatible API gateway for ObjectIO"
LABEL org.opencontainers.image.architecture="${TARGETARCH}"

COPY --from=builder /build/target/release/objectio-gateway /usr/local/bin/

USER objectio
EXPOSE 9000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9000/health || exit 1

ENTRYPOINT ["objectio-gateway"]
CMD ["--bind", "0.0.0.0:9000"]

# =============================================================================
# Stage 6: Metadata service (Raft consensus)
# =============================================================================
FROM runtime-base AS meta

ARG TARGETARCH

LABEL org.opencontainers.image.title="ObjectIO Metadata Service"
LABEL org.opencontainers.image.description="Raft-based metadata service for ObjectIO"
LABEL org.opencontainers.image.architecture="${TARGETARCH}"

COPY --from=builder /build/target/release/objectio-meta /usr/local/bin/

USER objectio

# 9100: Client gRPC API
# 9101: Raft peer communication
EXPOSE 9100 9101

VOLUME ["/var/lib/objectio"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD /usr/local/bin/healthcheck.sh

ENTRYPOINT ["objectio-meta"]
CMD ["--bind", "0.0.0.0:9100", "--raft-bind", "0.0.0.0:9101", "--data-dir", "/var/lib/objectio/meta"]

# =============================================================================
# Stage 7: OSD (Object Storage Daemon)
# =============================================================================
FROM runtime-base AS osd

ARG TARGETARCH

LABEL org.opencontainers.image.title="ObjectIO OSD"
LABEL org.opencontainers.image.description="Object Storage Daemon with erasure coding"
LABEL org.opencontainers.image.architecture="${TARGETARCH}"

COPY --from=builder /build/target/release/objectio-osd /usr/local/bin/

USER objectio

# 9200: Client gRPC API
# 9201: Data transfer / replication
EXPOSE 9200 9201

VOLUME ["/var/lib/objectio"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD /usr/local/bin/healthcheck.sh

ENTRYPOINT ["objectio-osd"]
CMD ["--bind", "0.0.0.0:9200", "--data-dir", "/var/lib/objectio/osd"]

# =============================================================================
# Stage 8: CLI tool
# =============================================================================
FROM runtime-base AS cli

ARG TARGETARCH

LABEL org.opencontainers.image.title="ObjectIO CLI"
LABEL org.opencontainers.image.description="Admin CLI for ObjectIO cluster management"
LABEL org.opencontainers.image.architecture="${TARGETARCH}"

COPY --from=builder /build/target/release/objectio-cli /usr/local/bin/

USER objectio

ENTRYPOINT ["objectio-cli"]
CMD ["--help"]

# =============================================================================
# Stage 9: All-in-one image (for development/testing)
# =============================================================================
FROM runtime-base AS all

ARG TARGETARCH

LABEL org.opencontainers.image.title="ObjectIO"
LABEL org.opencontainers.image.description="ObjectIO distributed object storage (all components)"
LABEL org.opencontainers.image.architecture="${TARGETARCH}"

COPY --from=builder /build/target/release/objectio-gateway /usr/local/bin/
COPY --from=builder /build/target/release/objectio-meta /usr/local/bin/
COPY --from=builder /build/target/release/objectio-osd /usr/local/bin/
COPY --from=builder /build/target/release/objectio-cli /usr/local/bin/
COPY --from=builder /build/target/release/objectio-install /usr/local/bin/

USER objectio

EXPOSE 9000 9100 9101 9200 9201

VOLUME ["/var/lib/objectio"]

# Default shows available commands
CMD ["sh", "-c", "echo 'ObjectIO - Available commands:' && echo '  objectio-gateway  - S3 API gateway' && echo '  objectio-meta     - Metadata service' && echo '  objectio-osd      - Storage daemon' && echo '  objectio-cli      - Admin CLI' && echo '  objectio-install  - Installation tool'"]
