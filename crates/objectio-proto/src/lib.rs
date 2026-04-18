//! ObjectIO Protocol - gRPC service definitions
//!
//! This crate contains the protobuf-generated code for ObjectIO's
//! internal gRPC services.

/// Storage service (OSD operations)
pub mod storage {
    tonic::include_proto!("objectio.storage");
}

/// Metadata service (bucket and object operations)
pub mod metadata {
    tonic::include_proto!("objectio.metadata");
}

/// Cluster service (node and disk management)
pub mod cluster {
    tonic::include_proto!("objectio.cluster");
}

/// Block service (volume and snapshot management)
pub mod block {
    tonic::include_proto!("objectio.block");
}

/// Raft consensus transport (meta ↔ meta, openraft 0.9).
/// `RaftEnvelope` carries a JSON-encoded openraft request/response as an
/// opaque byte payload so proto schema changes aren't needed when
/// openraft's struct shapes evolve.
pub mod raft {
    tonic::include_proto!("objectio.raft");
}
