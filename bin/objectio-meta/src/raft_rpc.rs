// `tonic::Status` is large — clippy complains about every fn that returns
// `Result<_, Status>`. Boxing here would obscure the tonic handler
// signatures without payoff. Allow at the module level.
#![allow(clippy::result_large_err)]

//! Server-side tonic handler for the `RaftRpc` service.
//!
//! Mirrors the client-side transport in [`objectio_meta_store::raft_network`].
//! For each of the three inter-meta RPCs we:
//!
//! 1. JSON-decode the envelope payload into an openraft request type.
//! 2. Hand the request to the local [`openraft::Raft`] instance.
//! 3. JSON-encode the response (or encode the error body) back into an
//!    envelope.
//!
//! Error semantics: network/decode failures come back as a `tonic::Status`
//! so the client sees `RPCError::Network`. Raft-protocol-level errors
//! (vote rejected, log gap, etc.) are already carried inside the response
//! type by openraft; they're normal successes from the transport's view.
//! Only genuine bugs (Fatal) escape as `Status::internal`.
//!
//! The handler owns an `Arc<Raft<MetaTypeConfig>>` — clone for each
//! request is a pointer bump.

use std::sync::Arc;

use objectio_meta_store::MetaTypeConfig;
use objectio_proto::raft::{RaftEnvelope, raft_rpc_server::RaftRpc};
use openraft::Raft;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use tonic::{Request, Response, Status};

/// tonic service implementation. Constructed by the meta binary once the
/// [`Raft`] instance is live.
pub struct RaftRpcService {
    pub raft: Arc<Raft<MetaTypeConfig>>,
    pub self_id: u64,
}

impl RaftRpcService {
    pub fn new(raft: Arc<Raft<MetaTypeConfig>>, self_id: u64) -> Self {
        Self { raft, self_id }
    }

    fn envelope<T: serde::Serialize>(&self, v: &T) -> Result<RaftEnvelope, Status> {
        Ok(RaftEnvelope {
            from: self.self_id,
            payload: serde_json::to_vec(v)
                .map_err(|e| Status::internal(format!("encode raft response: {e}")))?,
        })
    }
}

#[tonic::async_trait]
impl RaftRpc for RaftRpcService {
    async fn append_entries(
        &self,
        req: Request<RaftEnvelope>,
    ) -> Result<Response<RaftEnvelope>, Status> {
        let bytes = req.into_inner().payload;
        let req: AppendEntriesRequest<MetaTypeConfig> = serde_json::from_slice(&bytes)
            .map_err(|e| Status::invalid_argument(format!("decode append_entries: {e}")))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("append_entries: {e}")))?;
        Ok(Response::new(self.envelope(&resp)?))
    }

    async fn vote(
        &self,
        req: Request<RaftEnvelope>,
    ) -> Result<Response<RaftEnvelope>, Status> {
        let bytes = req.into_inner().payload;
        let req: VoteRequest<u64> = serde_json::from_slice(&bytes)
            .map_err(|e| Status::invalid_argument(format!("decode vote: {e}")))?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("vote: {e}")))?;
        Ok(Response::new(self.envelope(&resp)?))
    }

    async fn install_snapshot(
        &self,
        req: Request<RaftEnvelope>,
    ) -> Result<Response<RaftEnvelope>, Status> {
        let bytes = req.into_inner().payload;
        let req: InstallSnapshotRequest<MetaTypeConfig> = serde_json::from_slice(&bytes)
            .map_err(|e| Status::invalid_argument(format!("decode install_snapshot: {e}")))?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(format!("install_snapshot: {e}")))?;
        Ok(Response::new(self.envelope(&resp)?))
    }
}
