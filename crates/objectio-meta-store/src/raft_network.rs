//! gRPC transport for openraft RPCs between meta pods.
//!
//! Openraft calls [`RaftNetwork`] whenever it needs to talk to a peer —
//! AppendEntries replications, leader-election votes, snapshot installs.
//! We implement that over a tonic `RaftRpcClient` that wraps the
//! `RaftRpc` service defined in `proto/raft.proto`.
//!
//! ## Wire format
//!
//! `RaftEnvelope { from, payload }`, where `payload` is the JSON-encoded
//! openraft request or response. JSON lets us evolve openraft's types
//! without churning the proto schema; the cost is a bit of bandwidth we
//! don't care about at meta-RPC rates.
//!
//! ## Factory / connection model
//!
//! One `MetaRaftNetwork` per peer node_id. Tonic channels are created
//! lazily on first use and cached on the network instance — subsequent
//! RPCs reuse the HTTP/2 connection. We clone the network per RPC batch
//! (openraft takes `&mut self`), so the underlying Channel is `Arc`-like
//! and shareable.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use parking_lot::Mutex;
use tonic::transport::Channel;

use crate::raft::MetaTypeConfig;

type NodeId = u64;

/// Channel cache shared across every network instance — keyed by peer
/// address. Tonic `Channel` is cheap to clone (`Arc` inside), so a single
/// connected channel is reused across all concurrent RPC paths.
#[derive(Clone, Default)]
struct ChannelCache {
    inner: Arc<Mutex<HashMap<String, Channel>>>,
}

impl ChannelCache {
    fn get_or_connect(&self, addr: &str) -> Result<Channel, String> {
        if let Some(c) = self.inner.lock().get(addr) {
            return Ok(c.clone());
        }
        let uri = normalize_uri(addr);
        let channel = Channel::from_shared(uri)
            .map_err(|e| format!("invalid meta address `{addr}`: {e}"))?
            .connect_lazy();
        self.inner
            .lock()
            .insert(addr.to_string(), channel.clone());
        Ok(channel)
    }
}

fn normalize_uri(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Factory openraft calls on startup / membership change to get a network
/// handle per peer. Owns the shared connection cache.
#[derive(Clone, Default)]
pub struct MetaRaftNetworkFactory {
    self_id: u64,
    channels: ChannelCache,
}

impl MetaRaftNetworkFactory {
    #[must_use]
    pub fn new(self_id: u64) -> Self {
        Self {
            self_id,
            channels: ChannelCache::default(),
        }
    }
}

impl RaftNetworkFactory<MetaTypeConfig> for MetaRaftNetworkFactory {
    type Network = MetaRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        MetaRaftNetwork {
            self_id: self.self_id,
            target,
            target_addr: node.addr.clone(),
            channels: self.channels.clone(),
        }
    }
}

/// RaftNetwork for a single peer. Holds enough state to (re)dial the
/// target; every RPC opens a fresh tonic client using the cached channel.
pub struct MetaRaftNetwork {
    self_id: u64,
    /// Target peer node id — preserved for telemetry / future retry
    /// logic that differentiates per-peer error budgets.
    #[allow(dead_code)]
    target: u64,
    target_addr: String,
    channels: ChannelCache,
}

impl MetaRaftNetwork {
    fn client(
        &self,
    ) -> Result<objectio_proto::raft::raft_rpc_client::RaftRpcClient<Channel>, TransportErr> {
        let ch = self
            .channels
            .get_or_connect(&self.target_addr)
            .map_err(TransportErr)?;
        Ok(objectio_proto::raft::raft_rpc_client::RaftRpcClient::new(ch))
    }

    fn envelope(&self, payload: Vec<u8>) -> objectio_proto::raft::RaftEnvelope {
        objectio_proto::raft::RaftEnvelope {
            from: self.self_id,
            payload,
        }
    }
}

// Opaque error types that `RPCError::{Unreachable, Network}` accept. Both
// wrap a message string; we keep them distinct so debug logs can tell
// "couldn't dial" apart from "RPC returned garbage".
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct TransportErr(String);

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct RpcErr(String);

impl From<tonic::Status> for RpcErr {
    fn from(s: tonic::Status) -> Self {
        RpcErr(format!("{}: {}", s.code(), s.message()))
    }
}

impl From<serde_json::Error> for RpcErr {
    fn from(e: serde_json::Error) -> Self {
        RpcErr(format!("decode: {e}"))
    }
}

impl RaftNetwork<MetaTypeConfig> for MetaRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload = serde_json::to_vec(&rpc).expect("openraft payload must serialize");
        let mut client = self
            .client()
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let resp = client
            .append_entries(tonic::Request::new(self.envelope(payload)))
            .await
            .map_err(|s| RPCError::Network(NetworkError::new(&RpcErr::from(s))))?;
        serde_json::from_slice::<AppendEntriesResponse<NodeId>>(&resp.into_inner().payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&RpcErr::from(e))))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let payload = serde_json::to_vec(&rpc).expect("openraft payload must serialize");
        let mut client = self
            .client()
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let resp = client
            .install_snapshot(tonic::Request::new(self.envelope(payload)))
            .await
            .map_err(|s| RPCError::Network(NetworkError::new(&RpcErr::from(s))))?;
        serde_json::from_slice::<InstallSnapshotResponse<NodeId>>(&resp.into_inner().payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&RpcErr::from(e))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload = serde_json::to_vec(&rpc).expect("openraft payload must serialize");
        let mut client = self
            .client()
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let resp = client
            .vote(tonic::Request::new(self.envelope(payload)))
            .await
            .map_err(|s| RPCError::Network(NetworkError::new(&RpcErr::from(s))))?;
        serde_json::from_slice::<VoteResponse<NodeId>>(&resp.into_inner().payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&RpcErr::from(e))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_uri_adds_http_prefix() {
        assert_eq!(normalize_uri("127.0.0.1:9100"), "http://127.0.0.1:9100");
        assert_eq!(
            normalize_uri("http://meta-0:9100"),
            "http://meta-0:9100"
        );
        assert_eq!(
            normalize_uri("https://meta.example.com:9100"),
            "https://meta.example.com:9100"
        );
    }
}
