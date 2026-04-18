//! HTTP admin endpoints for Raft bootstrap + membership.
//!
//! These are meta-local — operators (or a helm post-install Job) call
//! them to turn a fresh pod into a single-voter cluster, then onboard
//! additional pods as learners, then promote them to voters. The gateway
//! doesn't proxy these: the cluster has to be up before the gateway has
//! someone to talk to.
//!
//! ## Endpoints (all on the meta admin port, default :9102)
//!
//! - `POST /init`
//!   Bootstrap a brand-new single-voter cluster with this node as the
//!   sole member. Idempotent — a second call fails with `NotAllowed`
//!   because a cluster already exists.
//! - `POST /add-learner  { "node_id": u64, "addr": "host:port" }`
//!   Register a peer as a learner (non-voting). It catches up on the
//!   log without holding up writes.
//! - `POST /change-membership  { "voters": [u64, ...] }`
//!   Promote the given set of node ids (which must already be learners)
//!   to voters. Writes go through quorum of this new voter set.
//! - `GET  /status`
//!   Current leader id, term, membership config, last_log_index.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use objectio_meta_store::MetaTypeConfig;
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

pub struct RaftAdminState {
    pub raft: Arc<Raft<MetaTypeConfig>>,
    pub self_id: u64,
    pub self_addr: String,
}

impl RaftAdminState {
    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/init", post(init))
            .route("/add-learner", post(add_learner))
            .route("/change-membership", post(change_membership))
            .route("/status", get(status))
            .with_state(self)
    }
}

/// POST /init — single-voter bootstrap. Use on the first meta pod only.
async fn init(State(s): State<Arc<RaftAdminState>>) -> impl IntoResponse {
    let mut members = std::collections::BTreeMap::new();
    members.insert(
        s.self_id,
        BasicNode {
            addr: s.self_addr.clone(),
        },
    );
    match s.raft.initialize(members).await {
        Ok(()) => {
            info!(
                "raft cluster initialized with self_id={} at {}",
                s.self_id, s.self_addr
            );
            Json(serde_json::json!({
                "ok": true,
                "node_id": s.self_id,
                "addr": s.self_addr,
            }))
            .into_response()
        }
        Err(e) => {
            warn!("raft init failed: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
struct AddLearnerBody {
    node_id: u64,
    addr: String,
}

/// POST /add-learner — register a peer as a non-voting learner. Must be
/// called on the current leader. Blocks until the learner is caught up.
async fn add_learner(
    State(s): State<Arc<RaftAdminState>>,
    Json(body): Json<AddLearnerBody>,
) -> impl IntoResponse {
    match s
        .raft
        .add_learner(body.node_id, BasicNode { addr: body.addr.clone() }, true)
        .await
    {
        Ok(resp) => {
            info!(
                "learner {} @ {} added at log_id={:?}",
                body.node_id, body.addr, resp.log_id
            );
            Json(serde_json::json!({
                "ok": true,
                "node_id": body.node_id,
                "addr": body.addr,
                "log_id": format!("{:?}", resp.log_id),
            }))
            .into_response()
        }
        Err(e) => {
            warn!("add_learner({}, {}) failed: {}", body.node_id, body.addr, e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
struct ChangeMembershipBody {
    voters: Vec<u64>,
}

/// POST /change-membership — promote the given set to voters. Must be
/// called on the leader. Replaces the existing voter set outright, so
/// pass the full intended voter list, not a delta.
async fn change_membership(
    State(s): State<Arc<RaftAdminState>>,
    Json(body): Json<ChangeMembershipBody>,
) -> impl IntoResponse {
    let voters: std::collections::BTreeSet<u64> = body.voters.into_iter().collect();
    // `retain_learners = false` drops any existing learner that isn't in
    // the new voter set. If you want to keep them as learners, call
    // `change_membership(voters, true)` — we default to false because the
    // typical flow promotes every learner to voter.
    match s.raft.change_membership(voters.clone(), false).await {
        Ok(resp) => {
            info!(
                "membership changed to voters={:?} at log_id={:?}",
                voters, resp.log_id
            );
            Json(serde_json::json!({
                "ok": true,
                "voters": voters,
                "log_id": format!("{:?}", resp.log_id),
            }))
            .into_response()
        }
        Err(e) => {
            warn!("change_membership({:?}) failed: {}", voters, e);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

#[derive(Serialize)]
struct StatusResponse {
    self_id: u64,
    self_addr: String,
    leader_id: Option<u64>,
    current_term: u64,
    last_log_index: Option<u64>,
    last_applied: Option<u64>,
    state: String,
    voters: Vec<u64>,
    learners: Vec<u64>,
}

/// GET /status — current leader / term / log / membership.
async fn status(State(s): State<Arc<RaftAdminState>>) -> impl IntoResponse {
    let m = s.raft.metrics().borrow().clone();
    let cfg = m.membership_config.membership();
    let voters: Vec<u64> = cfg.voter_ids().collect();
    let learners: Vec<u64> = cfg.learner_ids().collect();
    let resp = StatusResponse {
        self_id: s.self_id,
        self_addr: s.self_addr.clone(),
        leader_id: m.current_leader,
        current_term: m.current_term,
        last_log_index: m.last_log_index,
        last_applied: m.last_applied.map(|l| l.index),
        state: format!("{:?}", m.state),
        voters,
        learners,
    };
    Json(resp).into_response()
}
