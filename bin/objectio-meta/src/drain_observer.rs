//! Drain observer — the minimal slice of Phase 3.
//!
//! Watches OSDs marked `admin_state = Draining` and flips them to `Out`
//! once their physical `shard_count` reaches zero. That's useful TODAY
//! in three cases:
//!
//! 1. Operator ages data out via lifecycle rules before decomissioning.
//! 2. Operator manually runs an external migrator.
//! 3. Phase 3b's migrator runs — the observer is what detects when
//!    migration has finished and closes the loop by marking the OSD Out.
//!
//! # Why leader-only
//!
//! The observer issues `SetOsdAdminState` through Raft
//! (`raft.client_write`). On a follower that call returns
//! `ForwardToLeader` and the observer would burn through log indices
//! pointlessly. Every replica runs this task but only the leader ever
//! reaches `client_write` — followers return early the moment they see
//! a non-leader vote.
//!
//! # Why 30-second cadence
//!
//! `shard_count` on the OSD is eventually-consistent with local reality
//! (WAL flush, background compactor). Polling every 30s is fast enough
//! for a human-driven drain and slow enough that the task isn't
//! hammering each OSD's admin port.

use std::sync::Arc;
use std::time::Duration;

use objectio_proto::storage::{GetStatusRequest, storage_service_client::StorageServiceClient};
use tokio::time::{MissedTickBehavior, interval};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::service::MetaService;

/// How often we sweep Draining OSDs. Not a Raft timer — no correctness
/// implications if it runs late; it just delays the auto-flip.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Per-call timeout when asking an OSD for its status. Missed polls are
/// retried next sweep, so a tight deadline is safe.
const PER_OSD_TIMEOUT: Duration = Duration::from_secs(5);

/// Spawn the drain observer. Non-blocking: returns immediately.
///
/// The task runs forever; there's no shutdown hook because the meta
/// process's lifetime is the cluster's lifetime, and a stopped task
/// only means slower auto-finalisation, not correctness loss.
pub fn spawn(meta: Arc<MetaService>) {
    tokio::spawn(async move {
        run(meta).await;
    });
    info!("Drain observer spawned (sweep every {:?})", SWEEP_INTERVAL);
}

async fn run(meta: Arc<MetaService>) {
    let mut ticker = interval(SWEEP_INTERVAL);
    // If the process was suspended (e.g., laptop sleep) we'd rather
    // resume with a single sweep than fire every missed tick and
    // hammer every OSD in a burst.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if let Err(e) = sweep_once(&meta).await {
            warn!("drain observer sweep failed: {e}");
        }
    }
}

/// One pass: list Draining OSDs, check shard counts, flip any with 0.
async fn sweep_once(meta: &Arc<MetaService>) -> anyhow::Result<()> {
    // Bail if this replica isn't the Raft leader — only the leader
    // mutates state. Having multiple replicas all issue `client_write`
    // is safe (followers return ForwardToLeader) but wasteful.
    if !meta.is_raft_leader() {
        debug!("drain observer: not leader, skipping sweep");
        return Ok(());
    }

    // Collect (node_id, address) for every Draining OSD in a snapshot
    // so we don't hold the read lock across awaits.
    let draining: Vec<([u8; 16], String)> = {
        let osds = meta.osd_nodes_read().clone();
        osds.into_iter()
            .filter(|n| n.admin_state == objectio_common::OsdAdminState::Draining)
            .map(|n| (n.node_id, n.address))
            .collect()
    };

    if draining.is_empty() {
        return Ok(());
    }

    debug!("drain observer: sweeping {} draining OSDs", draining.len());

    for (node_id, address) in draining {
        match query_shard_count(&address).await {
            Ok(shards) if shards == 0 => {
                info!(
                    "drain observer: OSD {} has 0 shards at {address}; finalising → Out",
                    hex::encode(node_id)
                );
                if let Err(e) = meta.internal_set_osd_admin_state(
                    node_id,
                    objectio_common::OsdAdminState::Out,
                    "drain-observer".into(),
                )
                .await
                {
                    warn!(
                        "drain observer: failed to flip {} → Out: {e}",
                        hex::encode(node_id)
                    );
                }
            }
            Ok(shards) => {
                debug!(
                    "drain observer: OSD {} at {address} still has {shards} shards",
                    hex::encode(node_id)
                );
            }
            Err(e) => {
                // Transient — next sweep will retry. Log at debug so
                // we don't spam when an OSD is briefly down.
                debug!(
                    "drain observer: failed to query {} at {address}: {e}",
                    hex::encode(node_id)
                );
            }
        }
    }

    Ok(())
}

/// Ask an OSD for its total shard count via its GetStatus RPC. Opens a
/// fresh channel per call — drain polling is low-frequency and it
/// avoids stale-connection issues after an OSD reboot.
async fn query_shard_count(address: &str) -> anyhow::Result<u64> {
    let uri = if address.starts_with("http") {
        address.to_string()
    } else {
        format!("http://{address}")
    };

    // Short connect timeout — a missing OSD is a drain-observer fact,
    // not an error the operator cares about.
    let channel = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        Channel::from_shared(uri.clone())?.connect(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connect timeout"))??;

    let mut client = StorageServiceClient::new(channel);
    let resp = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        client.get_status(GetStatusRequest {}),
    )
    .await
    .map_err(|_| anyhow::anyhow!("get_status timeout"))??;

    Ok(resp.into_inner().shard_count)
}
