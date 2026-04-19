//! Drain observer **and** migrator (Phase 3a + 3b).
//!
//! Per sweep on the Raft leader, this task:
//!
//!  1. Finds OSDs marked `admin_state = Draining`.
//!  2. Updates each one's `DrainProgress` entry (shards_remaining,
//!     initial_shards on first sight) from the OSD's `GetStatus`.
//!  3. Migrates ONE affected shard per Draining OSD per sweep — reads
//!     from the draining OSD, writes to a CRUSH-chosen target,
//!     rewrites the ObjectMeta's ShardLocation on the primary OSD,
//!     deletes the source. Bounded concurrency (one per OSD per sweep)
//!     keeps live-traffic impact small and makes the progress bar
//!     advance smoothly.
//!  4. Flips Draining → Out when the OSD's `shard_count` hits 0.
//!
//! Non-goals for this phase:
//!  - LRC / replication (only MDS EC for now).
//!  - Reading the object's own ec_k/ec_m from its ObjectMeta to
//!    recompute CRUSH with the right template. We use the meta
//!    service's `default_ec_k/m` — this is correct for the common
//!    case where every object in the cluster uses the same scheme.
//!    Phase 3c: look up the object's ec scheme from its metadata.
//!  - Crash-safe resume: progress is in-memory, lost on leader
//!    failover, reconstructed on the next sweep. Safe because
//!    migration operations are idempotent at the shard level.

use std::sync::Arc;
use std::time::Duration;

use objectio_proto::metadata::ShardLocation;
use objectio_proto::storage::{
    FindObjectsReferencingNodeRequest, GetObjectMetaRequest, GetStatusRequest,
    PutObjectMetaRequest, ReadShardRequest, ShardId, WriteShardRequest,
    storage_service_client::StorageServiceClient,
};
use tokio::time::{MissedTickBehavior, interval};
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::service::MetaService;

/// How often we sweep. Not a Raft timer — no correctness implications
/// if it runs late; it just delays the auto-flip.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Per-RPC timeout when talking to an OSD during a sweep.
const PER_OSD_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap the migrator to one shard per Draining OSD per sweep. Low
/// enough to keep live IO unaffected on a small cluster; Phase 3c
/// can lift this into a config once we have rate-limiter plumbing.
const SHARDS_PER_SWEEP: usize = 1;

/// Spawn the drain observer. Non-blocking; returns immediately.
pub fn spawn(meta: Arc<MetaService>) {
    tokio::spawn(async move {
        run(meta).await;
    });
    info!("Drain observer spawned (sweep every {:?})", SWEEP_INTERVAL);
}

async fn run(meta: Arc<MetaService>) {
    let mut ticker = interval(SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Cluster-wide scan cursor for the rebalancer — one per OSD,
    // advanced page by page across sweeps so a large cluster doesn't
    // scan the full meta store on every tick. Lost on restart; the
    // next leader starts from the top (safe, just repeats work).
    let mut rebalance_cursors: std::collections::HashMap<[u8; 16], String> =
        std::collections::HashMap::new();
    loop {
        ticker.tick().await;
        if let Err(e) = sweep_once(&meta).await {
            warn!("drain observer sweep failed: {e}");
        }
        if let Err(e) = rebalance_sweep(&meta, &mut rebalance_cursors).await {
            warn!("rebalance sweep failed: {e}");
        }
    }
}

async fn sweep_once(meta: &Arc<MetaService>) -> anyhow::Result<()> {
    if !meta.is_raft_leader() {
        debug!("drain observer: not leader, skipping sweep");
        return Ok(());
    }

    let draining: Vec<([u8; 16], String)> = {
        let osds = meta.osd_nodes_read().clone();
        osds.into_iter()
            .filter(|n| n.admin_state == objectio_common::OsdAdminState::Draining)
            .map(|n| (n.node_id, n.address))
            .collect()
    };

    // Clean up stale progress entries for OSDs that are no longer
    // Draining (flipped back to In or removed). Keeps /_admin/drain-status
    // honest.
    {
        let draining_ids: std::collections::HashSet<[u8; 16]> =
            draining.iter().map(|(id, _)| *id).collect();
        let existing: Vec<[u8; 16]> =
            meta.drain_statuses_snapshot().keys().copied().collect();
        for id in existing {
            if !draining_ids.contains(&id) {
                meta.clear_drain_progress(&id);
            }
        }
    }

    if draining.is_empty() {
        return Ok(());
    }

    debug!("drain observer: sweeping {} draining OSDs", draining.len());

    for (node_id, address) in draining {
        // Always update shard_count first — the auto-finalize check
        // depends on it. Progress mirrors the observed count so the
        // console shows "X of Y migrated" even when migration stalls.
        let shards = match query_shard_count(&address).await {
            Ok(s) => {
                meta.update_drain_progress(node_id, |p| {
                    if p.initial_shards == 0 {
                        p.initial_shards = s;
                    }
                    p.shards_remaining = s;
                    p.updated_at = now_unix();
                    p.last_error.clear();
                });
                s
            }
            Err(e) => {
                // OSD offline → can't sweep. Record but don't abort
                // other OSDs in this pass.
                meta.update_drain_progress(node_id, |p| {
                    p.last_error = format!("osd unreachable: {e}");
                    p.updated_at = now_unix();
                });
                continue;
            }
        };

        // Auto-finalise when empty. We do this BEFORE attempting to
        // migrate anything — if shard_count is already zero, nothing
        // to migrate.
        if shards == 0 {
            info!(
                "drain observer: OSD {} has 0 shards at {address}; finalising → Out",
                hex::encode(node_id)
            );
            match meta
                .internal_set_osd_admin_state(
                    node_id,
                    objectio_common::OsdAdminState::Out,
                    "drain-observer".into(),
                )
                .await
            {
                Ok(()) => meta.clear_drain_progress(&node_id),
                Err(e) => warn!(
                    "drain observer: failed to flip {} → Out: {e}",
                    hex::encode(node_id)
                ),
            }
            continue;
        }

        // Shards remain — migrate one per sweep.
        if let Err(e) =
            migrate_one_shard(meta, node_id, &address, SHARDS_PER_SWEEP).await
        {
            warn!(
                "drain observer: migration step for {} failed: {e}",
                hex::encode(node_id)
            );
            meta.update_drain_progress(node_id, |p| {
                p.last_error = format!("migrate: {e}");
                p.updated_at = now_unix();
            });
        }
    }

    Ok(())
}

/// Ask one OSD for its shard count via GetStatus. Opens a fresh
/// channel per call — drain polling is low-frequency and it avoids
/// stale-connection issues after an OSD reboot.
async fn query_shard_count(address: &str) -> anyhow::Result<u64> {
    let uri = canonical_uri(address);
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

/// Migrate up to `batch` shards off the draining OSD.
///
/// Strategy:
///
///   a. Fan out `FindObjectsReferencingNode(draining_node_id)` to every
///      OSD in the cluster. Each OSD scans its own primary-held
///      ObjectMetas and returns the ones whose any stripe has a
///      ShardLocation on the draining node. The output names the OSD
///      that owns the meta (implicitly: whichever OSD answered with
///      that object) so we know where to PutObjectMeta later.
///   b. Pick up to `batch` (object, shard) pairs and migrate each:
///      1. Read shard bytes from draining OSD.
///      2. Pick a CRUSH target excluding the draining OSD.
///      3. Write to the target.
///      4. Update ObjectMeta on the primary OSD (the one that
///         returned this object in step a).
///      5. Delete the source shard. Idempotent — if we crash between
///         steps 3 and 4, a later sweep's step a still finds the same
///         ObjectMeta (unchanged) so retry continues.
async fn migrate_one_shard(
    meta: &Arc<MetaService>,
    draining: [u8; 16],
    draining_addr: &str,
    batch: usize,
) -> anyhow::Result<()> {
    if batch == 0 {
        return Ok(());
    }

    // Step a — fan out the search. Record which OSD owns each
    // returned object so step 4 can update meta on the right node.
    // Each (owner_addr, AffectedObject) stays distinct.
    let owners = meta.all_osd_addresses();
    let mut candidates: Vec<(String, objectio_proto::storage::AffectedObject)> =
        Vec::new();
    for (addr, _id) in &owners {
        match find_affected_objects(addr, &draining, batch as u32 * 4).await {
            Ok(objs) => {
                for o in objs {
                    candidates.push((addr.clone(), o));
                    if candidates.len() >= batch * 4 {
                        break;
                    }
                }
            }
            Err(e) => {
                debug!(
                    "drain migrator: find_affected on {addr} failed: {e} (ignoring)"
                );
            }
        }
    }

    if candidates.is_empty() {
        // No ObjectMeta references this OSD, yet shard_count > 0 —
        // possible if the OSD holds orphaned shards whose ObjectMeta
        // has already been deleted. Phase 3c handles orphan cleanup;
        // for now log and let the operator see a stalled count.
        debug!(
            "drain migrator: OSD {} reports shards but no ObjectMeta references it (orphans?)",
            hex::encode(draining)
        );
        return Ok(());
    }

    // Step b — migrate up to `batch` shards from the candidates list.
    // Flatten into per-shard work items.
    struct WorkItem {
        owner_addr: String,
        bucket: String,
        key: String,
        object_id: [u8; 16],
        stripe_id: u64,
        position: u32,
    }
    let mut work: Vec<WorkItem> = Vec::new();
    for (owner_addr, obj) in candidates {
        let Ok(object_id): Result<[u8; 16], _> = obj.object_id.as_slice().try_into()
        else {
            continue;
        };
        for s in obj.shards {
            work.push(WorkItem {
                owner_addr: owner_addr.clone(),
                bucket: obj.bucket.clone(),
                key: obj.key.clone(),
                object_id,
                stripe_id: s.stripe_id,
                position: s.position,
            });
            if work.len() >= batch {
                break;
            }
        }
        if work.len() >= batch {
            break;
        }
    }

    for item in &work {
        let borrowed = WorkItemRef {
            owner_addr: &item.owner_addr,
            bucket: &item.bucket,
            key: &item.key,
            object_id: item.object_id,
            stripe_id: item.stripe_id,
            position: item.position,
        };
        match migrate_shard_one(meta, &draining, draining_addr, &borrowed).await {
            Ok(()) => {
                meta.update_drain_progress(draining, |p| {
                    p.shards_migrated = p.shards_migrated.saturating_add(1);
                    p.updated_at = now_unix();
                    p.last_error.clear();
                });
                info!(
                    "drain migrator: moved {}/{} shard stripe={} pos={} off {}",
                    item.bucket,
                    item.key,
                    item.stripe_id,
                    item.position,
                    hex::encode(draining)
                );
            }
            Err(e) => {
                meta.update_drain_progress(draining, |p| {
                    p.last_error = format!(
                        "{}/{} stripe={} pos={}: {e}",
                        item.bucket, item.key, item.stripe_id, item.position
                    );
                    p.updated_at = now_unix();
                });
                warn!(
                    "drain migrator: failed to move {}/{} stripe={} pos={}: {e}",
                    item.bucket, item.key, item.stripe_id, item.position
                );
            }
        }
    }

    Ok(())
}

/// Drive one shard through read-target / write-target / update-meta /
/// delete-source. Fails fast on any step — the caller retries on the
/// next sweep.
async fn migrate_shard_one(
    meta: &Arc<MetaService>,
    draining: &[u8; 16],
    draining_addr: &str,
    item: &WorkItemRef<'_>,
) -> anyhow::Result<()> {
    // 1. Pick target via CRUSH (excludes draining OSDs by construction).
    let target_node = meta
        .pick_migration_target(&item.object_id, item.position, draining)
        .ok_or_else(|| anyhow::anyhow!("no CRUSH target available"))?;
    let target_addr = meta
        .osd_address_by_id(&target_node)
        .ok_or_else(|| anyhow::anyhow!("target not registered"))?;
    if target_addr == draining_addr {
        return Err(anyhow::anyhow!("CRUSH returned the draining node itself"));
    }

    // 2. Read shard from source.
    let shard_id = ShardId {
        object_id: item.object_id.to_vec(),
        stripe_id: item.stripe_id,
        position: item.position,
    };
    let source_ch = open_channel(draining_addr).await?;
    let mut source = StorageServiceClient::new(source_ch);
    let bytes = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        source.read_shard(ReadShardRequest {
            shard_id: Some(shard_id.clone()),
            offset: 0,
            length: 0,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("read_shard timeout"))??
    .into_inner()
    .data;

    // 3. Write to target.
    let target_ch = open_channel(&target_addr).await?;
    let mut target = StorageServiceClient::new(target_ch);
    let write_resp = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        target.write_shard(WriteShardRequest {
            shard_id: Some(shard_id.clone()),
            data: bytes,
            ec_k: 0, // Not inspected by OSD; kept for wire-compat.
            ec_m: 0,
            // Shard was already checksummed when first written; the
            // OSD recomputes on its side to validate the stored bytes.
            // Supplying None tells the OSD to skip the optional
            // client-provided check.
            checksum: None,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("write_shard timeout"))??
    .into_inner();

    // 4. Update ObjectMeta on the primary (the OSD that returned this
    //    object in step a — `owner_addr`).
    let owner_ch = open_channel(&item.owner_addr).await?;
    let mut owner = StorageServiceClient::new(owner_ch);
    let Some(mut object) = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        owner.get_object_meta(GetObjectMetaRequest {
            bucket: item.bucket.to_string(),
            key: item.key.to_string(),
            version_id: String::new(),
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("get_object_meta timeout"))??
    .into_inner()
    .object
    else {
        return Err(anyhow::anyhow!(
            "owner no longer has ObjectMeta {}/{}",
            item.bucket,
            item.key
        ));
    };

    let mut updated = false;
    for stripe in &mut object.stripes {
        if stripe.stripe_id != item.stripe_id {
            continue;
        }
        for shard in &mut stripe.shards {
            if shard.position == item.position && shard.node_id == draining.as_slice()
            {
                let target_disk = write_resp
                    .location
                    .as_ref()
                    .map(|l| l.disk_id.clone())
                    .unwrap_or_default();
                *shard = ShardLocation {
                    position: shard.position,
                    node_id: target_node.to_vec(),
                    disk_id: target_disk,
                    offset: 0,
                    shard_type: shard.shard_type,
                    local_group: shard.local_group,
                };
                updated = true;
            }
        }
    }

    if !updated {
        // Meta already re-pointed by an earlier sweep (crash-safety
        // path). Treat as success — the source shard delete below
        // still needs to run.
        debug!(
            "drain migrator: {}/{} stripe={} pos={} already updated, proceeding to delete source",
            item.bucket, item.key, item.stripe_id, item.position
        );
    }

    tokio::time::timeout(
        PER_OSD_TIMEOUT,
        owner.put_object_meta(PutObjectMetaRequest {
            bucket: item.bucket.to_string(),
            key: item.key.to_string(),
            object: Some(object),
            versioning_enabled: false,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("put_object_meta timeout"))??;

    // 5. Delete source shard. The OSD's shard_count drops on the next
    //    GetStatus sweep and the observer can eventually auto-finalise.
    tokio::time::timeout(
        PER_OSD_TIMEOUT,
        source.delete_shard(objectio_proto::storage::DeleteShardRequest {
            shard_id: Some(shard_id),
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("delete_shard timeout"))??;

    Ok(())
}

/// Borrowed form of WorkItem so migrate_shard_one doesn't take
/// ownership of the candidate list.
struct WorkItemRef<'a> {
    owner_addr: &'a str,
    bucket: &'a str,
    key: &'a str,
    object_id: [u8; 16],
    stripe_id: u64,
    position: u32,
}

impl<'a> WorkItemRef<'a> {
    #[allow(dead_code)] // constructed inline in migrate_shard_one
    fn new(
        owner_addr: &'a str,
        bucket: &'a str,
        key: &'a str,
        object_id: [u8; 16],
        stripe_id: u64,
        position: u32,
    ) -> Self {
        Self {
            owner_addr,
            bucket,
            key,
            object_id,
            stripe_id,
            position,
        }
    }
}

async fn find_affected_objects(
    addr: &str,
    draining: &[u8; 16],
    limit: u32,
) -> anyhow::Result<Vec<objectio_proto::storage::AffectedObject>> {
    let channel = open_channel(addr).await?;
    let mut client = StorageServiceClient::new(channel);
    let resp = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        client.find_objects_referencing_node(FindObjectsReferencingNodeRequest {
            draining_node_id: draining.to_vec(),
            limit,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("find_objects timeout"))??;
    Ok(resp.into_inner().objects)
}

async fn open_channel(address: &str) -> anyhow::Result<Channel> {
    let uri = canonical_uri(address);
    let channel = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        Channel::from_shared(uri)?.connect(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connect timeout"))??;
    Ok(channel)
}

fn canonical_uri(address: &str) -> String {
    if address.starts_with("http") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------
// Continuous auto-rebalancer
// ---------------------------------------------------------------------

/// One page size per sweep per OSD. Small because the migrator itself
/// is rate-limited to `SHARDS_PER_SWEEP` anyway — more candidates per
/// page than we can actually move is just wasted scan work.
const REBALANCE_PAGE_SIZE: u32 = 64;

/// One rebalance pass over the cluster. Per sweep, per OSD, we:
///   1. ListObjectsMeta (cluster-wide — bucket="") from the cursor we
///      remembered last time.
///   2. For each returned ObjectMeta, compute the CRUSH ideal placement
///      for each stripe, and compare every ShardLocation.node_id to
///      the ideal. Any mismatch is a drift candidate.
///   3. Migrate at most one drift per OSD per sweep (matches drain's
///      rate limit). Subsequent sweeps advance the cursor and continue.
/// When the cursor exhausts, we clear it so the next sweep starts
/// over — this is how new drifts caused by ongoing topology changes
/// eventually get picked up.
async fn rebalance_sweep(
    meta: &Arc<MetaService>,
    cursors: &mut std::collections::HashMap<[u8; 16], String>,
) -> anyhow::Result<()> {
    if !meta.is_raft_leader() {
        return Ok(());
    }
    if meta.is_rebalance_paused() {
        meta.update_rebalance_progress(|p| p.paused = true);
        return Ok(());
    }

    // Skip rebalance if any OSD is actively Draining — drain already
    // moves the big stuff, and running both in parallel doubles the IO
    // impact. Drain finishes fast enough that a 30-s delay on
    // rebalance is fine.
    let any_draining = meta
        .osd_nodes_read()
        .iter()
        .any(|n| n.admin_state == objectio_common::OsdAdminState::Draining);
    if any_draining {
        debug!("rebalance sweep: deferring — drain in progress");
        return Ok(());
    }

    let owners = meta.all_osd_addresses();
    if owners.is_empty() {
        return Ok(());
    }

    let mut scanned: u64 = 0;
    let mut drifts_seen: u64 = 0;
    let mut migrated_this_sweep = false;
    let draining_ids: std::collections::HashSet<[u8; 16]> = meta
        .osd_nodes_read()
        .iter()
        .filter(|n| n.admin_state != objectio_common::OsdAdminState::In)
        .map(|n| n.node_id)
        .collect();

    for (addr, node_id) in &owners {
        let cursor = cursors.get(node_id).cloned().unwrap_or_default();
        let (objects, next_cursor) =
            match list_all_object_metas(addr, &cursor, REBALANCE_PAGE_SIZE).await {
                Ok(r) => r,
                Err(e) => {
                    debug!(
                        "rebalance: list on {addr} failed: {e} (will retry next sweep)"
                    );
                    continue;
                }
            };
        scanned = scanned.saturating_add(objects.len() as u64);

        // Advance or reset the cursor. When the OSD reports no next
        // token, we've finished the scan on this OSD — clear to restart
        // from the top next time.
        if next_cursor.is_empty() {
            cursors.remove(node_id);
        } else {
            cursors.insert(*node_id, next_cursor);
        }

        // Scan each returned ObjectMeta for drift. Migrate the FIRST
        // one we find (per OSD, per sweep) and let the next sweep pick
        // up subsequent drifts.
        for object in objects {
            // Parse the stored object_id into a [u8; 16] for CRUSH.
            let Ok(object_id): Result<[u8; 16], _> =
                object.object_id.as_slice().try_into()
            else {
                continue;
            };

            for stripe in &object.stripes {
                for shard in &stripe.shards {
                    if shard.node_id.len() != 16 {
                        continue;
                    }
                    let mut current: [u8; 16] = [0; 16];
                    current.copy_from_slice(&shard.node_id);

                    // Shards whose current owner is not `In` ARE drifts
                    // (they shouldn't serve placement anymore), but we
                    // defer those to the drain migrator. So skip.
                    if draining_ids.contains(&current) {
                        continue;
                    }

                    let Some(ideal) = meta
                        .pick_migration_target(&object_id, shard.position, &current)
                    else {
                        continue;
                    };

                    // If CRUSH's pick is the current owner, no drift.
                    // pick_migration_target returns "anything but
                    // excluded", so ideal will equal current when the
                    // placement is already correct — treat as no drift.
                    if ideal == current {
                        continue;
                    }

                    drifts_seen = drifts_seen.saturating_add(1);

                    if migrated_this_sweep {
                        continue;
                    }

                    // One migration per sweep. Use the same primitive
                    // as drain; the only difference is the "target"
                    // comes from the rebalancer instead of the
                    // draining-node filter.
                    if let Err(e) = migrate_rebalance_one(
                        meta,
                        &current,
                        addr,
                        &object,
                        stripe.stripe_id,
                        shard.position,
                    )
                    .await
                    {
                        meta.update_rebalance_progress(|p| {
                            p.last_error = format!("{}: {e}", object.key);
                            p.last_sweep_at = now_unix();
                        });
                    } else {
                        info!(
                            "rebalance: moved {}/{} stripe={} pos={} → {}",
                            object.bucket,
                            object.key,
                            stripe.stripe_id,
                            shard.position,
                            hex::encode(ideal),
                        );
                        meta.update_rebalance_progress(|p| {
                            p.shards_rebalanced_total =
                                p.shards_rebalanced_total.saturating_add(1);
                            p.last_error.clear();
                        });
                        migrated_this_sweep = true;
                    }
                }
            }
        }
    }

    meta.update_rebalance_progress(|p| {
        p.started = true;
        p.paused = false;
        p.last_sweep_at = now_unix();
        p.scanned_this_pass = p.scanned_this_pass.saturating_add(scanned);
        p.drifts_seen_this_pass =
            p.drifts_seen_this_pass.saturating_add(drifts_seen);
        // Reset pass counters when every cursor is exhausted (we
        // finished a full loop over the cluster).
        if cursors.is_empty() {
            p.scanned_this_pass = 0;
            p.drifts_seen_this_pass = 0;
        }
    });

    Ok(())
}

/// One drift migration. Same steps as `migrate_shard_one` (read, write,
/// rewrite ObjectMeta, delete source) but the object is already in
/// hand so we skip the find-affected phase.
async fn migrate_rebalance_one(
    meta: &Arc<MetaService>,
    current_owner: &[u8; 16],
    current_owner_meta_addr: &str, // OSD that holds the ObjectMeta
    object: &objectio_proto::metadata::ObjectMeta,
    stripe_id: u64,
    position: u32,
) -> anyhow::Result<()> {
    let Ok(object_id): Result<[u8; 16], _> =
        object.object_id.as_slice().try_into()
    else {
        return Err(anyhow::anyhow!("object_id len != 16"));
    };

    let target_node = meta
        .pick_migration_target(&object_id, position, current_owner)
        .ok_or_else(|| anyhow::anyhow!("no CRUSH target"))?;
    if &target_node == current_owner {
        return Ok(()); // No-op
    }

    let source_addr = meta
        .osd_address_by_id(current_owner)
        .ok_or_else(|| anyhow::anyhow!("current owner not registered"))?;
    let target_addr = meta
        .osd_address_by_id(&target_node)
        .ok_or_else(|| anyhow::anyhow!("target not registered"))?;

    let shard_id = ShardId {
        object_id: object_id.to_vec(),
        stripe_id,
        position,
    };

    // 1. Read from current owner.
    let source_ch = open_channel(&source_addr).await?;
    let mut source = StorageServiceClient::new(source_ch);
    let bytes = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        source.read_shard(ReadShardRequest {
            shard_id: Some(shard_id.clone()),
            offset: 0,
            length: 0,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("read_shard timeout"))??
    .into_inner()
    .data;

    // 2. Write to target.
    let target_ch = open_channel(&target_addr).await?;
    let mut target = StorageServiceClient::new(target_ch);
    let write_resp = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        target.write_shard(WriteShardRequest {
            shard_id: Some(shard_id.clone()),
            data: bytes,
            ec_k: 0,
            ec_m: 0,
            checksum: None,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("write_shard timeout"))??
    .into_inner();

    // 3. Rewrite the ObjectMeta on the owner OSD (the one holding the
    //    primary-stored meta — same OSD we listed it from).
    let owner_ch = open_channel(current_owner_meta_addr).await?;
    let mut owner = StorageServiceClient::new(owner_ch);
    let Some(mut fresh) = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        owner.get_object_meta(GetObjectMetaRequest {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version_id: String::new(),
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("get_object_meta timeout"))??
    .into_inner()
    .object
    else {
        return Err(anyhow::anyhow!(
            "owner no longer has ObjectMeta {}/{}",
            object.bucket,
            object.key
        ));
    };

    for stripe in &mut fresh.stripes {
        if stripe.stripe_id != stripe_id {
            continue;
        }
        for shard in &mut stripe.shards {
            if shard.position == position && shard.node_id == current_owner.as_slice()
            {
                let target_disk = write_resp
                    .location
                    .as_ref()
                    .map(|l| l.disk_id.clone())
                    .unwrap_or_default();
                *shard = ShardLocation {
                    position: shard.position,
                    node_id: target_node.to_vec(),
                    disk_id: target_disk,
                    offset: 0,
                    shard_type: shard.shard_type,
                    local_group: shard.local_group,
                };
            }
        }
    }

    tokio::time::timeout(
        PER_OSD_TIMEOUT,
        owner.put_object_meta(PutObjectMetaRequest {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            object: Some(fresh),
            versioning_enabled: false,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("put_object_meta timeout"))??;

    // 4. Delete source shard.
    tokio::time::timeout(
        PER_OSD_TIMEOUT,
        source.delete_shard(objectio_proto::storage::DeleteShardRequest {
            shard_id: Some(shard_id),
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("delete_shard timeout"))??;

    Ok(())
}

/// Page through every primary-held ObjectMeta on an OSD. Returns
/// (objects, next_cursor) — empty cursor means the scan is complete.
async fn list_all_object_metas(
    addr: &str,
    cursor: &str,
    page_size: u32,
) -> anyhow::Result<(Vec<objectio_proto::metadata::ObjectMeta>, String)> {
    use objectio_proto::storage::ListObjectsMetaRequest;
    let channel = open_channel(addr).await?;
    let mut client = StorageServiceClient::new(channel);
    let resp = tokio::time::timeout(
        PER_OSD_TIMEOUT,
        client.list_objects_meta(ListObjectsMetaRequest {
            bucket: String::new(), // empty = cluster-wide scan
            prefix: String::new(),
            start_after: String::new(),
            max_keys: page_size,
            continuation_token: cursor.to_string(),
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("list_objects_meta timeout"))??
    .into_inner();
    Ok((resp.objects, resp.next_continuation_token))
}
