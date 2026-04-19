//! Placement-group balancer.
//!
//! The balancer runs on the Raft leader and periodically evaluates
//! every placement group against a cost function. When a PG is
//! overloaded on one of its OSDs relative to the ideal target, the
//! balancer picks a better copyset from the precomputed pool and
//! commits the new `osd_ids` via MultiCas.
//!
//! # Greenfield mode
//!
//! ObjectIO has no production data to preserve, so moves commit
//! **directly** — `osd_ids = new_set`, `version += 1`, no
//! `migrating_to_osd_ids` dance and no per-object shard copy. Any
//! shards left on OSDs that dropped out of a PG are orphans; the
//! terminal-loss GC in `drain_observer.rs` collects them. This is
//! intentional and recorded in project memory; if production data
//! lands, revisit before any further balancer runs.
//!
//! # Scheduling rules
//!
//! - **Hysteresis (threshold)**: a PG is only a candidate if at least
//!   one of its OSDs carries `>= OVERLOAD_MULTIPLIER × target` PGs.
//!   Prevents flipping around the fair-share line.
//! - **Hysteresis (improvement)**: a move only commits when the
//!   candidate copyset's total load is at least `IMPROVEMENT_FACTOR`
//!   better than the PG's current total load. Stops oscillation when
//!   two candidates are near-equal.
//! - **Concurrent-move cap**: `max(3, osd_count / 3)` moves per pool
//!   per tick. Bounds background state churn.
//! - **Leader-only**: non-leader replicas skip the tick.

use std::sync::Arc;
use std::time::Duration;

use objectio_placement::CopysetPool;
use objectio_proto::metadata::{ErasureType, PlacementGroup, PoolConfig};
use prost::Message;
use rand::SeedableRng;
use std::collections::HashMap;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::service::MetaService;

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const OVERLOAD_MULTIPLIER: f64 = 1.20;
/// A candidate copyset must be at least this much better (lower
/// total load) than the PG's current one for the move to commit.
/// 0.90 = ≥10% improvement required.
const IMPROVEMENT_FACTOR: f64 = 0.90;

pub fn spawn(meta: Arc<MetaService>) {
    tokio::spawn(async move {
        run(meta).await;
    });
    info!("PG balancer spawned (tick every {:?})", SWEEP_INTERVAL);
}

async fn run(meta: Arc<MetaService>) {
    let mut ticker = interval(SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if let Err(e) = sweep_once(&meta).await {
            warn!("balancer sweep failed: {e}");
        }
    }
}

async fn sweep_once(meta: &Arc<MetaService>) -> anyhow::Result<()> {
    if !meta.is_raft_leader() {
        debug!("balancer: not leader, skipping tick");
        return Ok(());
    }

    let topology = meta.topology_snapshot();
    let active_osds: usize = topology.active_nodes().count();
    if active_osds == 0 {
        return Ok(());
    }

    for pool in meta.pools_snapshot() {
        if pool.pg_count == 0 {
            continue;
        }
        if let Err(e) = evaluate_pool(meta, &pool, active_osds).await {
            warn!("balancer: pool '{}' evaluation failed: {e}", pool.name);
        }
    }
    Ok(())
}

fn copy_count(pool: &PoolConfig) -> usize {
    match pool.ec_type() {
        ErasureType::ErasureMds => (pool.ec_k + pool.ec_m) as usize,
        ErasureType::ErasureLrc => {
            (pool.ec_k + pool.ec_local_parity + pool.ec_global_parity) as usize
        }
        ErasureType::ErasureReplication => pool.replication_count as usize,
    }
}

async fn evaluate_pool(
    meta: &Arc<MetaService>,
    pool: &PoolConfig,
    active_osds: usize,
) -> anyhow::Result<()> {
    let pgs = meta.placement_groups_for_pool(&pool.name);
    if pgs.is_empty() {
        return Ok(());
    }

    let k_plus_m = copy_count(pool);
    if k_plus_m == 0 {
        return Ok(());
    }

    // Live per-OSD PG-membership counts — updated in-place as moves
    // commit in this tick so later PGs see the post-move state
    // instead of over-moving onto the same OSD.
    let mut osd_counts: HashMap<[u8; 16], usize> = HashMap::new();
    for pg in &pgs {
        for osd in &pg.osd_ids {
            if let Ok(arr) = <[u8; 16]>::try_from(osd.as_slice()) {
                *osd_counts.entry(arr).or_insert(0) += 1;
            }
        }
    }

    let total_slots = pgs.len() * k_plus_m;
    let target = (total_slots as f64) / (active_osds as f64);
    let threshold = target * OVERLOAD_MULTIPLIER;

    // Build the copyset pool once per pass — scoring candidate
    // replacements reuses it across every overloaded PG.
    let fd_level = parse_failure_domain(&pool.failure_domain);
    let seed = topology_seed(meta, pool);
    let cs_pool = CopysetPool::build(
        &meta.topology_snapshot(),
        fd_level,
        k_plus_m,
        10,
        seed,
    )?;
    if cs_pool.sets.is_empty() {
        warn!("balancer: pool '{}' has no feasible copysets", pool.name);
        return Ok(());
    }

    let per_tick_cap = active_osds.max(3) / 3;

    // Score PGs: cost = max count across its OSDs - target. Filter
    // by threshold. Sort hottest first.
    let mut scored: Vec<(f64, PlacementGroup)> = pgs
        .into_iter()
        .map(|pg| {
            let max_count = pg
                .osd_ids
                .iter()
                .filter_map(|osd| <[u8; 16]>::try_from(osd.as_slice()).ok())
                .map(|arr| *osd_counts.get(&arr).unwrap_or(&0))
                .max()
                .unwrap_or(0);
            ((max_count as f64) - target, pg)
        })
        .filter(|(_, pg)| {
            pg.osd_ids
                .iter()
                .filter_map(|osd| <[u8; 16]>::try_from(osd.as_slice()).ok())
                .any(|arr| (*osd_counts.get(&arr).unwrap_or(&0) as f64) >= threshold)
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    let mut committed = 0usize;
    for (cost, pg) in scored {
        if committed >= per_tick_cap {
            break;
        }

        let current_load: f64 = pg
            .osd_ids
            .iter()
            .filter_map(|osd| <[u8; 16]>::try_from(osd.as_slice()).ok())
            .map(|arr| *osd_counts.get(&arr).unwrap_or(&0) as f64)
            .sum();

        let mut rng =
            rand::rngs::StdRng::from_seed(seed_bytes(seed.wrapping_add(pg.pg_id.into())));
        let best_cost_cell = std::cell::Cell::new(f64::INFINITY);
        let target_set = cs_pool.pick_min_cost(&mut rng, |cs| {
            let c: f64 = cs
                .osds
                .iter()
                .map(|id| *osd_counts.get(id.as_bytes()).unwrap_or(&0) as f64)
                .sum();
            if c < best_cost_cell.get() {
                best_cost_cell.set(c);
            }
            c
        });
        let Some(cs) = target_set else {
            warn!(
                "balancer: pool={} pg_id={} no candidate copyset",
                pool.name, pg.pg_id
            );
            continue;
        };
        let best_cost = best_cost_cell.get();

        // Improvement gate: require best candidate ≤ 90% of current.
        if best_cost >= current_load * IMPROVEMENT_FACTOR {
            debug!(
                "balancer: pool={} pg_id={} improvement {}→{} below {}×; skip",
                pool.name, pg.pg_id, current_load, best_cost, IMPROVEMENT_FACTOR
            );
            continue;
        }

        // Commit the move — greenfield so we rewrite osd_ids in place.
        let old_bytes = pg.encode_to_vec();
        let new_pg = PlacementGroup {
            osd_ids: cs.osds.iter().map(|n| n.as_bytes().to_vec()).collect(),
            version: pg.version.wrapping_add(1),
            updated_at: now_unix(),
            ..pg.clone()
        };
        let new_bytes = new_pg.encode_to_vec();

        match commit_pg(meta, &pg, old_bytes, new_bytes).await {
            Ok(()) => {
                info!(
                    "balancer: moved pool={} pg_id={} cost={:.2} load {}→{} v{}→v{}",
                    pool.name,
                    pg.pg_id,
                    cost,
                    current_load,
                    best_cost,
                    pg.version,
                    new_pg.version
                );
                // Update in-memory osd_counts so the next PG in this
                // tick sees the post-commit state.
                for osd in &pg.osd_ids {
                    if let Ok(arr) = <[u8; 16]>::try_from(osd.as_slice())
                        && let Some(c) = osd_counts.get_mut(&arr)
                    {
                        *c = c.saturating_sub(1);
                    }
                }
                for n in &cs.osds {
                    *osd_counts.entry(*n.as_bytes()).or_insert(0) += 1;
                }
                committed += 1;
            }
            Err(e) => {
                warn!(
                    "balancer: pool={} pg_id={} commit failed: {e}",
                    pool.name, pg.pg_id
                );
            }
        }
    }

    if committed > 0 {
        info!(
            "balancer: pool '{}' committed {}/{} moves (target={:.2}, threshold={:.2})",
            pool.name, committed, per_tick_cap, target, threshold
        );
    }
    Ok(())
}

async fn commit_pg(
    meta: &Arc<MetaService>,
    pg: &PlacementGroup,
    old_bytes: Vec<u8>,
    new_bytes: Vec<u8>,
) -> anyhow::Result<()> {
    use objectio_meta_store::{CasOp, CasTable, MetaCommand, MetaResponse, MetaStore};

    let Some(raft) = meta.raft_handle() else {
        return Err(anyhow::anyhow!("no raft handle — cannot commit PG move"));
    };
    let cmd = MetaCommand::MultiCas {
        ops: vec![CasOp {
            table: CasTable::PlacementGroups,
            key: MetaStore::pg_key(&pg.pool, pg.pg_id),
            expected: Some(old_bytes),
            new_value: Some(new_bytes),
        }],
        requested_by: "balancer".into(),
    };
    match raft.client_write(cmd).await {
        Ok(r) => match r.data {
            MetaResponse::MultiCasOk => Ok(()),
            MetaResponse::MultiCasConflict { .. } => {
                Err(anyhow::anyhow!("pg changed concurrently — skip this tick"))
            }
            other => Err(anyhow::anyhow!("unexpected raft response: {other:?}")),
        },
        Err(e) => Err(anyhow::anyhow!("raft write failed: {e}")),
    }
}

fn parse_failure_domain(s: &str) -> objectio_common::FailureDomain {
    use objectio_common::FailureDomain as Fd;
    match s {
        "node" => Fd::Node,
        "rack" => Fd::Rack,
        "datacenter" => Fd::Datacenter,
        "zone" => Fd::Zone,
        "region" => Fd::Region,
        "disk" => Fd::Disk,
        _ => Fd::Host,
    }
}

/// Deterministic per-pool seed: topology version × pool name hash.
/// Keeps the copyset pool stable across ticks that see the same
/// topology, so the balancer doesn't jitter between near-equal
/// candidates.
fn topology_seed(meta: &Arc<MetaService>, pool: &PoolConfig) -> u64 {
    let v = meta.topology_snapshot().version;
    let name_hash = xxhash_rust::xxh64::xxh64(pool.name.as_bytes(), 0);
    v.wrapping_mul(1_000_003).wrapping_add(name_hash)
}

fn seed_bytes(seed: u64) -> [u8; 32] {
    let mut b = [0u8; 32];
    b[..8].copy_from_slice(&seed.to_le_bytes());
    b[8..16].copy_from_slice(&seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes());
    b[16..24].copy_from_slice(&seed.wrapping_add(0xbf58_476d_1ce4_e5b9).to_le_bytes());
    b[24..32].copy_from_slice(&seed.wrapping_mul(0x94d0_49bb_1331_11eb).to_le_bytes());
    b
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
