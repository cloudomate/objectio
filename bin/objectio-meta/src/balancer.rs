//! Placement-group balancer (Phase 4a — observation).
//!
//! The balancer runs on the Raft leader and periodically evaluates
//! every placement group against a cost function. When a PG is
//! sufficiently overloaded on one of its OSDs relative to the ideal
//! target, the balancer computes a candidate replacement copyset and
//! logs the decision. **It does not yet commit moves** — executing a
//! move requires the migration mechanics landed in Phase 5 so the
//! existing shards can be copied to the new OSD set before the PG's
//! `osd_ids` are flipped. Until that path exists, logging the cost
//! function gives operators visibility into what the balancer would
//! do without risking PGs getting stuck in a `migrating_to_osd_ids`
//! state that no one consumes.
//!
//! Scheduling rules the design doc commits to, implemented here:
//!
//! - **Hysteresis**: a PG is only considered overloaded if at least
//!   one of its OSDs holds PGs at `>= 1.20 × target_count`. Prevents
//!   thrash around the target value.
//! - **Concurrent-move cap**: per tick we emit at most
//!   `max(3, osd_count / 3)` move candidates (per pool). Keeps the
//!   background I/O bounded once moves are wired to actually commit.
//! - **Leader-only**: non-leader replicas skip the tick. The leader
//!   check matches the drain observer's pattern.

use std::sync::Arc;
use std::time::Duration;

use objectio_common::NodeId;
use objectio_placement::{CopysetPool, jump_consistent_hash};
use objectio_proto::metadata::{ErasureType, PlacementGroup, PoolConfig};
use std::collections::HashMap;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::service::MetaService;

/// How often the balancer evaluates. Non-critical timer — missing a
/// tick just delays balancing; correctness is independent.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Don't treat a PG as overloaded until at least one of its OSDs
/// carries this multiple of the ideal count. `1.20` means "20% above
/// fair share" — matches the test plan's target of max/min ≤ 1.2.
const OVERLOAD_MULTIPLIER: f64 = 1.20;

/// Spawn the balancer. Non-blocking.
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

    // Per-OSD count: how many PGs currently include this OSD. Ideal
    // is pg_count * k+m / active_osds — cluster-wide fair share.
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

    // PG cost = max count across its OSDs, minus target. Positive =
    // overloaded. Using max instead of avg highlights PGs that share
    // the single hottest node, which is where real rebalance payoff is.
    let mut scored: Vec<(f64, &PlacementGroup)> = pgs
        .iter()
        .filter(|pg| pg.migrating_to_osd_ids.is_empty())
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
        .filter(|(cost, _)| *cost > 0.0)
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    // How many moves we'd emit per tick. Cap rises with cluster size
    // so small clusters don't churn and large ones can make progress.
    let per_tick_cap = active_osds.max(3) / 3;
    let candidates: Vec<_> = scored
        .into_iter()
        .filter(|(_, pg)| {
            pg.osd_ids
                .iter()
                .filter_map(|osd| <[u8; 16]>::try_from(osd.as_slice()).ok())
                .any(|arr| {
                    (*osd_counts.get(&arr).unwrap_or(&0) as f64) >= threshold
                })
        })
        .take(per_tick_cap)
        .collect();

    if candidates.is_empty() {
        debug!(
            "balancer: pool '{}' balanced (target={target:.2}, threshold={threshold:.2}, \
             pgs={}, osds={})",
            pool.name,
            pgs.len(),
            active_osds
        );
        return Ok(());
    }

    // Build a copyset pool once per evaluation pass — scoring
    // candidate replacements reuses it across every overloaded PG.
    let fd_level = parse_failure_domain(&pool.failure_domain);
    let seed = topology_seed(meta, pool);
    let cs_pool = CopysetPool::build(
        &meta.topology_snapshot(),
        fd_level,
        k_plus_m,
        10,
        seed,
    )?;

    for (cost, pg) in candidates {
        // Lower total load across candidate's OSDs = better target.
        let mut rng = rand::rngs::StdRng::from_seed(seed_bytes(seed.wrapping_add(pg.pg_id.into())));
        let target_set = cs_pool.pick_min_cost(&mut rng, |cs| {
            cs.osds
                .iter()
                .map(|id| *osd_counts.get(id.as_bytes()).unwrap_or(&0) as f64)
                .sum()
        });
        match target_set {
            Some(cs) => {
                let current: Vec<String> =
                    pg.osd_ids.iter().map(|b| hex_short(b)).collect();
                let proposed: Vec<String> =
                    cs.osds.iter().map(|n| hex_short(n.as_bytes())).collect();
                info!(
                    "balancer[dry-run]: pool={} pg_id={} cost={:.2} overloaded \
                     (target={target:.2}, threshold={threshold:.2}); proposed move \
                     {current:?} -> {proposed:?}",
                    pool.name, pg.pg_id, cost,
                );
            }
            None => {
                warn!(
                    "balancer: pool={} pg_id={} overloaded but no better copyset found \
                     (cs_pool={} sets)",
                    pool.name,
                    pg.pg_id,
                    cs_pool.sets.len()
                );
            }
        }
    }

    Ok(())
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
/// Keeps the copyset pool stable across balancer ticks that see the
/// same topology, so proposed moves don't jitter just because the RNG
/// reshuffled.
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

fn hex_short(bytes: &[u8]) -> String {
    const N: usize = 4;
    let slice = if bytes.len() > N { &bytes[..N] } else { bytes };
    hex::encode(slice)
}

use rand::SeedableRng;

// Clippy/unused — the two re-exports below are imported for symmetry
// with the migration phase and intentionally exported so Phase 5 can
// reuse them without duplicating helpers.
#[allow(dead_code)]
pub(crate) fn pg_jump_hash(object_id_hash: u64, pg_count: u32) -> u32 {
    jump_consistent_hash(object_id_hash, pg_count as i32) as u32
}
#[allow(dead_code)]
pub(crate) fn node_id_from_bytes(b: &[u8]) -> Option<NodeId> {
    <[u8; 16]>::try_from(b).ok().map(NodeId::from_bytes)
}
