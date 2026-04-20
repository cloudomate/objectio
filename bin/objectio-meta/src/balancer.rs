//! Placement-group balancer.
//!
//! The balancer runs on the Raft leader and periodically evaluates
//! every placement group against a cost function. When a PG is
//! overloaded on one of its OSDs relative to the ideal target, or
//! an OSD is underloaded, the balancer picks a better copyset from
//! the precomputed pool and commits the new `osd_ids` via MultiCas.
//!
//! # Greenfield mode
//!
//! ObjectIO has no production data to preserve, so moves commit
//! **directly** — `osd_ids = new_set`, `version += 1`, no
//! `migrating_to_osd_ids` dance and no per-object shard copy. Any
//! shards left on OSDs that dropped out of a PG are orphans; the
//! terminal-loss GC in `drain_observer.rs` collects them.
//!
//! # Tuning knobs (config keys, all optional)
//!
//! Every tick re-reads these from the Meta `config` table, so they
//! hot-swap without a restart:
//!
//! - `balancer/sweep_interval_seconds` — how often the balancer
//!   evaluates. Default 60. Non-critical timer.
//! - `balancer/overload_multiplier` — a PG is only a candidate if at
//!   least one of its OSDs carries `>= multiplier × target` PGs.
//!   Default 1.20. Lower values = more aggressive rebalance.
//! - `balancer/underload_multiplier` — also flag a PG as a candidate
//!   if at least one of its OSDs carries `<= multiplier × target`
//!   PGs AND the PG has another OSD above average. Default 0.0
//!   (disabled). Set to e.g. 0.70 to drain cold OSDs too.
//! - `balancer/improvement_factor` — a move only commits when the
//!   candidate copyset's total load is `<= factor × current`.
//!   Default 0.90 (≥10% improvement required). Stops flip-flop
//!   between near-equal options.
//! - `balancer/scatter_width` — copyset-pool diversity target.
//!   Default 10 (Cidon/Stutsman recommended). Lower = fewer distinct
//!   copysets = lower correlated-loss probability but less diversity
//!   for the balancer to choose from.
//! - `balancer/per_tick_cap` — hard cap on moves per pool per tick.
//!   Default 0 (auto = `max(3, osd_count / 3)`). Set explicitly to
//!   bound churn on very large or very small clusters.
//! - `balancer/paused` — set to "true" to halt all moves. Logs still
//!   advance so operators can see what the balancer *would* do.
//!
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

const DEFAULT_SWEEP_SECS: u64 = 60;
const DEFAULT_OVERLOAD_MULTIPLIER: f64 = 1.20;
const DEFAULT_UNDERLOAD_MULTIPLIER: f64 = 0.0; // disabled
const DEFAULT_IMPROVEMENT_FACTOR: f64 = 0.90;
const DEFAULT_SCATTER_WIDTH: usize = 10;
/// Floor so very long configured intervals still leave the loop
/// responsive to a paused->unpaused flip within a reasonable window.
const MIN_SWEEP_SECS: u64 = 5;
const MAX_SWEEP_SECS: u64 = 3600;

/// Tuning knobs snapshot — read once per tick so hot-reloading config
/// doesn't race with the evaluation logic.
#[derive(Clone, Debug)]
struct Tuning {
    sweep: Duration,
    overload: f64,
    underload: f64,
    improvement: f64,
    scatter_width: usize,
    per_tick_cap_override: usize,
    paused: bool,
}

impl Tuning {
    fn load(meta: &Arc<MetaService>) -> Self {
        let secs = meta
            .config_parsed::<u64>("balancer/sweep_interval_seconds", DEFAULT_SWEEP_SECS)
            .clamp(MIN_SWEEP_SECS, MAX_SWEEP_SECS);
        Self {
            sweep: Duration::from_secs(secs),
            overload: meta
                .config_parsed::<f64>("balancer/overload_multiplier", DEFAULT_OVERLOAD_MULTIPLIER)
                .max(1.0),
            underload: meta
                .config_parsed::<f64>(
                    "balancer/underload_multiplier",
                    DEFAULT_UNDERLOAD_MULTIPLIER,
                )
                .clamp(0.0, 1.0),
            improvement: meta
                .config_parsed::<f64>("balancer/improvement_factor", DEFAULT_IMPROVEMENT_FACTOR)
                .clamp(0.0, 1.0),
            scatter_width: meta
                .config_parsed::<usize>("balancer/scatter_width", DEFAULT_SCATTER_WIDTH)
                .max(1),
            per_tick_cap_override: meta.config_parsed::<usize>("balancer/per_tick_cap", 0),
            paused: meta
                .config_str("balancer/paused", "false")
                .eq_ignore_ascii_case("true"),
        }
    }
}

pub fn spawn(meta: Arc<MetaService>) {
    tokio::spawn(async move {
        run(meta).await;
    });
    info!(
        "PG balancer spawned (tick every {}s by default; overrideable via balancer/* config)",
        DEFAULT_SWEEP_SECS
    );
}

async fn run(meta: Arc<MetaService>) {
    // Re-read the tuning knobs each iteration so hot-swapping the
    // config (via /_admin/config) takes effect on the next tick
    // without a restart. Interval is re-created only when the
    // sweep_interval_seconds knob changes.
    let mut cur_sweep = Tuning::load(&meta).sweep;
    let mut ticker = interval(cur_sweep);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let tuning = Tuning::load(&meta);
        if tuning.sweep != cur_sweep {
            info!(
                "balancer: sweep interval changed {:?} -> {:?}",
                cur_sweep, tuning.sweep
            );
            cur_sweep = tuning.sweep;
            ticker = interval(cur_sweep);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            ticker.tick().await; // consume the immediate first tick
        }
        if let Err(e) = sweep_once(&meta, &tuning).await {
            warn!("balancer sweep failed: {e}");
        }
    }
}

async fn sweep_once(meta: &Arc<MetaService>, tuning: &Tuning) -> anyhow::Result<()> {
    if !meta.is_raft_leader() {
        debug!("balancer: not leader, skipping tick");
        return Ok(());
    }
    // Mirror paused flag into RebalanceProgress so the UI banner
    // reflects it even when no moves happen this tick.
    meta.update_rebalance_progress(|p| {
        p.paused = tuning.paused;
    });
    if tuning.paused {
        debug!("balancer: paused via balancer/paused config");
        return Ok(());
    }

    let topology = meta.topology_snapshot();
    let active_osds: usize = topology.active_nodes().count();
    if active_osds == 0 {
        return Ok(());
    }

    let mut committed_this_tick: u64 = 0;
    let mut candidates_this_tick: u64 = 0;
    let mut scanned_this_tick: u64 = 0;
    let mut last_err = String::new();
    for pool in meta.pools_snapshot() {
        if pool.pg_count == 0 {
            continue;
        }
        match evaluate_pool(meta, &pool, active_osds, tuning).await {
            Ok(stats) => {
                committed_this_tick += stats.committed;
                candidates_this_tick += stats.candidates;
                scanned_this_tick += stats.scanned;
            }
            Err(e) => {
                last_err = format!("pool '{}': {e}", pool.name);
                warn!("balancer: pool '{}' evaluation failed: {e}", pool.name);
            }
        }
    }

    meta.update_rebalance_progress(|p| {
        p.started = true;
        p.last_sweep_at = now_unix();
        p.pgs_scanned_last_tick = scanned_this_tick;
        p.pg_candidates_last_tick = candidates_this_tick;
        p.pgs_moved_total = p.pgs_moved_total.saturating_add(committed_this_tick);
        // Keep legacy field live so the existing UI banner still
        // reflects activity while we migrate the console to the new
        // pgs_moved_total field.
        p.shards_rebalanced_total = p.pgs_moved_total;
        if !last_err.is_empty() {
            p.last_error = last_err;
        } else if committed_this_tick > 0 || candidates_this_tick > 0 {
            p.last_error.clear();
        }
    });
    Ok(())
}

/// Per-pool tick accounting returned by [`evaluate_pool`]. The sweep
/// aggregates this into [`RebalanceProgress`] for UI consumption.
#[derive(Default)]
struct PoolStats {
    scanned: u64,
    candidates: u64,
    committed: u64,
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
    tuning: &Tuning,
) -> anyhow::Result<PoolStats> {
    let mut stats = PoolStats::default();
    let pgs = meta.placement_groups_for_pool(&pool.name);
    if pgs.is_empty() {
        return Ok(stats);
    }
    stats.scanned = pgs.len() as u64;

    let k_plus_m = copy_count(pool);
    if k_plus_m == 0 {
        return Ok(stats);
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
    // Include zero-count OSDs so underload detection sees them.
    for node in meta.topology_snapshot().active_nodes() {
        osd_counts.entry(*node.id.as_bytes()).or_insert(0);
    }

    let total_slots = pgs.len() * k_plus_m;
    let target = (total_slots as f64) / (active_osds as f64);
    let overload_threshold = target * tuning.overload;
    let underload_threshold = target * tuning.underload;

    // Build the copyset pool once per pass — scoring candidate
    // replacements reuses it across every overloaded PG.
    let fd_level = parse_failure_domain(&pool.failure_domain);
    let seed = topology_seed(meta, pool);
    let cs_pool = CopysetPool::build(
        &meta.topology_snapshot(),
        fd_level,
        k_plus_m,
        tuning.scatter_width,
        seed,
    )?;
    if cs_pool.sets.is_empty() {
        warn!("balancer: pool '{}' has no feasible copysets", pool.name);
        return Ok(stats);
    }

    let per_tick_cap = if tuning.per_tick_cap_override > 0 {
        tuning.per_tick_cap_override
    } else {
        active_osds.max(3) / 3
    };

    // Score PGs: cost = max count across its OSDs - target. Filter
    // when either (a) some OSD >= overload_threshold, or (b) any
    // active OSD in the cluster is <= underload_threshold and this
    // PG has an OSD we could replace (i.e. a non-minimum OSD the
    // move can swap out). Sort hottest first.
    let has_cold_osd = tuning.underload > 0.0
        && osd_counts
            .values()
            .any(|c| (*c as f64) <= underload_threshold);
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
            let hot = pg
                .osd_ids
                .iter()
                .filter_map(|osd| <[u8; 16]>::try_from(osd.as_slice()).ok())
                .any(|arr| (*osd_counts.get(&arr).unwrap_or(&0) as f64) >= overload_threshold);
            // Underload path: there exists a cold OSD somewhere AND
            // this PG has an OSD above target (a candidate to swap
            // out toward the cold one).
            let cold_swap = has_cold_osd
                && pg
                    .osd_ids
                    .iter()
                    .filter_map(|osd| <[u8; 16]>::try_from(osd.as_slice()).ok())
                    .any(|arr| (*osd_counts.get(&arr).unwrap_or(&0) as f64) > target);
            hot || cold_swap
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    stats.candidates = scored.len() as u64;

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

        let mut rng = rand::rngs::StdRng::from_seed(seed_bytes(seed.wrapping_add(pg.pg_id.into())));
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

        // Improvement gate: require best candidate ≤ factor × current.
        if best_cost >= current_load * tuning.improvement {
            debug!(
                "balancer: pool={} pg_id={} improvement {}→{} below {}×; skip",
                pool.name, pg.pg_id, current_load, best_cost, tuning.improvement
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
                    pool.name, pg.pg_id, cost, current_load, best_cost, pg.version, new_pg.version
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

    stats.committed = committed as u64;
    if committed > 0 {
        info!(
            "balancer: pool '{}' committed {}/{} moves (target={:.2}, overload>={:.2}, underload<={:.2})",
            pool.name, committed, per_tick_cap, target, overload_threshold, underload_threshold
        );
    }
    Ok(stats)
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
