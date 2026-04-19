//! Copyset allocator — Cidon & Stutsman pattern for PG → OSD picking.
//!
//! Reference: "Copysets: Reducing the Frequency of Data Loss in Cloud
//! Storage" (USENIX ATC '13).
//!
//! # Why copysets
//!
//! With N OSDs and `(k+m)` shards per object, the naive pick-any-k+m
//! policy can form `C(N, k+m)` distinct "copysets" — the set of OSDs
//! that share any single object. If `k+m` OSDs fail concurrently, the
//! probability of data loss is proportional to the fraction of
//! copysets those OSDs cover. More unique copysets = higher
//! correlated-loss probability.
//!
//! The Cidon/Stutsman insight: don't let placement choose freely.
//! Pre-compute a small pool of copysets and only assign PGs from that
//! pool. With `scatter_width = S`, each OSD appears in ≈ S copysets,
//! so the total pool size is `(N × S) / (k + m)`. This caps the
//! number of distinct copysets to something much smaller than
//! `C(N, k+m)` — data loss probability drops by orders of magnitude
//! for realistic failure rates.
//!
//! # What this module does
//!
//! 1. Group OSDs by their value at a chosen failure-domain level
//!    (e.g. group by rack, so every copyset spans distinct racks).
//! 2. Sample copysets of size `k+m` such that each element comes from
//!    a distinct group (enforcing the failure-domain constraint).
//! 3. Run until every OSD appears in at least `scatter_width`
//!    copysets, or we exhaust the feasible combinations.
//!
//! The resulting [`CopysetPool`] is the input the PG allocator
//! samples from — one pool per `(tier, failure_domain_level, k+m)`
//! triple.

use objectio_common::{FailureDomain, NodeId};
use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::topology::ClusterTopology;

/// A single copyset — a `k+m`-tuple of OSD node ids, ordered by
/// position. The allocator assigns shard position `i` to
/// `osds[i]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Copyset {
    pub osds: Vec<NodeId>,
}

impl Copyset {
    /// Construct a copyset of specified size. Used by the allocator.
    #[must_use]
    pub fn new(osds: Vec<NodeId>) -> Self {
        Self { osds }
    }

    /// Number of OSDs in this copyset (= k+m).
    #[must_use]
    pub fn size(&self) -> usize {
        self.osds.len()
    }

    /// Does this copyset contain the given OSD?
    #[must_use]
    pub fn contains(&self, id: &NodeId) -> bool {
        self.osds.contains(id)
    }
}

/// A precomputed pool of valid copysets.
///
/// Built once per `(failure_domain_level, k+m)` configuration, then
/// consulted by the PG allocator (Phase 3) and the balancer (Phase 4)
/// every time they need to pick a new OSD tuple. Recomputed on
/// topology changes (OSD add/remove or failure-domain reshuffles).
#[derive(Clone, Debug)]
pub struct CopysetPool {
    /// Failure-domain level every copyset respects. Typically
    /// `FailureDomain::Host` for on-prem, `Rack` or `Datacenter` for
    /// larger clusters.
    pub level: FailureDomain,
    /// `k + m` — size of each copyset.
    pub copy_count: usize,
    /// The generated copyset set. Order is stable between two builds
    /// with the same seed, so logs and tests reproduce.
    pub sets: Vec<Copyset>,
}

/// Errors from pool construction.
#[derive(Debug, thiserror::Error)]
pub enum CopysetError {
    #[error("copy_count must be >= 1")]
    ZeroCopyCount,
    #[error("not enough distinct failure-domain groups: have {have}, need {need}")]
    InsufficientGroups { have: usize, need: usize },
    #[error("no viable OSDs in the topology")]
    EmptyTopology,
}

impl CopysetPool {
    /// Build a copyset pool from the live topology.
    ///
    /// - `level` — failure-domain level to enforce distinctness on.
    /// - `copy_count` — k+m per copyset.
    /// - `scatter_width` — target minimum appearances per OSD. The
    ///   Cidon/Stutsman paper recommends S ≈ 10 for a good balance of
    ///   MTTDL reduction and pool diversity. Smaller S → fewer
    ///   distinct copysets → lower correlated-loss probability →
    ///   fewer choices for the balancer.
    /// - `seed` — RNG seed. Pass the topology version for
    ///   reproducible pools across meta replicas.
    ///
    /// # Errors
    /// Returns [`CopysetError::InsufficientGroups`] when the topology
    /// has fewer than `copy_count` distinct values at `level` — no
    /// FD-respecting copyset exists in that case.
    pub fn build(
        topology: &ClusterTopology,
        level: FailureDomain,
        copy_count: usize,
        scatter_width: usize,
        seed: u64,
    ) -> Result<Self, CopysetError> {
        if copy_count == 0 {
            return Err(CopysetError::ZeroCopyCount);
        }

        // Bucket active OSDs by their FD-level value.
        let groups: BTreeMap<String, Vec<NodeId>> = group_active_by_level(topology, level);
        if groups.is_empty() {
            return Err(CopysetError::EmptyTopology);
        }
        if groups.len() < copy_count {
            return Err(CopysetError::InsufficientGroups {
                have: groups.len(),
                need: copy_count,
            });
        }

        // Target pool size — each OSD should appear in ≈ scatter_width
        // copysets. Total node-slots in the pool = pool_size × copy_count,
        // so pool_size ≈ (N × S) / (k+m).
        let total_osds: usize = groups.values().map(Vec::len).sum();
        let target_pool_size = ((total_osds * scatter_width) / copy_count).max(1);

        let mut rng = StdRng::seed_from_u64(seed);
        let mut coverage: HashMap<NodeId, usize> =
            groups.values().flatten().map(|n| (*n, 0_usize)).collect();
        let mut sets: Vec<Copyset> = Vec::with_capacity(target_pool_size);
        let mut seen: HashSet<Vec<[u8; 16]>> = HashSet::new();

        // Keep sampling until either every OSD hits scatter_width or
        // we exhaust a generous budget of attempts. The budget
        // accounts for the fact that late iterations often hit
        // duplicates once the pool is saturated.
        let attempt_budget = target_pool_size.saturating_mul(10).max(256);
        let mut attempts = 0usize;
        while attempts < attempt_budget
            && (sets.len() < target_pool_size
                || coverage.values().any(|&c| c < scatter_width))
        {
            attempts += 1;
            if let Some(cs) = sample_copyset(&groups, copy_count, &coverage, &mut rng) {
                // Canonicalise for dedup: sort ids (by raw bytes — NodeId
                // doesn't impl Ord) so (A,B,C) = (C,A,B).
                let mut canon: Vec<[u8; 16]> =
                    cs.osds.iter().map(|id| *id.as_bytes()).collect();
                canon.sort();
                if seen.insert(canon) {
                    for n in &cs.osds {
                        *coverage.entry(*n).or_insert(0) += 1;
                    }
                    sets.push(cs);
                }
            }
        }

        Ok(Self {
            level,
            copy_count,
            sets,
        })
    }

    /// Pick a uniformly random copyset from the pool.
    #[must_use]
    pub fn pick_random(&self, rng: &mut impl Rng) -> Option<&Copyset> {
        self.sets.choose(rng)
    }

    /// Pick the copyset with the smallest "cost" under the given
    /// function — used by the balancer to pick the least-loaded
    /// tuple. Ties broken uniformly at random among equally-best
    /// options.
    pub fn pick_min_cost<F>(&self, rng: &mut impl Rng, mut cost: F) -> Option<&Copyset>
    where
        F: FnMut(&Copyset) -> f64,
    {
        if self.sets.is_empty() {
            return None;
        }
        let mut best_cost = f64::INFINITY;
        let mut ties: Vec<&Copyset> = Vec::new();
        for cs in &self.sets {
            let c = cost(cs);
            if c < best_cost {
                best_cost = c;
                ties.clear();
                ties.push(cs);
            } else if (c - best_cost).abs() < f64::EPSILON {
                ties.push(cs);
            }
        }
        ties.choose(rng).copied()
    }
}

/// Group active OSDs by their value at the chosen failure-domain level.
/// Keys are the FD values (rack name, host name, ...); values are the
/// lists of OSDs sharing that value.
fn group_active_by_level(
    topology: &ClusterTopology,
    level: FailureDomain,
) -> BTreeMap<String, Vec<NodeId>> {
    let mut out: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
    for node in topology.active_nodes() {
        let key = node.failure_domain.at_level(level).to_string();
        out.entry(key).or_default().push(node.id);
    }
    out
}

/// Sample one copyset: pick `copy_count` groups uniformly without
/// replacement, then pick one OSD per group weighted inversely by
/// current coverage — favours under-covered OSDs so every node
/// converges to `scatter_width`.
fn sample_copyset(
    groups: &BTreeMap<String, Vec<NodeId>>,
    copy_count: usize,
    coverage: &HashMap<NodeId, usize>,
    rng: &mut impl Rng,
) -> Option<Copyset> {
    let chosen_groups: Vec<&Vec<NodeId>> = groups
        .values()
        .choose_multiple(rng, copy_count)
        .into_iter()
        .collect();
    if chosen_groups.len() != copy_count {
        return None;
    }
    let mut osds = Vec::with_capacity(copy_count);
    for group in chosen_groups {
        // Invert coverage: score = 1 / (1 + covered_count). Picks
        // under-covered OSDs preferentially, without ever collapsing
        // to "always the same one" (every OSD keeps nonzero weight).
        let weights: Vec<f64> = group
            .iter()
            .map(|id| 1.0 / (1.0 + *coverage.get(id).unwrap_or(&0) as f64))
            .collect();
        let total: f64 = weights.iter().sum();
        if total <= 0.0 {
            return None;
        }
        // `gen` is a reserved keyword in Rust 2024; use the raw form.
        let mut pick = rng.r#gen::<f64>() * total;
        let mut chosen: Option<NodeId> = None;
        for (id, w) in group.iter().zip(weights.iter()) {
            pick -= w;
            if pick <= 0.0 {
                chosen = Some(*id);
                break;
            }
        }
        osds.push(chosen.unwrap_or(group[0]));
    }
    Some(Copyset::new(osds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{ClusterTopology, NodeInfo};
    use objectio_common::NodeStatus;
    use std::net::SocketAddr;

    use crate::topology::FailureDomainInfo;

    fn mk_node(id_seed: u8, host: &str, rack: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::from_bytes([id_seed; 16]),
            name: format!("osd-{id_seed}"),
            address: "127.0.0.1:9200".parse().unwrap(),
            failure_domain: FailureDomainInfo::new_full(
                "local", "zone-a", "dc1", rack, host,
            ),
            status: NodeStatus::Active,
            disks: Vec::new(),
            weight: 1.0,
            last_heartbeat: 0,
        }
    }

    fn topology_with(hosts_per_rack: &[(usize, &str)]) -> ClusterTopology {
        let mut t = ClusterTopology::new();
        let mut seed = 1u8;
        for (count, rack) in hosts_per_rack {
            for i in 0..*count {
                let host = format!("{rack}-host-{i}");
                t.upsert_node(mk_node(seed, &host, rack));
                seed = seed.wrapping_add(1);
            }
        }
        t
    }

    #[test]
    fn rejects_copy_count_larger_than_groups() {
        // 2 racks → can't place 3 shards with Rack-level distinctness.
        let t = topology_with(&[(3, "rackA"), (3, "rackB")]);
        let err = CopysetPool::build(&t, FailureDomain::Rack, 3, 5, 0).unwrap_err();
        assert!(matches!(
            err,
            CopysetError::InsufficientGroups { have: 2, need: 3 }
        ));
    }

    #[test]
    fn every_copyset_spans_distinct_fd_values() {
        // 5 racks, 2 hosts each → 10 OSDs. k+m=3, scatter=10.
        let t = topology_with(&[
            (2, "rackA"),
            (2, "rackB"),
            (2, "rackC"),
            (2, "rackD"),
            (2, "rackE"),
        ]);
        let pool = CopysetPool::build(&t, FailureDomain::Rack, 3, 10, 42).unwrap();
        assert!(!pool.sets.is_empty());
        for cs in &pool.sets {
            // Resolve each OSD's rack and check distinctness.
            let mut racks: Vec<String> = cs
                .osds
                .iter()
                .map(|id| {
                    t.all_nodes()
                        .find(|n| n.id == *id)
                        .map(|n| n.failure_domain.rack.clone())
                        .unwrap()
                })
                .collect();
            racks.sort();
            let before = racks.len();
            racks.dedup();
            assert_eq!(racks.len(), before, "duplicate rack in copyset {cs:?}");
        }
    }

    #[test]
    fn each_osd_hits_scatter_width() {
        let t = topology_with(&[
            (2, "rackA"),
            (2, "rackB"),
            (2, "rackC"),
            (2, "rackD"),
            (2, "rackE"),
        ]);
        let pool = CopysetPool::build(&t, FailureDomain::Rack, 3, 5, 42).unwrap();
        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        for cs in &pool.sets {
            for id in &cs.osds {
                *counts.entry(*id).or_insert(0) += 1;
            }
        }
        assert_eq!(counts.len(), 10, "every OSD should appear");
        let min = counts.values().copied().min().unwrap();
        assert!(min >= 5, "min coverage {min} < scatter_width 5");
    }

    #[test]
    fn pool_is_deterministic_for_same_seed() {
        let t = topology_with(&[(3, "rackA"), (3, "rackB"), (3, "rackC")]);
        let a = CopysetPool::build(&t, FailureDomain::Rack, 3, 5, 0xdead).unwrap();
        let b = CopysetPool::build(&t, FailureDomain::Rack, 3, 5, 0xdead).unwrap();
        assert_eq!(a.sets, b.sets);
    }

    #[test]
    fn pick_min_cost_picks_the_cheapest() {
        let t = topology_with(&[(2, "r1"), (2, "r2"), (2, "r3")]);
        let pool = CopysetPool::build(&t, FailureDomain::Rack, 3, 5, 1).unwrap();
        // Assign cost = 0 to exactly one copyset, all others 10.
        let target = pool.sets[0].clone();
        let mut rng = StdRng::seed_from_u64(1);
        let got = pool
            .pick_min_cost(&mut rng, |cs| {
                if cs.osds == target.osds { 0.0 } else { 10.0 }
            })
            .unwrap();
        assert_eq!(got.osds, target.osds);
    }

    // Silence unused import warnings in builds that don't run tests.
    #[allow(dead_code)]
    fn _unused(_: SocketAddr) {}
}
