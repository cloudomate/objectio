import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  ArrowLeft,
  RefreshCw,
  Layers,
  GitBranch,
  HardDrive,
  AlertTriangle,
} from "lucide-react";
import {
  placementGroups,
  nodes as nodesApi,
  type PlacementGroup,
  type NodeInfo,
  request,
} from "../api/client";

interface Pool {
  name: string;
  ec_type: number;
  ec_k: number;
  ec_m: number;
  ec_local_parity: number;
  ec_global_parity: number;
  replication_count: number;
  failure_domain: string;
  pg_count?: number;
  tier?: string;
}

function ecString(p: Pool): string {
  if (p.ec_type === 2) return `${p.replication_count}× Replication`;
  if (p.ec_type === 1)
    return `LRC ${p.ec_k}+${p.ec_local_parity}+${p.ec_global_parity}`;
  return `${p.ec_k}+${p.ec_m} Reed-Solomon`;
}

function copyCount(p: Pool): number {
  if (p.ec_type === 2) return p.replication_count || 0;
  if (p.ec_type === 1) return p.ec_k + p.ec_local_parity + p.ec_global_parity;
  return p.ec_k + p.ec_m;
}

/// `/cluster/pools/:name` — drills into a pool's placement. Two
/// complementary views: a per-OSD load bar chart (which OSDs are hot
/// or cold) and a PG × OSD matrix (which OSDs each PG lives on). The
/// latter is scrollable so it scales to thousands of PGs.
export default function PoolPlacement() {
  const { name: poolName } = useParams<{ name: string }>();
  const nav = useNavigate();
  const [pool, setPool] = useState<Pool | null>(null);
  const [pgs, setPgs] = useState<PlacementGroup[]>([]);
  const [osds, setOsds] = useState<NodeInfo[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedOsd, setSelectedOsd] = useState<string | null>(null);
  const [selectedPg, setSelectedPg] = useState<number | null>(null);

  const load = async () => {
    if (!poolName) return;
    setLoading(true);
    setErr(null);
    try {
      const [p, pgsResp, nodesResp] = await Promise.all([
        request<Pool>("GET", `/_admin/pools/${encodeURIComponent(poolName)}`),
        placementGroups.list(poolName, { max: 10000 }),
        nodesApi.list(),
      ]);
      setPool(p);
      setPgs(pgsResp.pgs);
      setOsds(nodesResp.nodes);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [poolName]);

  // Per-OSD PG-membership count — the number the balancer actually
  // reasons about. Distinct from the shard count (which reflects
  // objects that have been written to PGs that happen to include
  // this OSD).
  const osdPgCount = useMemo(() => {
    const c = new Map<string, number>();
    for (const pg of pgs) {
      for (const osd of pg.osd_ids) {
        c.set(osd, (c.get(osd) ?? 0) + 1);
      }
    }
    return c;
  }, [pgs]);

  const osdEntries = useMemo(() => {
    // Sort by cluster position so the matrix columns stay stable.
    const entries = osds.map((o) => ({
      id: o.node_id,
      name: o.node_name || o.node_id.slice(0, 8),
      pgs: osdPgCount.get(o.node_id) ?? 0,
      shards: o.shard_count ?? 0,
      online: o.online,
      admin_state: o.admin_state,
    }));
    entries.sort((a, b) => a.name.localeCompare(b.name));
    return entries;
  }, [osds, osdPgCount]);

  const cc = pool ? copyCount(pool) : 0;
  const pgCount = pgs.length;
  const totalSlots = pgCount * cc;
  const target = osdEntries.length > 0 ? totalSlots / osdEntries.length : 0;
  const overloadThresh = target * 1.2;
  const underloadThresh = target * 0.7;

  const maxPg = osdEntries.reduce((m, o) => Math.max(m, o.pgs), 0);

  // Index osd ids into column positions for the matrix.
  const osdIndex = useMemo(() => {
    const m = new Map<string, number>();
    osdEntries.forEach((o, i) => m.set(o.id, i));
    return m;
  }, [osdEntries]);

  const migratingCount = pgs.filter(
    (pg) => pg.migrating_to_osd_ids.length > 0,
  ).length;

  return (
    <div className="p-4">
      {/* Header */}
      <div className="flex items-center justify-between gap-3 mb-3">
        <div className="flex items-center gap-3">
          <button
            onClick={() => nav("/cluster/pools")}
            className="flex items-center gap-1 text-[12px] text-gray-600 hover:text-gray-900"
          >
            <ArrowLeft size={14} />
            Pools
          </button>
          <div className="text-gray-300">/</div>
          <h1 className="text-[16px] font-semibold text-gray-900">{poolName}</h1>
          {pool && (
            <span className="text-[11px] font-mono px-1.5 py-0.5 rounded bg-gray-100 text-gray-700">
              {ecString(pool)}
            </span>
          )}
          {migratingCount > 0 && (
            <span className="flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded bg-amber-50 text-amber-800 border border-amber-200">
              <AlertTriangle size={11} />
              {migratingCount} migrating
            </span>
          )}
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 px-2.5 py-1 border border-gray-200 rounded-lg text-[11px] font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw size={12} className={loading ? "animate-spin" : ""} />
          Refresh
        </button>
      </div>

      {err && (
        <div className="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-[12px] text-red-800">
          {err}
        </div>
      )}

      {/* Summary stats */}
      <div className="grid grid-cols-4 gap-3 mb-4">
        <StatCard
          label="Placement groups"
          value={pgCount.toLocaleString()}
          icon={<Layers size={14} />}
          hint={pool?.pg_count && pool.pg_count !== pgCount
            ? `declared ${pool.pg_count}`
            : undefined}
        />
        <StatCard
          label="Shards per PG (k+m)"
          value={cc.toString()}
          icon={<GitBranch size={14} />}
          hint={pool?.failure_domain ? `fd=${pool.failure_domain}` : undefined}
        />
        <StatCard
          label="Target PGs/OSD"
          value={target.toFixed(1)}
          icon={<HardDrive size={14} />}
          hint={`overload ≥ ${overloadThresh.toFixed(1)}, underload ≤ ${underloadThresh.toFixed(1)}`}
        />
        <StatCard
          label="Active OSDs"
          value={osdEntries.length.toString()}
          icon={<HardDrive size={14} />}
          hint={pool?.tier ? `tier=${pool.tier}` : undefined}
        />
      </div>

      {/* Per-OSD bar chart */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden mb-4">
        <div className="px-3 py-2 border-b border-gray-100 flex items-center justify-between">
          <h2 className="text-[12px] font-medium text-gray-700 uppercase tracking-wide">
            PG-membership per OSD
          </h2>
          <div className="flex items-center gap-2 text-[10px] text-gray-500">
            <LegendSwatch color="bg-blue-500" label="PGs" />
            <LegendSwatch color="bg-red-100 border border-red-400" label="overload" />
            <LegendSwatch
              color="bg-amber-100 border border-amber-400"
              label="underload"
            />
          </div>
        </div>
        <div className="p-3 space-y-1.5">
          {osdEntries.map((o) => {
            const isOver = o.pgs >= overloadThresh;
            const isUnder = o.pgs <= underloadThresh;
            const pct = maxPg > 0 ? (o.pgs / maxPg) * 100 : 0;
            const targetPct = maxPg > 0 ? (target / maxPg) * 100 : 0;
            return (
              <button
                key={o.id}
                onClick={() =>
                  setSelectedOsd(selectedOsd === o.id ? null : o.id)
                }
                className={`w-full grid grid-cols-[140px_1fr_72px] items-center gap-2 px-1 py-0.5 rounded hover:bg-gray-50 text-left ${
                  selectedOsd === o.id ? "bg-blue-50" : ""
                }`}
              >
                <div className="flex items-center gap-1.5 min-w-0">
                  <span
                    className={`w-1.5 h-1.5 rounded-full ${
                      o.online ? "bg-green-500" : "bg-gray-400"
                    }`}
                  />
                  <span className="text-[11px] font-mono text-gray-700 truncate">
                    {o.name}
                  </span>
                </div>
                <div className="relative h-4 bg-gray-100 rounded overflow-hidden">
                  <div
                    className={`absolute inset-y-0 left-0 ${
                      isOver
                        ? "bg-red-400"
                        : isUnder
                          ? "bg-amber-400"
                          : "bg-blue-500"
                    }`}
                    style={{ width: `${pct}%` }}
                  />
                  {target > 0 && (
                    <div
                      className="absolute inset-y-0 w-px bg-gray-700 opacity-60"
                      style={{ left: `${targetPct}%` }}
                      title={`target ${target.toFixed(1)}`}
                    />
                  )}
                </div>
                <div className="flex items-center justify-end gap-2 text-[11px] font-mono">
                  <span className={isOver ? "text-red-700 font-semibold" : isUnder ? "text-amber-700 font-semibold" : "text-gray-700"}>
                    {o.pgs}
                  </span>
                  <span className="text-gray-400">·</span>
                  <span className="text-gray-400" title={`${o.shards} shards on disk`}>
                    {o.shards}s
                  </span>
                </div>
              </button>
            );
          })}
          {osdEntries.length === 0 && !loading && (
            <div className="text-center text-[12px] text-gray-400 py-4">
              No active OSDs
            </div>
          )}
        </div>
      </div>

      {/* PG × OSD matrix */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden mb-4">
        <div className="px-3 py-2 border-b border-gray-100 flex items-center justify-between">
          <h2 className="text-[12px] font-medium text-gray-700 uppercase tracking-wide">
            PG × OSD matrix
          </h2>
          <div className="flex items-center gap-2 text-[10px] text-gray-500">
            {selectedOsd && (
              <button
                onClick={() => setSelectedOsd(null)}
                className="px-1.5 py-0.5 rounded bg-gray-100 hover:bg-gray-200"
              >
                Clear OSD filter
              </button>
            )}
            {selectedPg !== null && (
              <button
                onClick={() => setSelectedPg(null)}
                className="px-1.5 py-0.5 rounded bg-gray-100 hover:bg-gray-200"
              >
                Clear PG filter
              </button>
            )}
            <LegendSwatch color="bg-blue-500" label="member" />
            <LegendSwatch
              color="bg-amber-400"
              label="migrating-to"
            />
          </div>
        </div>
        <div className="overflow-auto" style={{ maxHeight: "480px" }}>
          <table className="text-[10px] border-collapse">
            <thead className="sticky top-0 bg-white z-10">
              <tr>
                <th className="px-2 py-1 text-left text-gray-500 font-normal sticky left-0 bg-white z-20 border-b border-gray-100">
                  pg_id
                </th>
                {osdEntries.map((o) => (
                  <th
                    key={o.id}
                    onClick={() =>
                      setSelectedOsd(selectedOsd === o.id ? null : o.id)
                    }
                    className={`px-1 py-1 text-center font-normal cursor-pointer border-b border-gray-100 ${
                      selectedOsd === o.id ? "bg-blue-50 text-blue-700" : "text-gray-500 hover:bg-gray-50"
                    }`}
                    title={`${o.name} — ${o.pgs} PGs`}
                  >
                    <div className="font-mono">{o.name.replace("objectio-osd-", "osd")}</div>
                  </th>
                ))}
                <th className="px-2 py-1 text-center text-gray-500 font-normal border-b border-gray-100">
                  v
                </th>
              </tr>
            </thead>
            <tbody>
              {pgs.map((pg) => {
                const row = new Array(osdEntries.length).fill(
                  0,
                ) as Array<0 | 1 | 2>;
                pg.osd_ids.forEach((osd) => {
                  const i = osdIndex.get(osd);
                  if (i !== undefined) row[i] = 1;
                });
                pg.migrating_to_osd_ids.forEach((osd) => {
                  const i = osdIndex.get(osd);
                  if (i !== undefined) row[i] = 2;
                });
                const hidden =
                  (selectedOsd &&
                    !pg.osd_ids.includes(selectedOsd) &&
                    !pg.migrating_to_osd_ids.includes(selectedOsd)) ||
                  (selectedPg !== null && pg.pg_id !== selectedPg);
                if (hidden) return null;
                return (
                  <tr
                    key={pg.pg_id}
                    className={`hover:bg-gray-50 cursor-pointer ${
                      selectedPg === pg.pg_id ? "bg-blue-50" : ""
                    }`}
                    onClick={() =>
                      setSelectedPg(selectedPg === pg.pg_id ? null : pg.pg_id)
                    }
                  >
                    <td className="px-2 py-0.5 font-mono text-gray-600 sticky left-0 bg-white z-10">
                      {pg.pg_id}
                    </td>
                    {row.map((v, i) => (
                      <td
                        key={i}
                        className="text-center"
                        title={v ? osdEntries[i].name : ""}
                      >
                        <div
                          className={`inline-block w-3 h-3 rounded-sm ${
                            v === 1
                              ? "bg-blue-500"
                              : v === 2
                                ? "bg-amber-400"
                                : "bg-gray-100"
                          }`}
                        />
                      </td>
                    ))}
                    <td className="px-2 py-0.5 font-mono text-gray-400">
                      v{pg.version}
                    </td>
                  </tr>
                );
              })}
              {pgs.length === 0 && !loading && (
                <tr>
                  <td
                    colSpan={osdEntries.length + 2}
                    className="px-3 py-6 text-center text-gray-400"
                  >
                    No placement groups in this pool
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function StatCard({
  label,
  value,
  icon,
  hint,
}: {
  label: string;
  value: string;
  icon: React.ReactNode;
  hint?: string;
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 px-3 py-2.5">
      <div className="flex items-center gap-1.5 text-[10px] text-gray-500 uppercase tracking-wide">
        {icon}
        {label}
      </div>
      <div className="text-[18px] font-semibold text-gray-900 mt-0.5">
        {value}
      </div>
      {hint && <div className="text-[10px] text-gray-400 mt-0.5">{hint}</div>}
    </div>
  );
}

function LegendSwatch({ color, label }: { color: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1">
      <span className={`w-2.5 h-2.5 rounded-sm ${color}`} />
      {label}
    </span>
  );
}
