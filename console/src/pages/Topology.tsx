import { useEffect, useMemo, useState } from "react";
import {
  Globe2,
  Compass,
  Building2,
  Layers3,
  Server,
  HardDrive,
  CheckCircle2,
  AlertTriangle,
  ChevronRight,
  ChevronDown,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import Card from "../components/Card";

interface HostNode { host: string; osds: string[]; }
interface RackNode { rack: string; hosts: HostNode[]; }
interface DcNode { datacenter: string; racks: RackNode[]; }
interface ZoneNode { zone: string; datacenters: DcNode[]; }
interface RegionNode { region: string; zones: ZoneNode[]; }

interface TopologyData {
  osd_count: number;
  distinct: {
    region: number;
    zone: number;
    datacenter: number;
    rack: number;
    host: number;
  };
  tree: RegionNode[];
}

interface Pool { name: string; failure_domain: string; ec_k: number; ec_m: number; }

interface ValidationResult {
  pool: string;
  required_level: string;
  required_count: number;
  available_count: number;
  satisfiable: boolean;
  reason: string;
}

// Color + icon per topology level so the diagram reads at a glance.
const LEVEL_META = {
  region:     { color: "bg-indigo-100 text-indigo-800 border-indigo-300",  icon: Globe2,    ringColor: "ring-indigo-200" },
  zone:       { color: "bg-sky-100 text-sky-800 border-sky-300",           icon: Compass,   ringColor: "ring-sky-200" },
  datacenter: { color: "bg-teal-100 text-teal-800 border-teal-300",        icon: Building2, ringColor: "ring-teal-200" },
  rack:       { color: "bg-amber-100 text-amber-800 border-amber-300",     icon: Layers3,   ringColor: "ring-amber-200" },
  host:       { color: "bg-gray-100 text-gray-700 border-gray-300",        icon: Server,    ringColor: "ring-gray-200" },
  osd:        { color: "bg-white text-gray-700 border-gray-200",           icon: HardDrive, ringColor: "ring-gray-200" },
} as const;

type LevelKey = keyof typeof LEVEL_META;

/**
 * Single tree row. SVG-ish rendering via nested flex boxes so every level
 * gets its own color-coded pill and a chevron that opens/closes the
 * subtree. A single vertical line (absolute-positioned ::before) connects
 * a node to its children for the "diagram" feel without pulling in d3.
 */
function TreeNode({
  level,
  label,
  badge,
  defaultOpen = true,
  children,
}: {
  level: LevelKey;
  label: string;
  badge?: string;
  defaultOpen?: boolean;
  children?: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  const meta = LEVEL_META[level];
  const Icon = meta.icon;
  const hasChildren = Boolean(children);

  return (
    <div className="relative">
      <div
        onClick={() => hasChildren && setOpen(!open)}
        className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-lg border ${meta.color} text-[12px] font-medium select-none ${
          hasChildren ? "cursor-pointer hover:ring-2 " + meta.ringColor : ""
        } transition-shadow`}
      >
        {hasChildren && (open ? <ChevronDown size={12} /> : <ChevronRight size={12} />)}
        <Icon size={12} />
        <span className="text-[10px] uppercase tracking-wider opacity-60">{level}</span>
        <span className="font-mono">{label || "(none)"}</span>
        {badge && (
          <span className="ml-1 text-[10px] px-1.5 py-0.5 rounded-full bg-white/60 border border-current/20 font-normal">
            {badge}
          </span>
        )}
      </div>
      {hasChildren && open && (
        <div className="relative ml-3 pl-4 mt-1.5 border-l border-dashed border-gray-300 space-y-1.5">
          {children}
        </div>
      )}
    </div>
  );
}

/** Single OSD leaf: compact pill with the 8-char short id. */
function OsdPill({ id }: { id: string }) {
  return (
    <div className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border border-gray-200 bg-white text-[10px] font-mono text-gray-600 shadow-sm">
      <HardDrive size={10} className="text-gray-400" />
      {id.slice(0, 8)}
    </div>
  );
}

/** Tree renderer — walks the topology and lays it out with collapsible nodes. */
function TreeView({ tree }: { tree: RegionNode[] }) {
  if (tree.length === 0) {
    return <div className="text-[12px] text-gray-400 italic py-6 text-center">No OSDs registered.</div>;
  }
  return (
    <div className="space-y-3 overflow-x-auto py-2">
      {tree.map((r) => {
        const regionOsdCount = r.zones.reduce(
          (n, z) =>
            n +
            z.datacenters.reduce(
              (dn, d) => dn + d.racks.reduce(
                (rn, rk) => rn + rk.hosts.reduce((hn, h) => hn + h.osds.length, 0),
                0,
              ),
              0,
            ),
          0,
        );
        return (
          <TreeNode key={r.region} level="region" label={r.region} badge={`${regionOsdCount} osds`}>
            {r.zones.map((z) => {
              const zoneCount = z.datacenters.reduce(
                (n, d) =>
                  n + d.racks.reduce((rn, rk) => rn + rk.hosts.reduce((hn, h) => hn + h.osds.length, 0), 0),
                0,
              );
              return (
                <TreeNode key={z.zone} level="zone" label={z.zone} badge={`${zoneCount} osds`}>
                  {z.datacenters.map((d) => {
                    const dcCount = d.racks.reduce(
                      (n, rk) => n + rk.hosts.reduce((hn, h) => hn + h.osds.length, 0),
                      0,
                    );
                    return (
                      <TreeNode
                        key={d.datacenter}
                        level="datacenter"
                        label={d.datacenter}
                        badge={`${dcCount} osds`}
                      >
                        {d.racks.map((rk) => {
                          const rackCount = rk.hosts.reduce((n, h) => n + h.osds.length, 0);
                          return (
                            <TreeNode
                              key={rk.rack}
                              level="rack"
                              label={rk.rack}
                              badge={`${rackCount} osds`}
                            >
                              {rk.hosts.map((h) => (
                                <TreeNode
                                  key={h.host}
                                  level="host"
                                  label={h.host}
                                  badge={`${h.osds.length} osds`}
                                >
                                  <div className="flex flex-wrap gap-1">
                                    {h.osds.map((o) => (
                                      <OsdPill key={o} id={o} />
                                    ))}
                                  </div>
                                </TreeNode>
                              ))}
                            </TreeNode>
                          );
                        })}
                      </TreeNode>
                    );
                  })}
                </TreeNode>
              );
            })}
          </TreeNode>
        );
      })}
    </div>
  );
}

/** Top stat tile. Clicking the tile doesn't do anything yet — reserved for
 *  a future filter action that pivots the tree to show only that level. */
function StatTile({
  level,
  count,
  label,
}: {
  level: LevelKey;
  count: number;
  label: string;
}) {
  const meta = LEVEL_META[level];
  const Icon = meta.icon;
  return (
    <div className={`rounded-lg border px-3 py-2.5 ${meta.color.replace("text-", "border-")}`}>
      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider opacity-70">
        <Icon size={11} /> {label}
      </div>
      <div className="text-[20px] font-semibold mt-0.5">{count}</div>
    </div>
  );
}

export default function Topology() {
  const [topology, setTopology] = useState<TopologyData | null>(null);
  const [pools, setPools] = useState<Pool[]>([]);
  const [validations, setValidations] = useState<Record<string, ValidationResult>>({});
  const [loading, setLoading] = useState(true);
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      try {
        const t = await fetch("/_admin/topology").then((r) => r.json());
        const p = await fetch("/_admin/pools").then((r) => r.json());
        if (cancelled) return;
        setTopology(t);
        const poolList: Pool[] = Array.isArray(p) ? p : p.pools || [];
        setPools(poolList);
        const entries = await Promise.all(
          poolList.map(async (pool) => {
            const r = await fetch(
              `/_admin/placement/validate?pool=${encodeURIComponent(pool.name)}`,
            );
            return [pool.name, await r.json()] as const;
          }),
        );
        if (!cancelled) setValidations(Object.fromEntries(entries));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  // Sort pools: unsatisfiable first so operators see problems at a glance.
  const sortedPools = useMemo(() => {
    return [...pools].sort((a, b) => {
      const sa = validations[a.name]?.satisfiable ?? true;
      const sb = validations[b.name]?.satisfiable ?? true;
      if (sa === sb) return a.name.localeCompare(b.name);
      return sa ? 1 : -1;
    });
  }, [pools, validations]);

  return (
    <div className="p-6">
      <PageHeader
        title="Cluster Topology"
        description="Physical layout and placement health. Click a node to collapse/expand its subtree."
        action={
          <button
            onClick={() => setRefreshKey((k) => k + 1)}
            className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
          >
            Refresh
          </button>
        }
      />

      {loading && !topology && (
        <div className="text-[12px] text-gray-400">Loading topology…</div>
      )}

      {topology && (
        <>
          {/* Summary tiles */}
          <div className="grid grid-cols-2 md:grid-cols-6 gap-2 mb-4">
            <StatTile level="region" count={topology.distinct.region} label="regions" />
            <StatTile level="zone" count={topology.distinct.zone} label="zones" />
            <StatTile level="datacenter" count={topology.distinct.datacenter} label="datacenters" />
            <StatTile level="rack" count={topology.distinct.rack} label="racks" />
            <StatTile level="host" count={topology.distinct.host} label="hosts" />
            <StatTile level="osd" count={topology.osd_count} label="osds" />
          </div>

          {/* Pool satisfiability strip */}
          {pools.length > 0 && (
            <Card
              title={
                <div className="flex items-center gap-2">
                  <Layers3 size={13} className="text-amber-600" />
                  <span>Placement satisfiability</span>
                </div>
              }
              className="mb-4"
            >
              <ul className="divide-y divide-gray-100 -my-1">
                {sortedPools.map((pool) => {
                  const v = validations[pool.name];
                  const ok = v?.satisfiable ?? true;
                  return (
                    <li key={pool.name} className="flex items-start gap-2.5 py-2">
                      {ok ? (
                        <CheckCircle2 size={14} className="text-green-600 mt-0.5 shrink-0" />
                      ) : (
                        <AlertTriangle size={14} className="text-amber-600 mt-0.5 shrink-0" />
                      )}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center flex-wrap gap-1.5">
                          <span className="text-[13px] font-medium text-gray-900">{pool.name}</span>
                          <span className="text-[10px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-600 font-mono">
                            {pool.ec_k}+{pool.ec_m} / {pool.failure_domain}
                          </span>
                        </div>
                        {v && (
                          <div
                            className={`text-[11px] mt-0.5 ${ok ? "text-gray-500" : "text-amber-700"}`}
                          >
                            {v.reason}
                          </div>
                        )}
                      </div>
                    </li>
                  );
                })}
              </ul>
            </Card>
          )}

          {/* The tree itself */}
          <Card title="Topology tree">
            <TreeView tree={topology.tree} />
          </Card>

          <p className="mt-3 text-[11px] text-gray-400">
            Hierarchy: region → zone → datacenter → rack → host → OSDs. Empty values collapse to <code>(none)</code>.
          </p>
        </>
      )}
    </div>
  );
}
