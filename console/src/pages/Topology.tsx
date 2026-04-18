import { useEffect, useMemo, useState } from "react";
import {
  Globe2,
  Compass,
  Building2,
  Layers3,
  Server,
  HardDrive,
  ChevronRight,
  ChevronDown,
  RefreshCw,
  Power,
  Shuffle,
  Ban,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import Card from "../components/Card";
import Drawer from "../components/Drawer";
import CapacityBar from "../components/CapacityBar";
import BreadcrumbPath from "../components/BreadcrumbPath";
import StatusDot from "../components/StatusDot";
import { nodes as nodesApi, type NodeInfo } from "../api/client";

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

function formatBytes(b: number): string {
  if (b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(b) / Math.log(1024)));
  const v = b / Math.pow(1024, i);
  return `${v >= 10 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

// Per-level pill styling — lets every tree node read at a glance.
const LEVEL_META = {
  region:     { color: "bg-indigo-50 text-indigo-800 border-indigo-200",  icon: Globe2    },
  zone:       { color: "bg-sky-50 text-sky-800 border-sky-200",           icon: Compass   },
  datacenter: { color: "bg-teal-50 text-teal-800 border-teal-200",        icon: Building2 },
  rack:       { color: "bg-amber-50 text-amber-800 border-amber-200",     icon: Layers3   },
  host:       { color: "bg-gray-50 text-gray-800 border-gray-200",        icon: Server    },
} as const;

type LevelKey = keyof typeof LEVEL_META;

interface TreeRowProps {
  level: LevelKey;
  label: string;
  badge?: string;
  rightAccessory?: React.ReactNode;
  path: string;
  expanded: Set<string>;
  setExpanded: React.Dispatch<React.SetStateAction<Set<string>>>;
  hasChildren: boolean;
  children?: React.ReactNode;
  onSelect?: () => void;
  selected?: boolean;
}

/// Single row in the topology tree. Opens/closes via the chevron; the
/// label area is optionally clickable (used on host rows to open the
/// detail drawer). Expansion state is lifted so callers can collapse
/// everything at once or deep-link to a specific host.
function TreeRow({
  level,
  label,
  badge,
  rightAccessory,
  path,
  expanded,
  setExpanded,
  hasChildren,
  children,
  onSelect,
  selected,
}: TreeRowProps) {
  const open = expanded.has(path);
  const meta = LEVEL_META[level];
  const Icon = meta.icon;
  const toggle = () => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };
  return (
    <div className="relative">
      <div
        className={`group flex items-center gap-1.5 text-[12px] rounded-lg border px-2 py-1 ${
          meta.color
        } ${selected ? "ring-2 ring-blue-400" : ""} ${
          onSelect ? "cursor-pointer hover:ring-1 hover:ring-blue-200" : ""
        }`}
      >
        {hasChildren ? (
          <button
            onClick={(e) => {
              e.stopPropagation();
              toggle();
            }}
            className="text-current opacity-60 hover:opacity-100"
            aria-label={open ? "Collapse" : "Expand"}
          >
            {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          </button>
        ) : (
          <span className="w-3" />
        )}
        <Icon size={12} />
        <span className="text-[10px] uppercase tracking-wider opacity-60">
          {level}
        </span>
        <span
          onClick={onSelect}
          className={`font-mono font-medium ${onSelect ? "flex-1 truncate" : ""}`}
        >
          {label || "(none)"}
        </span>
        {badge && (
          <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-white/70 border border-current/10 opacity-80 font-normal">
            {badge}
          </span>
        )}
        {rightAccessory}
      </div>
      {hasChildren && open && (
        <div className="relative ml-3 pl-4 mt-1.5 border-l border-dashed border-gray-200 space-y-1.5">
          {children}
        </div>
      )}
    </div>
  );
}

/// Cluster Topology — hierarchical tree + selectable host rows with a
/// right-side detail drawer (mirrors the mock's Cluster Topology screen).
export default function Topology() {
  const [topology, setTopology] = useState<TopologyData | null>(null);
  const [osdNodes, setOsdNodes] = useState<NodeInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshKey, setRefreshKey] = useState(0);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selectedHost, setSelectedHost] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      try {
        const [t, n] = await Promise.all([
          fetch("/_admin/topology").then((r) => r.json()),
          nodesApi.list(),
        ]);
        if (cancelled) return;
        setTopology(t);
        setOsdNodes(n.nodes || []);
        // Auto-expand everything so the tree reads as a diagram on first
        // view. Users can collapse anything they don't care about.
        const paths = new Set<string>();
        for (const r of (t.tree || []) as RegionNode[]) {
          paths.add(`r:${r.region}`);
          for (const z of r.zones) {
            paths.add(`r:${r.region}/z:${z.zone}`);
            for (const d of z.datacenters) {
              paths.add(`r:${r.region}/z:${z.zone}/d:${d.datacenter}`);
              for (const rk of d.racks) {
                paths.add(
                  `r:${r.region}/z:${z.zone}/d:${d.datacenter}/rk:${rk.rack}`,
                );
              }
            }
          }
        }
        setExpanded(paths);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  // Fast lookup from host name → OSDs on that host. Used both to size
  // tree nodes and to populate the detail drawer.
  const osdsByHost = useMemo(() => {
    const m = new Map<string, NodeInfo[]>();
    for (const osd of osdNodes) {
      const key =
        osd.hostname || osd.kubernetes_node || osd.node_name || "(unknown)";
      if (!m.has(key)) m.set(key, []);
      m.get(key)!.push(osd);
    }
    return m;
  }, [osdNodes]);

  // Derived host health summary for the top strip.
  const hostSummary = useMemo(() => {
    let up = 0,
      warn = 0,
      down = 0;
    for (const osds of osdsByHost.values()) {
      const allUp = osds.every((o) => o.online);
      const allDown = osds.every((o) => !o.online);
      if (allDown) down += 1;
      else if (allUp) up += 1;
      else warn += 1;
    }
    return { up, warn, down };
  }, [osdsByHost]);

  const selectedOsds =
    selectedHost != null ? (osdsByHost.get(selectedHost) ?? []) : [];

  // Discover the topology path to the selected host for the drawer
  // breadcrumb — walk the tree so region/zone/dc/rack match what's
  // actually drawn on the left.
  const selectedPath = useMemo(() => {
    if (!selectedHost || !topology) return null;
    for (const r of topology.tree) {
      for (const z of r.zones) {
        for (const d of z.datacenters) {
          for (const rk of d.racks) {
            for (const h of rk.hosts) {
              if (h.host === selectedHost) {
                return {
                  region: r.region,
                  zone: z.zone,
                  datacenter: d.datacenter,
                  rack: rk.rack,
                };
              }
            }
          }
        }
      }
    }
    return null;
  }, [selectedHost, topology]);

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Main tree area */}
      <div className="flex-1 overflow-y-auto p-6 min-w-0">
        <PageHeader
          title="Cluster Topology"
          description="Visual hierarchy and status of physical storage resources."
          action={
            <div className="flex items-center gap-2">
              <StatusDot status="healthy" label={`${hostSummary.up} Hosts Up`} size="md" />
              <StatusDot status="warning" label={`${hostSummary.warn} Warn`} size="md" />
              <StatusDot status="error" label={`${hostSummary.down} Down`} size="md" />
              <button
                onClick={() => setRefreshKey((k) => k + 1)}
                className="flex items-center gap-1.5 px-2.5 py-1.5 border border-gray-200 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
              >
                <RefreshCw size={13} /> Refresh
              </button>
            </div>
          }
        />

        {loading && !topology && (
          <div className="text-[12px] text-gray-400">Loading topology…</div>
        )}

        {topology && (
          <Card title="Topology tree">
            {topology.tree.length === 0 ? (
              <div className="text-[12px] text-gray-400 italic py-6 text-center">
                No OSDs registered.
              </div>
            ) : (
              <div className="space-y-3 overflow-x-auto py-2">
                {topology.tree.map((r) => {
                  const regionOsds = r.zones.reduce(
                    (n, z) =>
                      n +
                      z.datacenters.reduce(
                        (dn, d) =>
                          dn +
                          d.racks.reduce(
                            (rn, rk) =>
                              rn + rk.hosts.reduce((hn, h) => hn + h.osds.length, 0),
                            0,
                          ),
                        0,
                      ),
                    0,
                  );
                  const rKey = `r:${r.region}`;
                  return (
                    <TreeRow
                      key={rKey}
                      level="region"
                      label={r.region}
                      badge={`${r.zones.length} Zone${r.zones.length !== 1 ? "s" : ""}  ${regionOsds} OSDs`}
                      path={rKey}
                      expanded={expanded}
                      setExpanded={setExpanded}
                      hasChildren
                    >
                      {r.zones.map((z) => {
                        const zKey = `${rKey}/z:${z.zone}`;
                        const zoneOsds = z.datacenters.reduce(
                          (n, d) =>
                            n +
                            d.racks.reduce(
                              (rn, rk) =>
                                rn + rk.hosts.reduce((hn, h) => hn + h.osds.length, 0),
                              0,
                            ),
                          0,
                        );
                        return (
                          <TreeRow
                            key={zKey}
                            level="zone"
                            label={z.zone}
                            badge={`${zoneOsds} OSDs`}
                            path={zKey}
                            expanded={expanded}
                            setExpanded={setExpanded}
                            hasChildren
                          >
                            {z.datacenters.map((d) => {
                              const dKey = `${zKey}/d:${d.datacenter}`;
                              const dcOsds = d.racks.reduce(
                                (n, rk) =>
                                  n + rk.hosts.reduce((hn, h) => hn + h.osds.length, 0),
                                0,
                              );
                              return (
                                <TreeRow
                                  key={dKey}
                                  level="datacenter"
                                  label={d.datacenter}
                                  badge={`${dcOsds} OSDs`}
                                  path={dKey}
                                  expanded={expanded}
                                  setExpanded={setExpanded}
                                  hasChildren
                                >
                                  {d.racks.map((rk) => {
                                    const rkKey = `${dKey}/rk:${rk.rack}`;
                                    const rackOsds = rk.hosts.reduce(
                                      (n, h) => n + h.osds.length,
                                      0,
                                    );
                                    return (
                                      <TreeRow
                                        key={rkKey}
                                        level="rack"
                                        label={rk.rack}
                                        badge={`${rk.hosts.length} Hosts, ${rackOsds} OSDs`}
                                        path={rkKey}
                                        expanded={expanded}
                                        setExpanded={setExpanded}
                                        hasChildren
                                      >
                                        {rk.hosts.map((h) => {
                                          const hostOsds = osdsByHost.get(h.host) ?? [];
                                          const allUp = hostOsds.every((o) => o.online);
                                          const someUp = hostOsds.some((o) => o.online);
                                          const status =
                                            hostOsds.length === 0
                                              ? "unknown"
                                              : allUp
                                                ? "healthy"
                                                : someUp
                                                  ? "warning"
                                                  : "error";
                                          const total = hostOsds.reduce(
                                            (s, o) => s + o.total_capacity,
                                            0,
                                          );
                                          const used = hostOsds.reduce(
                                            (s, o) => s + o.used_capacity,
                                            0,
                                          );
                                          const pct =
                                            total === 0
                                              ? 0
                                              : Math.round((used / total) * 100);
                                          return (
                                            <TreeRow
                                              key={h.host}
                                              level="host"
                                              label={h.host}
                                              badge={`${h.osds.length} OSDs · ${pct}% util`}
                                              path={`${rkKey}/h:${h.host}`}
                                              expanded={expanded}
                                              setExpanded={setExpanded}
                                              hasChildren={false}
                                              onSelect={() => setSelectedHost(h.host)}
                                              selected={selectedHost === h.host}
                                              rightAccessory={
                                                <StatusDot status={status} />
                                              }
                                            />
                                          );
                                        })}
                                      </TreeRow>
                                    );
                                  })}
                                </TreeRow>
                              );
                            })}
                          </TreeRow>
                        );
                      })}
                    </TreeRow>
                  );
                })}
              </div>
            )}
          </Card>
        )}

        <p className="mt-3 text-[11px] text-gray-400">
          Hierarchy: region → zone → datacenter → rack → host → OSDs. Click a
          host to inspect OSDs and host actions.
        </p>
      </div>

      {/* Right-side detail drawer */}
      {selectedHost != null && (
        <Drawer
          title={selectedHost}
          eyebrow={
            <div className="flex items-center gap-2">
              <span className="inline-flex items-center gap-1.5 px-1.5 py-0.5 rounded bg-blue-50 text-blue-700 text-[10px] font-semibold uppercase tracking-wider">
                Host
              </span>
              <StatusDot
                status={
                  selectedOsds.length === 0
                    ? "unknown"
                    : selectedOsds.every((o) => o.online)
                      ? "healthy"
                      : selectedOsds.some((o) => o.online)
                        ? "warning"
                        : "error"
                }
              />
            </div>
          }
          subtitle={
            selectedPath && (
              <BreadcrumbPath
                segments={[
                  selectedPath.region,
                  selectedPath.zone,
                  selectedPath.datacenter,
                  selectedPath.rack,
                ]}
              />
            )
          }
          onClose={() => setSelectedHost(null)}
          width="w-[22rem]"
        >
          <HostDrawerBody osds={selectedOsds} />
        </Drawer>
      )}
    </div>
  );
}

function HostDrawerBody({ osds }: { osds: NodeInfo[] }) {
  const upCount = osds.filter((o) => o.online).length;
  const total = osds.reduce((s, o) => s + o.total_capacity, 0);
  const used = osds.reduce((s, o) => s + o.used_capacity, 0);
  const allUp = osds.length > 0 && upCount === osds.length;

  return (
    <>
      <div className="grid grid-cols-2 gap-3">
        <StatTile
          label="Status"
          value={
            osds.length === 0 ? "–" : allUp ? "Up / In" : `${upCount}/${osds.length} Up`
          }
          tone={osds.length === 0 ? "muted" : allUp ? "good" : "warn"}
        />
        <StatTile
          label="OSDs"
          value={`${osds.length} Total`}
          tone="muted"
        />
      </div>

      <div>
        <div className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">
          Storage Utilization
        </div>
        <CapacityBar used={used} total={total} showLabel layout="stacked" />
      </div>

      <div>
        <div className="flex items-center justify-between mb-1.5">
          <div className="text-[10px] text-gray-500 uppercase tracking-wider">
            OSD Inventory
          </div>
          <div className="text-[10px] text-gray-400">
            Showing {osds.length} of {osds.length}
          </div>
        </div>
        <ul className="space-y-1">
          {osds.map((osd) => (
            <li
              key={osd.node_id}
              className="flex items-center justify-between bg-gray-50 border border-gray-100 rounded-md px-2 py-1.5"
            >
              <div className="flex items-center gap-2 min-w-0">
                <HardDrive size={12} className="text-gray-400 shrink-0" />
                <div className="min-w-0">
                  <div className="text-[12px] font-medium text-gray-800 truncate">
                    {osd.pod_name || osd.node_name}
                  </div>
                  <div className="text-[10px] text-gray-400 font-mono">
                    {osd.online ? "up / in" : "down"}
                  </div>
                </div>
              </div>
              <div className="text-[11px] text-gray-500 tabular-nums">
                {formatBytes(osd.total_capacity)}
              </div>
            </li>
          ))}
          {osds.length === 0 && (
            <li className="text-[11px] text-gray-400 italic py-2 text-center">
              No OSDs on this host.
            </li>
          )}
        </ul>
      </div>

      <div>
        <div className="text-[10px] text-gray-500 uppercase tracking-wider mb-1.5">
          Host Actions
        </div>
        <div className="grid grid-cols-2 gap-1.5">
          <HostActionButton icon={Power} label="Reboot" disabled />
          <HostActionButton icon={Shuffle} label="Drain" disabled />
        </div>
        <div className="mt-1.5">
          <HostActionButton icon={Ban} label="Mark Out" disabled tone="danger" />
        </div>
        <p className="mt-2 text-[10px] text-gray-400">
          Host actions require CLI integration and are not yet wired to the
          console.
        </p>
      </div>
    </>
  );
}

function StatTile({
  label,
  value,
  tone,
}: {
  label: string;
  value: React.ReactNode;
  tone: "good" | "warn" | "muted";
}) {
  const ring =
    tone === "good"
      ? "border-emerald-200 bg-emerald-50"
      : tone === "warn"
        ? "border-amber-200 bg-amber-50"
        : "border-gray-200 bg-gray-50";
  const valueColor =
    tone === "good"
      ? "text-emerald-700"
      : tone === "warn"
        ? "text-amber-700"
        : "text-gray-800";
  return (
    <div className={`rounded-lg border px-2.5 py-2 ${ring}`}>
      <div className="text-[10px] text-gray-500 uppercase tracking-wider">
        {label}
      </div>
      <div className={`text-[13px] font-semibold mt-0.5 ${valueColor}`}>
        {value}
      </div>
    </div>
  );
}

function HostActionButton({
  icon: Icon,
  label,
  disabled,
  tone = "default",
}: {
  icon: typeof Power;
  label: string;
  disabled?: boolean;
  tone?: "default" | "danger";
}) {
  const palette =
    tone === "danger"
      ? "border-red-200 text-red-600 bg-red-50/30 hover:bg-red-50"
      : "border-gray-200 text-gray-700 hover:bg-gray-50";
  return (
    <button
      disabled={disabled}
      className={`w-full flex items-center justify-center gap-1.5 border rounded-md px-2 py-1.5 text-[11px] font-medium ${palette} disabled:opacity-50 disabled:cursor-not-allowed`}
    >
      <Icon size={12} />
      {label}
    </button>
  );
}
