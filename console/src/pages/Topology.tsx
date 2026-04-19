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
import {
  nodes as nodesApi,
  hostProvider as hostProviderApi,
  drain as drainApi,
  type NodeInfo,
  type OsdAdminState,
  type HostProviderInfo,
  type DrainStatus,
} from "../api/client";

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
  const [provider, setProvider] = useState<HostProviderInfo | null>(null);
  const [drains, setDrains] = useState<DrainStatus[]>([]);

  useEffect(() => {
    hostProviderApi
      .info()
      .then(setProvider)
      .catch(() =>
        setProvider({
          provider: "noop",
          supports_add_host: false,
          supports_reboot: false,
        }),
      );
  }, []);

  // Pull drain progress alongside node list. Auto-refreshes with the
  // same 15 s cadence as the rest of the page when any OSD is
  // Draining.
  useEffect(() => {
    drainApi
      .status()
      .then((d) => setDrains(d.drains))
      .catch(() => setDrains([]));
  }, [refreshKey]);

  // If any OSD is Draining, auto-refresh every 15 s so the operator
  // sees the shard count drop toward zero (and the auto-finalise to Out
  // when it hits zero) without manually clicking Refresh.
  useEffect(() => {
    const someDraining = osdNodes.some((n) => n.admin_state === "draining");
    if (!someDraining) return;
    const t = setInterval(() => setRefreshKey((k) => k + 1), 15000);
    return () => clearInterval(t);
  }, [osdNodes]);

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

  // Fast lookup from host name → OSDs on that host. The topology tree
  // identifies hosts by the failure-domain `host` field, which in k8s
  // deployments maps to `spec.nodeName` (the worker node). Fall back
  // through alternative identifiers to cover bare-metal or legacy
  // registrations where only hostname / pod name is populated.
  //
  // Two independent dedup concerns:
  //
  //  1. The same OSD can match multiple candidate keys (hostname and
  //     node_name are usually identical on k8s pods), so we track a
  //     per-key Set of node_ids and skip repeats.
  //  2. Stale registrations: if an OSD pod restarted with a fresh
  //     persistent state, meta keeps the old entry until it re-roles
  //     the address. Prefer the online one (online=true over false),
  //     then the most recent — since meta appends newer entries at the
  //     end, a later duplicate wins.
  const osdsByHost = useMemo(() => {
    const m = new Map<string, Map<string, NodeInfo>>();
    for (const osd of osdNodes) {
      const candidates = Array.from(
        new Set(
          [osd.kubernetes_node, osd.hostname, osd.node_name].filter(
            Boolean,
          ) as string[],
        ),
      );
      for (const key of candidates) {
        if (!m.has(key)) m.set(key, new Map());
        const byId = m.get(key)!;
        const existing = byId.get(osd.node_id);
        // Keep the newer/online entry when IDs collide. Typically
        // node_ids ARE unique per OSD registration, so this branch
        // only fires on literal duplicates emitted by meta.
        if (!existing || (!existing.online && osd.online)) {
          byId.set(osd.node_id, osd);
        }
      }
    }
    // Collapse to arrays and also dedupe by address as a last-resort
    // guard against stale same-address / different-node_id ghosts —
    // meta evicts those on re-registration but they survive briefly
    // between heartbeats.
    const out = new Map<string, NodeInfo[]>();
    for (const [host, byId] of m) {
      const seenAddr = new Set<string>();
      const list: NodeInfo[] = [];
      for (const osd of byId.values()) {
        if (seenAddr.has(osd.address)) continue;
        seenAddr.add(osd.address);
        list.push(osd);
      }
      out.set(host, list);
    }
    return out;
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
          <HostDrawerBody
            osds={selectedOsds}
            provider={provider}
            drains={drains}
            onChanged={() => setRefreshKey((k) => k + 1)}
          />
        </Drawer>
      )}
    </div>
  );
}

function HostDrawerBody({
  osds,
  provider,
  drains,
  onChanged,
}: {
  osds: NodeInfo[];
  provider: HostProviderInfo | null;
  drains: DrainStatus[];
  onChanged: () => void;
}) {
  const [busy, setBusy] = useState<null | OsdAdminState | "reboot">(null);
  const [error, setError] = useState<string | null>(null);
  // Per-OSD busy state — keyed by node_id so clicking Out on one OSD
  // doesn't grey out the buttons on the others. Host-level batch ops
  // still use `busy` above; the two coexist.
  const [busyOsd, setBusyOsd] = useState<Record<string, OsdAdminState>>({});

  const setOneState = async (nodeId: string, target: OsdAdminState) => {
    setError(null);
    setBusyOsd((b) => ({ ...b, [nodeId]: target }));
    try {
      await nodesApi.setAdminState(nodeId, target);
      onChanged();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyOsd((b) => {
        const next = { ...b };
        delete next[nodeId];
        return next;
      });
    }
  };

  const reboot = async () => {
    setBusy("reboot");
    setError(null);
    try {
      // Rebooting one OSD pod is enough in k8s — the StatefulSet's
      // other pods stay up. For BYO-Linux / appliance hosts with
      // multiple OSDs per host, the provider itself decides whether
      // to recycle the whole host or just one OSD service.
      const first = osds[0];
      if (!first) return;
      await nodesApi.reboot(first.node_id);
      setTimeout(onChanged, 6000);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  };

  const upCount = osds.filter((o) => o.online).length;
  const total = osds.reduce((s, o) => s + o.total_capacity, 0);
  const used = osds.reduce((s, o) => s + o.used_capacity, 0);
  const allUp = osds.length > 0 && upCount === osds.length;
  // Split by operator intent. The topology tree shows only In OSDs,
  // which is how the rack/host "5 OSDs" count is derived. Without a
  // breakdown, a host that has 5 In + 4 Out reads as "9 Total" in the
  // drawer — confusing. Display In / Draining / Out explicitly so
  // the numbers reconcile with the tree at a glance.
  const inOsds = osds.filter((o) => (o.admin_state ?? "in") === "in");
  const drainingOsds = osds.filter((o) => o.admin_state === "draining");
  const outOsds = osds.filter((o) => o.admin_state === "out");
  // Sort the inventory: In first, Draining next, Out last — matches
  // the operational priority an operator triaging the host cares about.
  const orderedOsds = [...inOsds, ...drainingOsds, ...outOsds];

  // Host-level state summary: the drawer acts on ALL OSDs on the host
  // at once. Show the most restrictive state (Out > Draining > In) so
  // the operator can see "at least one OSD is already Out" without
  // clicking through each row.
  const effective: OsdAdminState = osds.some((o) => o.admin_state === "out")
    ? "out"
    : osds.some((o) => o.admin_state === "draining")
      ? "draining"
      : "in";
  // Sum shards across Draining OSDs so the drawer can render drain
  // progress. Meta's leader-only observer flips Draining → Out
  // automatically once a given OSD's count reaches 0.
  const drainingShardsRemaining = osds
    .filter((o) => o.admin_state === "draining")
    .reduce((s, o) => s + (o.shard_count ?? 0), 0);

  const applyToAll = async (target: OsdAdminState) => {
    setBusy(target);
    setError(null);
    try {
      await Promise.all(
        osds.map((o) => nodesApi.setAdminState(o.node_id, target)),
      );
      onChanged();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  };

  return (
    <>
      <div className="grid grid-cols-2 gap-3">
        <StatTile
          label="Status"
          value={
            osds.length === 0
              ? "–"
              : effective === "out"
                ? "Out"
                : effective === "draining"
                  ? "Draining"
                  : allUp
                    ? "Up / In"
                    : `${upCount}/${osds.length} Up`
          }
          tone={
            osds.length === 0
              ? "muted"
              : effective === "out"
                ? "warn"
                : effective === "draining"
                  ? "warn"
                  : allUp
                    ? "good"
                    : "warn"
          }
        />
        <StatTile
          label="OSDs"
          value={
            outOsds.length + drainingOsds.length === 0
              ? `${inOsds.length} In`
              : [
                  inOsds.length && `${inOsds.length} In`,
                  drainingOsds.length && `${drainingOsds.length} Drn`,
                  outOsds.length && `${outOsds.length} Out`,
                ]
                  .filter(Boolean)
                  .join(" · ")
          }
          tone="muted"
        />
      </div>

      <div>
        <div className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">
          Storage Utilization
        </div>
        <CapacityBar used={used} total={total} showLabel layout="stacked" />
      </div>

      {effective === "draining" && (
        <DrainProgressPanel
          drainingShardsRemaining={drainingShardsRemaining}
          drains={drains}
          drainingOsdIds={drainingOsds.map((o) => o.node_id)}
        />
      )}

      <div>
        <div className="flex items-center justify-between mb-1.5">
          <div className="text-[10px] text-gray-500 uppercase tracking-wider">
            OSD Inventory
          </div>
          <div className="text-[10px] text-gray-400">
            {orderedOsds.length} total
          </div>
        </div>
        <ul className="space-y-1">
          {orderedOsds.map((osd) => {
            const adminState = osd.admin_state ?? "in";
            const muted = adminState !== "in";
            const busyFor = busyOsd[osd.node_id];
            const rowBusy = busyFor !== undefined;
            return (
              <li
                key={osd.node_id}
                className={`flex items-center justify-between gap-2 border rounded-md px-2 py-1.5 ${
                  muted
                    ? "bg-gray-50/50 border-gray-100"
                    : "bg-gray-50 border-gray-100"
                }`}
              >
                <div className="flex items-center gap-2 min-w-0 flex-1">
                  <HardDrive
                    size={12}
                    className={muted ? "text-gray-300 shrink-0" : "text-gray-400 shrink-0"}
                  />
                  <div className="min-w-0">
                    <div
                      className={`text-[12px] font-medium truncate ${
                        muted ? "text-gray-500" : "text-gray-800"
                      }`}
                    >
                      {osd.pod_name || osd.node_name}
                    </div>
                    <div className="text-[10px] text-gray-400 font-mono">
                      {osd.online ? "up" : "down"} / {adminState}
                    </div>
                  </div>
                </div>
                <div className="text-[11px] text-gray-500 tabular-nums shrink-0">
                  {formatBytes(osd.total_capacity)}
                </div>
                {/* Per-OSD action: swap to the opposite state. Keep
                    it compact (one button, not three) since host-level
                    batch controls below cover the full matrix. */}
                <OsdRowAction
                  current={adminState}
                  busy={busyFor}
                  disabled={rowBusy || busy !== null}
                  onToggle={(next) => setOneState(osd.node_id, next)}
                />
              </li>
            );
          })}
          {orderedOsds.length === 0 && (
            <li className="text-[11px] text-gray-400 italic py-2 text-center">
              No OSDs on this host.
            </li>
          )}
        </ul>
      </div>

      <div>
        <div className="text-[10px] text-gray-500 uppercase tracking-wider mb-1.5">
          Host Actions <span className="text-gray-400 normal-case">— all OSDs</span>
        </div>
        {error && (
          <div className="mb-2 px-2 py-1 rounded bg-red-50 border border-red-200 text-[11px] text-red-700">
            {error}
          </div>
        )}
        <div className="grid grid-cols-2 gap-1.5">
          <HostActionButton
            icon={Power}
            label={busy === "reboot" ? "Rebooting…" : "Reboot"}
            disabled={
              busy !== null ||
              osds.length === 0 ||
              !(provider?.supports_reboot ?? false)
            }
            onClick={reboot}
            title={
              provider?.supports_reboot
                ? `Delete the first OSD's pod (${provider.provider})`
                : "Requires a host provider. Set --host-provider=k8s on the gateway."
            }
          />
          <HostActionButton
            icon={Shuffle}
            label={busy === "draining" ? "Draining…" : "Drain"}
            disabled={busy !== null || effective === "draining" || osds.length === 0}
            onClick={() => applyToAll("draining")}
            title="Mark all OSDs on this host as Draining (no new shards; real migration comes in Phase 3)"
          />
        </div>
        <div className="mt-1.5 grid grid-cols-2 gap-1.5">
          <HostActionButton
            icon={Ban}
            label={busy === "out" ? "Marking…" : "Mark Out"}
            disabled={busy !== null || effective === "out" || osds.length === 0}
            onClick={() => applyToAll("out")}
            tone="danger"
            title="Immediately remove from placement"
          />
          <HostActionButton
            icon={Power}
            label={busy === "in" ? "Returning…" : "Mark In"}
            disabled={busy !== null || effective === "in" || osds.length === 0}
            onClick={() => applyToAll("in")}
            title="Return to placement"
          />
        </div>
        <p className="mt-2 text-[10px] text-gray-400">
          Mark Out / Mark In / Drain persist through Raft. Reboot uses
          the configured platform provider ({provider?.provider ?? "…"}).
          Real shard migration for Drain is Phase 3.
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

/// Progress card for the Draining OSDs on the selected host. Sums
/// initial/migrated/remaining across every Draining OSD on the host so
/// a multi-OSD host drains as one visual bar. `last_error` from any
/// Draining OSD surfaces inline — surfaces real failures instead of
/// silent stalls.
function DrainProgressPanel({
  drainingShardsRemaining,
  drains,
  drainingOsdIds,
}: {
  drainingShardsRemaining: number;
  drains: DrainStatus[];
  drainingOsdIds: string[];
}) {
  const ours = drains.filter((d) => drainingOsdIds.includes(d.node_id));
  const initial = ours.reduce((s, d) => s + d.initial_shards, 0);
  const remaining = ours.reduce((s, d) => s + d.shards_remaining, 0);
  const migrated = ours.reduce((s, d) => s + d.shards_migrated, 0);
  const pct = initial > 0
    ? Math.min(100, Math.round(((initial - remaining) / initial) * 100))
    : 0;
  const lastError = ours.find((d) => d.last_error)?.last_error ?? "";

  return (
    <div className="rounded-md bg-amber-50 border border-amber-200 px-2.5 py-2">
      <div className="flex items-center justify-between gap-2">
        <div className="text-[11px] font-semibold text-amber-800">Draining</div>
        <div className="text-[11px] text-amber-800 tabular-nums">
          {(initial > 0 ? remaining : drainingShardsRemaining).toLocaleString()}{" "}
          shards remaining
        </div>
      </div>
      {initial > 0 && (
        <>
          <div className="mt-1.5 h-1.5 bg-amber-100 rounded-full overflow-hidden">
            <div
              className="h-full bg-amber-500 rounded-full transition-all"
              style={{ width: `${pct}%` }}
            />
          </div>
          <div className="mt-1 flex items-center justify-between text-[10px] text-amber-800/90 tabular-nums">
            <span>
              {migrated.toLocaleString()} of {initial.toLocaleString()} migrated
            </span>
            <span>{pct}%</span>
          </div>
        </>
      )}
      {lastError && (
        <p className="mt-1.5 text-[10px] text-red-700 truncate" title={lastError}>
          last error: {lastError}
        </p>
      )}
      <p className="mt-1.5 text-[10px] text-amber-700/80">
        Meta leader migrates one shard per OSD every 30 s. When the count
        hits zero the OSD auto-finalises to Out.
      </p>
    </div>
  );
}

/// Single compact action on an OSD row: flips it to whichever state
/// isn't current. From `in`, the next logical action is Out (fast
/// take-out-of-service). From `out` or `draining`, it's In (return
/// to placement). That mirrors what an operator actually clicks, and
/// keeps the row from ballooning into three buttons.
function OsdRowAction({
  current,
  busy,
  disabled,
  onToggle,
}: {
  current: OsdAdminState;
  busy: OsdAdminState | undefined;
  disabled: boolean;
  onToggle: (next: OsdAdminState) => void;
}) {
  const next: OsdAdminState = current === "in" ? "out" : "in";
  const labelBase = next === "in" ? "In" : "Out";
  const label = busy === next
    ? next === "in"
      ? "…"
      : "…"
    : labelBase;
  const tone =
    next === "out"
      ? "border-red-200 text-red-600 hover:bg-red-50"
      : "border-emerald-200 text-emerald-700 hover:bg-emerald-50";
  return (
    <button
      disabled={disabled}
      onClick={() => onToggle(next)}
      title={
        next === "out"
          ? "Take this OSD out of placement"
          : "Return this OSD to placement"
      }
      className={`text-[10px] font-semibold uppercase tracking-wider border rounded px-1.5 py-0.5 shrink-0 w-9 text-center ${tone} disabled:opacity-40 disabled:cursor-not-allowed`}
    >
      {label}
    </button>
  );
}

function HostActionButton({
  icon: Icon,
  label,
  disabled,
  tone = "default",
  onClick,
  title,
}: {
  icon: typeof Power;
  label: string;
  disabled?: boolean;
  tone?: "default" | "danger";
  onClick?: () => void;
  title?: string;
}) {
  const palette =
    tone === "danger"
      ? "border-red-200 text-red-600 bg-red-50/30 hover:bg-red-50"
      : "border-gray-200 text-gray-700 hover:bg-gray-50";
  return (
    <button
      disabled={disabled}
      onClick={onClick}
      title={title}
      className={`w-full flex items-center justify-center gap-1.5 border rounded-md px-2 py-1.5 text-[11px] font-medium ${palette} disabled:opacity-50 disabled:cursor-not-allowed`}
    >
      <Icon size={12} />
      {label}
    </button>
  );
}
