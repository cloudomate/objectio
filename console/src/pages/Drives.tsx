import { useEffect, useMemo, useState } from "react";
import {
  Server,
  HardDrive,
  RefreshCw,
  Container,
  Search,
  SlidersHorizontal,
  Rows3,
  Plus,
  MoreVertical,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import StatusDot from "../components/StatusDot";
import CapacityBar from "../components/CapacityBar";
import BreadcrumbPath from "../components/BreadcrumbPath";
import ExpandableRow from "../components/ExpandableRow";
import Tabs from "../components/Tabs";
import { nodes as nodesApi, type NodeInfo } from "../api/client";

function formatBytes(b: number): string {
  if (b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(b) / Math.log(1024)));
  const v = b / Math.pow(1024, i);
  return `${v >= 10 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

function formatUptime(seconds: number): string {
  if (!seconds) return "-";
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

interface Host {
  name: string;
  osds: NodeInfo[];
  totalCapacity: number;
  usedCapacity: number;
  totalShards: number;
  cpuCores: number;
  memoryBytes: number;
  osInfo: string;
  allOnline: boolean;
  /// All OSDs report the same k8s node — use it as the rack / host label
  /// when no explicit topology is configured upstream.
  k8sNode: string;
}

/// Node & OSD Management page — expandable host rows → OSD children, with
/// capacity, topology path, and status per row. Mirrors the layout from
/// the product mock.
export default function Drives() {
  const [nodeList, setNodeList] = useState<NodeInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<"all" | "issues">("all");
  const [groupByRack, setGroupByRack] = useState(false);

  const load = () => {
    setLoading(true);
    nodesApi
      .list()
      .then((data) => {
        const nodes = data.nodes || [];
        setNodeList(nodes);
        const k8sNames = new Set(
          nodes.map((n) => n.kubernetes_node || n.hostname || n.node_name),
        );
        setExpanded(k8sNames);
      })
      .catch(() => setNodeList([]))
      .finally(() => setLoading(false));
  };

  useEffect(load, []);

  const hosts: Host[] = useMemo(() => {
    const grouped = new Map<string, NodeInfo[]>();
    for (const osd of nodeList) {
      const key = osd.kubernetes_node || osd.hostname || osd.node_name;
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key)!.push(osd);
    }
    const out: Host[] = [];
    for (const [name, osds] of grouped) {
      out.push({
        name,
        osds,
        totalCapacity: osds.reduce((s, n) => s + n.total_capacity, 0),
        usedCapacity: osds.reduce((s, n) => s + n.used_capacity, 0),
        totalShards: osds.reduce((s, n) => s + n.shard_count, 0),
        cpuCores: osds[0]?.cpu_cores || 0,
        memoryBytes: osds[0]?.memory_bytes || 0,
        osInfo: osds[0]?.os_info || "",
        allOnline: osds.every((n) => n.online),
        k8sNode: osds[0]?.kubernetes_node || name,
      });
    }
    return out;
  }, [nodeList]);

  const issuesCount = hosts.filter((h) => !h.allOnline).length;

  const visibleHosts = hosts
    .filter((h) =>
      query
        ? h.name.toLowerCase().includes(query.toLowerCase()) ||
          h.osds.some((o) =>
            (o.pod_name || o.node_name).toLowerCase().includes(query.toLowerCase()),
          )
        : true,
    )
    .filter((h) => (filter === "issues" ? !h.allOnline : true));

  const toggleExpand = (name: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  return (
    <div className="p-6">
      <PageHeader
        title="Node & OSD Management"
        description="Manage physical hosts and storage daemons across the cluster topology."
        action={
          <div className="flex items-center gap-2">
            <button
              onClick={load}
              className="flex items-center gap-1.5 px-2.5 py-1.5 border border-gray-200 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
            >
              <RefreshCw size={13} /> Refresh
            </button>
            <button
              disabled
              title="Cluster onboarding UI coming soon"
              className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white rounded-lg text-[12px] font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Plus size={13} /> Add Host
            </button>
            <button
              disabled
              title="OSD provisioning UI coming soon"
              className="flex items-center gap-1.5 px-3 py-1.5 border border-gray-200 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Plus size={13} /> Add OSDs
            </button>
          </div>
        }
      />

      {/* Controls bar — search, All / Has Issues pill, filters, group-by-rack */}
      <div className="flex items-center gap-3 mb-4">
        <div className="relative flex-1 max-w-xs">
          <Search
            size={13}
            className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400"
          />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search hosts or OSDs..."
            className="w-full pl-7 pr-2 py-1.5 border border-gray-200 rounded-lg text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>
        <Tabs
          variant="pill"
          active={filter}
          onChange={(k) => setFilter(k)}
          tabs={[
            { key: "all" as const, label: "All Nodes" },
            { key: "issues" as const, label: "Has Issues", count: issuesCount },
          ]}
        />
        <div className="flex-1" />
        <button
          disabled
          title="Advanced filters coming soon"
          className="flex items-center gap-1.5 px-2.5 py-1.5 border border-gray-200 text-gray-600 rounded-lg text-[12px] font-medium hover:bg-gray-50 disabled:opacity-50"
        >
          <SlidersHorizontal size={12} /> Filters
        </button>
        <button
          onClick={() => setGroupByRack((v) => !v)}
          className={`flex items-center gap-1.5 px-2.5 py-1.5 border rounded-lg text-[12px] font-medium ${
            groupByRack
              ? "border-blue-200 bg-blue-50 text-blue-700"
              : "border-gray-200 text-gray-600 hover:bg-gray-50"
          }`}
          title="Topology grouping requires region/zone/dc/rack on the OSD — showing flat list until then"
        >
          <Rows3 size={12} /> Group by Rack
        </button>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        {/* Header */}
        <div className="grid grid-cols-[28px_1fr_120px_260px_220px_140px_40px] items-center gap-2 px-3 py-2 border-b border-gray-200 bg-gray-50 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
          <div />
          <div>Host / OSD</div>
          <div>Status</div>
          <div>Topology (Reg/Zone/DC/Rack)</div>
          <div>Capacity</div>
          <div>Labels</div>
          <div />
        </div>

        {loading ? (
          <div className="p-8 text-center">
            <div className="flex items-center justify-center gap-3">
              <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
              </div>
              <span className="text-[12px] text-gray-400">Loading</span>
            </div>
          </div>
        ) : visibleHosts.length === 0 ? (
          <div className="p-8 text-center text-[12px] text-gray-400">
            {hosts.length === 0 ? "No hosts registered" : "No matches"}
          </div>
        ) : (
          visibleHosts.map((host) => (
            <ExpandableRow
              key={host.name}
              open={expanded.has(host.name)}
              onToggle={() => toggleExpand(host.name)}
              header={
                <div className="grid grid-cols-[1fr_120px_260px_220px_140px_40px] items-center gap-2">
                  <div className="flex items-center gap-2 min-w-0">
                    <Server size={15} className="text-blue-500 shrink-0" />
                    <div className="min-w-0">
                      <div className="text-[13px] font-medium text-gray-900 truncate">
                        {host.name}
                      </div>
                      <div className="text-[11px] text-gray-400 font-mono truncate">
                        {host.osds[0]?.address
                          ?.replace("http://", "")
                          .replace(/:\d+$/, "") || host.k8sNode}
                      </div>
                    </div>
                  </div>
                  <StatusDot
                    status={host.allOnline ? "healthy" : "error"}
                    label={host.allOnline ? "Online" : "Degraded"}
                  />
                  <BreadcrumbPath
                    segments={["—", "—", "—", host.k8sNode]}
                  />
                  <CapacityBar
                    used={host.usedCapacity}
                    total={host.totalCapacity}
                    caption={`${host.osds.length} OSD${host.osds.length !== 1 ? "s" : ""}${issuesCount && !host.allOnline ? ` (${host.osds.filter((o) => !o.online).length} Down)` : ""}`}
                    layout="stacked"
                  />
                  <div className="flex items-center gap-1 flex-wrap">
                    {host.osInfo && (
                      <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-[10px] font-medium">
                        {host.osInfo.split(" ")[0].toLowerCase()}
                      </span>
                    )}
                    {host.cpuCores > 0 && (
                      <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-[10px] font-medium">
                        {host.cpuCores}c
                      </span>
                    )}
                    {host.memoryBytes > 0 && (
                      <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-[10px] font-medium">
                        {formatBytes(host.memoryBytes)}
                      </span>
                    )}
                  </div>
                  <button
                    onClick={(e) => e.stopPropagation()}
                    className="p-1 rounded hover:bg-gray-100 text-gray-400 hover:text-gray-700 justify-self-end"
                    title="More actions (not yet wired)"
                  >
                    <MoreVertical size={14} />
                  </button>
                </div>
              }
            >
              {/* Expanded: OSD rows for this host */}
              {host.osds.map((osd) => (
                <OsdRow key={osd.node_id} osd={osd} />
              ))}
            </ExpandableRow>
          ))
        )}
      </div>
    </div>
  );
}

function OsdRow({ osd }: { osd: NodeInfo }) {
  return (
    <div className="border-b border-gray-100 last:border-0 pl-9">
      <div className="grid grid-cols-[1fr_120px_260px_220px_140px_40px] items-center gap-2 py-2 pr-3">
        <div className="flex items-center gap-2 min-w-0">
          <Container size={13} className="text-orange-500 shrink-0" />
          <div className="min-w-0">
            <div className="text-[12px] font-medium text-gray-800 truncate">
              {osd.pod_name || osd.node_name}
            </div>
            <div className="text-[10px] text-gray-400 font-mono truncate">
              {osd.address.replace("http://", "")}
              {osd.version && ` · v${osd.version}`}
            </div>
          </div>
        </div>
        <StatusDot
          status={osd.online ? "healthy" : "error"}
          label={osd.online ? "In / Up" : "Offline"}
        />
        <div className="text-[11px] text-gray-400 font-mono">
          up {formatUptime(osd.uptime_seconds)}
        </div>
        <CapacityBar
          used={osd.used_capacity}
          total={osd.total_capacity}
          caption={`${osd.disks.length} disk${osd.disks.length !== 1 ? "s" : ""}, ${osd.shard_count.toLocaleString()} shards`}
          layout="stacked"
        />
        <div className="flex items-center gap-1">
          {osd.disks[0]?.status && (
            <span className="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-[10px] font-medium">
              {osd.disks[0].status}
            </span>
          )}
        </div>
        <button
          disabled
          title="OSD management actions coming soon"
          className="px-2 py-1 text-[11px] font-medium text-gray-400 border border-gray-200 rounded justify-self-end disabled:cursor-not-allowed"
        >
          Manage
        </button>
      </div>

      {/* Disks under this OSD */}
      {osd.disks.length > 0 && (
        <div className="pl-6 pr-3 pb-2 space-y-1">
          {osd.disks.map((disk) => (
            <div
              key={disk.disk_id}
              className="grid grid-cols-[1fr_120px_260px_220px_140px_40px] items-center gap-2 text-[11px] text-gray-500"
            >
              <div className="flex items-center gap-2 min-w-0">
                <HardDrive
                  size={11}
                  className={
                    disk.status === "healthy" ? "text-emerald-500" : "text-red-500"
                  }
                />
                <span className="font-mono truncate">
                  {disk.path || disk.disk_id.slice(0, 8)}
                </span>
              </div>
              <StatusDot
                status={disk.status === "healthy" ? "healthy" : "error"}
                label={disk.status}
              />
              <div />
              <CapacityBar
                used={disk.used_capacity}
                total={disk.total_capacity}
                layout="inline"
                showLabel
              />
              <div className="tabular-nums">
                {disk.shard_count.toLocaleString()} shards
              </div>
              <div />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
