import { useEffect, useState } from "react";
import {
  Server,
  HardDrive,
  ChevronDown,
  ChevronRight,
  RefreshCw,
  Container,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import StatusBadge from "../components/StatusBadge";
import { nodes as nodesApi, type NodeInfo } from "../api/client";

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function formatUptime(seconds: number): string {
  if (seconds === 0) return "-";
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

function UsageBar({ used, total }: { used: number; total: number }) {
  const pct = total === 0 ? 0 : Math.round((used / total) * 100);
  const color =
    pct > 90 ? "bg-red-500" : pct > 70 ? "bg-yellow-500" : "bg-blue-500";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-gray-200 rounded-full overflow-hidden">
        <div
          className={`h-full ${color} rounded-full`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-[10px] text-gray-500 w-8 text-right">{pct}%</span>
    </div>
  );
}

interface K8sNode {
  name: string;
  osds: NodeInfo[];
  totalCapacity: number;
  usedCapacity: number;
  totalShards: number;
  cpuCores: number;
  memoryBytes: number;
  osInfo: string;
  allOnline: boolean;
}

export default function Drives() {
  const [nodeList, setNodeList] = useState<NodeInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const load = () => {
    setLoading(true);
    nodesApi
      .list()
      .then((data) => {
        const nodes = data.nodes || [];
        setNodeList(nodes);
        // Auto-expand all K8s nodes
        const k8sNames = new Set(
          nodes.map((n) => n.kubernetes_node || n.hostname || n.node_name)
        );
        setExpanded(k8sNames);
      })
      .catch(() => setNodeList([]))
      .finally(() => setLoading(false));
  };

  useEffect(load, []);

  // Group OSDs by K8s node
  const k8sNodes: K8sNode[] = [];
  const grouped = new Map<string, NodeInfo[]>();
  for (const osd of nodeList) {
    const key = osd.kubernetes_node || osd.hostname || osd.node_name;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(osd);
  }
  for (const [name, osds] of grouped) {
    k8sNodes.push({
      name,
      osds,
      totalCapacity: osds.reduce((s, n) => s + n.total_capacity, 0),
      usedCapacity: osds.reduce((s, n) => s + n.used_capacity, 0),
      totalShards: osds.reduce((s, n) => s + n.shard_count, 0),
      cpuCores: osds[0]?.cpu_cores || 0,
      memoryBytes: osds[0]?.memory_bytes || 0,
      osInfo: osds[0]?.os_info || "",
      allOnline: osds.every((n) => n.online),
    });
  }

  const toggleExpand = (name: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const totalOsds = nodeList.length;
  const onlineOsds = nodeList.filter((n) => n.online).length;
  const totalCapacity = nodeList.reduce((s, n) => s + n.total_capacity, 0);
  const totalUsed = nodeList.reduce((s, n) => s + n.used_capacity, 0);
  const totalShards = nodeList.reduce((s, n) => s + n.shard_count, 0);

  return (
    <div className="p-6">
      <PageHeader
        title="Nodes & Drives"
        description="Physical nodes, OSD instances, and disk health"
        action={
          <button
            onClick={load}
            className="flex items-center gap-1.5 px-3 py-1.5 border border-gray-200 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
          >
            <RefreshCw size={13} /> Refresh
          </button>
        }
      />

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
        <div className="bg-white rounded-xl border border-gray-200 p-3">
          <p className="text-[10px] text-gray-500 uppercase font-medium">
            Nodes
          </p>
          <p className="text-xl font-semibold mt-0.5">{k8sNodes.length}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3">
          <p className="text-[10px] text-gray-500 uppercase font-medium">
            OSDs
          </p>
          <p className="text-xl font-semibold mt-0.5">
            {onlineOsds}
            <span className="text-[12px] text-gray-400 font-normal">
              /{totalOsds}
            </span>
          </p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3">
          <p className="text-[10px] text-gray-500 uppercase font-medium">
            Total Disks
          </p>
          <p className="text-xl font-semibold mt-0.5">
            {nodeList.reduce((s, n) => s + n.disks.length, 0)}
          </p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3">
          <p className="text-[10px] text-gray-500 uppercase font-medium">
            Capacity
          </p>
          <p className="text-xl font-semibold mt-0.5">
            {formatBytes(totalCapacity)}
          </p>
          <UsageBar used={totalUsed} total={totalCapacity} />
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3">
          <p className="text-[10px] text-gray-500 uppercase font-medium">
            Shards
          </p>
          <p className="text-xl font-semibold mt-0.5">
            {totalShards.toLocaleString()}
          </p>
        </div>
      </div>

      {/* Node list */}
      <div className="space-y-3">
        {loading ? (
          <div className="bg-white rounded-xl border border-gray-200 p-8 text-center">
            <div className="flex items-center justify-center gap-3">
              <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
              </div>
              <span className="text-[12px] text-gray-400">Loading</span>
            </div>
          </div>
        ) : k8sNodes.length === 0 ? (
          <div className="bg-white rounded-xl border border-gray-200 p-8 text-center text-[12px] text-gray-400">
            No nodes found
          </div>
        ) : (
          k8sNodes.map((k8s) => (
            <div
              key={k8s.name}
              className="bg-white rounded-xl border border-gray-200 overflow-hidden"
            >
              {/* K8s Node header */}
              <button
                onClick={() => toggleExpand(k8s.name)}
                className="w-full flex items-center justify-between px-4 py-3 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center gap-3">
                  {expanded.has(k8s.name) ? (
                    <ChevronDown size={14} className="text-gray-400" />
                  ) : (
                    <ChevronRight size={14} className="text-gray-400" />
                  )}
                  <Server size={15} className="text-blue-500" />
                  <div className="text-left">
                    <div className="flex items-center gap-2">
                      <span className="text-[13px] font-medium">
                        {k8s.name}
                      </span>
                    </div>
                    <div className="flex items-center gap-3 text-[11px] text-gray-400 mt-0.5">
                      {k8s.osInfo && <span>{k8s.osInfo}</span>}
                      {k8s.cpuCores > 0 && <span>{k8s.cpuCores} CPU</span>}
                      {k8s.memoryBytes > 0 && (
                        <span>{formatBytes(k8s.memoryBytes)} RAM</span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-4">
                  <div className="text-[11px] text-gray-500">
                    <span className="font-medium text-gray-700">
                      {k8s.osds.length}
                    </span>{" "}
                    OSD{k8s.osds.length !== 1 ? "s" : ""}
                  </div>
                  <div className="text-[11px] text-gray-500">
                    {formatBytes(k8s.usedCapacity)} /{" "}
                    {formatBytes(k8s.totalCapacity)}
                  </div>
                  <StatusBadge
                    status={k8s.allOnline ? "healthy" : "error"}
                    label={k8s.allOnline ? "Online" : "Degraded"}
                  />
                </div>
              </button>

              {/* OSD list under this node */}
              {expanded.has(k8s.name) && (
                <div className="border-t border-gray-100">
                  {k8s.osds.map((osd) => (
                    <div key={osd.node_id} className="border-b border-gray-50 last:border-0">
                      {/* OSD row */}
                      <div className="px-4 py-2.5 pl-12 flex items-center justify-between">
                        <div className="flex items-center gap-2.5">
                          <Container size={14} className="text-orange-500" />
                          <div>
                            <div className="flex items-center gap-2">
                              <span className="text-[12px] font-medium">
                                {osd.pod_name || osd.node_name}
                              </span>
                              {osd.version && (
                                <span className="text-[10px] text-gray-400">
                                  v{osd.version}
                                </span>
                              )}
                            </div>
                            <span className="text-[10px] text-gray-400 font-mono">
                              {osd.address.replace("http://", "")}
                            </span>
                          </div>
                        </div>
                        <div className="flex items-center gap-4">
                          <div className="text-[11px] text-gray-500">
                            {osd.disks.length} disk{osd.disks.length !== 1 ? "s" : ""}
                          </div>
                          <div className="text-[11px] text-gray-500">
                            {formatBytes(osd.used_capacity)} / {formatBytes(osd.total_capacity)}
                          </div>
                          <div className="text-[11px] text-gray-500">
                            {formatUptime(osd.uptime_seconds)}
                          </div>
                          <StatusBadge
                            status={osd.online ? "healthy" : "error"}
                            label={osd.online ? "Online" : "Offline"}
                          />
                        </div>
                      </div>

                      {/* Disks under this OSD */}
                      {osd.disks.length > 0 && (
                        <div className="pl-20 pr-4 pb-2">
                          {osd.disks.map((disk) => (
                            <div
                              key={disk.disk_id}
                              className="flex items-center justify-between py-1.5 text-[11px]"
                            >
                              <div className="flex items-center gap-2">
                                <HardDrive
                                  size={12}
                                  className={
                                    disk.status === "healthy"
                                      ? "text-green-500"
                                      : "text-red-500"
                                  }
                                />
                                <span className="font-mono text-gray-600">
                                  {disk.path || disk.disk_id.slice(0, 8)}
                                </span>
                              </div>
                              <div className="flex items-center gap-4">
                                <span className="text-gray-500">
                                  {formatBytes(disk.total_capacity)}
                                </span>
                                <div className="w-24">
                                  <UsageBar
                                    used={disk.used_capacity}
                                    total={disk.total_capacity}
                                  />
                                </div>
                                <span className="text-gray-500 w-12 text-right">
                                  {disk.shard_count} shards
                                </span>
                                <StatusBadge
                                  status={
                                    disk.status === "healthy"
                                      ? "healthy"
                                      : "error"
                                  }
                                  label={disk.status}
                                />
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
