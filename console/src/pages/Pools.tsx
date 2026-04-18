import { useEffect, useState } from "react";
import { Layers, Plus, Trash2, Edit, Shield } from "lucide-react";
import PageHeader from "../components/PageHeader";
import Card from "../components/Card";
import StatusBadge from "../components/StatusBadge";

interface Pool {
  name: string;
  ec_type: number;
  ec_k: number;
  ec_m: number;
  ec_local_parity: number;
  ec_global_parity: number;
  replication_count: number;
  osd_tags: string[];
  failure_domain: string;
  quota_bytes: number;
  description: string;
  enabled: boolean;
  created_at: number;
}

interface Preset {
  label: string;
  ec_type: number;
  ec_k: number;
  ec_m: number;
  ec_local_parity: number;
  ec_global_parity: number;
  replication_count: number;
  min_nodes: number;
  efficiency: string;
}

const PRESETS: Preset[] = [
  { label: "4+2 RS", ec_type: 0, ec_k: 4, ec_m: 2, ec_local_parity: 0, ec_global_parity: 0, replication_count: 0, min_nodes: 6, efficiency: "67%" },
  { label: "3+2 RS", ec_type: 0, ec_k: 3, ec_m: 2, ec_local_parity: 0, ec_global_parity: 0, replication_count: 0, min_nodes: 5, efficiency: "60%" },
  { label: "8+4 RS", ec_type: 0, ec_k: 8, ec_m: 4, ec_local_parity: 0, ec_global_parity: 0, replication_count: 0, min_nodes: 12, efficiency: "67%" },
  { label: "6+2+1 LRC", ec_type: 1, ec_k: 6, ec_m: 3, ec_local_parity: 2, ec_global_parity: 1, replication_count: 0, min_nodes: 9, efficiency: "67%" },
  { label: "10+2+2 LRC", ec_type: 1, ec_k: 10, ec_m: 4, ec_local_parity: 2, ec_global_parity: 2, replication_count: 0, min_nodes: 14, efficiency: "71%" },
  { label: "3-way Replication", ec_type: 2, ec_k: 0, ec_m: 0, ec_local_parity: 0, ec_global_parity: 0, replication_count: 3, min_nodes: 3, efficiency: "33%" },
  { label: "2-way Replication", ec_type: 2, ec_k: 0, ec_m: 0, ec_local_parity: 0, ec_global_parity: 0, replication_count: 2, min_nodes: 2, efficiency: "50%" },
];

const emptyPool: Omit<Pool, "created_at"> = {
  name: "", ec_type: 0, ec_k: 4, ec_m: 2,
  ec_local_parity: 0, ec_global_parity: 0, replication_count: 3,
  osd_tags: [], failure_domain: "rack", quota_bytes: 0,
  description: "", enabled: true,
};

function formatBytes(b: number) {
  if (!b) return "Unlimited";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(b) / Math.log(1024));
  return `${(b / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function ecLabel(p: Pool) {
  if (p.ec_type === 2) return `${p.replication_count}x Replication`;
  if (p.ec_type === 1) return `LRC ${p.ec_k}+${p.ec_local_parity}+${p.ec_global_parity}`;
  return `${p.ec_k}+${p.ec_m} RS`;
}

function ecEfficiency(p: Pool) {
  if (p.ec_type === 2) return `${(100 / (p.replication_count || 1)).toFixed(0)}%`;
  const total = p.ec_k + p.ec_m;
  if (total === 0) return "-";
  return `${((p.ec_k / total) * 100).toFixed(0)}%`;
}

function ecMinNodes(p: Pool) {
  if (p.ec_type === 2) return p.replication_count;
  return p.ec_k + p.ec_m;
}

function ecTypeBadge(ec_type: number) {
  if (ec_type === 1) return "bg-orange-100 text-orange-800";
  if (ec_type === 2) return "bg-blue-100 text-blue-800";
  return "bg-green-100 text-green-800";
}

function ecTypeName(ec_type: number) {
  if (ec_type === 1) return "LRC";
  if (ec_type === 2) return "Replication";
  return "Reed-Solomon";
}

export default function Pools() {
  const [pools, setPools] = useState<Pool[]>([]);
  const [editing, setEditing] = useState<string | null>(null);
  const [form, setForm] = useState(emptyPool);
  const [tagsStr, setTagsStr] = useState("");
  const [loading, setLoading] = useState(true);
  const [osdCount, setOsdCount] = useState(0);
  const [k8sNodeCount, setK8sNodeCount] = useState(0);

  const load = () => {
    setLoading(true);
    Promise.all([
      fetch("/_admin/pools").then((r) => r.json()).catch(() => []),
      fetch("/_admin/nodes").then((r) => r.json()).catch(() => ({ nodes: [] })),
    ]).then(([p, n]) => {
      setPools(Array.isArray(p) ? p : p.pools || []);
      const nodes = n.nodes || [];
      setOsdCount(nodes.filter((nd: { online: boolean }) => nd.online).length);
      const uniqueK8s = new Set(nodes.map((nd: { kubernetes_node: string }) => nd.kubernetes_node).filter(Boolean));
      setK8sNodeCount(uniqueK8s.size);
    }).finally(() => setLoading(false));
  };
  useEffect(load, []);

  const startEdit = (p?: Pool) => {
    if (p) {
      setForm({ ...emptyPool, ...p });
      setTagsStr(p.osd_tags?.join(", ") || "");
      setEditing(p.name);
    } else {
      setForm({ ...emptyPool });
      setTagsStr("");
      setEditing("__new__");
    }
  };

  const applyPreset = (preset: Preset) => {
    setForm({
      ...form,
      ec_type: preset.ec_type,
      ec_k: preset.ec_k,
      ec_m: preset.ec_m,
      ec_local_parity: preset.ec_local_parity,
      ec_global_parity: preset.ec_global_parity,
      replication_count: preset.replication_count,
    });
  };

  const save = async () => {
    const payload = {
      ...form,
      osd_tags: tagsStr
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean),
    };
    const method = editing === "__new__" ? "POST" : "PUT";
    const url =
      editing === "__new__"
        ? "/_admin/pools"
        : `/_admin/pools/${form.name}`;
    await fetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setEditing(null);
    load();
  };

  const remove = async (name: string) => {
    if (!confirm(`Delete pool "${name}"?`)) return;
    await fetch(`/_admin/pools/${name}`, { method: "DELETE" });
    load();
  };

  return (
    <div className="p-6">
      <PageHeader
        title="Storage Pools"
        description="Configure erasure coding, failure domains, and placement for each pool"
        action={
          <button
            onClick={() => startEdit()}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
          >
            <Plus size={14} /> Create Pool
          </button>
        }
      />

      {editing && (
        <Card
          title={editing === "__new__" ? "Create Storage Pool" : `Edit: ${editing}`}
          className="mb-4"
        >
          <div className="space-y-4">
            {/* Name + Description */}
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-[11px] font-medium text-gray-500 mb-1">
                  Pool Name
                </label>
                <input
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  disabled={editing !== "__new__"}
                  placeholder="e.g. fast-nvme"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none disabled:bg-gray-100"
                />
              </div>
              <div>
                <label className="block text-[11px] font-medium text-gray-500 mb-1">
                  Description
                </label>
                <input
                  value={form.description}
                  onChange={(e) =>
                    setForm({ ...form, description: e.target.value })
                  }
                  placeholder="High-performance NVMe pool"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                />
              </div>
            </div>

            {/* Protection Presets */}
            <div>
              <label className="block text-[11px] font-medium text-gray-500 mb-1">
                Protection Preset
                {osdCount > 0 && (
                  <span className="text-gray-400 font-normal ml-2">
                    {osdCount} OSDs on {k8sNodeCount} node{k8sNodeCount !== 1 ? "s" : ""} available
                  </span>
                )}
              </label>
              <div className="flex flex-wrap gap-1.5">
                {PRESETS.map((preset) => {
                  const feasible = osdCount === 0 || preset.min_nodes <= osdCount;
                  const isActive =
                    form.ec_type === preset.ec_type &&
                    form.ec_k === preset.ec_k &&
                    form.ec_m === preset.ec_m &&
                    form.ec_local_parity === preset.ec_local_parity &&
                    form.ec_global_parity === preset.ec_global_parity &&
                    form.replication_count === preset.replication_count;
                  return (
                    <button
                      key={preset.label}
                      onClick={() => feasible && applyPreset(preset)}
                      disabled={!feasible}
                      className={`px-2.5 py-1.5 rounded-lg text-[11px] font-medium border transition-colors ${
                        !feasible
                          ? "bg-gray-50 border-gray-100 text-gray-300 cursor-not-allowed line-through"
                          : isActive
                            ? "bg-blue-50 border-blue-300 text-blue-700"
                            : "bg-white border-gray-200 text-gray-600 hover:border-gray-300 hover:bg-gray-50"
                      }`}
                    >
                      <span className="font-semibold">{preset.label}</span>
                      <span className={feasible ? "text-gray-400" : "text-gray-300"}>
                        {" "}{preset.efficiency} · {preset.min_nodes} min
                      </span>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Custom EC Config */}
            <div className="grid grid-cols-3 gap-3">
              <div>
                <label className="block text-[11px] font-medium text-gray-500 mb-1">
                  Protection Type
                </label>
                <select
                  value={form.ec_type}
                  onChange={(e) =>
                    setForm({ ...form, ec_type: Number(e.target.value) })
                  }
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                >
                  <option value={0}>Reed-Solomon (MDS)</option>
                  <option value={1}>LRC (Local Repair Codes)</option>
                  <option value={2}>Replication</option>
                </select>
              </div>

              {/* RS fields */}
              {form.ec_type === 0 && (
                <>
                  <div>
                    <label className="block text-[11px] font-medium text-gray-500 mb-1">
                      Data Shards (k)
                    </label>
                    <input
                      type="number"
                      value={form.ec_k}
                      onChange={(e) =>
                        setForm({ ...form, ec_k: Number(e.target.value) })
                      }
                      min={1}
                      max={20}
                      className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                    />
                  </div>
                  <div>
                    <label className="block text-[11px] font-medium text-gray-500 mb-1">
                      Parity Shards (m)
                    </label>
                    <input
                      type="number"
                      value={form.ec_m}
                      onChange={(e) =>
                        setForm({ ...form, ec_m: Number(e.target.value) })
                      }
                      min={1}
                      max={10}
                      className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                    />
                  </div>
                </>
              )}

              {/* LRC fields */}
              {form.ec_type === 1 && (
                <>
                  <div>
                    <label className="block text-[11px] font-medium text-gray-500 mb-1">
                      Data Shards (k)
                    </label>
                    <input
                      type="number"
                      value={form.ec_k}
                      onChange={(e) =>
                        setForm({ ...form, ec_k: Number(e.target.value) })
                      }
                      min={1}
                      max={20}
                      className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                    />
                  </div>
                  <div>
                    <label className="block text-[11px] font-medium text-gray-500 mb-1">
                      Local Parity
                    </label>
                    <input
                      type="number"
                      value={form.ec_local_parity}
                      onChange={(e) =>
                        setForm({
                          ...form,
                          ec_local_parity: Number(e.target.value),
                        })
                      }
                      min={1}
                      max={10}
                      className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                    />
                  </div>
                </>
              )}

              {/* Replication fields */}
              {form.ec_type === 2 && (
                <>
                  <div>
                    <label className="block text-[11px] font-medium text-gray-500 mb-1">
                      Replica Count
                    </label>
                    <input
                      type="number"
                      value={form.replication_count}
                      onChange={(e) =>
                        setForm({
                          ...form,
                          replication_count: Number(e.target.value),
                        })
                      }
                      min={1}
                      max={10}
                      className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                    />
                  </div>
                  <div />
                </>
              )}
            </div>

            {/* LRC extra row for global parity */}
            {form.ec_type === 1 && (
              <div className="grid grid-cols-3 gap-3">
                <div>
                  <label className="block text-[11px] font-medium text-gray-500 mb-1">
                    Global Parity
                  </label>
                  <input
                    type="number"
                    value={form.ec_global_parity}
                    onChange={(e) =>
                      setForm({
                        ...form,
                        ec_global_parity: Number(e.target.value),
                      })
                    }
                    min={1}
                    max={10}
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                  />
                </div>
                <div className="col-span-2 flex items-end">
                  <p className="text-[11px] text-gray-400 pb-1.5">
                    Total shards: {form.ec_k} data + {form.ec_local_parity} local + {form.ec_global_parity} global = {form.ec_k + form.ec_local_parity + form.ec_global_parity} nodes
                  </p>
                </div>
              </div>
            )}

            {/* Failure domain + tags + quota */}
            <div className="grid grid-cols-3 gap-3">
              <div>
                <label className="block text-[11px] font-medium text-gray-500 mb-1">
                  Failure Domain
                  {k8sNodeCount <= 1 && k8sNodeCount > 0 && (
                    <span className="text-orange-500 font-normal ml-1">(single node — OSD level only)</span>
                  )}
                </label>
                <select
                  value={form.failure_domain}
                  onChange={(e) =>
                    setForm({ ...form, failure_domain: e.target.value })
                  }
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                >
                  <option value="osd">OSD (disk-level)</option>
                  <option value="node" disabled={k8sNodeCount <= 1 && k8sNodeCount > 0}>
                    Node{k8sNodeCount <= 1 && k8sNodeCount > 0 ? " (need 2+ nodes)" : ""}
                  </option>
                  <option value="rack" disabled={k8sNodeCount <= 1 && k8sNodeCount > 0}>
                    Rack{k8sNodeCount <= 1 && k8sNodeCount > 0 ? " (need multi-rack)" : ""}
                  </option>
                  <option value="datacenter" disabled={k8sNodeCount <= 1 && k8sNodeCount > 0}>
                    Datacenter
                  </option>
                </select>
              </div>
              <div>
                <label className="block text-[11px] font-medium text-gray-500 mb-1">
                  OSD Tags
                </label>
                <input
                  value={tagsStr}
                  onChange={(e) => setTagsStr(e.target.value)}
                  placeholder="nvme, ssd"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                />
              </div>
              <div>
                <label className="block text-[11px] font-medium text-gray-500 mb-1">
                  Storage Quota
                </label>
                <select
                  value={form.quota_bytes}
                  onChange={(e) =>
                    setForm({ ...form, quota_bytes: Number(e.target.value) })
                  }
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
                >
                  <option value={0}>Unlimited</option>
                  <option value={107374182400}>100 GB</option>
                  <option value={1099511627776}>1 TB</option>
                  <option value={10995116277760}>10 TB</option>
                  <option value={109951162777600}>100 TB</option>
                </select>
              </div>
            </div>

            {/* Summary bar */}
            <div className="flex items-center gap-4 px-3 py-2 bg-gray-50 rounded-lg text-[11px] text-gray-500">
              <span>
                <Shield size={12} className="inline mr-1" />
                {ecLabel(form as Pool)}
              </span>
              <span>Efficiency: {ecEfficiency(form as Pool)}</span>
              <span>Min nodes: {ecMinNodes(form as Pool)}</span>
              <span>Fault tolerance: {form.ec_type === 2 ? form.replication_count - 1 : form.ec_m} node failures</span>
            </div>

            <div className="flex gap-2 pt-3 border-t border-gray-100">
              <button
                onClick={save}
                className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
              >
                Save
              </button>
              <button
                onClick={() => setEditing(null)}
                className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
              >
                Cancel
              </button>
            </div>
          </div>
        </Card>
      )}

      {/* Pool table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Pool
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Protection
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Efficiency
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Domain
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Quota
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-24">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center">
                  <div className="flex items-center justify-center gap-3">
                    <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                      <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
                    </div>
                    <span className="text-[12px] text-gray-400">Loading</span>
                  </div>
                </td>
              </tr>
            ) : pools.length === 0 ? (
              <tr>
                <td
                  colSpan={7}
                  className="px-4 py-8 text-center text-[12px] text-gray-400"
                >
                  No storage pools configured
                </td>
              </tr>
            ) : (
              pools.map((p) => (
                <tr key={p.name} className="hover:bg-gray-50 group">
                  <td className="px-4 py-2">
                    <div className="flex items-center gap-2">
                      <Layers size={14} className="text-blue-500" />
                      <div>
                        <span className="text-[13px] font-medium">
                          {p.name}
                        </span>
                        {p.description && (
                          <p className="text-[10px] text-gray-400">
                            {p.description}
                          </p>
                        )}
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-2">
                    <div className="flex items-center gap-1.5">
                      <span
                        className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${ecTypeBadge(p.ec_type)}`}
                      >
                        {ecTypeName(p.ec_type)}
                      </span>
                      <span className="text-[12px] font-mono text-gray-700">
                        {ecLabel(p)}
                      </span>
                    </div>
                  </td>
                  <td className="px-4 py-2 text-[12px] text-gray-500 font-mono">
                    {ecEfficiency(p)}
                  </td>
                  <td className="px-4 py-2 text-[12px] text-gray-500">
                    {p.failure_domain}
                  </td>
                  <td className="px-4 py-2 text-[12px] text-gray-500">
                    {formatBytes(p.quota_bytes)}
                  </td>
                  <td className="px-4 py-2">
                    <StatusBadge
                      status={p.enabled ? "healthy" : "warning"}
                      label={p.enabled ? "Active" : "Disabled"}
                    />
                  </td>
                  <td className="px-4 py-2 text-right">
                    <div className="flex items-center justify-end gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        onClick={() => startEdit(p)}
                        className="text-gray-400 hover:text-blue-600 p-1"
                        title="Edit"
                      >
                        <Edit size={14} />
                      </button>
                      <button
                        onClick={() => remove(p.name)}
                        className="text-gray-400 hover:text-red-600 p-1"
                        title="Delete"
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
