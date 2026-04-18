import { useEffect, useState } from "react";
import { Building2, Plus, Trash2, Edit, ShieldCheck, UserPlus, X } from "lucide-react";
import PageHeader from "../components/PageHeader";
import Card from "../components/Card";
import StatusBadge from "../components/StatusBadge";

interface Tenant {
  name: string;
  display_name: string;
  default_pool: string;
  allowed_pools: string[];
  quota_bytes: number;
  quota_buckets: number;
  quota_objects: number;
  admin_users: string[];
  oidc_provider: string;
  labels: Record<string, string>;
  enabled: boolean;
  created_at: number;
}

interface TenantUser {
  user_id: string;
  display_name: string;
  arn: string;
  tenant: string;
}

const emptyTenant = {
  name: "", display_name: "", default_pool: "", allowed_pools: [] as string[],
  quota_bytes: 0, quota_buckets: 0, quota_objects: 0,
  admin_users: [] as string[], oidc_provider: "", labels: {} as Record<string, string>, enabled: true,
};

function formatBytes(b: number) {
  if (!b) return "Unlimited";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(b) / Math.log(1024));
  return `${(b / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

export default function Tenants() {
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [pools, setPools] = useState<string[]>([]);
  const [editing, setEditing] = useState<string | null>(null);
  const [form, setForm] = useState(emptyTenant);
  const [allowedPoolsStr, setAllowedPoolsStr] = useState("");
  const [loading, setLoading] = useState(true);
  const [managingAdmins, setManagingAdmins] = useState<string | null>(null);
  const [tenantUsers, setTenantUsers] = useState<TenantUser[]>([]);
  const [adminInput, setAdminInput] = useState("");
  const [adminError, setAdminError] = useState("");

  const load = () => {
    setLoading(true);
    Promise.all([
      fetch("/_admin/tenants").then(r => r.json()).catch(() => []),
      fetch("/_admin/pools").then(r => r.json()).catch(() => []),
    ]).then(([t, p]) => {
      setTenants(Array.isArray(t) ? t : t.tenants || []);
      setPools(Array.isArray(p) ? p.map((pool: { name: string }) => pool.name) : (p.pools || []).map((pool: { name: string }) => pool.name));
    }).finally(() => setLoading(false));
  };
  useEffect(load, []);

  const startEdit = (t?: Tenant) => {
    if (t) {
      setForm({ ...emptyTenant, ...t });
      setAllowedPoolsStr(t.allowed_pools?.join(", ") || "");
      setEditing(t.name);
    } else {
      setForm({ ...emptyTenant });
      setAllowedPoolsStr("");
      setEditing("__new__");
    }
  };

  const save = async () => {
    // admin_users is managed via the dedicated admin panel — preserve whatever
    // the tenant currently has and don't clobber it from the edit form.
    const current = tenants.find(t => t.name === form.name);
    const payload = {
      ...form,
      allowed_pools: allowedPoolsStr.split(",").map(s => s.trim()).filter(Boolean),
      admin_users: current?.admin_users || form.admin_users || [],
    };
    const method = editing === "__new__" ? "POST" : "PUT";
    const url = editing === "__new__" ? "/_admin/tenants" : `/_admin/tenants/${form.name}`;
    await fetch(url, { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    setEditing(null);
    load();
  };

  const remove = async (name: string) => {
    if (!confirm(`Delete tenant "${name}"? All buckets must be removed first.`)) return;
    const r = await fetch(`/_admin/tenants/${name}`, { method: "DELETE" });
    if (!r.ok) alert(await r.text());
    load();
  };

  const openAdmins = async (name: string) => {
    setManagingAdmins(name);
    setAdminInput("");
    setAdminError("");
    // Pull users scoped to this tenant so the picker doesn't leak other
    // tenants' users. Admin API already filters by caller tenant, but the
    // system admin sees everyone — filter client-side to the target tenant.
    try {
      const r = await fetch("/_admin/users");
      const d = await r.json();
      const list: TenantUser[] = (d.users || []).filter((u: TenantUser) => u.tenant === name);
      setTenantUsers(list);
    } catch {
      setTenantUsers([]);
    }
  };

  const addAdmin = async (entry: string) => {
    const val = entry.trim();
    if (!val) return;
    setAdminError("");
    const r = await fetch(`/_admin/tenants/${managingAdmins}/admins`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(val.startsWith("arn:") ? { user_arn: val } : { user_id: val }),
    });
    if (!r.ok) {
      setAdminError(await r.text());
      return;
    }
    setAdminInput("");
    load();
  };

  const removeAdmin = async (entry: string) => {
    if (!confirm(`Remove ${entry} from ${managingAdmins} admins?`)) return;
    const r = await fetch(`/_admin/tenants/${managingAdmins}/admins/${encodeURIComponent(entry)}`, {
      method: "DELETE",
    });
    if (!r.ok) {
      setAdminError(await r.text());
      return;
    }
    load();
  };

  const managingTenant = tenants.find(t => t.name === managingAdmins);

  return (
    <div className="p-6">
      <PageHeader
        title="Tenants"
        description="Multi-tenant isolation — each tenant gets its own buckets, quotas, and identity"
        action={
          <button onClick={() => startEdit()} className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800">
            <Plus size={14} /> Create Tenant
          </button>
        }
      />

      {managingAdmins && managingTenant && (
        <Card
          title={
            <div className="flex items-center gap-2">
              <ShieldCheck size={14} className="text-purple-500" />
              <span>Tenant Admins: {managingTenant.display_name || managingAdmins}</span>
            </div>
          }
          className="mb-4"
        >
          <p className="text-[12px] text-gray-500 mb-3">
            Tenant admins can manage users, access keys, buckets, warehouses, and shares <em>within this tenant only</em>.
            They cannot touch other tenants or system-level resources.
          </p>

          <div className="mb-3">
            <label className="block text-[11px] font-medium text-gray-500 mb-1">Current admins</label>
            {managingTenant.admin_users.length === 0 ? (
              <div className="text-[12px] text-gray-400 italic py-2">
                No tenant admins. Only the system admin can manage this tenant until one is added below.
              </div>
            ) : (
              <ul className="space-y-1">
                {managingTenant.admin_users.map((u) => {
                  const user = tenantUsers.find((tu) => tu.user_id === u || tu.arn === u);
                  return (
                    <li key={u} className="flex items-center justify-between px-2.5 py-1.5 bg-gray-50 rounded-lg">
                      <div className="flex flex-col min-w-0">
                        <span className="text-[12px] font-medium text-gray-900 truncate">
                          {user?.display_name || u}
                        </span>
                        <span className="text-[10px] text-gray-400 font-mono truncate">{u}</span>
                      </div>
                      <button
                        onClick={() => removeAdmin(u)}
                        className="text-gray-400 hover:text-red-600 p-1"
                        title="Remove admin"
                      >
                        <X size={14} />
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>

          <div>
            <label className="block text-[11px] font-medium text-gray-500 mb-1">Add admin</label>
            <div className="flex gap-2">
              <select
                value=""
                onChange={(e) => e.target.value && addAdmin(e.target.value)}
                className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
              >
                <option value="">— Pick a tenant user —</option>
                {tenantUsers
                  .filter((u) => !managingTenant.admin_users.includes(u.user_id))
                  .map((u) => (
                    <option key={u.user_id} value={u.user_id}>
                      {u.display_name} ({u.user_id})
                    </option>
                  ))}
              </select>
              <span className="text-[11px] text-gray-400 self-center">or</span>
              <input
                value={adminInput}
                onChange={(e) => setAdminInput(e.target.value)}
                placeholder="user_id or user ARN"
                className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
              />
              <button
                onClick={() => addAdmin(adminInput)}
                disabled={!adminInput.trim()}
                className="flex items-center gap-1 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800 disabled:opacity-40"
              >
                <UserPlus size={13} /> Add
              </button>
            </div>
            {adminError && (
              <p className="mt-2 text-[11px] text-red-600">{adminError}</p>
            )}
          </div>

          <div className="flex justify-end mt-4 pt-3 border-t border-gray-100">
            <button
              onClick={() => setManagingAdmins(null)}
              className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
            >
              Close
            </button>
          </div>
        </Card>
      )}

      {editing && (
        <Card title={editing === "__new__" ? "Create Tenant" : `Edit: ${editing}`} className="mb-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <label className="block text-[11px] font-medium text-gray-500 mb-1">Tenant Name</label>
              <input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} disabled={editing !== "__new__"}
                placeholder="e.g. acme-corp" className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none disabled:bg-gray-100" />
            </div>
            <div>
              <label className="block text-[11px] font-medium text-gray-500 mb-1">Display Name</label>
              <input value={form.display_name} onChange={e => setForm({ ...form, display_name: e.target.value })}
                placeholder="Acme Corporation" className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none" />
            </div>
            <div>
              <label className="block text-[11px] font-medium text-gray-500 mb-1">Default Pool</label>
              <select value={form.default_pool} onChange={e => setForm({ ...form, default_pool: e.target.value })}
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none">
                <option value="">System default</option>
                {pools.map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-[11px] font-medium text-gray-500 mb-1">OIDC Provider</label>
              <input value={form.oidc_provider} onChange={e => setForm({ ...form, oidc_provider: e.target.value })}
                placeholder="e.g. entra" className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none" />
            </div>
            <div>
              <label className="block text-[11px] font-medium text-gray-500 mb-1">Storage Quota</label>
              <select value={form.quota_bytes} onChange={e => setForm({ ...form, quota_bytes: Number(e.target.value) })}
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none">
                <option value={0}>Unlimited</option>
                <option value={107374182400}>100 GB</option>
                <option value={536870912000}>500 GB</option>
                <option value={1099511627776}>1 TB</option>
                <option value={10995116277760}>10 TB</option>
              </select>
            </div>
            <div>
              <label className="block text-[11px] font-medium text-gray-500 mb-1">Max Buckets (0=unlimited)</label>
              <input type="number" value={form.quota_buckets} onChange={e => setForm({ ...form, quota_buckets: Number(e.target.value) })}
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none" />
            </div>
          </div>
          <div className="flex gap-2 mt-4 pt-3 border-t border-gray-100">
            <button onClick={save} className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800">Save</button>
            <button onClick={() => setEditing(null)} className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50">Cancel</button>
          </div>
        </Card>
      )}

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Tenant</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Pool</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Quota</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Buckets</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">OIDC</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Admins</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Status</th>
              <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-32">Actions</th>
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
            ) : tenants.length === 0 ? (
              <tr><td colSpan={8} className="px-4 py-8 text-center text-[12px] text-gray-400">No tenants configured</td></tr>
            ) : (
              tenants.map(t => (
                <tr key={t.name} className="hover:bg-gray-50 group">
                  <td className="px-4 py-2">
                    <div className="flex items-center gap-2">
                      <Building2 size={14} className="text-purple-500" />
                      <div>
                        <span className="text-[13px] font-medium">{t.display_name || t.name}</span>
                        {t.display_name && t.display_name !== t.name && (
                          <span className="text-[10px] text-gray-400 ml-1.5 font-mono">{t.name}</span>
                        )}
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-2 text-[12px] text-gray-500 font-mono">{t.default_pool || "default"}</td>
                  <td className="px-4 py-2 text-[12px] text-gray-500">{formatBytes(t.quota_bytes)}</td>
                  <td className="px-4 py-2 text-[12px] text-gray-500">{t.quota_buckets || "∞"}</td>
                  <td className="px-4 py-2 text-[12px] text-gray-500">{t.oidc_provider || <span className="text-gray-300">-</span>}</td>
                  <td className="px-4 py-2 text-[12px] text-gray-500">
                    {t.admin_users && t.admin_users.length > 0 ? (
                      <span className="inline-flex items-center gap-1 px-1.5 py-0.5 bg-purple-50 text-purple-700 rounded text-[11px]">
                        <ShieldCheck size={11} /> {t.admin_users.length}
                      </span>
                    ) : (
                      <span className="text-gray-300">-</span>
                    )}
                  </td>
                  <td className="px-4 py-2">
                    <StatusBadge status={t.enabled ? "healthy" : "warning"} label={t.enabled ? "Active" : "Disabled"} />
                  </td>
                  <td className="px-4 py-2 text-right">
                    <div className="flex items-center justify-end gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button onClick={() => openAdmins(t.name)} className="text-gray-400 hover:text-purple-600 p-1" title="Manage tenant admins">
                        <ShieldCheck size={14} />
                      </button>
                      <button onClick={() => startEdit(t)} className="text-gray-400 hover:text-blue-600 p-1" title="Edit">
                        <Edit size={14} />
                      </button>
                      <button onClick={() => remove(t.name)} className="text-gray-400 hover:text-red-600 p-1" title="Delete">
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
