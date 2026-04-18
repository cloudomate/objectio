import React, { useEffect, useState } from "react";
import { Users as UsersIcon, Plus, Trash2, Key, Copy, Check } from "lucide-react";
import PageHeader from "../components/PageHeader";

interface User {
  user_id: string;
  display_name: string;
  status: string;
  created_at: number;
  tenant: string;
}

interface AccessKey {
  access_key_id: string;
  status: string;
  created_at: number;
}

interface Tenant {
  name: string;
}

export default function UsersPage() {
  const [userList, setUserList] = useState<User[]>([]);
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [showCreate, setShowCreate] = useState(false);
  const [newUsername, setNewUsername] = useState("");
  const [newTenant, setNewTenant] = useState("");
  const [credentials, setCredentials] = useState<{ access_key_id: string; secret_access_key: string } | null>(null);
  const [expandedUser, setExpandedUser] = useState<string | null>(null);
  const [keys, setKeys] = useState<AccessKey[]>([]);
  const [copied, setCopied] = useState("");
  const [loading, setLoading] = useState(true);

  const load = () => {
    setLoading(true);
    fetch("/_admin/users")
      .then((r) => r.json())
      .then((d) => setUserList(d.users || []))
      .catch(() => setUserList([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    fetch("/_admin/tenants")
      .then((r) => r.json())
      .then((d) => {
        const list = Array.isArray(d) ? d : d.tenants || [];
        setTenants(list);
      })
      .catch(() => {});
  }, []);

  const createUser = async () => {
    if (!newUsername.trim()) return;
    // Create user
    const r = await fetch("/_admin/users", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ display_name: newUsername, tenant: newTenant }),
    });
    if (!r.ok) return;
    const user = await r.json();

    // Auto-create access key for the new user
    try {
      const kr = await fetch(`/_admin/users/${user.user_id}/access-keys`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      if (kr.ok) {
        const keyData = await kr.json();
        setCredentials({
          access_key_id: keyData.access_key_id || keyData.access_key?.access_key_id || "",
          secret_access_key: keyData.secret_access_key || keyData.access_key?.secret_access_key || "",
        });
      } else {
        console.error("Failed to create access key:", kr.status, await kr.text());
      }
    } catch (e) {
      console.error("Error creating access key:", e);
    }

    setNewUsername("");
    setNewTenant("");
    setShowCreate(false);
    load();
  };

  const deleteUser = async (userId: string) => {
    if (!confirm(`Delete user "${userId}"?`)) return;
    await fetch(`/_admin/users/${userId}`, { method: "DELETE" });
    load();
  };

  const loadKeys = async (userId: string) => {
    if (expandedUser === userId) {
      setExpandedUser(null);
      return;
    }
    const r = await fetch(`/_admin/users/${userId}/access-keys`);
    const data = await r.json();
    setKeys(data.access_keys || []);
    setExpandedUser(userId);
  };

  const createKey = async (userId: string) => {
    const r = await fetch(`/_admin/users/${userId}/access-keys`, { method: "POST" });
    const data = await r.json();
    setCredentials(data);
    loadKeys(userId);
  };

  const deleteKey = async (keyId: string, userId: string) => {
    await fetch(`/_admin/access-keys/${keyId}`, { method: "DELETE" });
    loadKeys(userId);
  };

  const copyText = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopied(id);
    setTimeout(() => setCopied(""), 2000);
  };

  return (
    <div className="p-6">
      <PageHeader
        title="Users & Access Keys"
        description="Manage S3 API users and their credentials"
        action={
          <button
            onClick={() => { setShowCreate(true); setCredentials(null); }}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
          >
            <Plus size={14} /> Create User
          </button>
        }
      />

      {/* Credentials banner */}
      {credentials && (
        <div className="mb-4 bg-yellow-50 border border-yellow-200 rounded-xl p-4">
          <h3 className="text-[12px] font-medium text-yellow-800 mb-2">Save these credentials now — the secret will not be shown again</h3>
          <div className="space-y-1.5 font-mono text-[12px]">
            <div className="flex items-center gap-2">
              <span className="text-gray-500 w-24">Access Key:</span>
              <span className="font-medium">{credentials.access_key_id}</span>
              <button onClick={() => copyText(credentials.access_key_id, "ak")} className="p-0.5">
                {copied === "ak" ? <Check size={12} className="text-green-500" /> : <Copy size={12} className="text-gray-400" />}
              </button>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-gray-500 w-24">Secret Key:</span>
              <span className="font-medium">{credentials.secret_access_key}</span>
              <button onClick={() => copyText(credentials.secret_access_key, "sk")} className="p-0.5">
                {copied === "sk" ? <Check size={12} className="text-green-500" /> : <Copy size={12} className="text-gray-400" />}
              </button>
            </div>
          </div>
          <button onClick={() => setCredentials(null)} className="mt-2 text-[11px] text-yellow-700 underline">Dismiss</button>
        </div>
      )}

      {showCreate && (
        <div className="mb-4 bg-white rounded-xl border border-gray-200 p-4">
          <h3 className="text-[12px] font-medium mb-2">Create New User</h3>
          <div className="flex gap-2">
            <input
              value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)}
              placeholder="Username"
              className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              onKeyDown={(e) => e.key === "Enter" && createUser()}
              autoFocus
            />
            <select
              value={newTenant}
              onChange={(e) => setNewTenant(e.target.value)}
              className="px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
            >
              <option value="">System (no tenant)</option>
              {tenants.map((t) => (
                <option key={t.name} value={t.name}>{t.name}</option>
              ))}
            </select>
            <button onClick={createUser} className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800">Create</button>
            <button onClick={() => setShowCreate(false)} className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50">Cancel</button>
          </div>
        </div>
      )}

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">User</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Tenant</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Status</th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">Created</th>
              <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-24">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading ? (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center">
                  <div className="flex items-center justify-center gap-3">
                    <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                      <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
                    </div>
                    <span className="text-[12px] text-gray-400">Loading</span>
                  </div>
                </td>
              </tr>
            ) : userList.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-[12px] text-gray-400">No users</td></tr>
            ) : (
              userList.map((u) => (
                <React.Fragment key={u.user_id}>
                <tr className="hover:bg-gray-50 group">
                    <td className="px-4 py-2">
                      <div className="flex items-center gap-2">
                        <UsersIcon size={14} className="text-purple-500" />
                        <span className="text-[13px] font-medium">{u.display_name}</span>
                        <span className="text-gray-400 text-[10px] font-mono">{u.user_id.slice(0, 8)}</span>
                      </div>
                    </td>
                    <td className="px-4 py-2 text-[12px]">
                      {u.tenant ? (
                        <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[11px] font-medium bg-purple-100 text-purple-800">
                          {u.tenant}
                        </span>
                      ) : (
                        <span className="text-gray-300">system</span>
                      )}
                    </td>
                    <td className="px-4 py-2 text-[12px] text-gray-500">{u.status}</td>
                    <td className="px-4 py-2 text-[12px] text-gray-500">{new Date(u.created_at * 1000).toLocaleDateString()}</td>
                    <td className="px-4 py-2 text-right">
                      <div className="flex items-center justify-end gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button onClick={() => loadKeys(u.user_id)} className="text-gray-400 hover:text-blue-600 p-1" title="Access Keys">
                          <Key size={14} />
                        </button>
                        <button onClick={() => deleteUser(u.user_id)} className="text-gray-400 hover:text-red-600 p-1">
                          <Trash2 size={14} />
                        </button>
                      </div>
                    </td>
                  </tr>
                  {expandedUser === u.user_id && (
                    <tr>
                      <td colSpan={5} className="bg-gray-50 px-4 py-3">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Access Keys</h4>
                          <button onClick={() => createKey(u.user_id)} className="text-[11px] text-blue-600 hover:text-blue-800 flex items-center gap-1">
                            <Plus size={11} /> New Key
                          </button>
                        </div>
                        {keys.length === 0 ? (
                          <p className="text-[12px] text-gray-400">No access keys</p>
                        ) : (
                          <div className="space-y-1.5">
                            {keys.map((k) => (
                              <div key={k.access_key_id} className="flex items-center justify-between bg-white rounded-lg px-3 py-1.5 border border-gray-200">
                                <span className="font-mono text-[11px]">{k.access_key_id}</span>
                                <div className="flex items-center gap-2">
                                  <span className="text-[11px] text-gray-400">{k.status}</span>
                                  <button onClick={() => deleteKey(k.access_key_id, u.user_id)} className="text-red-400 hover:text-red-600">
                                    <Trash2 size={12} />
                                  </button>
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
