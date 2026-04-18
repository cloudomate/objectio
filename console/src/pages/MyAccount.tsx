import { useEffect, useState } from "react";
import { Key, Plus, Trash2, Copy, Check, User } from "lucide-react";
import PageHeader from "../components/PageHeader";

interface AccessKey {
  access_key_id: string;
  status: number;
  created_at: number;
}

export default function MyAccount() {
  const [keys, setKeys] = useState<AccessKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [newKey, setNewKey] = useState<{
    access_key_id: string;
    secret_access_key: string;
  } | null>(null);
  const [copied, setCopied] = useState("");
  const [session, setSession] = useState<{
    user: string;
    tenant: string;
  } | null>(null);

  const loadKeys = () => {
    setLoading(true);
    fetch("/_console/api/me/keys")
      .then((r) => r.json())
      .then((d) => setKeys(d.access_keys || []))
      .catch(() => setKeys([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    loadKeys();
    fetch("/_console/api/session")
      .then((r) => r.json())
      .then((d) => setSession(d))
      .catch(() => {});
  }, []);

  const createKey = async () => {
    const r = await fetch("/_console/api/me/keys", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    if (r.ok) {
      const data = await r.json();
      setNewKey(data);
      loadKeys();
    }
  };

  const deleteKey = async (keyId: string) => {
    if (!confirm(`Delete access key ${keyId}?`)) return;
    await fetch(`/_console/api/me/keys/${keyId}`, { method: "DELETE" });
    loadKeys();
  };

  const copyText = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopied(id);
    setTimeout(() => setCopied(""), 2000);
  };

  return (
    <div className="p-6">
      <PageHeader
        title="My Account"
        description="Manage your access keys for S3 API access"
        action={
          <button
            onClick={createKey}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
          >
            <Plus size={14} /> Create Access Key
          </button>
        }
      />

      {/* Profile info */}
      {session && (
        <div className="bg-white rounded-xl border border-gray-200 p-4 mb-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center">
              <User size={18} className="text-blue-600" />
            </div>
            <div>
              <p className="text-[13px] font-medium">{session.user}</p>
              {session.tenant ? (
                <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[11px] font-medium bg-purple-100 text-purple-800">
                  {session.tenant}
                </span>
              ) : (
                <span className="text-[11px] text-gray-400">
                  System Admin
                </span>
              )}
            </div>
          </div>
        </div>
      )}

      {/* New key banner */}
      {newKey && (
        <div className="mb-4 bg-yellow-50 border border-yellow-200 rounded-xl p-4">
          <h3 className="text-[12px] font-medium text-yellow-800 mb-2">
            Save these credentials now — the secret will not be shown again
          </h3>
          <div className="space-y-1.5 font-mono text-[12px]">
            <div className="flex items-center gap-2">
              <span className="text-gray-500 w-24">Access Key:</span>
              <span className="font-medium">{newKey.access_key_id}</span>
              <button
                onClick={() => copyText(newKey.access_key_id, "ak")}
                className="p-0.5"
              >
                {copied === "ak" ? (
                  <Check size={12} className="text-green-500" />
                ) : (
                  <Copy size={12} className="text-gray-400" />
                )}
              </button>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-gray-500 w-24">Secret Key:</span>
              <span className="font-medium">{newKey.secret_access_key}</span>
              <button
                onClick={() => copyText(newKey.secret_access_key, "sk")}
                className="p-0.5"
              >
                {copied === "sk" ? (
                  <Check size={12} className="text-green-500" />
                ) : (
                  <Copy size={12} className="text-gray-400" />
                )}
              </button>
            </div>
          </div>
          <button
            onClick={() => setNewKey(null)}
            className="mt-2 text-[11px] text-yellow-700 underline"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* Keys table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200">
          <h3 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">
            Access Keys
          </h3>
        </div>
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Access Key ID
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Created
              </th>
              <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {loading ? (
              <tr>
                <td colSpan={3} className="px-4 py-6 text-center">
                  <div className="flex items-center justify-center gap-3">
                    <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                      <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
                    </div>
                    <span className="text-[12px] text-gray-400">Loading</span>
                  </div>
                </td>
              </tr>
            ) : keys.length === 0 ? (
              <tr>
                <td
                  colSpan={3}
                  className="px-4 py-6 text-center text-[12px] text-gray-400"
                >
                  No access keys. Create one for S3 API access.
                </td>
              </tr>
            ) : (
              keys.map((k) => (
                <tr key={k.access_key_id} className="hover:bg-gray-50 group">
                  <td className="px-4 py-2.5">
                    <div className="flex items-center gap-2">
                      <Key size={13} className="text-orange-500" />
                      <span className="text-[12px] font-mono">
                        {k.access_key_id}
                      </span>
                    </div>
                  </td>
                  <td className="px-4 py-2.5 text-[12px] text-gray-500">
                    {k.created_at
                      ? new Date(k.created_at * 1000).toLocaleDateString()
                      : "-"}
                  </td>
                  <td className="px-4 py-2.5 text-right">
                    <button
                      onClick={() => deleteKey(k.access_key_id)}
                      className="text-gray-300 hover:text-red-500 p-0.5 opacity-0 group-hover:opacity-100 transition-opacity"
                    >
                      <Trash2 size={13} />
                    </button>
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
