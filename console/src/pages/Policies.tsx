import { useEffect, useState } from "react";
import { Shield, Plus, Trash2, Link2 } from "lucide-react";
import PageHeader from "../components/PageHeader";

interface Policy {
  name: string;
  policy: Record<string, unknown>;
  created_at: number;
}

export default function Policies() {
  const [policies, setPolicies] = useState<Policy[]>([]);
  const [loading, setLoading] = useState(true);

  // Create
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newPolicyJson, setNewPolicyJson] = useState("");

  // Attach
  const [showAttach, setShowAttach] = useState(false);
  const [attachPolicy, setAttachPolicy] = useState("");
  const [attachUserId, setAttachUserId] = useState("");
  const [attachGroupId, setAttachGroupId] = useState("");

  const load = () => {
    setLoading(true);
    fetch("/_admin/policies")
      .then((r) => r.json())
      .then((d) => setPolicies(d.policies || []))
      .catch(() => setPolicies([]))
      .finally(() => setLoading(false));
  };

  useEffect(load, []);

  const createPolicy = async () => {
    if (!newName.trim() || !newPolicyJson.trim()) return;
    try {
      JSON.parse(newPolicyJson);
    } catch {
      alert("Invalid JSON");
      return;
    }
    await fetch("/_admin/policies", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: newName,
        policy: JSON.parse(newPolicyJson),
      }),
    });
    setNewName("");
    setNewPolicyJson("");
    setShowCreate(false);
    load();
  };

  const deletePolicy = async (name: string) => {
    if (!confirm(`Delete policy "${name}"?`)) return;
    await fetch(`/_admin/policies/${name}`, { method: "DELETE" });
    load();
  };

  const doAttach = async () => {
    if (!attachPolicy) return;
    await fetch("/_admin/policies/attach", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        policy_name: attachPolicy,
        user_id: attachUserId,
        group_id: attachGroupId,
      }),
    });
    setShowAttach(false);
    setAttachPolicy("");
    setAttachUserId("");
    setAttachGroupId("");
  };

  const applyTemplate = (name: string) => {
    const templates: Record<string, object> = {
      readonly: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Action: ["s3:GetObject", "s3:GetBucketLocation"],
            Resource: ["arn:obio:s3:::*/*"],
          },
        ],
      },
      readwrite: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Action: ["s3:*"],
            Resource: ["arn:obio:s3:::*", "arn:obio:s3:::*/*"],
          },
        ],
      },
      writeonly: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Action: ["s3:PutObject"],
            Resource: ["arn:obio:s3:::*/*"],
          },
        ],
      },
    };
    if (templates[name]) {
      setNewName(name);
      setNewPolicyJson(JSON.stringify(templates[name], null, 2));
    }
  };

  return (
    <div className="p-6">
      <PageHeader
        title="Policies"
        description="Named IAM policies — create and attach to users or groups"
        action={
          <div className="flex gap-2">
            <button
              onClick={() => setShowAttach(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 border border-gray-200 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
            >
              <Link2 size={14} /> Attach
            </button>
            <button
              onClick={() => setShowCreate(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Policy
            </button>
          </div>
        }
      />

      {/* Attach dialog */}
      {showAttach && (
        <div className="mb-4 bg-white rounded-xl border border-gray-200 p-4">
          <h3 className="text-[12px] font-medium mb-2">
            Attach Policy to User or Group
          </h3>
          <div className="grid grid-cols-3 gap-2 mb-2">
            <div>
              <label className="block text-[11px] text-gray-500 mb-1">
                Policy
              </label>
              <select
                value={attachPolicy}
                onChange={(e) => setAttachPolicy(e.target.value)}
                className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="">Select policy...</option>
                {policies.map((p) => (
                  <option key={p.name} value={p.name}>
                    {p.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-[11px] text-gray-500 mb-1">
                User ID (optional)
              </label>
              <input
                value={attachUserId}
                onChange={(e) => setAttachUserId(e.target.value)}
                placeholder="User UUID"
                className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-[11px] text-gray-500 mb-1">
                Group ID (optional)
              </label>
              <input
                value={attachGroupId}
                onChange={(e) => setAttachGroupId(e.target.value)}
                placeholder="Group name"
                className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
          </div>
          <div className="flex gap-1.5">
            <button
              onClick={doAttach}
              className="px-3 py-1.5 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
            >
              Attach
            </button>
            <button
              onClick={() => setShowAttach(false)}
              className="px-3 py-1.5 text-gray-500 text-[11px]"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Create dialog */}
      {showCreate && (
        <div className="mb-4 bg-white rounded-xl border border-gray-200 p-4">
          <h3 className="text-[12px] font-medium mb-2">Create Policy</h3>
          <div className="mb-2">
            <input
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="Policy name"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500 mb-2"
            />
            <div className="flex gap-1.5 mb-2">
              <span className="text-[11px] text-gray-500 py-0.5">
                Templates:
              </span>
              {["readonly", "readwrite", "writeonly"].map((t) => (
                <button
                  key={t}
                  onClick={() => applyTemplate(t)}
                  className="px-2 py-0.5 rounded border border-gray-200 text-[11px] text-gray-600 hover:bg-gray-50"
                >
                  {t}
                </button>
              ))}
            </div>
            <textarea
              value={newPolicyJson}
              onChange={(e) => setNewPolicyJson(e.target.value)}
              placeholder='{"Version":"2012-10-17","Statement":[...]}'
              rows={8}
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[12px] font-mono focus:outline-none focus:ring-1 focus:ring-blue-500 bg-gray-50"
            />
          </div>
          <div className="flex gap-1.5">
            <button
              onClick={createPolicy}
              className="px-3 py-1.5 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
            >
              Create
            </button>
            <button
              onClick={() => setShowCreate(false)}
              className="px-3 py-1.5 text-gray-500 text-[11px]"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Policies table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Policy
              </th>
              <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
              <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20"></th>
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
            ) : policies.length === 0 ? (
              <tr>
                <td
                  colSpan={3}
                  className="px-4 py-6 text-center text-[12px] text-gray-400"
                >
                  No policies
                </td>
              </tr>
            ) : (
              policies.map((p) => {
                const stmts = (p.policy as { Statement?: unknown[] })?.Statement || [];
                const actions = stmts
                  .flatMap((s: any) =>
                    Array.isArray(s.Action) ? s.Action : [s.Action]
                  )
                  .filter(Boolean);
                const isBuiltin = [
                  "readonly",
                  "readwrite",
                  "writeonly",
                  "consoleAdmin",
                ].includes(p.name);
                return (
                  <tr key={p.name} className="hover:bg-gray-50 group">
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2">
                        <Shield size={13} className="text-blue-500" />
                        <span className="text-[13px] font-medium">
                          {p.name}
                        </span>
                        {isBuiltin && (
                          <span className="text-[10px] px-1 py-0.5 bg-gray-100 text-gray-500 rounded">
                            built-in
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex flex-wrap gap-1">
                        {actions.slice(0, 4).map((a: string, i: number) => (
                          <span
                            key={i}
                            className="text-[10px] px-1.5 py-0.5 bg-blue-50 text-blue-600 rounded font-mono"
                          >
                            {a}
                          </span>
                        ))}
                        {actions.length > 4 && (
                          <span className="text-[10px] text-gray-400">
                            +{actions.length - 4} more
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-2.5 text-right">
                      {!isBuiltin && (
                        <button
                          onClick={() => deletePolicy(p.name)}
                          className="text-gray-300 hover:text-red-500 p-0.5 opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <Trash2 size={13} />
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
