import { useEffect, useState } from "react";
import { Shield, Plus, Trash2, Link2 } from "lucide-react";
import PageHeader from "../components/PageHeader";
import {
  users as usersApi,
  groups as groupsApi,
  type User,
  type Group,
} from "../api/client";

interface Policy {
  name: string;
  policy: Record<string, unknown>;
  created_at: number;
}

export default function Policies() {
  const [policies, setPolicies] = useState<Policy[]>([]);
  const [loading, setLoading] = useState(true);
  const [users, setUsers] = useState<User[]>([]);
  const [groups, setGroups] = useState<Group[]>([]);

  // Create
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newPolicyJson, setNewPolicyJson] = useState("");

  // Attach
  const [showAttach, setShowAttach] = useState(false);
  const [attachPolicy, setAttachPolicy] = useState("");
  const [attachUserId, setAttachUserId] = useState("");
  const [attachGroupId, setAttachGroupId] = useState("");
  const [attachMsg, setAttachMsg] = useState<string | null>(null);
  const [attachErr, setAttachErr] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    fetch("/_admin/policies")
      .then((r) => r.json())
      .then((d) => setPolicies(d.policies || []))
      .catch(() => setPolicies([]))
      .finally(() => setLoading(false));
  };

  useEffect(load, []);
  useEffect(() => {
    usersApi.list().then((r) => setUsers(r.users || [])).catch(() => setUsers([]));
    groupsApi.list().then((r) => setGroups(r.groups || [])).catch(() => setGroups([]));
  }, []);

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
    setAttachErr(null);
    setAttachMsg(null);
    if (!attachPolicy) {
      setAttachErr("Pick a policy");
      return;
    }
    if (!attachUserId && !attachGroupId) {
      setAttachErr("Pick a user or enter a group");
      return;
    }
    const r = await fetch("/_admin/policies/attach", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        policy_name: attachPolicy,
        user_id: attachUserId,
        group_id: attachGroupId,
      }),
    });
    if (!r.ok) {
      setAttachErr(`${r.status}: ${await r.text()}`);
      return;
    }
    const target = attachUserId
      ? users.find((u) => u.user_id === attachUserId)?.display_name ||
        attachUserId
      : `group ${
          groups.find((g) => g.group_id === attachGroupId)?.group_name ||
          attachGroupId
        }`;
    setAttachMsg(`Attached "${attachPolicy}" to ${target}`);
    setAttachPolicy("");
    setAttachUserId("");
    setAttachGroupId("");
  };

  const applyTemplate = (name: string) => {
    const templates: Record<string, { suggested: string; doc: object }> = {
      // ---- S3 ----
      "s3-readonly": {
        suggested: "s3-readonly",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: ["s3:GetObject", "s3:GetBucketLocation", "s3:ListBucket"],
              Resource: ["arn:obio:s3:::*", "arn:obio:s3:::*/*"],
            },
          ],
        },
      },
      "s3-readwrite": {
        suggested: "s3-readwrite",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: ["s3:*"],
              Resource: ["arn:obio:s3:::*", "arn:obio:s3:::*/*"],
            },
          ],
        },
      },
      "s3-writeonly": {
        suggested: "s3-writeonly",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: ["s3:PutObject"],
              Resource: ["arn:obio:s3:::*/*"],
            },
          ],
        },
      },
      // Bucket-policy guardrail: deny anything that didn't arrive via an
      // STS-vended session. Auto-attached on Unity catalog backing
      // buckets at create time; useful as a manual attach on any bucket
      // that should only be reachable through Unity / Iceberg / Delta
      // Sharing presigned URLs.
      "s3-deny-non-sts": {
        suggested: "s3-deny-non-sts",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Sid: "DenyNonStsDirectAccess",
              Effect: "Deny",
              Principal: "*",
              Action: ["s3:*"],
              Resource: ["arn:obio:s3:::*", "arn:obio:s3:::*/*"],
              Condition: {
                StringNotEquals: { "obio:CredentialType": "STS" },
              },
            },
          ],
        },
      },
      // ---- Unity Catalog ----
      // Single Unity plane covers data + ML registry + governed volumes.
      // Resources use the `arn:obio:unity:::<catalog>[/<schema>[/<table>]]`
      // shape; `*` matches everything below the level you scope to.
      "unity-readonly": {
        suggested: "unity-readonly",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "unity:GetCatalog",
                "unity:GetSchema",
                "unity:GetTable",
                "unity:GetVolume",
                "unity:GetModel",
                "unity:GetModelVersion",
                "unity:GetFunction",
                "unity:ListSchemas",
                "unity:ListTables",
                "unity:ListVolumes",
                "unity:ListModels",
                "unity:ListModelVersions",
                "unity:ListFunctions",
              ],
              Resource: ["*"],
            },
          ],
        },
      },
      "unity-data-engineer": {
        suggested: "unity-data-engineer",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "unity:GetCatalog",
                "unity:GetSchema",
                "unity:GetTable",
                "unity:CreateTable",
                "unity:DeleteTable",
                "unity:GetVolume",
                "unity:CreateVolume",
                "unity:GetFunction",
                "unity:CreateFunction",
                "unity:DeleteFunction",
                "unity:GenerateTemporaryCredentials",
                "unity:ListSchemas",
                "unity:ListTables",
                "unity:ListVolumes",
                "unity:ListFunctions",
              ],
              Resource: ["*"],
            },
          ],
        },
      },
      // Security officer — can author row filter / column mask UDFs
      // and bind them on tables, but cannot read or modify data.
      "unity-security-officer": {
        suggested: "unity-security-officer",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "unity:GetCatalog",
                "unity:GetSchema",
                "unity:GetTable",
                "unity:GetFunction",
                "unity:CreateFunction",
                "unity:DeleteFunction",
                "unity:SetTableSecurity",
                "unity:ListSchemas",
                "unity:ListTables",
                "unity:ListFunctions",
              ],
              Resource: ["*"],
            },
          ],
        },
      },
      "unity-ml-engineer": {
        suggested: "unity-ml-engineer",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              Effect: "Allow",
              Action: [
                "unity:GetCatalog",
                "unity:GetSchema",
                "unity:GetModel",
                "unity:CreateModel",
                "unity:GetModelVersion",
                "unity:CreateModelVersion",
                "unity:UpdateModelVersion",
                "unity:ListModels",
                "unity:ListModelVersions",
                "unity:GenerateTemporaryCredentials",
              ],
              Resource: ["*"],
            },
          ],
        },
      },
      "unity-deny-delete": {
        suggested: "unity-deny-delete",
        doc: {
          Version: "2012-10-17",
          Statement: [
            {
              // Companion guardrail to attach alongside a broader Allow —
              // protects against accidental DROP TABLE / DELETE CATALOG.
              Effect: "Deny",
              Action: [
                "unity:DeleteCatalog",
                "unity:DeleteSchema",
                "unity:DeleteTable",
                "unity:DeleteVolume",
                "unity:DeleteModel",
                "unity:DeleteModelVersion",
              ],
              Resource: ["*"],
            },
          ],
        },
      },
    };
    const t = templates[name];
    if (t) {
      setNewName(t.suggested);
      setNewPolicyJson(JSON.stringify(t.doc, null, 2));
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
                User
              </label>
              <select
                value={attachUserId}
                onChange={(e) => {
                  setAttachUserId(e.target.value);
                  if (e.target.value) setAttachGroupId("");
                }}
                className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="">— pick user —</option>
                {users.map((u) => (
                  <option key={u.user_id} value={u.user_id}>
                    {u.display_name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-[11px] text-gray-500 mb-1">
                Group
              </label>
              <select
                value={attachGroupId}
                onChange={(e) => {
                  setAttachGroupId(e.target.value);
                  if (e.target.value) setAttachUserId("");
                }}
                disabled={!!attachUserId}
                className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:bg-gray-50 disabled:text-gray-400"
              >
                <option value="">— pick group —</option>
                {groups.map((g) => (
                  <option key={g.group_id} value={g.group_id}>
                    {g.group_name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {attachErr && (
            <div className="mb-2 text-[11px] text-red-600 font-mono break-all">
              {attachErr}
            </div>
          )}
          {attachMsg && (
            <div className="mb-2 text-[11px] text-green-700">{attachMsg}</div>
          )}
          <div className="flex gap-1.5">
            <button
              onClick={doAttach}
              className="px-3 py-1.5 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
            >
              Attach
            </button>
            <button
              onClick={() => {
                setShowAttach(false);
                setAttachErr(null);
                setAttachMsg(null);
              }}
              className="px-3 py-1.5 text-gray-500 text-[11px]"
            >
              Close
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
            <div className="space-y-1.5 mb-2">
              <div className="flex gap-1.5 items-center flex-wrap">
                <span className="text-[11px] text-gray-500 py-0.5 w-12">
                  S3:
                </span>
                {["s3-readonly", "s3-readwrite", "s3-writeonly", "s3-deny-non-sts"].map((t) => (
                  <button
                    key={t}
                    onClick={() => applyTemplate(t)}
                    className="px-2 py-0.5 rounded border border-gray-200 text-[11px] text-gray-600 hover:bg-gray-50"
                  >
                    {t.replace("s3-", "")}
                  </button>
                ))}
              </div>
              <div className="flex gap-1.5 items-center flex-wrap">
                <span className="text-[11px] text-gray-500 py-0.5 w-12">
                  Unity:
                </span>
                {[
                  "unity-readonly",
                  "unity-data-engineer",
                  "unity-ml-engineer",
                  "unity-security-officer",
                  "unity-deny-delete",
                ].map((t) => (
                  <button
                    key={t}
                    onClick={() => applyTemplate(t)}
                    className="px-2 py-0.5 rounded border border-gray-200 text-[11px] text-gray-600 hover:bg-gray-50"
                  >
                    {t.replace("unity-", "")}
                  </button>
                ))}
              </div>
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
