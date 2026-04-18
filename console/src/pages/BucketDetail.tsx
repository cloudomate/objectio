import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import {
  ArrowLeft,
  GitBranch,
  Lock,
  Timer,
  Shield,
  Plus,
  Trash2,
  Save,
  ToggleLeft,
  ToggleRight,
} from "lucide-react";
import PageHeader from "../components/PageHeader";

type Tab = "versioning" | "lock" | "lifecycle" | "policy";

interface LifecycleRule {
  id: string;
  status: string;
  prefix: string;
  expiration_days: number;
  noncurrent_days: number;
  abort_days: number;
  delete_markers: boolean;
}

interface LockConfig {
  enabled: boolean;
  mode: string;
  days: number;
  years: number;
}

export default function BucketDetail() {
  const { bucketName } = useParams<{ bucketName: string }>();
  const [tab, setTab] = useState<Tab>("versioning");

  // Versioning state
  const [versioning, setVersioning] = useState<string>("Disabled");
  const [versioningLoading, setVersioningLoading] = useState(true);

  // Object lock state
  const [lockConfig, setLockConfig] = useState<LockConfig>({
    enabled: false,
    mode: "COMPLIANCE",
    days: 0,
    years: 0,
  });
  const [lockLoading, setLockLoading] = useState(true);

  // Lifecycle state
  const [rules, setRules] = useState<LifecycleRule[]>([]);
  const [lifecycleLoading, setLifecycleLoading] = useState(true);
  const [showAddRule, setShowAddRule] = useState(false);
  const [newRule, setNewRule] = useState<LifecycleRule>({
    id: "",
    status: "Enabled",
    prefix: "",
    expiration_days: 0,
    noncurrent_days: 0,
    abort_days: 0,
    delete_markers: false,
  });

  // ---- Versioning ----
  const loadVersioning = () => {
    setVersioningLoading(true);
    fetch(`/${bucketName}?versioning`)
      .then((r) => r.text())
      .then((xml) => {
        const doc = new DOMParser().parseFromString(xml, "text/xml");
        const status = doc.querySelector("Status")?.textContent;
        setVersioning(status || "Disabled");
      })
      .catch(() => setVersioning("Disabled"))
      .finally(() => setVersioningLoading(false));
  };

  const toggleVersioning = async () => {
    const newState = versioning === "Enabled" ? "Suspended" : "Enabled";
    const body = `<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration><Status>${newState}</Status></VersioningConfiguration>`;
    await fetch(`/${bucketName}?versioning`, {
      method: "PUT",
      headers: { "Content-Type": "application/xml" },
      body,
    });
    loadVersioning();
  };

  // ---- Object Lock ----
  const loadLockConfig = () => {
    setLockLoading(true);
    fetch(`/${bucketName}?object-lock`)
      .then((r) => {
        if (!r.ok) throw new Error("not found");
        return r.text();
      })
      .then((xml) => {
        const doc = new DOMParser().parseFromString(xml, "text/xml");
        const enabled =
          doc.querySelector("ObjectLockEnabled")?.textContent === "Enabled";
        const mode =
          doc.querySelector("Mode")?.textContent || "COMPLIANCE";
        const days = parseInt(
          doc.querySelector("Days")?.textContent || "0",
          10
        );
        const years = parseInt(
          doc.querySelector("Years")?.textContent || "0",
          10
        );
        setLockConfig({ enabled, mode, days, years });
      })
      .catch(() =>
        setLockConfig({ enabled: false, mode: "COMPLIANCE", days: 0, years: 0 })
      )
      .finally(() => setLockLoading(false));
  };

  const saveLockConfig = async () => {
    const retentionXml =
      lockConfig.days > 0 || lockConfig.years > 0
        ? `<Rule><DefaultRetention><Mode>${lockConfig.mode}</Mode>${lockConfig.days > 0 ? `<Days>${lockConfig.days}</Days>` : ""}${lockConfig.years > 0 ? `<Years>${lockConfig.years}</Years>` : ""}</DefaultRetention></Rule>`
        : "";
    const body = `<?xml version="1.0" encoding="UTF-8"?><ObjectLockConfiguration><ObjectLockEnabled>${lockConfig.enabled ? "Enabled" : "Disabled"}</ObjectLockEnabled>${retentionXml}</ObjectLockConfiguration>`;
    await fetch(`/${bucketName}?object-lock`, {
      method: "PUT",
      headers: { "Content-Type": "application/xml" },
      body,
    });
    loadLockConfig();
  };

  // ---- Lifecycle ----
  const loadLifecycle = () => {
    setLifecycleLoading(true);
    fetch(`/${bucketName}?lifecycle`)
      .then((r) => {
        if (!r.ok) throw new Error("not found");
        return r.text();
      })
      .then((xml) => {
        const doc = new DOMParser().parseFromString(xml, "text/xml");
        const ruleNodes = doc.querySelectorAll("Rule");
        const parsed: LifecycleRule[] = [];
        ruleNodes.forEach((node) => {
          parsed.push({
            id: node.querySelector("ID")?.textContent || "",
            status: node.querySelector("Status")?.textContent || "Disabled",
            prefix:
              node.querySelector("Filter > Prefix")?.textContent ||
              node.querySelector("Prefix")?.textContent ||
              "",
            expiration_days: parseInt(
              node.querySelector("Expiration > Days")?.textContent || "0",
              10
            ),
            noncurrent_days: parseInt(
              node.querySelector(
                "NoncurrentVersionExpiration > NoncurrentDays"
              )?.textContent || "0",
              10
            ),
            abort_days: parseInt(
              node.querySelector(
                "AbortIncompleteMultipartUpload > DaysAfterInitiation"
              )?.textContent || "0",
              10
            ),
            delete_markers:
              node.querySelector(
                "Expiration > ExpiredObjectDeleteMarker"
              )?.textContent === "true",
          });
        });
        setRules(parsed);
      })
      .catch(() => setRules([]))
      .finally(() => setLifecycleLoading(false));
  };

  const saveLifecycle = async (updatedRules: LifecycleRule[]) => {
    const rulesXml = updatedRules
      .map(
        (r) =>
          `<Rule><ID>${r.id}</ID><Status>${r.status}</Status><Filter><Prefix>${r.prefix}</Prefix></Filter>${r.expiration_days > 0 ? `<Expiration><Days>${r.expiration_days}</Days>${r.delete_markers ? "<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>" : ""}</Expiration>` : ""}${r.noncurrent_days > 0 ? `<NoncurrentVersionExpiration><NoncurrentDays>${r.noncurrent_days}</NoncurrentDays></NoncurrentVersionExpiration>` : ""}${r.abort_days > 0 ? `<AbortIncompleteMultipartUpload><DaysAfterInitiation>${r.abort_days}</DaysAfterInitiation></AbortIncompleteMultipartUpload>` : ""}</Rule>`
      )
      .join("");
    const body = `<?xml version="1.0" encoding="UTF-8"?><LifecycleConfiguration>${rulesXml}</LifecycleConfiguration>`;
    await fetch(`/${bucketName}?lifecycle`, {
      method: "PUT",
      headers: { "Content-Type": "application/xml" },
      body,
    });
    loadLifecycle();
  };

  const addRule = () => {
    if (!newRule.id.trim()) return;
    const updated = [...rules, { ...newRule }];
    setRules(updated);
    saveLifecycle(updated);
    setNewRule({
      id: "",
      status: "Enabled",
      prefix: "",
      expiration_days: 0,
      noncurrent_days: 0,
      abort_days: 0,
      delete_markers: false,
    });
    setShowAddRule(false);
  };

  const removeRule = (id: string) => {
    const updated = rules.filter((r) => r.id !== id);
    if (updated.length === 0) {
      fetch(`/${bucketName}?lifecycle`, { method: "DELETE" });
      setRules([]);
    } else {
      saveLifecycle(updated);
    }
  };

  const toggleRuleStatus = (id: string) => {
    const updated = rules.map((r) =>
      r.id === id
        ? { ...r, status: r.status === "Enabled" ? "Disabled" : "Enabled" }
        : r
    );
    setRules(updated);
    saveLifecycle(updated);
  };

  // Policy state
  const [policyJson, setPolicyJson] = useState("");
  const [policyLoading, setPolicyLoading] = useState(true);
  const [policySaved, setPolicySaved] = useState(false);

  const loadPolicy = () => {
    setPolicyLoading(true);
    fetch(`/_admin/buckets/${bucketName}/policy`)
      .then((r) => r.json())
      .then((data) => {
        if (data.has_policy && data.policy) {
          setPolicyJson(JSON.stringify(data.policy, null, 2));
        } else {
          setPolicyJson("");
        }
      })
      .catch(() => setPolicyJson(""))
      .finally(() => setPolicyLoading(false));
  };

  const savePolicy = async () => {
    if (!policyJson.trim()) {
      await fetch(`/_admin/buckets/${bucketName}/policy`, { method: "DELETE" });
    } else {
      await fetch(`/_admin/buckets/${bucketName}/policy`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: policyJson,
      });
    }
    setPolicySaved(true);
    setTimeout(() => setPolicySaved(false), 2000);
    loadPolicy();
  };

  // @ts-expect-error TODO: wire this up to the Policy Templates UI
  const applyTemplate = (template: string) => {
    const templates: Record<string, object> = {
      "public-read": {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: "*",
            Action: ["s3:GetObject"],
            Resource: [`arn:obio:s3:::${bucketName}/*`],
          },
        ],
      },
      "read-write": {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: "*",
            Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            Resource: [`arn:obio:s3:::${bucketName}/*`],
          },
        ],
      },
      "deny-delete": {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Deny",
            Principal: "*",
            Action: ["s3:DeleteObject", "s3:DeleteBucket"],
            Resource: [
              `arn:obio:s3:::${bucketName}`,
              `arn:obio:s3:::${bucketName}/*`,
            ],
          },
        ],
      },
    };
    setPolicyJson(JSON.stringify(templates[template], null, 2));
  };

  useEffect(() => {
    loadVersioning();
    loadLockConfig();
    loadLifecycle();
    loadPolicy();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bucketName]);

  const tabs: { key: Tab; label: string; icon: typeof GitBranch }[] = [
    { key: "versioning", label: "Versioning", icon: GitBranch },
    { key: "lock", label: "Object Lock", icon: Lock },
    { key: "lifecycle", label: "Lifecycle", icon: Timer },
    { key: "policy", label: "Policy", icon: Shield },
  ];

  return (
    <div className="p-6">
      <div className="mb-4">
        <Link
          to="/buckets"
          className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
        >
          <ArrowLeft size={14} /> Back to Buckets
        </Link>
      </div>
      <PageHeader
        title={bucketName || "Bucket"}
        description="Bucket configuration and data protection settings"
      />

      {/* Tabs */}
      <div className="flex gap-1 mb-6 bg-white rounded-xl border border-gray-200 p-1 w-fit">
        {tabs.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              tab === key
                ? "bg-blue-50 text-blue-700"
                : "text-gray-600 hover:bg-gray-100"
            }`}
          >
            <Icon size={16} />
            {label}
          </button>
        ))}
      </div>

      {/* Versioning Tab */}
      {tab === "versioning" && (
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-semibold text-gray-900">
                Bucket Versioning
              </h3>
              <p className="text-sm text-gray-500 mt-1">
                When enabled, multiple versions of each object are preserved
                instead of being overwritten.
              </p>
            </div>
          </div>
          {versioningLoading ? (
            <p className="text-sm text-gray-400">Loading...</p>
          ) : (
            <div className="flex items-center gap-4">
              <button
                onClick={toggleVersioning}
                className="flex items-center gap-2 text-sm"
              >
                {versioning === "Enabled" ? (
                  <ToggleRight size={28} className="text-blue-600" />
                ) : (
                  <ToggleLeft size={28} className="text-gray-400" />
                )}
              </button>
              <span
                className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                  versioning === "Enabled"
                    ? "bg-green-100 text-green-800"
                    : versioning === "Suspended"
                      ? "bg-yellow-100 text-yellow-800"
                      : "bg-gray-100 text-gray-800"
                }`}
              >
                {versioning}
              </span>
              {versioning === "Suspended" && (
                <span className="text-xs text-gray-500">
                  New objects won't get version IDs; existing versions are
                  preserved.
                </span>
              )}
            </div>
          )}
        </div>
      )}

      {/* Object Lock Tab */}
      {tab === "lock" && (
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h3 className="text-sm font-semibold text-gray-900 mb-1">
            Object Lock Configuration
          </h3>
          <p className="text-sm text-gray-500 mb-4">
            Prevent objects from being deleted or overwritten for a fixed period
            (WORM). Requires versioning.
          </p>
          {lockLoading ? (
            <p className="text-sm text-gray-400">Loading...</p>
          ) : (
            <div className="space-y-4">
              <div className="flex items-center gap-3">
                <label className="text-sm font-medium text-gray-700 w-32">
                  Lock Enabled
                </label>
                <button
                  onClick={() =>
                    setLockConfig((c) => ({ ...c, enabled: !c.enabled }))
                  }
                >
                  {lockConfig.enabled ? (
                    <ToggleRight size={28} className="text-blue-600" />
                  ) : (
                    <ToggleLeft size={28} className="text-gray-400" />
                  )}
                </button>
                <span
                  className={`text-xs font-medium ${lockConfig.enabled ? "text-green-600" : "text-gray-500"}`}
                >
                  {lockConfig.enabled ? "Enabled" : "Disabled"}
                </span>
              </div>

              {lockConfig.enabled && (
                <>
                  <div className="flex items-center gap-3">
                    <label className="text-sm font-medium text-gray-700 w-32">
                      Retention Mode
                    </label>
                    <select
                      value={lockConfig.mode}
                      onChange={(e) =>
                        setLockConfig((c) => ({ ...c, mode: e.target.value }))
                      }
                      className="px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                      <option value="COMPLIANCE">Compliance</option>
                      <option value="GOVERNANCE">Governance</option>
                    </select>
                  </div>
                  <div className="flex items-center gap-3">
                    <label className="text-sm font-medium text-gray-700 w-32">
                      Retention Days
                    </label>
                    <input
                      type="number"
                      min={0}
                      value={lockConfig.days}
                      onChange={(e) =>
                        setLockConfig((c) => ({
                          ...c,
                          days: parseInt(e.target.value, 10) || 0,
                        }))
                      }
                      className="w-24 px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  <div className="flex items-center gap-3">
                    <label className="text-sm font-medium text-gray-700 w-32">
                      Retention Years
                    </label>
                    <input
                      type="number"
                      min={0}
                      value={lockConfig.years}
                      onChange={(e) =>
                        setLockConfig((c) => ({
                          ...c,
                          years: parseInt(e.target.value, 10) || 0,
                        }))
                      }
                      className="w-24 px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                </>
              )}

              <div className="pt-2">
                <button
                  onClick={saveLockConfig}
                  className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
                >
                  <Save size={16} /> Save Lock Configuration
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Lifecycle Tab */}
      {tab === "lifecycle" && (
        <div className="space-y-4">
          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h3 className="text-sm font-semibold text-gray-900">
                  Lifecycle Rules
                </h3>
                <p className="text-sm text-gray-500 mt-1">
                  Automatically expire objects, clean up delete markers, and
                  abort incomplete uploads.
                </p>
              </div>
              <button
                onClick={() => setShowAddRule(true)}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
              >
                <Plus size={16} /> Add Rule
              </button>
            </div>

            {showAddRule && (
              <div className="mb-4 p-4 bg-gray-50 rounded-lg border border-gray-200">
                <h4 className="text-sm font-medium mb-3">New Lifecycle Rule</h4>
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">
                      Rule ID
                    </label>
                    <input
                      value={newRule.id}
                      onChange={(e) =>
                        setNewRule((r) => ({ ...r, id: e.target.value }))
                      }
                      placeholder="e.g. expire-logs"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">
                      Prefix Filter
                    </label>
                    <input
                      value={newRule.prefix}
                      onChange={(e) =>
                        setNewRule((r) => ({ ...r, prefix: e.target.value }))
                      }
                      placeholder="e.g. logs/"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">
                      Expiration Days
                    </label>
                    <input
                      type="number"
                      min={0}
                      value={newRule.expiration_days}
                      onChange={(e) =>
                        setNewRule((r) => ({
                          ...r,
                          expiration_days: parseInt(e.target.value, 10) || 0,
                        }))
                      }
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">
                      Noncurrent Version Days
                    </label>
                    <input
                      type="number"
                      min={0}
                      value={newRule.noncurrent_days}
                      onChange={(e) =>
                        setNewRule((r) => ({
                          ...r,
                          noncurrent_days: parseInt(e.target.value, 10) || 0,
                        }))
                      }
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">
                      Abort Incomplete Uploads (Days)
                    </label>
                    <input
                      type="number"
                      min={0}
                      value={newRule.abort_days}
                      onChange={(e) =>
                        setNewRule((r) => ({
                          ...r,
                          abort_days: parseInt(e.target.value, 10) || 0,
                        }))
                      }
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  <div className="flex items-end">
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={newRule.delete_markers}
                        onChange={(e) =>
                          setNewRule((r) => ({
                            ...r,
                            delete_markers: e.target.checked,
                          }))
                        }
                        className="rounded border-gray-300"
                      />
                      Clean expired delete markers
                    </label>
                  </div>
                </div>
                <div className="flex gap-2 mt-3">
                  <button
                    onClick={addRule}
                    className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
                  >
                    Add Rule
                  </button>
                  <button
                    onClick={() => setShowAddRule(false)}
                    className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg text-sm font-medium hover:bg-gray-200"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            {lifecycleLoading ? (
              <p className="text-sm text-gray-400">Loading...</p>
            ) : rules.length === 0 ? (
              <p className="text-sm text-gray-400 py-4 text-center">
                No lifecycle rules configured
              </p>
            ) : (
              <table className="w-full text-sm">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="text-left px-4 py-2 text-xs font-medium text-gray-500 uppercase">
                      Rule ID
                    </th>
                    <th className="text-left px-4 py-2 text-xs font-medium text-gray-500 uppercase">
                      Prefix
                    </th>
                    <th className="text-left px-4 py-2 text-xs font-medium text-gray-500 uppercase">
                      Expiration
                    </th>
                    <th className="text-left px-4 py-2 text-xs font-medium text-gray-500 uppercase">
                      Noncurrent
                    </th>
                    <th className="text-left px-4 py-2 text-xs font-medium text-gray-500 uppercase">
                      Status
                    </th>
                    <th className="text-right px-4 py-2 text-xs font-medium text-gray-500 uppercase">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {rules.map((r) => (
                    <tr key={r.id} className="hover:bg-gray-50">
                      <td className="px-4 py-2 font-medium">{r.id}</td>
                      <td className="px-4 py-2 text-gray-500">
                        {r.prefix || "(all)"}
                      </td>
                      <td className="px-4 py-2 text-gray-500">
                        {r.expiration_days > 0
                          ? `${r.expiration_days}d`
                          : "-"}
                      </td>
                      <td className="px-4 py-2 text-gray-500">
                        {r.noncurrent_days > 0
                          ? `${r.noncurrent_days}d`
                          : "-"}
                      </td>
                      <td className="px-4 py-2">
                        <button
                          onClick={() => toggleRuleStatus(r.id)}
                          className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                            r.status === "Enabled"
                              ? "bg-green-100 text-green-800"
                              : "bg-gray-100 text-gray-600"
                          }`}
                        >
                          {r.status}
                        </button>
                      </td>
                      <td className="px-4 py-2 text-right">
                        <button
                          onClick={() => removeRule(r.id)}
                          className="text-red-500 hover:text-red-700 p-1"
                        >
                          <Trash2 size={14} />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}

      {/* Policy Tab */}
      {tab === "policy" && (
        <PolicyTab
          bucketName={bucketName || ""}
          policyJson={policyJson}
          policyLoading={policyLoading}
          onSave={savePolicy}
          onPolicyChange={setPolicyJson}
          policySaved={policySaved}
        />
      )}
    </div>
  );
}

// ============================================================
// Policy Tab — attach/detach named policies + view current
// ============================================================

function PolicyTab({
  bucketName,
  policyJson,
  policyLoading,
  onSave,
  onPolicyChange,
  policySaved,
}: {
  bucketName: string;
  policyJson: string;
  policyLoading: boolean;
  onSave: () => void;
  onPolicyChange: (v: string) => void;
  policySaved: boolean;
}) {
  const [availablePolicies, setAvailablePolicies] = useState<
    { name: string; policy: Record<string, unknown> }[]
  >([]);
  const [selectedPolicy, setSelectedPolicy] = useState("");

  useEffect(() => {
    fetch("/_admin/policies")
      .then((r) => r.json())
      .then((d) => setAvailablePolicies(d.policies || []))
      .catch(() => {});
  }, []);

  const attachPolicy = (policyName: string) => {
    const policy = availablePolicies.find((p) => p.name === policyName);
    if (!policy) return;

    // Get existing policy or create new one
    let existing: { Version: string; Statement: unknown[] };
    try {
      existing = policyJson ? JSON.parse(policyJson) : { Version: "2012-10-17", Statement: [] };
    } catch {
      existing = { Version: "2012-10-17", Statement: [] };
    }

    // Merge statements from the named policy, scoped to this bucket
    const namedPolicy = policy.policy as { Statement?: unknown[] };
    const newStatements = (namedPolicy.Statement || []).map((stmt: any) => ({
      ...stmt,
      Resource: [
        `arn:obio:s3:::${bucketName}`,
        `arn:obio:s3:::${bucketName}/*`,
      ],
    }));

    existing.Statement = [...existing.Statement, ...newStatements];
    onPolicyChange(JSON.stringify(existing, null, 2));
    setSelectedPolicy("");
  };

  const clearPolicy = () => {
    onPolicyChange("");
  };

  const hasPolicy = policyJson.trim().length > 0;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <h3 className="text-sm font-semibold text-gray-900 mb-1">
        Bucket Policy
      </h3>
      <p className="text-sm text-gray-500 mb-4">
        Attach a named policy to control access to this bucket.
      </p>

      {/* Attach policy */}
      <div className="mb-4">
        <label className="block text-[11px] font-medium text-gray-500 mb-1.5">
          Attach a policy
        </label>
        <div className="flex gap-2">
          <select
            value={selectedPolicy}
            onChange={(e) => setSelectedPolicy(e.target.value)}
            className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            <option value="">Select policy to attach...</option>
            {availablePolicies.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name}
              </option>
            ))}
          </select>
          <button
            onClick={() => selectedPolicy && attachPolicy(selectedPolicy)}
            disabled={!selectedPolicy}
            className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800 disabled:opacity-50"
          >
            Attach
          </button>
          {hasPolicy && (
            <button
              onClick={clearPolicy}
              className="px-3 py-1.5 border border-gray-300 text-gray-600 rounded-lg text-[12px] hover:bg-gray-50"
            >
              Remove All
            </button>
          )}
        </div>
      </div>

      {/* Current policy */}
      {policyLoading ? (
        <p className="text-sm text-gray-400">Loading...</p>
      ) : hasPolicy ? (
        <div className="mb-3">
          <label className="block text-[11px] font-medium text-gray-500 mb-1">
            Current policy
          </label>
          <pre className="bg-gray-50 border border-gray-200 rounded-lg p-3 text-[11px] font-mono overflow-auto max-h-64 text-gray-700">
            {policyJson}
          </pre>
        </div>
      ) : (
        <div className="bg-gray-50 rounded-lg p-4 text-center text-[12px] text-gray-400 mb-3">
          No policy attached to this bucket
        </div>
      )}

      <button
        onClick={onSave}
        className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
      >
        <Save size={16} /> {hasPolicy ? "Save Policy" : "Remove Policy"}
      </button>
      {policySaved && (
        <span className="text-[12px] text-green-600 ml-3">Saved</span>
      )}
    </div>
  );
}
