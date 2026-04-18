import { useEffect, useState } from "react";
import { KeyRound, Save, RefreshCw, CheckCircle2, XCircle } from "lucide-react";
import PageHeader from "../components/PageHeader";
import Card from "../components/Card";

type Backend = "disabled" | "local" | "vault";

interface KmsStatus {
  enabled: boolean;
  backend: "local" | "external" | "disabled";
  master_key_configured: boolean;
}

interface KmsConfigView {
  backend: Backend;
  // Vault-only fields (present when backend === "vault")
  addr?: string;
  transit_path?: string;
  token_set?: boolean;
}

interface TestResult {
  ok: boolean;
  error: string | null;
}

/** Settings → Encryption. Admin-only.
 *
 *  - Shows master-key status (read-only; master key is set via env/helm)
 *  - Shows + edits the KMS backend choice; on Save, PUT /_admin/kms/config
 *    persists the change to meta and the gateway hot-swaps its provider
 *    with no pod restart.
 *  - The Vault token is never returned from GET (only `token_set: true`).
 *    Leaving the token field blank on Save means "keep the existing token",
 *    so editing addr/transit_path doesn't force re-entering secrets.
 */
export default function Encryption() {
  const [status, setStatus] = useState<KmsStatus | null>(null);
  const [cfg, setCfg] = useState<KmsConfigView | null>(null);
  const [loading, setLoading] = useState(true);
  const [backend, setBackend] = useState<Backend>("disabled");
  const [addr, setAddr] = useState("");
  const [transitPath, setTransitPath] = useState("transit");
  const [tokenEntry, setTokenEntry] = useState("");
  const [saving, setSaving] = useState(false);
  const [testKey, setTestKey] = useState("");
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [testing, setTesting] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    try {
      const [s, c] = await Promise.all([
        fetch("/_admin/kms/status").then((r) => (r.ok ? r.json() : null)),
        fetch("/_admin/kms/config").then((r) => (r.ok ? r.json() : null)),
      ]);
      setStatus(s);
      setCfg(c);
      if (c) {
        setBackend(c.backend);
        if (c.backend === "vault") {
          setAddr(c.addr ?? "");
          setTransitPath(c.transit_path ?? "transit");
        }
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  const save = async () => {
    setSaveError(null);
    setSaving(true);
    try {
      let body: Record<string, unknown>;
      if (backend === "disabled") body = { backend: "disabled" };
      else if (backend === "local") body = { backend: "local" };
      else
        body = {
          backend: "vault",
          addr: addr.trim(),
          transit_path: transitPath.trim() || "transit",
          // Empty token means "keep existing" — server honors this.
          token: tokenEntry,
        };
      const resp = await fetch("/_admin/kms/config", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const text = await resp.text();
        setSaveError(text || `HTTP ${resp.status}`);
        return;
      }
      setTokenEntry(""); // wipe local token state after save
      await load();
    } finally {
      setSaving(false);
    }
  };

  const runTest = async () => {
    if (!testKey.trim()) return;
    setTesting(true);
    setTestResult(null);
    try {
      const resp = await fetch("/_admin/kms/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key_id: testKey.trim() }),
      });
      const result = (await resp.json()) as TestResult;
      setTestResult(result);
    } catch (e) {
      setTestResult({ ok: false, error: String(e) });
    } finally {
      setTesting(false);
    }
  };

  if (loading) {
    return (
      <div className="p-6">
        <PageHeader title="Encryption" description="Server-side encryption configuration" />
        <p className="text-sm text-gray-400">Loading…</p>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-4">
      <PageHeader
        title="Encryption"
        description="Server-side encryption and KMS backend configuration"
        action={
          <button
            onClick={() => void load()}
            className="flex items-center gap-2 px-3 py-2 text-sm text-gray-600 hover:text-gray-900"
          >
            <RefreshCw size={14} /> Refresh
          </button>
        }
      />

      {/* Service master key status — read-only */}
      <Card>
        <div className="flex items-start gap-3">
          <KeyRound size={20} className="text-blue-600 mt-0.5" />
          <div className="flex-1">
            <div className="text-sm font-medium">Service master key</div>
            <div className="text-xs text-gray-500 mt-0.5">
              Wraps per-object DEKs for SSE-S3 and, when the local KMS backend is used, KMS key
              material. Loaded from the <code>OBJECTIO_MASTER_KEY</code> environment variable at
              startup; rotation is an operator-only action to avoid orphaning existing objects.
            </div>
            <div className="mt-2 text-sm">
              {status?.master_key_configured ? (
                <span className="inline-flex items-center gap-1.5 text-green-700">
                  <CheckCircle2 size={14} /> Configured
                </span>
              ) : (
                <span className="inline-flex items-center gap-1.5 text-red-700">
                  <XCircle size={14} /> Not configured — SSE is disabled cluster-wide
                </span>
              )}
            </div>
          </div>
        </div>
      </Card>

      {/* KMS backend selector */}
      <Card>
        <div className="space-y-4">
          <div>
            <div className="text-sm font-medium">KMS backend</div>
            <div className="text-xs text-gray-500 mt-0.5">
              Controls where per-object Data Encryption Keys are wrapped for SSE-KMS. Changes
              take effect immediately — no pod restart required. Objects already encrypted under
              the previous backend remain readable as long as that backend is still reachable;
              use <code>CopyObject</code> to re-encrypt if you want to move them.
            </div>
          </div>

          <div className="grid grid-cols-3 gap-2 text-sm">
            {(["disabled", "local", "vault"] as Backend[]).map((b) => (
              <label
                key={b}
                className={`flex items-start gap-2 p-3 border rounded-lg cursor-pointer transition ${
                  backend === b ? "border-blue-500 bg-blue-50" : "border-gray-200 hover:bg-gray-50"
                }`}
              >
                <input
                  type="radio"
                  name="backend"
                  value={b}
                  checked={backend === b}
                  onChange={() => setBackend(b)}
                  className="mt-0.5"
                />
                <div>
                  <div className="font-medium capitalize">{b}</div>
                  <div className="text-xs text-gray-500">
                    {b === "disabled" && "SSE-KMS off; SSE-S3 still works."}
                    {b === "local" && "Keys stored via meta, wrapped by the service master key."}
                    {b === "vault" && "HashiCorp Vault Transit engine."}
                  </div>
                </div>
              </label>
            ))}
          </div>

          {backend === "vault" && (
            <div className="space-y-3 p-3 bg-gray-50 border border-gray-200 rounded-lg">
              <div>
                <label className="text-xs font-medium text-gray-700">VAULT_ADDR</label>
                <input
                  type="text"
                  value={addr}
                  onChange={(e) => setAddr(e.target.value)}
                  placeholder="https://vault.example.com:8200"
                  className="mt-1 w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-gray-700">Transit mount path</label>
                <input
                  type="text"
                  value={transitPath}
                  onChange={(e) => setTransitPath(e.target.value)}
                  placeholder="transit"
                  className="mt-1 w-full px-3 py-1.5 border border-gray-300 rounded text-sm"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-gray-700">
                  Vault token
                  {cfg?.backend === "vault" && cfg.token_set && (
                    <span className="ml-2 text-xs text-gray-500">
                      (leave blank to keep the stored token)
                    </span>
                  )}
                </label>
                <input
                  type="password"
                  value={tokenEntry}
                  onChange={(e) => setTokenEntry(e.target.value)}
                  placeholder={cfg?.backend === "vault" && cfg.token_set ? "••••••••" : "paste Vault token"}
                  className="mt-1 w-full px-3 py-1.5 border border-gray-300 rounded text-sm font-mono"
                />
              </div>
            </div>
          )}

          <div className="flex items-center gap-3">
            <button
              onClick={() => void save()}
              disabled={saving}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
            >
              <Save size={14} /> {saving ? "Saving…" : "Save & apply"}
            </button>
            {saveError && <span className="text-sm text-red-600">{saveError}</span>}
          </div>
        </div>
      </Card>

      {/* Test connection */}
      <Card>
        <div className="space-y-3">
          <div>
            <div className="text-sm font-medium">Test backend</div>
            <div className="text-xs text-gray-500 mt-0.5">
              Runs a <code>generate_data_key</code> + <code>decrypt</code> round-trip against the
              currently active provider. Safe to repeat — no data is persisted.
            </div>
          </div>
          <div className="flex items-center gap-3">
            <input
              type="text"
              value={testKey}
              onChange={(e) => setTestKey(e.target.value)}
              placeholder="key id (e.g. my-bucket-key for Vault, or kms-abc12345 for Local)"
              className="flex-1 px-3 py-1.5 border border-gray-300 rounded text-sm"
            />
            <button
              onClick={() => void runTest()}
              disabled={testing || !testKey.trim()}
              className="px-4 py-2 bg-gray-800 text-white rounded-lg text-sm font-medium hover:bg-gray-900 disabled:opacity-50"
            >
              {testing ? "Testing…" : "Test"}
            </button>
          </div>
          {testResult &&
            (testResult.ok ? (
              <div className="flex items-center gap-2 text-sm text-green-700">
                <CheckCircle2 size={14} /> Round-trip succeeded
              </div>
            ) : (
              <div className="flex items-start gap-2 text-sm text-red-700">
                <XCircle size={14} className="mt-0.5" />
                <div>
                  <div className="font-medium">Round-trip failed</div>
                  <div className="text-xs text-red-600 mt-0.5">{testResult.error}</div>
                </div>
              </div>
            ))}
        </div>
      </Card>
    </div>
  );
}
