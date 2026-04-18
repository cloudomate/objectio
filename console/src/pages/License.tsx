import { useEffect, useState } from "react";
import { ShieldCheck, Upload, Trash2, CheckCircle2, Lock } from "lucide-react";
import PageHeader from "../components/PageHeader";
import Card from "../components/Card";
import type { LicenseInfo as BaseLicenseInfo } from "../lib/license";

interface LicenseInfo extends BaseLicenseInfo {
  limits?: { max_nodes: number; max_raw_capacity_bytes: number };
  usage?: { node_count: number; raw_capacity_bytes: number };
}

function formatBytes(b: number): string {
  if (!b) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  const i = Math.min(Math.floor(Math.log(b) / Math.log(1024)), units.length - 1);
  return `${(b / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function UsageBar({ label, used, cap, formatter }: {
  label: string;
  used: number;
  cap: number;
  formatter: (n: number) => string;
}) {
  const unlimited = !cap;
  const pct = unlimited ? 0 : Math.min(100, Math.round((used / cap) * 100));
  const color = unlimited
    ? "bg-blue-400"
    : pct >= 100
      ? "bg-red-500"
      : pct >= 80
        ? "bg-amber-500"
        : "bg-green-500";
  return (
    <div className="mb-3">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[12px] font-medium text-gray-700">{label}</span>
        <span className="text-[11px] text-gray-500 font-mono">
          {formatter(used)} / {unlimited ? "unlimited" : formatter(cap)}
          {!unlimited && <span className="ml-1.5 text-gray-400">({pct}%)</span>}
        </span>
      </div>
      <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
        <div
          className={`h-full ${color} transition-all`}
          style={{ width: unlimited ? "100%" : `${pct}%`, opacity: unlimited ? 0.3 : 1 }}
        />
      </div>
    </div>
  );
}

const FEATURE_LABELS: Record<string, string> = {
  iceberg: "Iceberg REST Catalog",
  delta_sharing: "Delta Sharing",
  kms: "SSE-KMS + external KMS (Vault)",
  multi_tenancy: "Multi-tenancy",
  oidc: "OIDC / external SSO",
  lrc: "LRC erasure coding",
};

const ALL_ENTERPRISE_FEATURES = Object.keys(FEATURE_LABELS);

function formatTs(ts: number): string {
  if (!ts) return "never";
  return new Date(ts * 1000).toLocaleString();
}

export default function LicensePage() {
  const [license, setLicense] = useState<LicenseInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const load = () => {
    setError("");
    fetch("/_admin/license", { credentials: "include" })
      .then((r) => r.json())
      .then(setLicense)
      .catch((e) => setError(String(e)));
  };
  useEffect(load, []);

  const onFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (!f) return;
    setUploading(true);
    setError("");
    setNotice("");
    const body = await f.text();
    const r = await fetch("/_admin/license", {
      method: "PUT",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body,
    });
    setUploading(false);
    if (!r.ok) {
      try {
        const j = await r.json();
        setError(j.message || j.error || "Install failed");
      } catch {
        setError(await r.text());
      }
      return;
    }
    setNotice("License installed — Enterprise features unlocked.");
    load();
  };

  const remove = async () => {
    if (!confirm("Remove the installed license? Enterprise features will lock immediately.")) return;
    const r = await fetch("/_admin/license", { method: "DELETE", credentials: "include" });
    if (!r.ok) {
      setError(await r.text());
      return;
    }
    setNotice("License removed — reverted to Community tier.");
    load();
  };

  const isEnterprise = license?.tier === "enterprise";
  const enabled = new Set(license?.enabled_features || []);

  return (
    <div className="p-6 max-w-4xl">
      <PageHeader
        title="License"
        description="Install an Enterprise license to unlock Iceberg tables, Delta Sharing, SSE-KMS, multi-tenancy, OIDC, and LRC erasure codes."
      />

      {error && (
        <div className="mb-4 px-3 py-2 rounded-lg bg-red-50 border border-red-200 text-[12px] text-red-700">
          {error}
        </div>
      )}
      {notice && (
        <div className="mb-4 px-3 py-2 rounded-lg bg-green-50 border border-green-200 text-[12px] text-green-700 flex items-center gap-1.5">
          <CheckCircle2 size={13} /> {notice}
        </div>
      )}

      <Card
        title={
          <div className="flex items-center gap-2">
            <ShieldCheck size={14} className={isEnterprise ? "text-green-600" : "text-gray-400"} />
            <span>Current tier: {isEnterprise ? "Enterprise" : "Community"}</span>
          </div>
        }
        className="mb-4"
      >
        {license && (
          <div className="grid grid-cols-2 gap-3 text-[12px]">
            <div>
              <div className="text-[10px] uppercase text-gray-400 mb-0.5">Licensee</div>
              <div className="font-medium text-gray-900">{license.licensee}</div>
            </div>
            <div>
              <div className="text-[10px] uppercase text-gray-400 mb-0.5">Expires</div>
              <div className="font-medium text-gray-900">{formatTs(license.expires_at)}</div>
            </div>
            {license.issued_at > 0 && (
              <div>
                <div className="text-[10px] uppercase text-gray-400 mb-0.5">Issued</div>
                <div className="font-medium text-gray-900">{formatTs(license.issued_at)}</div>
              </div>
            )}
          </div>
        )}

        <div className="flex gap-2 mt-4 pt-3 border-t border-gray-100">
          <label className="flex items-center gap-1 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800 cursor-pointer">
            <Upload size={13} /> {uploading ? "Installing…" : "Install license file"}
            <input type="file" accept="application/json,.license,.json" onChange={onFile} className="hidden" disabled={uploading} />
          </label>
          {isEnterprise && (
            <button
              onClick={remove}
              className="flex items-center gap-1 px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
            >
              <Trash2 size={13} /> Remove license
            </button>
          )}
        </div>
      </Card>

      {license && (license.limits?.max_nodes || license.limits?.max_raw_capacity_bytes) ? (
        <Card title="Capacity usage" className="mb-4">
          <p className="text-[11px] text-gray-500 mb-3">
            New pools and buckets are blocked when any cap is reached (scale-up block).
            Existing data keeps serving reads and writes.
          </p>
          <UsageBar
            label="Storage nodes"
            used={license.usage?.node_count ?? 0}
            cap={license.limits?.max_nodes ?? 0}
            formatter={(n) => String(n)}
          />
          <UsageBar
            label="Raw capacity"
            used={license.usage?.raw_capacity_bytes ?? 0}
            cap={license.limits?.max_raw_capacity_bytes ?? 0}
            formatter={formatBytes}
          />
        </Card>
      ) : null}

      <Card title="Enterprise features">
        <ul className="space-y-1.5">
          {ALL_ENTERPRISE_FEATURES.map((f) => {
            const on = enabled.has(f);
            return (
              <li key={f} className="flex items-center justify-between px-2.5 py-1.5 bg-gray-50 rounded-lg">
                <div className="flex items-center gap-2">
                  {on ? (
                    <CheckCircle2 size={14} className="text-green-600" />
                  ) : (
                    <Lock size={14} className="text-gray-400" />
                  )}
                  <span className={`text-[12px] ${on ? "text-gray-900" : "text-gray-500"}`}>
                    {FEATURE_LABELS[f]}
                  </span>
                </div>
                <span
                  className={`text-[10px] uppercase tracking-wider ${
                    on ? "text-green-700" : "text-gray-400"
                  }`}
                >
                  {on ? "Enabled" : "Locked"}
                </span>
              </li>
            );
          })}
        </ul>
      </Card>
    </div>
  );
}
