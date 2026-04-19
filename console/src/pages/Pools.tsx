import { useEffect, useMemo, useState } from "react";
import {
  Layers,
  Plus,
  Trash2,
  Edit,
  Shield,
  Info,
  AlertTriangle,
  Check,
  ExternalLink,
  Minus,
  Plus as PlusIcon,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import StatusBadge from "../components/StatusBadge";
import Tabs from "../components/Tabs";
import SummaryPanel, {
  type ValidationState,
} from "../components/SummaryPanel";

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
  // PG-engine fields; optional because older pools + older gateways
  // may not return them.
  pg_count?: number;
  tier?: string;
}

const emptyPool: Omit<Pool, "created_at"> = {
  name: "",
  ec_type: 0,
  ec_k: 4,
  ec_m: 2,
  ec_local_parity: 0,
  ec_global_parity: 0,
  replication_count: 3,
  osd_tags: [],
  failure_domain: "rack",
  quota_bytes: 0,
  description: "",
  enabled: true,
};

function formatBytes(b: number) {
  if (!b) return "Unlimited";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(b) / Math.log(1024));
  return `${(b / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function ecLabel(p: Pool) {
  if (p.ec_type === 2) return `${p.replication_count}x Replication`;
  if (p.ec_type === 1)
    return `LRC ${p.ec_k}+${p.ec_local_parity}+${p.ec_global_parity}`;
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

// Maps pool EC config → an implicit CRUSH rule name. Real rules are
// defined server-side; this is just a convenience label for the summary.
function crushRuleName(form: Omit<Pool, "created_at">) {
  if (form.ec_type === 2)
    return `rep_${form.replication_count}_${form.failure_domain}_rule`;
  return `ec_${form.ec_k}_${form.ec_m}_${form.failure_domain}_rule`;
}

type WizardTab = "config" | "volumes" | "policies";

// ---------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------

export default function Pools() {
  const [pools, setPools] = useState<Pool[]>([]);
  const [editing, setEditing] = useState<string | null>(null);
  const [form, setForm] = useState(emptyPool);
  const [tagsStr, setTagsStr] = useState("");
  const [loading, setLoading] = useState(true);
  const [osdCount, setOsdCount] = useState(0);
  const [k8sNodeCount, setK8sNodeCount] = useState(0);
  const [tab, setTab] = useState<WizardTab>("config");
  const [compression, setCompression] = useState(true);
  const [encryption, setEncryption] = useState(false);

  const load = () => {
    setLoading(true);
    Promise.all([
      fetch("/_admin/pools")
        .then((r) => r.json())
        .catch(() => []),
      fetch("/_admin/nodes")
        .then((r) => r.json())
        .catch(() => ({ nodes: [] })),
    ])
      .then(([p, n]) => {
        setPools(Array.isArray(p) ? p : p.pools || []);
        const nodes = n.nodes || [];
        setOsdCount(
          nodes.filter((nd: { online: boolean }) => nd.online).length,
        );
        const uniqueK8s = new Set(
          nodes
            .map((nd: { kubernetes_node: string }) => nd.kubernetes_node)
            .filter(Boolean),
        );
        setK8sNodeCount(uniqueK8s.size);
      })
      .finally(() => setLoading(false));
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
    setTab("config");
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

  // -----------------------------------------------------------------
  // Derived — validation + summary rows for the right-side panel
  // -----------------------------------------------------------------
  const validation: ValidationState = useMemo(() => {
    if (!form.name.trim()) {
      return { kind: "warning", message: "Pool name is required." };
    }
    const needed = ecMinNodes(form as Pool);
    if (osdCount > 0 && needed > osdCount) {
      return {
        kind: "error",
        message: `Needs ${needed} OSDs but only ${osdCount} online.`,
      };
    }
    if (form.failure_domain !== "osd" && k8sNodeCount <= 1) {
      return {
        kind: "warning",
        message: `Failure domain "${form.failure_domain}" needs 2+ distinct nodes. Only ${k8sNodeCount} observed.`,
      };
    }
    return {
      kind: "passed",
      message: `Requested scheme is valid for ${osdCount} online OSD${osdCount !== 1 ? "s" : ""}.`,
    };
  }, [form, osdCount, k8sNodeCount]);

  return (
    <div className="p-6">
      <PageHeader
        title="Storage Management"
        description="Manage pools, volumes, and data policies."
        action={
          !editing ? (
            <button
              onClick={() => startEdit()}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white rounded-lg text-[12px] font-medium hover:bg-blue-700"
            >
              <Plus size={14} /> Create Pool
            </button>
          ) : null
        }
      />

      {editing ? (
        <PoolWizard
          editing={editing}
          form={form}
          setForm={setForm}
          tagsStr={tagsStr}
          setTagsStr={setTagsStr}
          tab={tab}
          setTab={setTab}
          osdCount={osdCount}
          k8sNodeCount={k8sNodeCount}
          validation={validation}
          compression={compression}
          setCompression={setCompression}
          encryption={encryption}
          setEncryption={setEncryption}
          onCancel={() => setEditing(null)}
          onSave={save}
        />
      ) : (
        <PoolList
          loading={loading}
          pools={pools}
          onEdit={startEdit}
          onDelete={remove}
        />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------
// Wizard (Create/Edit)
// ---------------------------------------------------------------------

interface WizardProps {
  editing: string;
  form: Omit<Pool, "created_at">;
  setForm: React.Dispatch<React.SetStateAction<Omit<Pool, "created_at">>>;
  tagsStr: string;
  setTagsStr: (s: string) => void;
  tab: WizardTab;
  setTab: (t: WizardTab) => void;
  osdCount: number;
  k8sNodeCount: number;
  validation: ValidationState;
  compression: boolean;
  setCompression: (b: boolean) => void;
  encryption: boolean;
  setEncryption: (b: boolean) => void;
  onCancel: () => void;
  onSave: () => void;
}

function PoolWizard({
  editing,
  form,
  setForm,
  tagsStr,
  setTagsStr,
  tab,
  setTab,
  osdCount,
  k8sNodeCount,
  validation,
  compression,
  setCompression,
  encryption,
  setEncryption,
  onCancel,
  onSave,
}: WizardProps) {
  const isNew = editing === "__new__";
  return (
    <div className="flex gap-4 items-start">
      {/* Form column */}
      <div className="flex-1 min-w-0 space-y-4">
        <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-4">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center">
                <Layers size={14} className="text-blue-600" />
              </div>
              <div>
                <h2 className="text-[14px] font-semibold text-gray-900">
                  {isNew ? "Create Storage Pool" : `Edit: ${editing}`}
                </h2>
                <p className="text-[11px] text-gray-500">
                  Configure redundancy, placement, and policies.
                </p>
              </div>
            </div>
            <Tabs
              variant="pill"
              active={tab}
              onChange={setTab}
              tabs={[
                { key: "config" as const, label: "Pool Config" },
                { key: "volumes" as const, label: "Volumes" },
                { key: "policies" as const, label: "Policies" },
              ]}
            />
          </div>

          <div className="p-4">
            {tab === "config" && (
              <ConfigTab
                form={form}
                setForm={setForm}
                tagsStr={tagsStr}
                setTagsStr={setTagsStr}
                osdCount={osdCount}
                k8sNodeCount={k8sNodeCount}
                compression={compression}
                setCompression={setCompression}
                encryption={encryption}
                setEncryption={setEncryption}
                isNew={isNew}
              />
            )}
            {tab === "volumes" && (
              <EmptyTabPlaceholder
                title="Volumes"
                message="Per-pool volume provisioning UI is not yet wired. Use the block CLI or block-gateway API in the meantime."
              />
            )}
            {tab === "policies" && (
              <EmptyTabPlaceholder
                title="Policies"
                message="Lifecycle + object-lock policies for this pool will live here. Bucket-level policies remain on the Policies page."
              />
            )}
          </div>
        </div>
      </div>

      {/* Right column: Change Summary */}
      <SummaryPanel
        title="Change Summary"
        subtitle="Review configuration before applying."
        validation={validation}
        rows={[
          { label: "Pool Name", value: form.name || "—", mono: true },
          { label: "Type", value: ecLabel(form as Pool) },
          { label: "Failure Domain", value: form.failure_domain },
          { label: "CRUSH Rule", value: crushRuleName(form), mono: true },
          {
            label: "Compression",
            value: compression ? "Enabled (LZ4)" : "Disabled",
          },
          {
            label: "Encryption",
            value: encryption ? "AES-256 (KMS)" : "Disabled",
          },
          {
            label: "Expected Efficiency",
            value: ecEfficiency(form as Pool),
          },
          {
            label: "Fault Tolerance",
            value: `${form.ec_type === 2 ? form.replication_count - 1 : form.ec_m} failures`,
          },
        ]}
        footer={
          <div className="space-y-2">
            <div className="flex items-start gap-1.5 p-2 rounded-md bg-amber-50 border border-amber-200">
              <AlertTriangle
                size={12}
                className="text-amber-600 shrink-0 mt-px"
              />
              <div className="text-[10px] text-amber-800">
                <div className="font-semibold">Data Rebalancing</div>
                <div className="opacity-80">
                  Applying this pool may initiate background peering and
                  temporarily affect cluster IOPS.
                </div>
              </div>
            </div>
            <div className="flex gap-1.5">
              <button
                onClick={onCancel}
                className="flex-1 px-2.5 py-1.5 border border-gray-200 text-gray-700 rounded-md text-[12px] font-medium hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={onSave}
                disabled={validation.kind === "error" || !form.name.trim()}
                className="flex-1 flex items-center justify-center gap-1 px-2.5 py-1.5 bg-blue-600 text-white rounded-md text-[12px] font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Check size={12} />
                {isNew ? "Create Pool" : "Apply"}
              </button>
            </div>
          </div>
        }
      />
    </div>
  );
}

// ---------------------------------------------------------------------
// Config tab sections
// ---------------------------------------------------------------------

interface ConfigTabProps {
  form: Omit<Pool, "created_at">;
  setForm: React.Dispatch<React.SetStateAction<Omit<Pool, "created_at">>>;
  tagsStr: string;
  setTagsStr: (s: string) => void;
  osdCount: number;
  k8sNodeCount: number;
  compression: boolean;
  setCompression: (b: boolean) => void;
  encryption: boolean;
  setEncryption: (b: boolean) => void;
  isNew: boolean;
}

function ConfigTab({
  form,
  setForm,
  tagsStr,
  setTagsStr,
  osdCount,
  k8sNodeCount,
  compression,
  setCompression,
  encryption,
  setEncryption,
  isNew,
}: ConfigTabProps) {
  return (
    <div className="space-y-6">
      {/* Basic Information */}
      <Section title="Basic Information" icon={<Info size={13} />}>
        <div className="grid grid-cols-2 gap-3">
          <FormField label="Pool Name">
            <input
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              disabled={!isNew}
              placeholder="production-nvme-pool"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none disabled:bg-gray-100"
            />
          </FormField>
          <FormField label="Application Type">
            <select
              value={form.description}
              onChange={(e) =>
                setForm({ ...form, description: e.target.value })
              }
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] bg-white focus:ring-1 focus:ring-blue-500 focus:outline-none"
            >
              <option value="">Object / S3</option>
              <option value="Database (Block)">Database (Block)</option>
              <option value="Filesystem">Filesystem</option>
              <option value="Archive">Archive</option>
            </select>
          </FormField>
        </div>
      </Section>

      {/* Redundancy Strategy */}
      <Section
        title="Redundancy Strategy"
        icon={<Shield size={13} />}
        rightAccessory={
          form.ec_type === 2 ? (
            <Tag tone="blue">Replication Selected</Tag>
          ) : (
            <Tag tone="blue">Erasure Coding Selected</Tag>
          )
        }
      >
        <div className="grid grid-cols-2 gap-2">
          <RadioCard
            selected={form.ec_type === 2}
            onClick={() =>
              setForm({ ...form, ec_type: 2, replication_count: 3 })
            }
            title="Replication"
            desc="High performance, higher storage overhead. Best for small block I/O."
          />
          <RadioCard
            selected={form.ec_type !== 2}
            onClick={() => setForm({ ...form, ec_type: 0, ec_k: 4, ec_m: 2 })}
            title="Erasure Coding"
            desc="Efficient storage, slightly higher latency. Best for object/bulk storage."
          />
        </div>

        {form.ec_type === 2 ? (
          <div className="mt-3 p-3 bg-gray-50 border border-gray-200 rounded-lg">
            <div className="text-[11px] text-gray-500 uppercase tracking-wider mb-2">
              Replica Count
            </div>
            <Stepper
              value={form.replication_count}
              onChange={(v) => setForm({ ...form, replication_count: v })}
              min={1}
              max={10}
              width="w-28"
            />
            <div className="mt-2 flex items-center gap-1.5 text-[11px] text-emerald-700 bg-emerald-50 border border-emerald-200 rounded px-2 py-1 w-fit">
              <Check size={11} />
              {(100 / Math.max(1, form.replication_count)).toFixed(0)}%
              efficiency. Survives {form.replication_count - 1} concurrent
              failures.
            </div>
          </div>
        ) : (
          <div className="mt-3 p-3 bg-gray-50 border border-gray-200 rounded-lg">
            <div className="text-[11px] text-gray-500 uppercase tracking-wider mb-2">
              Erasure Code Profile (K + M)
            </div>
            <div className="flex items-start gap-8">
              <div>
                <div className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">
                  Data Chunks (K)
                </div>
                <Stepper
                  value={form.ec_k}
                  onChange={(v) => setForm({ ...form, ec_k: v })}
                  min={1}
                  max={20}
                />
              </div>
              <div>
                <div className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">
                  Coding Chunks (M)
                </div>
                <Stepper
                  value={form.ec_m}
                  onChange={(v) => setForm({ ...form, ec_m: v })}
                  min={1}
                  max={10}
                />
              </div>
            </div>
            <div className="mt-3 flex items-center gap-1.5 text-[11px] text-emerald-700 bg-emerald-50 border border-emerald-200 rounded px-2 py-1 w-fit">
              <Check size={11} />
              Yields {ecEfficiency(form as Pool)} storage efficiency. Can
              survive {form.ec_m} concurrent {form.failure_domain}{" "}
              failures.
            </div>
          </div>
        )}
      </Section>

      {/* Placement & Failure Domain */}
      <Section
        title="Placement & Failure Domain"
        icon={<Layers size={13} />}
        rightAccessory={
          <button className="flex items-center gap-1 text-[11px] text-blue-600 hover:text-blue-700">
            View Topology <ExternalLink size={10} />
          </button>
        }
      >
        <FormField
          label="Failure Domain Level"
          hint="Determines the physical level at which data replicas or EC chunks are distributed to ensure survival."
        >
          <select
            value={form.failure_domain}
            onChange={(e) =>
              setForm({ ...form, failure_domain: e.target.value })
            }
            className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] bg-white focus:ring-1 focus:ring-blue-500 focus:outline-none"
          >
            <option value="osd">OSD (disk-level)</option>
            <option
              value="host"
              disabled={k8sNodeCount <= 1 && k8sNodeCount > 0}
            >
              Host{k8sNodeCount <= 1 && k8sNodeCount > 0 ? " (need 2+ hosts)" : ""}
            </option>
            <option
              value="rack"
              disabled={k8sNodeCount <= 1 && k8sNodeCount > 0}
            >
              Rack
            </option>
            <option
              value="datacenter"
              disabled={k8sNodeCount <= 1 && k8sNodeCount > 0}
            >
              Datacenter
            </option>
          </select>
        </FormField>
        <div className="mt-3">
          <FormField label="Target CRUSH Rule">
            <div className="flex gap-2">
              <input
                readOnly
                value={crushRuleName(form)}
                className="flex-1 px-2.5 py-1.5 border border-gray-200 bg-gray-50 rounded-lg text-[13px] font-mono text-gray-700"
              />
              <button
                disabled
                title="CRUSH rule editor is admin-only; coming soon"
                className="px-3 py-1.5 border border-gray-200 text-gray-500 rounded-lg text-[12px] font-medium hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Edit Rule
              </button>
            </div>
          </FormField>
        </div>
      </Section>

      {/* Features & Quotas */}
      <Section title="Features & Quotas" icon={<Shield size={13} />}>
        <div className="grid grid-cols-2 gap-3">
          <FeatureToggle
            title="Compression"
            desc="Inline LZ4 compression"
            checked={compression}
            onChange={setCompression}
          />
          <FeatureToggle
            title="Encryption (At Rest)"
            desc="AES-256 via KMS"
            checked={encryption}
            onChange={setEncryption}
          />
        </div>
        <div className="mt-3 grid grid-cols-2 gap-3">
          <FormField label="OSD Tags" hint="Optional — limits placement to matching OSDs.">
            <input
              value={tagsStr}
              onChange={(e) => setTagsStr(e.target.value)}
              placeholder="nvme, ssd"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:ring-1 focus:ring-blue-500 focus:outline-none"
            />
          </FormField>
          <FormField label="Storage Quota" hint="Optional">
            <select
              value={form.quota_bytes}
              onChange={(e) =>
                setForm({ ...form, quota_bytes: Number(e.target.value) })
              }
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] bg-white focus:ring-1 focus:ring-blue-500 focus:outline-none"
            >
              <option value={0}>Unlimited</option>
              <option value={107374182400}>100 GB</option>
              <option value={1099511627776}>1 TB</option>
              <option value={10995116277760}>10 TB</option>
              <option value={109951162777600}>100 TB</option>
            </select>
          </FormField>
        </div>
        {osdCount > 0 && (
          <p className="mt-2 text-[11px] text-gray-400">
            {osdCount} OSDs on {k8sNodeCount} host
            {k8sNodeCount !== 1 ? "s" : ""} available in the cluster.
          </p>
        )}
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------
// Shared local building blocks
// ---------------------------------------------------------------------

function Section({
  title,
  icon,
  rightAccessory,
  children,
}: {
  title: string;
  icon?: React.ReactNode;
  rightAccessory?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="border border-gray-200 rounded-xl">
      <header className="flex items-center justify-between px-4 py-2.5 border-b border-gray-200 bg-gray-50">
        <div className="flex items-center gap-1.5 text-[12px] font-semibold text-gray-800">
          <span className="text-gray-400">{icon}</span>
          {title}
        </div>
        {rightAccessory}
      </header>
      <div className="p-4">{children}</div>
    </section>
  );
}

function FormField({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block">
      <div className="text-[11px] font-medium text-gray-700 mb-1">{label}</div>
      {hint && <div className="text-[10px] text-gray-400 mb-1">{hint}</div>}
      {children}
    </label>
  );
}

function RadioCard({
  selected,
  onClick,
  title,
  desc,
}: {
  selected: boolean;
  onClick: () => void;
  title: string;
  desc: string;
}) {
  return (
    <button
      onClick={onClick}
      className={`text-left p-3 rounded-lg border transition-colors ${
        selected
          ? "border-blue-400 bg-blue-50 ring-1 ring-blue-400"
          : "border-gray-200 hover:bg-gray-50"
      }`}
    >
      <div className="flex items-center gap-2 mb-1">
        <span
          className={`w-3 h-3 rounded-full border-2 flex items-center justify-center shrink-0 ${
            selected ? "border-blue-500" : "border-gray-300"
          }`}
        >
          {selected && <span className="w-1.5 h-1.5 bg-blue-500 rounded-full" />}
        </span>
        <span className="text-[13px] font-semibold text-gray-900">{title}</span>
      </div>
      <p className="text-[11px] text-gray-500">{desc}</p>
    </button>
  );
}

function Stepper({
  value,
  onChange,
  min,
  max,
  width = "w-24",
}: {
  value: number;
  onChange: (n: number) => void;
  min: number;
  max: number;
  width?: string;
}) {
  return (
    <div className={`inline-flex items-center border border-gray-300 rounded-lg ${width}`}>
      <button
        onClick={() => onChange(Math.max(min, value - 1))}
        className="px-2 py-1.5 text-gray-500 hover:text-gray-900 hover:bg-gray-50 rounded-l-lg"
        aria-label="Decrement"
      >
        <Minus size={12} />
      </button>
      <input
        type="number"
        min={min}
        max={max}
        value={value}
        onChange={(e) => {
          const n = Number(e.target.value);
          if (!Number.isNaN(n)) onChange(Math.min(max, Math.max(min, n)));
        }}
        className="flex-1 min-w-0 px-1 py-1 text-center text-[13px] font-semibold text-gray-900 focus:outline-none tabular-nums"
      />
      <button
        onClick={() => onChange(Math.min(max, value + 1))}
        className="px-2 py-1.5 text-gray-500 hover:text-gray-900 hover:bg-gray-50 rounded-r-lg"
        aria-label="Increment"
      >
        <PlusIcon size={12} />
      </button>
    </div>
  );
}

function FeatureToggle({
  title,
  desc,
  checked,
  onChange,
}: {
  title: string;
  desc: string;
  checked: boolean;
  onChange: (b: boolean) => void;
}) {
  return (
    <div className="flex items-start justify-between gap-3 p-3 border border-gray-200 rounded-lg">
      <div>
        <div className="text-[13px] font-semibold text-gray-900">{title}</div>
        <div className="text-[11px] text-gray-500">{desc}</div>
      </div>
      <button
        onClick={() => onChange(!checked)}
        role="switch"
        aria-checked={checked}
        className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors shrink-0 ${
          checked ? "bg-emerald-500" : "bg-gray-300"
        }`}
      >
        <span
          className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
            checked ? "translate-x-4" : "translate-x-0.5"
          }`}
        />
      </button>
    </div>
  );
}

function Tag({
  tone,
  children,
}: {
  tone: "blue" | "green" | "gray";
  children: React.ReactNode;
}) {
  const style =
    tone === "blue"
      ? "bg-blue-50 text-blue-700 border-blue-200"
      : tone === "green"
        ? "bg-emerald-50 text-emerald-700 border-emerald-200"
        : "bg-gray-100 text-gray-700 border-gray-200";
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold border ${style}`}
    >
      {children}
    </span>
  );
}

function EmptyTabPlaceholder({
  title,
  message,
}: {
  title: string;
  message: string;
}) {
  return (
    <div className="py-12 text-center">
      <div className="text-[14px] font-semibold text-gray-900">{title}</div>
      <p className="mt-1 text-[12px] text-gray-500 max-w-md mx-auto">{message}</p>
    </div>
  );
}

// ---------------------------------------------------------------------
// Pool list (unchanged UX; kept for non-wizard mode)
// ---------------------------------------------------------------------

function PoolList({
  loading,
  pools,
  onEdit,
  onDelete,
}: {
  loading: boolean;
  pools: Pool[];
  onEdit: (p: Pool) => void;
  onDelete: (name: string) => void;
}) {
  return (
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
            <th
              className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider"
              title="Placement groups — 0 = legacy per-object CRUSH"
            >
              PGs
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
              <td colSpan={8} className="px-4 py-8 text-center">
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
                colSpan={8}
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
                      <span className="text-[13px] font-medium">{p.name}</span>
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
                <td className="px-4 py-2 text-[12px] font-mono text-gray-500">
                  {p.pg_count && p.pg_count > 0 ? (
                    <span
                      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-indigo-50 text-indigo-700 text-[10px] font-medium"
                      title="Placement-group routed"
                    >
                      {p.pg_count.toLocaleString()}
                    </span>
                  ) : (
                    <span className="text-gray-400 text-[10px]">legacy</span>
                  )}
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
                      onClick={() => onEdit(p)}
                      className="text-gray-400 hover:text-blue-600 p-1"
                      title="Edit"
                    >
                      <Edit size={14} />
                    </button>
                    <button
                      onClick={() => onDelete(p.name)}
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
  );
}
