import { useEffect, useState } from "react";
import { Activity, Pause, Play, RefreshCw, Save } from "lucide-react";
import { rebalance as rebalanceApi, type RebalanceStatus, request } from "../api/client";

/// Balancing tab — live PG balancer status + tuning knobs.
///
/// The balancer re-reads these config keys on every tick, so a
/// save round-trips through Meta's Raft and takes effect within
/// one sweep (default 60s). No restart required.

interface KnobDef {
  key: string;
  label: string;
  hint: string;
  type: "number" | "bool";
  min?: number;
  max?: number;
  step?: number;
  placeholder?: string;
}

const KNOBS: KnobDef[] = [
  {
    key: "balancer/sweep_interval_seconds",
    label: "Tick interval (s)",
    hint: "How often the balancer evaluates. Clamped 5–3600.",
    type: "number",
    min: 5,
    max: 3600,
    step: 1,
    placeholder: "60",
  },
  {
    key: "balancer/overload_multiplier",
    label: "Overload multiplier",
    hint: "Trigger a move when any OSD holds ≥ this × target PGs. Default 1.20.",
    type: "number",
    min: 1.0,
    max: 5.0,
    step: 0.05,
    placeholder: "1.20",
  },
  {
    key: "balancer/underload_multiplier",
    label: "Underload multiplier",
    hint: "Also trigger when any OSD is ≤ this × target. 0 disables. Default 0.70.",
    type: "number",
    min: 0,
    max: 1.0,
    step: 0.05,
    placeholder: "0.70",
  },
  {
    key: "balancer/improvement_factor",
    label: "Improvement factor",
    hint: "Move commits only if candidate's total load ≤ factor × current. Default 0.90 (≥10% better).",
    type: "number",
    min: 0.0,
    max: 1.0,
    step: 0.01,
    placeholder: "0.90",
  },
  {
    key: "balancer/scatter_width",
    label: "Scatter width",
    hint: "Copyset-pool diversity target (Cidon/Stutsman). Default 10.",
    type: "number",
    min: 1,
    max: 100,
    step: 1,
    placeholder: "10",
  },
  {
    key: "balancer/per_tick_cap",
    label: "Per-tick cap",
    hint: "Max moves per pool per tick. 0 = auto (max(3, osds/3)).",
    type: "number",
    min: 0,
    max: 1000,
    step: 1,
    placeholder: "0",
  },
  {
    key: "balancer/paused",
    label: "Paused",
    hint: "Halt all balancer commits. Ticks still run so logs stay fresh.",
    type: "bool",
  },
];

export default function Balancing() {
  const [status, setStatus] = useState<RebalanceStatus | null>(null);
  const [values, setValues] = useState<Record<string, string>>({});
  const [saved, setSaved] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const loadStatus = async () => {
    try {
      const s = await rebalanceApi.status();
      setStatus(s);
    } catch (e) {
      setErr(String(e));
    }
  };

  const loadKnobs = async () => {
    // One GET /_admin/config returns every config entry; we filter
    // the ones that start with balancer/ and seed both values and
    // "saved" (so we can tell if there are unsaved edits).
    try {
      const all = await request<
        Array<{ key: string; value: unknown; version: number }>
      >("GET", "/_admin/config");
      const next: Record<string, string> = {};
      for (const e of all) {
        if (!e.key.startsWith("balancer/")) continue;
        // Values are stored as raw JSON — unwrap the common
        // {"value":"..."} shape the generic PUT handler wrote.
        const raw =
          typeof e.value === "object" && e.value !== null && "value" in e.value
            ? (e.value as { value: unknown }).value
            : e.value;
        next[e.key] = String(raw ?? "");
      }
      setValues(next);
      setSaved(next);
    } catch (e) {
      setErr(String(e));
    }
  };

  useEffect(() => {
    loadStatus();
    loadKnobs();
    // Status poll — balancer ticks every 60s by default, so 5s is
    // plenty granular to show progress without hammering the API.
    const h = setInterval(loadStatus, 5000);
    return () => clearInterval(h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const saveOne = async (k: string, v: string) => {
    setSaving(k);
    setErr(null);
    try {
      // The server validates the body as JSON; send raw numbers /
      // booleans so the balancer's parse::<f64>()/parse::<u64>()
      // sees a clean value, not a {"value":"0.95"} wrapper.
      const body =
        KNOBS.find((x) => x.key === k)?.type === "bool"
          ? v === "true"
            ? "true"
            : "false"
          : v;
      await request(`PUT`, `/_admin/config/${k}`, body);
      setSaved((s) => ({ ...s, [k]: v }));
      // A knob change may flip paused — refresh status immediately.
      loadStatus();
    } catch (e) {
      setErr(String(e));
    } finally {
      setSaving(null);
    }
  };

  const tone = status?.paused
    ? "bg-red-50 border-red-200 text-red-800"
    : status?.started
      ? "bg-blue-50 border-blue-200 text-blue-800"
      : "bg-gray-50 border-gray-200 text-gray-600";

  return (
    <div className="flex flex-col gap-4">
      {/* Live status card */}
      <div className={`rounded-lg border px-4 py-3 ${tone}`}>
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <span
              className={`inline-flex items-center justify-center w-8 h-8 rounded-md ${
                status?.paused
                  ? "bg-red-100"
                  : status?.started
                    ? "bg-blue-100"
                    : "bg-gray-100"
              }`}
            >
              {status?.paused ? (
                <Pause size={16} />
              ) : (
                <Activity size={16} className={status?.started ? "animate-pulse" : ""} />
              )}
            </span>
            <div>
              <div className="text-[13px] font-semibold">
                Balancer{" "}
                {status?.paused
                  ? "paused"
                  : status?.started
                    ? "running"
                    : "idle"}
              </div>
              <div className="text-[11px] opacity-80 mt-0.5">
                {status ? (
                  <>
                    {(status.pgs_moved_total ?? 0).toLocaleString()} PG moves
                    total
                    {" · "}
                    {(status.pgs_scanned_last_tick ?? 0).toLocaleString()} PGs
                    scanned last tick
                    {(status.pg_candidates_last_tick ?? 0) > 0 && (
                      <>
                        {" · "}
                        {status.pg_candidates_last_tick!.toLocaleString()}{" "}
                        candidates pending
                      </>
                    )}
                    {status.last_sweep_at > 0 && (
                      <>
                        {" · last tick "}
                        {new Date(status.last_sweep_at * 1000).toLocaleTimeString()}
                      </>
                    )}
                  </>
                ) : (
                  "Loading…"
                )}
              </div>
            </div>
          </div>
          <button
            onClick={() => {
              const paused = status?.paused;
              saveOne("balancer/paused", paused ? "false" : "true");
            }}
            disabled={!status}
            className="flex items-center gap-1.5 px-3 py-1.5 border bg-white rounded-lg text-[12px] font-medium hover:bg-gray-50 disabled:opacity-50"
          >
            {status?.paused ? <Play size={13} /> : <Pause size={13} />}
            {status?.paused ? "Resume" : "Pause"}
          </button>
        </div>
        {status?.last_error && (
          <div className="mt-2 text-[11px] text-red-700">
            last error: {status.last_error}
          </div>
        )}
      </div>

      {err && (
        <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-[12px] text-red-800">
          {err}
        </div>
      )}

      {/* Knobs */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-2.5 border-b border-gray-200 flex items-center justify-between">
          <div className="text-[12px] font-medium text-gray-700 uppercase tracking-wide">
            Tuning
          </div>
          <button
            onClick={loadKnobs}
            className="flex items-center gap-1.5 px-2 py-0.5 text-[11px] text-gray-500 hover:text-gray-800"
          >
            <RefreshCw size={11} /> Reload
          </button>
        </div>
        <div className="divide-y divide-gray-100">
          {KNOBS.map((k) => {
            const cur = values[k.key] ?? "";
            const sv = saved[k.key] ?? "";
            const dirty = cur !== sv;
            return (
              <div key={k.key} className="flex items-center gap-3 px-4 py-2.5">
                <div className="flex-1 min-w-0">
                  <div className="text-[12px] font-medium text-gray-800">
                    {k.label}
                    <span className="ml-1.5 font-mono text-[10px] text-gray-400">
                      {k.key}
                    </span>
                  </div>
                  <div className="text-[11px] text-gray-500 mt-0.5">{k.hint}</div>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  {k.type === "bool" ? (
                    <select
                      value={cur || "false"}
                      onChange={(e) =>
                        setValues((s) => ({ ...s, [k.key]: e.target.value }))
                      }
                      className="px-2 py-1 border border-gray-300 rounded text-[12px] bg-white w-28"
                    >
                      <option value="false">false</option>
                      <option value="true">true</option>
                    </select>
                  ) : (
                    <input
                      type="number"
                      min={k.min}
                      max={k.max}
                      step={k.step}
                      value={cur}
                      placeholder={k.placeholder}
                      onChange={(e) =>
                        setValues((s) => ({ ...s, [k.key]: e.target.value }))
                      }
                      className="px-2 py-1 border border-gray-300 rounded text-[12px] w-28 font-mono"
                    />
                  )}
                  <button
                    onClick={() => saveOne(k.key, cur)}
                    disabled={!dirty || saving === k.key}
                    className={`flex items-center gap-1 px-2 py-1 rounded text-[11px] font-medium ${
                      dirty
                        ? "bg-blue-600 text-white hover:bg-blue-700"
                        : "bg-gray-100 text-gray-400 cursor-not-allowed"
                    } disabled:opacity-50`}
                  >
                    <Save size={11} />
                    {saving === k.key ? "Saving…" : dirty ? "Save" : "Saved"}
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <p className="text-[11px] text-gray-500">
        Changes hot-reload — no restart needed. Tick interval changes take effect
        on the next sweep. See the balancer docs in the source for the
        algorithm details.
      </p>
    </div>
  );
}
