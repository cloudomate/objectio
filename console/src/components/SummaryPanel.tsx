import { CheckCircle2, AlertCircle, Info } from "lucide-react";

export type ValidationState =
  | { kind: "passed"; message?: string }
  | { kind: "warning"; message: string }
  | { kind: "error"; message: string };

export interface SummaryRow {
  label: string;
  /// Usually a string or number; can be a node for chips / badges.
  value: React.ReactNode;
  /// When true, value is rendered in a monospace font (good for names,
  /// IDs, rule names).
  mono?: boolean;
}

interface Props {
  title?: React.ReactNode;
  subtitle?: React.ReactNode;
  validation?: ValidationState;
  rows: SummaryRow[];
  /// Free-form content rendered below the rows (e.g. a "Data Rebalancing"
  /// warning callout, or a final CTA button).
  footer?: React.ReactNode;
  className?: string;
}

const badge = {
  passed: {
    bg: "bg-emerald-50",
    border: "border-emerald-200",
    text: "text-emerald-700",
    Icon: CheckCircle2,
    defaultMsg: "Validation passed",
  },
  warning: {
    bg: "bg-amber-50",
    border: "border-amber-200",
    text: "text-amber-700",
    Icon: AlertCircle,
    defaultMsg: "Review before applying",
  },
  error: {
    bg: "bg-red-50",
    border: "border-red-200",
    text: "text-red-700",
    Icon: AlertCircle,
    defaultMsg: "Invalid configuration",
  },
};

/// Sticky side-panel that live-summarizes the form on its left. Use for
/// multi-field pages where users want to verify intent before applying
/// (Pool Create, Policy Edit, Tenant Onboarding).
export default function SummaryPanel({
  title = "Change Summary",
  subtitle,
  validation,
  rows,
  footer,
  className = "",
}: Props) {
  return (
    <aside
      className={`w-72 shrink-0 sticky top-0 self-start ${className}`}
    >
      <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200">
          <h3 className="text-[13px] font-semibold text-gray-900 flex items-center gap-1.5">
            <Info size={13} className="text-gray-400" />
            {title}
          </h3>
          {subtitle && (
            <p className="mt-0.5 text-[11px] text-gray-500">{subtitle}</p>
          )}
        </div>

        {validation && (
          <div className={`px-4 py-2 border-b ${badge[validation.kind].border} ${badge[validation.kind].bg}`}>
            {(() => {
              const cfg = badge[validation.kind];
              const Icon = cfg.Icon;
              return (
                <div className={`flex items-start gap-1.5 text-[11px] ${cfg.text}`}>
                  <Icon size={13} className="shrink-0 mt-px" />
                  <div>
                    <div className="font-semibold">
                      {validation.kind === "passed"
                        ? "Validation Passed"
                        : validation.kind === "warning"
                          ? "Warning"
                          : "Error"}
                    </div>
                    <div className="opacity-80">
                      {validation.message ?? cfg.defaultMsg}
                    </div>
                  </div>
                </div>
              );
            })()}
          </div>
        )}

        <dl className="px-4 py-3 space-y-2">
          {rows.map((r, i) => (
            <div key={i} className="flex items-baseline justify-between gap-3">
              <dt className="text-[11px] text-gray-500 shrink-0">{r.label}</dt>
              <dd
                className={`text-[12px] text-right text-gray-900 ${r.mono ? "font-mono" : "font-medium"}`}
              >
                {r.value}
              </dd>
            </div>
          ))}
        </dl>

        {footer && <div className="px-4 py-3 border-t border-gray-200">{footer}</div>}
      </div>
    </aside>
  );
}
