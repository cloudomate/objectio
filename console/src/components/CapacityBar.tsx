interface Props {
  used: number;
  total: number;
  /// Display the used/total as human-readable text next to the bar.
  showLabel?: boolean;
  /// Optional line shown above the bar, e.g. "12 OSDs" or "Pool capacity".
  caption?: React.ReactNode;
  /// Inline layout renders caption/bar/label side-by-side; stacked puts
  /// them one above the other (better inside a row with a caption header).
  layout?: "inline" | "stacked";
  className?: string;
}

function formatBytes(b: number): string {
  if (b === 0) return "0";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(b) / Math.log(1024)));
  const v = b / Math.pow(1024, i);
  return `${v >= 10 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

export default function CapacityBar({
  used,
  total,
  showLabel = true,
  caption,
  layout = "stacked",
  className = "",
}: Props) {
  const pct = total === 0 ? 0 : Math.min(100, Math.round((used / total) * 100));
  const fill =
    pct >= 90
      ? "bg-red-500"
      : pct >= 75
        ? "bg-amber-500"
        : "bg-gradient-to-r from-emerald-400 to-blue-500";

  const bar = (
    <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden">
      <div
        className={`h-full rounded-full ${fill}`}
        style={{ width: `${pct}%` }}
      />
    </div>
  );

  if (layout === "inline") {
    return (
      <div className={`flex items-center gap-2 ${className}`}>
        {caption && <span className="text-[11px] text-gray-500">{caption}</span>}
        <div className="flex-1 min-w-[60px]">{bar}</div>
        {showLabel && (
          <span className="text-[11px] text-gray-500 w-12 text-right tabular-nums">
            {pct}%
          </span>
        )}
      </div>
    );
  }

  return (
    <div className={className}>
      {caption && (
        <div className="text-[12px] font-medium text-gray-800 mb-0.5">
          {caption}
        </div>
      )}
      {bar}
      {showLabel && (
        <div className="text-[10px] text-gray-400 mt-1 tabular-nums">
          {formatBytes(used)} / {formatBytes(total)}
        </div>
      )}
    </div>
  );
}
