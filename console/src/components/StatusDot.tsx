interface Props {
  status: "healthy" | "warning" | "error" | "unknown" | "info";
  label?: string;
  size?: "sm" | "md";
}

const dots = {
  healthy: "bg-emerald-500",
  warning: "bg-amber-500",
  error: "bg-red-500",
  unknown: "bg-gray-400",
  info: "bg-blue-500",
};

const labelColor = {
  healthy: "text-emerald-700",
  warning: "text-amber-700",
  error: "text-red-700",
  unknown: "text-gray-500",
  info: "text-blue-700",
};

/// Colored dot + optional label. Use inline for compact status indicators
/// in row headers, drawers, tree nodes. For badges with a chip background
/// use StatusBadge; a dot + text is cheaper visually.
export default function StatusDot({ status, label, size = "sm" }: Props) {
  const dotSize = size === "md" ? "w-2 h-2" : "w-1.5 h-1.5";
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className={`${dotSize} rounded-full ${dots[status]}`} />
      {label && (
        <span className={`text-[12px] font-medium ${labelColor[status]}`}>
          {label}
        </span>
      )}
    </span>
  );
}
