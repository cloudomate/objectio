interface Props {
  /// Topology path segments in order, e.g. ["us-east", "us-east-1a", "dc1", "rack-a"].
  /// Empty or missing segments are rendered as "—" to keep alignment stable.
  segments: (string | null | undefined)[];
  /// Muted separator character between segments.
  sep?: string;
  className?: string;
}

export default function BreadcrumbPath({
  segments,
  sep = "/",
  className = "",
}: Props) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 font-mono text-[11px] text-gray-500 ${className}`}
    >
      {segments.map((s, i) => (
        <span key={i} className="inline-flex items-center gap-1.5">
          <span className={s ? "text-gray-700" : "text-gray-300"}>
            {s || "—"}
          </span>
          {i < segments.length - 1 && (
            <span className="text-gray-300">{sep}</span>
          )}
        </span>
      ))}
    </span>
  );
}
