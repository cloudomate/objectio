interface Tab<K extends string> {
  key: K;
  label: React.ReactNode;
  /// Optional count/badge rendered after the label — e.g. "Has Issues (2)".
  count?: number;
  /// Optional icon before the label.
  icon?: React.ReactNode;
  disabled?: boolean;
}

interface Props<K extends string> {
  tabs: Tab<K>[];
  active: K;
  onChange: (key: K) => void;
  /// "pill" renders a segmented-control style grouped pill (matches the
  /// "All Nodes / Has Issues" control in the mock). "underline" renders
  /// classic underlined tabs for page-level tabs like Pool Config /
  /// Volumes / Policies.
  variant?: "pill" | "underline";
  className?: string;
}

export default function Tabs<K extends string>({
  tabs,
  active,
  onChange,
  variant = "underline",
  className = "",
}: Props<K>) {
  if (variant === "pill") {
    return (
      <div
        className={`inline-flex items-center gap-1 bg-gray-100 p-0.5 rounded-lg ${className}`}
        role="tablist"
      >
        {tabs.map((t) => {
          const isActive = t.key === active;
          return (
            <button
              key={t.key}
              role="tab"
              aria-selected={isActive}
              disabled={t.disabled}
              onClick={() => onChange(t.key)}
              className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[12px] font-medium transition-colors ${
                isActive
                  ? "bg-white text-gray-900 shadow-sm"
                  : "text-gray-600 hover:text-gray-900"
              } disabled:opacity-40 disabled:cursor-not-allowed`}
            >
              {t.icon}
              {t.label}
              {typeof t.count === "number" && (
                <span
                  className={`inline-flex items-center justify-center min-w-[16px] px-1 h-4 rounded-full text-[10px] font-semibold ${
                    isActive
                      ? "bg-blue-100 text-blue-700"
                      : "bg-gray-200 text-gray-600"
                  }`}
                >
                  {t.count}
                </span>
              )}
            </button>
          );
        })}
      </div>
    );
  }

  return (
    <div
      className={`flex items-center gap-1 border-b border-gray-200 ${className}`}
      role="tablist"
    >
      {tabs.map((t) => {
        const isActive = t.key === active;
        return (
          <button
            key={t.key}
            role="tab"
            aria-selected={isActive}
            disabled={t.disabled}
            onClick={() => onChange(t.key)}
            className={`flex items-center gap-1.5 px-3 py-2 text-[13px] font-medium border-b-2 -mb-px transition-colors ${
              isActive
                ? "border-blue-600 text-blue-700"
                : "border-transparent text-gray-500 hover:text-gray-900"
            } disabled:opacity-40 disabled:cursor-not-allowed`}
          >
            {t.icon}
            {t.label}
            {typeof t.count === "number" && (
              <span
                className={`inline-flex items-center justify-center min-w-[16px] px-1 h-4 rounded-full text-[10px] font-semibold ${
                  isActive
                    ? "bg-blue-100 text-blue-700"
                    : "bg-gray-100 text-gray-600"
                }`}
              >
                {t.count}
              </span>
            )}
          </button>
        );
      })}
    </div>
  );
}
