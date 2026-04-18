import { useEffect } from "react";
import { X } from "lucide-react";

interface Props {
  /// Rendered as the drawer title.
  title: React.ReactNode;
  /// Optional sub-line under the title — usually a BreadcrumbPath.
  subtitle?: React.ReactNode;
  /// Small status indicator in the header (e.g. a StatusDot).
  eyebrow?: React.ReactNode;
  /// Drawer body; usually a stack of Cards.
  children: React.ReactNode;
  /// Called when the close button or Escape is pressed.
  onClose: () => void;
  /// Width in tailwind units. Default w-80 (320px) matches the mock's
  /// right-panel sizing for host detail.
  width?: string;
}

/// Right-side in-flow drawer. Unlike a modal overlay, this primitive
/// takes flex space next to the main content — callers wrap the page
/// in a flex container and conditionally render the drawer. That keeps
/// the tree / table responsive to the drawer being open, which matches
/// the Cluster Topology mock where the left tree narrows while the
/// right detail is visible.
export default function Drawer({
  title,
  subtitle,
  eyebrow,
  children,
  onClose,
  width = "w-80",
}: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <aside
      className={`${width} shrink-0 border-l border-gray-200 bg-white overflow-y-auto`}
    >
      <div className="px-4 py-3 border-b border-gray-200 flex items-start justify-between gap-2">
        <div className="min-w-0">
          {eyebrow && <div className="mb-1">{eyebrow}</div>}
          <h2 className="text-[14px] font-semibold text-gray-900 truncate">
            {title}
          </h2>
          {subtitle && <div className="mt-0.5">{subtitle}</div>}
        </div>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-700 p-1 rounded hover:bg-gray-50 shrink-0"
          aria-label="Close details"
        >
          <X size={14} />
        </button>
      </div>
      <div className="p-4 space-y-4">{children}</div>
    </aside>
  );
}
