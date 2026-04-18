import { ChevronDown, ChevronRight } from "lucide-react";

interface Props {
  open: boolean;
  onToggle: () => void;
  /// Main row content. Rendered next to the chevron toggle.
  header: React.ReactNode;
  /// Children rows rendered below when expanded. Caller is responsible
  /// for styling the inner rows — this primitive only handles the
  /// chevron, padding, and border between parent and children.
  children: React.ReactNode;
  /// Optional click handler for selecting the parent row itself (e.g.
  /// opening a side drawer). Triggered by clicking the header area,
  /// but NOT the chevron itself (that only toggles expansion).
  onSelect?: () => void;
  selected?: boolean;
  className?: string;
}

export default function ExpandableRow({
  open,
  onToggle,
  header,
  children,
  onSelect,
  selected = false,
  className = "",
}: Props) {
  return (
    <div
      className={`border-b border-gray-100 ${selected ? "bg-blue-50/60" : ""} ${className}`}
    >
      <div
        className={`flex items-stretch ${onSelect ? "cursor-pointer hover:bg-gray-50" : ""}`}
        onClick={onSelect}
      >
        <button
          onClick={(e) => {
            e.stopPropagation();
            onToggle();
          }}
          className="px-2 flex items-center text-gray-400 hover:text-gray-700 shrink-0"
          aria-label={open ? "Collapse" : "Expand"}
          aria-expanded={open}
        >
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </button>
        <div className="flex-1 min-w-0 py-2 pr-3">{header}</div>
      </div>
      {open && <div className="bg-gray-50/50 border-t border-gray-100">{children}</div>}
    </div>
  );
}
