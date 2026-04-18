interface Props {
  title?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  headerAction?: React.ReactNode;
}

export default function Card({ title, children, className = "", headerAction }: Props) {
  return (
    <div className={`bg-white border border-gray-200 rounded-xl overflow-hidden ${className}`}>
      {title && (
        <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
          <h3 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">{title}</h3>
          {headerAction}
        </div>
      )}
      <div className="p-4">{children}</div>
    </div>
  );
}
