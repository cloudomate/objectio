interface Props {
  title: string;
  description?: string;
  action?: React.ReactNode;
}

export default function PageHeader({ title, description, action }: Props) {
  return (
    <div className="flex items-center justify-between mb-4">
      <div>
        <h1 className="text-[15px] font-medium text-gray-900">{title}</h1>
        {description && (
          <p className="text-[12px] text-gray-500 mt-0.5">{description}</p>
        )}
      </div>
      {action && <div>{action}</div>}
    </div>
  );
}
