import { useEffect, useState } from "react";
import {
  Warehouse,
  Table2,
  Plus,
  Trash2,
  FolderOpen,
  ChevronRight,
  ArrowLeft,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import { iceberg } from "../api/client";

interface WarehouseInfo {
  name: string;
  bucket: string;
  location: string;
  tenant: string;
  created_at: number;
}

type View = "warehouses" | "namespaces" | "tables" | "detail";

export default function IcebergCatalog() {
  const [view, setView] = useState<View>("warehouses");

  // Warehouses
  const [warehouses, setWarehouses] = useState<WarehouseInfo[]>([]);
  const [showCreateWh, setShowCreateWh] = useState(false);
  const [newWhName, setNewWhName] = useState("");

  // Namespaces
  const [selectedWh, setSelectedWh] = useState<WarehouseInfo | null>(null);
  const [namespaces, setNamespaces] = useState<string[][]>([]);
  const [showCreateNs, setShowCreateNs] = useState(false);
  const [newNsName, setNewNsName] = useState("");

  // Tables
  const [selectedNs, setSelectedNs] = useState("");
  const [tables, setTables] = useState<{ namespace: string[]; name: string }[]>(
    []
  );

  // Detail
  const [selectedTable, setSelectedTable] = useState<Record<
    string,
    unknown
  > | null>(null);
  const [selectedTableName, setSelectedTableName] = useState("");

  const [loading, setLoading] = useState(true);

  // Load warehouses
  const loadWarehouses = () => {
    setLoading(true);
    fetch("/_admin/warehouses")
      .then((r) => r.json())
      .then((d) => setWarehouses(d.warehouses || []))
      .catch(() => setWarehouses([]))
      .finally(() => setLoading(false));
  };

  useEffect(loadWarehouses, []);

  const createWarehouse = async () => {
    if (!newWhName.trim()) return;
    await fetch("/_admin/warehouses", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: newWhName }),
    });
    setNewWhName("");
    setShowCreateWh(false);
    loadWarehouses();
  };

  const deleteWarehouse = async (name: string) => {
    if (!confirm(`Delete warehouse "${name}" and its backing bucket?`)) return;
    await fetch(`/_admin/warehouses/${name}`, { method: "DELETE" });
    loadWarehouses();
  };

  // Open warehouse → show namespaces
  const openWarehouse = (wh: WarehouseInfo) => {
    setSelectedWh(wh);
    setView("namespaces");
    setLoading(true);
    iceberg
      .listNamespaces(wh.name)
      .then((r) => setNamespaces(r.namespaces || []))
      .catch(() => setNamespaces([]))
      .finally(() => setLoading(false));
  };

  const whName = selectedWh?.name;

  const createNamespace = async () => {
    if (!newNsName.trim()) return;
    await iceberg.createNamespace([newNsName], {}, whName);
    setNewNsName("");
    setShowCreateNs(false);
    openWarehouse(selectedWh!);
  };

  const deleteNamespace = async (ns: string) => {
    if (!confirm(`Delete namespace "${ns}"?`)) return;
    await iceberg.deleteNamespace(ns, whName);
    openWarehouse(selectedWh!);
  };

  // Open namespace → show tables
  const openNamespace = async (ns: string) => {
    setSelectedNs(ns);
    setView("tables");
    const r = await iceberg.listTables(ns, whName);
    setTables(r.identifiers || []);
  };

  const deleteTable = async (ns: string, name: string) => {
    if (!confirm(`Delete table "${ns}.${name}"?`)) return;
    await iceberg.deleteTable(ns, name, whName);
    openNamespace(ns);
  };

  // Open table detail
  const openTable = async (ns: string, name: string) => {
    setSelectedTableName(`${ns}.${name}`);
    setView("detail");
    try {
      const r = await iceberg.getTable(ns, name, whName);
      // Strip vended credentials from display — they're transient, not metadata
      const { config: _config, ...tableMetadata } = r;
      setSelectedTable(tableMetadata);
    } catch (e) {
      setSelectedTable({ error: String(e) });
    }
  };

  const goBack = () => {
    if (view === "detail") {
      setView("tables");
      setSelectedTable(null);
    } else if (view === "tables") {
      setView("namespaces");
      setSelectedNs("");
    } else if (view === "namespaces") {
      setView("warehouses");
      setSelectedWh(null);
    }
  };

  // Breadcrumb
  const breadcrumb: { label: string; action?: () => void }[] = [
    { label: "Warehouses", action: () => setView("warehouses") },
  ];
  if (selectedWh) {
    breadcrumb.push({
      label: selectedWh.name,
      action: () => openWarehouse(selectedWh),
    });
  }
  if (selectedNs) {
    breadcrumb.push({
      label: selectedNs,
      action: () => openNamespace(selectedNs),
    });
  }
  if (view === "detail") {
    breadcrumb.push({ label: selectedTableName });
  }

  return (
    <div className="p-6">
      <PageHeader
        title="Tables"
        description="Manage warehouses, namespaces, and Iceberg tables"
      />

      {/* Breadcrumb */}
      {view !== "warehouses" && (
        <div className="flex items-center gap-1 text-[12px] mb-4">
          <button
            onClick={goBack}
            className="text-gray-400 hover:text-gray-600 p-0.5 mr-1"
          >
            <ArrowLeft size={14} />
          </button>
          {breadcrumb.map((b, i) => (
            <span key={i} className="flex items-center gap-1">
              {i > 0 && (
                <ChevronRight size={12} className="text-gray-300" />
              )}
              {b.action ? (
                <button
                  onClick={b.action}
                  className="text-gray-500 hover:text-blue-600"
                >
                  {b.label}
                </button>
              ) : (
                <span className="text-gray-900 font-medium">{b.label}</span>
              )}
            </span>
          ))}
        </div>
      )}

      {/* Warehouses View */}
      {view === "warehouses" && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">
              Warehouses
            </h2>
            <button
              onClick={() => setShowCreateWh(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Warehouse
            </button>
          </div>

          {showCreateWh && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4">
              <div className="flex gap-2">
                <input
                  value={newWhName}
                  onChange={(e) => setNewWhName(e.target.value)}
                  placeholder="warehouse-name"
                  className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  onKeyDown={(e) => e.key === "Enter" && createWarehouse()}
                  autoFocus
                />
                <button
                  onClick={createWarehouse}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Create
                </button>
                <button
                  onClick={() => setShowCreateWh(false)}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Warehouse
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Location
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Bucket
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <tr>
                    <td colSpan={4} className="px-4 py-8 text-center">
                      <div className="flex items-center justify-center gap-3">
                        <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                          <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
                        </div>
                        <span className="text-[12px] text-gray-400">
                          Loading
                        </span>
                      </div>
                    </td>
                  </tr>
                ) : warehouses.length === 0 ? (
                  <tr>
                    <td
                      colSpan={4}
                      className="px-4 py-8 text-center text-[12px] text-gray-400"
                    >
                      No warehouses. Create one to start managing Iceberg
                      tables.
                    </td>
                  </tr>
                ) : (
                  warehouses.map((wh) => (
                    <tr
                      key={wh.name}
                      className="hover:bg-gray-50 cursor-pointer group"
                      onClick={() => openWarehouse(wh)}
                    >
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <Warehouse size={14} className="text-indigo-500" />
                          <span className="text-[13px] font-medium">
                            {wh.name}
                          </span>
                          <ChevronRight
                            size={14}
                            className="ml-auto text-gray-300"
                          />
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500 font-mono">
                        {wh.location}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500 font-mono">
                        {wh.bucket}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            deleteWarehouse(wh.name);
                          }}
                          className="text-gray-400 hover:text-red-600 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <Trash2 size={13} />
                        </button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Namespaces View */}
      {view === "namespaces" && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">
              Namespaces in {selectedWh?.name}
            </h2>
            <button
              onClick={() => setShowCreateNs(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Namespace
            </button>
          </div>

          {showCreateNs && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4">
              <div className="flex gap-2">
                <input
                  value={newNsName}
                  onChange={(e) => setNewNsName(e.target.value)}
                  placeholder="namespace_name"
                  className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  onKeyDown={(e) => e.key === "Enter" && createNamespace()}
                  autoFocus
                />
                <button
                  onClick={createNamespace}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Create
                </button>
                <button
                  onClick={() => setShowCreateNs(false)}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Namespace
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <tr>
                    <td colSpan={2} className="px-4 py-8 text-center">
                      <div className="flex items-center justify-center gap-3">
                        <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                          <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
                        </div>
                        <span className="text-[12px] text-gray-400">
                          Loading
                        </span>
                      </div>
                    </td>
                  </tr>
                ) : namespaces.length === 0 ? (
                  <tr>
                    <td
                      colSpan={2}
                      className="px-4 py-8 text-center text-[12px] text-gray-400"
                    >
                      No namespaces
                    </td>
                  </tr>
                ) : (
                  namespaces.map((ns) => {
                    const name = ns.join(".");
                    return (
                      <tr
                        key={name}
                        className="hover:bg-gray-50 cursor-pointer group"
                        onClick={() => openNamespace(name)}
                      >
                        <td className="px-4 py-2.5">
                          <div className="flex items-center gap-2">
                            <FolderOpen
                              size={14}
                              className="text-yellow-500"
                            />
                            <span className="text-[13px] font-medium">
                              {name}
                            </span>
                            <ChevronRight
                              size={14}
                              className="ml-auto text-gray-300"
                            />
                          </div>
                        </td>
                        <td className="px-4 py-2.5 text-right">
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              deleteNamespace(name);
                            }}
                            className="text-gray-400 hover:text-red-600 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
                          >
                            <Trash2 size={13} />
                          </button>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Tables View */}
      {view === "tables" && (
        <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                  Table
                </th>
                <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {tables.length === 0 ? (
                <tr>
                  <td
                    colSpan={2}
                    className="px-4 py-8 text-center text-[12px] text-gray-400"
                  >
                    No tables in {selectedNs}
                  </td>
                </tr>
              ) : (
                tables.map((t) => (
                  <tr
                    key={t.name}
                    className="hover:bg-gray-50 cursor-pointer group"
                    onClick={() => openTable(selectedNs, t.name)}
                  >
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2">
                        <Table2 size={14} className="text-emerald-500" />
                        <span className="text-[13px] font-medium">
                          {t.name}
                        </span>
                        <ChevronRight
                          size={14}
                          className="ml-auto text-gray-300"
                        />
                      </div>
                    </td>
                    <td className="px-4 py-2.5 text-right">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          deleteTable(selectedNs, t.name);
                        }}
                        className="text-gray-400 hover:text-red-600 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
                      >
                        <Trash2 size={13} />
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Table Detail View */}
      {view === "detail" && (
        <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
          <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200">
            <h3 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">
              Table Metadata
            </h3>
          </div>
          <div className="p-4">
            {selectedTable ? (
              <pre className="bg-gray-50 rounded-lg p-3 text-[11px] overflow-auto max-h-[calc(100vh-280px)] border border-gray-200 font-mono text-gray-700">
                {JSON.stringify(selectedTable, null, 2)}
              </pre>
            ) : (
              <div className="flex items-center justify-center gap-3 py-8">
                <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                  <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
                </div>
                <span className="text-[12px] text-gray-400">Loading</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
