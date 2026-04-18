import { useEffect, useState } from "react";
import {
  Share2,
  Plus,
  Trash2,
  Table2,
  Copy,
  Check,
  ArrowLeft,
  Key,
  ChevronRight,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import { iceberg } from "../api/client";

interface Share {
  name: string;
  comment?: string;
}
interface ShareTable {
  share: string;
  schema: string;
  name: string;
  table_type?: string;
}
interface Recipient {
  name: string;
  shares: string[];
}

type View = "list" | "detail";

export default function DeltaSharing() {
  const [view, setView] = useState<View>("list");
  const [shares, setShares] = useState<Share[]>([]);
  const [recipients, setRecipients] = useState<Recipient[]>([]);
  const [loading, setLoading] = useState(true);

  // Selected share
  const [selectedShare, setSelectedShare] = useState("");
  const [shareTables, setShareTables] = useState<ShareTable[]>([]);

  // Create share
  const [showCreateShare, setShowCreateShare] = useState(false);
  const [newShareName, setNewShareName] = useState("");
  const [newShareComment, setNewShareComment] = useState("");

  // Add table
  const [showAddTable, setShowAddTable] = useState(false);
  const [tableType, setTableType] = useState("uniform");
  const [addSchema, setAddSchema] = useState("");
  const [addTableName, setAddTableName] = useState("");
  // For uniform: pick from existing iceberg tables
  const [icebergNamespaces, setIcebergNamespaces] = useState<string[]>([]);
  const [icebergTables, setIcebergTables] = useState<string[]>([]);
  const [selectedNs, setSelectedNs] = useState("");
  // For delta
  const [deltaBucket, setDeltaBucket] = useState("");
  const [deltaPath, setDeltaPath] = useState("");

  // Create recipient
  const [showCreateRecipient, setShowCreateRecipient] = useState(false);
  const [newRecipientName, setNewRecipientName] = useState("");
  const [selectedRecipientShares, setSelectedRecipientShares] = useState<
    Set<string>
  >(new Set());

  // Token display
  const [newToken, setNewToken] = useState("");
  const [copied, setCopied] = useState("");

  const load = () => {
    setLoading(true);
    Promise.all([
      fetch("/_admin/shares")
        .then((r) => r.json())
        .catch(() => ({ shares: [] })),
      fetch("/_admin/recipients")
        .then((r) => r.json())
        .catch(() => ({ recipients: [] })),
    ])
      .then(([s, r]) => {
        setShares(s.shares || []);
        setRecipients(r.recipients || []);
      })
      .finally(() => setLoading(false));
  };

  useEffect(load, []);

  const openShare = async (name: string) => {
    setSelectedShare(name);
    setView("detail");
    const r = await fetch(
      `/_admin/delta-sharing/shares/${name}/tables`
    )
      .then((r) => r.json())
      .catch(() => ({ tables: [] }));
    setShareTables(r.tables || []);
  };

  const createShare = async () => {
    if (!newShareName.trim()) return;
    await fetch("/_admin/shares", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: newShareName,
        comment: newShareComment,
      }),
    });
    setNewShareName("");
    setNewShareComment("");
    setShowCreateShare(false);
    load();
  };

  const deleteShare = async (name: string) => {
    if (!confirm(`Delete share "${name}"?`)) return;
    await fetch(`/_admin/delta-sharing/shares/${name}`, {
      method: "DELETE",
    });
    if (selectedShare === name) setView("list");
    load();
  };

  // Load iceberg namespaces for dropdown
  const loadIcebergNamespaces = async () => {
    const r = await iceberg.listNamespaces().catch(() => ({ namespaces: [] }));
    setIcebergNamespaces(
      (r.namespaces || []).map((ns: string[]) => ns.join("."))
    );
  };

  const loadIcebergTables = async (ns: string) => {
    setSelectedNs(ns);
    const r = await iceberg.listTables(ns).catch(() => ({ identifiers: [] }));
    setIcebergTables(
      (r.identifiers || []).map((t: { name: string }) => t.name)
    );
  };

  const startAddTable = () => {
    setShowAddTable(true);
    loadIcebergNamespaces();
  };

  const addTable = async () => {
    if (!addSchema || !addTableName) return;
    await fetch(
      `/_admin/delta-sharing/shares/${selectedShare}/tables`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          schema: addSchema,
          name: addTableName,
          table_type: tableType,
          warehouse: "",
          namespace: selectedNs,
          bucket: deltaBucket,
          path: deltaPath,
        }),
      }
    );
    setShowAddTable(false);
    setAddSchema("");
    setAddTableName("");
    setSelectedNs("");
    openShare(selectedShare);
  };

  const removeTable = async (schema: string, table: string) => {
    await fetch(
      `/_admin/delta-sharing/shares/${selectedShare}/schemas/${schema}/tables/${table}`,
      { method: "DELETE" }
    );
    openShare(selectedShare);
  };

  const toggleRecipientShare = (share: string) => {
    setSelectedRecipientShares((prev) => {
      const next = new Set(prev);
      if (next.has(share)) next.delete(share);
      else next.add(share);
      return next;
    });
  };

  const createRecipient = async () => {
    if (!newRecipientName.trim()) return;
    const r = await fetch("/_admin/delta-sharing/recipients", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: newRecipientName,
        shares: [...selectedRecipientShares],
      }),
    });
    const data = await r.json();
    setNewToken(data.token || data.raw_token || "");
    setNewRecipientName("");
    setSelectedRecipientShares(new Set());
    setShowCreateRecipient(false);
    load();
  };

  const deleteRecipient = async (name: string) => {
    if (!confirm(`Delete recipient "${name}"?`)) return;
    await fetch(`/_admin/delta-sharing/recipients/${name}`, {
      method: "DELETE",
    });
    load();
  };

  const copyText = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopied(id);
    setTimeout(() => setCopied(""), 2000);
  };

  const profileJson = newToken
    ? JSON.stringify(
        {
          shareCredentialsVersion: 1,
          endpoint: `${window.location.origin}/delta-sharing`,
          bearerToken: newToken,
        },
        null,
        2
      )
    : "";

  return (
    <div className="p-6">
      <PageHeader
        title="Table Sharing"
        description="Share Delta and Iceberg tables with external consumers"
      />

      {/* Token banner */}
      {newToken && (
        <div className="mb-4 bg-yellow-50 border border-yellow-200 rounded-xl p-4">
          <h3 className="text-[12px] font-medium text-yellow-800 mb-2">
            Save this token now — it will not be shown again
          </h3>
          <div className="flex items-center gap-2 mb-2">
            <code className="text-[11px] bg-yellow-100 px-2 py-1 rounded font-mono flex-1 truncate">
              {newToken}
            </code>
            <button onClick={() => copyText(newToken, "token")} className="p-1">
              {copied === "token" ? (
                <Check size={14} className="text-green-500" />
              ) : (
                <Copy size={14} className="text-gray-400" />
              )}
            </button>
          </div>
          <details className="text-[11px] text-yellow-700">
            <summary className="cursor-pointer font-medium">
              profile.share (save as file)
            </summary>
            <pre className="mt-1 bg-yellow-100 p-2 rounded text-[10px] font-mono overflow-auto">
              {profileJson}
            </pre>
          </details>
          <button
            onClick={() => setNewToken("")}
            className="mt-2 text-[11px] text-yellow-700 underline"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* Back button when in detail view */}
      {view === "detail" && (
        <button
          onClick={() => setView("list")}
          className="flex items-center gap-1 text-[12px] text-gray-500 hover:text-gray-700 mb-3"
        >
          <ArrowLeft size={14} /> Back to shares
        </button>
      )}

      {/* ===== LIST VIEW ===== */}
      {view === "list" && (
        <div className="space-y-4">
          {/* Shares table */}
          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
              <h3 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Shares
              </h3>
              <button
                onClick={() => setShowCreateShare(true)}
                className="flex items-center gap-1 px-2 py-1 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
              >
                <Plus size={12} /> New Share
              </button>
            </div>

            {showCreateShare && (
              <div className="p-3 border-b border-gray-100">
                <div className="flex gap-2 mb-1.5">
                  <input
                    value={newShareName}
                    onChange={(e) => setNewShareName(e.target.value)}
                    placeholder="share-name"
                    className="flex-1 px-2 py-1 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    onKeyDown={(e) => e.key === "Enter" && createShare()}
                    autoFocus
                  />
                  <input
                    value={newShareComment}
                    onChange={(e) => setNewShareComment(e.target.value)}
                    placeholder="Description (optional)"
                    className="flex-1 px-2 py-1 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                </div>
                <div className="flex gap-1.5">
                  <button
                    onClick={createShare}
                    className="px-2 py-1 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
                  >
                    Create
                  </button>
                  <button
                    onClick={() => setShowCreateShare(false)}
                    className="px-2 py-1 text-gray-500 text-[11px]"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-100">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Share
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {loading ? (
                  <tr>
                    <td colSpan={2} className="px-4 py-6 text-center">
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
                ) : shares.length === 0 ? (
                  <tr>
                    <td
                      colSpan={2}
                      className="px-4 py-6 text-center text-[12px] text-gray-400"
                    >
                      No shares. Create one to start sharing tables.
                    </td>
                  </tr>
                ) : (
                  shares.map((s) => (
                    <tr
                      key={s.name}
                      className="hover:bg-gray-50 cursor-pointer group"
                      onClick={() => openShare(s.name)}
                    >
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <Share2 size={13} className="text-blue-500" />
                          <span className="text-[13px] font-medium">
                            {s.name}
                          </span>
                          {s.comment && (
                            <span className="text-[11px] text-gray-400">
                              {s.comment}
                            </span>
                          )}
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
                            deleteShare(s.name);
                          }}
                          className="text-gray-300 hover:text-red-500 p-0.5 opacity-0 group-hover:opacity-100 transition-opacity"
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

          {/* Recipients table */}
          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
              <h3 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                Recipients
              </h3>
              <button
                onClick={() => setShowCreateRecipient(true)}
                className="flex items-center gap-1 px-2 py-1 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
              >
                <Plus size={12} /> New Recipient
              </button>
            </div>

            {showCreateRecipient && (
              <div className="p-3 border-b border-gray-100 space-y-2">
                <input
                  value={newRecipientName}
                  onChange={(e) => setNewRecipientName(e.target.value)}
                  placeholder="Recipient name"
                  className="w-full px-2 py-1 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  autoFocus
                />
                <div>
                  <label className="block text-[11px] text-gray-500 mb-1">
                    Grant access to shares:
                  </label>
                  <div className="flex flex-wrap gap-1.5">
                    {shares.map((s) => (
                      <button
                        key={s.name}
                        onClick={() => toggleRecipientShare(s.name)}
                        className={`px-2 py-0.5 rounded text-[11px] border ${
                          selectedRecipientShares.has(s.name)
                            ? "bg-blue-50 border-blue-300 text-blue-700"
                            : "bg-white border-gray-200 text-gray-500 hover:border-gray-300"
                        }`}
                      >
                        {s.name}
                      </button>
                    ))}
                  </div>
                </div>
                <div className="flex gap-1.5">
                  <button
                    onClick={createRecipient}
                    className="px-2 py-1 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
                  >
                    Create
                  </button>
                  <button
                    onClick={() => setShowCreateRecipient(false)}
                    className="px-2 py-1 text-gray-500 text-[11px]"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            <table className="w-full">
              <tbody className="divide-y divide-gray-50">
                {recipients.length === 0 ? (
                  <tr>
                    <td className="px-4 py-6 text-center text-[12px] text-gray-400">
                      No recipients
                    </td>
                  </tr>
                ) : (
                  recipients.map((r) => (
                    <tr
                      key={r.name}
                      className="hover:bg-gray-50 group"
                    >
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <Key size={13} className="text-purple-500" />
                          <span className="text-[12px] font-medium">
                            {r.name}
                          </span>
                          <div className="flex gap-1 ml-2">
                            {r.shares?.map((s) => (
                              <span
                                key={s}
                                className="text-[10px] px-1 py-0.5 bg-blue-50 text-blue-600 rounded"
                              >
                                {s}
                              </span>
                            ))}
                          </div>
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={() => deleteRecipient(r.name)}
                          className="text-gray-300 hover:text-red-500 p-0.5 opacity-0 group-hover:opacity-100 transition-opacity"
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
        </div>
      )}

      {/* ===== DETAIL VIEW ===== */}
      {view === "detail" && (
        <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
          <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
            <h3 className="text-[13px] font-medium text-gray-700">
              Tables in <span className="text-blue-600">{selectedShare}</span>
            </h3>
            <button
              onClick={startAddTable}
              className="flex items-center gap-1 px-2 py-1 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
            >
              <Plus size={12} /> Add Table
            </button>
          </div>

          {showAddTable && (
            <div className="p-4 border-b border-gray-100 space-y-3">
              <div className="grid grid-cols-3 gap-2">
                <div>
                  <label className="block text-[11px] text-gray-500 mb-1">
                    Type
                  </label>
                  <select
                    value={tableType}
                    onChange={(e) => setTableType(e.target.value)}
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    <option value="uniform">Iceberg (UniForm)</option>
                    <option value="delta">Delta Lake</option>
                  </select>
                </div>
                <div>
                  <label className="block text-[11px] text-gray-500 mb-1">
                    Schema
                  </label>
                  <input
                    value={addSchema}
                    onChange={(e) => setAddSchema(e.target.value)}
                    placeholder="e.g. HR"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-[11px] text-gray-500 mb-1">
                    Table Name
                  </label>
                  {tableType === "uniform" ? (
                    <select
                      value={addTableName}
                      onChange={(e) => setAddTableName(e.target.value)}
                      className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    >
                      <option value="">Select table...</option>
                      {icebergTables.map((t) => (
                        <option key={t} value={t}>
                          {t}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <input
                      value={addTableName}
                      onChange={(e) => setAddTableName(e.target.value)}
                      placeholder="Table name"
                      className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    />
                  )}
                </div>
              </div>

              {tableType === "uniform" && (
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <label className="block text-[11px] text-gray-500 mb-1">
                      Iceberg Namespace
                    </label>
                    <select
                      value={selectedNs}
                      onChange={(e) => {
                        loadIcebergTables(e.target.value);
                      }}
                      className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    >
                      <option value="">Select namespace...</option>
                      {icebergNamespaces.map((ns) => (
                        <option key={ns} value={ns}>
                          {ns}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="flex items-end">
                    <p className="text-[11px] text-gray-400 pb-1.5">
                      Select namespace to browse existing Iceberg tables
                    </p>
                  </div>
                </div>
              )}

              {tableType === "delta" && (
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <label className="block text-[11px] text-gray-500 mb-1">
                      S3 Bucket
                    </label>
                    <input
                      value={deltaBucket}
                      onChange={(e) => setDeltaBucket(e.target.value)}
                      placeholder="analytics"
                      className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-[11px] text-gray-500 mb-1">
                      Path
                    </label>
                    <input
                      value={deltaPath}
                      onChange={(e) => setDeltaPath(e.target.value)}
                      placeholder="employees/"
                      className="w-full px-2 py-1.5 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    />
                  </div>
                </div>
              )}

              <div className="flex gap-1.5">
                <button
                  onClick={addTable}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded text-[11px] hover:bg-gray-800"
                >
                  Add Table
                </button>
                <button
                  onClick={() => setShowAddTable(false)}
                  className="px-3 py-1.5 text-gray-500 text-[11px]"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                  Table
                </th>
                <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {shareTables.length === 0 ? (
                <tr>
                  <td
                    colSpan={3}
                    className="px-4 py-6 text-center text-[12px] text-gray-400"
                  >
                    No tables in this share. Add one to start sharing.
                  </td>
                </tr>
              ) : (
                shareTables.map((t) => (
                  <tr
                    key={`${t.schema}.${t.name}`}
                    className="hover:bg-gray-50 group"
                  >
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2">
                        <Table2 size={13} className="text-emerald-500" />
                        <span className="text-[13px] font-medium">
                          {t.schema}.{t.name}
                        </span>
                      </div>
                    </td>
                    <td className="px-4 py-2.5">
                      <span
                        className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                          t.table_type === "delta"
                            ? "bg-blue-100 text-blue-700"
                            : "bg-green-100 text-green-700"
                        }`}
                      >
                        {t.table_type || "iceberg"}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-right">
                      <button
                        onClick={() => removeTable(t.schema, t.name)}
                        className="text-gray-300 hover:text-red-500 p-0.5 opacity-0 group-hover:opacity-100 transition-opacity"
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
    </div>
  );
}
