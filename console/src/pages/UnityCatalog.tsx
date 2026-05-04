import { useEffect, useState } from "react";
import {
  Database,
  FolderOpen,
  Table2,
  Plus,
  Trash2,
  ChevronRight,
  ArrowLeft,
  X,
  Sigma,
  HardDrive,
  Brain,
  Shield,
  Filter,
} from "lucide-react";
import PageHeader from "../components/PageHeader";
import PolicyEditor from "../components/PolicyEditor";
import TableSecurityEditor from "../components/TableSecurityEditor";
import {
  unity,
  type UnityCatalogInfo,
  type UnitySchemaInfo,
  type UnityTableInfo,
  type UnityColumnInfo,
  type UnityFunctionInfo,
  type UnityVolumeInfo,
  type UnityModelInfo,
} from "../api/client";

type View = "catalogs" | "schemas" | "tables" | "detail";
type EntityTab = "tables" | "functions" | "volumes" | "models";

interface NewColumnRow {
  name: string;
  type_text: string;
  nullable: boolean;
}

const DEFAULT_COLUMN: NewColumnRow = {
  name: "",
  type_text: "string",
  nullable: true,
};

export default function UnityCatalog() {
  const [view, setView] = useState<View>("catalogs");

  // Catalogs
  const [catalogs, setCatalogs] = useState<UnityCatalogInfo[]>([]);
  const [showCreateCat, setShowCreateCat] = useState(false);
  const [newCatName, setNewCatName] = useState("");
  const [newCatComment, setNewCatComment] = useState("");

  // Schemas
  const [selectedCat, setSelectedCat] = useState<UnityCatalogInfo | null>(null);
  const [schemas, setSchemas] = useState<UnitySchemaInfo[]>([]);
  const [showCreateSchema, setShowCreateSchema] = useState(false);
  const [newSchemaName, setNewSchemaName] = useState("");
  const [newSchemaComment, setNewSchemaComment] = useState("");

  // Tables
  const [selectedSchema, setSelectedSchema] = useState<UnitySchemaInfo | null>(
    null,
  );
  const [tables, setTables] = useState<UnityTableInfo[]>([]);
  const [showCreateTable, setShowCreateTable] = useState(false);
  const [newTableName, setNewTableName] = useState("");
  const [newTableType, setNewTableType] = useState("MANAGED");
  const [newTableFormat, setNewTableFormat] = useState("DELTA");
  const [newTableLocation, setNewTableLocation] = useState("");
  const [newTableColumns, setNewTableColumns] = useState<NewColumnRow[]>([
    { ...DEFAULT_COLUMN },
  ]);

  // Detail
  const [selectedTable, setSelectedTable] = useState<UnityTableInfo | null>(null);

  // Entity tabs (functions / volumes / models live alongside tables under
  // a schema — same parent, different child types).
  const [entityTab, setEntityTab] = useState<EntityTab>("tables");
  const [functions, setFunctions] = useState<UnityFunctionInfo[]>([]);
  const [showCreateFunction, setShowCreateFunction] = useState(false);
  const [newFnName, setNewFnName] = useState("");
  // routine_body is the dispatch flag per Databricks spec: "SQL" | "EXTERNAL".
  // For EXTERNAL, external_language carries the runtime (e.g. "PYTHON").
  const [newFnRoutineBody, setNewFnRoutineBody] = useState("SQL");
  const [newFnExternalLanguage, setNewFnExternalLanguage] = useState("PYTHON");
  const [newFnDataType, setNewFnDataType] = useState("STRING");
  const [newFnDefinition, setNewFnDefinition] = useState("");
  const [newFnComment, setNewFnComment] = useState("");

  const [volumes, setVolumes] = useState<UnityVolumeInfo[]>([]);
  const [showCreateVolume, setShowCreateVolume] = useState(false);
  const [newVolName, setNewVolName] = useState("");
  const [newVolType, setNewVolType] = useState("MANAGED");
  const [newVolLocation, setNewVolLocation] = useState("");
  const [newVolComment, setNewVolComment] = useState("");

  const [models, setModels] = useState<UnityModelInfo[]>([]);
  const [showCreateModel, setShowCreateModel] = useState(false);
  const [newModelName, setNewModelName] = useState("");
  const [newModelComment, setNewModelComment] = useState("");

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Active entity-policy editor target. Null = closed; otherwise carries the
  // scope (which level + identifier) so the modal knows which API to call.
  // Tuple variants: ["catalog", name] | ["schema", cat, schema] | ["table", cat, schema, table].
  const [policyTarget, setPolicyTarget] = useState<
    | { kind: "catalog"; name: string }
    | { kind: "schema"; catalog: string; schema: string }
    | { kind: "table"; catalog: string; schema: string; table: string }
    | null
  >(null);

  // Active row-filter / column-mask editor target. Independent from
  // policyTarget so the user can keep one open without losing the other.
  const [securityTarget, setSecurityTarget] = useState<{
    catalog: string;
    schema: string;
    table: string;
  } | null>(null);

  // -------- Catalogs --------
  const loadCatalogs = () => {
    setLoading(true);
    setError(null);
    unity
      .listCatalogs()
      .then((d) => setCatalogs(d.catalogs || []))
      .catch((e: Error) => {
        setError(e.message);
        setCatalogs([]);
      })
      .finally(() => setLoading(false));
  };

  useEffect(loadCatalogs, []);

  const createCatalog = async () => {
    if (!newCatName.trim()) return;
    try {
      await unity.createCatalog(newCatName.trim(), newCatComment.trim());
      setNewCatName("");
      setNewCatComment("");
      setShowCreateCat(false);
      loadCatalogs();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const deleteCatalog = async (name: string) => {
    if (
      !confirm(
        `Delete catalog "${name}"?\n\nThis force-deletes all schemas and tables inside it, and removes the backing bucket "unity-${name}".`,
      )
    )
      return;
    try {
      await unity.deleteCatalog(name, true);
      loadCatalogs();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const openCatalog = (cat: UnityCatalogInfo) => {
    setSelectedCat(cat);
    setView("schemas");
    setLoading(true);
    setError(null);
    unity
      .listSchemas(cat.name)
      .then((r) => setSchemas(r.schemas || []))
      .catch((e: Error) => {
        setError(e.message);
        setSchemas([]);
      })
      .finally(() => setLoading(false));
  };

  // -------- Schemas --------
  const createSchema = async () => {
    if (!selectedCat || !newSchemaName.trim()) return;
    try {
      await unity.createSchema(
        selectedCat.name,
        newSchemaName.trim(),
        newSchemaComment.trim(),
      );
      setNewSchemaName("");
      setNewSchemaComment("");
      setShowCreateSchema(false);
      openCatalog(selectedCat);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const deleteSchema = async (schema: string) => {
    if (!selectedCat) return;
    if (!confirm(`Delete schema "${selectedCat.name}.${schema}"?`)) return;
    try {
      await unity.deleteSchema(selectedCat.name, schema);
      openCatalog(selectedCat);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const openSchema = async (schema: UnitySchemaInfo) => {
    if (!selectedCat) return;
    setSelectedSchema(schema);
    setView("tables");
    setLoading(true);
    setError(null);
    // Load all four entity lists in parallel — each tab is cheap to render
    // and most schemas have few of each type, so up-front is fine. A single
    // failure surfaces but doesn't blank the other lists.
    const cat = selectedCat.name;
    const [tRes, fRes, vRes, mRes] = await Promise.allSettled([
      unity.listTables(cat, schema.name),
      unity.listFunctions(cat, schema.name),
      unity.listVolumes(cat, schema.name),
      unity.listModels(cat, schema.name),
    ]);
    setTables(tRes.status === "fulfilled" ? tRes.value.tables || [] : []);
    setFunctions(
      fRes.status === "fulfilled" ? fRes.value.functions || [] : [],
    );
    setVolumes(vRes.status === "fulfilled" ? vRes.value.volumes || [] : []);
    setModels(
      mRes.status === "fulfilled" ? mRes.value.registered_models || [] : [],
    );
    const firstErr = [tRes, fRes, vRes, mRes].find(
      (r) => r.status === "rejected",
    );
    if (firstErr && firstErr.status === "rejected") {
      setError((firstErr.reason as Error).message);
    }
    setLoading(false);
  };

  // ---- Functions ----
  const reloadEntities = () => {
    if (selectedSchema) openSchema(selectedSchema);
  };

  const createFunction = async () => {
    if (
      !selectedCat ||
      !selectedSchema ||
      !newFnName.trim() ||
      !newFnDefinition.trim()
    )
      return;
    try {
      await unity.createFunction({
        name: newFnName.trim(),
        catalog_name: selectedCat.name,
        schema_name: selectedSchema.name,
        routine_definition: newFnDefinition,
        routine_body: newFnRoutineBody,
        external_language:
          newFnRoutineBody === "EXTERNAL" ? newFnExternalLanguage : undefined,
        data_type: newFnDataType,
        comment: newFnComment.trim() || undefined,
      });
      setNewFnName("");
      setNewFnDefinition("");
      setNewFnComment("");
      setShowCreateFunction(false);
      reloadEntities();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const deleteFunction = async (name: string) => {
    if (!selectedCat || !selectedSchema) return;
    if (
      !confirm(
        `Delete function "${selectedCat.name}.${selectedSchema.name}.${name}"?`,
      )
    )
      return;
    try {
      await unity.deleteFunction(selectedCat.name, selectedSchema.name, name);
      reloadEntities();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  // ---- Volumes ----
  const createVolume = async () => {
    if (!selectedCat || !selectedSchema || !newVolName.trim()) return;
    try {
      await unity.createVolume({
        name: newVolName.trim(),
        catalog_name: selectedCat.name,
        schema_name: selectedSchema.name,
        volume_type: newVolType,
        storage_location:
          newVolType === "EXTERNAL" ? newVolLocation.trim() : undefined,
        comment: newVolComment.trim() || undefined,
      });
      setNewVolName("");
      setNewVolLocation("");
      setNewVolComment("");
      setShowCreateVolume(false);
      reloadEntities();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const deleteVolume = async (name: string) => {
    if (!selectedCat || !selectedSchema) return;
    if (
      !confirm(
        `Delete volume "${selectedCat.name}.${selectedSchema.name}.${name}"?`,
      )
    )
      return;
    try {
      await unity.deleteVolume(selectedCat.name, selectedSchema.name, name);
      reloadEntities();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  // ---- Models ----
  const createModel = async () => {
    if (!selectedCat || !selectedSchema || !newModelName.trim()) return;
    try {
      await unity.createModel({
        name: newModelName.trim(),
        catalog_name: selectedCat.name,
        schema_name: selectedSchema.name,
        comment: newModelComment.trim() || undefined,
      });
      setNewModelName("");
      setNewModelComment("");
      setShowCreateModel(false);
      reloadEntities();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const deleteModel = async (name: string) => {
    if (!selectedCat || !selectedSchema) return;
    if (
      !confirm(
        `Delete model "${selectedCat.name}.${selectedSchema.name}.${name}"?\n\nAll versions of this model will be deleted.`,
      )
    )
      return;
    try {
      await unity.deleteModel(selectedCat.name, selectedSchema.name, name);
      reloadEntities();
    } catch (e) {
      setError((e as Error).message);
    }
  };

  // -------- Tables --------
  const resetTableForm = () => {
    setNewTableName("");
    setNewTableType("MANAGED");
    setNewTableFormat("DELTA");
    setNewTableLocation("");
    setNewTableColumns([{ ...DEFAULT_COLUMN }]);
    setShowCreateTable(false);
  };

  const createTable = async () => {
    if (!selectedCat || !selectedSchema || !newTableName.trim()) return;
    const columns: UnityColumnInfo[] = newTableColumns
      .filter((c) => c.name.trim())
      .map((c, i) => ({
        name: c.name.trim(),
        type_text: c.type_text.trim() || "string",
        type_name: (c.type_text.trim() || "string").toUpperCase(),
        position: i,
        nullable: c.nullable,
      }));
    try {
      await unity.createTable({
        name: newTableName.trim(),
        catalog_name: selectedCat.name,
        schema_name: selectedSchema.name,
        table_type: newTableType,
        data_source_format: newTableFormat,
        storage_location:
          newTableType === "EXTERNAL" ? newTableLocation.trim() : undefined,
        columns,
      });
      resetTableForm();
      openSchema(selectedSchema);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const deleteTable = async (name: string) => {
    if (!selectedCat || !selectedSchema) return;
    if (
      !confirm(
        `Delete table "${selectedCat.name}.${selectedSchema.name}.${name}"?`,
      )
    )
      return;
    try {
      await unity.deleteTable(selectedCat.name, selectedSchema.name, name);
      openSchema(selectedSchema);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const openTable = async (name: string) => {
    if (!selectedCat || !selectedSchema) return;
    setView("detail");
    setSelectedTable(null);
    setError(null);
    try {
      const t = await unity.getTable(selectedCat.name, selectedSchema.name, name);
      setSelectedTable(t);
    } catch (e) {
      setError((e as Error).message);
    }
  };

  // -------- Navigation --------
  const goBack = () => {
    if (view === "detail") {
      setView("tables");
      setSelectedTable(null);
    } else if (view === "tables") {
      setView("schemas");
      setSelectedSchema(null);
      setTables([]);
    } else if (view === "schemas") {
      setView("catalogs");
      setSelectedCat(null);
      setSchemas([]);
    }
  };

  const breadcrumb: { label: string; action?: () => void }[] = [
    { label: "Catalogs", action: () => setView("catalogs") },
  ];
  if (selectedCat) {
    breadcrumb.push({
      label: selectedCat.name,
      action: () => openCatalog(selectedCat),
    });
  }
  if (selectedSchema) {
    breadcrumb.push({
      label: selectedSchema.name,
      action: () => openSchema(selectedSchema),
    });
  }
  if (view === "detail" && selectedTable) {
    breadcrumb.push({ label: selectedTable.name });
  }

  return (
    <div className="p-6">
      <PageHeader
        title="Unity Catalog"
        description="Databricks-compatible three-level namespace (catalog.schema.table)"
      />

      {/* Breadcrumb */}
      {view !== "catalogs" && (
        <div className="flex items-center gap-1 text-[12px] mb-4">
          <button
            onClick={goBack}
            className="text-gray-400 hover:text-gray-600 p-0.5 mr-1"
          >
            <ArrowLeft size={14} />
          </button>
          {breadcrumb.map((b, i) => (
            <span key={i} className="flex items-center gap-1">
              {i > 0 && <ChevronRight size={12} className="text-gray-300" />}
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

      {error && (
        <div className="mb-3 px-3 py-2 bg-red-50 border border-red-200 rounded-lg text-[12px] text-red-700 flex items-start justify-between gap-2">
          <span className="font-mono">{error}</span>
          <button
            onClick={() => setError(null)}
            className="text-red-400 hover:text-red-700"
          >
            <X size={12} />
          </button>
        </div>
      )}

      {/* ================= Catalogs ================= */}
      {view === "catalogs" && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">Catalogs</h2>
            <button
              onClick={() => setShowCreateCat(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Catalog
            </button>
          </div>

          {showCreateCat && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4 space-y-2">
              <input
                value={newCatName}
                onChange={(e) => setNewCatName(e.target.value)}
                placeholder="catalog-name"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                autoFocus
              />
              <input
                value={newCatComment}
                onChange={(e) => setNewCatComment(e.target.value)}
                placeholder="comment (optional)"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <div className="flex gap-2 justify-end">
                <button
                  onClick={() => {
                    setShowCreateCat(false);
                    setNewCatName("");
                    setNewCatComment("");
                  }}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={createCatalog}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Create
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Catalog
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Comment
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-40">
                    Created
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <LoadingRow cols={4} />
                ) : catalogs.length === 0 ? (
                  <EmptyRow cols={4} text="No catalogs. Create one to start." />
                ) : (
                  catalogs.map((cat) => (
                    <tr
                      key={cat.name}
                      className="hover:bg-gray-50 cursor-pointer group"
                      onClick={() => openCatalog(cat)}
                    >
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <Database size={14} className="text-indigo-500" />
                          <span className="text-[13px] font-medium">
                            {cat.name}
                          </span>
                          <ChevronRight
                            size={14}
                            className="ml-auto text-gray-300"
                          />
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {cat.comment || "—"}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {fmtTs(cat.created_at)}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setPolicyTarget({ kind: "catalog", name: cat.name });
                          }}
                          className="text-gray-400 hover:text-indigo-600 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
                          title="Edit catalog policy"
                        >
                          <Shield size={13} />
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            deleteCatalog(cat.name);
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

      {/* ================= Schemas ================= */}
      {view === "schemas" && selectedCat && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">
              Schemas in {selectedCat.name}
            </h2>
            <button
              onClick={() => setShowCreateSchema(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Schema
            </button>
          </div>

          {showCreateSchema && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4 space-y-2">
              <input
                value={newSchemaName}
                onChange={(e) => setNewSchemaName(e.target.value)}
                placeholder="schema_name"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                autoFocus
              />
              <input
                value={newSchemaComment}
                onChange={(e) => setNewSchemaComment(e.target.value)}
                placeholder="comment (optional)"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <div className="flex gap-2 justify-end">
                <button
                  onClick={() => {
                    setShowCreateSchema(false);
                    setNewSchemaName("");
                    setNewSchemaComment("");
                  }}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={createSchema}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Create
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Schema
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Comment
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <LoadingRow cols={3} />
                ) : schemas.length === 0 ? (
                  <EmptyRow cols={3} text="No schemas in this catalog." />
                ) : (
                  schemas.map((s) => (
                    <tr
                      key={s.name}
                      className="hover:bg-gray-50 cursor-pointer group"
                      onClick={() => openSchema(s)}
                    >
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <FolderOpen size={14} className="text-yellow-500" />
                          <span className="text-[13px] font-medium">
                            {s.name}
                          </span>
                          <ChevronRight
                            size={14}
                            className="ml-auto text-gray-300"
                          />
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {s.comment || "—"}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setPolicyTarget({
                              kind: "schema",
                              catalog: selectedCat!.name,
                              schema: s.name,
                            });
                          }}
                          className="text-gray-400 hover:text-indigo-600 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
                          title="Edit schema policy"
                        >
                          <Shield size={13} />
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            deleteSchema(s.name);
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

      {/* ================= Schema-level entities (tabbed) ================= */}
      {view === "tables" && selectedCat && selectedSchema && (
        <>
          {/* Tab bar — Tables / Functions / Volumes / Models */}
          <div className="flex border-b border-gray-200 mb-4">
            {(
              [
                { key: "tables", label: "Tables", icon: Table2, count: tables.length },
                {
                  key: "functions",
                  label: "Functions",
                  icon: Sigma,
                  count: functions.length,
                },
                {
                  key: "volumes",
                  label: "Volumes",
                  icon: HardDrive,
                  count: volumes.length,
                },
                { key: "models", label: "Models", icon: Brain, count: models.length },
              ] as const
            ).map((t) => {
              const Icon = t.icon;
              const active = entityTab === t.key;
              return (
                <button
                  key={t.key}
                  onClick={() => setEntityTab(t.key)}
                  className={`flex items-center gap-1.5 px-3 py-2 text-[12px] font-medium border-b-2 -mb-px transition-colors ${
                    active
                      ? "border-blue-600 text-blue-600"
                      : "border-transparent text-gray-500 hover:text-gray-900"
                  }`}
                >
                  <Icon size={14} />
                  {t.label}
                  <span className="text-gray-400 font-mono">({t.count})</span>
                </button>
              );
            })}
          </div>
        </>
      )}

      {/* ================= Tables ================= */}
      {view === "tables" &&
        entityTab === "tables" &&
        selectedCat &&
        selectedSchema && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">
              Tables in {selectedCat.name}.{selectedSchema.name}
            </h2>
            <button
              onClick={() => setShowCreateTable(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Table
            </button>
          </div>

          {showCreateTable && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4 space-y-3">
              <div className="grid grid-cols-3 gap-2">
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Table name
                  </label>
                  <input
                    value={newTableName}
                    onChange={(e) => setNewTableName(e.target.value)}
                    placeholder="events"
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                    autoFocus
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Type
                  </label>
                  <select
                    value={newTableType}
                    onChange={(e) => setNewTableType(e.target.value)}
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    <option value="MANAGED">MANAGED</option>
                    <option value="EXTERNAL">EXTERNAL</option>
                  </select>
                </div>
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Format
                  </label>
                  <select
                    value={newTableFormat}
                    onChange={(e) => setNewTableFormat(e.target.value)}
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    <option value="DELTA">DELTA</option>
                    <option value="ICEBERG">ICEBERG</option>
                    <option value="PARQUET">PARQUET</option>
                    <option value="CSV">CSV</option>
                    <option value="JSON">JSON</option>
                  </select>
                </div>
              </div>

              {newTableType === "EXTERNAL" && (
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Storage location (s3://bucket/path/)
                  </label>
                  <input
                    value={newTableLocation}
                    onChange={(e) => setNewTableLocation(e.target.value)}
                    placeholder="s3://my-bucket/external/events/"
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                  />
                </div>
              )}

              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <label className="text-[10px] font-medium text-gray-500 uppercase tracking-wider">
                    Columns
                  </label>
                  <button
                    onClick={() =>
                      setNewTableColumns([
                        ...newTableColumns,
                        { ...DEFAULT_COLUMN },
                      ])
                    }
                    className="text-[11px] text-blue-600 hover:text-blue-800"
                  >
                    + add column
                  </button>
                </div>
                <div className="space-y-1.5">
                  {newTableColumns.map((c, i) => (
                    <div key={i} className="flex gap-2 items-center">
                      <input
                        value={c.name}
                        onChange={(e) =>
                          updateColumn(newTableColumns, setNewTableColumns, i, {
                            name: e.target.value,
                          })
                        }
                        placeholder="column_name"
                        className="flex-1 px-2 py-1 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                      />
                      <input
                        value={c.type_text}
                        onChange={(e) =>
                          updateColumn(newTableColumns, setNewTableColumns, i, {
                            type_text: e.target.value,
                          })
                        }
                        placeholder="string"
                        className="w-32 px-2 py-1 border border-gray-300 rounded text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                      />
                      <label className="flex items-center gap-1 text-[11px] text-gray-600">
                        <input
                          type="checkbox"
                          checked={c.nullable}
                          onChange={(e) =>
                            updateColumn(
                              newTableColumns,
                              setNewTableColumns,
                              i,
                              { nullable: e.target.checked },
                            )
                          }
                        />
                        nullable
                      </label>
                      <button
                        onClick={() =>
                          setNewTableColumns(
                            newTableColumns.filter((_, j) => j !== i),
                          )
                        }
                        className="text-gray-400 hover:text-red-600 p-1"
                        disabled={newTableColumns.length === 1}
                      >
                        <X size={12} />
                      </button>
                    </div>
                  ))}
                </div>
              </div>

              <div className="flex gap-2 justify-end">
                <button
                  onClick={resetTableForm}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={createTable}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Create
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Table
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-24">
                    Type
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-24">
                    Format
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Location
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <LoadingRow cols={5} />
                ) : tables.length === 0 ? (
                  <EmptyRow cols={5} text="No tables in this schema." />
                ) : (
                  tables.map((t) => (
                    <tr
                      key={t.name}
                      className="hover:bg-gray-50 cursor-pointer group"
                      onClick={() => openTable(t.name)}
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
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {t.table_type}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {t.data_source_format}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500 font-mono truncate max-w-[280px]">
                        {t.storage_location}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setSecurityTarget({
                              catalog: selectedCat!.name,
                              schema: selectedSchema!.name,
                              table: t.name,
                            });
                          }}
                          className={`p-1 transition-opacity ${
                            t.row_filter ||
                            (t.column_masks &&
                              Object.keys(t.column_masks).length > 0)
                              ? "text-emerald-600 hover:text-emerald-700 opacity-100"
                              : "text-gray-400 hover:text-emerald-600 opacity-0 group-hover:opacity-100"
                          }`}
                          title={
                            t.row_filter ||
                            (t.column_masks &&
                              Object.keys(t.column_masks).length > 0)
                              ? "Edit row filter / column masks (active)"
                              : "Bind row filter / column masks"
                          }
                        >
                          <Filter size={13} />
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setPolicyTarget({
                              kind: "table",
                              catalog: selectedCat!.name,
                              schema: selectedSchema!.name,
                              table: t.name,
                            });
                          }}
                          className="text-gray-400 hover:text-indigo-600 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
                          title="Edit table policy"
                        >
                          <Shield size={13} />
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            deleteTable(t.name);
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

      {/* ================= Functions ================= */}
      {view === "tables" &&
        entityTab === "functions" &&
        selectedCat &&
        selectedSchema && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">
              Functions in {selectedCat.name}.{selectedSchema.name}
            </h2>
            <button
              onClick={() => setShowCreateFunction(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Function
            </button>
          </div>

          {showCreateFunction && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4 space-y-3">
              <div className="grid grid-cols-3 gap-2">
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Function name
                  </label>
                  <input
                    value={newFnName}
                    onChange={(e) => setNewFnName(e.target.value)}
                    placeholder="lower"
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                    autoFocus
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Routine body
                  </label>
                  <select
                    value={newFnRoutineBody}
                    onChange={(e) => setNewFnRoutineBody(e.target.value)}
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    <option value="SQL">SQL</option>
                    <option value="EXTERNAL">EXTERNAL</option>
                  </select>
                </div>
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Return type
                  </label>
                  <input
                    value={newFnDataType}
                    onChange={(e) => setNewFnDataType(e.target.value)}
                    placeholder="STRING"
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                  />
                </div>
              </div>
              {newFnRoutineBody === "EXTERNAL" && (
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    External language
                  </label>
                  <select
                    value={newFnExternalLanguage}
                    onChange={(e) => setNewFnExternalLanguage(e.target.value)}
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    <option value="PYTHON">PYTHON</option>
                    <option value="SCALA">SCALA</option>
                    <option value="JAVA">JAVA</option>
                  </select>
                </div>
              )}
              <div>
                <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                  Routine definition
                </label>
                <textarea
                  value={newFnDefinition}
                  onChange={(e) => setNewFnDefinition(e.target.value)}
                  placeholder="RETURN LOWER(input_value)"
                  rows={4}
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[12px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                />
              </div>
              <input
                value={newFnComment}
                onChange={(e) => setNewFnComment(e.target.value)}
                placeholder="comment (optional)"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <div className="flex gap-2 justify-end">
                <button
                  onClick={() => setShowCreateFunction(false)}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={createFunction}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Create
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Function
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-28">
                    Body
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-28">
                    Returns
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Definition
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <LoadingRow cols={5} />
                ) : functions.length === 0 ? (
                  <EmptyRow cols={5} text="No functions in this schema." />
                ) : (
                  functions.map((f) => (
                    <tr key={f.name} className="hover:bg-gray-50 group">
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <Sigma size={14} className="text-purple-500" />
                          <span className="text-[13px] font-medium font-mono">
                            {f.name}
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {f.routine_body}
                        {f.external_language ? ` (${f.external_language})` : ""}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500 font-mono">
                        {f.data_type || "—"}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500 font-mono truncate max-w-[280px]">
                        {f.routine_definition}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={() => deleteFunction(f.name)}
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

      {/* ================= Volumes ================= */}
      {view === "tables" &&
        entityTab === "volumes" &&
        selectedCat &&
        selectedSchema && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">
              Volumes in {selectedCat.name}.{selectedSchema.name}
            </h2>
            <button
              onClick={() => setShowCreateVolume(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Create Volume
            </button>
          </div>

          {showCreateVolume && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4 space-y-3">
              <div className="grid grid-cols-3 gap-2">
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Volume name
                  </label>
                  <input
                    value={newVolName}
                    onChange={(e) => setNewVolName(e.target.value)}
                    placeholder="raw_data"
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                    autoFocus
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Type
                  </label>
                  <select
                    value={newVolType}
                    onChange={(e) => setNewVolType(e.target.value)}
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    <option value="MANAGED">MANAGED</option>
                    <option value="EXTERNAL">EXTERNAL</option>
                  </select>
                </div>
              </div>
              {newVolType === "EXTERNAL" && (
                <div>
                  <label className="block text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-1">
                    Storage location (s3://bucket/path/)
                  </label>
                  <input
                    value={newVolLocation}
                    onChange={(e) => setNewVolLocation(e.target.value)}
                    placeholder="s3://my-bucket/external/raw/"
                    className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                  />
                </div>
              )}
              <input
                value={newVolComment}
                onChange={(e) => setNewVolComment(e.target.value)}
                placeholder="comment (optional)"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <div className="flex gap-2 justify-end">
                <button
                  onClick={() => setShowCreateVolume(false)}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={createVolume}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Create
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Volume
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-24">
                    Type
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Storage location
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <LoadingRow cols={4} />
                ) : volumes.length === 0 ? (
                  <EmptyRow cols={4} text="No volumes in this schema." />
                ) : (
                  volumes.map((v) => (
                    <tr key={v.name} className="hover:bg-gray-50 group">
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <HardDrive size={14} className="text-cyan-500" />
                          <span className="text-[13px] font-medium font-mono">
                            {v.name}
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {v.volume_type}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500 font-mono truncate max-w-[360px]">
                        {v.storage_location}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={() => deleteVolume(v.name)}
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

      {/* ================= Models ================= */}
      {view === "tables" &&
        entityTab === "models" &&
        selectedCat &&
        selectedSchema && (
        <>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-[13px] font-medium text-gray-700">
              Models in {selectedCat.name}.{selectedSchema.name}
            </h2>
            <button
              onClick={() => setShowCreateModel(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
            >
              <Plus size={14} /> Register Model
            </button>
          </div>

          {showCreateModel && (
            <div className="mb-3 bg-white rounded-xl border border-gray-200 p-4 space-y-2">
              <input
                value={newModelName}
                onChange={(e) => setNewModelName(e.target.value)}
                placeholder="model_name"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500 font-mono"
                autoFocus
              />
              <input
                value={newModelComment}
                onChange={(e) => setNewModelComment(e.target.value)}
                placeholder="comment (optional)"
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded-lg text-[13px] focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <div className="flex gap-2 justify-end">
                <button
                  onClick={() => setShowCreateModel(false)}
                  className="px-3 py-1.5 border border-gray-300 text-gray-700 rounded-lg text-[12px] font-medium hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={createModel}
                  className="px-3 py-1.5 bg-gray-900 text-white rounded-lg text-[12px] font-medium hover:bg-gray-800"
                >
                  Register
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Model
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Storage location
                  </th>
                  <th className="text-left px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider">
                    Comment
                  </th>
                  <th className="text-right px-4 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wider w-20">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading ? (
                  <LoadingRow cols={4} />
                ) : models.length === 0 ? (
                  <EmptyRow cols={4} text="No models in this schema." />
                ) : (
                  models.map((m) => (
                    <tr key={m.name} className="hover:bg-gray-50 group">
                      <td className="px-4 py-2.5">
                        <div className="flex items-center gap-2">
                          <Brain size={14} className="text-pink-500" />
                          <span className="text-[13px] font-medium font-mono">
                            {m.name}
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500 font-mono truncate max-w-[360px]">
                        {m.storage_location}
                      </td>
                      <td className="px-4 py-2.5 text-[12px] text-gray-500">
                        {m.comment || "—"}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <button
                          onClick={() => deleteModel(m.name)}
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

      {/* ================= Detail ================= */}
      {view === "detail" && (
        <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
          <div className="px-4 py-2.5 bg-gray-50 border-b border-gray-200">
            <h3 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider">
              Table Metadata
            </h3>
          </div>
          {selectedTable ? (
            <div className="p-4 space-y-4">
              <DetailGrid
                rows={[
                  ["Full name", selectedTable.full_name || ""],
                  ["Table ID", selectedTable.table_id || ""],
                  ["Type", selectedTable.table_type],
                  ["Format", selectedTable.data_source_format],
                  ["Owner", selectedTable.owner || "—"],
                  ["Storage location", selectedTable.storage_location],
                  ["Created", fmtTs(selectedTable.created_at)],
                  ["Updated", fmtTs(selectedTable.updated_at)],
                ]}
              />
              {selectedTable.columns && selectedTable.columns.length > 0 && (
                <div>
                  <h4 className="text-[11px] font-medium text-gray-500 uppercase tracking-wider mb-2">
                    Columns
                  </h4>
                  <table className="w-full border border-gray-200 rounded-lg overflow-hidden">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="text-left px-3 py-1.5 text-[11px] font-medium text-gray-500">
                          #
                        </th>
                        <th className="text-left px-3 py-1.5 text-[11px] font-medium text-gray-500">
                          Name
                        </th>
                        <th className="text-left px-3 py-1.5 text-[11px] font-medium text-gray-500">
                          Type
                        </th>
                        <th className="text-left px-3 py-1.5 text-[11px] font-medium text-gray-500">
                          Nullable
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {selectedTable.columns.map((c) => (
                        <tr key={c.position}>
                          <td className="px-3 py-1.5 text-[12px] text-gray-500 font-mono">
                            {c.position}
                          </td>
                          <td className="px-3 py-1.5 text-[12px] font-mono font-medium">
                            {c.name}
                          </td>
                          <td className="px-3 py-1.5 text-[12px] text-gray-600 font-mono">
                            {c.type_text}
                          </td>
                          <td className="px-3 py-1.5 text-[12px] text-gray-500">
                            {c.nullable ? "yes" : "no"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ) : (
            <div className="p-4">
              <div className="flex items-center justify-center gap-3 py-8">
                <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
                  <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
                </div>
                <span className="text-[12px] text-gray-400">Loading</span>
              </div>
            </div>
          )}
        </div>
      )}

      {policyTarget && (() => {
        const t = policyTarget;
        const scope =
          t.kind === "catalog"
            ? t.name
            : t.kind === "schema"
              ? `${t.catalog}.${t.schema}`
              : `${t.catalog}.${t.schema}.${t.table}`;
        const load =
          t.kind === "catalog"
            ? () => unity.getCatalogPolicy(t.name)
            : t.kind === "schema"
              ? () => unity.getSchemaPolicy(t.catalog, t.schema)
              : () => unity.getTablePolicy(t.catalog, t.schema, t.table);
        const save =
          t.kind === "catalog"
            ? (p: unknown) => unity.setCatalogPolicy(t.name, p)
            : t.kind === "schema"
              ? (p: unknown) => unity.setSchemaPolicy(t.catalog, t.schema, p)
              : (p: unknown) =>
                  unity.setTablePolicy(t.catalog, t.schema, t.table, p);
        return (
          <PolicyEditor
            scope={`${t.kind} · ${scope}`}
            load={load}
            save={save}
            onClose={() => setPolicyTarget(null)}
          />
        );
      })()}

      {securityTarget && (
        <TableSecurityEditor
          catalog={securityTarget.catalog}
          schema={securityTarget.schema}
          table={securityTarget.table}
          onClose={() => setSecurityTarget(null)}
          onSaved={(info) => {
            setTables((ts) =>
              ts.map((row) => (row.name === info.name ? info : row)),
            );
          }}
        />
      )}
    </div>
  );
}

// ---------- Helpers ----------

function updateColumn(
  cols: NewColumnRow[],
  setCols: (c: NewColumnRow[]) => void,
  i: number,
  patch: Partial<NewColumnRow>,
) {
  setCols(cols.map((c, j) => (j === i ? { ...c, ...patch } : c)));
}

function fmtTs(unixSec: number): string {
  if (!unixSec) return "—";
  return new Date(unixSec * 1000).toLocaleString();
}

function LoadingRow({ cols }: { cols: number }) {
  return (
    <tr>
      <td colSpan={cols} className="px-4 py-8 text-center">
        <div className="flex items-center justify-center gap-3">
          <div className="w-16 h-0.5 bg-gray-200 rounded-full overflow-hidden">
            <div className="h-full w-1/2 bg-blue-400 rounded-full animate-loading-bar" />
          </div>
          <span className="text-[12px] text-gray-400">Loading</span>
        </div>
      </td>
    </tr>
  );
}

function EmptyRow({ cols, text }: { cols: number; text: string }) {
  return (
    <tr>
      <td
        colSpan={cols}
        className="px-4 py-8 text-center text-[12px] text-gray-400"
      >
        {text}
      </td>
    </tr>
  );
}

function DetailGrid({ rows }: { rows: [string, string][] }) {
  return (
    <dl className="grid grid-cols-[180px_1fr] gap-x-4 gap-y-1.5 text-[12px]">
      {rows.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-gray-500">{k}</dt>
          <dd className="text-gray-900 font-mono break-all">{v || "—"}</dd>
        </div>
      ))}
    </dl>
  );
}
