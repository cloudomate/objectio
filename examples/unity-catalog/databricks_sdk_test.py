"""Real-world Unity Catalog test using the Databricks Python SDK.

Talks to ObjectIO's `/api/2.1/unity-catalog/*` REST surface using the same
client Databricks customers use against Unity. Proves the wire shape is
SDK-compatible end-to-end.

Auth: aio's session-cookie path doesn't fit the SDK, but the SDK sends a
Bearer token. With OIDC unconfigured, our auth chain falls through to "no
auth" and the handlers (which take Option<Extension<AuthResult>>) accept
the request — exactly the behaviour we want from this kind of smoke test.
For tightly authenticated runs, set OBJECTIO_OIDC_* and pass a real PAT.

Usage:
    OBJECTIO_HOST=http://127.0.0.1:9000 \
      ./.venv-dbx/bin/python databricks_sdk_test.py
"""

import os
import sys
import time
import warnings
from typing import Optional

# Silence the urllib3/LibreSSL and google-auth EOL warnings on macOS Python 3.9.
warnings.filterwarnings("ignore", category=Warning)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnTypeName,
    DataSourceFormat,
    TableType,
    VolumeType,
)
from databricks.sdk.errors import DatabricksError, ResourceAlreadyExists


HOST = os.environ.get("OBJECTIO_HOST", "http://127.0.0.1:9000")
TOKEN = os.environ.get("OBJECTIO_TOKEN", "dummy-bearer-token-no-oidc")
RUN_ID = str(int(time.time()))
CATALOG = f"sdk_test_{RUN_ID}"
SCHEMA = "default"
TABLE = "events"


def fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def step(title: str) -> None:
    print(f"\n• {title}")


def main() -> None:
    print(f"Host:    {HOST}")
    print(f"Catalog: {CATALOG}")

    # The Databricks SDK is workspace-aware and expects /api/2.1/unity-catalog
    # to live at <host>/api/2.1/unity-catalog — exactly where ObjectIO hosts
    # it. No `auth_type` override needed; pat → Authorization: Bearer header.
    w = WorkspaceClient(host=HOST, token=TOKEN, auth_type="pat")

    # ---------- 1. Catalogs ----------
    step("List catalogs (auth round-trip)")
    before = list(w.catalogs.list())
    ok(f"catalog count before: {len(before)}")

    step(f"Create catalog {CATALOG!r}")
    cat = w.catalogs.create(
        name=CATALOG, comment="databricks-sdk smoke-test catalog"
    )
    if cat.name != CATALOG:
        fail(f"create returned wrong name: {cat.name!r}")
    ok(f"created storage_root={cat.storage_root!r}")

    got = w.catalogs.get(CATALOG)
    if got.name != CATALOG:
        fail(f"get returned wrong name: {got.name!r}")
    ok("get-after-create round-trips")

    # ---------- 2. Schemas ----------
    full_schema = f"{CATALOG}.{SCHEMA}"
    step(f"Create schema {full_schema}")
    sch = w.schemas.create(name=SCHEMA, catalog_name=CATALOG, comment="default")
    if sch.full_name != full_schema:
        fail(f"schema full_name mismatch: {sch.full_name!r}")
    ok("schema created")

    schemas = list(w.schemas.list(catalog_name=CATALOG))
    if SCHEMA not in [s.name for s in schemas]:
        fail(f"newly-created schema not listed: {[s.name for s in schemas]}")
    ok(f"list returns {len(schemas)} schema(s)")

    # ---------- 3. Tables ----------
    full_table = f"{CATALOG}.{SCHEMA}.{TABLE}"
    step(f"Create MANAGED table {full_table}")
    columns = [
        ColumnInfo(
            name="event_id",
            type_text="string",
            type_name=ColumnTypeName.STRING,
            position=0,
            nullable=False,
        ),
        ColumnInfo(
            name="ts",
            type_text="timestamp",
            type_name=ColumnTypeName.TIMESTAMP,
            position=1,
            nullable=False,
        ),
        ColumnInfo(
            name="payload",
            type_text="string",
            type_name=ColumnTypeName.STRING,
            position=2,
            nullable=True,
        ),
    ]
    # Newer SDK signatures require storage_location positionally even for
    # MANAGED tables. Empty string lets our backend derive the location.
    tbl = w.tables.create(
        name=TABLE,
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        table_type=TableType.MANAGED,
        data_source_format=DataSourceFormat.DELTA,
        storage_location="",
        columns=columns,
    )
    if not tbl.table_id:
        fail(f"table_id missing in create response: {tbl}")
    if not (tbl.storage_location or "").startswith("s3://"):
        fail(
            f"MANAGED table storage_location should be s3://…, got {tbl.storage_location!r}"
        )
    ok(f"table_id={tbl.table_id} storage={tbl.storage_location}")

    got_tbl = w.tables.get(full_table)
    if got_tbl.table_id != tbl.table_id:
        fail(
            f"GET returned different table_id: "
            f"{got_tbl.table_id!r} vs {tbl.table_id!r}"
        )
    if not got_tbl.columns or len(got_tbl.columns) != 3:
        fail(f"GET returned wrong column count: {got_tbl.columns}")
    ok(f"get-after-create round-trips {len(got_tbl.columns)} columns")

    tables = list(w.tables.list(catalog_name=CATALOG, schema_name=SCHEMA))
    if TABLE not in [t.name for t in tables]:
        fail(f"newly-created table not listed: {[t.name for t in tables]}")
    ok(f"list returns {len(tables)} table(s)")

    # ---------- 4. Volumes ----------
    vol_name = f"raw_{RUN_ID}"
    full_vol = f"{CATALOG}.{SCHEMA}.{vol_name}"
    step(f"Create MANAGED volume {full_vol}")
    vol = w.volumes.create(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name=vol_name,
        volume_type=VolumeType.MANAGED,
        comment="sdk-test volume",
    )
    if not vol.volume_id:
        fail(f"volume_id missing in create response: {vol}")
    if "/__volumes/" not in (vol.storage_location or ""):
        fail(
            f"MANAGED volume storage should include /__volumes/ marker: "
            f"{vol.storage_location!r}"
        )
    ok(f"volume_id={vol.volume_id} storage={vol.storage_location}")

    vols = list(w.volumes.list(catalog_name=CATALOG, schema_name=SCHEMA))
    if vol_name not in [v.name for v in vols]:
        fail(f"newly-created volume not listed: {[v.name for v in vols]}")
    ok(f"list returns {len(vols)} volume(s)")

    # ---------- 5. Functions ----------
    # The Databricks SDK's functions API takes a richer FunctionInfo object;
    # build it from the model class to verify our shape matches.
    fn_name = f"to_lower_{RUN_ID}"
    full_fn = f"{CATALOG}.{SCHEMA}.{fn_name}"
    step(f"Create UDF {full_fn}")
    try:
        from databricks.sdk.service.catalog import (
            CreateFunction,
            CreateFunctionParameterStyle,
            CreateFunctionRoutineBody,
            CreateFunctionSecurityType,
            CreateFunctionSqlDataAccess,
            FunctionParameterInfo,
            FunctionParameterInfos,
            FunctionParameterMode,
            FunctionParameterType,
        )
    except ImportError as e:
        fail(f"could not import function-info classes: {e}")

    fn_req = CreateFunction(
        name=fn_name,
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        input_params=FunctionParameterInfos(
            parameters=[
                FunctionParameterInfo(
                    name="input_value",
                    type_text="string",
                    type_name=ColumnTypeName.STRING,
                    position=0,
                    parameter_mode=FunctionParameterMode.IN,
                    parameter_type=FunctionParameterType.PARAM,
                )
            ]
        ),
        data_type=ColumnTypeName.STRING,
        full_data_type="STRING",
        routine_body=CreateFunctionRoutineBody.SQL,
        routine_definition="RETURN LOWER(input_value)",
        parameter_style=CreateFunctionParameterStyle.S,
        is_deterministic=True,
        sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
        is_null_call=False,
        security_type=CreateFunctionSecurityType.DEFINER,
        specific_name=fn_name,
    )
    try:
        fn = w.functions.create(function_info=fn_req)
    except DatabricksError as e:
        fail(f"create function failed: {e}")
    ok(f"function created (function_id={getattr(fn, 'function_id', '?')})")

    fns = list(w.functions.list(catalog_name=CATALOG, schema_name=SCHEMA))
    if fn_name not in [f.name for f in fns]:
        fail(f"newly-created function not listed: {[f.name for f in fns]}")
    ok(f"list returns {len(fns)} function(s)")

    # ---------- 6. Cleanup ----------
    step("Cleanup")
    w.functions.delete(full_fn)
    ok("function deleted")
    w.volumes.delete(full_vol)
    ok("volume deleted")
    w.tables.delete(full_table)
    ok("table deleted")
    w.schemas.delete(full_schema)
    ok("schema deleted")
    w.catalogs.delete(CATALOG, force=True)
    ok("catalog deleted (force=true)")

    print("\n[PASS] Databricks SDK round-trip green against ObjectIO Unity Catalog")


if __name__ == "__main__":
    main()
