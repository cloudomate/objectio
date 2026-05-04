"""
ObjectIO Unity Catalog REST API — smoke test.

Walks the full Databricks-style lifecycle against the gateway:
  1. List catalogs (sanity ping)
  2. Create catalog          -> auto-provisions backing bucket
  3. Create schema
  4. Create MANAGED table    -> storage_location auto-derived
  5. Get / list table
  6. Request temporary table credentials (vended STS)
  7. Tear everything down (table -> schema -> catalog with force)

Run from your local Mac against s3.imys.in:

  python3 -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  export AWS_ACCESS_KEY_ID=AKIA...
  export AWS_SECRET_ACCESS_KEY=...
  python unity_smoke_test.py

Auth: SigV4 with the same access key / secret as S3. The gateway's
unified iceberg-auth layer accepts SigV4, OIDC bearer, or session
cookie — SigV4 keeps this script self-contained.

Each request that fails halts the script with a non-zero exit so
this can be wired into CI.
"""

import os
import sys
import time
import uuid
from urllib.parse import urlparse

import boto3
import botocore.auth
import botocore.awsrequest
import botocore.credentials
import requests

HOST = os.environ.get("OBJECTIO_HOST", "https://s3.imys.in")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

# Use a unique catalog name so re-runs don't collide and so a half-failed
# previous run can be inspected before the next one begins.
RUN_ID = os.environ.get("UNITY_RUN_ID", uuid.uuid4().hex[:8])
CATALOG = f"smoke_{RUN_ID}"
SCHEMA = "analytics"
TABLE = "events"

UNITY_BASE = f"{HOST}/api/2.1/unity-catalog"


def fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"  [ok] {msg}")


def step(title: str) -> None:
    print(f"\n=== {title} ===")


def sigv4_session() -> requests.Session:
    """A requests.Session that signs every outgoing request with SigV4.

    The gateway's iceberg-auth middleware accepts SigV4 with `service=s3`
    just like the S3 API — same access key / secret key, same region.
    """
    creds = botocore.credentials.Credentials(ACCESS_KEY, SECRET_KEY)
    signer = botocore.auth.SigV4Auth(creds, "s3", REGION)

    sess = requests.Session()

    def sign(req: requests.PreparedRequest) -> requests.PreparedRequest:
        # Re-sign on every send because the body / URL can change in
        # redirects. botocore's AWSRequest is the canonical input shape.
        aws_req = botocore.awsrequest.AWSRequest(
            method=req.method,
            url=req.url,
            data=req.body or b"",
            headers={k: v for k, v in req.headers.items() if k.lower() != "host"},
        )
        # Strip any host header — botocore re-derives it from the URL.
        signer.add_auth(aws_req)
        for k, v in aws_req.headers.items():
            req.headers[k] = v
        return req

    # Wrap the session's send so signing happens after requests.prepare()
    # has filled in Content-Length / Host etc.
    original_send = sess.send

    def send(req, **kwargs):
        sign(req)
        return original_send(req, **kwargs)

    sess.send = send  # type: ignore[assignment]
    return sess


def boto3_with(creds: dict) -> "boto3.client":
    """Return a boto3 S3 client signed with the given vended creds."""
    return boto3.client(
        "s3",
        endpoint_url=HOST,
        region_name=REGION,
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        aws_session_token=creds["session_token"],
    )


def expect(resp: requests.Response, *codes: int, action: str) -> dict:
    if resp.status_code not in codes:
        fail(
            f"{action}: HTTP {resp.status_code} "
            f"(expected one of {codes})\n  body: {resp.text[:500]}"
        )
    if not resp.content:
        return {}
    try:
        return resp.json()
    except ValueError:
        fail(f"{action}: response was not JSON: {resp.text[:200]}")


def main() -> None:
    if not ACCESS_KEY or not SECRET_KEY:
        fail("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")

    parsed = urlparse(HOST)
    if not parsed.scheme or not parsed.netloc:
        fail(f"OBJECTIO_HOST must be an absolute URL, got {HOST!r}")

    print(f"Host:    {HOST}")
    print(f"Region:  {REGION}")
    print(f"Run ID:  {RUN_ID}")
    print(f"Catalog: {CATALOG}")

    s = sigv4_session()
    s.headers["Content-Type"] = "application/json"
    s.headers["Accept"] = "application/json"

    # ---------- 1. List catalogs (round-trip the auth layer) ----------
    step("List catalogs (auth round-trip)")
    body = expect(
        s.get(f"{UNITY_BASE}/catalogs"),
        200,
        action="GET /catalogs",
    )
    ok(f"catalog count before: {len(body.get('catalogs', []))}")

    # ---------- 2. Create catalog ----------
    step(f"Create catalog {CATALOG!r}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/catalogs",
            json={
                "name": CATALOG,
                "comment": "smoke-test catalog (safe to delete)",
            },
        ),
        200,
        201,
        action="POST /catalogs",
    )
    ok(f"created at={body.get('created_at')} owner={body.get('owner')!r}")

    body = expect(
        s.get(f"{UNITY_BASE}/catalogs/{CATALOG}"),
        200,
        action="GET /catalogs/{name}",
    )
    if body.get("name") != CATALOG:
        fail(f"GET returned wrong name: {body.get('name')!r}")
    ok("get-after-create round-trips")

    # ---------- 3. Create schema ----------
    step(f"Create schema {CATALOG}.{SCHEMA}")
    expect(
        s.post(
            f"{UNITY_BASE}/schemas",
            json={
                "name": SCHEMA,
                "catalog_name": CATALOG,
                "comment": "smoke-test schema",
            },
        ),
        200,
        201,
        action="POST /schemas",
    )
    ok("created")

    body = expect(
        s.get(
            f"{UNITY_BASE}/schemas",
            params={"catalog_name": CATALOG},
        ),
        200,
        action="GET /schemas?catalog_name=…",
    )
    names = [sch["name"] for sch in body.get("schemas", [])]
    if SCHEMA not in names:
        fail(f"newly-created schema not in list: {names}")
    ok(f"schema list contains {SCHEMA!r}")

    # ---------- 4. Create MANAGED table ----------
    step(f"Create MANAGED table {CATALOG}.{SCHEMA}.{TABLE}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/tables",
            json={
                "name": TABLE,
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "columns": [
                    {
                        "name": "event_id",
                        "type_text": "string",
                        "type_name": "STRING",
                        "position": 0,
                        "nullable": False,
                    },
                    {
                        "name": "ts",
                        "type_text": "timestamp",
                        "type_name": "TIMESTAMP",
                        "position": 1,
                        "nullable": False,
                    },
                    {
                        "name": "payload",
                        "type_text": "string",
                        "type_name": "STRING",
                        "position": 2,
                        "nullable": True,
                    },
                ],
            },
        ),
        200,
        201,
        action="POST /tables",
    )
    table_id = body.get("table_id") or ""
    storage = body.get("storage_location") or ""
    if not table_id:
        fail(f"table response missing table_id: {body}")
    if not storage.startswith("s3://"):
        fail(f"MANAGED table storage_location should be s3://…, got {storage!r}")
    ok(f"table_id={table_id}")
    ok(f"storage_location={storage}")

    full_name = f"{CATALOG}.{SCHEMA}.{TABLE}"
    body = expect(
        s.get(f"{UNITY_BASE}/tables/{full_name}"),
        200,
        action="GET /tables/{full_name}",
    )
    if body.get("table_id") != table_id:
        fail(
            f"GET returned different table_id: "
            f"{body.get('table_id')!r} vs {table_id!r}"
        )
    ok("get-after-create round-trips")

    # ---------- 5. Vended READ credentials ----------
    step(f"Request READ credentials for table_id={table_id}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/temporary-table-credentials",
            json={"table_id": table_id, "operation": "READ"},
        ),
        200,
        action="POST /temporary-table-credentials (READ)",
    )
    read_creds = body.get("aws_temp_credentials", {})
    for f in ("access_key_id", "secret_access_key", "session_token"):
        if not read_creds.get(f):
            fail(f"READ credentials missing {f}")
    if not read_creds["access_key_id"].startswith("ASIA"):
        fail(f"expected ASIA* key, got {read_creds['access_key_id']!r}")
    if not body.get("url", "").startswith("s3://"):
        fail(f"vended credentials url should be s3://…, got {body.get('url')!r}")
    expiry = body.get("expiration_time", 0)
    now = int(time.time())
    if expiry < now:
        fail(f"READ credentials already expired (now={now}, expiry={expiry})")
    ok(f"READ creds valid for ~{expiry - now}s")

    bucket = storage[len("s3://"):].split("/", 1)[0]
    table_prefix = storage[len("s3://"):].split("/", 1)[1] if "/" in storage[5:] else ""

    read_s3 = boto3_with(read_creds)

    # 5a. READ creds CAN list within the table prefix.
    step("READ creds: list within table prefix")
    try:
        read_s3.list_objects_v2(Bucket=bucket, Prefix=table_prefix, MaxKeys=1)
        ok(f"list_objects_v2(bucket={bucket!r}, prefix={table_prefix!r}) succeeded")
    except Exception as e:
        fail(f"in-scope list rejected: {e}")

    # 5b. READ creds CANNOT write — even within the prefix.
    step("READ creds: PUT into table prefix → expect AccessDenied")
    try:
        read_s3.put_object(
            Bucket=bucket,
            Key=f"{table_prefix}smoke-read-write-attempt.txt",
            Body=b"x",
        )
        fail("PUT with READ creds was unexpectedly allowed")
    except Exception as e:
        if "AccessDenied" not in str(e) and "Forbidden" not in str(e):
            fail(f"PUT with READ creds rejected, but with wrong error: {e}")
        ok("PUT correctly rejected with AccessDenied")

    # 5c. READ creds CANNOT read from a different bucket.
    step("READ creds: list a different bucket → expect AccessDenied")
    try:
        read_s3.list_objects_v2(Bucket="some-other-bucket", MaxKeys=1)
        fail("cross-bucket list with READ creds was unexpectedly allowed")
    except Exception as e:
        if "AccessDenied" not in str(e) and "Forbidden" not in str(e):
            fail(f"cross-bucket list rejected, but with wrong error: {e}")
        ok("cross-bucket list correctly rejected with AccessDenied")

    # ---------- 6. Vended READ_WRITE credentials ----------
    step(f"Request READ_WRITE credentials for table_id={table_id}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/temporary-table-credentials",
            json={"table_id": table_id, "operation": "READ_WRITE"},
        ),
        200,
        action="POST /temporary-table-credentials (READ_WRITE)",
    )
    rw_creds = body.get("aws_temp_credentials", {})
    rw_s3 = boto3_with(rw_creds)

    test_key = f"{table_prefix}smoke-roundtrip.txt"

    # 6a. POSITIVE: READ_WRITE PUT inside scope succeeds with byte-exact GET.
    step("READ_WRITE creds: PUT inside scope")
    try:
        rw_s3.put_object(Bucket=bucket, Key=test_key, Body=b"hello unity")
        ok("PUT accepted")
    except Exception as e:
        fail(f"in-scope PUT rejected: {e}")

    step("READ_WRITE creds: GET it back, bytes match")
    try:
        got = rw_s3.get_object(Bucket=bucket, Key=test_key)["Body"].read()
        if got != b"hello unity":
            fail(f"GET returned wrong bytes: {got!r}")
        ok(f"GET returned {len(got)} bytes, content matches")
    except Exception as e:
        fail(f"in-scope GET rejected: {e}")

    # 6b. POSITIVE: READ creds CAN read the object the RW creds wrote.
    # Same scope, just downgraded operation — proves R is not a no-op.
    step("READ creds: GET an existing object inside scope")
    try:
        got = read_s3.get_object(Bucket=bucket, Key=test_key)["Body"].read()
        if got != b"hello unity":
            fail(f"READ creds GET returned wrong bytes: {got!r}")
        ok(f"READ creds successfully read {len(got)} bytes")
    except Exception as e:
        fail(f"READ-creds in-scope GET rejected: {e}")

    # 6c. POSITIVE: READ_WRITE HEAD returns metadata for the object.
    step("READ_WRITE creds: HEAD the object")
    try:
        head = rw_s3.head_object(Bucket=bucket, Key=test_key)
        cl = head.get("ContentLength")
        if cl != len(b"hello unity"):
            fail(f"HEAD returned wrong ContentLength: {cl}")
        ok(f"HEAD returned ContentLength={cl}")
    except Exception as e:
        fail(f"in-scope HEAD rejected: {e}")

    # 6d. POSITIVE: READ_WRITE DELETE removes the object, then a follow-up
    # GET 404s — proves the delete actually landed (and exercises the
    # mutating-method allowance one more time).
    step("READ_WRITE creds: DELETE the object")
    try:
        rw_s3.delete_object(Bucket=bucket, Key=test_key)
        ok("DELETE accepted")
    except Exception as e:
        fail(f"in-scope DELETE rejected: {e}")

    step("READ creds: GET after DELETE → expect 404 (NoSuchKey)")
    try:
        read_s3.get_object(Bucket=bucket, Key=test_key)
        fail("GET on deleted key was unexpectedly allowed")
    except Exception as e:
        msg = str(e)
        if "NoSuchKey" in msg or "Not Found" in msg or "404" in msg:
            ok("GET correctly returned NoSuchKey")
        elif "AccessDenied" in msg:
            fail(f"GET on deleted key returned AccessDenied — scope check is broken: {e}")
        else:
            fail(f"GET on deleted key returned unexpected error: {e}")

    # ---------- 7. Functions ----------
    fn_name = f"to_lower_{RUN_ID}"
    fn_full = f"{CATALOG}.{SCHEMA}.{fn_name}"
    step(f"Create UDF {fn_full}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/functions",
            # Wrapped envelope per Databricks spec — same shape the official
            # Databricks SDK posts. Our handler also accepts the flat form.
            json={
                "function_info": {
                    "name": fn_name,
                    "catalog_name": CATALOG,
                    "schema_name": SCHEMA,
                    "routine_definition": "RETURN LOWER(input_value)",
                    "routine_body": "SQL",
                    "data_type": "STRING",
                    "comment": "smoke-test UDF",
                }
            },
        ),
        200,
        201,
        action="POST /functions",
    )
    if not body.get("function_id"):
        fail(f"function response missing function_id: {body}")
    ok(f"function_id={body['function_id']} body={body['routine_body']}")

    body = expect(
        s.get(
            f"{UNITY_BASE}/functions",
            params={"catalog_name": CATALOG, "schema_name": SCHEMA},
        ),
        200,
        action="GET /functions?catalog_name=…&schema_name=…",
    )
    if fn_name not in [f["name"] for f in body.get("functions", [])]:
        fail(f"function not in list: {body.get('functions')}")
    ok("function visible in list")

    body = expect(
        s.get(f"{UNITY_BASE}/functions/{fn_full}"),
        200,
        action="GET /functions/{full_name}",
    )
    if body.get("routine_definition") != "RETURN LOWER(input_value)":
        fail(
            f"routine_definition round-trip mismatch: {body.get('routine_definition')!r}"
        )
    if body.get("routine_body") != "SQL":
        fail(f"routine_body should be 'SQL', got {body.get('routine_body')!r}")
    ok("function round-trip OK")

    expect(
        s.delete(f"{UNITY_BASE}/functions/{fn_full}"),
        200,
        204,
        action="DELETE /functions/{full_name}",
    )
    ok("function deleted")

    # ---------- 8. Volumes ----------
    vol_name = f"raw_{RUN_ID}"
    vol_full = f"{CATALOG}.{SCHEMA}.{vol_name}"
    step(f"Create MANAGED volume {vol_full}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/volumes",
            json={
                "name": vol_name,
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "volume_type": "MANAGED",
                "comment": "smoke-test volume",
            },
        ),
        200,
        201,
        action="POST /volumes",
    )
    vol_id = body.get("volume_id") or ""
    vol_storage = body.get("storage_location") or ""
    if not vol_id:
        fail(f"volume response missing volume_id: {body}")
    if "/__volumes/" not in vol_storage:
        fail(f"MANAGED volume storage should include __volumes/ marker: {vol_storage!r}")
    ok(f"volume_id={vol_id} storage={vol_storage}")

    # Vended READ_WRITE creds for the volume — proves the volume scope is
    # honored end-to-end exactly like tables.
    step(f"Request READ_WRITE credentials for volume_id={vol_id}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/temporary-volume-credentials",
            json={"volume_id": vol_id, "operation": "READ_WRITE"},
        ),
        200,
        action="POST /temporary-volume-credentials (READ_WRITE)",
    )
    vol_creds = body.get("aws_temp_credentials", {})
    if not vol_creds.get("access_key_id", "").startswith("ASIA"):
        fail(f"expected ASIA* key, got {vol_creds.get('access_key_id')!r}")
    ok(f"got STS creds, expires in ~{body.get('expiration_time', 0) - int(time.time())}s")

    expect(
        s.delete(f"{UNITY_BASE}/volumes/{vol_full}"),
        200,
        204,
        action="DELETE /volumes/{full_name}",
    )
    ok("volume deleted")

    # ---------- 9. Models + ModelVersions ----------
    model_name = f"clf_{RUN_ID}"
    model_full = f"{CATALOG}.{SCHEMA}.{model_name}"
    step(f"Register model {model_full}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/models",
            json={
                "name": model_name,
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "comment": "smoke-test model",
            },
        ),
        200,
        201,
        action="POST /models",
    )
    if not body.get("model_id"):
        fail(f"model response missing model_id: {body}")
    if "/__models/" not in (body.get("storage_location") or ""):
        fail(
            f"MANAGED model storage should include __models/ marker: "
            f"{body.get('storage_location')!r}"
        )
    ok(f"model_id={body['model_id']} storage={body['storage_location']}")

    step(f"Create model version v1 for {model_full}")
    body = expect(
        s.post(
            f"{UNITY_BASE}/models/{model_full}/versions",
            json={
                "model_name": model_name,
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "source": "s3://artifacts/runs/abc/model",
                "run_id": "mlflow-run-abc",
                "description": "first version",
            },
        ),
        200,
        201,
        action="POST /models/{full_name}/versions",
    )
    if body.get("version") != 1:
        fail(f"first version should be 1, got {body.get('version')!r}")
    if body.get("status") != "PENDING_REGISTRATION":
        fail(f"new version should be PENDING_REGISTRATION, got {body.get('status')!r}")
    ok(f"version=1 status=PENDING_REGISTRATION version_id={body.get('version_id')}")

    step("Promote v1 to READY")
    body = expect(
        s.patch(
            f"{UNITY_BASE}/models/{model_full}/versions/1",
            json={"status": "READY"},
        ),
        200,
        action="PATCH /models/{full_name}/versions/{v}",
    )
    if body.get("status") != "READY":
        fail(f"status should be READY after promotion, got {body.get('status')!r}")
    ok("status flipped to READY")

    step("Create v2, verify monotonic numbering")
    body = expect(
        s.post(
            f"{UNITY_BASE}/models/{model_full}/versions",
            json={
                "model_name": model_name,
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "source": "s3://artifacts/runs/def/model",
            },
        ),
        200,
        201,
        action="POST /models/{full_name}/versions (v2)",
    )
    if body.get("version") != 2:
        fail(f"second version should be 2, got {body.get('version')!r}")
    ok("version=2")

    body = expect(
        s.get(f"{UNITY_BASE}/models/{model_full}/versions"),
        200,
        action="GET /models/{full_name}/versions",
    )
    versions = sorted(v["version"] for v in body.get("model_versions", []))
    if versions != [1, 2]:
        fail(f"expected versions [1, 2], got {versions}")
    ok(f"list returns versions={versions}")

    step("Delete model — versions cascade")
    expect(
        s.delete(f"{UNITY_BASE}/models/{model_full}"),
        200,
        204,
        action="DELETE /models/{full_name}",
    )
    body = expect(
        s.get(f"{UNITY_BASE}/models/{model_full}/versions"),
        200,
        404,
        action="GET versions after model delete",
    )
    remaining = body.get("model_versions", [])
    if remaining:
        fail(f"versions not cascaded: {remaining}")
    ok("model + all versions removed")

    # ---------- 10. Cleanup ----------
    step("Cleanup")
    expect(
        s.delete(f"{UNITY_BASE}/tables/{full_name}"),
        200,
        204,
        action="DELETE /tables/{full_name}",
    )
    ok("table deleted")

    expect(
        s.delete(f"{UNITY_BASE}/schemas/{CATALOG}.{SCHEMA}"),
        200,
        204,
        action="DELETE /schemas/{full_name}",
    )
    ok("schema deleted")

    # `force=true` because the catalog still owns the now-empty backing
    # bucket and the auto-provisioned bucket lifecycle is tied to the
    # catalog, not the schemas inside it.
    expect(
        s.delete(
            f"{UNITY_BASE}/catalogs/{CATALOG}",
            params={"force": "true"},
        ),
        200,
        204,
        action="DELETE /catalogs/{name}?force=true",
    )
    ok("catalog deleted (force=true)")

    print("\n[PASS] Unity Catalog smoke test green")


if __name__ == "__main__":
    main()
