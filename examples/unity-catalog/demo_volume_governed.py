"""End-to-end Unity Volume governed-prefix demo.

Real workflow: create a MANAGED volume → mint short-lived STS creds scoped
to that volume's S3 prefix → use boto3 with those creds to write a CSV +
a binary blob → list the prefix → download → verify bytes round-trip.

Then prove the scope guardrails actually fire:
  * the same READ_WRITE creds can NOT touch a sibling volume
  * READ-only creds can list/get but cannot PUT

Usage:
    OBJECTIO_HOST=http://127.0.0.1:9000 \
      .venv-dbx/bin/python demo_volume_governed.py
"""

import io
import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")

import boto3
import requests
from botocore.config import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType


HOST = os.environ.get("OBJECTIO_HOST", "http://127.0.0.1:9000")
TOKEN = os.environ.get("OBJECTIO_TOKEN", "dummy")
RUN = str(int(time.time()))
CATALOG = f"vol_demo_{RUN}"
SCHEMA = "raw"
VOLUME = "ingest"
VOLUME_OTHER = "neighbor"


def fail(msg: str) -> None:
    print(f"\n[FAIL] {msg}")
    sys.exit(1)


def step(t: str) -> None:
    print(f"\n• {t}")


def ok(t: str) -> None:
    print(f"  ✓ {t}")


def vend_volume_creds(volume_id: str, op: str) -> dict:
    r = requests.post(
        f"{HOST}/api/2.1/unity-catalog/temporary-volume-credentials",
        json={"volume_id": volume_id, "operation": op},
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=30,
    )
    if r.status_code != 200:
        fail(f"vend {op} creds failed: {r.status_code} {r.text}")
    return r.json()["aws_temp_credentials"]


def s3_for(creds: dict) -> "boto3.client":
    return boto3.client(
        "s3",
        endpoint_url=HOST,
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        aws_session_token=creds["session_token"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def main() -> None:
    print(f"Host:    {HOST}")
    print(f"Catalog: {CATALOG}")

    w = WorkspaceClient(host=HOST, token=TOKEN, auth_type="pat")

    # ---------- Bootstrap two volumes ----------
    step("Create catalog, schema, two MANAGED volumes")
    w.catalogs.create(name=CATALOG, comment="volume governance demo")
    w.schemas.create(name=SCHEMA, catalog_name=CATALOG, comment="raw data")
    vol = w.volumes.create(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name=VOLUME,
        volume_type=VolumeType.MANAGED,
    )
    vol_other = w.volumes.create(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name=VOLUME_OTHER,
        volume_type=VolumeType.MANAGED,
    )
    bucket = vol.storage_location[len("s3://"):].split("/", 1)[0]
    prefix = vol.storage_location[len("s3://"):].split("/", 1)[1]
    other_prefix = vol_other.storage_location[len("s3://"):].split("/", 1)[1]
    ok(f"volume {VOLUME!r} → {vol.storage_location} (id={vol.volume_id})")
    ok(f"volume {VOLUME_OTHER!r} → {vol_other.storage_location} (id={vol_other.volume_id})")

    # ---------- Mint READ_WRITE creds + happy-path upload ----------
    step("Mint READ_WRITE STS creds for the ingest volume")
    rw = vend_volume_creds(vol.volume_id, "READ_WRITE")
    ok(f"got ASIA*{rw['access_key_id'][-4:]}, expires in ~1h")
    rw_s3 = s3_for(rw)

    csv_bytes = b"id,event,ts\n1,signup,2026-05-04T10:00:00Z\n2,login,2026-05-04T10:01:00Z\n"
    blob_bytes = os.urandom(64 * 1024)

    step(f"Upload events.csv ({len(csv_bytes)} B) and blob.bin ({len(blob_bytes)} B)")
    rw_s3.put_object(Bucket=bucket, Key=f"{prefix}events.csv", Body=csv_bytes)
    rw_s3.put_object(Bucket=bucket, Key=f"{prefix}blob.bin", Body=blob_bytes)
    ok("both PUTs accepted")

    step("List the volume prefix")
    listing = rw_s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    names = sorted(o["Key"][len(prefix):] for o in listing)
    if names != ["blob.bin", "events.csv"]:
        fail(f"unexpected listing: {names}")
    ok(f"list returns {names}")

    step("Download both objects + verify bytes")
    got_csv = rw_s3.get_object(Bucket=bucket, Key=f"{prefix}events.csv")["Body"].read()
    got_blob = rw_s3.get_object(Bucket=bucket, Key=f"{prefix}blob.bin")["Body"].read()
    if got_csv != csv_bytes:
        fail("CSV round-trip mismatch")
    if got_blob != blob_bytes:
        fail(f"blob round-trip mismatch: {len(got_blob)} vs {len(blob_bytes)}")
    ok("byte-exact round-trip on both")

    # ---------- Scope guardrail #1: cross-volume PUT must fail ----------
    step(f"Try to PUT into the {VOLUME_OTHER!r} volume with these creds → expect AccessDenied")
    try:
        rw_s3.put_object(Bucket=bucket, Key=f"{other_prefix}leak.txt", Body=b"x")
        fail("cross-volume write was unexpectedly allowed — scope check is broken!")
    except Exception as e:
        if "AccessDenied" not in str(e) and "Forbidden" not in str(e):
            fail(f"cross-volume PUT rejected, but with wrong error: {e}")
        ok("cross-volume PUT correctly rejected")

    # ---------- Scope guardrail #2: READ creds can't write ----------
    step("Mint READ-only creds, prove they can list+get but not PUT")
    ro = vend_volume_creds(vol.volume_id, "READ")
    ro_s3 = s3_for(ro)
    ro_s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    ok("READ list inside scope works")
    got = ro_s3.get_object(Bucket=bucket, Key=f"{prefix}events.csv")["Body"].read()
    if got != csv_bytes:
        fail("READ-creds GET returned wrong bytes")
    ok("READ get inside scope works (bytes match)")
    try:
        ro_s3.put_object(Bucket=bucket, Key=f"{prefix}leak.txt", Body=b"x")
        fail("READ-creds PUT was unexpectedly allowed")
    except Exception as e:
        if "AccessDenied" not in str(e) and "Forbidden" not in str(e):
            fail(f"READ PUT rejected, but with wrong error: {e}")
        ok("READ PUT correctly rejected")

    # ---------- Cleanup ----------
    step("Cleanup")
    w.volumes.delete(f"{CATALOG}.{SCHEMA}.{VOLUME}")
    w.volumes.delete(f"{CATALOG}.{SCHEMA}.{VOLUME_OTHER}")
    w.schemas.delete(f"{CATALOG}.{SCHEMA}")
    w.catalogs.delete(CATALOG, force=True)
    ok("catalog removed")

    print("\n[PASS] Volume governed-prefix demo green — vended STS scoped to volume + 2 guardrails enforced")


if __name__ == "__main__":
    main()
