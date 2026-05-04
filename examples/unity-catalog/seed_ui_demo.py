"""Seed a realistic, browsable Unity Catalog state for the console UI.

Creates two catalogs with multiple schemas, tables, functions, volumes,
models, and model versions — and leaves it all in place so you can poke
around in http://127.0.0.1:9000/_console/ → Unity Catalog.

Re-run safely: every entity is created idempotently (delete-then-create).

Usage:
    OBJECTIO_HOST=http://127.0.0.1:9000 \
      AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
      .venv-dbx/bin/python seed_ui_demo.py
"""

import os
import pickle
import sys
import warnings

warnings.filterwarnings("ignore")

import boto3
import requests
from botocore.config import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnTypeName,
    DataSourceFormat,
    TableType,
    VolumeType,
)
from sklearn.linear_model import LogisticRegression


HOST = os.environ.get("OBJECTIO_HOST", "http://127.0.0.1:9000")
TOKEN = os.environ.get("OBJECTIO_TOKEN", "dummy")


def post(path: str, body: dict) -> dict:
    r = requests.post(
        f"{HOST}/api/2.1/unity-catalog{path}",
        json=body,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"POST {path} → {r.status_code}: {r.text}")
    return r.json()


def patch(path: str, body: dict) -> dict:
    r = requests.patch(
        f"{HOST}/api/2.1/unity-catalog{path}",
        json=body,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"PATCH {path} → {r.status_code}: {r.text}")
    return r.json()


def reset_catalog(w: WorkspaceClient, name: str) -> None:
    try:
        w.catalogs.delete(name, force=True)
    except Exception:
        pass


def step(t: str) -> None:
    print(f"\n• {t}")


def ok(t: str) -> None:
    print(f"  ✓ {t}")


def main() -> None:
    print(f"Host: {HOST}")
    w = WorkspaceClient(host=HOST, token=TOKEN, auth_type="pat")

    # =========================================================
    # Catalog 1: prod_analytics — realistic data warehouse
    # =========================================================
    step("Seed catalog: prod_analytics")
    reset_catalog(w, "prod_analytics")
    w.catalogs.create(name="prod_analytics", comment="Production analytics warehouse")

    # Schema: marketing
    w.schemas.create(name="marketing", catalog_name="prod_analytics", comment="Campaign + funnel metrics")
    w.tables.create(
        name="campaigns", catalog_name="prod_analytics", schema_name="marketing",
        table_type=TableType.MANAGED, data_source_format=DataSourceFormat.DELTA, storage_location="",
        columns=[
            ColumnInfo(name="campaign_id", type_text="string", type_name=ColumnTypeName.STRING, position=0, nullable=False),
            ColumnInfo(name="name",        type_text="string", type_name=ColumnTypeName.STRING, position=1, nullable=False),
            ColumnInfo(name="budget_usd",  type_text="decimal(12,2)", type_name=ColumnTypeName.DECIMAL, position=2, nullable=True),
            ColumnInfo(name="started_at",  type_text="timestamp", type_name=ColumnTypeName.TIMESTAMP, position=3, nullable=False),
        ],
    )
    w.tables.create(
        name="funnel_events", catalog_name="prod_analytics", schema_name="marketing",
        table_type=TableType.MANAGED, data_source_format=DataSourceFormat.DELTA, storage_location="",
        columns=[
            ColumnInfo(name="user_id",  type_text="bigint", type_name=ColumnTypeName.LONG, position=0, nullable=False),
            ColumnInfo(name="step",     type_text="string", type_name=ColumnTypeName.STRING, position=1, nullable=False),
            ColumnInfo(name="ts",       type_text="timestamp", type_name=ColumnTypeName.TIMESTAMP, position=2, nullable=False),
        ],
    )
    ok("schema marketing: 2 tables")

    # Schema: finance
    w.schemas.create(name="finance", catalog_name="prod_analytics", comment="GL + revenue tables")
    w.tables.create(
        name="general_ledger", catalog_name="prod_analytics", schema_name="finance",
        table_type=TableType.MANAGED, data_source_format=DataSourceFormat.ICEBERG, storage_location="",
        columns=[
            ColumnInfo(name="entry_id",  type_text="bigint", type_name=ColumnTypeName.LONG, position=0, nullable=False),
            ColumnInfo(name="account",   type_text="string", type_name=ColumnTypeName.STRING, position=1, nullable=False),
            ColumnInfo(name="amount",    type_text="decimal(18,2)", type_name=ColumnTypeName.DECIMAL, position=2, nullable=False),
            ColumnInfo(name="posted_at", type_text="timestamp", type_name=ColumnTypeName.TIMESTAMP, position=3, nullable=False),
        ],
    )

    # Functions on finance
    post("/functions", {"function_info": {
        "name": "to_usd_cents", "catalog_name": "prod_analytics", "schema_name": "finance",
        "routine_definition": "RETURN CAST(amount * 100 AS BIGINT)",
        "routine_body": "SQL", "data_type": "BIGINT", "full_data_type": "BIGINT",
        "is_deterministic": True, "comment": "Convert dollar amount to integer cents",
    }})
    post("/functions", {"function_info": {
        "name": "fiscal_quarter", "catalog_name": "prod_analytics", "schema_name": "finance",
        "routine_definition": "RETURN CONCAT('Q', CAST(((MONTH(d) - 1) / 3) + 1 AS STRING))",
        "routine_body": "SQL", "data_type": "STRING", "full_data_type": "STRING",
        "is_deterministic": True, "comment": "Map a date to its fiscal quarter label",
    }})
    post("/functions", {"function_info": {
        "name": "score_tx", "catalog_name": "prod_analytics", "schema_name": "finance",
        "routine_definition": "import math\nreturn math.tanh(amount / 1000.0)",
        "routine_body": "EXTERNAL", "external_language": "PYTHON",
        "data_type": "DOUBLE", "full_data_type": "DOUBLE",
        "is_deterministic": True, "comment": "Anomaly score for a transaction",
    }})
    ok("schema finance: 1 table + 3 functions (2 SQL, 1 Python EXTERNAL)")

    # Schema: ml — models + volumes for ML artifacts
    w.schemas.create(name="ml", catalog_name="prod_analytics", comment="ML model registry + features")
    vol_features = w.volumes.create(
        catalog_name="prod_analytics", schema_name="ml", name="feature_store",
        volume_type=VolumeType.MANAGED, comment="Daily feature snapshots",
    )
    w.volumes.create(
        catalog_name="prod_analytics", schema_name="ml", name="training_data",
        volume_type=VolumeType.MANAGED, comment="Raw training inputs",
    )
    w.volumes.create(
        catalog_name="prod_analytics", schema_name="ml", name="external_share",
        volume_type=VolumeType.EXTERNAL, storage_location="s3://partner-bucket/shared/",
        comment="Read-only mount of partner-supplied data",
    )

    # Models with real artifacts uploaded + versions in different states
    fraud_model = post("/models", {
        "name": "fraud_detector", "catalog_name": "prod_analytics", "schema_name": "ml",
        "comment": "Production fraud-detection classifier",
    })
    churn_model = post("/models", {
        "name": "churn_predictor", "catalog_name": "prod_analytics", "schema_name": "ml",
        "comment": "Subscriber churn risk model",
    })

    # Train a couple tiny sklearn models and upload the actual pickles —
    # so the storage_location prefix has real bytes the user can browse.
    import numpy as np
    admin_s3 = boto3.client(
        "s3", endpoint_url=HOST,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    def upload_version(model: dict, version_n: int, model_obj) -> None:
        loc = model["storage_location"]
        bucket = loc[len("s3://"):].split("/", 1)[0]
        prefix = loc[len("s3://"):].split("/", 1)[1]
        artifact_key = f"{prefix}{version_n}/model.pkl"
        admin_s3.put_object(Bucket=bucket, Key=artifact_key, Body=pickle.dumps(model_obj))
        post(f"/models/prod_analytics.ml.{model['name']}/versions", {
            "model_name": model["name"], "catalog_name": "prod_analytics", "schema_name": "ml",
            "source": f"s3://{bucket}/{prefix}{version_n}/",
            "run_id": f"mlflow-run-{model['name']}-{version_n}",
            "description": f"v{version_n} trained {version_n*7}d ago",
        })

    X = np.array([[i, i * 2] for i in range(10)] + [[100 + i, 50 + i] for i in range(10)])
    y = np.array([0] * 10 + [1] * 10)
    fraud_v1 = LogisticRegression().fit(X, y)
    fraud_v2 = LogisticRegression(C=0.5).fit(X, y)
    fraud_v3 = LogisticRegression(C=0.1).fit(X, y)
    upload_version(fraud_model, 1, fraud_v1)
    upload_version(fraud_model, 2, fraud_v2)
    upload_version(fraud_model, 3, fraud_v3)
    # Promote v1 + v2 to READY, leave v3 PENDING_REGISTRATION
    patch("/models/prod_analytics.ml.fraud_detector/versions/1", {"status": "READY"})
    patch("/models/prod_analytics.ml.fraud_detector/versions/2", {"status": "READY"})

    churn_v1 = LogisticRegression().fit(X, y)
    upload_version(churn_model, 1, churn_v1)
    patch("/models/prod_analytics.ml.churn_predictor/versions/1", {"status": "READY"})
    ok("schema ml: 3 volumes, 2 models, 4 versions (3 READY, 1 PENDING)")

    # Use the feature_store volume for some real CSVs via STS
    creds = requests.post(
        f"{HOST}/api/2.1/unity-catalog/temporary-volume-credentials",
        json={"volume_id": vol_features.volume_id, "operation": "READ_WRITE"},
        headers={"Authorization": f"Bearer {TOKEN}"},
    ).json()["aws_temp_credentials"]
    vol_s3 = boto3.client(
        "s3", endpoint_url=HOST,
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        aws_session_token=creds["session_token"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    vol_loc = vol_features.storage_location
    bucket = vol_loc[len("s3://"):].split("/", 1)[0]
    prefix = vol_loc[len("s3://"):].split("/", 1)[1]
    vol_s3.put_object(Bucket=bucket, Key=f"{prefix}snapshot_2026-05-04.csv",
                      Body=b"user_id,score,risk_band\n1,0.92,HIGH\n2,0.31,LOW\n")
    vol_s3.put_object(Bucket=bucket, Key=f"{prefix}snapshot_2026-05-03.csv",
                      Body=b"user_id,score,risk_band\n1,0.87,HIGH\n2,0.28,LOW\n")
    vol_s3.put_object(Bucket=bucket, Key=f"{prefix}README.md",
                      Body=b"# feature_store\nDaily snapshots of model input features.\n")
    ok("uploaded 3 real files to feature_store volume via vended STS")

    # =========================================================
    # Catalog 2: dev_sandbox — minimal exploratory catalog
    # =========================================================
    step("Seed catalog: dev_sandbox")
    reset_catalog(w, "dev_sandbox")
    w.catalogs.create(name="dev_sandbox", comment="Developer experimentation")
    w.schemas.create(name="alice", catalog_name="dev_sandbox", comment="Alice's scratch space")
    w.tables.create(
        name="experiments", catalog_name="dev_sandbox", schema_name="alice",
        table_type=TableType.MANAGED, data_source_format=DataSourceFormat.DELTA, storage_location="",
        columns=[
            ColumnInfo(name="exp_id", type_text="string", type_name=ColumnTypeName.STRING, position=0, nullable=False),
            ColumnInfo(name="notes", type_text="string", type_name=ColumnTypeName.STRING, position=1, nullable=True),
        ],
    )
    post("/functions", {"function_info": {
        "name": "hello", "catalog_name": "dev_sandbox", "schema_name": "alice",
        "routine_definition": "RETURN CONCAT('hi, ', name)",
        "routine_body": "SQL", "data_type": "STRING",
    }})
    ok("schema alice: 1 table + 1 function")

    print("\n[DONE] Seeded — open http://127.0.0.1:9000/_console/ → Unity Catalog")
    print("       prod_analytics.{marketing,finance,ml} + dev_sandbox.alice")
    print("       Tables, Functions, Volumes, Models tabs all populated")
    print("       feature_store volume has 3 real CSV/MD files browsable via S3 API")


if __name__ == "__main__":
    main()
