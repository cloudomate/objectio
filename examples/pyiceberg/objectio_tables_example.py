"""
ObjectIO Tables — PyIceberg Example

Demonstrates fundamental operations with ObjectIO Iceberg Catalog using PyIceberg:
- Connecting to an ObjectIO Tables catalog with SigV4 authentication
- Loading an existing table
- Appending data to a table
- Running various types of queries

Prerequisites:
- ObjectIO gateway running with Iceberg REST Catalog enabled
- A warehouse, namespace, and table already created (via console or API)
- PyIceberg and PyArrow installed: pip install pyiceberg pyarrow pandas

Setup (via ObjectIO console at https://s3.imys.in/_console/):
1. Iceberg Catalog → Create warehouse (e.g., "production")
2. Inside warehouse → Create namespace (e.g., "analytics")
3. Tables are created programmatically via PyIceberg (see create_table below)

Or via curl:
    curl -X POST https://s3.imys.in/_admin/warehouses \\
         -H "Content-Type: application/json" -d '{"name": "production"}'
    curl -X POST https://s3.imys.in/iceberg/v1/namespaces \\
         -H "Content-Type: application/json" -d '{"namespace": ["analytics"]}'
"""

import os

import pandas as pd
import pyarrow as pa
import pyiceberg.catalog.rest
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

# =============================================================================
# Configuration - Update these values for your environment
# =============================================================================

CATALOG_URL = os.environ.get("OBJECTIO_CATALOG_URL", "https://s3.imys.in/iceberg")
HOST = os.environ.get("OBJECTIO_HOST", "https://s3.imys.in")
WAREHOUSE = os.environ.get("OBJECTIO_WAREHOUSE", "warehouse1")
NAMESPACE = os.environ.get("OBJECTIO_NAMESPACE", "nm1")
TABLE_NAME = os.environ.get("OBJECTIO_TABLE", "products")

# ObjectIO credentials (from console → Users & Keys)
# Set these environment variables before running:
#   export AWS_ACCESS_KEY_ID=AKIA...
#   export AWS_SECRET_ACCESS_KEY=...
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

if not ACCESS_KEY or not SECRET_KEY:
    print("Error: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
    print("  export AWS_ACCESS_KEY_ID=AKIA...")
    print("  export AWS_SECRET_ACCESS_KEY=...")
    print("Get credentials from ObjectIO console → Users & Keys")
    exit(1)


def setup_environment(access_key: str, secret_key: str, region: str) -> None:
    """Configure AWS environment variables for S3 data access."""
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_REGION"] = region


def create_catalog(
    catalog_url: str,
    warehouse: str,
    access_key: str,
    secret_key: str,
    host: str,
    region: str,
) -> Catalog:
    """
    Create a PyIceberg REST catalog connected to ObjectIO Tables.

    Uses SigV4 authentication — same access key / secret key as S3.
    ObjectIO returns vended credentials in LoadTable responses for data access.

    Args:
        catalog_url: URL of the ObjectIO Iceberg REST catalog endpoint
        warehouse: Name of the warehouse (created via console or API)
        access_key: ObjectIO access key (from Users & Keys page)
        secret_key: ObjectIO secret key
        host: ObjectIO S3 endpoint URL
        region: Region for SigV4 signing

    Returns:
        Configured RestCatalog instance
    """
    config = {
        # SigV4 authentication for the REST catalog
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3",
        "rest.signing-region": region,
        # S3 configuration for data access
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
        "s3.endpoint": host,
        "s3.path-style-access": "true",
        "s3.region": region,
    }

    catalog = pyiceberg.catalog.rest.RestCatalog(
        name="objectio",
        uri=catalog_url,
        warehouse=warehouse,
        **config,
    )

    print(f"Connected to ObjectIO catalog at {catalog_url}")
    return catalog


def create_table_if_not_exists(
    catalog: Catalog, namespace: str, table_name: str
) -> Table:
    """
    Create the products table if it doesn't exist, or load it.

    Args:
        catalog: PyIceberg catalog instance
        namespace: Target namespace
        table_name: Table name to create

    Returns:
        Table instance
    """
    table_identifier = (namespace, table_name)

    # Check if table exists
    try:
        table = catalog.load_table(table_identifier)
        print(f"Table already exists: {namespace}.{table_name}")
        return table
    except Exception:
        pass

    # Create table with schema
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(
            field_id=3, name="category", field_type=StringType(), required=False
        ),
        NestedField(field_id=4, name="price", field_type=DoubleType(), required=False),
        NestedField(
            field_id=5, name="in_stock", field_type=BooleanType(), required=False
        ),
    )

    table = catalog.create_table(table_identifier, schema=schema)
    print(f"Created table: {namespace}.{table_name}")
    return table


def load_table(catalog: Catalog, namespace: str, table_name: str) -> Table:
    """Load an existing table from the catalog."""
    table_identifier = (namespace, table_name)
    table = catalog.load_table(table_identifier)
    print(f"Loaded table: {namespace}.{table_name}")
    return table


def append_data(table: Table, data: dict) -> None:
    """
    Append data to an Iceberg table.

    Args:
        table: Target Iceberg table
        data: Dictionary with column names as keys and lists as values
    """
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("category", pa.string()),
            pa.field("price", pa.float64()),
            pa.field("in_stock", pa.bool_()),
        ]
    )

    arrow_table = pa.Table.from_pydict(data, schema=schema)
    table.append(arrow_table)

    row_count = len(next(iter(data.values())))
    print(f"Appended {row_count} rows to table")


# =============================================================================
# Query Examples
# =============================================================================


def query_all_rows(table: Table) -> pd.DataFrame:
    """Scan all rows from the table."""
    print("\n--- Query: All Rows ---")
    df = table.scan().to_pandas()
    print(f"Found {len(df)} total rows")
    print(df)
    return df


def query_with_filter(table: Table, filter_expression: str) -> pd.DataFrame:
    """Query rows matching a filter condition."""
    print(f"\n--- Query: Filter '{filter_expression}' ---")
    df = table.scan(row_filter=filter_expression).to_pandas()
    print(f"Found {len(df)} matching rows")
    print(df)
    return df


def query_selected_columns(
    table: Table, columns: tuple, filter_expression: str = None
) -> pd.DataFrame:
    """Query specific columns, optionally with a filter."""
    print(f"\n--- Query: Columns {columns} ---")
    scan = table.scan(selected_fields=columns)
    if filter_expression:
        scan = table.scan(selected_fields=columns, row_filter=filter_expression)
        print(f"    Filter: {filter_expression}")
    df = scan.to_pandas()
    print(f"Found {len(df)} rows")
    print(df)
    return df


def query_with_limit(table: Table, limit: int) -> pd.DataFrame:
    """Query a limited number of rows."""
    print(f"\n--- Query: Limit {limit} rows ---")
    df = table.scan(limit=limit).to_pandas()
    print(f"Retrieved {len(df)} rows")
    print(df)
    return df


def display_table_info(table: Table) -> None:
    """Display basic table metadata and schema."""
    print("\n--- Table Information ---")
    print(f"Name: {table.name()}")
    print(f"Location: {table.location()}")
    print("\nSchema:")
    for field in table.schema().fields:
        nullable = "nullable" if field.optional else "required"
        print(f"  {field.name}: {field.field_type} ({nullable})")


def display_snapshot_info(table: Table) -> None:
    """Display current snapshot information."""
    print("\n--- Snapshot Information ---")
    snapshot = table.current_snapshot()
    if snapshot:
        print(f"Snapshot ID: {snapshot.snapshot_id}")
        print(f"Timestamp: {snapshot.timestamp_ms}")
        if snapshot.summary:
            print(f"Operation: {snapshot.summary.operation}")
    else:
        print("No snapshots available")


# =============================================================================
# Main Example
# =============================================================================


def main():
    """
    Main function demonstrating ObjectIO Tables operations.

    This example will:
    1. Connect to the catalog using SigV4 (same credentials as S3)
    2. Create the table if it doesn't exist
    3. Append sample data
    4. Run various queries
    """
    print("=" * 60)
    print("ObjectIO Tables — PyIceberg Example")
    print("=" * 60)

    # Step 0: Ensure warehouse exists (via authenticated admin API)
    import requests
    session = requests.Session()
    try:
        # Login to get session cookie
        login_resp = session.post(
            f"{HOST}/_console/api/login",
            json={"accessKey": ACCESS_KEY, "secretKey": SECRET_KEY},
            timeout=10,
        )
        if login_resp.ok:
            # Create warehouse
            resp = session.post(
                f"{HOST}/_admin/warehouses",
                json={"name": WAREHOUSE},
                timeout=10,
            )
            if resp.status_code == 200:
                print(f"Created warehouse: {WAREHOUSE}")
            elif "already exists" in resp.text.lower():
                print(f"Warehouse already exists: {WAREHOUSE}")
            else:
                print(f"Warehouse setup: {resp.status_code} {resp.text}")
        else:
            print("Note: Admin login failed. Create warehouse via the console.")
    except Exception as e:
        print(f"Note: Could not auto-create warehouse ({e}). Create it via the console.")

    # Step 1: Configure environment and connect to catalog
    setup_environment(ACCESS_KEY, SECRET_KEY, AWS_REGION)
    catalog = create_catalog(
        CATALOG_URL, WAREHOUSE, ACCESS_KEY, SECRET_KEY, HOST, AWS_REGION
    )

    # Step 2: Ensure namespace exists, then create table
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"Created namespace: {NAMESPACE}")
    except Exception:
        print(f"Namespace already exists: {NAMESPACE}")

    table = create_table_if_not_exists(catalog, NAMESPACE, TABLE_NAME)

    # Step 3: Display table information
    display_table_info(table)

    # Step 4: Append sample data
    sample_data = {
        "id": [101, 102, 103],
        "name": ["Widget A", "Widget B", "Widget C"],
        "category": ["Electronics", "Electronics", "Hardware"],
        "price": [29.99, 49.99, 19.99],
        "in_stock": [True, True, False],
    }

    print("\n" + "=" * 60)
    print("Appending Data")
    print("=" * 60)
    append_data(table, sample_data)

    # Refresh table to see the new snapshot
    table = load_table(catalog, NAMESPACE, TABLE_NAME)

    # Step 5: Run various queries
    print("\n" + "=" * 60)
    print("Query Examples")
    print("=" * 60)

    query_all_rows(table)
    query_with_filter(table, "price > 30")
    query_with_filter(table, "in_stock = true")
    query_with_filter(table, "category = 'Electronics'")
    query_selected_columns(table, ("name", "price"))
    query_selected_columns(
        table, ("name", "price", "in_stock"), "category = 'Electronics'"
    )
    query_with_limit(table, 5)

    # Step 6: Display snapshot info after changes
    print("\n" + "=" * 60)
    print("Snapshot After Append")
    print("=" * 60)
    display_snapshot_info(table)

    print("\n" + "=" * 60)
    print("Example Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
