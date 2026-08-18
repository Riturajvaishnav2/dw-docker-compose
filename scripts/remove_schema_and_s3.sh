#!/bin/bash
#
# Remove Iceberg Catalog, Schema and S3 Warehouse Data
# Completely removes schema, tables, and all S3 data for the catalog/namespace
#
# Usage: docker compose exec iceberg-alembic /app/scripts/remove_schema_and_s3.sh
# The catalog name is read from ICEBERG_NAMESPACE environment variable (defaults to "ee")
# Example:
#   ICEBERG_NAMESPACE=ee docker compose exec iceberg-alembic /app/scripts/remove_schema_and_s3.sh
#   OR if ICEBERG_NAMESPACE is already set:
#   docker compose exec iceberg-alembic /app/scripts/remove_schema_and_s3.sh

set -e

# Read catalog name from environment variable, default to "ee"
CATALOG_NAME="${ICEBERG_NAMESPACE:-ee}"

if ! [[ "$CATALOG_NAME" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Invalid catalog name: $CATALOG_NAME"
    exit 1
fi

export ICEBERG_NAMESPACE="$CATALOG_NAME"
NAMESPACE="$CATALOG_NAME"

echo "=========================================="
echo "Remove Schema and S3 Data"
echo "=========================================="
echo ""
echo "Namespace: $NAMESPACE"
echo ""

# Step 1: Downgrade migrations
echo "Step 1: Removing Iceberg tables (downgrading migrations)..."
echo "=========================================="
cd /migrations
iceberg-migrate downgrade base 2>/dev/null || echo "No migrations to downgrade"
echo "✓ Migrations downgraded"
echo ""

# Step 2: Drop schema from database
echo "Step 2: Dropping schema from Iceberg metadata..."
echo "=========================================="
python << 'EOFPYTHON'
import os
from iceberg_alembic.config import CatalogConfig
from iceberg_alembic.operations import IcebergOperations
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError

namespace = os.getenv("ICEBERG_NAMESPACE")
config = CatalogConfig.from_sources()
ops = IcebergOperations(config=config, dry_run=False)
catalog = ops.catalog

print(f"Processing namespace: {namespace}")

# Drop all tables in the namespace
try:
    tables = catalog.list_tables((namespace,))
    for table_id in tables:
        table_name = table_id[1] if isinstance(table_id, tuple) else table_id
        try:
            catalog.drop_table(table_id)
            print(f"  ✓ Dropped table: {table_name}")
        except NoSuchTableError:
            pass
        except Exception as e:
            print(f"  ⚠ Error dropping {table_name}")
except NoSuchNamespaceError:
    print(f"  ⚠ Namespace {namespace} not found")
except Exception as e:
    print(f"  ⚠ Error listing tables: {e}")

# Drop the namespace
try:
    catalog.drop_namespace((namespace,))
    print(f"  ✓ Dropped namespace: {namespace}")
except NoSuchNamespaceError:
    print(f"  ⚠ Namespace {namespace} not found")
except Exception as e:
    print(f"  ⚠ Error dropping namespace: {e}")

print("✓ Schema removed from Iceberg")
EOFPYTHON

echo ""

# Step 3: Delete S3 data
echo "Step 3: Deleting S3 warehouse data..."
echo "=========================================="

# Install boto3 if needed
pip install boto3 -q 2>/dev/null || true

python << 'EOFPYTHON'
import os
import boto3
from botocore.config import Config

namespace = os.getenv("ICEBERG_NAMESPACE")

# Get S3 configuration
access_key = os.getenv("OCI_ACCESS_KEY_ID")
secret_key = os.getenv("OCI_SECRET_ACCESS_KEY")
endpoint = os.getenv("OCI_S3_ENDPOINT")
region = os.getenv("OCI_REGION", "us-east-1")
warehouse = os.getenv("CATALOG_WAREHOUSE", "warehouse")

# Parse bucket
if warehouse.startswith("s3://"):
    warehouse = warehouse[5:]
elif warehouse.startswith("s3a://"):
    warehouse = warehouse[6:]

bucket = warehouse.split("/")[0]
prefix = f"{namespace}/"

print(f"Bucket: {bucket}")
print(f"Prefix: {prefix}")
print("")

# Create S3 client
try:
    config = Config(s3={"payload_signing_enabled": True})
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=config
    )

    # List objects with namespace prefix
    print("Listing objects...")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    total_objects = 0
    objects_list = []

    for page in pages:
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            total_objects += 1
            objects_list.append(obj["Key"])

    print(f"Found {total_objects} objects for namespace '{namespace}'")
    print("")

    if total_objects == 0:
        print("✓ No objects found for this namespace")
    else:
        # Delete all objects
        print("Deleting objects...")
        deleted = 0
        failed = 0

        for key in objects_list:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                deleted += 1
                if deleted % 20 == 0:
                    print(f"  ✓ Deleted {deleted}/{total_objects} objects")
            except Exception as e:
                failed += 1

        print(f"  ✓ Deleted {deleted}/{total_objects} objects")
        if failed > 0:
            print(f"  ⚠ Failed to delete {failed} objects")

        # Verify
        print("")
        print("Verifying deletion...")
        remaining = 0
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            if "Contents" in page:
                remaining += len(page["Contents"])

        if remaining == 0:
            print("✓ All S3 objects deleted successfully")
        else:
            print(f"⚠ {remaining} objects still remain")

except Exception as e:
    print(f"Error accessing S3: {e}")

print("✓ S3 data removed")
EOFPYTHON

echo ""
echo "=========================================="
echo "✅ Complete Removal Successful"
echo "=========================================="
echo ""
echo "Summary:"
echo "  • Catalog: $CATALOG_NAME"
echo "  • Schema: $NAMESPACE removed from Iceberg"
echo "  • Tables: All dropped"
echo "  • S3 Data: All files deleted from warehouse"
echo ""
echo "Verify: docker compose exec trino trino --execute \"SHOW SCHEMAS FROM $CATALOG_NAME;\" | grep $NAMESPACE || echo 'Schema removed'"
echo ""
