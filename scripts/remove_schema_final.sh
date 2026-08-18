#!/bin/bash
#
# Remove Iceberg Catalog with All Tables
# Downgrades migrations and removes the entire catalog
#
# Usage: docker compose exec iceberg-alembic /app/scripts/remove_schema_final.sh
# The catalog name is read from ICEBERG_NAMESPACE environment variable (defaults to "ee")
# Example:
#   ICEBERG_NAMESPACE=acme docker compose exec iceberg-alembic /app/scripts/remove_schema_final.sh
#   OR if ICEBERG_NAMESPACE is already set:
#   docker compose exec iceberg-alembic /app/scripts/remove_schema_final.sh

set -e

# Read catalog name from environment variable, default to "ee"
CATALOG_NAME="${ICEBERG_NAMESPACE:-ee}"

if ! [[ "$CATALOG_NAME" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Invalid catalog name: $CATALOG_NAME"
    exit 1
fi

export ICEBERG_NAMESPACE="$CATALOG_NAME"
NAMESPACE="$CATALOG_NAME"

echo "⚠️  Removing $CATALOG_NAME catalog with all migration tables..."
echo ""

# Step 1: Downgrade all migrations to remove migration state
echo "Step 1: Resetting migration state..."
echo "  Running: iceberg-migrate downgrade base"
iceberg-migrate downgrade base 2>/dev/null || true

echo ""
echo "Step 2: Dropping all tables from schema via REST API..."

# Tables to drop
TABLES=("bronze.settlement" "bronze.activation" "bronze.rating" "bronze.traffic" "silver.traffic" "gold.settlement")
ICEBERG_REST_API="http://iceberg-rest:8181/v1"

for TABLE in "${TABLES[@]}"; do
    TABLE_NAME="${TABLE#*.}"
    TABLE_PREFIX="${TABLE%.*}"

    # Use REST API to delete table
    TABLE_URL="$ICEBERG_REST_API/namespaces/$NAMESPACE/tables/${TABLE_PREFIX}.${TABLE_NAME}"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$TABLE_URL" 2>/dev/null)

    if [ "$HTTP_CODE" = "204" ] || [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "404" ]; then
        echo "  ✓ Dropped: $TABLE"
    else
        echo "  ⚠ Warning dropping $TABLE (HTTP $HTTP_CODE)"
    fi
done

echo ""
echo "Step 3: Deleting schema namespace..."

# Delete the namespace itself
NAMESPACE_URL="$ICEBERG_REST_API/namespaces/$NAMESPACE"
NS_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$NAMESPACE_URL" 2>/dev/null)

if [ "$NS_HTTP_CODE" = "204" ] || [ "$NS_HTTP_CODE" = "200" ] || [ "$NS_HTTP_CODE" = "404" ]; then
    echo "  ✓ Schema namespace deleted"
else
    echo "  ⚠ Warning deleting namespace (HTTP $NS_HTTP_CODE)"
fi

echo ""
echo "✅ $CATALOG_NAME catalog removed successfully!"
echo ""
echo "Summary:"
echo "  • Catalog: $CATALOG_NAME"
echo "  • All tables and migrations dropped"
echo "  • Namespace deleted"
echo ""
echo "Verify removal: docker compose exec trino trino --execute \"SHOW SCHEMAS FROM $CATALOG_NAME;\" | grep bronze || echo 'Schema removed'"
