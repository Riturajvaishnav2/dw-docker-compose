#!/bin/bash
#
# Create Iceberg Catalog with All Tables - WORKING VERSION
# Forces migrations to apply even if iceberg-migrate says "no pending revisions"
#
# Usage: docker compose exec iceberg-alembic /app/scripts/create_schema_final.sh
# The catalog name is read from ICEBERG_NAMESPACE environment variable (defaults to "ee")
# Example:
#   ICEBERG_NAMESPACE=acme docker compose exec iceberg-alembic /app/scripts/create_schema_final.sh
#   OR if ICEBERG_NAMESPACE is already set:
#   docker compose exec iceberg-alembic /app/scripts/create_schema_final.sh

set -e

# Read catalog name from environment variable, default to "ee"
CATALOG_NAME="${ICEBERG_NAMESPACE:-ee}"

if ! [[ "$CATALOG_NAME" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Invalid catalog name: $CATALOG_NAME"
    exit 1
fi

export ICEBERG_NAMESPACE="$CATALOG_NAME"
NAMESPACE="$CATALOG_NAME"

echo "Creating $CATALOG_NAME catalog with all migration tables..."
echo ""

# Method: Force migrations by downgrading then upgrading
# This works even if iceberg-migrate says "no pending revisions"

echo "Step 1: Initializing migrations..."
# Try downgrade to base, then upgrade - forces re-application
iceberg-migrate downgrade base 2>/dev/null || true

echo "Step 2: Applying all migrations..."
iceberg-migrate upgrade head

echo ""
echo "✅ $CATALOG_NAME catalog created successfully!"
echo ""
echo "Summary:"
echo "  • Catalog: $CATALOG_NAME"
echo "  • Tables created in namespaces: bronze, silver, gold"
echo ""
echo "Tables in namespace:"
echo "  • bronze.settlement"
echo "  • bronze.activation"
echo "  • bronze.rating"
echo "  • bronze.imsi_level_traffic"
echo "  • silver.fact_imsi_level_traffic"
echo "  • silver.dim_* (14 dimension tables)"
echo "  • gold.settlement"
echo "  • gold.imsi_level_traffic_daily"
echo "  • gold.client_partner_traffic_monthly"
echo ""
echo "Verify: docker compose exec trino trino --execute \"SHOW TABLES FROM $CATALOG_NAME.bronze;\""
