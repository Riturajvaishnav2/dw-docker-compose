#!/bin/bash
#
# Run Iceberg Migrations - Container Version
# Use this script when running INSIDE the docker container
# Catalog name is read from ICEBERG_NAMESPACE environment variable
# Supports: upgrade, downgrade, current, history, version, init
#
# Usage: ./run_iceberg_migration_container.sh [action]
# The catalog name is read from ICEBERG_NAMESPACE environment variable (defaults to "ee")
# Examples:
#   ICEBERG_NAMESPACE=tt ./run_iceberg_migration_container.sh upgrade
#   ICEBERG_NAMESPACE=acme ./run_iceberg_migration_container.sh current
#   ./run_iceberg_migration_container.sh history   (uses default "ee")

set -e

ACTION="${1:-upgrade}"

# Read catalog name from environment variable, default to "ee"
CATALOG_NAME="${ICEBERG_NAMESPACE:-ee}"

# Validate catalog name
if ! [[ "$CATALOG_NAME" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "❌ Error: Invalid catalog name '$CATALOG_NAME'"
    echo "   Use only: letters, numbers, underscore"
    exit 1
fi

# Validate namespace
if ! [[ "$NAMESPACE" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "❌ Error: Invalid namespace '$NAMESPACE'"
    echo "   Use only: letters, numbers, underscore"
    exit 1
fi

# Validate action
VALID_ACTIONS=("upgrade" "downgrade" "current" "history" "version" "init")
if ! [[ " ${VALID_ACTIONS[@]} " =~ " ${ACTION} " ]]; then
    echo "❌ Error: Invalid action '$ACTION'"
    echo "   Valid actions: ${VALID_ACTIONS[@]}"
    exit 1
fi

echo "=========================================="
echo "🔧 Iceberg Migration Control (Container)"
echo "=========================================="
echo ""
echo "📦 Configuration:"
echo "  Catalog:  $CATALOG_NAME"
echo "  Action:   $ACTION"
echo ""

# Export catalog name for migration
export ICEBERG_NAMESPACE="$CATALOG_NAME"

# Show environment info
echo "🔍 Environment:"
echo "  Catalog URI:   ${ICEBERG_ALEMBIC_CATALOG_URI:-http://iceberg-rest:8181}"
echo "  Warehouse:     ${ICEBERG_ALEMBIC_WAREHOUSE:-s3://${CATALOG_WAREHOUSE}/}"
echo "  S3 Endpoint:   ${ICEBERG_ALEMBIC_PROPERTY_S3_ENDPOINT:-Not set}"
echo ""

case "$ACTION" in
    upgrade)
        echo "⏳ Running: iceberg-migrate upgrade head"
        echo ""
        iceberg-migrate upgrade head
        echo ""
        echo "✅ Upgrade complete!"
        ;;
    downgrade)
        echo "⏳ Running: iceberg-migrate downgrade -1"
        echo ""
        iceberg-migrate downgrade -1
        echo ""
        echo "✅ Downgrade complete!"
        ;;
    current)
        echo "📍 Current migration version:"
        echo ""
        iceberg-migrate current
        ;;
    history)
        echo "📚 Migration history:"
        echo ""
        iceberg-migrate history
        ;;
    version)
        echo "🔖 Version information:"
        echo ""
        iceberg-migrate version
        ;;
    init)
        echo "🔧 Initialize migrations:"
        echo ""
        iceberg-migrate init
        ;;
esac

echo ""
echo "=========================================="
echo "✨ Action Complete!"
echo "=========================================="
echo ""
echo "📊 Catalog: $CATALOG_NAME"
echo "💾 Action:  $ACTION"
echo ""
