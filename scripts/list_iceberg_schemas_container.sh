#!/bin/bash
#
# List Iceberg Schemas - Container Version
# Use this script when running INSIDE the docker container
# Shows all schemas and their tables
#
# Usage: ./list_iceberg_schemas_container.sh

set -e

echo "=========================================="
echo "📊 Iceberg Schemas (Container Version)"
echo "=========================================="
echo ""

# Check if iceberg-migrate command exists
if ! command -v iceberg-migrate &> /dev/null; then
    echo "❌ Error: iceberg-migrate command not found"
    echo ""
    echo "Make sure you're running this inside the iceberg-alembic container"
    exit 1
fi

echo "🔍 Getting current migration status..."
echo ""

# Show current namespace
if [ ! -z "$ICEBERG_NAMESPACE" ]; then
    echo "Current namespace: $ICEBERG_NAMESPACE"
    echo ""
fi

# Show current migration info
echo "📋 Current Migration Status:"
echo "  Current version: $(iceberg-migrate current 2>/dev/null || echo 'None')"
echo "  Migration heads:"
iceberg-migrate heads 2>/dev/null | head -5
echo ""

echo "📚 Migration History:"
echo ""
iceberg-migrate history 2>/dev/null || echo "  (No migrations applied yet)"

echo ""
echo "=========================================="
echo "✨ Migration Information"
echo "=========================================="
echo ""

echo "To view schemas in Trino, exit this container and run:"
echo "  docker compose exec trino trino --execute \"SHOW SCHEMAS FROM iceberg;\""
echo ""
echo "To view tables in a specific schema:"
echo "  docker compose exec trino trino --execute \"SHOW TABLES FROM iceberg.<namespace>;\""
echo ""
