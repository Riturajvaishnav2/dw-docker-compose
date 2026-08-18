#!/bin/bash
# Fix migrations then run alembic
# Add to docker-compose entrypoint when needed:
#   entrypoint: /app/iceberg-alembic/scripts/fix-and-migrate.sh
# Remove entrypoint to run normally

set -euo pipefail

echo "=========================================="
echo "Fixing migration versions..."
echo "=========================================="

python3 /app/iceberg-alembic/scripts/fix_migration_versions.py

echo ""
echo "✓ Migration fix completed"
echo ""

# Run original entrypoint
echo "=========================================="
echo "Running alembic migrations..."
echo "=========================================="

exec /entrypoint.sh "$@"
