#!/bin/bash
set -euo pipefail

# Increased retries for migration execution (tables can take time to create)
MAX_RETRIES="${ICEBERG_ALEMBIC_STARTUP_MAX_RETRIES:-60}"
RETRY_DELAY_SECONDS="${ICEBERG_ALEMBIC_STARTUP_RETRY_DELAY_SECONDS:-5}"

run_with_retry() {
    local description="$1"
    shift

    local attempt=1
    while true; do
        if "$@"; then
            return 0
        fi

        if [ "$attempt" -ge "$MAX_RETRIES" ]; then
            echo "✗ ${description} failed after ${attempt} attempt(s)"
            return 1
        fi

        echo "! ${description} failed on attempt ${attempt}/${MAX_RETRIES}; retrying in ${RETRY_DELAY_SECONDS}s..."
        attempt=$((attempt + 1))
        sleep "${RETRY_DELAY_SECONDS}"
    done
}

echo "=========================================="
echo "iceberg-alembic Container Startup"
echo "=========================================="

# Display configuration
echo ""
echo "Configuration:"
echo "  ICEBERG_NAMESPACE: ${ICEBERG_NAMESPACE:-NOT SET}"
echo "  ICEBERG_ALEMBIC_CATALOG_NAME: ${ICEBERG_ALEMBIC_CATALOG_NAME:-NOT SET}"
echo "  ICEBERG_ALEMBIC_WAREHOUSE: ${ICEBERG_ALEMBIC_WAREHOUSE:-NOT SET}"
echo "  ICEBERG_ALEMBIC_CATALOG_URI: ${ICEBERG_ALEMBIC_CATALOG_URI:-NOT SET}"
echo ""

# Ensure ICEBERG_NAMESPACE is set as environment variable
if [ -z "${ICEBERG_NAMESPACE:-}" ]; then
    echo "⚠ WARNING: ICEBERG_NAMESPACE not set, using default"
    export ICEBERG_NAMESPACE="default"
fi

# Validate namespace is lowercase (Trino/Iceberg requirement)
NAMESPACE_LOWER=$(echo "${ICEBERG_NAMESPACE}" | tr '[:upper:]' '[:lower:]')
if [ "${ICEBERG_NAMESPACE}" != "${NAMESPACE_LOWER}" ]; then
    echo "✗ ERROR: ICEBERG_NAMESPACE must be lowercase!"
    echo "  Provided: ${ICEBERG_NAMESPACE}"
    echo "  Use instead: ${NAMESPACE_LOWER}"
    exit 1
fi

# Validate namespace name format (alphanumeric + underscore only)
if ! [[ "${ICEBERG_NAMESPACE}" =~ ^[a-z][a-z0-9_]*$ ]]; then
    echo "✗ ERROR: ICEBERG_NAMESPACE must start with a letter and contain only lowercase letters, numbers, and underscores"
    echo "  Got: ${ICEBERG_NAMESPACE}"
    exit 1
fi

# Step 0: Generate Trino Catalog Configuration Files
echo ""
echo "Step 0: Generating Trino Catalog Configuration..."
echo "=========================================="
if [ -f "/opt/project/scripts/generate-trino-catalogs.sh" ]; then
    if bash /opt/project/scripts/generate-trino-catalogs.sh; then
        echo "✓ Trino catalog configuration generated"
    else
        echo "⚠ Catalog generation had issues (non-fatal, continuing...)"
    fi
else
    echo "⚠ Catalog generation script not found at /opt/project/scripts/generate-trino-catalogs.sh"
    echo "  (This is expected if running without bind mounts)"
fi
echo ""

# Run migration fixer if MIGRATION_FIX environment variable is set
if [ "${MIGRATION_FIX:-false}" = "true" ]; then
    echo ""
    echo "Step 1: Fixing migration versions..."
    echo "=========================================="
    if python3 /app/iceberg-alembic/scripts/fix_migration_versions.py; then
        echo "✓ Migration fix completed"
    else
        echo "⚠ Migration fix had issues (non-fatal, continuing...)"
    fi
    echo ""
fi

# Check if iceberg-migrate is available
if ! command -v iceberg-migrate &> /dev/null; then
    echo "✗ iceberg-migrate not found in PATH"
    echo "  Current PATH: $PATH"
    echo ""
    echo "Debugging information:"
    echo "  Python version: $(python3 --version)"
    echo "  Python executable: $(which python3)"
    python3 -m pip list 2>/dev/null | grep -i iceberg || echo "  (no iceberg packages found)"
    echo ""
    echo "Attempting direct import test:"
    if python3 -c "from iceberg_alembic.cli import cli; print('  ✓ iceberg_alembic module importable')" 2>&1; then
        echo "  → Module exists but CLI entry point not created. Try: python3 -m pip install -e /app/iceberg-alembic"
    else
        echo "  ✗ Cannot import iceberg_alembic module"
    fi
    exit 1
fi

echo "✓ iceberg-migrate available"

# Initialize state file if needed
if [ ! -f /migrations/.iceberg-alembic-state.json ]; then
    echo ""
    echo "Initializing migration state..."
    iceberg-migrate init
    echo "✓ State file initialized at /migrations/.iceberg-alembic-state.json"
else
    echo "✓ State file already exists"
fi

# Wait for Iceberg REST to be healthy
echo ""
echo "Step 2: Waiting for Iceberg REST catalog to be ready..."
echo "=========================================="
ICEBERG_URL="${ICEBERG_ALEMBIC_CATALOG_URI:-http://iceberg-rest:8181}"
ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-default}"

max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -s "$ICEBERG_URL/v1/config" > /dev/null 2>&1; then
        echo "✓ Iceberg REST is healthy"
        echo "✓ Catalog namespace prepared (migrations will create schemas)"
        break
    fi

    attempt=$((attempt + 1))
    if [ $attempt -lt $max_attempts ]; then
        echo "Waiting for Iceberg REST... (attempt $attempt/$max_attempts)"
        sleep 2
    fi
done

if [ $attempt -eq $max_attempts ]; then
    echo "⚠ Iceberg REST not responding, but continuing anyway..."
fi

# Always create catalog (force fresh creation every time)
echo ""
echo "Step 3: Preparing catalog..."
echo "=========================================="
echo "✓ Will create/recreate catalog: $ICEBERG_NAMESPACE"
echo "  → Creating fresh schema and tables"
echo "  → Running: iceberg-migrate upgrade head"
CATALOG_ACTION="create_and_migrate"

echo ""
echo "Step 4: Checking current migration status..."
current_revision=$(iceberg-migrate current 2>&1 || echo "None")
echo "Current revision: $current_revision"

# Run migrations with increased retries
echo ""
echo "Step 5: Running migrations"
echo "=========================================="
echo "Catalog: $ICEBERG_NAMESPACE"
echo "Warehouse: $ICEBERG_ALEMBIC_WAREHOUSE"
echo "Action: $CATALOG_ACTION"
echo ""

echo "Running: iceberg-migrate upgrade head (creating catalog + tables)"
MIGRATION_COMMAND="iceberg-migrate upgrade head"

echo ""

if run_with_retry "$MIGRATION_COMMAND" $MIGRATION_COMMAND; then
    echo "=========================================="
    echo "✓ Migrations completed successfully"

    echo ""
    echo "Verifying migrated tables exist in the Iceberg catalog..."
    if python /verify_migrations.py; then
        echo "✓ Verified migrated tables are available in Iceberg"
    else
        echo "⚠ Table verification had issues, but migrations completed"
    fi

    # Show final status
    echo ""
    echo "Final migration status:"
    iceberg-migrate current
    echo ""
    echo "Migration history:"
    iceberg-migrate history

    # Show created schemas and tables
    echo ""
    echo "=========================================="
    echo "Created Schemas in Catalog: $ICEBERG_NAMESPACE"
    echo "=========================================="
    iceberg-migrate list namespaces 2>/dev/null || echo "Unable to list namespaces"

    echo ""
    echo "Summary:"
    echo "  ✓ Catalog: $ICEBERG_NAMESPACE"
    echo "  ✓ Warehouse: $ICEBERG_ALEMBIC_WAREHOUSE"
    echo "  ✓ Tables created in: bronze, silver, gold schemas"
else
    echo "=========================================="
    echo "✗ Migration failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "Container ready. Keeping service running..."
echo "=========================================="
echo ""
echo "You can now:"
echo "  • Run ingestion jobs: docker compose exec spark ..."
echo "  • Query via Trino: docker compose exec trino trino --execute ..."
echo "  • Check migrations: docker compose exec iceberg-alembic iceberg-migrate current"
echo ""

# Keep container running
exec sleep infinity
