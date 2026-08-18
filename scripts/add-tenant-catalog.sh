#!/bin/bash
set -e

# ============================================================================
# Add a New Tenant Catalog
# ============================================================================
# Usage: ./scripts/add-tenant-catalog.sh <tenant_name>
# Example: ./scripts/add-tenant-catalog.sh mycompany
#
# This script:
# 1. Validates the tenant name (lowercase, alphanumeric + underscore)
# 2. Updates .env with the new ICEBERG_NAMESPACE
# 3. Generates the Trino catalog configuration
# 4. Restarts services to apply migrations
# ============================================================================

if [ $# -ne 1 ]; then
    echo "Usage: $0 <tenant_name>"
    echo ""
    echo "Examples:"
    echo "  $0 acme_corp      # Create 'acme_corp' catalog"
    echo "  $0 vodafone       # Create 'vodafone' catalog"
    echo ""
    exit 1
fi

TENANT_NAME="$1"

# Normalize to lowercase
TENANT_NAME_LOWER=$(echo "$TENANT_NAME" | tr '[:upper:]' '[:lower:]')

if [ "$TENANT_NAME" != "$TENANT_NAME_LOWER" ]; then
    echo "⚠ WARNING: Tenant name contains uppercase letters."
    echo "  Normalizing: '$TENANT_NAME' → '$TENANT_NAME_LOWER'"
    TENANT_NAME="$TENANT_NAME_LOWER"
fi

echo "=========================================================================="
echo "Adding New Tenant Catalog: $TENANT_NAME"
echo "=========================================================================="
echo ""

# Validate tenant name format
if ! [[ "$TENANT_NAME" =~ ^[a-z][a-z0-9_]*$ ]]; then
    echo "✗ ERROR: Tenant name must start with a letter and contain only lowercase letters, numbers, and underscores"
    echo "  Got: $TENANT_NAME"
    exit 1
fi

echo "✓ Tenant name validation passed"
echo ""

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "✗ ERROR: .env file not found"
    echo "  Please run this script from the project root directory"
    exit 1
fi

# Check if tenant already exists
if grep -q "^ICEBERG_NAMESPACE=$TENANT_NAME$" ".env"; then
    echo "⚠ Note: Tenant '$TENANT_NAME' is already configured in .env"
    echo "  To switch to this tenant, run:"
    echo "    ICEBERG_NAMESPACE=$TENANT_NAME docker compose up -d"
    exit 0
fi

# Show current configuration
echo "Current Configuration:"
CURRENT_NAMESPACE=$(grep "^ICEBERG_NAMESPACE=" .env | cut -d'=' -f2 || echo "NOT SET")
echo "  Current ICEBERG_NAMESPACE: $CURRENT_NAMESPACE"
CATALOG_WAREHOUSE=$(grep "^CATALOG_WAREHOUSE=" .env | cut -d'=' -f2 || echo "NOT SET")
echo "  CATALOG_WAREHOUSE: $CATALOG_WAREHOUSE"
echo ""

# Confirm action
echo "This will:"
echo "  1. Update ICEBERG_NAMESPACE=$TENANT_NAME in .env"
echo "  2. Generate Trino catalog configuration"
echo "  3. Create schemas (bronze, silver, gold) via migrations"
echo "  4. Create all data tables"
echo ""

read -p "Continue? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo "=========================================================================="
echo "Step 1: Updating .env"
echo "=========================================================================="

# Update .env file
if grep -q "^ICEBERG_NAMESPACE=" .env; then
    # Replace existing value
    sed -i "s/^ICEBERG_NAMESPACE=.*/ICEBERG_NAMESPACE=$TENANT_NAME/" .env
    echo "✓ Updated ICEBERG_NAMESPACE in .env"
else
    # Add new line
    echo "ICEBERG_NAMESPACE=$TENANT_NAME" >> .env
    echo "✓ Added ICEBERG_NAMESPACE to .env"
fi

echo ""
echo "=========================================================================="
echo "Step 2: Generating Trino Catalog Configuration"
echo "=========================================================================="

export ICEBERG_NAMESPACE="$TENANT_NAME"
export CATALOG_WAREHOUSE=$(grep "^CATALOG_WAREHOUSE=" .env | cut -d'=' -f2)

if bash scripts/generate-trino-catalogs.sh; then
    echo "✓ Trino catalog configuration generated"
else
    echo "✗ Failed to generate Trino catalog configuration"
    exit 1
fi

echo ""
echo "=========================================================================="
echo "Step 3: Creating Services and Running Migrations"
echo "=========================================================================="
echo ""
echo "Starting Docker services (this may take a minute)..."
echo "  - Iceberg REST catalog will initialize"
echo "  - Alembic will create schemas and tables"
echo ""

ICEBERG_NAMESPACE="$TENANT_NAME" docker compose up -d iceberg-alembic

# Wait for migrations to complete
echo ""
echo "Waiting for migrations to complete..."
max_wait=120
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    if docker compose logs iceberg-alembic 2>/dev/null | grep -q "Container ready"; then
        echo "✓ Migrations completed successfully"
        break
    fi
    echo "  ... still waiting ($elapsed/$max_wait seconds)"
    sleep 5
    elapsed=$((elapsed + 5))
done

echo ""
echo "=========================================================================="
echo "✓ Tenant Catalog Created Successfully!"
echo "=========================================================================="
echo ""
echo "Your new tenant catalog '$TENANT_NAME' is ready:"
echo ""
echo "Access via Trino:"
echo "  docker compose exec trino trino --execute \"SHOW TABLES FROM $TENANT_NAME.bronze\""
echo ""
echo "View configuration:"
echo "  cat conf/trino/catalog/${TENANT_NAME}.properties"
echo ""
echo "S3 warehouse location:"
echo "  s3://$CATALOG_WAREHOUSE/$TENANT_NAME/"
echo ""
echo "Switch back to previous tenant:"
if [ "$CURRENT_NAMESPACE" != "NOT SET" ]; then
    echo "  ICEBERG_NAMESPACE=$CURRENT_NAMESPACE docker compose up -d"
else
    echo "  Update ICEBERG_NAMESPACE in .env and restart services"
fi
echo ""
