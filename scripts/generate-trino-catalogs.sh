#!/bin/bash

# ============================================================================
# Generate Trino Catalog Configuration Files Dynamically
# ============================================================================
# This script generates Trino catalog .properties files based on
# ICEBERG_NAMESPACE environment variable
# ============================================================================

set -e

# Get namespace from environment
NAMESPACE="${ICEBERG_NAMESPACE:-default}"

# Use relative path if running on host, otherwise use container path
if [ -d "/opt/project/conf/trino/catalog" ] 2>/dev/null; then
    # Running inside Docker container
    CATALOG_DIR="${CATALOG_DIR:-/opt/project/conf/trino/catalog}"
else
    # Running on host (relative to project root)
    CATALOG_DIR="${CATALOG_DIR:-conf/trino/catalog}"
fi

echo "=========================================================================="
echo "Generating Trino Catalog Configuration Files"
echo "=========================================================================="
echo "Namespace: $NAMESPACE"
echo "Catalog Directory: $CATALOG_DIR"
echo ""

# Validate inputs
if [ -z "$NAMESPACE" ]; then
    echo "ERROR: ICEBERG_NAMESPACE not set"
    exit 1
fi

if [ -z "$CATALOG_WAREHOUSE" ]; then
    echo "ERROR: CATALOG_WAREHOUSE not set"
    exit 1
fi

# Validate namespace is lowercase (Trino/Iceberg requirement)
if [ "$NAMESPACE" != "$(echo "$NAMESPACE" | tr '[:upper:]' '[:lower:]')" ]; then
    echo "ERROR: ICEBERG_NAMESPACE must be lowercase (got: $NAMESPACE)"
    echo "  Hint: Use '$(echo "$NAMESPACE" | tr '[:upper:]' '[:lower:]')' instead"
    exit 1
fi

# Validate namespace name format (alphanumeric + underscore only)
if ! [[ "$NAMESPACE" =~ ^[a-z][a-z0-9_]*$ ]]; then
    echo "ERROR: ICEBERG_NAMESPACE must start with a letter and contain only lowercase letters, numbers, and underscores"
    echo "  Got: $NAMESPACE"
    exit 1
fi

if [ ! -d "$CATALOG_DIR" ]; then
    echo "Creating catalog directory: $CATALOG_DIR"
    mkdir -p "$CATALOG_DIR"
fi

# Clean up old tenant catalog files (except system catalogs)
# Keep: iceberg.properties, clickhouse.properties (system catalogs)
# Remove: all other tenant-specific catalogs (except the current NAMESPACE)
echo "Cleaning up old tenant catalog files..."
for file in "${CATALOG_DIR}"/*.properties; do
    filename=$(basename "$file")
    basename_no_ext="${filename%.properties}"

    # Keep system catalogs
    if [ "$basename_no_ext" = "iceberg" ] || [ "$basename_no_ext" = "clickhouse" ]; then
        continue
    fi

    # Keep current namespace catalog
    if [ "$basename_no_ext" = "$NAMESPACE" ]; then
        continue
    fi

    # Remove old tenant catalog
    if [ -f "$file" ]; then
        echo "  ✗ Removing old catalog: $filename"
        rm -f "$file"
    fi
done
echo "✓ Old tenant catalogs cleaned up"
echo ""

# Generate catalog properties file
CATALOG_FILE="${CATALOG_DIR}/${NAMESPACE}.properties"

echo "Generating: $CATALOG_FILE"

cat > "$CATALOG_FILE" << EOFCATALOG
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=\${ENV:ICEBERG_CATALOG_URI}
iceberg.rest-catalog.warehouse=s3://\${ENV:CATALOG_WAREHOUSE}
iceberg.rest-catalog.view-endpoints-enabled=false
iceberg.rest-catalog.nested-namespace-enabled=false
fs.native-s3.enabled=true
s3.endpoint=\${ENV:OCI_S3_ENDPOINT}
s3.path-style-access=true
s3.region=\${ENV:OCI_REGION}
s3.aws-access-key=\${ENV:AWS_ACCESS_KEY_ID}
s3.aws-secret-key=\${ENV:AWS_SECRET_ACCESS_KEY}
EOFCATALOG

echo "✓ Generated: ${NAMESPACE}.properties"
echo ""

# Display generated file content
echo "Content of ${NAMESPACE}.properties:"
echo "---"
cat "$CATALOG_FILE"
echo "---"
echo ""

# List all generated catalogs
echo "Available Trino Catalogs:"
ls -1 "${CATALOG_DIR}"/*.properties | xargs -I {} basename {} .properties | sed 's/^/  ✓ /'
echo ""

echo "=========================================================================="
echo "✓ Trino Catalog Configuration Generation Complete"
echo "=========================================================================="
