#!/bin/bash

# ============================================================================
# Test Dynamic Trino Catalog Generation
# ============================================================================

set -e

echo "=========================================================================="
echo "Testing Dynamic Trino Catalog Generation"
echo "=========================================================================="
echo ""

# Simulate different namespace values
NAMESPACES=("orange" "ee" "vodafone" "default")

for NAMESPACE in "${NAMESPACES[@]}"; do
    echo "Test Case: ICEBERG_NAMESPACE=$NAMESPACE"
    echo "---"

    # Simulate environment
    export ICEBERG_NAMESPACE=$NAMESPACE
    export CATALOG_WAREHOUSE="warehouse_dev"
    export OCI_S3_ENDPOINT="https://test.oci.com"
    export OCI_REGION="uk-london-1"
    export AWS_ACCESS_KEY_ID="test_key"
    export AWS_SECRET_ACCESS_KEY="test_secret"

    # Create temp catalog directory for testing
    TEMP_CATALOG_DIR="/tmp/test_catalogs_$$"
    mkdir -p "$TEMP_CATALOG_DIR"
    export CATALOG_DIR="$TEMP_CATALOG_DIR"

    # Run catalog generation
    if bash /home/rituraj.vaishnav@nextgen.local/projects/datawarehouse/scripts/generate-trino-catalogs.sh > /tmp/test_output_$$ 2>&1; then
        echo "✓ Generation succeeded"

        # Verify file was created
        if [ -f "$TEMP_CATALOG_DIR/${NAMESPACE}.properties" ]; then
            echo "✓ File created: ${NAMESPACE}.properties"

            # Check content
            if grep -q "iceberg.rest-catalog.warehouse=s3://warehouse_dev/${NAMESPACE}" "$TEMP_CATALOG_DIR/${NAMESPACE}.properties"; then
                echo "✓ Correct warehouse path"
            else
                echo "✗ Incorrect warehouse path"
            fi

            if grep -q "connector.name=iceberg" "$TEMP_CATALOG_DIR/${NAMESPACE}.properties"; then
                echo "✓ Iceberg connector configured"
            else
                echo "✗ Iceberg connector not found"
            fi
        else
            echo "✗ File not created"
        fi
    else
        echo "✗ Generation failed"
        cat /tmp/test_output_$$
    fi

    # Cleanup
    rm -rf "$TEMP_CATALOG_DIR" /tmp/test_output_$$
    echo ""
done

echo "=========================================================================="
echo "✓ All Tests Complete"
echo "=========================================================================="
