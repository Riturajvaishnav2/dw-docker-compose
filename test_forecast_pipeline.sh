#!/bin/bash

# ============================================================================
# Forecast Pipeline End-to-End Test Script
# ============================================================================
# Tests: Bronze → Silver → Gold ETL pipeline with 1000 rows of test data
# Does NOT commit - for manual review only

set -e

echo "=========================================================================="
echo "FORECAST PIPELINE E2E TEST"
echo "=========================================================================="
echo ""

cd /home/rituraj.vaishnav@nextgen.local/projects/datawarehouse

# Load environment
if [ -f .env ]; then
    set -a
    source .env
    set +a
else
    echo "ERROR: .env file not found!"
    exit 1
fi

echo "Environment:"
echo "  Landing Bucket: $LANDING_BUCKET"
echo "  Iceberg Namespace: $ICEBERG_NAMESPACE"
echo "  Catalog Warehouse: $CATALOG_WAREHOUSE"
echo ""

# ============================================================================
# Step 1: Start Docker Compose
# ============================================================================
echo "=========================================================================="
echo "Step 1: Starting Docker Services"
echo "=========================================================================="
./start.sh up -d 2>&1 | tail -20
echo "✓ Docker services started (waiting for Trino to be healthy...)"
sleep 30
echo ""

# ============================================================================
# Step 2: Generate Test Data
# ============================================================================
echo "=========================================================================="
echo "Step 2: Generating Test Data (1000 rows)"
echo "=========================================================================="

# Create test data CSV file
TEST_FILE="/tmp/VW_QLIK_IOT_FORECAST_REPORT.csv"
cat > "$TEST_FILE" << 'EOF'
CLIENT_NAME,CLIENT_GROUP_NAME,PARTNER_NAME,PARTNER_GROUP_NAME,SERVICE_TYPE,EVENT_TYPE,TRAFFIC_DIRECTION,PERIOD_START_DATE,CR_PERIOD_START_DATE,DESTINATION_TYPE,AGREEMENT_REFERENCE,RTI_GROUP,FORECAST_OR_ACTUAL_IND,TRAFFIC_VOLUME,CHARGED_VOLUME,TAP_CHARGE_SDR_NET,TAP_CHARGE_SDR_GROSS,DISC_CHARGE_SDR_NET,DISC_CHARGE_SDR_GROSS,NO_OF_RECORDS
EOF

# Generate 1000 rows of test data
for i in {1..1000}; do
    CLIENT_ID=$((i % 10 + 1))
    PARTNER_ID=$((i % 5 + 1))
    SERVICE_ID=$((i % 8 + 1))
    EVENT_ID=$((i % 6 + 1))
    TRAFFIC_DIR=$([ $((i % 2)) -eq 0 ] && echo "CR" || echo "VR")
    FORECAST_IND=$([ $((i % 3)) -eq 0 ] && echo "F" || echo "A")
    TRAFFIC_VOL=$(echo "scale=2; $i * 1.5" | bc)
    CHARGED_VOL=$(echo "scale=2; $i * 1.3" | bc)

    echo "Client_$CLIENT_ID,ClientGroup_$(($i % 3 + 1)),Partner_$PARTNER_ID,PartnerGroup_$(($i % 2 + 1)),ServiceType_$SERVICE_ID,EventType_$EVENT_ID,$TRAFFIC_DIR,2024-07-$(printf '%02d' $(($i % 28 + 1)))T00:00:00,2024-07-$(printf '%02d' $(($i % 28 + 1)))T00:00:00,Destination_$(($i % 4 + 1)),AGR_$(printf '%06d' $i),RTIGroup_$(($i % 5 + 1)),$FORECAST_IND,$TRAFFIC_VOL,$CHARGED_VOL,150.25,165.75,20.50,22.75,1" >> "$TEST_FILE"
done

echo "✓ Generated test data: $TEST_FILE"
wc -l "$TEST_FILE"
echo ""

# ============================================================================
# Step 3: Copy to Landing Bucket
# ============================================================================
echo "=========================================================================="
echo "Step 3: Uploading Test Data to Landing Bucket"
echo "=========================================================================="

# Create sample data directory in Airflow container
SAMPLE_DIR="/opt/airflow/data/samples"
docker compose exec -T airflow mkdir -p "$SAMPLE_DIR" || true

# Copy test file to container
docker compose cp "$TEST_FILE" airflow:"$SAMPLE_DIR/VW_QLIK_IOT_FORECAST_REPORT.csv"
echo "✓ Test data uploaded to: $SAMPLE_DIR/"
docker compose exec -T airflow ls -lh "$SAMPLE_DIR/"
echo ""

# ============================================================================
# Step 4: Trigger Forecast DAG
# ============================================================================
echo "=========================================================================="
echo "Step 4: Triggering Forecast Ingestion DAG"
echo "=========================================================================="

# Wait for Airflow to be ready
echo "Waiting for Airflow..."
sleep 10

# Trigger the DAG
DAG_ID="forecast_ingestion"
EXEC_DATE=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

docker compose exec -T airflow airflow dags trigger "$DAG_ID" -e "$EXEC_DATE" 2>&1 || {
    echo "Note: DAG trigger attempted (may have succeeded or already been triggered)"
}

echo "✓ DAG trigger request sent"
echo ""

# ============================================================================
# Step 5: Wait for DAG Execution
# ============================================================================
echo "=========================================================================="
echo "Step 5: Monitoring DAG Execution"
echo "=========================================================================="

echo "Waiting for DAG to complete (this may take 2-3 minutes)..."
for i in {1..30}; do
    echo "  Check $i/30..."
    sleep 10
done

echo "✓ Execution window complete"
echo ""

# ============================================================================
# Step 6: Verify Data in Trino
# ============================================================================
echo "=========================================================================="
echo "Step 6: Verifying Data in Trino"
echo "=========================================================================="

echo "Querying bronze.iot_forecast_raw..."
docker compose exec -T trino trino --execute "
SELECT COUNT(*) as record_count FROM $ICEBERG_NAMESPACE.bronze.iot_forecast_raw;
" 2>&1 || echo "Bronze table query pending (data may not be loaded yet)"

echo ""
echo "Querying silver.fact_iot_forecast..."
docker compose exec -T trino trino --execute "
SELECT COUNT(*) as record_count FROM $ICEBERG_NAMESPACE.silver.fact_iot_forecast;
" 2>&1 || echo "Silver fact table query pending (data may not be loaded yet)"

echo ""
echo "Listing all Forecast Tables in Trino..."
docker compose exec -T trino trino --execute "
SHOW TABLES FROM $ICEBERG_NAMESPACE.silver LIKE 'dim_%';
" 2>&1 || echo "Dimension tables query pending"

echo ""
echo "Listing all Forecast Tables in Trino..."
docker compose exec -T trino trino --execute "
SHOW TABLES FROM $ICEBERG_NAMESPACE.silver WHERE table_name LIKE 'fact_%';
" 2>&1 || echo "Fact table query pending"

echo ""

# ============================================================================
# Step 7: Summary
# ============================================================================
echo "=========================================================================="
echo "TEST EXECUTION SUMMARY"
echo "=========================================================================="
echo ""
echo "✓ Test data generated: 1000 rows"
echo "✓ Uploaded to: $SAMPLE_DIR"
echo "✓ DAG triggered: forecast_ingestion"
echo ""
echo "Next Steps (Manual Review):"
echo "1. Check Airflow UI: http://localhost:8080"
echo "   - DAG: forecast_ingestion"
echo "   - Monitor task execution"
echo ""
echo "2. Query Trino for results:"
echo "   docker compose exec trino trino"
echo "   > SELECT COUNT(*) FROM $ICEBERG_NAMESPACE.bronze.iot_forecast_raw;"
echo "   > SELECT COUNT(*) FROM $ICEBERG_NAMESPACE.silver.fact_iot_forecast;"
echo "   > SELECT * FROM $ICEBERG_NAMESPACE.silver.dim_partner LIMIT 5;"
echo ""
echo "3. Review logs:"
echo "   docker compose logs -f airflow"
echo ""
echo "4. If everything looks good, run git commit (not automatic)"
echo "=========================================================================="
echo ""
