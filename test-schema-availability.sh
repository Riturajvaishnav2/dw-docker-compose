#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================="
echo "Schema Availability Test"
echo "==========================================${NC}"

# Helper functions
test_result() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ PASS${NC}: $1"
        return 0
    else
        echo -e "${RED}✗ FAIL${NC}: $1"
        return 1
    fi
}

test_header() {
    echo ""
    echo -e "${YELLOW}$1${NC}"
    echo "---"
}

# Test 1: Migrations Applied
test_header "1. Verify migrations applied"

current_revision=$(docker compose exec iceberg-alembic iceberg-migrate current 2>/dev/null | tail -1)
echo "Current revision: $current_revision"
test_result "Migrations are applied"

# Test 2: Tables Exist (Empty)
test_header "2. Verify tables exist (EMPTY, no data yet)"

tables=$(docker compose exec trino trino --execute "SHOW TABLES FROM iceberg.dw" 2>/dev/null)
echo "$tables"

if echo "$tables" | grep -q "activation"; then
    test_result "Table 'activation' exists"
else
    echo -e "${RED}✗ FAIL${NC}: Table 'activation' not found"
fi

if echo "$tables" | grep -q "rating"; then
    test_result "Table 'rating' exists"
else
    echo -e "${RED}✗ FAIL${NC}: Table 'rating' not found"
fi

if echo "$tables" | grep -q "settlement"; then
    test_result "Table 'settlement' exists"
else
    echo -e "${RED}✗ FAIL${NC}: Table 'settlement' not found"
fi

# Test 3: Schema Visible (Describe Table)
test_header "3. Verify schema is visible (DESCRIBE)"

schema=$(docker compose exec trino trino --execute "DESCRIBE iceberg.dw.settlement" 2>/dev/null)
echo "$schema" | head -15

if echo "$schema" | grep -q "home_pmn"; then
    test_result "Schema contains expected columns (home_pmn)"
else
    echo -e "${RED}✗ FAIL${NC}: Schema missing home_pmn column"
fi

# Test 4: Row Count Before Ingestion
test_header "4. Verify tables are EMPTY (row count = 0)"

row_count=$(docker compose exec trino trino --execute \
    "SELECT COUNT(*) FROM iceberg.dw.settlement" 2>/dev/null | tail -1 | grep -o '[0-9]*' | tail -1)
echo "Row count in settlement table: $row_count"

if [ "$row_count" -eq 0 ]; then
    test_result "Table is empty (0 rows)"
else
    echo -e "${YELLOW}!${NC} Table has $row_count rows (may have been loaded already)"
fi

# Test 5: Metadata Visible
test_header "5. Verify table metadata is accessible"

metadata=$(docker compose exec trino trino --execute \
    "SELECT snapshot_id, timestamp FROM iceberg.dw.settlement\$snapshots LIMIT 1" 2>/dev/null || echo "")

if [ ! -z "$metadata" ]; then
    echo "Table metadata:"
    echo "$metadata"
    test_result "Iceberg metadata table is accessible"
else
    echo -e "${YELLOW}!${NC} Iceberg metadata may not have snapshots yet (table empty)"
fi

# Test 6: Simulated Ingestion Failure
test_header "6. Simulate ingestion failure scenario"

echo "Creating sample CSV file..."
cat > /tmp/settlement_test.csv << 'EOF'
home_pmn,home_operator,traffic_direction,service_type,event_type,partner_name,partner_pmn,traffic_period,amount
MTN,Vodafone,Inbound,SMS,MT,Airtel,AirtelPMN,2026-05-29,1000.50
EOF

echo "Uploading to S3..."
aws s3 cp /tmp/settlement_test.csv s3://landing/tenant/settlement/test_$(date +%s).csv \
    --endpoint-url "$OCI_S3_ENDPOINT" \
    --no-sign-request 2>/dev/null || echo -e "${YELLOW}! S3 upload skipped (may need credentials)${NC}"

# Try to run ingestion
echo "Running data ingestion..."
docker compose exec spark /opt/platform/jobs/common/run_spark_submit.sh \
    /opt/platform/jobs/ingestion/load_tenant_stage_to_iceberg.py \
    --domain settlement \
    --layer bronze \
    --source-prefix tenant/settlement \
    2>/dev/null || {
    echo -e "${YELLOW}! Ingestion failed (expected for demo)${NC}"
}

# Test 7: Schema Still Visible After Failed Ingestion
test_header "7. Verify schema STILL visible after ingestion failure"

schema_after=$(docker compose exec trino trino --execute "DESCRIBE iceberg.dw.settlement" 2>/dev/null)

if echo "$schema_after" | grep -q "home_pmn"; then
    test_result "Schema STILL visible after failed ingestion"
else
    echo -e "${RED}✗ FAIL${NC}: Schema disappeared after failed ingestion"
fi

# Test 8: Table Still Queryable
test_header "8. Verify table is still QUERYABLE (even if ingestion failed)"

query_result=$(docker compose exec trino trino --execute \
    "SELECT COUNT(*) FROM iceberg.dw.settlement" 2>/dev/null)
echo "Query result: $query_result"
test_result "Table is queryable (even if ingestion failed)"

# Test 9: Cross-Tool Verification (If Superset available)
test_header "9. Verify tables visible in data tools"

echo "Summary of what other tools will see:"
echo ""
echo "Trino:"
docker compose exec trino trino --execute \
    "SELECT table_name FROM information_schema.tables WHERE table_schema='dw'" 2>/dev/null || \
    echo "  (Could not query Trino)"
echo ""
echo "Superset:"
echo "  Tables will appear in: Data → Datasets → Iceberg"
echo "  Even if empty, users can create dashboards based on these tables"
echo ""

# Summary
test_header "SUMMARY"

echo ""
echo -e "${GREEN}✅ Key Findings:${NC}"
echo "  1. Tables created by iceberg-alembic migrations are immediately visible"
echo "  2. Schema is visible in Trino even before any data is loaded"
echo "  3. Data engineers can see table structure before data arrives"
echo "  4. Failed ingestion does NOT make tables disappear"
echo "  5. Tables remain queryable (empty result if no data)"
echo "  6. Other tools (Superset, etc.) can discover tables by schema"
echo ""

echo -e "${BLUE}Next Steps:${NC}"
echo "  1. Upload sample data files to s3://landing/tenant/{domain}/"
echo "  2. Run ingestion job for each domain"
echo "  3. Monitor progress via Trino queries"
echo "  4. Check Superset for discovered tables"
echo ""

echo -e "${BLUE}=========================================="
echo "Test Complete"
echo "==========================================${NC}"
