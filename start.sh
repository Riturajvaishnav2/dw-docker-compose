#!/bin/bash

# ============================================================================
# Startup Script: Generate catalogs BEFORE starting Docker containers
# ============================================================================
# This script ensures catalog generation runs FIRST, on the host,
# so Trino has valid catalogs ready when it starts

set -e

echo "=========================================================================="
echo "Datawarehouse Startup Script"
echo "=========================================================================="
echo ""

# Load environment variables from .env
if [ -f .env ]; then
    echo "Loading environment variables from .env..."
    set -a
    source .env
    set +a
    echo "✓ Environment loaded"
else
    echo "ERROR: .env file not found!"
    exit 1
fi

echo ""
echo "=========================================================================="
echo "Step 1: Generate Trino Catalog Configuration (on HOST)"
echo "=========================================================================="
echo "ICEBERG_NAMESPACE: $ICEBERG_NAMESPACE"

if [ -f scripts/generate-trino-catalogs.sh ]; then
    bash scripts/generate-trino-catalogs.sh
    echo "✓ Catalog generation complete"
else
    echo "ERROR: scripts/generate-trino-catalogs.sh not found!"
    exit 1
fi

echo ""
echo "=========================================================================="
echo "Step 2: Start Docker Containers"
echo "=========================================================================="

if [ "$1" = "down" ]; then
    echo "Stopping containers..."
    docker compose down
    exit 0
fi

if [ "$1" = "restart" ]; then
    echo "Restarting containers..."
    docker compose down >/dev/null 2>&1
fi

echo "Starting Docker Compose..."
docker compose up -d

echo ""
echo "=========================================================================="
echo "Step 3: Wait for Services to Become Healthy"
echo "=========================================================================="
echo "Waiting for Iceberg REST to be healthy..."

max_attempts=60
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker compose ps iceberg-rest 2>/dev/null | grep -q "(healthy)"; then
        echo "✓ Iceberg REST is healthy"
        break
    fi
    attempt=$((attempt + 1))
    if [ $((attempt % 10)) -eq 0 ]; then
        echo "  Waiting for Iceberg REST... ($attempt/$max_attempts)"
    fi
    sleep 2
done

echo ""
echo "Waiting for iceberg-alembic to be healthy..."

attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker compose ps iceberg-alembic 2>/dev/null | grep -q "(healthy)"; then
        echo "✓ iceberg-alembic is healthy"
        break
    fi
    attempt=$((attempt + 1))
    if [ $((attempt % 10)) -eq 0 ]; then
        echo "  Waiting for iceberg-alembic... ($attempt/$max_attempts)"
    fi
    sleep 2
done

echo ""
echo "Waiting for Trino to be healthy (this may take 1-2 minutes)..."

attempt=0
max_attempts=120
while [ $attempt -lt $max_attempts ]; do
    if docker compose ps trino 2>/dev/null | grep -q "(healthy)"; then
        echo "✓ Trino is healthy"
        break
    fi
    attempt=$((attempt + 1))
    if [ $((attempt % 20)) -eq 0 ]; then
        echo "  Waiting for Trino to fully start... ($attempt/$max_attempts)"
    fi
    sleep 2
done

echo ""
echo "=========================================================================="
echo "✓ Startup Complete!"
echo "=========================================================================="
echo ""
echo "Catalog: $ICEBERG_NAMESPACE"
echo "Warehouse: s3://$CATALOG_WAREHOUSE/$ICEBERG_NAMESPACE"
echo ""
echo "Next steps:"
echo "  • Query via Trino:"
echo "    docker compose exec trino trino --execute \"SHOW CATALOGS;\""
echo ""
echo "  • To switch tenants:"
echo "    1. Edit .env: ICEBERG_NAMESPACE=<new-name>"
echo "    2. Run: ./start.sh restart"
echo ""
