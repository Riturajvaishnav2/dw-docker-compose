#!/bin/bash
#
# Cleanup Airflow DAGs - Remove all DAGs not part of the codebase
# Keeps only DAGs defined in dags/ directory
#
# Usage: ./cleanup_airflow_dags.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DAGS_DIR="$PROJECT_ROOT/dags"

echo "=========================================="
echo "Airflow DAG Cleanup Script"
echo "=========================================="
echo ""

# Get list of DAGs in codebase (without .py extension)
echo "📦 DAGs in codebase:"
CODEBASE_DAGS=()
for dag_file in "$DAGS_DIR"/*.py; do
    if [ -f "$dag_file" ]; then
        # Extract dag_id from the file
        dag_id=$(grep -oP "dag_id\s*=\s*['\"]?\K[^'\"]*" "$dag_file" | head -1)
        if [ -n "$dag_id" ]; then
            CODEBASE_DAGS+=("$dag_id")
            echo "  ✓ $dag_id (from $(basename "$dag_file"))"
        fi
    fi
done

if [ ${#CODEBASE_DAGS[@]} -eq 0 ]; then
    echo "  ⚠️  No DAGs found in codebase!"
    exit 1
fi

echo ""
echo "🔍 Fetching DAGs from Airflow database..."
echo ""

# Get all DAGs from Airflow database
AIRFLOW_DAGS=$(docker compose exec -T airflow-webserver airflow dags list --output table 2>/dev/null | tail -n +3 | awk '{print $1}' | sort)

if [ -z "$AIRFLOW_DAGS" ]; then
    echo "  ⚠️  Could not connect to Airflow or no DAGs found"
    echo "  Make sure Airflow services are running: docker compose up -d"
    exit 1
fi

echo "📋 DAGs in Airflow:"
echo "$AIRFLOW_DAGS" | while read dag; do
    if [[ " ${CODEBASE_DAGS[@]} " =~ " ${dag} " ]]; then
        echo "  ✓ $dag (KEEP - part of codebase)"
    else
        echo "  ✗ $dag (REMOVE - not in codebase)"
    fi
done

echo ""
echo "=========================================="
echo "Cleanup Summary:"
echo "=========================================="

# Find DAGs to delete
DAGS_TO_DELETE=()
while IFS= read -r dag; do
    if ! [[ " ${CODEBASE_DAGS[@]} " =~ " ${dag} " ]]; then
        DAGS_TO_DELETE+=("$dag")
    fi
done <<< "$AIRFLOW_DAGS"

if [ ${#DAGS_TO_DELETE[@]} -eq 0 ]; then
    echo "✅ No cleanup needed - all DAGs are part of codebase"
    exit 0
fi

echo "Found ${#DAGS_TO_DELETE[@]} DAG(s) to remove:"
for dag in "${DAGS_TO_DELETE[@]}"; do
    echo "  - $dag"
done

echo ""
read -p "❓ Do you want to delete these DAGs? (yes/no): " -r confirm

if [[ ! "$confirm" =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "❌ Cleanup cancelled"
    exit 0
fi

echo ""
echo "🗑️  Deleting unwanted DAGs..."
for dag in "${DAGS_TO_DELETE[@]}"; do
    echo "  Deleting $dag..."
    docker compose exec -T airflow-webserver airflow dags delete --yes "$dag" 2>&1 | grep -v "^$" || true
done

echo ""
echo "🔄 Restarting Airflow services..."
docker compose restart airflow-webserver airflow-scheduler

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 5

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "Final DAG list:"
docker compose exec -T airflow-webserver airflow dags list --output table 2>/dev/null || echo "  (Airflow still starting...)"
