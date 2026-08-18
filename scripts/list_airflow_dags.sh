#!/bin/bash
#
# List Airflow DAGs - Show which DAGs are in codebase and which are running
#
# Usage: ./list_airflow_dags.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DAGS_DIR="$PROJECT_ROOT/dags"

echo "=========================================="
echo "Airflow DAG Status Report"
echo "=========================================="
echo ""

# Get list of DAGs in codebase
echo "📦 DAGs in codebase (dags/ directory):"
CODEBASE_DAGS=()
if [ -d "$DAGS_DIR" ]; then
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
else
    echo "  ⚠️  dags/ directory not found at $DAGS_DIR"
fi

echo ""
echo "🔍 DAGs running in Airflow:"

# Try to get DAGs from Airflow
if docker compose exec -T airflow-webserver airflow version &>/dev/null; then
    AIRFLOW_DAGS=$(docker compose exec -T airflow-webserver airflow dags list --output table 2>/dev/null | tail -n +3 | awk '{print $1}' | sort)

    if [ -z "$AIRFLOW_DAGS" ]; then
        echo "  (No DAGs loaded yet or Airflow still starting)"
    else
        echo "$AIRFLOW_DAGS" | while read dag; do
            if [[ " ${CODEBASE_DAGS[@]} " =~ " ${dag} " ]]; then
                echo "  ✓ $dag (KEEP - part of codebase)"
            else
                echo "  ✗ $dag (REMOVE - NOT in codebase)"
            fi
        done
    fi
else
    echo "  ⚠️  Cannot connect to Airflow webserver"
    echo "  Make sure Airflow is running: docker compose up -d airflow-webserver airflow-scheduler"
fi

echo ""
echo "=========================================="
