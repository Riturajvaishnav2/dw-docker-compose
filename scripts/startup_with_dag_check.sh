#!/bin/bash
#
# Start Docker Compose and automatically check DAG status
# This script brings up all services and then lists the DAGs
#
# Usage: ./scripts/startup_with_dag_check.sh [docker-compose-args]
# Examples:
#   ./scripts/startup_with_dag_check.sh          # Start in background
#   ./scripts/startup_with_dag_check.sh up       # Start in foreground
#   ./scripts/startup_with_dag_check.sh up -d    # Explicitly background

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Starting Data Warehouse Services"
echo "=========================================="
echo ""

# Determine if running in background or foreground
# Default to background (-d) if no args provided
if [ $# -eq 0 ]; then
    COMPOSE_ARGS=("up" "-d")
else
    COMPOSE_ARGS=("$@")
fi

echo "🚀 Starting docker compose with args: ${COMPOSE_ARGS[@]}"
cd "$PROJECT_ROOT"
docker compose "${COMPOSE_ARGS[@]}"

# Only wait and check DAGs if we started in background mode
if [[ "${COMPOSE_ARGS[*]}" =~ "-d" ]] || [[ "${COMPOSE_ARGS[*]}" == "up" && ! "${COMPOSE_ARGS[*]}" =~ "-d" ]]; then
    if [[ "${COMPOSE_ARGS[*]}" =~ "-d" ]]; then
        echo ""
        echo "⏳ Waiting for Airflow services to be ready..."

        # Wait for airflow-webserver to be healthy
        for i in {1..30}; do
            if docker compose exec -T airflow-webserver airflow version &>/dev/null; then
                echo "✅ Airflow is ready!"
                break
            fi
            echo "  Attempt $i/30... waiting..."
            sleep 2
        done

        echo ""
        echo "📋 Checking DAG Status..."
        echo ""

        # Run the DAG list script
        "$SCRIPT_DIR/list_airflow_dags.sh"
    fi
fi

echo ""
echo "=========================================="
echo "Startup Complete!"
echo "=========================================="
echo ""
echo "📊 Access the services:"
echo "  - Airflow UI:     http://localhost:8080"
echo "  - Trino:          http://localhost:8443"
echo "  - Superset:       http://localhost:8088"
echo "  - Iceberg REST:   http://localhost:8181"
echo "  - OpenMetadata:   http://localhost:8585"
echo ""
echo "📖 To check DAG status anytime:"
echo "  ./scripts/list_airflow_dags.sh"
echo ""
echo "🗑️  To cleanup old DAGs:"
echo "  ./scripts/cleanup_airflow_dags.sh"
echo ""
