#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "iceberg-alembic Migration Runner"
echo "=================================="
echo ""

# Function to run migrations inside Docker
run_in_docker() {
    local command="$1"
    echo "Running: docker compose exec iceberg-alembic iceberg-migrate $command"
    docker compose exec iceberg-alembic iceberg-migrate $command
}

# Function to run migrations from host (venv)
run_in_venv() {
    local command="$1"

    if [ ! -d ".venv" ]; then
        echo "ERROR: Virtual environment not found. Run setup-dev.sh first."
        exit 1
    fi

    source .venv/bin/activate
    echo "Running: iceberg-migrate $command"
    iceberg-migrate $command
}

# Detect execution mode
USE_DOCKER=false
if docker compose ps 2>/dev/null | grep -q iceberg-alembic; then
    USE_DOCKER=true
fi

echo "Execution mode: $([ "$USE_DOCKER" = true ] && echo "Docker Compose" || echo "Host venv")"
echo ""

# Parse command
case "${1:-help}" in
    init)
        if [ "$USE_DOCKER" = true ]; then
            run_in_docker "init"
        else
            run_in_venv "init"
        fi
        ;;
    upgrade|downgrade|current|history|version)
        if [ "$USE_DOCKER" = true ]; then
            run_in_docker "$@"
        else
            run_in_venv "$@"
        fi
        ;;
    shell)
        if [ "$USE_DOCKER" = true ]; then
            echo "Entering Docker iceberg-alembic container..."
            docker compose exec iceberg-alembic /bin/bash
        else
            echo "ERROR: Use 'source .venv/bin/activate' instead"
            exit 1
        fi
        ;;
    verify)
        echo "Verifying migration state..."
        if [ "$USE_DOCKER" = true ]; then
            run_in_docker "current"
            run_in_docker "history"
        else
            run_in_venv "current"
            run_in_venv "history"
        fi

        echo ""
        echo "Querying tables in Trino..."
        CATALOG="${ICEBERG_NAMESPACE:-ee}"
        docker compose exec trino trino --execute "SHOW TABLES FROM ${CATALOG}.bronze" || echo "Note: Trino may not be running"
        ;;
    *)
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  init              Initialize migration state"
        echo "  upgrade HEAD      Apply all pending migrations"
        echo "  downgrade -1      Undo last migration"
        echo "  current           Show current revision"
        echo "  history           Show migration history"
        echo "  version           Show framework version"
        echo "  verify            Check migration status and tables"
        echo "  shell             Enter container shell (Docker only)"
        echo ""
        echo "Examples:"
        echo "  $0 upgrade head --dry-run"
        echo "  $0 downgrade -1"
        echo "  $0 verify"
        exit 1
        ;;
esac
