#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_SUBMIT="/opt/platform/jobs/common/run_spark_submit.sh"

cd "${ROOT_DIR}"

if [[ -f .env ]]; then
  set -a
  # This project .env uses simple KEY=VALUE assignments consumed by Docker Compose.
  # Sourcing it here keeps helper commands aligned with the same local settings.
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

compose() {
  docker compose "$@"
}

usage() {
  cat <<'USAGE'
Iceberg local platform helper

Usage:
  scripts/platform.sh <command> [args]

Stack commands:
  up                         Build and start the full stack
  down                       Stop the stack, preserving volumes
  restart                    Restart all running services
  restart-query              Restart Iceberg REST and Trino
  status                     Show service status
  logs [service]             Tail logs for one service or all services

Data load commands:
  load sales                 Load sales orders raw and build silver
  load dch                   Load DCH raw and build silver
  load fch                   Load FCH raw, bronze, silver, and gold
  load traffic               Load traffic data into bronze and silver
  load all                   Load every demo pipeline

Validation and query commands:
  validate                   Validate row counts and sample query output
  counts                     Show row counts for demo Iceberg tables
  query "<sql>"              Run a Trino SQL statement

Airflow and Superset helpers:
  trigger-dag <dag_id>       Trigger an Airflow DAG
  reset-superset-db          Reset local Superset metadata DB, then restart Superset

Examples:
  scripts/platform.sh up
  scripts/platform.sh load all
  scripts/platform.sh counts
  scripts/platform.sh query "SELECT * FROM iceberg.silver.sales_orders LIMIT 5"
  scripts/platform.sh logs trino
USAGE
}

info() {
  printf '\n==> %s\n' "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

require_docker() {
  require_cmd docker
  if ! docker compose version >/dev/null 2>&1; then
    printf 'Docker Compose is not available through `docker compose`.\n' >&2
    exit 1
  fi
}

ensure_services_running() {
  local missing=0
  for service in "$@"; do
    if ! compose ps --status running --services | grep -qx "${service}"; then
      printf 'Service is not running: %s\n' "${service}" >&2
      missing=1
    fi
  done

  if [[ "${missing}" -ne 0 ]]; then
    printf 'Start the stack first with: scripts/platform.sh up\n' >&2
    exit 1
  fi
}

run_spark_job() {
  local job_path="$1"
  ensure_services_running spark iceberg-rest minio
  info "Running Spark job: ${job_path}"
  compose exec -T spark "${SPARK_SUBMIT}" "${job_path}"
}

run_trino() {
  local sql="$1"
  ensure_services_running trino iceberg-rest minio
  compose exec -T trino trino --execute "${sql}"
}

load_ingestion_data() {
  run_spark_job /opt/platform/jobs/ingestion/load_ingestion_data.py
  run_spark_job /opt/platform/jobs/ingestion/load_traffic_bronze_to_iceberg.py
  run_spark_job /opt/platform/jobs/ingestion/load_traffic_silver_to_iceberg.py
}

counts() {
  run_trino "SELECT  'raw.fch_data', count(*) FROM iceberg.raw.fch_data

UNION ALL SELECT 'traffic.bronze', count(*) FROM iceberg.traffic.bronze
UNION ALL SELECT 'traffic.silver', count(*) FROM iceberg.traffic.silver
ORDER BY table_name"
}

validate() {
  info "Demo row counts"
  counts


  info "Traffic bronze sample"
  run_trino "SELECT client_operator, rp_tadig, call_date, cdr_count, duration, volume
FROM iceberg.traffic.bronze
LIMIT 10"

  info "Traffic silver sample"
  run_trino "SELECT client_operator, rp_tadig, call_date, cdr_count, duration, volume
FROM iceberg.traffic.silver
LIMIT 10"
}

trigger_dag() {
  local dag_id="${1:?Usage: scripts/platform.sh trigger-dag <dag_id>}"
  ensure_services_running airflow-webserver
  compose exec -T airflow-webserver airflow dags trigger "${dag_id}"
}

confirm_reset() {
  if [[ "${CONFIRM_RESET:-}" == "1" ]]; then
    return
  fi

  printf 'This deletes only local Superset metadata: databases, dashboards, charts, users.\n'
  printf 'It does not delete MinIO or Iceberg table data.\n'
  printf 'Type RESET to continue: '
  read -r answer
  if [[ "${answer}" != "RESET" ]]; then
    printf 'Cancelled.\n'
    exit 1
  fi
}

reset_superset_db() {
  ensure_services_running postgres
  confirm_reset
  info "Stopping Superset"
  compose stop superset
  info "Resetting Superset metadata database"
  compose exec -T postgres dropdb --if-exists --force -U "${POSTGRES_USER:-platform}" "${SUPERSET_DB:-superset}"
  compose exec -T postgres createdb -U "${POSTGRES_USER:-platform}" "${SUPERSET_DB:-superset}"
  info "Starting Superset"
  compose up -d superset
}

main() {
  local command="${1:-help}"
  shift || true

  case "${command}" in
    help|-h|--help)
      usage
      ;;
    up)
      compose up --build -d
      ;;
    down)
      compose down
      ;;
    restart)
      compose restart
      ;;
    restart-query)
      compose restart iceberg-rest trino
      ;;
    status|ps)
      compose ps
      ;;
    logs)
      if [[ "$#" -eq 0 ]]; then
        compose logs --tail=120
      else
        compose logs --tail=120 "$@"
      fi
      ;;
    load)
      case "${1:-}" in
        all)
          load_ingestion_data
          ;;
        *)
          printf 'Usage: scripts/platform.sh load sales|dch|fch|all\n' >&2
          exit 1
          ;;
      esac
      ;;
    validate)
      validate
      ;;
    counts)
      counts
      ;;
    query)
      if [[ "$#" -eq 0 ]]; then
        printf 'Usage: scripts/platform.sh query "<sql>"\n' >&2
        exit 1
      fi
      run_trino "$*"
      ;;
    trigger-dag)
      trigger_dag "$@"
      ;;
    reset-superset-db)
      reset_superset_db
      ;;
    *)
      printf 'Unknown command: %s\n\n' "${command}" >&2
      usage >&2
      exit 1
      ;;
  esac
}

case "${1:-help}" in
  help|-h|--help)
    main "$@"
    ;;
  *)
    require_docker
    main "$@"
    ;;
esac
