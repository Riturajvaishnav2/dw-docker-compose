#!/usr/bin/env bash
set -euo pipefail

PGHOST="${PGHOST:-postgres}"
PGPORT="${PGPORT:-5432}"

psql_db_exists() {
  psql -h "${PGHOST}" -p "${PGPORT}" -U "${POSTGRES_USER}" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${1}'" | grep -q 1
}

ensure_db() {
  local db_name="$1"
  if ! psql_db_exists "${db_name}"; then
    createdb -h "${PGHOST}" -p "${PGPORT}" -U "${POSTGRES_USER}" "${db_name}"
  fi
}

ensure_db "${AIRFLOW_DB:-airflow}"
ensure_db "${SUPERSET_DB:-superset}"
ensure_db "openmetadata"
ensure_db "iceberg_catalog"
ensure_db "${KEYCLOAK_DB:-keycloak}"
