#!/usr/bin/env bash
# Run an OpenMetadata ingestion connector from inside the openmetadata-ingestion container.
# Usage:  ./run_ingestion.sh <connector>   (trino | airflow | superset)
#
# Prerequisites:
#   1. Get the ingestion-bot JWT from OpenMetadata UI → Settings → Bots → ingestion-bot
#   2. Export: export OM_INGESTION_JWT="<paste token here>"
#   3. Run:    docker compose exec openmetadata-ingestion /conf/run_ingestion.sh trino
set -euo pipefail

CONNECTOR="${1:-}"
if [ -z "$CONNECTOR" ]; then
  echo "Usage: $0 <trino|airflow|superset|oci_storage|clickhouse|pinot>"
  exit 1
fi

CONFIG_FILE="/conf/${CONNECTOR}_connector.yaml"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Config not found: $CONFIG_FILE"
  exit 1
fi

TMPFILE=$(mktemp /tmp/om_connector_XXXXXX.yaml)
trap 'rm -f "$TMPFILE"' EXIT

envsubst < "$CONFIG_FILE" > "$TMPFILE"
echo "[ingestion] Running connector: $CONNECTOR"
metadata ingest -c "$TMPFILE"
