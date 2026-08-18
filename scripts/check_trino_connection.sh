#!/usr/bin/env bash
# Verifies that the Trino ngrok endpoint is reachable and both catalogs return data.
# Authentication uses SSO (--external-authentication): a browser window opens once
# for Keycloak login, then all checks run automatically.
#
# Usage:
#   ./check_trino_connection.sh
#   TRINO_URL=https://trino.ngrok-free.app ./check_trino_connection.sh

set -euo pipefail

TRINO_URL="${TRINO_URL:-https://trino.ngrok-free.app}"
TRINO_CLI_VERSION="480"
TRINO_CLI_JAR="/tmp/trino-cli-${TRINO_CLI_VERSION}.jar"
TRINO_CLI_DOWNLOAD="https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_CLI_VERSION}/trino-cli-${TRINO_CLI_VERSION}-executable.jar"

PASS="✓"
FAIL="✗"

_green()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
_red()    { printf '\033[0;31m%s\033[0m\n' "$*"; }
_yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
_bold()   { printf '\033[1m%s\033[0m\n' "$*"; }

passed=0
failed=0

ok()      { _green "  ${PASS} $*"; ((passed++)); }
fail()    { _red   "  ${FAIL} $*"; ((failed++)); }
section() { echo; _bold "── $* ──"; }

# Build the base CLI command (SSO — no --user or --password)
trino_cmd() {
  java -jar "${TRINO_CLI_JAR}" \
    --server "${TRINO_URL}" \
    --external-authentication \
    "$@"
}

run_query() {
  local label="$1"
  local query="$2"
  local result
  if result=$(trino_cmd --output-format TSV_HEADER --execute "${query}" 2>/dev/null); then
    ok "${label}"
    echo "${result}" | sed 's/^/       /'
    echo
  else
    fail "${label}"
  fi
}

# ── 1. Prerequisites ──────────────────────────────────────────────────────────
section "Prerequisites"

if command -v java &>/dev/null; then
  java_ver=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
  if [[ "${java_ver}" -ge 17 ]]; then
    ok "Java ${java_ver} found"
  else
    fail "Java ${java_ver} found — need Java 17 or later"
    _red "    Install from https://adoptium.net"
    exit 1
  fi
else
  fail "Java not found — install from https://adoptium.net"
  exit 1
fi

if command -v curl &>/dev/null; then
  ok "curl found"
else
  fail "curl not found — install curl to continue"
  exit 1
fi

# ── 2. Trino CLI ──────────────────────────────────────────────────────────────
section "Trino CLI"

if [[ -f "${TRINO_CLI_JAR}" ]]; then
  ok "CLI jar already present at ${TRINO_CLI_JAR}"
else
  echo "  Downloading Trino CLI v${TRINO_CLI_VERSION}..."
  if curl -fsSL -o "${TRINO_CLI_JAR}" "${TRINO_CLI_DOWNLOAD}"; then
    chmod +x "${TRINO_CLI_JAR}"
    ok "Downloaded to ${TRINO_CLI_JAR}"
  else
    fail "Download failed — check your internet connection"
    exit 1
  fi
fi

# ── 3. Network reachability ───────────────────────────────────────────────────
section "Network"

if curl -fsSL --max-time 5 -o /dev/null "${TRINO_URL}/v1/info" 2>/dev/null; then
  ok "Trino endpoint reachable: ${TRINO_URL}"
else
  fail "Cannot reach ${TRINO_URL} — is the stack running with USE_NGROK=true?"
  exit 1
fi

trino_version=$(curl -fsSL --max-time 5 "${TRINO_URL}/v1/info" 2>/dev/null \
  | grep -o '"nodeVersion":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
ok "Server version: ${trino_version}"

# ── 4. Authentication (SSO browser flow) ─────────────────────────────────────
section "Authentication"

_yellow "  ${PASS} A browser window will open for SSO login."
_yellow "      1. The CLI will print a URL below — open it in your browser."
_yellow "      2. Log in with your SSO credentials (Keycloak)."
_yellow "      3. The browser will show 'OAuth2 authentication successful'."
_yellow "      4. Return here — the script continues automatically."
echo

if trino_cmd --output-format TSV --execute "SELECT 1" &>/dev/null; then
  ok "SSO login successful (browser flow completed)"
else
  fail "Authentication failed — check that your SSO account has Trino access"
  exit 1
fi

# ── 5. Catalog discovery ──────────────────────────────────────────────────────
section "Catalogs"

run_query "SHOW CATALOGS" "SHOW CATALOGS"

# ── 6. Iceberg catalog ────────────────────────────────────────────────────────
section "Iceberg Catalog"

CATALOG="${ICEBERG_NAMESPACE:-ee}"

run_query "SHOW SCHEMAS FROM ${CATALOG}" \
  "SHOW SCHEMAS FROM ${CATALOG}"

run_query "SHOW TABLES FROM ${CATALOG}.bronze" \
  "SHOW TABLES FROM ${CATALOG}.bronze"

run_query "Row count — ${CATALOG}.bronze.imsi_level_traffic" \
  "SELECT COUNT(*) AS row_count FROM ${CATALOG}.bronze.imsi_level_traffic" || true

run_query "Sample rows — ${CATALOG}.bronze.imsi_level_traffic (5 rows)" \
  "SELECT client_pmn, partner_pmn, call_date, call_type, imsi
   FROM ${CATALOG}.bronze.imsi_level_traffic
   LIMIT 5" || true

# ── 7. ClickHouse catalog ─────────────────────────────────────────────────────
section "ClickHouse Catalog"

run_query "SHOW SCHEMAS FROM clickhouse" \
  "SHOW SCHEMAS FROM clickhouse"

run_query "Row count — clickhouse.demo.tims_kpn_monthly_agg" \
  "SELECT COUNT(*) AS row_count FROM clickhouse.demo.tims_kpn_monthly_agg"

run_query "Sample rows — clickhouse.demo.tims_kpn_monthly_agg (5 rows)" \
  "SELECT
     call_month,
     client_pmn,
     partner_pmn,
     traffic_direction_short,
     partner_country_code,
     call_type
   FROM clickhouse.demo.tims_kpn_monthly_agg
   LIMIT 5"

# ── 8. Cross-catalog query ────────────────────────────────────────────────────
section "Cross-catalog (Iceberg ⟷ ClickHouse)"

run_query "Iceberg traffic data cross-referenced against ClickHouse" \
  "SELECT
     i.client_pmn,
     i.partner_pmn,
     COUNT(*)           AS iceberg_rows,
     COUNT(DISTINCT i.call_date) AS distinct_dates
   FROM ${CATALOG}.bronze.imsi_level_traffic AS i
   GROUP BY i.client_pmn, i.partner_pmn
   ORDER BY iceberg_rows DESC
   LIMIT 5" || true

# ── Summary ───────────────────────────────────────────────────────────────────
echo
_bold "── Summary ──"
_green "  Passed : ${passed}"
if [[ "${failed}" -gt 0 ]]; then
  _red "  Failed : ${failed}"
  echo
  _red "  One or more checks failed. See errors above."
  exit 1
else
  _green "  Failed : ${failed}"
  echo
  _green "  All checks passed. Trino is ready to use."
fi
