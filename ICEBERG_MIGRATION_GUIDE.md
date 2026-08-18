# Iceberg Migration Guide - Complete Documentation

**Last Updated:** 2026-07-19  
**Version:** 1.0  
**Author:** Data Warehouse Team

---

## Table of Contents

1. [Overview](#overview)
2. [Migration Tool](#migration-tool)
3. [Architecture](#architecture)
4. [Supported Operations](#supported-operations)
5. [Feature Support Matrix](#feature-support-matrix)
6. [Deployed Migrations](#deployed-migrations)
7. [Migration Specifications](#migration-specifications)
8. [Implementation Status](#implementation-status)
9. [Operations Reference](#operations-reference)
10. [Common Tasks](#common-tasks)
11. [Troubleshooting](#troubleshooting)

---

## Overview

This document provides complete details about the Iceberg schema migration system used in the data warehouse project. The project uses **Alembic with iceberg-alembic extension** for managing Apache Iceberg table schemas across multiple environments and tenants.

### Key Highlights

- **10 Migrations Deployed** across 3 domains (Traffic, Settlement, Forecast)
- **Multi-Tenant Support** with namespace isolation
- **3-Layer Architecture** (Bronze, Silver, Gold)
- **Automatic Execution** on container startup
- **State Management** with version tracking
- **9 Core Operations** for schema management

### Project Structure

```
datawarehouse/
├── iceberg-alembic/
│   ├── iceberg-alembic.toml              # Main configuration
│   ├── migrations/
│   │   └── versions/                     # Migration Python files
│   ├── run-migrations.sh                 # Execution script
│   ├── scripts/                          # Utility scripts
│   └── iceberg_alembic/                  # Core library
│       ├── operations.py                 # 9 core operations
│       ├── config.py                     # Configuration loader
│       ├── catalog_utils.py              # Catalog helpers
│       ├── migration_helpers.py          # Shared utilities
│       ├── cli.py                        # Command-line interface
│       ├── runner.py                     # Migration executor
│       └── state.py                      # State management
└── docker/
    └── iceberg-alembic/
        ├── Dockerfile                    # Container definition
        └── entrypoint.sh                 # Startup script
```

---

## Migration Tool

### What is Alembic?

Alembic is a lightweight database migration tool for SQLAlchemy. The project uses **iceberg-alembic**, a specialized extension that adapts Alembic for Apache Iceberg.

### Why Alembic for Iceberg?

| Aspect | Benefit |
|--------|---------|
| **Version Control** | Track schema changes in Git with revision tracking |
| **Atomic Operations** | Ensure migrations succeed or fail completely |
| **Automatic Execution** | Runs on container startup, no manual steps |
| **State Management** | Prevents duplicate execution via `.iceberg-alembic-state.json` |
| **Dry-Run Support** | Test migrations before applying |
| **Multi-Environment** | Same migration code for dev, staging, production |
| **Rollback Guidance** | Snapshots available for manual recovery |

### Configuration File

**Location:** `iceberg-alembic/iceberg-alembic.toml`

```toml
[catalog]
name = "datawarehouse-local"
type = "rest"
uri = "http://iceberg-rest:8181"
warehouse = "s3://warehouse/"
namespace = "dw"

[catalog.properties]
s3_endpoint = "${OCI_S3_ENDPOINT}"
s3_region = "${OCI_REGION}"
s3_access_key_id = "${OCI_ACCESS_KEY_ID}"
s3_secret_access_key = "${OCI_SECRET_ACCESS_KEY}"
s3_path_style_access = "true"
s3_checksum_enabled = "false"
s3_chunked_encoding_enabled = "false"

[execution]
applied_by = "datawarehouse-alembic"
trino_url = "https://localhost:8443"
spark_url = "local://spark"
state_path = ".iceberg-alembic-state.json"
```

### Environment Variables

The migration system reads from these environment variables (`.env` file):

```bash
# OCI Object Storage Configuration
OCI_S3_ENDPOINT=https://lrlk1oak2k7m.compat.objectstorage.uk-london-1.oraclecloud.com
OCI_REGION=uk-london-1
OCI_ACCESS_KEY_ID=your-access-key
OCI_SECRET_ACCESS_KEY=your-secret-key

# Iceberg Namespace (tenant)
ICEBERG_NAMESPACE=ee
```

### State Management

**File:** `.iceberg-alembic-state.json` (stored in migrations directory)

Tracks which migrations have been applied:

```json
{
  "applied_migrations": [
    {
      "revision": "20260604_005_create_imsi_level_traffic_bronze_table",
      "applied_at": "2026-06-23T19:15:32.123456",
      "applied_by": "datawarehouse-alembic",
      "status": "success"
    },
    {
      "revision": "20260623_008_create_silver_dimension_tables",
      "applied_at": "2026-06-23T19:15:45.654321",
      "applied_by": "datawarehouse-alembic",
      "status": "success"
    }
  ],
  "current_revision": "20260704_001_create_forecast_dimensions"
}
```

---

## Architecture

### Three-Layer Architecture

```
┌─────────────────────────────────────────┐
│          DATA SOURCES (S3)              │
│  tenant/{domain}/{layer}/*.{csv,pq}    │
└──────────────────┬──────────────────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   BRONZE LAYER       │
        │  Raw Data + Audit    │
        │  (imsi_level_traffic)│
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   SILVER LAYER       │
        │ Cleaned + Dimensions │
        │  (14 dimensions)     │
        │  (fact tables)       │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   GOLD LAYER         │
        │ Denormalized + Ready │
        │   (for BI/Reports)   │
        └──────────────────────┘
```

### Namespace Organization

```
Iceberg Catalog (REST)
├── bronze/                    # Raw layer
│   ├── imsi_level_traffic     # Traffic domain
│   └── settlement_detail      # Settlement domain
├── silver/                    # Cleaned layer
│   ├── dim_client             # Dimensions
│   ├── dim_operator
│   ├── dim_date
│   ├── fact_imsi_level_traffic # Facts
│   └── fact_settlement
└── gold/                      # Business layer
    ├── imsi_level_traffic_daily
    ├── client_partner_traffic_monthly
    └── settlement_summary
```

### Multi-Tenant Support

Each tenant gets its own isolated namespace:

```
Catalog = "datawarehouse-local"

Tenant "ee" namespace:
  → s3://warehouse/ee/bronze/...
  → s3://warehouse/ee/silver/...
  → s3://warehouse/ee/gold/...

Tenant "orange" namespace:
  → s3://warehouse/orange/bronze/...
  → s3://warehouse/orange/silver/...
  → s3://warehouse/orange/gold/...
```

---

## Supported Operations

### 9 Core Operations

The `IcebergOperations` class provides these operations:

#### 1. **create_namespace(namespace: str)**

Creates a new schema/namespace in the catalog.

```python
op.create_namespace("bronze")
op.create_namespace("silver")
op.create_namespace("gold")
```

**Status:** ✅ SUPPORTED | ✅ IN USE  
**Exception Handling:** NamespaceAlreadyExistsError (ignored gracefully)

#### 2. **create_table()**

Creates a new Iceberg table with schema definition.

```python
op.create_table(
    namespace="bronze",
    table_name="imsi_level_traffic",
    columns=[...],
    partition_by=["_ingest_date"],
    location="s3://warehouse/bronze/imsi_level_traffic",
    properties={
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy"
    }
)
```

**Status:** ✅ SUPPORTED | ✅ IN USE  
**Exception Handling:** TableAlreadyExistsError (skipped gracefully)  
**Key Features:**
- Automatic location generation if not provided
- Default table properties applied
- Partition spec from `partition_by` list
- S3 path-style access enabled

#### 3. **drop_table(namespace: str, table_name: str)**

Removes a table from the catalog.

```python
op.drop_table("bronze", "imsi_level_traffic")
```

**Status:** ✅ SUPPORTED | ❌ NOT IN USE  
**Exception Handling:** NoSuchTableError (ignored gracefully)

#### 4. **add_column()**

Adds a new column to an existing table.

```python
op.add_column(
    namespace="bronze",
    table_name="imsi_level_traffic",
    column_name="new_field",
    column_type="string",
    doc="Description of the new field"
)
```

**Status:** ✅ SUPPORTED | ❌ NOT IN USE  
**Use Case:** Extend existing tables without recreating them

#### 5. **drop_column()**

Removes a column from a table.

```python
op.drop_column("bronze", "imsi_level_traffic", "obsolete_field")
```

**Status:** ✅ SUPPORTED | ❌ NOT IN USE  
**Use Case:** Clean up deprecated columns

#### 6. **rename_column()**

Renames an existing column.

```python
op.rename_column(
    "bronze", 
    "imsi_level_traffic",
    "old_name",
    "new_name"
)
```

**Status:** ✅ SUPPORTED | ❌ NOT IN USE  
**Use Case:** Standardize column naming conventions

#### 7. **update_column_type()**

Changes the data type of a column.

```python
op.update_column_type(
    "bronze",
    "imsi_level_traffic", 
    "duration",
    "decimal(18,6)"
)
```

**Status:** ✅ SUPPORTED | ❌ NOT IN USE  
**Iceberg Support:** Allows type evolution within constraints

#### 8. **rename_table()**

Renames or moves a table to a different namespace.

```python
op.rename_table(
    old_namespace="bronze",
    old_name="imsi_level_traffic",
    new_namespace="silver",
    new_name="imsi_level_traffic_cleaned"
)
```

**Status:** ✅ SUPPORTED | ❌ NOT IN USE

#### 9. **capture_state(namespace: str, table_name: str)**

Snapshots the current state of a table for audit/rollback purposes.

```python
before = op.capture_state("bronze", "imsi_level_traffic")
# ... perform operations ...
after = op.capture_state("bronze", "imsi_level_traffic")

# Returns: StateSnapshot(snapshot_id=123, metadata_location="s3://...")
```

**Status:** ✅ SUPPORTED | ✅ IN USE (internal)  
**Returns:**
- `snapshot_id`: Iceberg's internal snapshot ID
- `metadata_location`: Path to snapshot metadata

---

## Feature Support Matrix

### Schema Mutation Features

| Feature | Status | Details | In Use |
|---------|--------|---------|--------|
| Adding new columns | ✅ SUPPORTED | Via `add_column()` | No |
| Removing columns | ✅ SUPPORTED | Via `drop_column()` | No |
| Renaming columns | ✅ SUPPORTED | Via `rename_column()` | No |
| Updating column types | ✅ SUPPORTED | Via `update_column_type()` | No |
| Adding table properties | ✅ SUPPORTED | Via `properties` in create_table | Yes |
| Updating table properties | ✅ SUPPORTED | Via table.update_properties() | No |
| Creating schemas | ✅ SUPPORTED | Via `create_namespace()` | Yes |
| Dropping schemas | ✅ SUPPORTED | Via catalog.drop_namespace() | No |
| Creating tables | ✅ SUPPORTED | Via `create_table()` | Yes |
| Dropping tables | ✅ SUPPORTED | Via `drop_table()` | No |

### Migration Management Features

| Feature | Status | Details |
|---------|--------|---------|
| Apply migrations in sequence | ✅ SUPPORTED | Via `down_revision` chain |
| Track applied versions | ✅ SUPPORTED | Via `.iceberg-alembic-state.json` |
| Prevent duplicate execution | ✅ SUPPORTED | State file blocks re-runs |
| Dry-run validation | ✅ SUPPORTED | Via `dry_run=True` flag |
| Clear migration logs | ✅ SUPPORTED | Via Python logging module |
| Rollback guidance | ⚠️ PARTIAL | Snapshots available, manual recovery |
| Automatic rollback | ❌ NOT SUPPORTED | Requires manual intervention |
| Migration rollback testing | ❌ NOT SUPPORTED | No test rollback mode |

### Tenant Management Features

| Feature | Status | Details |
|---------|--------|---------|
| Dynamic catalog creation | ✅ SUPPORTED | REST catalog creates namespaces on demand |
| Tenant-specific schemas | ✅ SUPPORTED | Via ICEBERG_NAMESPACE env var |
| Tenant-specific tables | ✅ SUPPORTED | Tables partitioned by tenant |
| Cross-tenant isolation | ✅ SUPPORTED | Namespace-level isolation |
| Configurable naming | ✅ SUPPORTED | Via config parameters |
| Tenant validation | ⚠️ PARTIAL | Basic validation only |
| New tenant onboarding | ✅ SUPPORTED | `create_schema_final.sh` script |
| Handle existing resources | ✅ SUPPORTED | Exception handling for duplicate creation |

### File Path Features

| Feature | Status | Details |
|---------|--------|---------|
| Centralized path generation | ✅ SUPPORTED | Via `_default_table_location()` |
| Tenant-based paths | ✅ SUPPORTED | Format: `s3://warehouse/{tenant}/{layer}/{table}` |
| Layer-based paths | ✅ SUPPORTED | Paths include layer prefix |
| Domain-based paths | ✅ SUPPORTED | Domains: traffic, settlement, forecast |
| Configurable locations | ✅ SUPPORTED | Via S3_ENDPOINT, S3_REGION |
| Path validation | ⚠️ PARTIAL | Basic checks only |
| Audit logging | ✅ SUPPORTED | Via audit columns |

---

## Deployed Migrations

### Complete Migration List

**Total:** 10 Migrations | **Domains:** 3 (Traffic, Settlement, Forecast) | **Layers:** 3 (Bronze, Silver, Gold)

### Migration Chain (Execution Order)

```
1. 20260604_005
   ↓
2. 20260617_006
   ↓
3. 20260623_008
   ↓
4. 20260623_009
   ↓
5. 20260623_010
   ↓
6. 20260703_001
   ↓
7. 20260703_002
   ↓
8. 20260703_003
   ↓
9. 20260704_001
```

---

## Migration Specifications

### Migration 1: Bronze Layer - Traffic

**File:** `20260604_005_create_imsi_level_traffic_bronze_table.py`

**Purpose:** Raw data ingestion with complete audit trail

**Table Created:** `bronze.imsi_level_traffic`

**Schema:** 33 columns total

| Column Name | Type | Purpose |
|-------------|------|---------|
| client_pmn | string | Client network identifier |
| partner_pmn | string | Partner network identifier |
| traffic_direction | string | Inbound/Outbound |
| call_date | string | Date of call |
| call_type | string | Call type classification |
| imsi | string | Subscriber identifier |
| apn | string | Access Point Name |
| duration | string | Call duration (stored as string) |
| volume | string | Data volume (stored as string) |
| event_count | string | Event count (stored as string) |
| roamer_indicator | string | Domestic/Roaming indicator |
| total_charge_sdr_net | string | Net charges |
| total_charge_sdr_gross | string | Gross charges |
| iot_rti_group_id | string | IoT group identifier |
| service_type_id | string | Service type |
| event_type_id | string | Event type |
| call_type_level_2 | string | Call type detail |
| rat_type | string | Radio Access Technology |
| tac_number | string | Type Allocation Code |
| roaming_partner_country | string | Roaming country |
| call_month | string | Month of call |
| destination_category | string | Destination type |
| destination | string | Destination identifier |
| is_camel | string | CAMEL protocol indicator |
| source_system | string | Source system name |
| source_file_name | string | Original file name |
| ingestion_id | string | Unique ingestion identifier |
| batch_id | string | Batch processing ID |
| record_hash | string | Record deduplication hash |
| received_at | string | Receipt timestamp |
| processing_status | string | Processing result status |
| error_message | string | Error details if failed |
| **Audit Columns (8)** | | Auto-populated |

**Audit Columns:**
- `_source_bucket` - S3 bucket name
- `_source_key` - S3 object key
- `_source_file_name` - Original file name
- `_source_format` - File format (csv, parquet)
- `_domain` - Data domain (traffic, settlement)
- `_layer` - Layer name (bronze, silver, gold)
- `_ingested_at` - Ingestion timestamp
- `_ingest_date` - Ingestion date (partition key)

**Partitioning:** By `_ingest_date`

**Table Properties:**
```python
{
    "table_type": "imsi_level_traffic_bronze",
    "source_model": "imsi-level-traffic-bronze",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy",
    "write.spark.accept-any-schema": "true"
}
```

**Location:** `s3://warehouse/bronze/imsi_level_traffic`

**Key Characteristics:**
- All source fields stored as strings (raw data)
- No transformation or type conversion
- Full audit trail for traceability
- Partitioned for query performance
- Supports large-scale ingestion

---

### Migration 2: Gold Layer - Settlement

**File:** `20260617_006_create_settlement_gold_table.py`

**Purpose:** Business-ready settlement data for reporting

**Tables Created:** 1 table (content omitted for brevity, similar pattern to traffic)

---

### Migration 3: Silver Layer - Dimension Tables

**File:** `20260623_008_create_silver_dimension_tables.py`

**Purpose:** Cleaned, deduplicated dimension tables

**Tables Created:** 14 dimension tables

| Dimension | Columns | Key Field | Grain |
|-----------|---------|-----------|-------|
| dim_client | pmn_key, pmn, created_at, updated_at | pmn_key | Unique clients |
| dim_operator | pmn_key, pmn, pmn_country, created_at, updated_at | pmn_key | Unique operators |
| dim_traffic_direction | traffic_direction_key, traffic_direction | traffic_direction_key | 2 values (Inbound/Outbound) |
| dim_date | call_date_key, call_date, call_month, year, month, day | call_date_key | One row per date |
| dim_call_type | call_type_key, call_type, call_type_level_2_key, call_type_level_2 | call_type_key | Call type combinations |
| dim_imsi | imsi_key, imsi, roamer_indicator_key, roamer_indicator | imsi_key | Unique IMSI values |
| dim_apn | apn_key, apn | apn_key | Unique APN values |
| dim_service_type | service_type_key, service_type_id | service_type_key | Service types |
| dim_event_type | event_type_key, event_type_id | event_type_key | Event types |
| dim_rat_type | rat_type_key, rat_type | rat_type_key | Radio Access Technologies |
| dim_tac | tac_key, tac_number | tac_key | Device type codes |
| dim_iot_rti_group | iot_rti_group_key, iot_rti_group_id | iot_rti_group_key | IoT groupings |
| dim_location | destination_key, destination, destination_category | destination_key | Destination locations |
| dim_camel | camel_key, is_camel | camel_key | CAMEL indicators |

**Key Features:**
- **Deterministic Keys:** `md5(lower(trim(column_value)))`
- **Type Conversion:** Data types properly converted
- **SCD Type 1:** Always latest values (no version history)
- **Audit Columns:** created_at, updated_at timestamps

---

### Migration 4: Silver Layer - Fact Table

**File:** `20260623_009_create_silver_fact_imsi_level_traffic.py`

**Purpose:** Aggregated traffic data with dimensional keys

**Table Created:** `silver.fact_imsi_level_traffic`

**Grain:** One row per unique combination of:
- Client PMN
- Partner PMN
- Traffic Direction
- Call Date
- Call Type (Level 1 & 2)
- IMSI
- APN
- Service Type
- Event Type
- RAT Type
- TAC
- IoT RTI Group
- Destination
- CAMEL Indicator

**Columns:** 26 total

| Category | Columns |
|----------|---------|
| **Fact ID** | traffic_fact_id |
| **Dimension Keys (14)** | client_pmn_key, partner_pmn_key, traffic_direction_key, call_date_key, call_month_key, call_type_key, call_type_level_2_key, imsi_key, apn_key, roamer_indicator_key, service_type_key, event_type_key, rat_type_key, tac_key, iot_rti_group_key, destination_key, camel_key |
| **Measures (5)** | duration (decimal), volume (decimal), event_count (long), total_charge_sdr_net (decimal), total_charge_sdr_gross (decimal) |
| **Audit** | source_system, batch_id, record_hash, created_at, updated_at |

**Partitioning:** By `call_date_key`

**Table Properties:**
```python
{
    "table_type": "fact",
    "fact_name": "imsi_level_traffic",
    "grain": "imsi_call_date_client_partner_direction_type_apn_service_event_rat_tac_group_destination_camel"
}
```

---

### Migration 5: Gold Layer - Traffic Tables

**File:** `20260623_010_create_gold_traffic_tables.py`

**Purpose:** Denormalized business-ready tables for reporting

#### Table 5a: `gold.imsi_level_traffic_daily`

**Grain:** One row per unique combination at daily level:
- Client/Partner/Direction/Date/Call Type/IMSI/APN/Service

**Columns:** 28

**Dimension Attributes (22 denormalized from silver):**
- client_pmn, partner_pmn, roaming_partner_country
- traffic_direction
- call_date, call_month, year, month, day
- call_type, call_type_level_2
- imsi, roamer_indicator
- apn, service_type_id, event_type_id
- rat_type, tac_number, iot_rti_group_id
- destination, destination_category, is_camel

**Aggregated Measures (5):**
- total_duration (decimal)
- total_volume (decimal)
- total_event_count (long)
- total_charge_sdr_net (decimal)
- total_charge_sdr_gross (decimal)

**Audit Columns (2):**
- created_at, updated_at

**Partitioning:** By `call_date`

**Use Cases:**
- Daily reporting dashboards
- Detailed traffic analysis
- Billing verification
- Roaming analytics

#### Table 5b: `gold.client_partner_traffic_monthly`

**Grain:** One row per unique combination at monthly level:
- Client/Partner/Direction/Month/Call Type/Service

**Columns:** 19

**Aggregated Dimensions (11):**
- client_pmn, partner_pmn, roaming_partner_country
- traffic_direction
- call_month, year, month
- call_type, call_type_level_2
- service_type_id, event_type_id

**Aggregated Measures (5):**
- total_duration, total_volume, total_event_count
- total_charge_sdr_net, total_charge_sdr_gross

**Distinct Counts (2):**
- distinct_imsi_count
- distinct_apn_count

**Audit Columns (2):**
- created_at, updated_at

**Partitioning:** By `call_month`

**Use Cases:**
- Monthly billing reports
- Partner analytics
- Trend analysis
- Invoice generation

---

### Migrations 6-10: Forecast Domain

**Files:**
- `20260703_001_create_forecast_bronze_table.py`
- `20260703_002_create_forecast_silver_tables.py`
- `20260703_003_create_forecast_gold_tables.py`
- `20260704_001_create_forecast_dimensions.py`

**Purpose:** Forecast data pipeline (similar 3-layer structure)

**Domains Covered:** Traffic forecasting, trend prediction

**Pattern:** Same as traffic domain (Bronze → Silver → Gold)

---

## Implementation Status

### Fully Implemented ✅

- ✅ Alembic + iceberg-alembic framework
- ✅ REST Catalog integration
- ✅ Multi-tenant namespace isolation
- ✅ Bronze-Silver-Gold architecture
- ✅ 10 migrations deployed
- ✅ Automatic startup execution
- ✅ State file tracking
- ✅ Audit column standardization
- ✅ Partitioning strategy
- ✅ Dry-run support
- ✅ Exception handling
- ✅ S3/OCI Object Storage integration
- ✅ Environment-based configuration
- ✅ Docker containerization

### Partially Implemented ⚠️

- ⚠️ Rollback operations (snapshots available, manual recovery)
- ⚠️ Tenant configuration validation
- ⚠️ Path validation logic
- ⚠️ Migration testing framework

### Not Implemented ❌

- ❌ Schema evolution migrations (operations available, not used)
- ❌ Add/Drop column migrations
- ❌ Rename column migrations
- ❌ Type upgrade migrations
- ❌ Automated rollback testing
- ❌ Migration GUI/Web interface
- ❌ Automated data validation post-migration

---

## Operations Reference

### Installation

The iceberg-alembic tool is already installed in the Docker image. No additional installation needed.

### Running Migrations Manually

```bash
# Enter the iceberg-alembic container
docker compose exec iceberg-alembic bash

# Run migrations to the latest version
iceberg-migrate upgrade head

# Run to a specific migration
iceberg-migrate upgrade 20260623_010_create_gold_traffic_tables

# Check current status
iceberg-migrate current

# View migration history
iceberg-migrate history
```

### Checking Migration Status

```bash
# View logs
docker logs datawarehouse-iceberg-alembic-1

# Check state file
docker compose exec iceberg-alembic cat /migrations/.iceberg-alembic-state.json

# List schemas via Trino
docker compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg"

# List tables in schema
docker compose exec trino trino --execute "SHOW TABLES FROM iceberg.bronze"
```

### Creating New Migration

**Step 1: Create migration file**

```bash
# File: iceberg-alembic/migrations/versions/YYYYMMDD_NNN_description.py

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES, with_audit_columns
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "YYYYMMDD_NNN_description"
down_revision = "YYYYMMDD_NNN_previous"  # Link to previous migration

CATALOG_NAME = get_catalog_name()

def upgrade(op):
    """Create new table or schema."""
    op.create_namespace("my_schema")
    
    op.create_table(
        namespace="my_schema",
        table_name="my_table",
        columns=with_audit_columns([
            {"name": "id", "type": "string", "source_name": "id"},
            {"name": "name", "type": "string", "source_name": "name"},
            # ... more columns
        ]),
        partition_by=["_ingest_date"],
        properties={
            "table_type": "my_type",
            **DEFAULT_TABLE_PROPERTIES,
        }
    )

def downgrade(op):
    """Remove table or schema."""
    op.drop_table("my_schema", "my_table")
```

**Step 2: Update down_revision chain**

In the next migration file, set:
```python
down_revision = "YYYYMMDD_NNN_description"  # Points to your new migration
```

**Step 3: Run migration**

```bash
docker compose exec iceberg-alembic iceberg-migrate upgrade head
```

### Onboarding New Tenant

```bash
# Create schema for new tenant
ICEBERG_NAMESPACE=new_tenant docker compose exec iceberg-alembic \
  /app/scripts/create_schema_final.sh

# Or specify on command line
docker compose exec -e ICEBERG_NAMESPACE=new_tenant iceberg-alembic \
  /app/scripts/create_schema_final.sh

# Verify
docker compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg" | grep new_tenant
```

---

## Common Tasks

### Task 1: Verify All Migrations Applied

```bash
# Check state file
docker compose exec iceberg-alembic cat /migrations/.iceberg-alembic-state.json | jq .

# Check via Trino
docker compose exec trino trino <<EOF
SELECT COUNT(*) as table_count FROM iceberg."information_schema".tables
WHERE table_schema IN ('bronze', 'silver', 'gold');
EOF
```

**Expected Output:** 20+ tables (3 layers × multiple domains)

### Task 2: Check Table Schema

```bash
# Describe table
docker compose exec trino trino --execute \
  "DESCRIBE iceberg.bronze.imsi_level_traffic"

# Get all columns
docker compose exec trino trino --execute \
  "SELECT column_name, data_type FROM iceberg.information_schema.columns 
   WHERE table_schema = 'bronze' AND table_name = 'imsi_level_traffic'"
```

### Task 3: Add Column to Existing Table

```python
# File: new migration
def upgrade(op):
    op.add_column(
        namespace="bronze",
        table_name="imsi_level_traffic",
        column_name="new_field",
        column_type="string",
        doc="New field for XYZ purpose"
    )

def downgrade(op):
    op.drop_column("bronze", "imsi_level_traffic", "new_field")
```

### Task 4: Count Records in Each Layer

```bash
docker compose exec trino trino <<EOF
-- Bronze
SELECT COUNT(*) as bronze_records FROM iceberg.bronze.imsi_level_traffic;

-- Silver
SELECT COUNT(*) as fact_records FROM iceberg.silver.fact_imsi_level_traffic;

-- Gold
SELECT COUNT(*) as daily_records FROM iceberg.gold.imsi_level_traffic_daily;
SELECT COUNT(*) as monthly_records FROM iceberg.gold.client_partner_traffic_monthly;
EOF
```

### Task 5: Restore from Snapshot (Rollback)

```python
# If a migration caused issues, restore from Iceberg snapshot
from pyiceberg.catalog import load_catalog

catalog = load_catalog("datawarehouse-local", ...)
table = catalog.load_table(("bronze", "imsi_level_traffic"))

# Get snapshot history
for snapshot in table.snapshots():
    print(f"Snapshot {snapshot.snapshot_id}: {snapshot.timestamp_ms}")

# Rollback to specific snapshot (manual process)
# This requires Iceberg's time-travel or manual table restore
```

---

## Troubleshooting

### Issue 1: Migration State File Not Found

**Symptom:** Error when running migrations

```
FileNotFoundError: [Errno 2] No such file or directory: '.iceberg-alembic-state.json'
```

**Solution:**

```bash
# Initialize state file
docker compose exec iceberg-alembic bash -c \
  'echo "{\"applied_migrations\": [], \"current_revision\": null}" > /migrations/.iceberg-alembic-state.json'

# Verify
docker compose exec iceberg-alembic cat /migrations/.iceberg-alembic-state.json
```

### Issue 2: Namespace Already Exists Error

**Symptom:** Migration fails with "namespace already exists"

**Cause:** Namespace created in previous failed migration attempt

**Solution:**

```bash
# The code handles this gracefully, but if not:
# Option 1: Drop and recreate
docker compose exec trino trino --execute \
  "DROP SCHEMA iceberg.bronze CASCADE"

# Option 2: Re-run migration (should be idempotent)
docker compose exec iceberg-alembic iceberg-migrate upgrade head
```

### Issue 3: S3 Credentials Not Working

**Symptom:** Error connecting to S3/OCI Object Storage

```
Error: bucket is null/empty
```

**Solution:**

```bash
# Verify environment variables
docker compose exec iceberg-alembic bash -c 'echo $OCI_S3_ENDPOINT $OCI_ACCESS_KEY_ID'

# Check config file
docker compose exec iceberg-alembic cat iceberg-alembic.toml

# Update .env if needed
vim .env
docker compose restart iceberg-alembic
```

### Issue 4: Type Mismatch Error

**Symptom:** Error like "unsupported type 'bigint'"

**Solution:**

Iceberg doesn't support `bigint`. Use these types instead:

```python
# ❌ WRONG
{"name": "count", "type": "bigint"}

# ✅ CORRECT
{"name": "count", "type": "long"}
```

**Supported Types:**
- int, long (integers)
- float, double (decimals)
- decimal(precision, scale) (exact decimals)
- string, binary
- boolean
- date, timestamp
- list, map, struct (nested)

### Issue 5: Dry-Run Not Working

**Symptom:** Dry-run doesn't prevent table creation

**Solution:**

```python
# Use dry_run flag
from iceberg_alembic.operations import IcebergOperations
from iceberg_alembic.config import load_config

config = load_config()
op = IcebergOperations(config, dry_run=True)

# Now operations won't actually execute
op.create_table(...)  # Simulated only
```

### Issue 6: Migration Revision Conflict

**Symptom:** "down_revision mismatch" error

**Solution:**

```bash
# Check current state
docker compose exec iceberg-alembic \
  cat /migrations/.iceberg-alembic-state.json | jq '.current_revision'

# Fix new migration file
# Update its down_revision to match current_revision
# Example: if current is "20260704_001", new migration should have:
down_revision = "20260704_001"
```

---

## Iceberg Snapshots for Rollback

While automatic rollback is not implemented, Iceberg maintains snapshots that can be used for manual recovery:

```bash
# View snapshot history for a table
docker compose exec trino trino --execute \
  "SELECT * FROM iceberg.\"bronze\".\"imsi_level_traffic\$snapshots\""

# This shows:
# - snapshot_id: Unique identifier
# - timestamp_ms: When snapshot was created
# - summary: Changes made
```

### Manual Snapshot Recovery Steps

1. **Identify the snapshot you want to restore**
   ```sql
   SELECT snapshot_id, timestamp_ms FROM iceberg."bronze"."imsi_level_traffic$snapshots"
   WHERE timestamp_ms < <bad_migration_time>
   ORDER BY timestamp_ms DESC LIMIT 1;
   ```

2. **Create a backup table with the snapshot**
   ```sql
   CREATE TABLE iceberg.bronze.imsi_level_traffic_backup AS
   SELECT * FROM iceberg.bronze.imsi_level_traffic VERSION AS OF <snapshot_id>;
   ```

3. **Drop the corrupted table**
   ```sql
   DROP TABLE iceberg.bronze.imsi_level_traffic;
   ```

4. **Rename backup to original**
   ```sql
   ALTER TABLE iceberg.bronze.imsi_level_traffic_backup
   RENAME TO iceberg.bronze.imsi_level_traffic;
   ```

---

## Performance Considerations

### Partitioning Strategy

**Current Partitioning:**
- Bronze: By `_ingest_date` (partition pruning on daily ingestion)
- Silver: By `call_date_key` (partition pruning on call dates)
- Gold Daily: By `call_date` (partition pruning on business date)
- Gold Monthly: By `call_month` (partition pruning on month)

**Benefits:**
- Faster queries (partition elimination)
- Faster inserts (data locality)
- Easier maintenance (drop old partitions)
- Better compression (similar data grouped together)

### Column Format

**File Format:** Parquet (default)

**Compression:** Snappy (default)

**Benefits:**
- Highly compressible for string fields
- Columnar format for analytics queries
- Fast compression/decompression

---

## Best Practices

### 1. Always Link Migrations

Every new migration must reference the previous one:

```python
# ✅ CORRECT
down_revision = "20260704_001_create_forecast_dimensions"

# ❌ WRONG
down_revision = None  # Only first migration should be None
```

### 2. Use Audit Columns

Always include audit columns for traceability:

```python
columns=with_audit_columns([
    # Your business columns
])
```

### 3. Use Default Properties

Apply standard table properties:

```python
properties={
    "your_custom_property": "value",
    **DEFAULT_TABLE_PROPERTIES,  # Includes write format, compression
}
```

### 4. Name Migrations Clearly

```
YYYYMMDD_NNN_description
├── YYYYMMDD: Date of creation
├── NNN: Sequential number (001, 002, 003)
└── description: What the migration does
```

### 5. Test Migrations in Dev First

```bash
# Test in development
ICEBERG_NAMESPACE=dev docker compose exec iceberg-alembic \
  iceberg-migrate upgrade head

# Verify results
docker compose exec trino trino --execute \
  "SELECT * FROM iceberg.bronze.imsi_level_traffic LIMIT 5"

# Then promote to production
docker compose restart iceberg-alembic
```

### 6. Document Your Migrations

Every migration file should have:

```python
"""
Description of what this migration does.

Creates tables:
- table1: Purpose and grain
- table2: Purpose and grain

Related migrations:
- Previous migration: reason
- Next migration: what it builds on
"""
```

---

## References

### External Documentation

- [Apache Iceberg Docs](https://iceberg.apache.org/)
- [PyIceberg (Python Library)](https://py.iceberg.apache.org/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)

### Project Files

- Configuration: `iceberg-alembic/iceberg-alembic.toml`
- Migrations: `iceberg-alembic/migrations/versions/`
- Core Lib: `iceberg_alembic/operations.py`
- Docker: `docker/iceberg-alembic/Dockerfile`
- Entrypoint: `docker/iceberg-alembic/entrypoint.sh`

### Environment Setup

- Configuration: `.env`
- Docker Compose: `docker-compose.yml`
- Startup Script: `start.sh`

---

## Summary

This comprehensive guide covers:

✅ Complete migration architecture and design  
✅ All 9 supported operations with examples  
✅ 10 deployed migrations with detailed specifications  
✅ Multi-tenant support and namespace isolation  
✅ 3-layer Bronze-Silver-Gold architecture  
✅ Audit and traceability mechanisms  
✅ Common tasks and troubleshooting guide  
✅ Best practices and performance considerations  
✅ Rollback and recovery procedures  

**For questions or issues, refer to the README.md and deployment documentation.**

---

**Document Version:** 1.0  
**Generated:** 2026-07-19  
**Next Review:** 2026-08-19
