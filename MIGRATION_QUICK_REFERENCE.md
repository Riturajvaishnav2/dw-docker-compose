# Iceberg Migration Quick Reference

**Document Size:** 1273 lines (comprehensive guide)  
**Location:** `ICEBERG_MIGRATION_GUIDE.md`

---

## What Was Documented

### 1. Migration Tool Overview
- **What:** Alembic + iceberg-alembic extension for Apache Iceberg
- **Why:** Version control, atomic operations, automatic execution, state management
- **Configuration:** `iceberg-alembic.toml` with S3/OCI credentials
- **State:** `.iceberg-alembic-state.json` prevents duplicate execution

### 2. Architecture Details
- **3-Layer:** Bronze (raw) → Silver (clean) → Gold (business-ready)
- **Multi-Tenant:** Namespace isolation via ICEBERG_NAMESPACE env var
- **Partitioning:** By date fields for performance
- **Audit Trail:** Complete source tracking with standard audit columns

### 3. 9 Core Operations

| Operation | Status | Use Case |
|-----------|--------|----------|
| `create_namespace()` | ✅ In Use | Create bronze, silver, gold schemas |
| `create_table()` | ✅ In Use | Create new tables |
| `drop_table()` | ⚠️ Available | Remove tables |
| `add_column()` | ⚠️ Available | Extend existing tables |
| `drop_column()` | ⚠️ Available | Remove deprecated columns |
| `rename_column()` | ⚠️ Available | Standardize naming |
| `update_column_type()` | ⚠️ Available | Type evolution |
| `rename_table()` | ⚠️ Available | Refactor tables |
| `capture_state()` | ✅ In Use | Snapshot for rollback |

### 4. Feature Support Matrix

**Schema Mutation:** ✅ 10/10 features supported  
**Migration Management:** ✅ 8/9 features supported (no auto rollback)  
**Tenant Management:** ✅ 10/10 features supported  
**File Paths:** ✅ 9/9 features supported  

### 5. 10 Deployed Migrations

#### Migration Chain
```
20260604_005 (Bronze Traffic)
    ↓
20260617_006 (Gold Settlement)
    ↓
20260623_008 (Silver Dimensions - 14 tables)
    ↓
20260623_009 (Silver Fact - IMSI Level Traffic)
    ↓
20260623_010 (Gold Traffic - Daily & Monthly)
    ↓
20260703_001 (Forecast Bronze)
    ↓
20260703_002 (Forecast Silver)
    ↓
20260703_003 (Forecast Gold)
    ↓
20260704_001 (Forecast Dimensions)
```

### 6. Table Inventory

**Total Tables Created:** 20+

#### Bronze Layer (2 tables)
- `imsi_level_traffic` - 33 columns, partitioned by `_ingest_date`
- `settlement_detail` - Similar structure

#### Silver Layer (15 tables)
- 14 Dimension Tables
  - dim_client, dim_operator, dim_traffic_direction
  - dim_date, dim_call_type, dim_imsi, dim_apn
  - dim_service_type, dim_event_type, dim_rat_type
  - dim_tac, dim_iot_rti_group, dim_location, dim_camel

- 1 Fact Table
  - fact_imsi_level_traffic - 26 columns with 18 foreign keys

#### Gold Layer (3+ tables)
- `imsi_level_traffic_daily` - 28 columns, partitioned by `call_date`
- `client_partner_traffic_monthly` - 19 columns, partitioned by `call_month`
- `settlement_summary` - Business-ready summary

#### Forecast Layer (4 tables)
- Forecast Bronze, Silver, Gold tables + Dimensions

### 7. Column Specifications

#### Bronze: IMSI Level Traffic (33 columns)

**Core Fields:**
- client_pmn, partner_pmn, traffic_direction, call_date, call_type
- imsi, apn, duration, volume, event_count, roamer_indicator

**Financial:**
- total_charge_sdr_net, total_charge_sdr_gross

**Technical:**
- iot_rti_group_id, service_type_id, event_type_id
- call_type_level_2, rat_type, tac_number

**Roaming:**
- roaming_partner_country, destination_category, destination, is_camel

**Audit (8 auto-populated):**
- _source_bucket, _source_key, _source_file_name, _source_format
- _domain, _layer, _ingested_at, _ingest_date

**Processing:**
- source_system, source_file_name, ingestion_id, batch_id
- record_hash, received_at, processing_status, error_message

#### Silver: Fact Table (26 columns)

**Fact ID:** traffic_fact_id

**Foreign Keys (18):**
- client/partner/direction/date/month/call_type/imsi/apn/roamer/service/event/rat/tac/group/destination/camel

**Measures (5):**
- duration, volume, event_count (all decimal or long types)
- total_charge_sdr_net, total_charge_sdr_gross

**Audit:**
- source_system, batch_id, record_hash, created_at, updated_at

#### Gold: Daily (28 columns)

**Denormalized Dimensions (22):**
- All client/partner/date/call attributes expanded from keys
- Direction, roaming country, destination, camel indicator

**Aggregated Measures (5):**
- total_duration, total_volume, total_event_count
- total_charge_sdr_net, total_charge_sdr_gross

**Audit:**
- created_at, updated_at

#### Gold: Monthly (19 columns)

**Aggregated Dimensions (11):**
- Client, partner, direction, month, call type, service

**Aggregated Measures (5):**
- Same totals as daily

**Distinct Counts (2):**
- distinct_imsi_count, distinct_apn_count

---

## Key Technical Details

### Type System

**Supported Types:**
```
Integers:  int, long
Decimals:  float, double, decimal(18,6)
Strings:   string, binary
Dates:     date, timestamp
Boolean:   boolean
Complex:   list, map, struct
```

**NOT Supported:**
- ❌ `bigint` (use `long` instead)

### Partitioning Strategy

| Layer | Partition Key | Reason |
|-------|---|---|
| Bronze | `_ingest_date` | Separate by daily ingestion |
| Silver Fact | `call_date_key` | Query by business date |
| Gold Daily | `call_date` | Query by calendar date |
| Gold Monthly | `call_month` | Query by month |

### Storage Format

- **File Format:** Parquet (columnar, compressible)
- **Compression:** Snappy (fast compression)
- **Partitioning:** Identity (no transform)
- **Location:** `s3://warehouse/{tenant}/{layer}/{table}/`

### Deterministic Keys

All dimension keys use:
```
md5(lower(trim(source_value)))
```

**Benefits:**
- Consistent across loads
- Handles whitespace/case variations
- Deduplicates automatically

### Audit Columns (Standardized)

```python
# Bronze layer
_source_bucket, _source_key, _source_file_name, _source_format
_domain, _layer, _ingested_at, _ingest_date

# Silver/Gold layers  
source_system, batch_id, record_hash
created_at, updated_at
```

---

## Common Operations

### View Current Migrations
```bash
docker compose exec iceberg-alembic cat /migrations/.iceberg-alembic-state.json | jq .
```

### Run Migrations Manually
```bash
docker compose exec iceberg-alembic iceberg-migrate upgrade head
```

### Create Schema for Tenant
```bash
ICEBERG_NAMESPACE=tenant_name docker compose exec iceberg-alembic \
  /app/scripts/create_schema_final.sh
```

### Query Table Schema
```bash
docker compose exec trino trino --execute \
  "DESCRIBE iceberg.bronze.imsi_level_traffic"
```

### Count Records
```bash
docker compose exec trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.imsi_level_traffic"
```

### View Table Properties
```bash
docker compose exec trino trino --execute \
  "SELECT * FROM iceberg.\"bronze\".\"imsi_level_traffic$properties\""
```

### Create New Migration File

```python
# File: iceberg-alembic/migrations/versions/YYYYMMDD_NNN_description.py

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES, with_audit_columns
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "YYYYMMDD_NNN_description"
down_revision = "YYYYMMDD_NNN_previous"

CATALOG_NAME = get_catalog_name()

def upgrade(op):
    op.create_table(
        namespace="schema_name",
        table_name="table_name",
        columns=with_audit_columns([
            {"name": "col1", "type": "string", "source_name": "col1"},
        ]),
        partition_by=["_ingest_date"],
        properties=DEFAULT_TABLE_PROPERTIES,
    )

def downgrade(op):
    op.drop_table("schema_name", "table_name")
```

---

## Migration Status Summary

### ✅ Fully Implemented
- Framework & integration
- All 10 migrations deployed
- State management
- Multi-tenant support
- Automatic startup
- Audit logging
- Partitioning
- Exception handling
- S3/OCI integration

### ⚠️ Partially Implemented
- Rollback (snapshots available, manual recovery)
- Tenant validation (basic checks)
- Path validation (basic checks)
- Testing framework (not comprehensive)

### ❌ Not Implemented
- Automated rollback
- Schema evolution migrations (ops available, not used)
- GUI/Web interface
- Auto-validation post-migration

---

## Performance Notes

### Partitioning Benefits
- Partition elimination → faster queries
- Data locality → faster inserts
- Easier maintenance → drop old partitions
- Better compression → similar data grouped

### Compression
- Snappy provides ~50-70% compression on text data
- Parquet columnar format ideal for analytics queries
- No overhead from compression/decompression

### Expected Performance
- Bronze ingestion: ~1-5 seconds per file
- Silver aggregation: ~30-90 seconds per load
- Gold denormalization: ~90-120 seconds per load

---

## Disaster Recovery

### Snapshot History
```bash
docker compose exec trino trino --execute \
  "SELECT * FROM iceberg.\"bronze\".\"imsi_level_traffic\$snapshots\""
```

### Manual Recovery Steps
1. Find pre-error snapshot
2. Create backup table from snapshot
3. Drop corrupted table
4. Rename backup to original
5. Verify data integrity

---

## Key Files Location

| File | Purpose | Size |
|------|---------|------|
| `ICEBERG_MIGRATION_GUIDE.md` | Full documentation | 1273 lines |
| `iceberg-alembic/iceberg-alembic.toml` | Configuration | 36 lines |
| `iceberg-alembic/iceberg_alembic/operations.py` | 9 core operations | 358 lines |
| `iceberg-alembic/migrations/versions/*.py` | 10 migrations | ~300 lines total |
| `docker/iceberg-alembic/entrypoint.sh` | Container startup | ~50 lines |

---

## Quick Stats

- **Total Migrations:** 10
- **Total Tables:** 20+
- **Total Columns:** 200+ (including audit columns)
- **Domains:** 3 (Traffic, Settlement, Forecast)
- **Layers:** 3 (Bronze, Silver, Gold)
- **Tenants:** Unlimited (via namespace isolation)
- **Supported Operations:** 9 core operations
- **Audit Columns:** 8 standardized fields
- **Documentation:** 1273 lines (comprehensive guide)

---

## Next Steps

1. **Review** the full `ICEBERG_MIGRATION_GUIDE.md` for complete details
2. **Test** migrations in development environment
3. **Monitor** migration logs after deployment
4. **Document** any custom migrations you create
5. **Plan** schema evolution if needed (use operations 4-8)

---

**For complete details, see:** `ICEBERG_MIGRATION_GUIDE.md`
