# Complete Traffic Data Flow Guide: S3 to Gold Tables

**Document Type:** Technical Deep Dive  
**Subject:** End-to-End Traffic Data Pipeline  
**Date:** 2026-07-19  
**Scope:** File Discovery → Bronze → Silver → Gold  

---

## Table of Contents

1. [Pipeline Overview](#pipeline-overview)
2. [Step 1: S3 File Discovery](#step-1-s3-file-discovery)
3. [Step 2: File Validation](#step-2-file-validation)
4. [Step 3: Bronze Layer Loading](#step-3-bronze-layer-loading)
5. [Step 4: Silver Layer Transformation](#step-4-silver-layer-transformation)
6. [Step 5: Gold Layer Aggregation](#step-5-gold-layer-aggregation)
7. [Error Handling & Recovery](#error-handling--recovery)
8. [Data Quality Validation](#data-quality-validation)
9. [Monitoring & Success Indicators](#monitoring--success-indicators)
10. [Troubleshooting Guide](#troubleshooting-guide)

---

## Pipeline Overview

### Complete Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         S3 LANDING BUCKET                                   │
│  (OCI Object Storage / MinIO)                                               │
│                                                                              │
│  Structure: s3://landing/tenant/{tenant_name}/Bronze/Traffic/               │
│  Files: *.parquet, *.csv                                                    │
└────────────────────────────┬────────────────────────────────────────────────┘
                             ↓
                    ┌────────────────────────┐
                    │  STEP 1: DISCOVERY     │
                    │  - List S3 files       │
                    │  - Filter by tenant    │
                    │  - Filter by format    │
                    └────────────┬───────────┘
                                 ↓
                    ┌────────────────────────┐
                    │  STEP 2: VALIDATION    │
                    │  - Format check        │
                    │  - Schema check        │
                    │  - File size check     │
                    └────────────┬───────────┘
                                 ↓
    ┌────────────────────────────────────────────────────────┐
    │  STEP 3: BRONZE LAYER (Raw/Unmodified)                │
    │  Table: bronze.imsi_level_traffic                      │
    │  - Download file from S3 to local temp                 │
    │  - Normalize column names                              │
    │  - Remap source columns to target schema               │
    │  - Add audit columns (_tenant, _ingested_at, etc.)     │
    │  - Cast to string (preserve original values)           │
    │  - Append to bronze table                              │
    │  Strategy: APPEND (immutable history)                  │
    └────────┬─────────────────────────────────┬─────────────┘
             ↓                                 ↓
    ┌─────────────────────────┐   ┌──────────────────────────┐
    │ STEP 4A: SILVER DIMS    │   │ STEP 4B: SILVER FACT     │
    │ (Dimension Tables)      │   │ (Fact Table)             │
    │                         │   │                          │
    │ Load 14 dimensions:     │   │ Transforms bronze raw    │
    │ - dim_client            │   │ data into normalized:    │
    │ - dim_operator          │   │ - fact_imsi_level_       │
    │ - dim_date              │   │   traffic (facts)        │
    │ - dim_call_type         │   │ - Join with dimensions   │
    │ - dim_imsi              │   │ - Generate keys          │
    │ - dim_apn               │   │ - Replace strings        │
    │ - dim_service_type      │   │   with dimension keys    │
    │ - dim_event_type        │   │ - Normalized schema      │
    │ - dim_rat_type          │   │ - Type casting           │
    │ - dim_tac               │   │                          │
    │ - dim_iot_rti_group     │   │                          │
    │ - dim_location          │   │                          │
    │ - dim_camel             │   │                          │
    │ - dim_traffic_direction │   │                          │
    │                         │   │                          │
    │ Strategy: UPSERT        │   │ Strategy: MERGE/UPSERT   │
    │ (only new records)      │   │                          │
    └────────┬────────────────┘   └───────────┬──────────────┘
             │                                │
             └────────────────┬───────────────┘
                              ↓
                   ┌──────────────────────────┐
                   │  STEP 5A: GOLD DAILY     │
                   │ (Denormalized & Details) │
                   │                          │
                   │ Table:                   │
                   │ gold.imsi_level_         │
                   │ traffic_daily            │
                   │                          │
                   │ - Join fact + dimensions │
                   │ - Flatten to business    │
                   │   column names           │
                   │ - Keep all detail        │
                   │ - Grain: Per day         │
                   │                          │
                   │ Strategy: APPEND         │
                   └────────┬─────────────────┘
                            ↓
                   ┌──────────────────────────┐
                   │  STEP 5B: GOLD MONTHLY   │
                   │ (Aggregated & Summarized)│
                   │                          │
                   │ Table:                   │
                   │ gold.client_partner_     │
                   │ traffic_monthly          │
                   │                          │
                   │ - Aggregate daily data   │
                   │ - SUM metrics            │
                   │ - COUNT DISTINCT        │
                   │ - Group by month         │
                   │                          │
                   │ Strategy: OVERWRITE*     │
                   │ (*Currently APPEND-BROKEN│
                   │ Fix: Use OVERWRITE)      │
                   └────────┬─────────────────┘
                            ↓
                   ┌──────────────────────────┐
                   │   ✓ GOLD DATA READY      │
                   │   For Analytics & Billing│
                   └──────────────────────────┘
```

---

## Step 1: S3 File Discovery

### Purpose
Find all unprocessed traffic data files waiting in S3 landing zone.

### S3 Directory Structure

```
s3://landing/                                          # Landing bucket
├── tenant/                                            # Source prefix
│   ├── airtel/                                        # Tenant 1
│   │   ├── Bronze/
│   │   │   ├── Traffic/
│   │   │   │   ├── traffic_2026_07_01.csv            # File to load
│   │   │   │   ├── traffic_2026_07_02.parquet        # File to load
│   │   │   │   ├── original/                         # Archived files
│   │   │   │   │   └── 2026-07-01/
│   │   │   │   │       └── traffic_2026_07_01.csv
│   │   │   │   ├── processed/                        # Already processed
│   │   │   │   │   └── traffic_2026_07_00.csv
│   │   │   │   └── failed/                           # Failed files
│   │   │   │       └── traffic_bad_format.csv
│   │   │   ├── Settlement/                           # Other domain
│   │   │   │   └── settlement_2026_07_01.csv         # Skip this
│   │   │   └── Agreement/
│   │   │       └── agreement_2026_07_01.csv          # Skip this
│   │   └── Silver/                                   # Different layer
│   │       └── Traffic/                              # Skip Silver layer
│   │
│   ├── jio/                                          # Tenant 2
│   │   └── Bronze/
│   │       └── Traffic/
│   │           ├── jio_traffic_07_01.csv
│   │           └── jio_traffic_07_02.csv
│   │
│   └── vodafone/                                     # Tenant 3
│       └── Bronze/
│           └── Traffic/
│               └── vodafone_cdr_2026_07_01.parquet
```

### Discovery Logic

**Code Location:** `dags/orchestration/traffic_ingest.py:discover_traffic_files()`

**Algorithm:**

```python
1. Connect to S3 with boto3
2. List all directories under "tenant/"
3. For each tenant found:
   a. Search for "Bronze" directory (case-insensitive)
   b. Within Bronze, search for "Traffic" directory
   c. List all files in tenant/TenantName/Bronze/Traffic/
   d. Filter files:
      - Include: *.csv, *.parquet
      - Exclude: /processed/, /failed/, /original/ subdirectories
      - Exclude: Non-traffic domains (Settlement, Agreement, etc.)
4. Return list of {file_key, tenant} tuples
5. Sort alphabetically by file_key
```

### Discovered File Configuration

```python
file_configs = [
    {
        "file_key": "tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv",
        "tenant": "airtel"
    },
    {
        "file_key": "tenant/airtel/Bronze/Traffic/traffic_2026_07_02.parquet",
        "tenant": "airtel"
    },
    {
        "file_key": "tenant/jio/Bronze/Traffic/jio_traffic_07_01.csv",
        "tenant": "jio"
    }
]
```

### Example Discovery Output

```
Found 3 traffic file(s):
  - tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  - tenant/airtel/Bronze/Traffic/traffic_2026_07_02.parquet
  - tenant/jio/Bronze/Traffic/jio_traffic_07_01.csv
```

---

## Step 2: File Validation

### Pre-Load Validation Checks

Before downloading and processing, files are validated:

#### **Check 1: File Extension**
```
Allowed:  .csv, .parquet
Rejected: .txt, .json, .xlsx, .dat, etc.
Status:   Must pass or file is skipped

Message:  "Skipping file with unsupported extension: traffic.txt"
```

#### **Check 2: File Path Validation**
```
Must contain: /Bronze/Traffic/
Must NOT contain: /processed/, /failed/, /original/, /Silver/, /Gold/

Status:   Must pass or file is skipped
Message:  "Skipping non-Bronze-Traffic file: tenant/airtel/Silver/Traffic/file.csv"
```

#### **Check 3: Domain Validation**
```
Bronze/Traffic/ only (traffic is single domain in Bronze)

Other domains to reject: Settlement, Agreement, Rating, Activation

Status:   Must pass
Message:  "Skipping Bronze non-Traffic domain file: tenant/airtel/Bronze/Settlement/file.csv"
```

#### **Check 4: File Size**
```
Minimum: > 0 bytes (not empty)
Maximum: No hard limit (handles up to 5GB+)

Spark will handle large files efficiently via Scala backend
```

### Validation Flow Chart

```
File discovered
     ↓
Is extension .csv or .parquet?
     ├─ NO → Skip file
     │         Log: "Skipping unsupported extension"
     │         Continue to next file
     │
     └─ YES ↓
        Is path in /Bronze/Traffic/?
             ├─ NO → Skip file
             │         Log: "Not Bronze/Traffic layer"
             │         Continue to next file
             │
             └─ YES ↓
                Is it a traffic domain (not Settlement/Agreement)?
                     ├─ NO → Skip file
                     │         Log: "Non-traffic domain"
                     │         Continue to next file
                     │
                     └─ YES ↓
                        Does file exist in S3?
                             ├─ NO → Skip file
                             │         Log: "File not found"
                             │         Continue to next file
                             │
                             └─ YES ↓
                                ✓ FILE VALIDATED
                                Ready for download & processing
```

---

## Step 3: Bronze Layer Loading

### Purpose
Load raw data from S3 as-is, with minimal transformation. Bronze layer is the **immutable history** of all source data.

### Process Overview

**Job Location:** `jobs/ingestion/traffic/load_bronze.py`

**DAG Step:** `load_bronze_traffic_to_iceberg` (BashOperator executing Spark job)

### Detailed Bronze Loading Process

#### **Phase 1: File Download & Format Detection**

**Input:** S3 file path (CSV or Parquet)

**For CSV Files:**
```
1. Download from S3 to local /tmp/ directory
   - Efficient for up to 5GB+ files
   - Temporary file for processing
   - Cleaned up after loading

2. Detect delimiter
   - Read first 8KB of file
   - Check for: ~ , ; | TAB
   - Count occurrences in header row
   - Use delimiter with highest count
   
   Example:
   Header: "client_pmn~partner_pmn~call_date~volume"
   ~ count: 3
   , count: 0
   | count: 0
   Result: delimiter = '~'

3. Read with Spark
   - option("header", True)           # First row is header
   - option("inferSchema", False)     # All columns as String
   - option("escape", '"')            # Quoted fields
   - option("sep", detected_delimiter)
   - option("mergeDelimiter", False)  # Don't merge delimiters
```

**For Parquet Files:**
```
1. Download from S3 to local /tmp/
   - Schema already embedded
   - Read with Spark native reader
   - No delimiter detection needed

2. Read with Spark
   - spark.read.parquet(local_path)
```

#### **Phase 2: Column Name Normalization**

**Purpose:** Handle messy source column names

**Transformations:**

```
Input Column Name           → Normalized Output
"Client PMN"               → "client_pmn"
"PARTNER_PMN"              → "partner_pmn"
"Call-Date"                → "call_date"
"Data Volume (MB)"         → "data_volume_mb"
"SMS#Count"                → "smscount"
"Call Date_2"              → "call_date_2"
"   Spaces   "             → "spaces"
"Special!@#$%Chars"        → "specialchars"
"Unicode™ñøé"              → "unicodeno"

Algorithm:
1. Remove leading/trailing spaces
2. Replace non-alphanumeric with underscore: [^0-9a-zA-Z]
3. Remove leading/trailing underscores
4. Convert to lowercase
5. If empty after cleaning → rename to "col"
6. If duplicate names → append counter (col, col_2, col_3)
```

#### **Phase 3: Column Mapping (Source → Target)**

**Purpose:** Map flexible source columns to standardized target schema

**Mapping Strategy:** **First-match wins**

For each **target column**, try source column variations in order. First match found is used.

**Example Mapping:**

```python
# Target column → [Source variations in order]
COLUMN_MAP = {
    "client_pmn": ["iot_client_pmn", "client_operator"],
    "partner_pmn": ["partner_pmn", "rp_tadig"],
    "traffic_direction": ["traffic_direction", "file_direction"],
    "call_date": ["call_date"],
    "call_type": ["call_type"],
    "imsi": ["imsi"],
    "apn": ["apn"],
    "duration": ["billed_minutes", "actual_minutes", "duration"],
    "volume": ["billed_data_volume_mb", "actual_data_volume_mb", "volume"],
    "event_count": ["number_of_sms", "cdr_count"],
    "total_charge_sdr_net": ["total_charge_sdr_net"],
    "total_charge_sdr_gross": ["total_charge_sdr_gross"],
}
```

**Process for Each Source File:**

```
Source File Columns: [iot_client_pmn, partner_pmn, traffic_direction, call_date, ...]

For target_col = "client_pmn":
    Check variations: [iot_client_pmn, client_operator]
    ✓ "iot_client_pmn" found in source
    → Map: iot_client_pmn → client_pmn

For target_col = "duration":
    Check variations: [billed_minutes, actual_minutes, duration]
    ✓ "billed_minutes" found in source
    → Map: billed_minutes → duration

For target_col = "volume":
    Check variations: [billed_data_volume_mb, actual_data_volume_mb, volume]
    ✓ "billed_data_volume_mb" found in source
    → Map: billed_data_volume_mb → volume
```

**Unmapped Targets Warning:**

```
If target columns not found in source:
WARNING: COLUMN MAPPING: 5 target columns not found in source
  - service_type_id
  - event_type_id
  - call_type_level_2
  - rat_type
  - tac_number

These will be NULL in bronze layer
Status: WARNING (non-critical, continue loading)
```

#### **Phase 4: Date Format Normalization**

**Purpose:** Standardize date formats from multiple sources

**Supported Input Formats:**

```
Format          Example         → Normalized
YYYYMMDD        20260709       → 2026-07-09
YYYY-MM-DD      2026-07-09     → 2026-07-09 (unchanged)
Empty/NULL      (empty)        → (NULL) (unchanged)
Other formats   2026/07/09     → 2026/07/09 (unchanged - preserved as-is)
```

**Implementation:**

```python
df.withColumn(
    "call_date",
    F.when(F.col("call_date").isNull() | (F.col("call_date") == ""), F.col("call_date"))
     .when(F.col("call_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.col("call_date"))  # Already correct format
     .when(F.col("call_date").rlike(r"^\d{8}$"), F.concat(
         F.substring(F.col("call_date"), 1, 4), F.lit("-"),
         F.substring(F.col("call_date"), 5, 2), F.lit("-"),
         F.substring(F.col("call_date"), 7, 2)
     ))
     .otherwise(F.col("call_date"))
)

Examples:
20260709 → 2026-07-09
2026-07-09 → 2026-07-09
20260729 → 2026-07-29
(null) → (null)
```

#### **Phase 5: Cast All to String**

**Purpose:** Preserve source data exactly, no type conversion

```python
# All columns cast to STRING
df.select([F.col(column).cast(StringType()).alias(column) for column in df.columns])

Why STRING?
- Preserve original formatting (leading zeros, decimals, etc.)
- No data loss from type conversion
- Silver layer will handle proper type casting
- Debugging easier (see exact source data)

Example:
Source: "000123"   → Bronze: "000123" (string)
        "123.456"  → Bronze: "123.456" (string)
        "TRUE"     → Bronze: "TRUE" (string)
```

#### **Phase 6: Add Audit Columns**

**Purpose:** Track data lineage and ingestion metadata

```python
Added columns (all prefixed with _):

_source_bucket      → "landing"
_source_key         → "tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv"
_source_file_name   → "traffic_2026_07_01.csv"
_source_format      → "csv" (or "parquet")
_tenant             → "airtel" (lowercase)
_layer              → "bronze"
_stage              → "traffic"
_ingested_at        → CURRENT_TIMESTAMP() (e.g., 2026-07-19 14:30:45.123)
_ingest_date        → DATE part of _ingested_at (e.g., 2026-07-19)
```

**Purpose of Each:**

| Column | Purpose |
|--------|---------|
| `_source_bucket` | Which S3 bucket file came from |
| `_source_key` | Full S3 path for traceability |
| `_source_file_name` | Filename only for grouping |
| `_source_format` | Track which format was used |
| `_tenant` | Multi-tenant filtering |
| `_layer` | Document which layer (bronze) |
| `_stage` | Track domain (traffic) |
| `_ingested_at` | Exact timestamp with milliseconds |
| `_ingest_date` | Date only for daily partitioning |

**Example Row:**

```
Original columns:
  client_pmn: "airtel"
  call_date: "2026-07-01"
  volume: "250.5"

After adding audit columns:
  client_pmn: "airtel"
  call_date: "2026-07-01"
  volume: "250.5"
  _source_bucket: "landing"
  _source_key: "tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv"
  _source_file_name: "traffic_2026_07_01.csv"
  _source_format: "csv"
  _tenant: "airtel"
  _layer: "bronze"
  _stage: "traffic"
  _ingested_at: "2026-07-19T14:30:45.123Z"
  _ingest_date: "2026-07-19"
```

#### **Phase 7: Write to Bronze Table**

**Target Table:** `{catalog}.bronze.imsi_level_traffic`

**Writing Strategy:** `.append()` (Immutable history)

```python
final_df.writeTo(table_name).append()

Why APPEND?
- Bronze is immutable source of truth
- Every ingestion is historical record
- Never update or delete bronze data
- Audit trail of all data received
- Data lineage fully traceable
```

**Table Location:** Iceberg table in warehouse

**Partitioning:** By `_ingest_date`

```
Iceberg partitions by date:
  _ingest_date=2026-07-01/
  _ingest_date=2026-07-02/
  _ingest_date=2026-07-03/
  
Benefits:
- Faster queries for specific dates
- Efficient data pruning
- Parallel processing
```

### Bronze Loading Example

**Source CSV File:**

```
iot_client_pmn,partner_pmn,call_date,billed_minutes,billed_data_volume_mb,number_of_sms
airtel,rp_1234,20260701,120,250.5,50
airtel,rp_5678,20260701,90,150.0,30
jio,rp_1234,20260701,180,500.0,100
```

**Processing Steps:**

```
Step 1: Normalize column names
  iot_client_pmn → iot_client_pmn (already normalized)
  partner_pmn → partner_pmn (already normalized)
  call_date → call_date (already normalized)
  billed_minutes → billed_minutes (already normalized)
  billed_data_volume_mb → billed_data_volume_mb (already normalized)
  number_of_sms → number_of_sms (already normalized)

Step 2: Map columns
  iot_client_pmn → client_pmn
  partner_pmn → partner_pmn (keep as is)
  call_date → call_date (keep)
  billed_minutes → duration
  billed_data_volume_mb → volume
  number_of_sms → event_count

Step 3: Normalize dates
  20260701 → 2026-07-01

Step 4: Cast to string
  All columns: STRING type

Step 5: Add audit columns
  All rows get:
    _source_bucket: "landing"
    _source_file_name: "traffic.csv"
    _source_format: "csv"
    _tenant: "airtel" (and "jio" for row 3)
    _ingested_at: "2026-07-19T14:30:45.123Z"
    _ingest_date: "2026-07-19"

Step 6: Final bronze table row
  client_pmn: "airtel"
  partner_pmn: "rp_1234"
  call_date: "2026-07-01"
  duration: "120"
  volume: "250.5"
  event_count: "50"
  _source_bucket: "landing"
  _source_file_name: "traffic.csv"
  _source_format: "csv"
  _tenant: "airtel"
  _layer: "bronze"
  _stage: "traffic"
  _ingested_at: "2026-07-19T14:30:45.123Z"
  _ingest_date: "2026-07-19"
```

### Bronze Output

```
Table: bronze.imsi_level_traffic

Total rows appended: 3
Rows by tenant:
  - airtel: 2
  - jio: 1

Sample rows:
  Row 1: airtel | rp_1234 | 2026-07-01 | 120 | 250.5 | 50 | [audit cols]
  Row 2: airtel | rp_5678 | 2026-07-01 | 90 | 150.0 | 30 | [audit cols]
  Row 3: jio | rp_1234 | 2026-07-01 | 180 | 500.0 | 100 | [audit cols]

Status: ✓ SUCCESS - 3 rows loaded to bronze
```

---

## Step 4: Silver Layer Transformation

### Purpose
Transform raw bronze data into business-normalized facts and dimensions.

### Silver Layer Architecture

```
Bronze Layer (Raw)
       ↓
   ┌───┴────────────────────────────────────┐
   ↓                                         ↓
┌──────────────────────────┐   ┌────────────────────────────┐
│ STEP 4A: DIMENSIONS      │   │ STEP 4B: FACT TABLE        │
│ (14 tables)              │   │ (1 table)                  │
│                          │   │                            │
│ Unique values extracted  │   │ Join dimensions with facts │
│ & deduplicated           │   │ Replace strings with keys  │
│                          │   │ Type casting (not string)  │
│ Strategy: UPSERT         │   │ Normalized schema          │
│ (skip existing)          │   │                            │
│                          │   │ Strategy: MERGE/UPSERT     │
│ Tables:                  │   │                            │
│ - dim_client             │   │ Table:                     │
│ - dim_operator           │   │ - fact_imsi_level_traffic  │
│ - dim_date               │   │                            │
│ - dim_call_type          │   │ This is normalization,     │
│ - dim_imsi               │   │ not denormalization        │
│ - dim_apn                │   │ (opposite of gold layer)   │
│ - dim_service_type       │   │                            │
│ - dim_event_type         │   │                            │
│ - dim_rat_type           │   │                            │
│ - dim_tac                │   │                            │
│ - dim_iot_rti_group      │   │                            │
│ - dim_location           │   │                            │
│ - dim_camel              │   │                            │
│ - dim_traffic_direction  │   │                            │
└──────────────────────────┘   └────────────────────────────┘
```

### Step 4A: Dimension Loading

**Job Location:** `jobs/ingestion/traffic/load_silver.py`

**DAG Step:** `load_silver_dimensions_and_fact` → Calls `load_silver.py`

**Purpose:** Extract unique business values and create lookup tables

**Process for Each Dimension:**

```
For each dimension (e.g., dim_client):

1. Read bronze table
2. Select relevant column (e.g., client_pmn)
3. Filter out NULLs
4. Get DISTINCT values
5. Generate deterministic key = MD5(LOWER(TRIM(value)))
6. Create dimension row:
   {
     pmn_key: MD5(LOWER(TRIM(client_pmn))),
     pmn: client_pmn,
     created_at: CURRENT_TIMESTAMP(),
     updated_at: CURRENT_TIMESTAMP()
   }
7. Upsert to dimension table:
   - If key exists → Skip (don't update)
   - If key is new → Insert
```

**Example: Loading dim_client**

```
Bronze Data:
  Row 1: client_pmn = "Airtel"
  Row 2: client_pmn = "AIRTEL"   (different case)
  Row 3: client_pmn = "  airtel  " (whitespace)
  Row 4: client_pmn = "JIO"

Processing:
  Row 1: MD5(LOWER(TRIM("Airtel"))) = MD5("airtel") = [hash1]
  Row 2: MD5(LOWER(TRIM("AIRTEL"))) = MD5("airtel") = [hash1] (duplicate!)
  Row 3: MD5(LOWER(TRIM("  airtel  "))) = MD5("airtel") = [hash1] (duplicate!)
  Row 4: MD5(LOWER(TRIM("JIO"))) = MD5("jio") = [hash2]

DISTINCT values:
  [hash1] → "Airtel"
  [hash2] → "JIO"

Dimension Table (dim_client):
  pmn_key | pmn     | created_at           | updated_at
  --------|---------|----------------------|----------------------
  [hash1] | Airtel  | 2026-07-19 14:31:00  | 2026-07-19 14:31:00
  [hash2] | JIO     | 2026-07-19 14:31:00  | 2026-07-19 14:31:00

Result: 2 unique clients (case-insensitive, whitespace-trimmed)
```

**All 14 Dimensions:**

| Dimension | Source Column(s) | Key Generation | Purpose |
|-----------|-------------------|---------------|---------| 
| dim_client | client_pmn | MD5(LOWER(TRIM(client_pmn))) | Identify home network operator |
| dim_operator | partner_pmn | MD5(LOWER(TRIM(partner_pmn))) | Identify roaming partner |
| dim_traffic_direction | traffic_direction | MD5(LOWER(TRIM(traffic_direction))) | Incoming/Outgoing |
| dim_date | call_date | MD5(LOWER(TRIM(call_date))) | Calendar dates |
| dim_call_type | call_type | MD5(LOWER(TRIM(call_type))) | Voice/SMS/Data/etc |
| dim_imsi | imsi | MD5(LOWER(TRIM(imsi))) | Specific SIM cards |
| dim_apn | apn | MD5(LOWER(TRIM(apn))) | Access point names |
| dim_service_type | service_type_id | MD5(LOWER(TRIM(service_type_id))) | Service classifications |
| dim_event_type | event_type_id | MD5(LOWER(TRIM(event_type_id))) | Event classifications |
| dim_rat_type | rat_type | MD5(LOWER(TRIM(rat_type))) | 2G/3G/4G/5G |
| dim_tac | tac_number | MD5(LOWER(TRIM(tac_number))) | Device terminal codes |
| dim_iot_rti_group | iot_rti_group_id | MD5(LOWER(TRIM(iot_rti_group_id))) | IoT device groups |
| dim_location | destination | MD5(LOWER(TRIM(destination))) | Roaming destinations |
| dim_camel | is_camel | MD5(LOWER(TRIM(is_camel))) | CAMEL service indicator |

**Upsert Logic:**

```python
def upsert_dimension(spark, bronze_df, key_col, table_name, select_cols):
    # Get distinct values from bronze
    new_df = bronze_df.select(*select_cols).distinct()
    
    # Try to read existing dimension
    try:
        existing_df = spark.table(table_name).select(key_col)
        existing_keys = existing_df.rdd.map(lambda r: r[0]).collect()
        
        # Filter to only new records (keys not already in dimension)
        new_df = new_df.filter(~F.col(key_col).isin(existing_keys))
        new_count = new_df.count()
        
        if new_count > 0:
            new_df.writeTo(table_name).append()
            logger.info("Inserted %d new records to %s", new_count, table_name)
        else:
            logger.info("No new records for %s (all exist)", table_name)
        return new_count
    except Exception as e:
        # First time loading this dimension
        logger.warning("Could not read existing %s, assuming fresh: %s", table_name, e)
        new_df.writeTo(table_name).append()
        return new_df.count()
```

### Step 4B: Fact Table Loading

**Job Location:** `jobs/ingestion/traffic/load_silver_fact.py`

**DAG Step:** `load_silver_dimensions_and_fact` → Calls `load_silver_fact.py`

**Purpose:** Create normalized fact table with dimension keys

**Process:**

```
1. Read bronze table
2. For each row, look up dimension keys:
   a. client_pmn → Join with dim_client → Get pmn_key
   b. partner_pmn → Join with dim_operator → Get pmn_key
   c. call_date → Join with dim_date → Get call_date_key
   ... (12 more dimension lookups)
3. Build fact row with:
   - Dimension keys (FK to dimensions)
   - Metrics (duration, volume, event_count, charges)
   - Type casting (string → numeric where appropriate)
4. Insert/Update to fact table (merge pattern)
```

**Fact Table Schema:**

```
Dimension Keys (Foreign Keys):
  client_pmn_key          → Links to dim_client
  partner_pmn_key         → Links to dim_operator
  traffic_direction_key   → Links to dim_traffic_direction
  call_date_key           → Links to dim_date
  call_type_key           → Links to dim_call_type
  imsi_key                → Links to dim_imsi
  apn_key                 → Links to dim_apn
  service_type_key        → Links to dim_service_type
  event_type_key          → Links to dim_event_type
  rat_type_key            → Links to dim_rat_type
  tac_key                 → Links to dim_tac
  iot_rti_group_key       → Links to dim_iot_rti_group
  destination_key         → Links to dim_location
  camel_key               → Links to dim_camel

Metrics (Numeric):
  duration                → DECIMAL(18,6)
  volume                  → DECIMAL(18,6)
  event_count             → BIGINT
  total_charge_sdr_net    → DECIMAL(18,6)
  total_charge_sdr_gross  → DECIMAL(18,6)

Metadata:
  created_at              → TIMESTAMP
  updated_at              → TIMESTAMP
```

**Example Fact Row Construction:**

```
Bronze Row:
  client_pmn: "airtel"
  partner_pmn: "rp_1234"
  call_date: "2026-07-01"
  call_type: "Voice"
  volume: "250.5"

Lookups:
  Lookup "airtel" in dim_client:
    → Find MD5("airtel") key
    → Get pmn_key = "abc123def..."
  
  Lookup "rp_1234" in dim_operator:
    → Find MD5("rp_1234") key
    → Get pmn_key = "xyz789uvw..."
  
  Lookup "2026-07-01" in dim_date:
    → Find MD5("2026-07-01") key
    → Get call_date_key = "date001..."
  
  Lookup "Voice" in dim_call_type:
    → Find MD5("voice") key
    → Get call_type_key = "type001..."

Fact Row:
  client_pmn_key: "abc123def..."
  partner_pmn_key: "xyz789uvw..."
  call_date_key: "date001..."
  call_type_key: "type001..."
  ...other dimension keys...
  duration: 120 (numeric, not string)
  volume: 250.5 (numeric)
  event_count: 50 (numeric)
  total_charge_sdr_net: 100.00 (numeric)
  total_charge_sdr_gross: 120.00 (numeric)
```

---

## Step 5: Gold Layer Aggregation

### Purpose
Create denormalized and aggregated business-ready tables for analytics and billing.

### Step 5A: Gold Daily (Denormalized)

**Job Location:** `jobs/ingestion/traffic/load_gold_daily.py`

**Process:**

```
1. Read silver fact table
2. Read all 13 dimension tables
3. JOIN fact with each dimension:
   - fact.client_pmn_key = dim_client.pmn_key
   - fact.partner_pmn_key = dim_operator.pmn_key
   ... (11 more joins)
4. Flatten: Replace all dimension keys with business values
5. Select business columns only (drop all _key columns)
6. Cast metrics to decimal/bigint (not string)
7. Add timestamps (created_at, updated_at)
8. Write to gold.imsi_level_traffic_daily (APPEND)
```

**Denormalization Example:**

```
Input (Normalized Silver):
  client_pmn_key: "abc123def..."
  partner_pmn_key: "xyz789uvw..."
  call_date_key: "date001..."
  volume: "250.5"

After 13 Joins:
  client_pmn_key: "abc123def..."
  pmn (from dim_client): "airtel"
  partner_pmn_key: "xyz789uvw..."
  pmn (from dim_operator): "rp_1234"
  call_date_key: "date001..."
  call_date (from dim_date): "2026-07-01"
  volume: "250.5"

Output (Denormalized Gold Daily):
  client_pmn: "airtel"
  partner_pmn: "rp_1234"
  call_date: "2026-07-01"
  total_volume: 250.5 (cast to DECIMAL)
```

### Step 5B: Gold Monthly (Aggregated)

**Job Location:** `jobs/ingestion/traffic/load_gold_monthly.py`

**Process:**

```
1. Read gold.imsi_level_traffic_daily (ALL rows, all days)
2. GROUP BY:
   - client_pmn, partner_pmn, roaming_partner_country
   - traffic_direction, call_month, year, month
   - call_type, call_type_level_2
   - service_type_id, event_type_id
3. AGGREGATE (SUM):
   - total_duration = SUM(daily.total_duration)
   - total_volume = SUM(daily.total_volume)
   - total_event_count = SUM(daily.total_event_count)
   - total_charge_sdr_net = SUM(daily.total_charge_sdr_net)
   - total_charge_sdr_gross = SUM(daily.total_charge_sdr_gross)
4. DISTINCT COUNTS:
   - distinct_imsi_count = COUNT(DISTINCT imsi)
   - distinct_apn_count = COUNT(DISTINCT apn)
5. Write to gold.client_partner_traffic_monthly
   
⚠️  CURRENT: .append() → Creates duplicates
✓  SHOULD BE: .overwritePartitions() → Correct aggregation
```

---

## Error Handling & Recovery

### Error Types & Recovery

#### **Error Type 1: S3 File Not Found**

```
Cause: File disappeared after discovery but before download

Detection:
  S3.get_object() throws NoSuchKey exception

Recovery:
  - Log error with file key
  - Skip file and continue
  - File will be picked up again next run if reappears
  - DAG continues with other files

Message: "Warning: Failed to access S3 file after discovery: 
         s3://landing/tenant/airtel/Bronze/Traffic/file.csv"

Impact: LOW - One file missed, retry on next DAG run
```

#### **Error Type 2: Unsupported CSV Delimiter**

```
Cause: CSV has non-standard delimiter that wasn't detected

Detection:
  CSV read fails or produces wrong column count

Recovery:
  1. Use default delimiter (comma)
  2. Spark may fail during read if wrong delimiter used
  3. Job fails and goes to error state

Resolution:
  - Manually verify CSV format
  - Update file or provide FORMAT_HINT environment variable
  - Retry DAG run

Impact: HIGH - File fails to load, manual intervention needed
```

#### **Error Type 3: Column Mapping Failure**

```
Cause: None of the source column variations found

Detection:
  Target column marked as "unmapped" in logs

Recovery:
  - Column becomes NULL in bronze table
  - Non-critical if not required (e.g., service_type_id, event_type_id)
  - Critical if required (e.g., client_pmn, call_date)

Bronze Loading Result:
  - If critical column is missing: Job FAILS
  - If optional column is missing: Job CONTINUES (WARNING)

Message: "WARNING: COLUMN MAPPING: 5 target columns not found in source:
         service_type_id, event_type_id, call_type_level_2, rat_type, tac_number"

Impact: MEDIUM - Data completeness reduced, analysis affected
```

#### **Error Type 4: Date Format Parse Error**

```
Cause: Date in unsupported format

Detection:
  Date normalization fails if non-standard format

Recovery:
  - Keep original date value as-is
  - Bronze stores as STRING (no validation error)
  - Silver layer may fail on date operations

Impact: MEDIUM - Date filtering breaks, dimension lookup fails
```

#### **Error Type 5: Spark Job Timeout**

```
Cause: Large file or slow processing

Detection:
  Spark job runs longer than timeout threshold

Timeout Setting:
  DAG setting: timeout=3600 seconds (1 hour per tenant)

Recovery:
  - Job killed after timeout
  - Partial data may be written (rolled back if transaction supported)
  - DAG fails and triggers retry logic

Message: "BashOperator task for load_bronze_traffic_to_iceberg timed out
         after 1 hour"

Retry Logic:
  - retries: 2 (retry 2 times)
  - retry_delay: 5 minutes between retries
  - After 2 retries fail: DAG fails and alerts

Impact: HIGH - Entire ingestion fails, delays data pipeline
```

#### **Error Type 6: Iceberg Table Write Conflict**

```
Cause: Concurrent writes to same table/partition

Detection:
  Iceberg merge/append operation fails with write conflict

Recovery:
  - Spark job fails
  - Manual review needed
  - Check for other running processes

Prevention:
  - DAG setting: max_active_runs=1 (only one DAG run at a time)
  - DAG setting: max_active_tis_per_dagrun=1 (sequential file processing)

Impact: HIGH - Data consistency risk, manual intervention
```

#### **Error Type 7: Archive Operation Failure**

```
Cause: S3 copy to /original/ subdirectory fails

Detection:
  archive_traffic_originals task fails

Recovery:
  - Non-critical (doesn't affect data loading)
  - File is logged but not archived
  - Next run may retry archiving
  - Continue with bronze load anyway

Message: "Warning: Failed to archive 
         s3://landing/tenant/airtel/Bronze/Traffic/file.csv → 
         s3://landing/tenant/airtel/Bronze/Traffic/original/2026-07-19/file.csv"

Impact: LOW - Operational (no data loading impact)
```

### DAG Error Handling Configuration

```python
default_args={
    "retries": 2,              # Retry failed tasks 2 times
    "retry_delay": 5 minutes,  # Wait 5 min between retries
}

DAG settings={
    "max_active_runs": 1,      # Only one DAG run at a time
    "max_active_tis_per_dagrun": 1,  # Sequential task processing
    "execution_timeout": 1 hour,  # Per task timeout
}

Critical Tasks (no retry):
  - load_bronze_traffic_to_iceberg: retries=0
  - (Retries controlled at DAG level instead)
```

### Recovery Actions

**Automatic (System Handles):**
- Task retry (2 times, 5 min intervals)
- Partial failures skip and continue
- DAG continues with remaining files

**Manual (Operator Action):**
- Review error logs in Airflow UI
- Fix configuration/data issue
- Trigger DAG re-run
- Monitor logs for success

**Monitoring Dashboards:**
- Airflow DAG success/failure rate
- Table row counts (bronze, silver, gold)
- Processing duration per file
- Error logs and warnings

---

## Data Quality Validation

### Validation Points in Pipeline

#### **Phase 1: Discovery Validation**

```
Check: File exists in S3
  ├─ Path validation: /Bronze/Traffic/
  ├─ Extension validation: .csv or .parquet
  └─ Domain validation: traffic (not settlement/agreement)
  
Status: PASS/SKIP (file not loaded if fails)
```

#### **Phase 2: Pre-Load Validation**

```
Check: File format compatibility
  ├─ CSV: Can detect delimiter
  └─ Parquet: Has valid schema
  
Check: File not empty
  ├─ File size > 0 bytes
  └─ Contains at least 1 row
  
Status: PASS/FAIL (job fails if fails)
```

#### **Phase 3: Bronze Load Validation**

```
Check: Column presence
  ├─ At least some target columns mapped
  ├─ Critical columns (client_pmn, call_date) exist
  └─ Optional columns may be NULL
  
Check: Row count
  ├─ At least 1 row loaded
  ├─ Log warning if unusual counts
  └─ Log if 0 rows (empty file)
  
Check: Data types
  ├─ All columns STRING (by design)
  ├─ Audit columns added
  └─ Timestamps created correctly
  
Status: Count rows loaded
  Message: "Loaded 1,234,567 rows to bronze.imsi_level_traffic"
```

#### **Phase 4: Silver Load Validation**

```
Check: Dimension records
  ├─ Count unique values per dimension
  ├─ Verify MD5 keys generated
  └─ Log new vs existing counts
  
Check: Fact table joins
  ├─ Verify all dimension lookups succeed
  ├─ No NULL foreign keys (unless optional)
  └─ Count fact rows created
  
Status: Summary statistics
  Message: "Loaded 14 dimensions and 1 fact table
           Dimensions: dim_client (10 new, 5 existing)
           Fact table: 1,234,567 rows"
```

#### **Phase 5: Gold Load Validation**

```
Check: Denormalization successful
  ├─ Gold daily rows match fact rows count
  ├─ Business columns populated (not NULL)
  └─ Metrics are numeric (not string)
  
Check: Aggregation accuracy
  ├─ Duplicate check: no duplicate (client, partner, month)
  ├─ Totals validation: gold sum = daily sum (per month/client)
  └─ Distinct counts non-zero where expected
  
Status: Counts and reconciliation
  Message: "Loaded 1,234,567 rows to gold daily
           Loaded 12,345 rows to gold monthly
           Reconciliation: PASS (sums match daily totals)"
```

### Reconciliation Queries

**Daily vs Bronze Counts:**

```sql
-- Should match (or bronze > daily if some rows NULL out after mapping)
SELECT 
  COUNT(*) as bronze_count
FROM bronze.imsi_level_traffic;

SELECT 
  COUNT(*) as daily_count
FROM gold.imsi_level_traffic_daily;
```

**Monthly vs Daily Aggregation:**

```sql
-- Should match
SELECT 
  SUM(total_volume) as daily_total
FROM gold.imsi_level_traffic_daily
WHERE call_month = '2026-07';

SELECT 
  SUM(total_volume) as monthly_total
FROM gold.client_partner_traffic_monthly
WHERE call_month = '2026-07';
```

---

## Monitoring & Success Indicators

### Pipeline Success Indicators

**Complete Success:** All tasks pass

```
✓ discover_traffic_files     → Found 5 files
✓ archive_traffic_originals  → Archived 5 files
✓ prepare_traffic_load_commands  → Generated 5 commands
✓ load_bronze_traffic_to_iceberg → Loaded 5 files
✓ load_silver_dimensions_and_fact → Loaded dimensions + fact
✓ load_gold_layer            → Loaded daily + monthly
```

### Key Metrics to Monitor

#### **Metric 1: File Discovery**

```
Expected: > 0 files found
Warning: 0 files found (check if files exist in S3)

Example Output:
  Found 5 traffic file(s)
  - tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  - tenant/airtel/Bronze/Traffic/traffic_2026_07_02.parquet
  - tenant/jio/Bronze/Traffic/jio_2026_07_01.csv
  - ...
```

#### **Metric 2: Row Counts by Layer**

```
Bronze Layer:
  Rows loaded: 1,234,567
  File count: 5
  Average per file: 246,913

Silver Layer:
  Dimensions: 14 tables
    - dim_client: 50 unique values
    - dim_operator: 100 unique values
    - ... (12 more)
  Fact table: 1,234,567 rows

Gold Layer:
  Daily: 1,234,567 rows
  Monthly: 12,345 rows (1,234,567 / ~100 per month aggregation)
```

#### **Metric 3: Processing Duration**

```
Bronze load: 5 minutes
Silver load: 8 minutes
Gold load: 6 minutes
Total: 19 minutes

Expected: < 30 minutes for typical load
Alert: If > 60 minutes, investigate bottleneck
```

#### **Metric 4: Data Quality Score**

```
Column mapping success: 95%
  (Most columns found in source)

Date format normalization: 100%
  (All dates in YYYY-MM-DD)

Row preservation: 98%
  (98% of rows loaded, 2% skipped due to data issues)

Dimension uniqueness: PASS
  (No duplicate keys in dimensions)
```

#### **Metric 5: Error Count**

```
Expected: 0 critical errors
Acceptable: 0-2 warnings (non-blocking)

Examples:
  - 0 critical errors ✓
  - 1 warning: "5 unmapped target columns (optional)"
  - 0 failures ✓
```

### Log Monitoring

**Key Log Patterns to Check:**

```
✓ SUCCESS patterns:
  "Successfully read CSV file"
  "CSV DELIMITER DETECTED: '~'"
  "Loaded X rows to bronze.imsi_level_traffic"
  "Inserted X new records to dim_client"
  "Loaded X rows to gold.imsi_level_traffic_daily"

⚠️  WARNING patterns:
  "Column mapping: X target columns not found"
  "Skipping file with unsupported extension"
  "No new records for dim_xxx (all exist)"

❌ ERROR patterns:
  "Failed to list traffic files from S3"
  "Failed to read CSV"
  "Failed to load dimensions for tenant"
  "Failed to load gold daily table"
```

### Alerting Thresholds

| Metric | OK | Warning | Critical |
|--------|----|---------|---------| 
| Files found | > 0 | 0 | N/A |
| Rows loaded (bronze) | > 1000 | < 1000 | 0 |
| Bronze → Gold ratio | 80-100% | 50-79% | < 50% |
| Processing time | < 30 min | 30-60 min | > 60 min |
| Error count | 0 | 1-2 | > 2 |
| DAG success rate | 99-100% | 90-98% | < 90% |

---

## Troubleshooting Guide

### Common Issues & Solutions

**Issue 1: "No traffic files found"**

```
Symptom:
  DAG runs but discovers 0 files

Causes:
  1. No files uploaded to S3
  2. Files in wrong S3 directory
  3. Wrong tenant prefix
  4. S3 credentials invalid

Diagnosis:
  1. Check S3 bucket: s3://landing/tenant/{tenant}/Bronze/Traffic/
  2. Verify files exist: aws s3 ls s3://landing/tenant/
  3. Check file extensions: must be .csv or .parquet
  4. Verify S3 credentials in Airflow connections

Solution:
  1. Upload files to correct S3 path
  2. Check file naming conventions
  3. Verify S3 access permissions
  4. Re-run DAG: airflow dags trigger bronze_traffic_ingest
```

**Issue 2: "CSV file has 0 rows"**

```
Symptom:
  CSV loads but produces 0 rows

Causes:
  1. File is empty
  2. Delimiter detection failed
  3. All rows filtered out during normalization

Diagnosis:
  Check file size: aws s3 ls s3://landing/.../file.csv
  Head file: aws s3 cp s3://landing/.../file.csv - | head -5

Solution:
  1. Verify file is not empty
  2. Check CSV format/delimiter
  3. Manually upload test file
  4. Check data quality before upload
```

**Issue 3: "Column mapping - target columns not found"**

```
Symptom:
  Warning: "5 target columns not found in source"

Causes:
  Source file missing expected columns

Solution:
  1. Not critical if optional columns (service_type_id, etc.)
  2. Critical if required columns (client_pmn, call_date)
  3. Add missing columns to source CSV
  4. Update mapping configuration if needed
```

**Issue 4: "Gold monthly table has duplicates"**

```
Symptom:
  Query returns 310 GB instead of expected 210 GB

Root Cause:
  .append() is creating duplicate rows

Solution:
  ✓ APPLY THE FIX:
    Change line 110 in load_gold_monthly.py:
    FROM: final_df.writeTo(table_name).append()
    TO: final_df.writeTo(table_name).overwritePartitions()
```

**Issue 5: "Spark job timeout"**

```
Symptom:
  BashOperator task fails after 1 hour

Causes:
  1. Very large file (> 5GB)
  2. Slow S3 connection
  3. Slow Spark cluster
  4. Resource contention

Solution:
  1. Increase timeout in DAG (change 3600 to 7200 seconds)
  2. Check S3/Spark cluster performance
  3. Split large files into smaller chunks
  4. Schedule DAG at off-peak hours
```

**Issue 6: "Iceberg write conflict"**

```
Symptom:
  Iceberg merge operation fails with write conflict

Causes:
  Concurrent writes to same partition

Solution:
  1. Ensure max_active_runs=1 in DAG
  2. Ensure max_active_tis_per_dagrun=1
  3. Check for manual/external writes to table
  4. Wait for current run to complete
```

---

## Summary Table

| Phase | Component | Input | Output | Strategy | Status |
|-------|-----------|-------|--------|----------|--------|
| Discovery | S3 scan | S3 path | File list | List & filter | ✓ Working |
| Validation | Pre-checks | File path | Valid/Skip | Format check | ✓ Working |
| Bronze | Raw load | S3 file (CSV/Parquet) | 1,234,567 rows | Append | ✓ Working |
| Silver Dim | Dimension | Bronze table | 14 dim tables | Upsert | ✓ Working |
| Silver Fact | Fact table | Bronze + Dims | 1,234,567 fact rows | Merge | ✓ Working |
| Gold Daily | Denormalize | Silver fact+dims | 1,234,567 daily rows | Append | ✓ Working |
| Gold Monthly | Aggregate | Gold daily | 12,345 month rows | **Append ❌** | ⚠️ **FIX: OVERWRITE** |

---

**End of Document**
