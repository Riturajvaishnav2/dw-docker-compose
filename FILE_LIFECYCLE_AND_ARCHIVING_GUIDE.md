# Traffic Data - File Lifecycle & Archiving Strategy

**Document Type:** Operational Guide  
**Subject:** Complete file handling from discovery through archiving  
**Date:** 2026-07-19  
**Scope:** File movement, archiving, error handling, and retention

---

## Table of Contents

1. [File Lifecycle Overview](#file-lifecycle-overview)
2. [Step-by-Step File Journey](#step-by-step-file-journey)
3. [Directory Structure](#directory-structure)
4. [Discovery Phase](#discovery-phase)
5. [Processing Phase](#processing-phase)
6. [Success Path](#success-path)
7. [Error Path](#error-path)
8. [Archive Strategy](#archive-strategy)
9. [Retention Policy](#retention-policy)
10. [Monitoring File States](#monitoring-file-states)

---

## File Lifecycle Overview

### Complete File Journey

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FILE LIFECYCLE WORKFLOW                                   │
└─────────────────────────────────────────────────────────────────────────────┘

S3 LANDING BUCKET
     ↓
┌────────────────────────┐
│ STEP 1: DISCOVERY      │
│ Identify new files     │ File State: INCOMING (not processed/failed/original)
│ in Bronze/Traffic/     │
└────────┬───────────────┘
         ↓
    ┌─────────────────────┐
    │ File in Processing  │ File State: PROCESSING
    │ Location: Original  │
    └────────┬────────────┘
             ↓
    ┌──────────────────────────────────────────────┐
    │ Execute Loading Pipeline                     │
    │ ├─ Bronze layer load                         │
    │ ├─ Silver layer load                         │
    │ └─ Gold layer load                           │
    │                                              │
    │ File State: LOADING (in memory/temp)         │
    └──────┬──────────────────────────────────────┘
           ↓
    ┌──────────────────────────────────────────────┐
    │  SUCCESS                   ERROR             │
    │                                              │
    ├─ All steps pass      ├─ Bronze load fails   │
    ├─ Data validated      ├─ Silver load fails   │
    ├─ Row counts OK       ├─ Gold load fails     │
    │                      ├─ Validation fails    │
    │                      └─ Timeout/crash       │
    └────┬─────────────────┬──────────────────────┘
         ↓                 ↓
    ┌─────────────┐   ┌──────────────┐
    │ SUCCESS     │   │ ERROR        │
    │ PATH        │   │ PATH         │
    │             │   │              │
    └─────┬───────┘   └────┬─────────┘
          ↓                ↓
    ┌─────────────┐   ┌──────────────────────────┐
    │ Archive to  │   │ Move to Failed Directory │
    │ /original/  │   │ s3://.../failed/         │
    │ {date}/     │   │                          │
    │             │   │ File State: FAILED       │
    │ File State: │   │                          │
    │ ARCHIVED    │   │ Next actions:            │
    │             │   │ ├─ Alert operator       │
    │ Next:       │   │ ├─ Do NOT retry auto    │
    │ Processing  │   │ ├─ Manual investigation │
    │ complete    │   │ └─ Manual fix + retry   │
    └─────────────┘   └──────────────────────────┘
          ↓
    ┌─────────────────────────────────┐
    │ Mark in Tracking Table          │
    │ processed = TRUE                │
    │ processed_at = CURRENT_TIME     │
    │                                 │
    │ File State: PROCESSED           │
    └─────────────────────────────────┘
          ↓
    ┌─────────────────────────────────┐
    │ Pipeline Complete ✓             │
    │ File is now:                    │
    │ - Archived in /original/        │
    │ - Data in bronze/silver/gold    │
    │ - Retained per policy           │
    └─────────────────────────────────┘
```

---

## Step-by-Step File Journey

### File Timeline Example

**File:** `traffic_2026_07_01.csv`  
**Location:** `s3://landing/tenant/airtel/Bronze/Traffic/`

```
Timeline:
─────────────────────────────────────────────────────────────

14:00:00 - FILE CREATED IN LANDING BUCKET
  Location: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  State: INCOMING (new, unprocessed)
  Size: 1.5 GB
  Format: CSV
  
14:05:00 - DAG TRIGGERS
  Airflow DAG: bronze_traffic_ingest scheduled to run every 5 minutes
  
14:05:30 - DISCOVERY PHASE
  Task: discover_traffic_files
  Action: List all files in s3://landing/tenant/*/Bronze/Traffic/
  Found: traffic_2026_07_01.csv
  Validation: ✓ Path correct, ✓ Format CSV, ✓ Not in processed/failed/original
  Result: File QUEUED for processing
  State: DISCOVERED
  
14:06:00 - ARCHIVE BEFORE PROCESSING
  Task: archive_traffic_originals
  Action: Copy file to /original/ subdirectory
  Source: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  Dest:   s3://landing/tenant/airtel/Bronze/Traffic/original/2026-07-01/traffic_2026_07_01.csv
  Result: Copy created (original still in main dir for loading)
  State: ARCHIVED_COPY_CREATED
  
14:07:00 - PROCESSING STARTS
  Task: load_bronze_traffic_to_iceberg
  
  14:07:01 - Download to local temp
    S3 → /tmp/traffic_2026_07_01.csv (1.5 GB)
    State: DOWNLOADING
    
  14:10:00 - Format detection
    Detect CSV delimiter: '~'
    State: FORMAT_DETECTED
    
  14:10:05 - Column normalization
    120 columns normalized to standard names
    State: COLUMNS_NORMALIZED
    
  14:10:10 - Column mapping
    Map source columns to target schema
    Result: 115 columns mapped, 5 unmapped (warnings only)
    State: COLUMNS_MAPPED
    
  14:10:15 - Date normalization
    YYYYMMDD format → YYYY-MM-DD format
    State: DATES_NORMALIZED
    
  14:10:20 - Audit columns added
    Add: _source_bucket, _source_key, _tenant, _ingested_at, etc.
    State: AUDIT_COLUMNS_ADDED
    
  14:10:25 - Type casting
    Cast all to STRING (preserve original)
    State: TYPES_CAST
    
  14:15:00 - Write to bronze table
    Write 1,234,567 rows to bronze.imsi_level_traffic
    State: BRONZE_LOADED
    Result: ✓ SUCCESS - 1,234,567 rows appended
    
14:20:00 - SILVER LAYER PROCESSING
  Task: load_silver_dimensions_and_fact
  
  14:20:05 - Load dimensions
    Extract 14 unique dimension values
    Generate MD5 keys
    Result: ✓ 12 dimensions new records, 2 all existing
    State: DIMENSIONS_LOADED
    
  14:25:00 - Load fact table
    Join dimensions with bronze
    Result: ✓ 1,234,567 fact rows
    State: FACT_LOADED
    
14:30:00 - GOLD LAYER PROCESSING
  Task: load_gold_layer
  
  14:30:05 - Load gold daily
    Denormalize silver fact + dimensions
    Result: ✓ 1,234,567 daily rows
    State: GOLD_DAILY_LOADED
    
  14:35:00 - Load gold monthly
    Aggregate daily data
    Result: ✓ 12,345 monthly rows
    State: GOLD_MONTHLY_LOADED
    
14:40:00 - VALIDATION CHECKS
  ✓ Bronze rows: 1,234,567
  ✓ Silver dimensions: 14 tables
  ✓ Silver fact: 1,234,567 rows
  ✓ Gold daily: 1,234,567 rows
  ✓ Gold monthly: 12,345 rows
  ✓ All row counts match expectations
  State: VALIDATION_PASSED
  
14:45:00 - PROCESSING COMPLETE ✓ SUCCESS
  All pipeline steps completed successfully
  File is already archived in /original/2026-07-01/
  State: PROCESSING_COMPLETE
  
  Action: Update file tracking
    processed = TRUE
    processed_at = 2026-07-19 14:45:00
    status = SUCCESS
    error_message = NULL
    
14:46:00 - NEXT DAG RUN (5 min schedule)
  DAG runs again
  discovery_traffic_files executes
  Discovers same file again BUT:
    - File exists in /original/ subdirectory
    - Discovery skips files in /original/ ← FILE FILTERED OUT
    - File NOT in processing list
    - File is skipped (not processed again)
    
Result: FILE PROCESSED ONCE, NO DUPLICATES ✓
```

---

## Directory Structure

### Complete S3 Directory Layout

```
s3://landing/
│
└── tenant/
    │
    ├── airtel/
    │   └── Bronze/
    │       └── Traffic/
    │           ├── traffic_2026_07_01.csv           ← NEW file (will process)
    │           ├── traffic_2026_07_02.parquet       ← NEW file (will process)
    │           │
    │           ├── original/
    │           │   ├── 2026-07-01/
    │           │   │   └── traffic_2026_07_01.csv   ← ARCHIVE (processed)
    │           │   ├── 2026-07-02/
    │           │   │   └── traffic_2026_07_02.parquet ← ARCHIVE (processed)
    │           │   ├── 2026-07-03/
    │           │   │   └── traffic_2026_07_03.csv   ← ARCHIVE (processed)
    │           │   └── 2026-07-04/
    │           │       └── traffic_2026_07_04.csv   ← ARCHIVE (processed)
    │           │
    │           ├── failed/
    │           │   ├── traffic_bad_format.csv       ← ERROR (bad delimiter)
    │           │   ├── traffic_corrupted.csv        ← ERROR (zero rows)
    │           │   └── traffic_wrong_schema.csv     ← ERROR (missing columns)
    │           │
    │           └── processed/
    │               ├── traffic_20260625.csv         ← PROCESSED (old)
    │               ├── traffic_20260626.csv         ← PROCESSED (old)
    │               └── traffic_20260627.csv         ← PROCESSED (old)
    │
    ├── jio/
    │   └── Bronze/
    │       └── Traffic/
    │           ├── jio_traffic_20260701.csv
    │           ├── jio_traffic_20260702.csv
    │           ├── original/
    │           │   ├── 2026-07-01/
    │           │   │   └── jio_traffic_20260701.csv
    │           │   └── 2026-07-02/
    │           │       └── jio_traffic_20260702.csv
    │           ├── failed/
    │           └── processed/
    │
    └── vodafone/
        └── Bronze/
            └── Traffic/
                ├── vodafone_cdr_20260701.parquet
                ├── original/
                │   └── 2026-07-01/
                │       └── vodafone_cdr_20260701.parquet
                ├── failed/
                └── processed/
```

### Directory Purposes

| Directory | Purpose | File Movement |
|-----------|---------|---|
| **Main** | Incoming files ready to process | Files awaiting discovery |
| **original/** | Archive of all successfully processed files | Move after SUCCESS |
| **failed/** | Files that encountered errors | Move after FAILURE |
| **processed/** | Legacy/old processed files (for audit) | Move after long-term retention |

---

## Discovery Phase

### How Files Are Selected for Processing

**Location:** `dags/orchestration/traffic_ingest.py:discover_traffic_files()`

```python
def discover_traffic_files(**context):
    """Discover all Bronze/Traffic files across all tenant directories."""
    
    # Step 1: Connect to S3
    s3 = boto3.client("s3", endpoint_url=storage_endpoint)
    
    # Step 2: List all tenant directories
    # Pattern: s3://landing/tenant/{tenant_name}/
    discovered_tenants = []
    for tenant in list_tenant_dirs():
        discovered_tenants.append(tenant)
    
    # Step 3: For each tenant, find Bronze/Traffic/ directory
    file_configs = []
    for tenant in discovered_tenants:
        prefix = f"tenant/{tenant}/Bronze/Traffic/"
        
        # List ALL files in this directory
        files = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        
        for file_obj in files:
            file_key = file_obj["Key"]
            
            # FILTER 1: Skip directories
            if file_key.endswith("/"):
                continue
            
            # FILTER 2: Skip files in special subdirectories (already processed/failed)
            if any(part.lower() in {"processed", "failed", "original"} 
                   for part in file_key.split("/")):
                logger.info(f"Skipping (in special dir): {file_key}")
                continue
            
            # FILTER 3: Accept only supported formats
            if not file_key.lower().endswith((".parquet", ".csv")):
                logger.info(f"Skipping (unsupported format): {file_key}")
                continue
            
            # FILTER 4: Validate Bronze/Traffic layer
            if not "/bronze/traffic/" in file_key.lower():
                logger.info(f"Skipping (not Bronze/Traffic): {file_key}")
                continue
            
            # ✓ FILE PASSED ALL FILTERS - ADD TO PROCESSING LIST
            file_configs.append({
                "file_key": file_key,
                "tenant": tenant.lower(),
                "status": "DISCOVERED"
            })
    
    return file_configs
```

### Discovery Output Example

```
Input: s3://landing/tenant/*/Bronze/Traffic/

Processing Logic:
├─ File: tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
│  ├─ Check 1: Not a directory? ✓
│  ├─ Check 2: Not in /original/, /failed/, /processed/? ✓
│  ├─ Check 3: Format is .csv? ✓
│  ├─ Check 4: Path contains /bronze/traffic/? ✓
│  └─ Result: ✓ INCLUDE
│
├─ File: tenant/airtel/Bronze/Traffic/original/2026-07-01/traffic_2026_07_01.csv
│  ├─ Check 1: Not a directory? ✓
│  ├─ Check 2: Not in /original/, /failed/, /processed/? ✗ (in /original/)
│  └─ Result: ✗ SKIP (already processed)
│
├─ File: tenant/airtel/Bronze/Traffic/failed/traffic_corrupted.csv
│  ├─ Check 1: Not a directory? ✓
│  ├─ Check 2: Not in /original/, /failed/, /processed/? ✗ (in /failed/)
│  └─ Result: ✗ SKIP (already failed)
│
└─ File: tenant/airtel/Bronze/Settlement/settlement_2026_07_01.csv
   ├─ Check 1: Not a directory? ✓
   ├─ Check 2: Not in /original/, /failed/, /processed/? ✓
   ├─ Check 3: Format is .csv? ✓
   ├─ Check 4: Path contains /bronze/traffic/? ✗ (/settlement/)
   └─ Result: ✗ SKIP (not traffic domain)

FINAL DISCOVERED FILES:
  ✓ tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  ✓ tenant/airtel/Bronze/Traffic/traffic_2026_07_02.parquet
  ✓ tenant/jio/Bronze/Traffic/jio_traffic_07_01.csv
```

---

## Processing Phase

### Real-Time File Status During Processing

```
Timeline of File Status Changes:

14:05:30 - DISCOVERED
  File: traffic_2026_07_01.csv
  Location: s3://landing/tenant/airtel/Bronze/Traffic/
  Status: DISCOVERED (in processing queue)
  Action Next: Archive copy to /original/

14:06:00 - ARCHIVING
  File: traffic_2026_07_01.csv
  Action: Copy file to s3://landing/tenant/airtel/Bronze/Traffic/original/2026-07-01/
  Status: ARCHIVING (creating backup copy)
  Action Next: Start processing

14:06:30 - PROCESSING (BRONZE)
  File: traffic_2026_07_01.csv
  Downloaded to: /tmp/traffic_2026_07_01.csv
  Status: PROCESSING (in memory, being parsed)
  Action: Bronze layer load
  
14:10:00 - PROCESSING (SILVER)
  File: traffic_2026_07_01.csv (data now in bronze table)
  Status: PROCESSING (data in silver layer)
  Action: Dimensions + fact loading

14:30:00 - PROCESSING (GOLD)
  File: traffic_2026_07_01.csv (data in silver layer)
  Status: PROCESSING (data in gold layer)
  Action: Daily + monthly aggregation

14:40:00 - VALIDATION
  File: traffic_2026_07_01.csv (data loaded to all layers)
  Status: VALIDATING (checking row counts, data quality)
  Checks:
    ✓ Bronze: 1,234,567 rows
    ✓ Silver: Dimensions + fact loaded
    ✓ Gold daily: 1,234,567 rows
    ✓ Gold monthly: 12,345 rows
  Result: ✓ VALIDATION PASSED

14:45:00 - COMPLETE ✓ SUCCESS
  File: traffic_2026_07_01.csv
  Status: COMPLETE_SUCCESS
  Location: Original in /original/2026-07-01/
  Data: Successfully loaded to bronze/silver/gold
  Action: Update processing status
  
14:46:00 - NEXT DISCOVERY
  DAG runs again
  Tries to discover traffic_2026_07_01.csv again
  Filter check: Is file in /original/? YES
  Result: ✓ SKIPPED (prevents duplicate processing)
```

---

## Success Path

### When Processing Completes Successfully

#### **Step 1: All Pipeline Tasks Pass**

```python
# In DAG orchestration:
task_discover_files → discover_traffic_files()
    ↓ Returns list of files
task_archive_files → archive_original_files()
    ↓ Copies files to /original/{date}/
task_load_bronze → load_bronze_traffic_to_iceberg()
    ↓ Loads to bronze table ✓
task_load_silver → load_silver_dimensions_and_fact()
    ↓ Loads dimensions + fact ✓
task_load_gold → load_gold_layer()
    ↓ Loads daily + monthly ✓
    
All tasks succeeded ✓
```

#### **Step 2: Archive File**

**When:** Before processing starts  
**Why:** Backup in case of any issues  
**Code Location:** `dags/orchestration/traffic_ingest.py:archive_original_files()`

```python
def archive_original_files(**context):
    """Copy original traffic files to original/{date}/ within source directory."""
    
    file_configs = context["ti"].xcom_pull(task_ids="discover_traffic_files")
    
    for file_config in file_configs:
        source_key = file_config["file_key"]
        # Extract directory and filename
        source_dir = source_key.rsplit("/", 1)[0]  # dir before filename
        filename = source_key.split("/")[-1]       # filename only
        
        # Create archive path with date
        archive_date = datetime.now().strftime("%Y-%m-%d")
        archive_key = f"{source_dir}/original/{archive_date}/{filename}"
        
        try:
            # Copy file (not move - keep original too)
            s3.copy_object(
                CopySource={"Bucket": BUCKET, "Key": source_key},
                Bucket=BUCKET,
                Key=archive_key
            )
            logger.info(f"Archived: {source_key} → {archive_key}")
            
        except Exception as e:
            logger.warning(f"Failed to archive {source_key}: {e}")
            # Continue anyway - not blocking
```

**Result:**

```
Before:
  s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv

After:
  ORIGINAL: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  ARCHIVE:  s3://landing/tenant/airtel/Bronze/Traffic/original/2026-07-01/traffic_2026_07_01.csv
  
Benefit: File is safely backed up, source remains for processing
```

#### **Step 3: Data Validation**

After all layers loaded, automatic validation:

```
✓ Check 1: Row counts match expectations
  Bronze: 1,234,567 rows ✓
  Gold daily: 1,234,567 rows ✓
  Gold monthly: 12,345 rows ✓
  
✓ Check 2: No NULL in critical columns
  client_pmn: 100% populated ✓
  call_date: 100% populated ✓
  
✓ Check 3: Date formats correct
  All dates in YYYY-MM-DD format ✓
  
✓ Check 4: Numeric casts successful
  Volume: DECIMAL(18,6) ✓
  Duration: DECIMAL(18,6) ✓
  Event count: BIGINT ✓
  
✓ Check 5: Monthly duplicates check
  No duplicate (client, partner, month) keys ✓
  
Overall: ✓ ALL VALIDATION CHECKS PASSED
```

#### **Step 4: Mark as Processed**

In tracking table:

```sql
INSERT INTO file_processing_log (
    file_name,
    tenant,
    file_path,
    processed_date,
    status,
    row_count,
    error_message,
    processing_duration_seconds
) VALUES (
    'traffic_2026_07_01.csv',
    'airtel',
    's3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv',
    '2026-07-19',
    'SUCCESS',
    1234567,
    NULL,
    900  -- 15 minutes
);
```

#### **File State After Success**

```
File: traffic_2026_07_01.csv

Location: 
  Original:  s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  Archive:   s3://landing/tenant/airtel/Bronze/Traffic/original/2026-07-01/traffic_2026_07_01.csv
  
Status: SUCCESS
  processed = TRUE
  processed_at = 2026-07-19 14:45:00
  status_code = SUCCESS
  row_count = 1,234,567
  error_message = NULL
  
Data Location:
  Bronze: bronze.imsi_level_traffic (1,234,567 rows)
  Silver: Dimensions (14 tables) + Fact (1,234,567 rows)
  Gold: Daily (1,234,567 rows) + Monthly (12,345 rows)
  
Next Status: 
  File will NOT be processed again (filtered in next discovery)
  Archive retained per retention policy (default: 90 days)
```

---

## Error Path

### When Processing Encounters Errors

#### **Possible Error Points**

```
Discovery Phase
  ├─ S3 connection fails
  ├─ File disappeared after discovery
  └─ Bucket permissions denied

Download & Format Detection
  ├─ File corrupted
  ├─ CSV delimiter not detected
  ├─ Parquet schema unreadable
  └─ File empty (0 rows)

Column Processing
  ├─ Required columns missing
  ├─ Date format unrecognizable
  ├─ Type casting fails
  └─ Memory overflow on large file

Bronze Write
  ├─ Iceberg write conflict
  ├─ Table doesn't exist
  ├─ Permissions denied
  └─ Disk space full

Silver Processing
  ├─ Dimension lookup fails
  ├─ Foreign key violation
  ├─ Timeout (file too large)
  └─ Resource exhaustion

Gold Processing
  ├─ Aggregation fails
  ├─ Timeout
  └─ Validation fails

Any Layer
  └─ Spark job timeout (>1 hour)
```

#### **Error Detection & Response**

**Code Location:** `dags/orchestration/traffic_ingest.py` DAG task handlers

```python
# Example: Catch error from bronze load task
load_traffic = BashOperator.partial(
    task_id="load_bronze_traffic_to_iceberg",
    execution_timeout=timedelta(hours=1),  # Timeout: 1 hour
    retries=0,  # No automatic retry (retry at DAG level)
    max_active_tis_per_dagrun=1,  # Sequential processing
    on_failure_callback=handle_task_failure  # Custom error handler
).expand(bash_command=prepare_load_commands.output)

def handle_task_failure(context):
    """Handle task failure - move file to error directory."""
    
    task_instance = context['task_instance']
    file_key = context['task_instance'].xcom_pull(key='file_key')
    error_message = context['exception']
    
    if file_key:
        # Move file to failed/ directory
        source_key = file_key
        failed_key = source_key.replace(
            "/Bronze/Traffic/", 
            "/Bronze/Traffic/failed/"
        )
        
        try:
            s3.copy_object(
                CopySource={"Bucket": BUCKET, "Key": source_key},
                Bucket=BUCKET,
                Key=failed_key
            )
            logger.error(f"ERROR: Moved {source_key} → {failed_key}")
            logger.error(f"Reason: {error_message}")
            
            # Log to tracking table
            log_processing_failure(file_key, error_message)
            
        except Exception as e:
            logger.error(f"Failed to move error file: {e}")
    
    # Trigger alert
    send_alert_to_operator(
        severity="HIGH",
        message=f"File processing failed: {file_key}\n{error_message}"
    )
```

#### **Step 1: Detect Error**

```
Processing starts:
  download_from_s3() → 1.5 GB file
  detect_csv_delimiter() → Delimiter detected: '~'
  read_csv() → Start reading...
  parse_rows() → Row 100,000: MALFORMED ROW
  
Error Detected: "CSV parsing error at row 100,000"
Error Type: DATA_QUALITY_ERROR
Severity: HIGH (file corrupted)
```

#### **Step 2: Stop Processing**

```
Current Process: Bronze load job
Action: STOP
    ├─ Cancel remaining row processing
    ├─ Roll back Iceberg write (transaction)
    ├─ Clean up /tmp/ files
    └─ Free resources

Result: Partial data NOT written to bronze
        (Iceberg transaction ensures all-or-nothing)
```

#### **Step 3: Move to Error Directory**

```
Step 1: Identify file
  Original: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  Extract directory: s3://landing/tenant/airtel/Bronze/Traffic/
  Extract filename: traffic_2026_07_01.csv

Step 2: Create error path
  Error path: s3://landing/tenant/airtel/Bronze/Traffic/failed/traffic_2026_07_01.csv

Step 3: Copy to error directory
  FROM: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  TO:   s3://landing/tenant/airtel/Bronze/Traffic/failed/traffic_2026_07_01.csv
  Status: COPIED
  
  Note: Original file stays in place (for manual inspection)
        Some systems delete original after move, we copy instead

Step 4: Result
  Location:
    Failed: s3://landing/tenant/airtel/Bronze/Traffic/failed/traffic_2026_07_01.csv
    Original: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv (still there for review)
```

#### **Step 4: Update Status & Log Error**

```sql
INSERT INTO file_processing_log (
    file_name,
    tenant,
    file_path,
    processed_date,
    status,
    row_count,
    error_message,
    error_type,
    moved_to_failed_path,
    processing_duration_seconds,
    retry_count,
    manual_intervention_required
) VALUES (
    'traffic_2026_07_01.csv',
    'airtel',
    's3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv',
    '2026-07-19',
    'ERROR',
    0,  -- No rows loaded
    'CSV parsing error at row 100,000: Expected 120 columns, got 119',
    'DATA_QUALITY_ERROR',
    's3://landing/tenant/airtel/Bronze/Traffic/failed/traffic_2026_07_01.csv',
    600,  -- 10 minutes before error
    0,
    TRUE  -- Requires manual fix
);
```

#### **Step 5: Notify Operator**

```
Alert Sent to Operations Team:

Subject: ❌ TRAFFIC DATA LOAD FAILED

Priority: HIGH
Timestamp: 2026-07-19 14:13:45 UTC

Details:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
File: traffic_2026_07_01.csv
Tenant: airtel
S3 Path: s3://landing/tenant/airtel/Bronze/Traffic/

Status: ERROR ❌
Error Type: DATA_QUALITY_ERROR
Error Message: CSV parsing error at row 100,000
  Expected: 120 columns
  Got: 119 columns

Action Taken:
  • File MOVED to: s3://.../failed/traffic_2026_07_01.csv
  • Processing STOPPED (no partial data written)
  • Original file retained for manual review
  • No automatic retry (requires fix)

Required Actions:
  1. Inspect file: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  2. Identify cause: Missing column?
  3. Fix file format
  4. Either:
     a) Re-upload fixed file to main directory
     b) Move from /failed/ back to main directory to retry
  5. Manual re-run of DAG

Contact: data-ops@company.com
Escalation: data-lead@company.com if not resolved in 1 hour
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

#### **Step 6: File State After Error**

```
File: traffic_2026_07_01.csv

Locations:
  Original: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
            (still in original location for inspection)
  Failed:   s3://landing/tenant/airtel/Bronze/Traffic/failed/traffic_2026_07_01.csv
            (backup copy for archiving)

Status: ERROR
  processed = FALSE
  processed_at = NULL
  status = ERROR
  error_type = DATA_QUALITY_ERROR
  error_message = "CSV parsing error at row 100,000: Expected 120 columns, got 119"
  manual_intervention_required = TRUE
  
Data Loaded: NONE (rolled back transaction)
  Bronze: 0 rows (transaction rolled back)
  Silver: 0 rows
  Gold: Not affected (no new data)

Next Steps (Manual):
  1. Fix the source file
  2. Restore to main directory
  3. Manually trigger DAG re-run
  
Retention: Failed file kept indefinitely for audit
```

#### **Manual Recovery Process**

```
Scenario: Fix discovered in file format

Timeline:

T+1 hour - OPERATOR RECEIVES ALERT
  Alert shows file in /failed/ directory
  Error: "Expected 120 columns, got 119"
  
T+1.5 hours - INVESTIGATION
  Operator checks original file location
  Reviews file in /failed/ directory
  Identifies issue: Column header missing in file
  
T+2 hours - FIX & RE-UPLOAD
  Option 1: Fix locally
    1. Download file from S3
    2. Add missing column header
    3. Re-upload to main directory
    
  Option 2: Move from failed
    1. Copy file from /failed/ back to main directory
    2. Update file
    3. Keep in main directory
  
T+2.1 hours - MANUAL DAG TRIGGER
  $ airflow dags trigger bronze_traffic_ingest
  OR via Airflow UI: Trigger DAG
  
T+2.15 hours - DISCOVERY RUNS AGAIN
  discover_traffic_files() executes
  Finds the re-uploaded file in main directory
  File is NOT in /original/, /failed/, /processed/
  ✓ File included in processing list
  
T+2.2 hours - PROCESSING RESTARTS
  File downloaded again
  Bronze load retry ← NOW SUCCEEDS ✓
  Silver load ✓
  Gold load ✓
  Validation ✓
  Archive to /original/ ✓
  Mark processed = TRUE ✓

T+2.5 hours - RECOVERY COMPLETE
  File successfully processed on retry
  Operator notified of success
  No manual intervention needed (automatically detected)
```

---

## Archive Strategy

### Archiving After Success

#### **What Gets Archived**

```
After successful processing, file is copied to:
  /original/{YYYY-MM-DD}/{filename}

Example:
  Source: s3://landing/tenant/airtel/Bronze/Traffic/traffic_2026_07_01.csv
  Archive: s3://landing/tenant/airtel/Bronze/Traffic/original/2026-07-01/traffic_2026_07_01.csv
  
  Original file remains in place (NOT deleted)
  Archive is a backup copy
```

#### **Archive Metadata**

```
Each archived file contains:
  - Original file content (binary, compressed if parquet)
  - S3 metadata:
    - Content-Type: text/csv or application/octet-stream
    - Content-MD5: Hash for integrity verification
    - Last-Modified: Upload timestamp
    - StorageClass: STANDARD (or infrequent access after retention)
  - Custom metadata (S3 tags):
    - archive_date: 2026-07-01
    - tenant: airtel
    - status: SUCCESS
    - processing_date: 2026-07-19
    - row_count: 1234567
    - data_quality: PASS
```

#### **Archive Organization by Date**

```
Archiving Pattern:
  /original/{YYYY-MM-DD}/

Examples:
  s3://landing/tenant/airtel/Bronze/Traffic/original/
    ├── 2026-07-01/
    │   ├── traffic_2026_07_01.csv
    │   └── traffic_2026_07_01_metadata.json
    ├── 2026-07-02/
    │   ├── traffic_2026_07_02.parquet
    │   └── traffic_2026_07_02_metadata.json
    ├── 2026-07-03/
    │   ├── traffic_2026_07_03.csv
    │   └── traffic_2026_07_03_metadata.json
    └── 2026-07-04/
        ├── traffic_2026_07_04.csv
        └── traffic_2026_07_04_metadata.json

Benefit: Easy to find files by processing date
```

#### **Archive Verification**

```python
def verify_archive(original_file_key, archive_file_key):
    """Verify archive matches original."""
    
    # Get MD5 of original
    original_obj = s3.head_object(Bucket=BUCKET, Key=original_file_key)
    original_md5 = original_obj.get('ETag')
    
    # Get MD5 of archive
    archive_obj = s3.head_object(Bucket=BUCKET, Key=archive_file_key)
    archive_md5 = archive_obj.get('ETag')
    
    # Compare
    if original_md5 == archive_md5:
        logger.info(f"✓ Archive verified: {archive_file_key}")
        return True
    else:
        logger.error(f"✗ Archive MISMATCH: {archive_file_key}")
        # Alert operator - file may be corrupted
        return False
```

---

## Retention Policy

### File Retention Timeline

```
File Lifecycle:

Day 0: Processing Complete
  File: original location + archived in /original/{date}/
  Retention: Keep indefinitely (source of truth backup)

Day 1-89: Active Period
  Location: /original/{date}/
  Access: Random access for verification/audit
  Storage Class: STANDARD (frequent access assumed)
  Cost: Normal S3 pricing

Day 90: Long-term Archive
  Action: Transition to INFREQUENT_ACCESS storage class
  Cost: 50% cheaper than STANDARD
  Access: Still available (slower retrieval ~5 min)

Day 180: Audit Archive
  Action: Optional - Transition to GLACIER storage
  Cost: 80% cheaper than STANDARD
  Access: Very slow (hours) - rare access
  Note: Usually kept only for regulatory compliance

Day 365: Auto-Delete (Optional)
  Action: Delete files older than 1 year (configurable)
  Reason: Cost optimization, compliance requirements
  Note: Should have separate data warehouse for analytics by this point
```

### Retention Configuration

```yaml
# S3 Lifecycle Policy for Traffic Archives

RetentionPolicy:
  name: traffic-archive-retention
  target: s3://landing/tenant/*/Bronze/Traffic/original/
  
  rules:
    - rule_name: transition-to-infrequent-access
      after_days: 90
      storage_class: INFREQUENT_ACCESS
      enabled: true
      
    - rule_name: transition-to-glacier
      after_days: 180
      storage_class: GLACIER
      enabled: true
      
    - rule_name: delete-after-one-year
      after_days: 365
      action: delete
      enabled: false  # Disabled for now (keep indefinitely)
      
    - rule_name: delete-incomplete-multipart
      incomplete_multipart_upload_days: 7
      enabled: true
```

### Cost Optimization

```
Before Retention:
  90 files × 1.5 GB × $0.023/GB/month = $3.11/month
  
After Retention (1 year):
  Days 1-90: 90 files × 1.5 GB × $0.023/GB = $3.11/month
  Days 91-180: 90 files × 1.5 GB × $0.0115/GB = $1.56/month (IA)
  Days 181-365: 90 files × 1.5 GB × $0.004/GB = $0.54/month (Glacier)
  
  Annual Cost Savings: ~$25 per 90 files
  Benefit: Keeps data for audit, reduced storage cost
```

---

## Monitoring File States

### File Status Tracking

```sql
-- Create file processing log table
CREATE TABLE file_processing_log (
    id                          BIGINT PRIMARY KEY AUTO_INCREMENT,
    file_name                   VARCHAR(500),
    tenant                      VARCHAR(50),
    file_path                   VARCHAR(1000),
    file_size_bytes             BIGINT,
    
    -- Processing metadata
    discovered_at               TIMESTAMP,
    processing_started_at       TIMESTAMP,
    processing_completed_at     TIMESTAMP,
    processing_duration_seconds INT,
    
    -- Status tracking
    status                      VARCHAR(50),  -- DISCOVERED, PROCESSING, SUCCESS, ERROR
    status_code                 VARCHAR(50),  -- SUCCESS, ERROR_DOWNLOAD, ERROR_PARSE, ERROR_VALIDATION
    
    -- Data quality
    row_count                   BIGINT,
    row_count_expected          BIGINT,
    data_quality_score          DECIMAL(5,2),
    validation_passed           BOOLEAN,
    
    -- Error tracking
    error_message               VARCHAR(2000),
    error_type                  VARCHAR(50),
    error_stack_trace           TEXT,
    
    -- Archiving
    archived                    BOOLEAN,
    archived_path               VARCHAR(1000),
    archive_verified            BOOLEAN,
    
    -- Failure handling
    moved_to_failed_path        VARCHAR(1000),
    manual_intervention_required BOOLEAN,
    retry_count                 INT,
    last_retry_at               TIMESTAMP,
    
    -- Audit
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### Queries for Monitoring

#### **Query 1: Current Processing Status**

```sql
-- Show files currently being processed or recently completed
SELECT 
    file_name,
    tenant,
    status,
    IF(processing_completed_at IS NULL, 'IN_PROGRESS', 'COMPLETED') as current_state,
    TIMESTAMPDIFF(MINUTE, processing_started_at, CURRENT_TIMESTAMP) as minutes_processing,
    row_count,
    CASE 
        WHEN status = 'SUCCESS' THEN '✓ SUCCESS'
        WHEN status = 'ERROR' THEN '✗ ERROR'
        WHEN status = 'PROCESSING' THEN '⏳ PROCESSING'
        ELSE '? UNKNOWN'
    END as status_icon
FROM file_processing_log
WHERE DATE(discovered_at) = CURDATE()
ORDER BY discovered_at DESC;

Output:
file_name                   | tenant  | status     | current_state | minutes_processing | row_count   | status_icon
traffic_2026_07_02.parquet  | airtel  | PROCESSING | IN_PROGRESS   | 15                 | 0           | ⏳ PROCESSING
traffic_2026_07_01.csv      | airtel  | SUCCESS    | COMPLETED     | 45                 | 1234567     | ✓ SUCCESS
jio_traffic_07_01.csv       | jio     | SUCCESS    | COMPLETED     | 52                 | 987654      | ✓ SUCCESS
```

#### **Query 2: Error Summary**

```sql
-- Show all errors and failures in last 7 days
SELECT 
    DATE(discovered_at) as date,
    status_code,
    error_type,
    COUNT(*) as error_count,
    GROUP_CONCAT(DISTINCT file_name) as affected_files
FROM file_processing_log
WHERE status = 'ERROR' 
  AND discovered_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY DATE(discovered_at), status_code, error_type
ORDER BY date DESC, error_count DESC;

Output:
date       | status_code      | error_type           | error_count | affected_files
2026-07-19 | ERROR_PARSE      | DATA_QUALITY_ERROR   | 1           | traffic_2026_07_01.csv
2026-07-18 | ERROR_VALIDATION | MISSING_COLUMNS      | 2           | jio_traffic_07_01.csv, jio_traffic_07_02.csv
2026-07-17 | ERROR_DOWNLOAD   | S3_CONNECTION_ERROR  | 1           | vodafone_cdr_20260701.parquet
```

#### **Query 3: Archiving Status**

```sql
-- Show archived files and verify completeness
SELECT 
    file_name,
    tenant,
    row_count,
    CASE 
        WHEN archived = TRUE AND archive_verified = TRUE THEN '✓ Archived & Verified'
        WHEN archived = TRUE AND archive_verified = FALSE THEN '⚠️ Archived (Unverified)'
        WHEN archived IS NULL THEN '✗ Not Archived'
    END as archive_status,
    archived_path,
    DATEDIFF(NOW(), processing_completed_at) as days_since_processed
FROM file_processing_log
WHERE status = 'SUCCESS'
  AND processing_completed_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
ORDER BY processing_completed_at DESC;

Output:
file_name                   | tenant  | row_count | archive_status                | archived_path           | days_since_processed
traffic_2026_07_02.parquet  | airtel  | 1234567   | ✓ Archived & Verified         | original/2026-07-02/... | 0
traffic_2026_07_01.csv      | airtel  | 1234567   | ✓ Archived & Verified         | original/2026-07-01/... | 1
jio_traffic_07_01.csv       | jio     | 987654    | ✓ Archived & Verified         | original/2026-07-01/... | 1
```

#### **Query 4: Performance Metrics**

```sql
-- Show processing duration and throughput
SELECT 
    DATE(discovered_at) as date,
    tenant,
    COUNT(*) as files_processed,
    AVG(processing_duration_seconds) / 60.0 as avg_duration_minutes,
    SUM(row_count) as total_rows_processed,
    ROUND(SUM(row_count) / SUM(processing_duration_seconds) * 60, 0) as rows_per_minute
FROM file_processing_log
WHERE status = 'SUCCESS'
  AND discovered_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY DATE(discovered_at), tenant
ORDER BY date DESC, tenant;

Output:
date       | tenant  | files_processed | avg_duration_minutes | total_rows_processed | rows_per_minute
2026-07-19 | airtel  | 2               | 15.5                 | 2469134              | 2666
2026-07-19 | jio     | 1               | 12.3                 | 987654               | 1336
2026-07-18 | airtel  | 1               | 14.2                 | 1234567              | 1448
```

#### **Query 5: Discovery Filtering Audit**

```sql
-- Show which files are being filtered out and why
SELECT 
    'Files in /original/ (already processed)' as filter_reason,
    COUNT(*) as count
FROM s3_file_inventory
WHERE path LIKE '%/original/%'
  AND tenant LIKE 'airtel%'
UNION ALL
SELECT 
    'Files in /failed/ (errors)',
    COUNT(*)
FROM s3_file_inventory
WHERE path LIKE '%/failed/%'
  AND tenant LIKE 'airtel%'
UNION ALL
SELECT 
    'Files in /processed/ (legacy)',
    COUNT(*)
FROM s3_file_inventory
WHERE path LIKE '%/processed/%'
  AND tenant LIKE 'airtel%'
UNION ALL
SELECT 
    'Non-Traffic domains (settlement, agreement)',
    COUNT(*)
FROM s3_file_inventory
WHERE path LIKE '%/Bronze/%'
  AND path NOT LIKE '%/Traffic/%'
  AND tenant LIKE 'airtel%';

Output:
filter_reason                                    | count
Files in /original/ (already processed)          | 90
Files in /failed/ (errors)                       | 3
Files in /processed/ (legacy)                    | 15
Non-Traffic domains (settlement, agreement)      | 8

Result: 116 files filtered out, prevents duplicate processing
```

---

## Summary: Complete File Lifecycle

| Stage | Action | Location | File State | Next |
|-------|--------|----------|------------|------|
| **1. Arrival** | Upload to S3 | `/Bronze/Traffic/` | INCOMING | Discovery |
| **2. Discovery** | Find in S3 | `/Bronze/Traffic/` | DISCOVERED | Archive |
| **3. Archive** | Copy backup | `/original/{date}/` | ARCHIVED | Process |
| **4. Processing** | Load data | Memory/temp | PROCESSING | Validate |
| **5a. Success** | Validate | All layers | SUCCESS | Mark complete |
| **5b. Error** | Stop & move | `/failed/` | ERROR | Manual fix |
| **6. Complete** | Update status | DB tracking | COMPLETE | Retain |
| **7. Retain** | Archive long-term | Storage classes | RETAINED | Lifecycle |

---

**End of Document**
