# Traffic Gold Data Layer - Complete Detailed Guide

**Author:** Claude Code Analysis  
**Date:** 2026-07-19  
**Project:** Python Data Warehouse - Traffic Data Pipeline  
**Status:** Documentation of Current Implementation + Issue Identification

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Overview](#architecture-overview)
3. [Daily Data Flow](#daily-data-flow)
4. [Monthly Data Flow](#monthly-data-flow)
5. [Data Grain & Granularity](#data-grain--granularity)
6. [Calculation Logic](#calculation-logic)
7. [The Current Problem](#the-current-problem)
8. [Impact Analysis](#impact-analysis)
9. [The Solution](#the-solution)
10. [Data Quality Verification](#data-quality-verification)
11. [SQL Queries & Examples](#sql-queries--examples)
12. [Practical Scenarios](#practical-scenarios)
13. [Debugging Guide](#debugging-guide)
14. [Implementation Checklist](#implementation-checklist)

---

## Overview

Your data warehouse has a **three-layer architecture** for traffic data processing:

```
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                            │
│                    (Raw Data Files)                         │
│         CDR files, raw traffic records, unprocessed         │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                            │
│              (Normalized & Standardized)                    │
│  ├─ Fact Table: fact_imsi_level_traffic                    │
│  ├─ Dimensions: dim_client, dim_operator, dim_date, etc.  │
│  └─ Clean, deduplicated, business logic applied            │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
              ┌────────────┴────────────┐
              ↓                         ↓
    ┌──────────────────┐    ┌──────────────────────┐
    │   GOLD LAYER     │    │   GOLD LAYER         │
    │   (DAILY)        │    │   (MONTHLY)          │
    │                  │    │                      │
    │ Denormalized     │    │ Aggregated &         │
    │ Business-Ready   │    │ Summarized           │
    │ Daily Details    │    │ For Reporting        │
    └──────────────────┘    └──────────────────────┘
```

The **Gold Layer** serves analytics, reporting, and billing use cases with business-ready data.

---

## Architecture Overview

### Layer Responsibilities

#### **Bronze Layer**
- Raw incoming data from external sources
- Minimal transformation (schema application only)
- One file per day typically
- Example: `traffic_2026-07-01.parquet`

#### **Silver Layer**
- Normalized fact and dimension tables
- Data quality rules applied
- Historical tracking (slowly changing dimensions)
- Business logic standardization
- Deduplicated records

**Silver Schema for Traffic:**
```
Fact Table:
├─ fact_imsi_level_traffic (raw transaction events)
│  └─ Keys: client_pmn_key, partner_pmn_key, traffic_direction_key, 
│           call_date_key, call_type_key, imsi_key, apn_key, 
│           service_type_key, event_type_key, rat_type_key, tac_key,
│           iot_rti_group_key, destination_key, camel_key
│  └─ Measures: duration, volume, event_count, total_charge_sdr_net, total_charge_sdr_gross

Dimension Tables (13 total):
├─ dim_client (Client information)
├─ dim_operator (Roaming partner information)
├─ dim_traffic_direction (Incoming/Outgoing)
├─ dim_date (Calendar dimensions)
├─ dim_call_type (Voice, SMS, Data, etc.)
├─ dim_imsi (SIM card information)
├─ dim_apn (Access Point Names)
├─ dim_service_type (Service classifications)
├─ dim_event_type (Event classifications)
├─ dim_rat_type (Radio Access Type: 2G, 3G, 4G, 5G)
├─ dim_tac (Terminal Access Code - device type)
├─ dim_iot_rti_group (IoT device classifications)
├─ dim_location (Destination locations)
└─ dim_camel (CAMEL service indicator)
```

#### **Gold Layer**
Two main tables for different use cases:

1. **`gold.imsi_level_traffic_daily`** - Denormalized daily detail
   - Purpose: Daily reporting, detailed analysis
   - Grain: One row per (client, partner, direction, date, call_type, IMSI, APN, service, event, RAT, TAC, IoT RTI, location, CAMEL)
   - Strategy: Append-only (daily data is immutable)
   - Partition: By `call_date`

2. **`gold.client_partner_traffic_monthly`** - Aggregated monthly summary
   - Purpose: Billing, monthly reporting, trend analysis
   - Grain: One row per (client, partner, direction, month, call_type, service, event)
   - Strategy: Should be OVERWRITE (currently APPEND - this is the problem!)
   - Partition: By `call_month`

---

## Daily Data Flow

### Process Overview

Every day, when new traffic data arrives, the gold daily layer is populated through these steps:

```
Step 1: Data Arrives
   └─> File: traffic_2026-07-02.parquet

Step 2: Load to Silver (Normalized)
   ├─> Parse and validate format
   ├─> Apply business rules
   ├─> Deduplicate
   └─> Store in silver layer

Step 3: Load to Gold Daily (Denormalized)
   ├─> Read: fact_imsi_level_traffic (for 2026-07-02)
   ├─> Join: 13 dimension tables
   ├─> Flatten: Replace keys with business values
   ├─> Add Metadata: created_at, updated_at timestamps
   ├─> Write: Append to gold.imsi_level_traffic_daily
   └─> Complete

Result: New daily records ready for analysis
```

### Code Implementation: `load_gold_daily.py`

**Location:** `/opt/airflow/jobs/ingestion/traffic/load_gold_daily.py`

**Key Logic:**

```python
def load_gold_daily(spark: SparkSession, catalog: str) -> None:
    """Load gold daily table by denormalizing silver fact + dimensions."""
    
    # Step 1: Read silver fact table
    fact_df = spark.table(f"{catalog}.silver.`fact_imsi_level_traffic`")
    
    # Step 2: Read all dimension tables with selective columns
    dim_client = spark.table(f"{catalog}.silver.`dim_client`").select(
        F.col("pmn_key").alias("client_pmn_key_dim"),
        F.col("pmn").alias("client_pmn")
    )
    # ... (13 dimension tables total)
    
    # Step 3: Join fact with dimensions (denormalize)
    daily_df = (
        fact_df
        .join(dim_client, fact_df.client_pmn_key == dim_client.client_pmn_key_dim, "left")
        .join(dim_operator, fact_df.partner_pmn_key == dim_operator.partner_pmn_key_dim, "left")
        # ... (11 more joins)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
    
    # Step 4: Select final columns (drop dimension keys)
    final_df = daily_df.select(
        "client_pmn",
        "partner_pmn",
        "roaming_partner_country",
        # ... (business columns only)
        F.col("duration").cast("decimal(18,6)").alias("total_duration"),
        F.col("volume").cast("decimal(18,6)").alias("total_volume"),
        F.col("event_count").cast("bigint").alias("total_event_count"),
        # ...
    )
    
    # Step 5: Write to gold (APPEND - this is correct for daily)
    table_name = f"{catalog}.gold.`imsi_level_traffic_daily`"
    final_df.writeTo(table_name).append()
    logger.info("Loaded %d rows to %s", final_df.count(), table_name)
```

### Data Transformation Sequence

#### **Before Denormalization (Silver Fact Table)**

```
Columns: (keys only, dimension values stored separately)

client_pmn_key | partner_pmn_key | traffic_direction_key | call_date_key | call_type_key | ...
      101      |       205       |         1            |    5674      |      10       | ...
      102      |       206       |         2            |    5674      |      11       | ...
```

#### **After Denormalization (Gold Daily Table)**

```
Columns: (business values, keys dropped)

client_pmn | partner_pmn | roaming_partner_country | traffic_direction | call_date | call_type | ...
 ClientA   |  PartnerX   |        US              |    Incoming       | 2026-07-02 | Voice     | ...
 ClientB   |  PartnerY   |        UK              |    Outgoing       | 2026-07-02 | SMS       | ...
```

### Daily Table Schema

```sql
CREATE TABLE gold.imsi_level_traffic_daily (
    -- Dimension Attributes (Denormalized)
    client_pmn                  STRING,
    partner_pmn                 STRING,
    roaming_partner_country     STRING,
    traffic_direction           STRING,
    call_date                   DATE,
    call_month                  STRING,  -- Format: 'YYYY-MM'
    year                        INT,
    month                       INT,
    day                         INT,
    call_type                   STRING,
    call_type_level_2           STRING,
    imsi                        STRING,
    roamer_indicator            STRING,
    apn                         STRING,
    service_type_id             STRING,
    event_type_id               STRING,
    rat_type                    STRING,
    tac_number                  STRING,
    iot_rti_group_id            STRING,
    destination                 STRING,
    destination_category        STRING,
    is_camel                    STRING,
    
    -- Aggregated Measures
    total_duration              DECIMAL(18,6),
    total_volume                DECIMAL(18,6),
    total_event_count           BIGINT,
    total_charge_sdr_net        DECIMAL(18,6),
    total_charge_sdr_gross      DECIMAL(18,6),
    
    -- Metadata
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP
)
PARTITIONED BY (call_date)
STORED AS ICEBERG;
```

### Example Daily Data

```
Row 1:
  client_pmn: ClientA
  partner_pmn: PartnerX
  call_date: 2026-07-01
  call_type: Voice
  imsi: 310150123456789
  apn: internet.com
  total_duration: 1200.123456
  total_volume: 100.234567
  total_event_count: 50000
  total_charge_sdr_net: 50.00
  total_charge_sdr_gross: 60.00

Row 2:
  client_pmn: ClientB
  partner_pmn: PartnerY
  call_date: 2026-07-01
  call_type: SMS
  imsi: 358390123456789
  apn: sms.com
  total_duration: 300.456789
  total_volume: 5.567890
  total_event_count: 1000
  total_charge_sdr_net: 2.50
  total_charge_sdr_gross: 3.00
```

---

## Monthly Data Flow

### Process Overview

The monthly aggregation recalculates every time new daily data arrives:

```
Step 1: New Daily Data Loaded
   └─> gold.imsi_level_traffic_daily updated with new day

Step 2: Monthly Aggregation Triggered
   ├─> Read: ALL rows from gold.imsi_level_traffic_daily
   ├─> Group: By (client, partner, direction, month, call_type, service, event)
   ├─> Aggregate: SUM metrics, COUNT DISTINCT IMSIs/APNs
   ├─> Calculate: Monthly totals
   └─> Result: Aggregated monthly rows

Step 3: Write to Gold Monthly
   ├─ Current Code: .append() ❌ WRONG (creates duplicates)
   └─ Should Be: .overwritePartitions() ✓ CORRECT
```

### Code Implementation: `load_gold_monthly.py`

**Location:** `/opt/airflow/jobs/ingestion/traffic/load_gold_monthly.py`

**Key Logic:**

```python
def load_gold_monthly(spark: SparkSession, catalog: str) -> None:
    """Load gold monthly table by aggregating daily table."""
    
    # Step 1: Read all daily data (for entire month)
    daily_df = spark.table(f"{catalog}.gold.`imsi_level_traffic_daily`")
    
    # Step 2: Group and aggregate
    monthly_df = (
        daily_df.groupBy(
            "client_pmn",
            "partner_pmn",
            "roaming_partner_country",
            "traffic_direction",
            "call_month",
            "year",
            "month",
            "call_type",
            "call_type_level_2",
            "service_type_id",
            "event_type_id"
        )
        .agg(
            # Sum aggregations
            F.sum(F.col("total_duration")).cast("decimal(18,6)").alias("total_duration"),
            F.sum(F.col("total_volume")).cast("decimal(18,6)").alias("total_volume"),
            F.sum(F.col("total_event_count")).cast("bigint").alias("total_event_count"),
            F.sum(F.col("total_charge_sdr_net")).cast("decimal(18,6)").alias("total_charge_sdr_net"),
            F.sum(F.col("total_charge_sdr_gross")).cast("decimal(18,6)").alias("total_charge_sdr_gross"),
            
            # Distinct counts
            F.countDistinct(F.col("imsi")).alias("distinct_imsi_count"),
            F.countDistinct(F.col("apn")).alias("distinct_apn_count"),
        )
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
    
    # Step 3: Select columns in order
    final_df = monthly_df.select(
        "client_pmn",
        "partner_pmn",
        "roaming_partner_country",
        "traffic_direction",
        "call_month",
        "year",
        "month",
        "call_type",
        "call_type_level_2",
        "service_type_id",
        "event_type_id",
        "total_duration",
        "total_volume",
        "total_event_count",
        "total_charge_sdr_net",
        "total_charge_sdr_gross",
        "distinct_imsi_count",
        "distinct_apn_count",
        "created_at",
        "updated_at"
    )
    
    # Step 4: Write to gold (CURRENTLY APPEND - THIS IS THE PROBLEM!)
    table_name = f"{catalog}.gold.`client_partner_traffic_monthly`"
    final_df.writeTo(table_name).append()  # ❌ WRONG
    # Should be: final_df.writeTo(table_name).overwritePartitions()  # ✓ CORRECT
    
    logger.info("Loaded %d rows to %s", final_df.count(), table_name)
```

### Monthly Table Schema

```sql
CREATE TABLE gold.client_partner_traffic_monthly (
    -- Dimension Attributes
    client_pmn                  STRING,
    partner_pmn                 STRING,
    roaming_partner_country     STRING,
    traffic_direction           STRING,
    call_month                  STRING,  -- Format: 'YYYY-MM'
    year                        INT,
    month                       INT,
    call_type                   STRING,
    call_type_level_2           STRING,
    service_type_id             STRING,
    event_type_id               STRING,
    
    -- Aggregated Measures (SUM from daily)
    total_duration              DECIMAL(18,6),
    total_volume                DECIMAL(18,6),
    total_event_count           BIGINT,
    total_charge_sdr_net        DECIMAL(18,6),
    total_charge_sdr_gross      DECIMAL(18,6),
    
    -- Distinct Counts
    distinct_imsi_count         BIGINT,
    distinct_apn_count          BIGINT,
    
    -- Metadata
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP
)
PARTITIONED BY (call_month)
STORED AS ICEBERG;
```

### Example Monthly Data

```
Row 1 (After aggregating July 1-3 daily data):
  client_pmn: ClientA
  partner_pmn: PartnerX
  roaming_partner_country: US
  traffic_direction: Incoming
  call_month: 2026-07
  year: 2026
  month: 7
  call_type: Voice
  call_type_level_2: VoIP
  service_type_id: SVC001
  event_type_id: EVT001
  
  -- AGGREGATED VALUES (sum of all July 1-3 data for this client+partner)
  total_duration: 3960.000000       (1200 + 1320 + 1440)
  total_volume: 331.037024          (100.234567 + 110.345678 + 120.567890)
  total_event_count: 165000         (50000 + 55000 + 60000)
  total_charge_sdr_net: 165.00      (sum of all charges)
  total_charge_sdr_gross: 198.00    (sum of all charges)
  
  -- DISTINCT COUNTS (deduplicated across entire month)
  distinct_imsi_count: 5            (5 unique SIM cards used by this client-partner pair)
  distinct_apn_count: 3             (3 different APNs used)
  
  created_at: 2026-07-03 15:30:00
  updated_at: 2026-07-03 15:30:00
```

---

## Data Grain & Granularity

### What is Data Grain?

The **grain** of a table defines the level of detail - "one row represents what?"

### Gold Daily Grain

**One row per:**
- Client (who is using the service)
- Partner/Operator (who is providing roaming)
- Direction (Incoming or Outgoing)
- Date (specific calendar date)
- Call Type (Voice, SMS, Data, etc.)
- IMSI (specific SIM card)
- APN (specific access point)
- Service Type
- Event Type
- RAT Type (2G, 3G, 4G, 5G)
- TAC (device type)
- IoT RTI Group
- Location/Destination
- CAMEL indicator

**Granularity:** Very Fine - **Transaction or event-level aggregation**

**Example:** All voice calls from ClientA to PartnerX on 2026-07-01 via IMSI ending in 56789 on internet.com APN = 1 row

### Gold Monthly Grain

**One row per:**
- Client
- Partner/Operator
- Direction
- Month
- Call Type
- Service Type
- Event Type

**Granularity:** Coarse - **Monthly summary level**

**Example:** All traffic from ClientA to PartnerX in 2026-07 for voice calls = 1 row

### Why Two Tables?

| Use Case | Better Table | Reason |
|----------|-------------|--------|
| Daily reporting dashboards | Daily | Need daily breakdown |
| Weekly trending analysis | Daily | Allows date filtering |
| Monthly billing | Monthly | Pre-aggregated |
| Yearly trend analysis | Monthly | Compressed data |
| Device type analysis (by IMSI) | Daily | IMSI dimension in daily |
| Top destinations | Daily | Destination detail in daily |
| Partner revenue summary | Monthly | Already aggregated |
| Cost tracking | Monthly | Pre-aggregated charges |

---

## Calculation Logic

### How Aggregation Works

#### **Step 1: Identify Group**

When calculating monthly aggregation for ClientA + PartnerX in July:

```
Find ALL rows in daily table where:
  client_pmn = 'ClientA'
  AND partner_pmn = 'PartnerX'
  AND call_month = '2026-07'
  AND traffic_direction = 'Incoming'
  AND call_type = 'Voice'
  AND call_type_level_2 = 'VoIP'
  AND service_type_id = 'SVC001'
  AND event_type_id = 'EVT001'

Result: 3 rows (one per day in July)
```

#### **Step 2: Apply Aggregation Functions**

```
Daily Rows:
┌────────────┬────────────┬─────────────┬──────────────┐
│ call_date  │ total_vol  │ total_event │ total_charge │
├────────────┼────────────┼─────────────┼──────────────┤
│ 2026-07-01 │ 100.234567 │ 50000       │ 50.00        │
│ 2026-07-02 │ 110.345678 │ 55000       │ 55.00        │
│ 2026-07-03 │ 120.567890 │ 60000       │ 60.00        │
└────────────┴────────────┴─────────────┴──────────────┘

SUM(total_volume) = 100.234567 + 110.345678 + 120.567890 = 331.148135
SUM(total_event_count) = 50000 + 55000 + 60000 = 165000
SUM(total_charge) = 50.00 + 55.00 + 60.00 = 165.00
```

#### **Step 3: Calculate Distinct Counts**

```
For distinct_imsi_count:
  IMSI values across 3 daily rows: ['310150123456789', '310150123456789', '310150987654321']
  DISTINCT: 2 unique IMSIs

For distinct_apn_count:
  APN values across 3 daily rows: ['internet.com', 'internet.com', 'data.com']
  DISTINCT: 2 unique APNs
```

#### **Step 4: Final Monthly Row**

```
Monthly aggregation result:
  client_pmn: ClientA
  partner_pmn: PartnerX
  traffic_direction: Incoming
  call_month: 2026-07
  call_type: Voice
  call_type_level_2: VoIP
  service_type_id: SVC001
  event_type_id: EVT001
  total_volume: 331.148135
  total_event_count: 165000
  total_charge: 165.00
  distinct_imsi_count: 2
  distinct_apn_count: 2
```

### Decimal Precision

Note the data types used:

```
- total_duration:        DECIMAL(18,6)    -- 12 integer digits, 6 decimal places
- total_volume:          DECIMAL(18,6)    -- Up to 999,999,999,999.999999 GB
- total_charge_sdr:      DECIMAL(18,6)    -- Precise money values
- total_event_count:     BIGINT            -- Up to 9,223,372,036,854,775,807
```

This ensures billing calculations are accurate (not using floating point approximations).

### Time Dimensions

All time dimensions are denormalized in gold daily:

```
call_date: 2026-07-02 (specific date)
call_month: '2026-07'  (YYYY-MM format for grouping)
year: 2026
month: 7
day: 2

In monthly table:
call_month: '2026-07'   (used for filtering entire months)
year: 2026
month: 7
(day is dropped - no longer needed)
```

---

## The Current Problem

### Problem Statement

Your monthly load job currently uses `.append()` instead of `.overwritePartitions()`, which creates **duplicate business keys** in the monthly table every time the job runs.

### How the Problem Manifests

#### **Timeline Example: July 2026**

**Day 1 (July 1st):**

```
Gold Daily Table (After load):
├─ ClientA + PartnerX | 2026-07-01 | 100 GB
├─ ClientB + PartnerY | 2026-07-01 | 150 GB
└─ ClientA + PartnerY | 2026-07-01 | 80 GB

Gold Monthly Table (After aggregation - FIRST RUN):
├─ ClientA + PartnerX | 2026-07 | 100 GB   (SUM of July 1st data)
├─ ClientB + PartnerY | 2026-07 | 150 GB
└─ ClientA + PartnerY | 2026-07 | 80 GB

Status: 3 rows, correct state ✓
```

**Day 2 (July 2nd - NEW DATA ARRIVES):**

```
Gold Daily Table (After load):
├─ ClientA + PartnerX | 2026-07-01 | 100 GB  (old)
├─ ClientB + PartnerY | 2026-07-01 | 150 GB  (old)
├─ ClientA + PartnerY | 2026-07-01 | 80 GB   (old)
├─ ClientA + PartnerX | 2026-07-02 | 110 GB  (NEW)
└─ ClientC + PartnerX | 2026-07-02 | 90 GB   (NEW)

Gold Monthly Table (After aggregation - SECOND RUN):
Current aggregation should be:
├─ ClientA + PartnerX | 2026-07 | 210 GB   (100 + 110 from both days)
├─ ClientB + PartnerY | 2026-07 | 150 GB   (only July 1st had data)
├─ ClientA + PartnerY | 2026-07 | 80 GB    (only July 1st had data)
└─ ClientC + PartnerX | 2026-07 | 90 GB    (NEW, July 2nd only)

But with .append() it becomes:
├─ ClientA + PartnerX | 2026-07 | 100 GB   ← OLD row (from Day 1 load)
├─ ClientB + PartnerY | 2026-07 | 150 GB   ← OLD row (from Day 1 load)
├─ ClientA + PartnerY | 2026-07 | 80 GB    ← OLD row (from Day 1 load)
├─ ClientA + PartnerX | 2026-07 | 210 GB   ← NEW row (recalculated with Day 2)
├─ ClientB + PartnerY | 2026-07 | 150 GB   ← DUPLICATE (same as row 2)
├─ ClientA + PartnerY | 2026-07 | 80 GB    ← DUPLICATE (same as row 3)
└─ ClientC + PartnerX | 2026-07 | 90 GB    ← NEW row

Status: 7 rows, DUPLICATES EXIST ❌

Query Result (WRONG):
SELECT SUM(total_volume)
FROM gold.client_partner_traffic_monthly
WHERE client_pmn = 'ClientA'
  AND partner_pmn = 'PartnerX'
  AND call_month = '2026-07';

Result: 100 + 210 = 310 GB ❌ WRONG! (Should be 210 GB)
```

**Day 3 (July 3rd - MORE NEW DATA):**

```
Gold Daily Table: Now has 8 rows total

Gold Monthly Table (After aggregation - THIRD RUN):
New aggregation with all 3 days:
├─ ClientA + PartnerX | 2026-07 | 330 GB  (100+110+120)
├─ ClientB + PartnerY | 2026-07 | 310 GB  (150+160)
├─ ClientA + PartnerY | 2026-07 | 175 GB  (80+95)
└─ ClientC + PartnerX | 2026-07 | 90 GB

But with .append() it keeps appending:
├─ ClientA + PartnerX | 2026-07 | 100 GB   ← OLD (Day 1)
├─ ClientB + PartnerY | 2026-07 | 150 GB   ← OLD (Day 1)
├─ ClientA + PartnerY | 2026-07 | 80 GB    ← OLD (Day 1)
├─ ClientA + PartnerX | 2026-07 | 210 GB   ← OLD (Day 2)
├─ ClientB + PartnerY | 2026-07 | 150 GB   ← OLD DUP (Day 2)
├─ ClientA + PartnerY | 2026-07 | 80 GB    ← OLD DUP (Day 2)
├─ ClientC + PartnerX | 2026-07 | 90 GB    ← OLD (Day 2)
├─ ClientA + PartnerX | 2026-07 | 330 GB   ← NEW (Day 3)
├─ ClientB + PartnerY | 2026-07 | 310 GB   ← NEW (Day 3)
├─ ClientA + PartnerY | 2026-07 | 175 GB   ← NEW (Day 3)
└─ ClientC + PartnerX | 2026-07 | 90 GB    ← NEW (Day 3)

Status: 11 rows, MANY DUPLICATES ❌

Query Result (WRONG):
SELECT SUM(total_volume)
WHERE client_pmn = 'ClientA' AND partner_pmn = 'PartnerX' AND call_month = '2026-07';

Result: 100 + 210 + 330 = 640 GB ❌ VERY WRONG!
```

### Root Cause Analysis

The issue is a **design mismatch** between the processing logic and the storage strategy:

| Aspect | Current Design | Problem |
|--------|---|---|
| **Processing Logic** | Recalculates entire month from scratch | Uses all available daily data each run |
| **Storage Strategy** | .append() (append-only) | Never deletes old rows |
| **Result** | Duplicate keys | Same business key appears multiple times |
| **Idempotency** | Not idempotent | Different results on repeated runs |
| **Data Consistency** | Breaks referential integrity | Query results sum duplicates |

### Why This Happens

Monthly aggregation is a **derived** calculation from daily data:
- Daily data is immutable (source of truth)
- Monthly should always equal SUM of daily data
- Each month should have exactly 1 row per unique (client, partner, direction, month, call_type, service, event)

When using `.append()`:
- Old aggregations stay
- New aggregations get added
- Both exist simultaneously
- Queries double/triple count values

---

## Impact Analysis

### Business Impact

#### **Financial Impact**
```
If monthly billing is based on gold.client_partner_traffic_monthly:
  Correct charge: 210 GB * $0.50/GB = $105.00
  Actual charge: (100 + 210) GB * $0.50/GB = $155.00
  
  Error per month: $50.00 OVERCHARGE per client-partner pair
  With 100+ client pairs: Potential $5,000+ overbilling per month
```

#### **Analytics Impact**
```
If monthly reports use this table:
  Report: "ClientA used 640 GB in July"
  Reality: "ClientA used 330 GB in July"
  
  Report accuracy: 48% (640/330 = 1.94x too high)
  Decision impact: Budget forecasts will be completely wrong
```

#### **Operational Impact**
```
If data quality monitoring is in place:
  ├─ Duplicate key detection triggers alerts
  ├─ Data science team must investigate
  ├─ Time lost debugging
  ├─ Potential data corruption
  └─ Loss of trust in data warehouse
```

### Data Quality Metrics

#### **Duplicate Key Ratio**

After several days, the monthly table becomes highly duplicated:

```
Days Running | Daily Rows | Monthly Rows | Duplicates | Accuracy
1            | 3          | 3            | 0%         | 100%
2            | 5          | 7            | 57%        | 43%
3            | 8          | 11           | 73%        | 27%
4            | 11         | 15           | 80%        | 20%
5            | 14         | 19           | 84%        | 16%
```

#### **Query Result Accuracy**

```
Days Running | True Total | Query Result | Accuracy
1            | 100 GB     | 100 GB       | 100%
2            | 210 GB     | 310 GB       | 68%
3            | 330 GB     | 640 GB       | 52%
4            | 440 GB     | 990 GB       | 44%
5            | 540 GB     | 1,440 GB     | 37%
```

---

## The Solution

### Root Solution

**Change the storage strategy** from `.append()` to `.overwritePartitions()` in the monthly load.

### Implementation

#### **File to Modify**

**Path:** `/home/rituraj.vaishnav@nextgen.local/projects/python/datawarehouse/jobs/ingestion/traffic/load_gold_monthly.py`

**Line:** 110

#### **Exact Change**

```python
# BEFORE (WRONG - Line 110)
final_df.writeTo(table_name).append()

# AFTER (CORRECT - Line 110)
final_df.writeTo(table_name).overwritePartitions()
```

### Why This Works

#### **How `.overwritePartitions()` Works**

Iceberg's `.overwritePartitions()` is intelligent:

```
1. Read the aggregated data (new calculation)
2. Extract partition values (call_month = '2026-07')
3. Delete all rows with that partition value
4. Insert the new rows
5. All atomically (all or nothing)

Result: Clean replacement, no duplicates
```

#### **Partition Isolation**

```
Before overwritePartitions():
├─ 2026-05 partition: 20 rows
├─ 2026-06 partition: 22 rows
└─ 2026-07 partition: 11 rows (DUPLICATES)

After running overwritePartitions() for 2026-07:
├─ 2026-05 partition: 20 rows ← UNCHANGED
├─ 2026-06 partition: 22 rows ← UNCHANGED
└─ 2026-07 partition: 4 rows  ← REPLACED (clean data)
```

### Idempotency Guarantee

```
Run 1 (July 1-3):
  Input: 8 daily rows
  Output: 4 monthly rows

Run 1 Again (same input):
  Input: Same 8 daily rows
  Output: Same 4 monthly rows ✓ Idempotent

Run 2 (July 1-4):
  Input: 11 daily rows
  Output: 4-5 monthly rows
  
Run 2 Again (same input):
  Input: Same 11 daily rows
  Output: Same 4-5 monthly rows ✓ Idempotent
```

### Alternative Solutions (Considered but Not Recommended)

#### **Option 1: Full Table Overwrite**

```python
final_df.writeTo(table_name).overwrite()
```

**Pros:** Simple, comprehensive
**Cons:** Overwrites entire table including unrelated months

#### **Option 2: Merge Pattern**

```python
from pyspark.sql.iceberg import IcebergMergePlan
(final_df.writeTo(table_name)
 .usingMerge()
 .onColumnValues("client_pmn", "partner_pmn", "call_month")
 .whenMatched()
 .updateAll()
 .whenNotMatched()
 .insertAll()
 .execute())
```

**Pros:** More control, explicit logic
**Cons:** More complex, unnecessary overhead

#### **Recommended: `.overwritePartitions()`**

**Pros:**
- Simple one-line change
- Only affects current month partition
- Atomic operation
- Efficient (Iceberg optimized)
- Standard pattern for analytical loads

**Cons:** None

---

## Data Quality Verification

### Pre-Fix Verification Queries

Before applying the fix, run these to confirm the problem exists:

#### **Query 1: Detect Duplicates**

```sql
-- Shows duplicate business keys
SELECT 
  client_pmn,
  partner_pmn,
  traffic_direction,
  call_month,
  call_type,
  call_type_level_2,
  service_type_id,
  event_type_id,
  COUNT(*) as occurrences
FROM gold.client_partner_traffic_monthly
GROUP BY 
  client_pmn,
  partner_pmn,
  traffic_direction,
  call_month,
  call_type,
  call_type_level_2,
  service_type_id,
  event_type_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- Expected (current): Multiple rows showing duplicates
-- Expected (after fix): Empty result set (0 rows)
```

#### **Query 2: Compare Totals**

```sql
-- Compares daily sum vs monthly aggregation
WITH daily_totals AS (
  SELECT
    client_pmn,
    partner_pmn,
    SUBSTRING(call_date, 1, 7) as month_calc,
    SUM(total_volume) as daily_volume_sum,
    SUM(total_event_count) as daily_events_sum
  FROM gold.imsi_level_traffic_daily
  GROUP BY 
    client_pmn,
    partner_pmn,
    SUBSTRING(call_date, 1, 7)
)
SELECT
  d.client_pmn,
  d.partner_pmn,
  d.month_calc,
  d.daily_volume_sum,
  SUM(m.total_volume) as monthly_volume_sum,
  (SUM(m.total_volume) - d.daily_volume_sum) as volume_difference,
  ROUND(100.0 * ABS(SUM(m.total_volume) - d.daily_volume_sum) / d.daily_volume_sum, 2) as percent_error
FROM daily_totals d
LEFT JOIN gold.client_partner_traffic_monthly m
  ON d.client_pmn = m.client_pmn
  AND d.partner_pmn = m.partner_pmn
  AND d.month_calc = m.call_month
GROUP BY 
  d.client_pmn,
  d.partner_pmn,
  d.month_calc,
  d.daily_volume_sum
HAVING SUM(m.total_volume) != d.daily_volume_sum
ORDER BY percent_error DESC;

-- Expected (current): Shows percent_error > 0 for most rows
-- Expected (after fix): Empty result set (0 rows)
```

#### **Query 3: Row Count Explosion**

```sql
-- Shows how quickly rows grow without fix
SELECT
  call_month,
  COUNT(*) as total_rows,
  COUNT(DISTINCT CONCAT(client_pmn, '|', partner_pmn, '|', traffic_direction, '|', call_type, '|', service_type_id, '|', event_type_id)) as unique_keys,
  ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT CONCAT(client_pmn, '|', partner_pmn, '|', traffic_direction, '|', call_type, '|', service_type_id, '|', event_type_id))) / COUNT(*), 2) as duplicate_percentage
FROM gold.client_partner_traffic_monthly
GROUP BY call_month
ORDER BY call_month DESC;

-- Expected (current): duplicate_percentage > 0
-- Expected (after fix): duplicate_percentage = 0
```

### Post-Fix Verification Queries

After applying the fix, run these to confirm it's working correctly:

#### **Query 1: Verify No Duplicates**

```sql
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT CONCAT(client_pmn, '|', partner_pmn, '|', traffic_direction, '|', call_month, '|', call_type, '|', service_type_id, '|', event_type_id)) as unique_keys,
  CASE 
    WHEN COUNT(*) = COUNT(DISTINCT CONCAT(client_pmn, '|', partner_pmn, '|', traffic_direction, '|', call_month, '|', call_type, '|', service_type_id, '|', event_type_id))
    THEN 'PASS - No duplicates'
    ELSE 'FAIL - Duplicates exist'
  END as data_quality_status
FROM gold.client_partner_traffic_monthly;

-- Expected: "PASS - No duplicates"
```

#### **Query 2: Verify Aggregation Accuracy**

```sql
WITH daily_check AS (
  SELECT
    client_pmn,
    partner_pmn,
    SUBSTRING(call_date, 1, 7) as calc_month,
    traffic_direction,
    call_type,
    call_type_level_2,
    service_type_id,
    event_type_id,
    SUM(total_volume) as d_volume,
    SUM(total_duration) as d_duration,
    SUM(total_event_count) as d_events
  FROM gold.imsi_level_traffic_daily
  GROUP BY 
    client_pmn, partner_pmn, SUBSTRING(call_date, 1, 7),
    traffic_direction, call_type, call_type_level_2,
    service_type_id, event_type_id
)
SELECT
  d.client_pmn,
  d.partner_pmn,
  d.calc_month,
  d.d_volume,
  m.total_volume,
  CASE
    WHEN ABS(d.d_volume - m.total_volume) < 0.01 THEN 'MATCH'
    ELSE 'MISMATCH'
  END as volume_match
FROM daily_check d
LEFT JOIN gold.client_partner_traffic_monthly m
  ON d.client_pmn = m.client_pmn
  AND d.partner_pmn = m.partner_pmn
  AND d.calc_month = m.call_month
  AND d.traffic_direction = m.traffic_direction
  AND d.call_type = m.call_type
  AND d.service_type_id = m.service_type_id
  AND d.event_type_id = m.event_type_id
WHERE ABS(d.d_volume - m.total_volume) >= 0.01 OR m.total_volume IS NULL;

-- Expected: Empty result set (0 rows) - all match
```

#### **Query 3: Verify Distinct Counts**

```sql
WITH daily_distinct AS (
  SELECT
    client_pmn,
    partner_pmn,
    SUBSTRING(call_date, 1, 7) as calc_month,
    traffic_direction,
    call_type,
    service_type_id,
    event_type_id,
    COUNT(DISTINCT imsi) as distinct_imsis,
    COUNT(DISTINCT apn) as distinct_apns
  FROM gold.imsi_level_traffic_daily
  GROUP BY 
    client_pmn, partner_pmn, SUBSTRING(call_date, 1, 7),
    traffic_direction, call_type, service_type_id, event_type_id
)
SELECT
  d.client_pmn,
  d.partner_pmn,
  d.calc_month,
  d.distinct_imsis as daily_distinct_imsi,
  m.distinct_imsi_count as monthly_distinct_imsi,
  d.distinct_apns as daily_distinct_apn,
  m.distinct_apn_count as monthly_distinct_apn
FROM daily_distinct d
LEFT JOIN gold.client_partner_traffic_monthly m
  ON d.client_pmn = m.client_pmn
  AND d.partner_pmn = m.partner_pmn
  AND d.calc_month = m.call_month
  AND d.traffic_direction = m.traffic_direction
  AND d.call_type = m.call_type
  AND d.service_type_id = m.service_type_id
  AND d.event_type_id = m.event_type_id
WHERE d.distinct_imsis != m.distinct_imsi_count
   OR d.distinct_apns != m.distinct_apn_count;

-- Expected: Empty result set (0 rows) - all match
```

---

## SQL Queries & Examples

### Analytical Queries

#### **Query 1: Top Clients by Traffic**

```sql
SELECT
  client_pmn,
  call_month,
  SUM(total_volume) as total_volume_gb,
  SUM(total_event_count) as total_events,
  COUNT(DISTINCT partner_pmn) as num_partners,
  ROUND(AVG(total_volume), 2) as avg_volume_per_partner
FROM gold.client_partner_traffic_monthly
WHERE call_month >= '2026-05'
GROUP BY client_pmn, call_month
ORDER BY call_month DESC, total_volume_gb DESC;

-- Shows which clients are using the most traffic each month
```

#### **Query 2: Traffic Direction Analysis**

```sql
SELECT
  call_month,
  traffic_direction,
  COUNT(DISTINCT CONCAT(client_pmn, ':', partner_pmn)) as client_partner_pairs,
  SUM(total_volume) as total_volume,
  SUM(total_duration) as total_duration_hours,
  SUM(total_event_count) as total_events,
  ROUND(SUM(total_charge_sdr_gross), 2) as total_revenue
FROM gold.client_partner_traffic_monthly
WHERE call_month >= '2026-05'
GROUP BY call_month, traffic_direction
ORDER BY call_month DESC, total_volume DESC;

-- Compares incoming vs outgoing traffic patterns
```

#### **Query 3: Partner Performance**

```sql
SELECT
  partner_pmn,
  roaming_partner_country,
  call_month,
  SUM(total_volume) as total_volume,
  COUNT(DISTINCT client_pmn) as num_clients,
  ROUND(AVG(total_duration), 2) as avg_duration_per_session,
  ROUND(SUM(total_charge_sdr_gross) / SUM(total_event_count), 4) as revenue_per_event
FROM gold.client_partner_traffic_monthly
WHERE call_month >= '2026-05'
GROUP BY partner_pmn, roaming_partner_country, call_month
ORDER BY call_month DESC, total_volume DESC;

-- Evaluates partner network quality and revenue contribution
```

#### **Query 4: Monthly Trend Analysis**

```sql
SELECT
  call_month,
  SUM(total_volume) as monthly_volume,
  SUM(total_event_count) as monthly_events,
  ROUND(SUM(total_duration), 0) as monthly_hours,
  ROUND(SUM(total_charge_sdr_gross), 2) as monthly_revenue,
  ROUND(SUM(total_charge_sdr_gross) / SUM(total_event_count), 4) as revenue_per_event,
  LAG(SUM(total_volume)) OVER (ORDER BY call_month) as prev_month_volume,
  ROUND(100.0 * (SUM(total_volume) - LAG(SUM(total_volume)) OVER (ORDER BY call_month)) / LAG(SUM(total_volume)) OVER (ORDER BY call_month), 2) as volume_growth_percent
FROM gold.client_partner_traffic_monthly
GROUP BY call_month
ORDER BY call_month DESC;

-- Shows growth trends month over month
```

#### **Query 5: Service Type Breakdown**

```sql
SELECT
  call_month,
  call_type,
  call_type_level_2,
  SUM(total_volume) as total_volume,
  SUM(total_event_count) as total_events,
  COUNT(DISTINCT client_pmn) as num_clients,
  ROUND(100.0 * SUM(total_volume) / SUM(SUM(total_volume)) OVER (PARTITION BY call_month), 2) as volume_percentage
FROM gold.client_partner_traffic_monthly
WHERE call_month >= '2026-05'
GROUP BY call_month, call_type, call_type_level_2
ORDER BY call_month DESC, total_volume DESC;

-- Shows which call types (Voice, SMS, Data) generate most traffic
```

#### **Query 6: Billing Verification**

```sql
SELECT
  client_pmn,
  partner_pmn,
  call_month,
  total_volume,
  total_charge_sdr_gross,
  total_charge_sdr_net,
  (total_charge_sdr_gross - total_charge_sdr_net) as charges_deducted,
  ROUND(total_charge_sdr_gross / NULLIF(total_volume, 0), 4) as rate_per_gb,
  ROUND(total_charge_sdr_gross / NULLIF(total_event_count, 0), 6) as rate_per_event
FROM gold.client_partner_traffic_monthly
WHERE call_month = '2026-07'
ORDER BY total_charge_sdr_gross DESC;

-- For finance: validate billing calculations per client-partner pair
```

#### **Query 7: Device Type Analysis (From Daily)**

```sql
SELECT
  call_date,
  rat_type,
  COUNT(DISTINCT imsi) as unique_devices,
  SUM(total_volume) as total_volume,
  SUM(total_event_count) as total_events,
  ROUND(AVG(total_duration), 2) as avg_session_duration
FROM gold.imsi_level_traffic_daily
WHERE call_date >= '2026-07-01'
GROUP BY call_date, rat_type
ORDER BY call_date DESC, total_volume DESC;

-- Shows which network generations (2G/3G/4G/5G) carry most traffic
```

#### **Query 8: Destination Analysis (From Daily)**

```sql
SELECT
  destination,
  destination_category,
  COUNT(DISTINCT imsi) as unique_users,
  SUM(total_volume) as total_volume,
  COUNT(DISTINCT SUBSTRING(call_date, 1, 7)) as months_active
FROM gold.imsi_level_traffic_daily
WHERE call_date >= '2026-05-01'
GROUP BY destination, destination_category
ORDER BY total_volume DESC
LIMIT 20;

-- Top destinations where users roam
```

### Aggregation Verification Queries

#### **Query 1: Rebuild Monthly from Daily (Verification)**

```sql
-- This query recreates monthly table manually to verify correctness
SELECT
  d.client_pmn,
  d.partner_pmn,
  d.roaming_partner_country,
  d.traffic_direction,
  d.call_month,
  YEAR(CAST(SUBSTRING(d.call_month || '-01', 1, 10) AS DATE)) as year,
  MONTH(CAST(SUBSTRING(d.call_month || '-01', 1, 10) AS DATE)) as month,
  d.call_type,
  d.call_type_level_2,
  d.service_type_id,
  d.event_type_id,
  SUM(d.total_duration) as total_duration,
  SUM(d.total_volume) as total_volume,
  SUM(d.total_event_count) as total_event_count,
  SUM(d.total_charge_sdr_net) as total_charge_sdr_net,
  SUM(d.total_charge_sdr_gross) as total_charge_sdr_gross,
  COUNT(DISTINCT d.imsi) as distinct_imsi_count,
  COUNT(DISTINCT d.apn) as distinct_apn_count,
  CURRENT_TIMESTAMP() as created_at,
  CURRENT_TIMESTAMP() as updated_at
FROM gold.imsi_level_traffic_daily d
GROUP BY
  d.client_pmn,
  d.partner_pmn,
  d.roaming_partner_country,
  d.traffic_direction,
  d.call_month,
  d.call_type,
  d.call_type_level_2,
  d.service_type_id,
  d.event_type_id;

-- Run this query and compare output with gold.client_partner_traffic_monthly
-- Should match exactly (after fix is applied)
```

### Debug Queries

#### **Query 1: Find Problematic Records**

```sql
-- Find records with unusual aggregation values
SELECT
  client_pmn,
  partner_pmn,
  call_month,
  total_volume,
  total_event_count,
  CASE
    WHEN total_event_count = 0 THEN 'ERROR: Zero events'
    WHEN total_volume = 0 AND total_event_count > 0 THEN 'WARNING: No volume'
    WHEN total_volume > 10000 THEN 'WARNING: Very high volume'
    WHEN total_event_count > 1000000 THEN 'WARNING: Very high events'
    ELSE 'OK'
  END as data_quality_flag
FROM gold.client_partner_traffic_monthly
WHERE call_month >= '2026-05'
  AND (total_event_count = 0 
       OR (total_volume = 0 AND total_event_count > 0)
       OR total_volume > 10000
       OR total_event_count > 1000000);

-- Identifies outliers and data quality issues
```

#### **Query 2: Row Count Tracking**

```sql
-- Track row count growth over time
SELECT
  call_month,
  COUNT(*) as row_count,
  COUNT(DISTINCT client_pmn) as unique_clients,
  COUNT(DISTINCT partner_pmn) as unique_partners,
  COUNT(DISTINCT call_type) as unique_call_types,
  ROUND(SUM(total_volume), 2) as total_volume_gb
FROM gold.client_partner_traffic_monthly
GROUP BY call_month
ORDER BY call_month DESC;

-- Should show stable row counts after fix
-- Before fix: explosive growth each day
```

---

## Practical Scenarios

### Scenario 1: Daily Billing Calculation

**Use Case:** Calculate daily usage charge for ClientA

**Query:**

```sql
-- Daily billing for specific client
SELECT
  call_date,
  call_type,
  SUM(total_volume) as daily_volume,
  SUM(total_event_count) as daily_events,
  ROUND(SUM(total_volume) * 0.50, 2) as daily_charge_volume_based
FROM gold.imsi_level_traffic_daily
WHERE client_pmn = 'ClientA'
  AND call_date = '2026-07-02'
GROUP BY call_date, call_type;

-- Result:
-- 2026-07-02 | Voice | 45.5 GB | 22000 | $22.75
-- 2026-07-02 | SMS   | 0.1 GB  | 2000  | $0.05
-- 2026-07-02 | Data  | 15.2 GB | 8000  | $7.60
-- Total Daily: $30.40
```

### Scenario 2: Monthly Invoice Generation

**Use Case:** Generate monthly invoice for ClientA with PartnerX

**Query:**

```sql
SELECT
  client_pmn,
  partner_pmn,
  roaming_partner_country,
  call_month,
  COUNT(DISTINCT traffic_direction) as directions,
  SUM(total_volume) as total_volume,
  SUM(total_event_count) as total_events,
  SUM(total_duration) as total_hours,
  ROUND(SUM(total_charge_sdr_gross), 2) as total_charge_gross,
  ROUND(SUM(total_charge_sdr_net), 2) as total_charge_net,
  ROUND(SUM(total_charge_sdr_gross) - SUM(total_charge_sdr_net), 2) as discounts
FROM gold.client_partner_traffic_monthly
WHERE client_pmn = 'ClientA'
  AND partner_pmn = 'PartnerX'
  AND call_month = '2026-07';

-- Result:
-- ClientA | PartnerX | US | 2026-07 | 2 | 330.15 GB | 165000 | 3960 hrs | $165.08 | $150.00 | $15.08
```

### Scenario 3: Partner Performance Comparison

**Use Case:** Compare July performance across all roaming partners

**Query:**

```sql
SELECT
  partner_pmn,
  roaming_partner_country,
  COUNT(DISTINCT client_pmn) as num_clients,
  COUNT(DISTINCT traffic_direction) as directions,
  ROUND(SUM(total_volume), 2) as total_volume,
  ROUND(SUM(total_duration), 0) as total_hours,
  ROUND(AVG(total_volume), 2) as avg_volume_per_client_pair,
  ROUND(SUM(total_charge_sdr_gross), 2) as revenue
FROM gold.client_partner_traffic_monthly
WHERE call_month = '2026-07'
GROUP BY partner_pmn, roaming_partner_country
ORDER BY total_volume DESC;

-- Shows which partners drive most traffic and revenue
```

### Scenario 4: Trend Analysis

**Use Case:** Identify traffic trends over last 3 months

**Query:**

```sql
SELECT
  call_month,
  call_type,
  SUM(total_volume) as volume,
  LAG(SUM(total_volume)) OVER (PARTITION BY call_type ORDER BY call_month) as prev_month_volume,
  ROUND(100.0 * (SUM(total_volume) - LAG(SUM(total_volume)) OVER (PARTITION BY call_type ORDER BY call_month)) / LAG(SUM(total_volume)) OVER (PARTITION BY call_type ORDER BY call_month), 2) as growth_percent
FROM gold.client_partner_traffic_monthly
WHERE call_month IN ('2026-05', '2026-06', '2026-07')
GROUP BY call_month, call_type
ORDER BY call_month, call_type;

-- Shows month-over-month growth for each call type
```

### Scenario 5: Roaming Device Analysis

**Use Case:** Identify roaming vs non-roaming user patterns

**Query:**

```sql
SELECT
  call_date,
  roamer_indicator,
  COUNT(DISTINCT imsi) as unique_devices,
  SUM(total_volume) as total_volume,
  ROUND(AVG(total_duration), 2) as avg_session_duration,
  ROUND(SUM(total_volume) / COUNT(DISTINCT imsi), 2) as avg_volume_per_device
FROM gold.imsi_level_traffic_daily
WHERE call_date >= '2026-07-01'
GROUP BY call_date, roamer_indicator
ORDER BY call_date DESC;

-- Compare roaming vs home user behavior
```

---

## Debugging Guide

### Common Issues & Solutions

#### **Issue 1: Query Results Keep Changing**

**Symptom:**
```
Same query, run at different times, returns different results
SELECT SUM(total_volume) FROM gold.client_partner_traffic_monthly
WHERE client_pmn = 'ClientA' AND call_month = '2026-07';

Time 1: 310 GB
Time 2: 640 GB
Time 3: 990 GB
```

**Root Cause:** `.append()` is adding duplicate rows each time monthly load runs

**Diagnosis Query:**
```sql
SELECT COUNT(*) as total_rows,
       COUNT(DISTINCT CONCAT(client_pmn, '|', partner_pmn, '|', call_month)) as unique_groups
FROM gold.client_partner_traffic_monthly
WHERE call_month = '2026-07';

-- If total_rows > unique_groups → duplicates exist
```

**Solution:** Apply the fix to use `.overwritePartitions()`

#### **Issue 2: Duplicate Rows in Monthly Table**

**Symptom:**
```
SELECT client_pmn, partner_pmn, call_month, COUNT(*) as cnt
FROM gold.client_partner_traffic_monthly
GROUP BY client_pmn, partner_pmn, call_month
HAVING cnt > 1;

Results: Multiple rows with cnt = 2, 3, 4, etc.
```

**Root Cause:** Monthly load using `.append()` instead of `.overwrite()`

**Diagnosis:**
```sql
-- See which keys have duplicates
SELECT client_pmn, partner_pmn, call_month, COUNT(*) as cnt
FROM gold.client_partner_traffic_monthly
GROUP BY client_pmn, partner_pmn, call_month
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 20;

-- Then look at specific duplicates
SELECT * FROM gold.client_partner_traffic_monthly
WHERE client_pmn = 'ClientA' 
  AND partner_pmn = 'PartnerX' 
  AND call_month = '2026-07'
ORDER BY created_at;
```

**Solution:** Apply the fix

#### **Issue 3: Monthly Aggregation Doesn't Match Daily Sum**

**Symptom:**
```
Daily sum: 210 GB
Monthly aggregation: 331 GB
(Should match for that month and client-partner)
```

**Root Cause:** Could be duplicates in monthly (summing multiple times), or aggregation logic error

**Diagnosis:**
```sql
-- Calculate daily sum
SELECT SUM(total_volume) as daily_sum
FROM gold.imsi_level_traffic_daily
WHERE client_pmn = 'ClientA' 
  AND partner_pmn = 'PartnerX' 
  AND SUBSTRING(call_date, 1, 7) = '2026-07';

-- Calculate monthly sum
SELECT SUM(total_volume) as monthly_sum
FROM gold.client_partner_traffic_monthly
WHERE client_pmn = 'ClientA' 
  AND partner_pmn = 'PartnerX' 
  AND call_month = '2026-07';

-- If different, check for duplicates in monthly
SELECT COUNT(*) FROM gold.client_partner_traffic_monthly
WHERE client_pmn = 'ClientA' 
  AND partner_pmn = 'PartnerX' 
  AND call_month = '2026-07';
-- If > 1, duplicates exist
```

**Solution:** Apply the fix to eliminate duplicates

#### **Issue 4: Daily Load Fails But Monthly Load Runs**

**Symptom:**
```
Error: load_gold_daily.py fails
Result: Monthly load still appends old aggregations
Impact: Incorrect monthly data created
```

**Prevention:** Monthly load should wait for successful daily load

**Check:** Review DAG dependencies in `traffic_ingest.py`

#### **Issue 5: Data Freshness Problem**

**Symptom:**
```
Today's date: 2026-07-05
Latest data in gold.imsi_level_traffic_daily: 2026-07-03
Gap: 2 days
```

**Root Cause:** Daily load job not running or failing silently

**Diagnosis:**
```sql
SELECT MAX(call_date) as latest_date,
       DATEDIFF(day, MAX(call_date), CURRENT_DATE()) as days_behind
FROM gold.imsi_level_traffic_daily;

-- Check job logs
```

**Solution:** Check job scheduler, retry failed jobs

---

## Implementation Checklist

### Pre-Implementation

- [ ] **Backup Current Data**
  ```bash
  # Create backup of current monthly table
  # (Ask your DBA/data engineer for backup procedure)
  ```

- [ ] **Verify Problem Exists**
  ```sql
  SELECT COUNT(*) as total_rows,
         COUNT(DISTINCT CONCAT(client_pmn, '|', partner_pmn, '|', call_month)) as unique_keys
  FROM gold.client_partner_traffic_monthly;
  
  -- If total_rows > unique_keys, problem confirmed
  ```

- [ ] **Run All Diagnostic Queries**
  ```sql
  -- Run Query 1, 2, 3 from "Pre-Fix Verification Queries" section
  -- Document baseline metrics
  ```

### Implementation

- [ ] **Update Code**
  
  **File:** `load_gold_monthly.py` (Line 110)
  
  ```python
  # Change from:
  final_df.writeTo(table_name).append()
  
  # Change to:
  final_df.writeTo(table_name).overwritePartitions()
  ```

- [ ] **Test Change**
  ```bash
  # Run job in test environment first
  python load_gold_monthly.py --test --month 2026-07
  ```

- [ ] **Verify Job Runs Successfully**
  ```bash
  # Check job logs for:
  # ✓ "Loaded X rows to ..."
  # ✓ No errors
  # ✓ Job completed
  ```

- [ ] **Run Post-Fix Verification**
  ```sql
  -- Run Query 1, 2, 3 from "Post-Fix Verification Queries" section
  -- Verify results match expected
  ```

### Post-Implementation

- [ ] **Run Full Daily & Monthly Load**
  ```bash
  # Load today's data
  python load_gold_daily.py
  python load_gold_monthly.py
  ```

- [ ] **Verify No Duplicates**
  ```sql
  SELECT COUNT(*) as cnt
  FROM gold.client_partner_traffic_monthly m1
  WHERE EXISTS (
    SELECT 1 FROM gold.client_partner_traffic_monthly m2
    WHERE m1.rowid != m2.rowid
      AND m1.client_pmn = m2.client_pmn
      AND m1.partner_pmn = m2.partner_pmn
      AND m1.call_month = m2.call_month
  );
  
  -- Should return 0
  ```

- [ ] **Compare with Previous Data**
  ```sql
  -- Old (problematic) vs new (correct) monthly table
  -- Verify metrics are reasonable
  ```

- [ ] **Update Documentation**
  ```markdown
  # Update CLAUDE.md or project docs
  - Note the fix applied
  - Record the date
  - Add verification queries used
  ```

- [ ] **Commit Changes**
  ```bash
  git add jobs/ingestion/traffic/load_gold_monthly.py
  git commit -m "Fix: Gold monthly table duplicate rows using overwritePartitions
  
  - Changed load_gold_monthly.py line 110
  - From: final_df.writeTo(table_name).append()
  - To: final_df.writeTo(table_name).overwritePartitions()
  - Fixes: Issue with duplicate client+partner+month keys
  - Impact: Monthly aggregations now idempotent and accurate"
  ```

- [ ] **Monitor for 7 Days**
  ```sql
  -- Run daily to confirm no regressions
  SELECT call_month, COUNT(*) as row_count
  FROM gold.client_partner_traffic_monthly
  GROUP BY call_month
  ORDER BY call_month DESC;
  
  -- Should show stable row counts
  ```

---

## Summary Table

| Aspect | Daily Load | Monthly Load |
|--------|-----------|-------------|
| **Source** | Silver fact table | Gold daily table |
| **Transformation** | Denormalize (join 13 dimensions) | Aggregate (SUM, COUNT DISTINCT) |
| **Grain** | Transaction-level detail | Monthly summary |
| **Writing Strategy** | `.append()` ✓ | `.append()` ❌ → `.overwritePartitions()` ✓ |
| **Idempotent** | Yes (immutable daily data) | No (with `.append()`) / Yes (with `.overwrite()`) |
| **Partitioning** | By `call_date` | By `call_month` |
| **Row Growth** | Linear (1 day = new rows) | Exponential with `.append()` (duplicates) |
| **Business Use** | Detailed reporting | Billing, monthly summaries |
| **Data Quality** | Generally good | **Currently broken, fixed by change** |
| **Performance** | Fast (daily partitions) | Good after fix |

---

## Contact & Support

**Issue Identified By:** Claude Code Analysis  
**Date:** 2026-07-19  
**Severity:** High (Financial impact)  
**Fix Complexity:** Very Low (One-line change)  
**Estimated Fix Time:** 5 minutes  
**Risk Level:** Very Low (only affects future loads, immutable history)

---

**End of Document**
