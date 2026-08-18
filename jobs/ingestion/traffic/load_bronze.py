"""
Load Bronze Traffic data from tenant-specific directories.

Discovers all tenant directories and loads files ONLY from tenant/{tenant_name}/Bronze/Traffic/
into the configured Iceberg table.

LAYER VALIDATION: Only picks files from Bronze/Traffic/ directory.
Avoids loading files from Silver/Traffic/ or any other layers.

Usage:
    docker compose exec spark /opt/platform/jobs/common/run_spark_submit.sh \
        /opt/platform/jobs/ingestion/load_bronze_traffic.py
"""

import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List

import boto3
from botocore.config import Config as BotoConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, "/opt/airflow")
from jobs.common.domain_to_table_mapping import DomainTableMapping, validate_domain_and_layer
from jobs.common.spark_catalog import register_iceberg_catalog

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def env_or_default(name: str, default: str) -> str:
    return os.getenv(name) or default


@dataclass
class Config:
    storage_endpoint: str = field(default_factory=lambda: env_or_default("OCI_S3_ENDPOINT", "http://minio:9000"))
    source_bucket: str = field(default_factory=lambda: env_or_default("SOURCE_BUCKET", env_or_default("LANDING_BUCKET", "landing")))
    source_prefix: str = field(default_factory=lambda: "tenant")
    domain: str = field(default_factory=lambda: "traffic")
    layer: str = field(default_factory=lambda: "bronze")
    tenant: str = field(default_factory=lambda: env_or_default("TENANT", ""))
    file_key: str | None = field(default_factory=lambda: os.getenv("FILE_KEY"))
    aws_access_key: str | None = field(default_factory=lambda: os.getenv("OCI_ACCESS_KEY_ID"))
    aws_secret_key: str | None = field(default_factory=lambda: os.getenv("OCI_SECRET_ACCESS_KEY"))
    merge_schema: bool = field(default_factory=lambda: env_or_default("MERGE_SCHEMA", "false").lower() == "true")

    SUPPORTED_EXTENSIONS = (".parquet", ".csv")
    CSV_DELIMITERS = ("~", ",", ";", "|", "\t")

    def __post_init__(self) -> None:
        # Use tenant name as namespace if provided, otherwise use ICEBERG_NAMESPACE env var
        if self.tenant:
            # Normalize tenant name to lowercase for Iceberg namespace consistency
            normalized_tenant = self.tenant.lower()
            os.environ["ICEBERG_NAMESPACE"] = normalized_tenant
            self.tenant = normalized_tenant

        mapping = DomainTableMapping()
        table_config = mapping.get_table_config(self.domain, self.layer)
        if not table_config:
            raise ValueError(
                f"Domain '{self.domain}' with layer '{self.layer}' is not configured. "
                f"Supported combinations: {list(mapping.DOMAIN_MAPPINGS.keys())}"
            )

        self.iceberg_namespace = table_config.iceberg_namespace
        self.iceberg_table_name = table_config.table_name
        self.iceberg_table = table_config.full_path
        self.table_location = table_config.warehouse_location


BRONZE_IMSI_LEVEL_TRAFFIC_COLUMN_MAP = {
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
    "roamer_indicator": ["permanent_roamer_ind", "subscriber_profile"],
    "total_charge_sdr_net": ["total_charge_sdr_net"],
    "total_charge_sdr_gross": ["total_charge_sdr_gross"],
    "iot_rti_group_id": ["iot_rti_group_id"],
    "service_type_id": ["service_type_id"],
    "event_type_id": ["event_type_id"],
    "call_type_level_2": ["call_type_level_2"],
    "rat_type": ["rat_type"],
    "tac_number": ["tac_number"],
    "roaming_partner_country": ["roaming_partner_country"],
    "call_month": ["call_month"],
    "destination_category": ["destination_category"],
    "destination": ["destination"],
    "is_camel": ["is_camel"],
    "source_system": ["source_system"],
    "source_file_name": ["source_file_name"],
    "ingestion_id": ["ingestion_id"],
    "batch_id": ["batch_id"],
    "record_hash": ["record_hash"],
    "received_at": ["received_at"],
    "processing_status": ["processing_status"],
    "error_message": ["error_message"],
}


def normalize_column_name(name: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip()).strip("_").lower()
    return cleaned or "col"


def normalize_columns(df: DataFrame) -> DataFrame:
    seen: dict[str, int] = {}
    for old_name in df.columns:
        new_name = normalize_column_name(old_name)
        if new_name in seen:
            seen[new_name] += 1
            new_name = f"{new_name}_{seen[new_name]}"
        else:
            seen[new_name] = 0
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def cast_all_columns_to_string(df: DataFrame) -> DataFrame:
    return df.select([F.col(column).cast(StringType()).alias(column) for column in df.columns])


def normalize_date_formats(df: DataFrame) -> DataFrame:
    """Normalize date columns to YYYY-MM-DD format.

    Handles multiple input formats:
    - YYYYMMDD (e.g., 20260409) → 2026-04-09
    - YYYY-MM-DD (e.g., 2026-04-09) → unchanged
    - Empty/null → unchanged
    """
    if "call_date" not in df.columns:
        return df

    # Detect and normalize call_date format
    df = df.withColumn(
        "call_date",
        F.when(
            F.col("call_date").isNull() | (F.col("call_date") == ""),
            F.col("call_date")
        ).when(
            # Already in YYYY-MM-DD format
            F.col("call_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.col("call_date")
        ).when(
            # YYYYMMDD format (8 digits)
            F.col("call_date").rlike(r"^\d{8}$"),
            F.concat(
                F.substring(F.col("call_date"), 1, 4),
                F.lit("-"),
                F.substring(F.col("call_date"), 5, 2),
                F.lit("-"),
                F.substring(F.col("call_date"), 7, 2)
            )
        ).otherwise(F.col("call_date"))
    )

    logger.info("Normalized date formats in call_date column")
    return df


def remap_source_columns(df: DataFrame) -> DataFrame:
    """Remap source columns to target column names using BRONZE_IMSI_LEVEL_TRAFFIC_COLUMN_MAP.

    For each target column, try source column variations in order. First match found is used.
    """
    source_columns = {col.lower(): col for col in df.columns}
    remapping = {}
    unmapped_targets = []

    for target_col, source_variations in BRONZE_IMSI_LEVEL_TRAFFIC_COLUMN_MAP.items():
        found = False
        for source_col in source_variations:
            source_lower = source_col.lower()
            if source_lower in source_columns:
                if source_columns[source_lower] != target_col:
                    remapping[source_columns[source_lower]] = target_col
                    logger.debug("Mapping FOUND: target=%s, source=%s (from variations: %s)", target_col, source_columns[source_lower], source_variations)
                found = True
                break
        if not found:
            unmapped_targets.append(target_col)

    if unmapped_targets:
        logger.warning("COLUMN MAPPING: %d target columns not found in source: %s", len(unmapped_targets), unmapped_targets)

    remap_count = len(remapping)
    logger.info("COLUMN REMAPPING: %d source columns will be renamed", remap_count)

    for old_name, new_name in remapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            logger.info("  Remapped: %s → %s", old_name, new_name)

    logger.info("COLUMN REMAPPING COMPLETED: %d columns renamed", remap_count)
    return df


def add_audit_columns(df: DataFrame, file_key: str, file_ext: str, tenant: str, ingested_at: str, config: Config) -> DataFrame:
    return (
        df.withColumn("_source_bucket", F.lit(config.source_bucket))
        .withColumn("_source_key", F.lit(file_key))
        .withColumn("_source_file_name", F.lit(os.path.basename(file_key)))
        .withColumn("_source_format", F.lit(file_ext.lstrip(".")))
        .withColumn("_tenant", F.lit(tenant.lower()))
        .withColumn("_layer", F.lit(config.layer.lower()))
        .withColumn("_stage", F.lit(config.domain.lower()))
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_ingest_date", F.lit(ingested_at[:10]).cast("date"))
    )


def detect_csv_delimiter(s3_client, key: str, config: Config) -> str:
    logger.debug("Detecting CSV delimiter from S3 file: %s", key)
    try:
        logger.debug("Reading sample from S3 for delimiter detection (first 8KB)...")
        sample = s3_client.get_object(Bucket=config.source_bucket, Key=key)["Body"].read(8192)
        text = sample.decode("utf-8-sig", errors="ignore")
        logger.debug("Sample read: %d bytes", len(sample))

        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            logger.warning("CSV file appears to be empty or has no valid lines, using default delimiter ','")
            return ","

        header = lines[0]
        logger.debug("Header line: %s...", header[:100])

        best_delimiter = ","
        best_score = -1
        delimiter_scores = {}
        for delimiter in config.CSV_DELIMITERS:
            score = header.count(delimiter)
            delimiter_scores[repr(delimiter)] = score
            if score > best_score:
                best_delimiter, best_score = delimiter, score

        logger.debug("CSV delimiter detection scores: %s, selected: %r", delimiter_scores, best_delimiter)
        return best_delimiter
    except Exception as e:
        logger.error("Error detecting CSV delimiter: %s", str(e), exc_info=True)
        raise


def read_csv(spark: SparkSession, s3_client, key: str, config: Config) -> tuple[DataFrame, str, str | None]:
    """Read CSV by downloading to local temp file for large files (up to 10GB+).

    Returns: (DataFrame, delimiter, temp_file_path)
    - temp_file_path: path to temp file (if downloaded) for cleanup after processing, None if local file
    """
    import tempfile
    import shutil

    logger.info("CSV READ STARTING")
    logger.info("  S3 Path: s3://%s/%s", config.source_bucket, key)

    local_path = None
    temp_file_path = None
    try:
        # Support local file paths for testing (starts with /)
        if key.startswith("/"):
            local_path = key
            logger.info("Using local file path: %s", local_path)
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Local file not found: {local_path}")
            # Detect delimiter from local file
            with open(local_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                header = f.readline()
            delimiter = ","
            for delim in config.CSV_DELIMITERS:
                if header.count(delim) > 5:
                    delimiter = delim
                    break
            logger.info("CSV DELIMITER DETECTED: %r (from local file)", delimiter)
        else:
            # Download S3 file to local temp directory (handles large files efficiently up to 5GB+)
            temp_dir = tempfile.gettempdir()
            local_path = os.path.join(temp_dir, os.path.basename(key))
            temp_file_path = local_path  # Track for cleanup later

            logger.info("Downloading S3 file to local temp: %s", local_path)
            logger.debug("S3 download starting...")
            s3_client.download_file(config.source_bucket, key, local_path)
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            logger.info("S3 file downloaded: %.1f MB", file_size_mb)

            # Detect delimiter from downloaded file
            delimiter = detect_csv_delimiter(s3_client, key, config)
            logger.info("CSV DELIMITER DETECTED: %r", delimiter)

        # Read with Spark native CSV reader on local file
        logger.info("Reading CSV with Spark native reader (local path: %s, delimiter: %r)", local_path, delimiter)
        df = spark.read.option("header", True).option("inferSchema", False).option("escape", '"').option("sep", delimiter).option("mergeDelimiter", False).csv(local_path)

        logger.info("SUCCESS: CSV file read with Spark native reader")
        return df, delimiter, temp_file_path

    except Exception as e:
        logger.error("Failed to read CSV: %s", str(e), exc_info=True)
        raise


def read_parquet(spark: SparkSession, s3_client, key: str, config: Config) -> tuple[DataFrame, str | None]:
    """Read Parquet by downloading to local temp file.

    Returns: (DataFrame, temp_file_path)
    - temp_file_path: path to temp file for cleanup after processing
    """
    import tempfile

    logger.info("PARQUET READ STARTING")
    logger.info("  S3 Path: s3://%s/%s", config.source_bucket, key)

    try:
        # Support local file paths for testing (starts with /)
        if key.startswith("/"):
            local_path = key
            logger.info("Using local file path: %s", local_path)
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Local file not found: {local_path}")
            temp_file_path = None
        else:
            # Download S3 file to local temp directory
            temp_dir = tempfile.gettempdir()
            local_path = os.path.join(temp_dir, os.path.basename(key))
            temp_file_path = local_path

            logger.info("Downloading S3 parquet file to local temp: %s", local_path)
            s3_client.download_file(config.source_bucket, key, local_path)
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            logger.info("S3 parquet file downloaded: %.1f MB", file_size_mb)

        # Read with Spark native parquet reader on local file
        logger.info("Reading Parquet with Spark native reader (local path: %s)", local_path)
        df = spark.read.parquet(local_path)

        logger.info("SUCCESS: Parquet file read with Spark native reader")
        return df, temp_file_path

    except Exception as e:
        logger.error("Failed to read parquet file: %s", str(e), exc_info=True)
        raise


def discover_tenant_names(s3_client, config: Config) -> List[str]:
    """Discover all tenant directories under tenant/ prefix."""
    tenant_names = set()
    prefix = f"{config.source_prefix}/"

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            # Extract tenant name from prefix like "tenant/airtel/"
            parts = common_prefix["Prefix"].split("/")
            if len(parts) >= 2 and parts[0] == config.source_prefix and parts[1]:
                tenant_names.add(parts[1])

    return sorted(list(tenant_names))


def list_traffic_files_for_tenant(s3_client, tenant_name: str, config: Config) -> Generator[tuple[str, str], None, None]:
    """List all BRONZE TRAFFIC files for a specific tenant (case-insensitive S3 paths).

    Only yields files from tenant/{tenant_name}/Bronze/Traffic/ directory.
    Excludes:
    - Other layers: Silver/Traffic, Gold/*
    - Other domains: Bronze/Settlement, Bronze/Agreement, etc.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    actual_tenant_dir = None

    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=f"{config.source_prefix}/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            dir_name = prefix["Prefix"].split("/")[-2]  # Extract tenant name from path
            if dir_name.lower() == tenant_name.lower():
                actual_tenant_dir = dir_name
                break
        if actual_tenant_dir:
            break

    if not actual_tenant_dir:
        logger.warning("Tenant directory not found for: %s", tenant_name)
        return

    # Find the actual Bronze directory (case-insensitive)
    bronze_dir = None
    bronze_prefix = f"{config.source_prefix}/{actual_tenant_dir}/"
    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=bronze_prefix, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            dir_name = prefix["Prefix"].split("/")[-2]
            if dir_name.lower() == "bronze":
                bronze_dir = dir_name
                break
        if bronze_dir:
            break

    if not bronze_dir:
        logger.warning("Bronze directory not found for tenant: %s", actual_tenant_dir)
        return

    # Find the actual Traffic directory within Bronze (case-insensitive)
    traffic_dir = None
    traffic_search_prefix = f"{config.source_prefix}/{actual_tenant_dir}/{bronze_dir}/"
    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=traffic_search_prefix, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            dir_name = prefix["Prefix"].split("/")[-2]
            if dir_name.lower() == "traffic":
                traffic_dir = dir_name
                break
        if traffic_dir:
            break

    if not traffic_dir:
        logger.warning("Traffic directory not found in Bronze layer for tenant: %s", actual_tenant_dir)
        return

    # Explicitly use Bronze layer Traffic (not Silver, Gold, or other domains)
    source_prefix = f"{config.source_prefix}/{actual_tenant_dir}/{bronze_dir}/{traffic_dir}"
    logger.info("Searching for Bronze/Traffic files in: s3://%s/%s/", config.source_bucket, source_prefix)

    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=source_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            # Validate BOTH layer AND domain (case-insensitive)
            normalized_key = key.replace("\\", "/").lower()
            if not "/bronze/traffic/" in normalized_key:
                logger.warning("Skipping non-Bronze-Traffic file: %s", key)
                continue
            # Reject other domains in Bronze layer (Settlement, Agreement, etc.)
            if any(domain in normalized_key for domain in ["/settlement/", "/agreement/", "/rating/", "/activation/"]):
                logger.warning("Skipping Bronze non-Traffic domain file: %s", key)
                continue
            # Skip processed/failed subdirectories, but allow original/ (case-insensitive)
            path_parts = key.split("/")
            if any(part.lower() in {"processed", "failed"} for part in path_parts):
                continue
            if key.lower().endswith(config.SUPPORTED_EXTENSIONS):
                logger.info("Found file to process: %s", key)
                yield (key, tenant_name)


def archive_file(key: str, status: str, config: Config, ingest_date: str = None) -> str:
    """Move file to processed/failed/invalid subdirectory with optional date.

    For processed and failed: tenant/{tenant}/Bronze/Traffic/{status}/{date}/{filename}
    For invalid: tenant/{tenant}/Bronze/Traffic/{status}/{date}/{filename}

    Args:
        key: Original file key (e.g., tenant/EE/Bronze/Traffic/file.csv)
        status: Directory status (processed, failed, invalid)
        config: Configuration object
        ingest_date: Optional date for subdirectory (YYYY-MM-DD format)
    """
    filename = os.path.basename(key)

    # Extract directory path only (up to and including Traffic/), removing any existing status dirs
    parts = key.split("/")
    base_parts = []
    for part in parts:
        if part.lower() in {"processed", "failed", "invalid"}:
            # Stop collecting when we hit a status directory
            break
        # Skip the filename (last part)
        if part != filename:
            base_parts.append(part)

    # Ensure we have at least tenant/tenant_name/Bronze/Traffic structure
    if len(base_parts) < 4:
        base_parts = parts[:4]

    logger.debug("Archive path construction: original_key=%s, base_parts=%s, status=%s, filename=%s",
                 key, base_parts, status, filename)

    # Reconstruct path: tenant/EE/Bronze/Traffic/status/date/filename
    if ingest_date:
        new_path = "/".join(base_parts + [status, ingest_date, filename])
    else:
        new_path = "/".join(base_parts + [status, filename])

    logger.info("Archive file mapping: %s -> %s", key, new_path)
    return new_path


def move_file(s3_client, source_key: str, dest_key: str, bucket: str) -> None:
    if source_key == dest_key:
        logger.debug("Source and destination are same, skipping move: %s", source_key)
        return

    logger.info("Moving file in S3...")
    logger.info("  Source: s3://%s/%s", bucket, source_key)
    logger.info("  Destination: s3://%s/%s", bucket, dest_key)

    try:
        logger.debug("Copying object from source to destination")
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": source_key},
            Key=dest_key,
        )
        logger.debug("Copy successful, deleting source")
        s3_client.delete_object(Bucket=bucket, Key=source_key)
        logger.info("File move completed successfully")
    except Exception as e:
        logger.error("Failed to move file: %s", str(e), exc_info=True)
        raise


def filter_empty_rows(df: DataFrame) -> DataFrame:
    """Filter out empty rows (all columns null or empty)."""
    non_null_count = sum([F.when(F.col(col).isNotNull() & (F.col(col) != ""), 1).otherwise(0) for col in df.columns])
    return df.filter(non_null_count > 0)


def split_valid_invalid_rows(df: DataFrame, config: Config) -> tuple[DataFrame, DataFrame]:
    """Split rows into valid and invalid based on schema field types.

    Returns:
        (valid_df, invalid_df) - DataFrames with valid rows and rows with conversion failures
    """
    spark = df.sparkSession
    target_schema = spark.table(config.iceberg_table).schema

    logger.info("=" * 70)
    logger.info("STARTING ROW VALIDATION AGAINST ICEBERG TABLE SCHEMA")
    logger.info("  Target table: %s", config.iceberg_table)
    logger.info("=" * 70)

    invalid_conditions = []
    date_fields = []
    decimal_fields = []
    validated_fields = []

    for field in target_schema.fields:
        if field.name not in df.columns or field.name.startswith("_"):
            continue

        field_type = str(field.dataType).lower()
        validated_fields.append(field.name)

        if "date" in field_type and "timestamp" not in field_type:
            date_fields.append(field.name)
            logger.debug("Date field validation: %s (type: %s)", field.name, field.dataType)
            invalid_conditions.append(
                (F.col(field.name).isNotNull() &
                 (F.col(field.name) != "") &
                 ~F.col(field.name).rlike(r"^\d{4}-\d{2}-\d{2}$"))
            )
        elif "decimal" in field_type:
            decimal_fields.append(field.name)
            logger.debug("Decimal field validation: %s (type: %s)", field.name, field.dataType)
            invalid_conditions.append(
                (F.col(field.name).isNotNull() &
                 (F.col(field.name) != "") &
                 ~F.col(field.name).rlike(r"^-?\d+(\.\d+)?$"))
            )

    logger.info("VALIDATION RULES CONFIGURED:")
    logger.info("  Date fields validated (%d): %s", len(date_fields), date_fields)
    logger.info("  Decimal fields validated (%d): %s", len(decimal_fields), decimal_fields)
    logger.info("  Total validated fields: %d", len(validated_fields))

    if not invalid_conditions:
        logger.info("No validation rules configured - all rows will be considered valid")
        return df, spark.createDataFrame([], schema=df.schema)

    combined_condition = invalid_conditions[0]
    for condition in invalid_conditions[1:]:
        combined_condition = combined_condition | condition

    logger.info("Filtering rows based on %d validation conditions", len(invalid_conditions))
    invalid_df = df.filter(combined_condition)
    valid_df = df.filter(~combined_condition)

    invalid_count = invalid_df.count()
    valid_count = valid_df.count()
    total_count = valid_count + invalid_count

    logger.info("=" * 70)
    logger.info("ROW VALIDATION RESULTS:")
    logger.info("  Valid rows: %d (%.1f%%)", valid_count, (valid_count / total_count * 100) if total_count > 0 else 0)
    logger.info("  Invalid rows: %d (%.1f%%)", invalid_count, (invalid_count / total_count * 100) if total_count > 0 else 0)
    logger.info("=" * 70)

    if invalid_count > 0:
        logger.warning("Row validation found %d invalid rows (date fields: %s, decimal fields: %s)",
                      invalid_count, date_fields, decimal_fields)
        try:
            sample_cols = [f for f in date_fields + decimal_fields if f in df.columns] or df.columns[:5]
            sample_invalid = invalid_df.select(*sample_cols).limit(3)
            logger.warning("Sample invalid rows (showing first 3):\n%s", sample_invalid.toPandas().to_string())
        except Exception as e:
            logger.warning("Could not log sample invalid rows: %s", e)
    else:
        logger.info("Row validation: All %d rows are valid", valid_count)

    return valid_df, invalid_df


def write_invalid_rows(spark: SparkSession, invalid_df: DataFrame, file_key: str, config: Config, ingested_at: str) -> None:
    """Archive invalid rows to S3 in parquet format with date subdirectory."""
    invalid_count = invalid_df.count()
    if invalid_count == 0:
        logger.info("No invalid rows to archive")
        return

    logger.info("=" * 70)
    logger.info("ARCHIVING INVALID ROWS TO S3")
    logger.info("  Count: %d rows", invalid_count)
    logger.info("=" * 70)

    # Extract date from ingested_at (format: YYYY-MM-DDTHH:MM:SS)
    ingest_date = ingested_at.split('T')[0] if 'T' in ingested_at else ingested_at[:10]
    invalid_archive_key = archive_file(file_key, "invalid", config, ingest_date)
    s3_path = f"s3a://{config.source_bucket}/{invalid_archive_key}"

    logger.info("Writing invalid rows to: s3://%s/%s", config.source_bucket, invalid_archive_key)
    try:
        invalid_df.write.mode("overwrite").parquet(s3_path)
        logger.info("Successfully archived %d invalid rows", invalid_count)
    except Exception as e:
        logger.error("Failed to write invalid rows to S3: %s", str(e), exc_info=True)
        raise


def process_file(spark: SparkSession, s3_client, key: str, tenant: str, ingested_at: str, config: Config) -> tuple[DataFrame, str | None]:
    """Process file and return (DataFrame, temp_file_path).

    Returns:
    - DataFrame: processed data
    - temp_file_path: path to temp CSV file (if downloaded) for cleanup after writing, None if parquet/local
    """
    file_ext = os.path.splitext(key)[1].lower()
    logger.info("=" * 70)
    logger.info("STARTING FILE INGESTION: %s", key)
    logger.info("  File Extension: %s", file_ext)
    logger.info("  Tenant: %s", tenant)
    logger.info("  S3 Path: s3://%s/%s", config.source_bucket, key)
    logger.info("=" * 70)

    temp_file_path = None
    try:
        if file_ext == ".parquet":
            logger.info("Reading PARQUET file using Spark native reader")
            df, temp_file_path = read_parquet(spark, s3_client, key, config)
        else:
            logger.info("Reading CSV file with delimiter detection")
            df, delimiter, temp_file_path = read_csv(spark, s3_client, key, config)
            logger.info("CSV DELIMITER DETECTED: %r", delimiter)

        logger.debug("Counting rows from source (this materializes data)...")
        initial_row_count = df.count()
        logger.info("Initial row count from source: %d rows", initial_row_count)
        logger.debug("Initial column names: %s", df.columns)

        # Cache early and often to prevent S3 connection loss
        logger.debug("Caching source DataFrame...")
        df.cache()
        logger.debug("Source DataFrame cached")

        # Filter empty rows
        logger.info("Filtering empty rows...")
        df = filter_empty_rows(df)
        after_filter_count = df.count()
        removed_count = initial_row_count - after_filter_count
        if removed_count > 0:
            logger.warning("Removed %d empty rows (%.1f%%)", removed_count, (removed_count / initial_row_count * 100) if initial_row_count > 0 else 0)
        else:
            logger.info("No empty rows found")

        # Normalize columns
        logger.info("NORMALIZING COLUMN NAMES...")
        df = normalize_columns(df)
        logger.info("Normalized columns: %s", df.columns)

        # Remap source columns
        logger.info("REMAPPING SOURCE COLUMNS to target schema...")
        df = remap_source_columns(df)
        logger.info("Remapped columns: %s", df.columns)

        # Cast to string
        logger.info("CASTING ALL COLUMNS TO STRING for type standardization...")
        df = cast_all_columns_to_string(df)

        # Normalize dates
        logger.info("NORMALIZING DATE FORMATS in call_date column...")
        df = normalize_date_formats(df)

        # Add audit columns
        logger.info("ADDING AUDIT COLUMNS...")
        df = add_audit_columns(df, key, file_ext, tenant, ingested_at, config)
        final_columns = df.columns
        logger.info("Final schema with audit columns (%d columns): %s", len(final_columns), final_columns)

        # Count rows without caching to avoid OOM on large files
        processed_row_count = df.count()

        logger.info("=" * 70)
        logger.info("FILE INGESTION COMPLETED SUCCESSFULLY")
        logger.info("  Total rows after processing: %d", processed_row_count)
        logger.info("  Total columns: %d", len(final_columns))
        logger.info("=" * 70)

        return df, temp_file_path
    except Exception as e:
        logger.error("ERROR DURING FILE INGESTION: %s", str(e), exc_info=True)
        raise


def align_to_target_schema(df: DataFrame, config: Config) -> DataFrame:
    spark = df.sparkSession
    target_schema = spark.table(config.iceberg_table).schema

    source_columns = set(df.columns)
    target_columns = [field.name for field in target_schema.fields]

    missing_columns = [field.name for field in target_schema.fields if field.name not in source_columns]
    extra_columns = [column for column in df.columns if column not in target_columns]

    if missing_columns:
        logger.warning("Missing source columns for %s: %s", config.iceberg_table, ", ".join(missing_columns))
    if extra_columns:
        logger.info("Ignoring extra source columns for %s: %s", config.iceberg_table, ", ".join(extra_columns))

    return df.select([
        (
            F.col(field.name).cast(field.dataType).alias(field.name)
            if field.name in source_columns
            else F.lit(None).cast(field.dataType).alias(field.name)
        )
        for field in target_schema.fields
    ])


def write_dataframe(df: DataFrame, config: Config) -> None:
    spark = df.sparkSession
    spark.conf.set("spark.sql.iceberg.merge-schema", str(config.merge_schema).lower())

    logger.info("=" * 70)
    logger.info("WRITING DATA TO ICEBERG TABLE")
    logger.info("  Table: %s", config.iceberg_table)
    logger.info("  Merge schema enabled: %s", config.merge_schema)
    logger.info("=" * 70)

    if not spark.catalog.tableExists(config.iceberg_table):
        raise RuntimeError(
            f"Table {config.iceberg_table} does not exist. "
            "It should have been created by iceberg-alembic migration. "
            "Run: iceberg-migrate upgrade head"
        )

    logger.info("Table existence check PASSED")

    logger.info("Aligning DataFrame to target schema...")
    aligned_df = align_to_target_schema(df, config)
    row_count_before = df.count()
    row_count_after = aligned_df.count()
    logger.info("Schema alignment: %d rows -> %d rows", row_count_before, row_count_after)

    logger.info("Writing %d rows to Iceberg table using APPEND mode", row_count_after)
    aligned_df.writeTo(config.iceberg_table).append()

    logger.info("=" * 70)
    logger.info("ICEBERG WRITE COMPLETED SUCCESSFULLY")
    logger.info("  Rows written: %d", row_count_after)
    logger.info("=" * 70)


def main() -> None:
    is_valid, error_msg = validate_domain_and_layer("traffic", "bronze")
    if not is_valid:
        logger.error(error_msg)
        raise ValueError(error_msg)

    config = Config()

    s3 = boto3.client(
        "s3",
        endpoint_url=config.storage_endpoint,
        aws_access_key_id=config.aws_access_key,
        aws_secret_access_key=config.aws_secret_key,
        config=BotoConfig(
            retries={"max_attempts": 3}
        ),
    )

    spark = (
        SparkSession.builder.appName("load-bronze-traffic-to-iceberg")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    # Register tenant-specific Iceberg catalog
    register_iceberg_catalog(spark)

    try:
        logger.info("=" * 70)
        logger.info("Bronze Traffic Ingestion -> %s", config.iceberg_table)
        logger.info("=" * 70)
        logger.info("Tenant:        %s", config.tenant or "all")
        logger.info("Namespace:     %s", config.iceberg_namespace)
        logger.info("Table:         %s", config.iceberg_table_name)
        logger.info("Layer:         %s", config.layer)
        logger.info("Domain:        %s", config.domain)
        logger.info("Location:      %s", config.table_location)
        logger.info("Source Path:   s3://%s/%s/*/Bronze/Traffic/", config.source_bucket, config.source_prefix)
        logger.info("=" * 70)

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{config.iceberg_namespace}`")

        # If specific tenant is provided, only process that tenant
        if config.tenant:
            tenant_names = [config.tenant]
            logger.info("Processing single tenant: %s", config.tenant)
        else:
            # Discover all tenant names
            tenant_names = discover_tenant_names(s3, config)
            if not tenant_names:
                logger.warning("No tenant directories found under s3://%s/%s/", config.source_bucket, config.source_prefix)
                return
            logger.info("Discovered tenants: %s", ", ".join(tenant_names))

        # Collect all files from specified tenant(s)
        all_files = []
        if config.file_key:
            # Process specific file passed by DAG
            all_files.append((config.file_key, config.tenant))
            logger.info("Processing file from DAG: %s", config.file_key)
        else:
            # Discover files if no specific file provided
            for tenant_name in tenant_names:
                for file_key, tenant in list_traffic_files_for_tenant(s3, tenant_name, config):
                    all_files.append((file_key, tenant))

        if not all_files:
            logger.warning("No traffic files found in any tenant directories")
            return

        logger.info("Processing %d file(s) from %d tenant(s)", len(all_files), len(tenant_names))
        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        for file_key, tenant in all_files:
            temp_file_path = None
            ingest_date = ingested_at.split('T')[0] if 'T' in ingested_at else ingested_at[:10]
            try:
                print(f"DEBUG: Processing file {file_key} for tenant {tenant}")
                logger.info("Processing file: %s", file_key)

                # Step 1: Create backup copy to original/ directory BEFORE processing
                original_key = archive_file(file_key, "original", config, ingest_date)
                s3.copy_object(
                    CopySource={'Bucket': config.source_bucket, 'Key': file_key},
                    Bucket=config.source_bucket,
                    Key=original_key
                )
                logger.info("Backed up original file: %s", original_key)
                print(f"DEBUG: Original file backed up to {original_key}")

                # Step 2: Process from the source location (tenant/EE/Bronze/Traffic/)
                df, temp_file_path = process_file(spark, s3, file_key, tenant, ingested_at, config)
                row_count = df.count()
                print(f"DEBUG: File {file_key} has {row_count} rows after processing")
                logger.info("Schema for %s (%s rows):", file_key, row_count)
                df.printSchema()

                valid_df, invalid_df = split_valid_invalid_rows(df, config)
                invalid_count = invalid_df.count()
                valid_count = valid_df.count()
                print(f"DEBUG: File {file_key} - Valid: {valid_count}, Invalid: {invalid_count}")

                if invalid_count > 0:
                    write_invalid_rows(spark, invalid_df, file_key, config, ingested_at)

                if valid_count > 0:
                    write_dataframe(valid_df, config)
                    logger.info("Wrote %s valid rows to %s", valid_count, config.iceberg_table)
                    print(f"DEBUG: Successfully wrote {valid_count} rows to {config.iceberg_table}")
                else:
                    logger.warning("No valid rows to write after filtering %d invalid rows", invalid_count)
                    print(f"DEBUG: No valid rows to write for {file_key}")

                # Step 3: Move from source to processed/ AFTER successful write to Iceberg
                processed_key = archive_file(file_key, "processed", config, ingest_date)
                move_file(s3, file_key, processed_key, config.source_bucket)
                logger.info("Moved to processed directory: %s", processed_key)
                print(f"DEBUG: File moved to processed: {processed_key}")

                # Clean up temp file AFTER successful write to Iceberg
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.remove(temp_file_path)
                        logger.info("Cleaned up temp file: %s", temp_file_path)
                    except Exception as cleanup_error:
                        logger.warning("Could not cleanup temp file %s: %s", temp_file_path, str(cleanup_error))

            except Exception as exc:
                print(f"DEBUG: ERROR processing {file_key}: {exc}")
                logger.error("Failed to process %s: %s", file_key, exc, exc_info=True)
                # Clean up temp file even on error
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.remove(temp_file_path)
                        logger.info("Cleaned up temp file after error: %s", temp_file_path)
                    except Exception as cleanup_error:
                        logger.warning("Could not cleanup temp file %s: %s", temp_file_path, str(cleanup_error))
                try:
                    # Step 4: Move from source to failed/ on error
                    failed_key = archive_file(file_key, "failed", config, ingest_date)
                    move_file(s3, file_key, failed_key, config.source_bucket)
                    logger.info("Moved to failed/: %s", failed_key)
                except Exception as move_exc:
                    logger.error("Could not archive failed file: %s", move_exc)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
