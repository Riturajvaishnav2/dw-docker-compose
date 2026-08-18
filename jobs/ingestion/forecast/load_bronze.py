"""
Load Bronze Forecast Data from S3 bucket or local files.

Discovers and ingests forecast CSV files into bronze.iot_forecast_raw table.

LAYER VALIDATION: Only picks files from bronze/forecast/ directory.
Avoids loading files from Silver/Forecast/ or any other layers.

ARCHITECTURE:
  - Bronze: Stores raw values (no IDs) - CLIENT_NAME, PARTNER_NAME, SERVICE_TYPE, etc.
  - Silver: Creates dimensions and fact table with ID references
  - Gold: Creates aggregations

USAGE:
    # From S3 bucket (landing_test/tenant/ee/bronze/forecast/)
    docker compose exec spark /opt/airflow/jobs/common/run_spark_submit.sh \
        /opt/airflow/jobs/ingestion/forecast/load_bronze.py

    # From local file
    python load_bronze.py <csv_path>

ENVIRONMENT VARIABLES:
    TENANT: Tenant namespace (default: 'ee')
    ICEBERG_NAMESPACE: Iceberg catalog namespace
    DATA_DATE: Data load date (default: today)
    OCI_S3_ENDPOINT: S3 endpoint URL (optional, for S3 reads)
    OCI_ACCESS_KEY_ID: S3 access key (optional)
    OCI_SECRET_ACCESS_KEY: S3 secret key (optional)
"""

import logging
import os
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, "/opt/airflow")
from jobs.common.domain_to_table_mapping import DomainTableMapping
from jobs.common.spark_catalog import register_iceberg_catalog

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def env_or_default(name: str, default: str) -> str:
    """Get environment variable with fallback to default."""
    return os.getenv(name) or default


@dataclass
class Config:
    """Configuration for forecast bronze load - matches traffic_ingest pattern."""
    # Core configuration
    tenant: str = field(default_factory=lambda: env_or_default("ICEBERG_NAMESPACE", "ee").lower())
    domain: str = field(default_factory=lambda: "forecast")
    layer: str = field(default_factory=lambda: "bronze")

    # Data source
    storage_endpoint: str = field(default_factory=lambda: env_or_default("OCI_S3_ENDPOINT", ""))
    landing_bucket: str = field(default_factory=lambda: env_or_default("LANDING_BUCKET", "landing_test"))
    source_prefix: str = field(default_factory=lambda: env_or_default("TENANT_SOURCE_PREFIX", "tenant"))

    # FILE_KEY from environment (set by DAG) - matches traffic_ingest pattern
    file_key: str | None = field(default_factory=lambda: os.getenv("FILE_KEY"))

    # S3 credentials
    aws_access_key: str | None = field(default_factory=lambda: env_or_default("OCI_ACCESS_KEY_ID", "") or None)
    aws_secret_key: str | None = field(default_factory=lambda: env_or_default("OCI_SECRET_ACCESS_KEY", "") or None)

    # Load options
    data_date: str = field(default_factory=lambda: env_or_default("DATA_DATE", ""))
    merge_schema: bool = field(default_factory=lambda: env_or_default("MERGE_SCHEMA", "false").lower() == "true")

    SUPPORTED_EXTENSIONS = (".csv", ".parquet")
    CSV_DELIMITERS = (",", "~", ";", "|", "\t")

    def __post_init__(self) -> None:
        # Normalize and set tenant in environment
        self.tenant = self.tenant.lower()
        os.environ["ICEBERG_NAMESPACE"] = self.tenant

        # Get table configuration from mapping
        mapping = DomainTableMapping()
        table_config = mapping.get_table_config(self.domain, self.layer)

        if not table_config:
            raise ValueError(
                f"Domain '{self.domain}' with layer '{self.layer}' is not configured. "
                f"Check DomainTableMapping for available combinations."
            )

        self.iceberg_namespace = table_config.iceberg_namespace
        self.iceberg_table_name = table_config.table_name
        self.iceberg_table = table_config.full_path
        self.table_location = table_config.warehouse_location


def validate_csv_file(csv_path: str) -> tuple[int, str]:
    """
    Validate CSV file exists and is readable.

    Returns:
        Tuple of (file_size, file_path)
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    if not os.access(csv_path, os.R_OK):
        raise PermissionError(f"CSV file not readable: {csv_path}")

    file_size = os.path.getsize(csv_path)
    if file_size == 0:
        raise ValueError(f"CSV file is empty: {csv_path}")

    logger.info(f"CSV file validated: {csv_path} ({file_size} bytes)")
    return file_size, csv_path


def load_forecast_bronze_from_file(spark: SparkSession, config: Config, s3_file_key: str) -> int:
    """
    Load forecast CSV file into bronze table.
    Downloads file from S3 to local temp, then reads (matches traffic_ingest pattern).

    Args:
        spark: SparkSession
        config: Configuration
        s3_file_key: S3 file key (e.g., tenant/EE/Bronze/Forecast/file.csv)

    Returns:
        Number of records loaded
    """
    logger.info(f"{'='*70}")
    logger.info(f"FORECAST BRONZE LOAD - From File")
    logger.info(f"{'='*70}")
    logger.info(f"S3 File Key: {s3_file_key}")
    logger.info(f"Target Table: {config.iceberg_table}")
    logger.info(f"Tenant: {config.tenant}")

    # Initialize S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=config.storage_endpoint,
        aws_access_key_id=config.aws_access_key,
        aws_secret_access_key=config.aws_secret_key,
        region_name="uk-london-1"
    )

    # Download file to local temp (matches traffic_ingest pattern)
    temp_dir = tempfile.gettempdir()
    local_path = os.path.join(temp_dir, os.path.basename(s3_file_key))

    logger.info(f"Downloading S3 file to local temp: {local_path}")
    s3.download_file(config.landing_bucket, s3_file_key, local_path)
    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    logger.info(f"S3 file downloaded: {file_size_mb:.1f} MB")

    try:
        # Read CSV from local file
        logger.info(f"Reading CSV file from local temp...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("nullValue", "") \
            .option("mode", "FAILFAST") \
            .csv(local_path)

        record_count = df.count()
        logger.info(f"Read {record_count} records from CSV")

        # Process and write, pass S3 client and file key for post-load archival
        return _process_and_write_bronze(spark, config, df, record_count, s3, s3_file_key)
    finally:
        # Cleanup temp file
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.info(f"Cleaned up temp file: {local_path}")


def archive_file(key: str, status: str, config: Config, ingest_date: str = "") -> str:
    """Move file to processed/failed/invalid subdirectory with optional date.

    For processed and failed: tenant/{tenant}/Bronze/Forecast/{status}/{date}/{filename}

    Args:
        key: Original file key (e.g., tenant/EE/Bronze/Forecast/file.csv)
        status: Directory status (processed, failed, invalid)
        config: Configuration object
        ingest_date: Optional date for subdirectory (YYYY-MM-DD format)
    """
    filename = os.path.basename(key)

    # Extract directory path only (up to and including Forecast/), removing any existing status dirs
    parts = key.split("/")
    base_parts = []
    for part in parts:
        if part.lower() in {"processed", "failed", "invalid"}:
            # Stop collecting when we hit a status directory
            break
        # Skip the filename (last part)
        if part != filename:
            base_parts.append(part)

    # Ensure we have at least tenant/tenant_name/Bronze/Forecast structure
    if len(base_parts) < 4:
        base_parts = parts[:4]

    logger.debug("Archive path construction: original_key=%s, base_parts=%s, status=%s, filename=%s",
                 key, base_parts, status, filename)

    # Reconstruct path: tenant/EE/Bronze/Forecast/status/date/filename
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


def _process_and_write_bronze(spark: SparkSession, config: Config, df, record_count: int, s3_client=None, file_key: str = "") -> int:
    """
    Process DataFrame and write to bronze table.

    Adds audit columns and normalizes schema.
    """
    # Add audit columns
    load_ts = datetime.now(timezone.utc)
    load_date = load_ts.date()

    logger.info("Adding audit columns...")
    df = df \
        .withColumn("_load_ts", F.lit(load_ts).cast(T.TimestampType())) \
        .withColumn("_load_date", F.lit(load_date).cast(T.DateType()))

    # Standardize column names to lowercase
    logger.info("Normalizing column names...")
    for col in df.columns:
        if col not in ["_load_ts", "_load_date"]:
            new_col = col.lower()
            if col != new_col:
                df = df.withColumnRenamed(col, new_col)

    logger.info("Casting columns to match CSV and bronze table types...")
    csv_column_casts = {
        "client_name": T.StringType(),
        "client_group_name": T.StringType(),
        "partner_name": T.StringType(),
        "partner_group_name": T.StringType(),
        "service_type": T.StringType(),
        "event_type": T.StringType(),
        "traffic_direction": T.StringType(),
        "period_start_date": T.TimestampType(),
        "cr_period_start_date": T.TimestampType(),
        "destination_type": T.StringType(),
        "agreement_reference": T.StringType(),
        "rti_group": T.StringType(),
        "forecast_or_actual_ind": T.StringType(),
        "traffic_volume": T.DecimalType(20, 2),
        "charged_volume": T.DecimalType(20, 2),
        "tap_charge_sdr_net": T.DecimalType(20, 5),
        "tap_charge_sdr_gross": T.DecimalType(20, 5),
        "disc_charge_sdr_net": T.DecimalType(20, 5),
        "disc_charge_sdr_gross": T.DecimalType(20, 5),
        "no_of_records": T.IntegerType(),
    }

    for col_name, col_type in csv_column_casts.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))

    # Select only the raw CSV columns in the EXACT order of the Iceberg table schema
    # Bronze layer stores raw values, fact table does dimension lookups
    schema_order = [
        "client_name", "client_group_name", "partner_name", "partner_group_name",
        "service_type", "event_type", "traffic_direction",
        "period_start_date", "cr_period_start_date",
        "destination_type", "agreement_reference", "rti_group",
        "traffic_volume", "charged_volume",
        "tap_charge_sdr_net", "tap_charge_sdr_gross",
        "disc_charge_sdr_net", "disc_charge_sdr_gross",
        "no_of_records", "forecast_or_actual_ind",
        "_load_ts", "_load_date"
    ]
    available_cols = [col for col in schema_order if col in df.columns]
    df = df.select(available_cols)

    logger.info(f"Bronze table columns to write ({len(available_cols)} total):")
    for col in available_cols:
        logger.info(f"  - {col}")

    logger.info("Schema:")
    df.printSchema()

    logger.info(f"Writing {record_count} records to {config.iceberg_table}...")
    df.writeTo(config.iceberg_table) \
        .option("mergeSchema", "true") \
        .append()

    logger.info(f"{'='*70}")
    logger.info(f"✓ Successfully loaded {record_count} records")
    logger.info(f"{'='*70}")

    # Move file to processed/ directory after successful write
    if s3_client and file_key:
        try:
            ingest_date = load_date.strftime("%Y-%m-%d")
            processed_key = archive_file(file_key, "processed", config, ingest_date)
            move_file(s3_client, file_key, processed_key, config.landing_bucket)
            logger.info("Moved to processed directory: %s", processed_key)
        except Exception as e:
            logger.warning("Failed to move file to processed: %s", str(e))

    return record_count


def main() -> None:
    """
    Main entry point - matches traffic_ingest pattern.
    FILE_KEY must be set by DAG as environment variable.
    Downloads from S3, then processes locally.
    """
    config = Config()

    if not config.file_key:
        raise ValueError(
            "FILE_KEY environment variable not set. "
            "This script must be called by the DAG with FILE_KEY exported."
        )

    logger.info(f"Loading forecast file: {config.file_key}")

    spark = SparkSession.builder \
        .appName(f"ForecastBronzeLoad-{config.tenant}") \
        .getOrCreate()

    try:
        register_iceberg_catalog(spark, config.tenant)
        load_forecast_bronze_from_file(spark, config, config.file_key)
    except Exception as e:
        logger.error(f"Failed to load forecast CSV: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
