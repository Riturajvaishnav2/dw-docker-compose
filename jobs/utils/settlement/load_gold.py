"""
Load Gold Settlement data from tenant-specific directories.

Discovers all tenant directories and loads files ONLY from tenant/{tenant_name}/Gold/settlement/
into the configured Iceberg table.

LAYER VALIDATION: Only picks files from Gold/settlement/ directory.
Avoids loading files from Bronze/settlement/ or Silver/settlement/ layers.

Usage:
    docker compose exec spark /opt/platform/jobs/common/run_spark_submit.sh \
        /opt/platform/jobs/ingestion/load_gold_settlement.py
"""

import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

sys.path.insert(0, "/opt/airflow")
from jobs.common.domain_to_table_mapping import DomainTableMapping, validate_domain_and_layer
from jobs.common.spark_catalog import register_iceberg_catalog

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def env_or_default(name: str, default: str) -> str:
    return os.getenv(name) or default


def normalize_bucket_name(bucket: str) -> str:
    """
    Normalize bucket name to handle both formats:
    - "landing" → "landing"
    - "s3://landing" → "landing"
    - "s3a://landing" → "landing"
    """
    if not bucket:
        return bucket
    bucket = bucket.strip()
    bucket = bucket.rstrip("/")
    for prefix in ["s3a://", "s3://"]:
        if bucket.startswith(prefix):
            return bucket[len(prefix):].strip()
    return bucket


@dataclass
class Config:
    storage_endpoint: str = field(default_factory=lambda: env_or_default("OCI_S3_ENDPOINT", "http://minio:9000"))
    source_bucket: str = field(default_factory=lambda: normalize_bucket_name(env_or_default("SOURCE_BUCKET", env_or_default("LANDING_BUCKET", "landing"))))
    source_prefix: str = field(default_factory=lambda: "tenant")
    domain: str = field(default_factory=lambda: "settlement")
    layer: str = field(default_factory=lambda: "gold")
    tenant: str = field(default_factory=lambda: env_or_default("TENANT", ""))
    file_key: str | None = field(default_factory=lambda: os.getenv("FILE_KEY"))
    aws_access_key: str | None = field(default_factory=lambda: os.getenv("OCI_ACCESS_KEY_ID"))
    aws_secret_key: str | None = field(default_factory=lambda: os.getenv("OCI_SECRET_ACCESS_KEY"))
    merge_schema: bool = field(default_factory=lambda: env_or_default("MERGE_SCHEMA", "false").lower() == "true")

    SUPPORTED_EXTENSIONS = (".parquet", ".csv")
    CSV_DELIMITERS = ("~", ",", ";", "|", "\t")

    def __post_init__(self) -> None:
        if self.tenant:
            normalized_tenant = self.tenant.lower()
            os.environ["ICEBERG_NAMESPACE"] = normalized_tenant
            self.tenant = normalized_tenant

        if not self.tenant:
            raise ValueError("TENANT environment variable is required for gold settlement ingestion")

        if not self.source_bucket or not self.source_bucket.strip():
            raise ValueError(
                f"source_bucket is not configured properly (value='{self.source_bucket}'). "
                "Set SOURCE_BUCKET or LANDING_BUCKET environment variables."
            )

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


def normalize_column_name(name: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip()).strip("_").lower()
    return cleaned or "col"


def normalize_columns(df: DataFrame) -> DataFrame:
    """Normalize column names to snake_case and log the mapping.

    Maps CSV header names (Title Case with spaces) to target names (snake_case).
    Special handling for settlement_status to ensure it's mapped if present.
    """
    seen: dict[str, int] = {}
    column_map = {}  # Track original → normalized mappings

    for old_name in df.columns:
        new_name = normalize_column_name(old_name)
        if new_name in seen:
            seen[new_name] += 1
            new_name = f"{new_name}_{seen[new_name]}"
        else:
            seen[new_name] = 0

        column_map[old_name] = new_name

        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
            if new_name == "settlement_status":
                logger.info("✓ Mapped Settlement Status column: '%s' → '%s'", old_name, new_name)

    return df


def cast_all_columns_to_string(df: DataFrame) -> DataFrame:
    return df.select([F.col(column).cast(StringType()).alias(column) for column in df.columns])


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


def get_custom_column_mappings(domain: str) -> dict[str, str]:
    """Get custom column name mappings for specific domains.

    Maps source column names (as they appear in CSV) to target column names (in Iceberg).
    Used when source column name doesn't normalize to the expected target name.

    Args:
        domain: Domain name (e.g., 'settlement')

    Returns:
        Dict mapping source column names to target column names
    """
    mappings = {
        "settlement": {
            # Explicit mappings for settlement domain
            # Format: "Source Column Name": "target_column_name"
            "Settlement Status": "settlement_status",
        }
    }
    return mappings.get(domain, {})


def apply_custom_mappings(df: DataFrame, domain: str) -> DataFrame:
    """Apply custom column mappings to handle non-standard source column names.

    Args:
        df: Input DataFrame
        domain: Domain name for selecting appropriate mappings

    Returns:
        DataFrame with custom mappings applied
    """
    mappings = get_custom_column_mappings(domain)
    if not mappings:
        return df

    for source_col, target_col in mappings.items():
        if source_col in df.columns:
            df = df.withColumnRenamed(source_col, target_col)
            logger.info("Applied custom mapping: '%s' → '%s'", source_col, target_col)

    return df


def detect_csv_delimiter(s3_client, key: str, config: Config) -> str:
    sample = s3_client.get_object(Bucket=config.source_bucket, Key=key)["Body"].read(8192)
    text = sample.decode("utf-8-sig", errors="ignore")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return ","
    header = lines[0]
    best_delimiter = ","
    best_score = -1
    for delimiter in config.CSV_DELIMITERS:
        score = header.count(delimiter)
        if score > best_score:
            best_delimiter, best_score = delimiter, score
    return best_delimiter


def read_csv(spark: SparkSession, s3_client, key: str, config: Config) -> tuple[DataFrame, str]:
    # Support reading from local paths for testing (starts with /)
    if key.startswith("/"):
        path = key
        delimiters = [",", ";", "|", "\t", "~"]
        delimiter = ","
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8-sig') as f:
                header = f.readline()
                for delim in delimiters:
                    if header.count(delim) > 5:
                        delimiter = delim
                        break
    else:
        # Download file from S3 to temp local file using boto3
        import tempfile
        temp_dir = tempfile.gettempdir()
        local_file = os.path.join(temp_dir, os.path.basename(key))
        logger.info("Downloading from S3: s3://%s/%s -> %s", config.source_bucket, key, local_file)
        s3_client.download_file(config.source_bucket, key, local_file)
        path = local_file
        delimiter = detect_csv_delimiter(s3_client, key, config)

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("sep", delimiter)
        .option("mergeDelimiter", False)
        .csv(path)
    )
    return df, delimiter


def read_parquet(spark: SparkSession, key: str, config: Config) -> DataFrame:
    path = f"s3a://{config.source_bucket}/{key}"
    return spark.read.parquet(path)


def list_settlement_files_for_tenant(s3_client, tenant_name: str, config: Config) -> Generator[tuple[str, str], None, None]:
    """List all GOLD SETTLEMENT files for a specific tenant (case-insensitive S3 paths).

    Only yields files from tenant/{tenant_name}/Gold/settlement/ directory.
    Excludes:
    - Other layers: Bronze/settlement, Silver/settlement
    - Other domains: Gold/traffic, Gold/agreement, Gold/rating, Gold/activation
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    actual_tenant_dir = None

    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=f"{config.source_prefix}/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            dir_name = prefix["Prefix"].split("/")[-2]
            if dir_name.lower() == tenant_name.lower():
                actual_tenant_dir = dir_name
                break
        if actual_tenant_dir:
            break

    if not actual_tenant_dir:
        logger.warning("Tenant directory not found for: %s", tenant_name)
        return

    # Find the actual Gold directory (case-insensitive)
    gold_dir = None
    gold_prefix = f"{config.source_prefix}/{actual_tenant_dir}/"
    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=gold_prefix, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            dir_name = prefix["Prefix"].split("/")[-2]
            if dir_name.lower() == "gold":
                gold_dir = dir_name
                break
        if gold_dir:
            break

    if not gold_dir:
        logger.warning("Gold directory not found for tenant: %s", actual_tenant_dir)
        return

    # Find the actual settlement directory within Gold (case-insensitive)
    settlement_dir = None
    settlement_search_prefix = f"{config.source_prefix}/{actual_tenant_dir}/{gold_dir}/"
    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=settlement_search_prefix, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            dir_name = prefix["Prefix"].split("/")[-2]
            if dir_name.lower() == "settlement":
                settlement_dir = dir_name
                break
        if settlement_dir:
            break

    if not settlement_dir:
        logger.warning("Settlement directory not found in Gold layer for tenant: %s", actual_tenant_dir)
        return

    # Explicitly use Gold layer Settlement (not Bronze, Silver, or other Gold domains)
    source_prefix = f"{config.source_prefix}/{actual_tenant_dir}/{gold_dir}/{settlement_dir}"
    logger.info("Searching for Gold/settlement files in: s3://%s/%s/", config.source_bucket, source_prefix)

    for page in paginator.paginate(Bucket=config.source_bucket, Prefix=source_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            # Validate BOTH layer AND domain (case-insensitive)
            normalized_key = key.replace("\\", "/").lower()
            if not "/gold/settlement/" in normalized_key:
                logger.warning("Skipping non-Gold-settlement file: %s", key)
                continue
            # Reject other domains in Gold layer (Traffic, Agreement, Rating, Activation, etc.)
            if any(domain in normalized_key for domain in ["/traffic/", "/agreement/", "/rating/", "/activation/"]):
                logger.warning("Skipping Gold non-settlement domain file: %s", key)
                continue
            if any(part.lower() in {"processed", "failed", "invalid"} for part in key.split("/")):
                continue
            if key.lower().endswith(config.SUPPORTED_EXTENSIONS):
                yield (key, tenant_name)


def archive_file(key: str, status: str, config: Config, ingest_date: str = None) -> str:
    """Move file to processed/failed/invalid subdirectory with optional date.

    For processed and failed: {status}/{date}/{filename}
    For invalid: {status}/{date}/{filename}

    Args:
        key: Original file key
        status: Directory status (processed, failed, invalid)
        config: Configuration object
        ingest_date: Optional date for subdirectory (YYYY-MM-DD format)
    """
    parts = key.split("/")
    # Only use the filename, not any parent directories
    filename = os.path.basename(key)
    base_parts = parts[:4]  # tenant/{tenant_name}/{layer}/{domain}

    # Add date subdirectory for processed/failed/invalid if provided
    if ingest_date:
        return "/".join(base_parts + [status, ingest_date, filename])
    else:
        return "/".join(base_parts + [status, filename])


def move_file(s3_client, source_key: str, dest_key: str, bucket: str) -> None:
    if source_key == dest_key:
        return
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )
    s3_client.delete_object(Bucket=bucket, Key=source_key)


def filter_empty_rows(df: DataFrame) -> DataFrame:
    """Filter out empty rows (all columns null or empty)."""
    conditions = [F.when(F.col(col).isNotNull() & (F.col(col) != ""), 1).otherwise(0) for col in df.columns]
    non_null_count = sum(conditions) if conditions else F.lit(0)
    return df.filter(non_null_count > 0)


def split_valid_invalid_rows(df: DataFrame, config: Config) -> tuple[DataFrame, DataFrame]:
    """Split rows into valid and invalid based on home_pmn and partner_pmn requirements.

    Invalid = rows where EITHER home_pmn OR partner_pmn is missing/empty (validation criteria)
    All other data transformations handled in align_to_target_schema.

    Returns:
        (valid_df, invalid_df) - DataFrames with valid rows (both home_pmn and partner_pmn present) and invalid rows
    """
    # Check if required columns exist in source
    has_home_pmn = 'home_pmn' in df.columns
    has_partner_pmn = 'partner_pmn' in df.columns

    if not has_home_pmn and not has_partner_pmn:
        logger.info("Row validation: Neither home_pmn nor partner_pmn in source - all %d rows will be ingested (NULL-filled)", df.count())
        return df, df.sparkSession.createDataFrame([], schema=df.schema)

    if not has_home_pmn or not has_partner_pmn:
        missing_fields = []
        if not has_home_pmn:
            missing_fields.append("home_pmn")
        if not has_partner_pmn:
            missing_fields.append("partner_pmn")
        logger.warning("Row validation: %s not in source - all %d rows will be ingested (NULL-filled)", ", ".join(missing_fields), df.count())
        return df, df.sparkSession.createDataFrame([], schema=df.schema)

    # Both columns exist: split based on their presence
    # Row is invalid if EITHER field is NULL, empty, or whitespace-only
    home_pmn_invalid = (F.col('home_pmn').isNull()) | (F.col('home_pmn') == '') | (F.trim(F.col('home_pmn')) == '')
    partner_pmn_invalid = (F.col('partner_pmn').isNull()) | (F.col('partner_pmn') == '') | (F.trim(F.col('partner_pmn')) == '')

    invalid_condition = home_pmn_invalid | partner_pmn_invalid

    invalid_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    invalid_count = invalid_df.count()
    valid_count = valid_df.count()

    if invalid_count > 0:
        logger.warning("Row validation: %d valid (both home_pmn and partner_pmn present), %d invalid (either field missing)",
                      valid_count, invalid_count)
        sample_cols = [col for col in ['home_pmn', 'partner_pmn'] if col in invalid_df.columns]
        sample_invalid = invalid_df.select(sample_cols).limit(3)
        logger.warning("Sample invalid rows (missing home_pmn or partner_pmn):\n%s", sample_invalid.toPandas().to_string())
    else:
        logger.info("Row validation: All %d rows have valid home_pmn and partner_pmn", valid_count)

    return valid_df, invalid_df


def write_invalid_rows(spark: SparkSession, s3_client, invalid_df: DataFrame, file_key: str, config: Config, ingested_at: str) -> None:
    """Archive invalid rows to S3 as CSV file in invalid/{date}/ subdirectory."""
    if invalid_df.count() == 0:
        return

    if not config.source_bucket or not config.source_bucket.strip():
        logger.warning("Cannot archive invalid rows: source_bucket is not configured (bucket='%s')", config.source_bucket)
        return

    # Extract date from ingested_at (format: YYYY-MM-DDTHH:MM:SS)
    ingest_date = ingested_at.split('T')[0] if 'T' in ingested_at else ingested_at[:10]
    # Archive file to invalid/{date}/ directory with same filename
    invalid_archive_key = archive_file(file_key, "invalid", config, ingest_date)
    invalid_archive_key_csv = f"{os.path.splitext(invalid_archive_key)[0]}.csv"

    logger.info("Writing %d invalid rows to: s3://%s/%s", invalid_df.count(), config.source_bucket, invalid_archive_key_csv)
    try:
        import tempfile

        # Write to temp CSV file first to avoid S3A filesystem issues
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_csv = os.path.join(tmpdir, os.path.basename(invalid_archive_key_csv))

            # Convert to Pandas and write CSV
            pdf = invalid_df.toPandas()
            pdf.to_csv(temp_csv, index=False)

            # Read file into memory and upload to avoid OCI chunked encoding issues
            with open(temp_csv, 'rb') as f:
                csv_bytes = f.read()

            s3_client.put_object(
                Bucket=config.source_bucket,
                Key=invalid_archive_key_csv,
                Body=csv_bytes
            )

            logger.info("Successfully archived %d invalid rows to s3://%s/%s", invalid_df.count(), config.source_bucket, invalid_archive_key_csv)
    except Exception as e:
        logger.error("Failed to archive invalid rows to s3://%s/%s: %s", config.source_bucket, invalid_archive_key_csv, str(e), exc_info=True)
        raise


def process_file(spark: SparkSession, s3_client, key: str, tenant: str, ingested_at: str, config: Config) -> DataFrame:
    file_ext = os.path.splitext(key)[1].lower()
    logger.info("Processing %s file for tenant %s: s3://%s/%s", file_ext, tenant, config.source_bucket, key)

    if file_ext == ".parquet":
        df = read_parquet(spark, key, config)
    else:
        df, delimiter = read_csv(spark, s3_client, key, config)
        logger.info("CSV delimiter: %r", delimiter)

    logger.info("Raw CSV columns (%d): %s", len(df.columns), ", ".join(df.columns))

    # Apply custom column mappings (e.g., "Settlement Status" → "settlement_status")
    df = apply_custom_mappings(df, config.domain)

    df = normalize_columns(df)
    logger.info("Normalized columns (%d): %s", len(df.columns), ", ".join(df.columns))

    df = filter_empty_rows(df)
    df = cast_all_columns_to_string(df)
    return add_audit_columns(df, key, file_ext, tenant, ingested_at, config)


def convert_to_date(col_value: str) -> str:
    """Convert date strings to YYYY-MM-DD format."""
    if not col_value or str(col_value).strip() == "":
        return None
    try:
        from datetime import datetime
        parsed = datetime.fromisoformat(str(col_value).strip())
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return col_value


def convert_to_decimal(col_value: str) -> str:
    """Convert numeric strings to decimal, handling empty/invalid values."""
    if not col_value or str(col_value).strip() == "":
        return None
    try:
        float(str(col_value).strip())
        return str(col_value).strip()
    except ValueError:
        logger.warning("Invalid decimal value: %s", col_value)
        return None


def clean_decimal_value(value):
    """
    Clean decimal values: remove commas and convert exponential notation with precision preservation.
    Handles both large and very small decimal values (e.g., 5.541656101035829E7, 1.02886535219e-7).
    Returns cleaned value or None if empty.
    """
    from decimal import Decimal

    if not value or str(value).strip() == "":
        return None

    value_str = str(value).strip()

    # Handle exponential notation while preserving precision
    try:
        if 'e' in value_str.lower():
            # Use Decimal for arbitrary precision conversion from scientific notation
            decimal_val = Decimal(value_str)
            # Convert to string without scientific notation for small/large values
            # Using string formatting to avoid scientific notation
            value_str = str(decimal_val)
            # If it's still in scientific notation (shouldn't happen with Decimal), convert explicitly
            if 'e' in value_str.lower():
                value_str = format(decimal_val, 'f')
    except (ValueError, TypeError, Exception):
        pass

    # Remove comma separators (thousands separators)
    value_str = value_str.replace(',', '')

    return value_str if value_str else None


def align_to_target_schema(df: DataFrame, config: Config) -> DataFrame:
    spark = df.sparkSession
    target_schema = spark.table(config.iceberg_table).schema

    source_columns = set(df.columns)
    target_columns = [field.name for field in target_schema.fields]

    missing_columns = [field.name for field in target_schema.fields if field.name not in source_columns]
    extra_columns = [column for column in df.columns if column not in target_columns]

    if missing_columns:
        non_audit_missing = [c for c in missing_columns if not c.startswith("_")]
        if non_audit_missing:
            logger.warning("Missing source columns for %s: %s", config.iceberg_table, ", ".join(non_audit_missing))
            # Special handling for settlement_status - log if missing
            if "settlement_status" in missing_columns:
                logger.warning("⚠️  CRITICAL: settlement_status column is missing from source data. "
                             "This column is defined in migration but not in source CSV. "
                             "Column will be NULL-filled in output.")
    if extra_columns:
        logger.info("Ignoring extra source columns for %s: %s", config.iceberg_table, ", ".join(extra_columns))

    logger.info("Column Mapping for %s:", config.iceberg_table)
    logger.info("  Source columns: %d | Target columns: %d", len(source_columns), len(target_columns))

    selected_cols = []
    column_types = {"mapped": [], "null_filled": [], "date": [], "decimal": []}

    # Register UDF for decimal cleaning
    clean_decimal_udf = F.udf(clean_decimal_value)

    for field in target_schema.fields:
        if field.name in source_columns:
            col = F.col(field.name)
            field_type = str(field.dataType)

            if "date" in field_type.lower() and "timestamp" not in field_type.lower():
                col = F.when(F.col(field.name) == "", None).otherwise(
                    F.coalesce(
                        F.to_date(F.col(field.name), "yyyy-MM-dd"),
                        F.to_date(F.col(field.name), "dd-MMM-yy"),
                        F.to_date(F.col(field.name), "dd/MM/yyyy"),
                        F.to_date(F.col(field.name), "MM/dd/yyyy"),
                        F.to_date(F.col(field.name), "dd/MM/yy"),
                        F.to_date(F.col(field.name), "MM/dd/yy"),
                        F.to_date(F.col(field.name), "yyyy/MM/dd")
                    )
                )
                column_types["date"].append(field.name)
            elif "decimal" in field_type.lower():
                # Clean exponential notation and commas, then cast to decimal
                col = F.when(
                    F.col(field.name) == "", None
                ).otherwise(
                    clean_decimal_udf(F.col(field.name)).cast(field.dataType)
                )
                column_types["decimal"].append(field.name)
            else:
                col = col.cast(field.dataType)
                column_types["mapped"].append(field.name)

            selected_cols.append(col.alias(field.name))
        else:
            selected_cols.append(F.lit(None).cast(field.dataType).alias(field.name))
            column_types["null_filled"].append(field.name)

    if column_types["date"]:
        logger.info("  Date conversions (%d): %s", len(column_types["date"]), ", ".join(column_types["date"]))
    if column_types["decimal"]:
        logger.info("  Decimal conversions + exponential handling + comma removal (%d): %s",
                   len(column_types["decimal"]), ", ".join(column_types["decimal"]))
    if column_types["null_filled"]:
        logger.info("  NULL-filled columns (%d): %s", len(column_types["null_filled"]), ", ".join(column_types["null_filled"]))

    aligned_df = df.select(selected_cols)

    return aligned_df


def write_dataframe(df: DataFrame, config: Config) -> None:
    spark = df.sparkSession
    spark.conf.set("spark.sql.iceberg.merge-schema", str(config.merge_schema).lower())

    if not spark.catalog.tableExists(config.iceberg_table):
        raise RuntimeError(
            f"Table {config.iceberg_table} does not exist. "
            "It should have been created by iceberg-alembic migration. "
            "Run: iceberg-migrate upgrade head"
        )

    aligned_df = align_to_target_schema(df, config)
    aligned_df.writeTo(config.iceberg_table).append()


def main() -> None:
    is_valid, error_msg = validate_domain_and_layer("settlement", "gold")
    if not is_valid:
        logger.error(error_msg)
        raise ValueError(error_msg)

    config = Config()

    s3 = boto3.client(
        "s3",
        endpoint_url=config.storage_endpoint,
        aws_access_key_id=config.aws_access_key,
        aws_secret_access_key=config.aws_secret_key,
    )

    spark = (
        SparkSession.builder.appName("load-gold-settlement-to-iceberg")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.hadoop.fs.s3a.endpoint", config.storage_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # Register tenant-specific Iceberg catalog
    register_iceberg_catalog(spark)

    try:
        logger.info("=" * 70)
        logger.info("Gold Settlement Ingestion -> %s", config.iceberg_table)
        logger.info("=" * 70)
        logger.info("Tenant:        %s", config.tenant or "all")
        logger.info("Namespace:     %s", config.iceberg_namespace)
        logger.info("Table:         %s", config.iceberg_table_name)
        logger.info("Layer:         %s", config.layer)
        logger.info("Domain:        %s", config.domain)
        logger.info("Location:      %s", config.table_location)
        logger.info("Source Path:   s3://%s/%s/*/Gold/settlement/", config.source_bucket, config.source_prefix)
        logger.info("=" * 70)

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{config.iceberg_namespace}`")

        if config.tenant:
            tenant_names = [config.tenant]
            logger.info("Processing single tenant: %s", config.tenant)
        else:
            raise ValueError("TENANT is required for gold settlement ingestion")

        all_files = []
        if config.file_key:
            # Process specific file passed by DAG
            all_files.append((config.file_key, config.tenant))
            logger.info("Processing file from DAG: %s", config.file_key)
        else:
            # Discover files if no specific file provided
            for tenant_name in tenant_names:
                for file_key, tenant in list_settlement_files_for_tenant(s3, tenant_name, config):
                    all_files.append((file_key, tenant))

        if not all_files:
            logger.warning("No settlement files found in tenant directory")
            return

        logger.info("Processing %d file(s) from %d tenant(s)", len(all_files), len(tenant_names))
        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        processing_failures = []
        for file_key, tenant in all_files:
            try:
                df = process_file(spark, s3, file_key, tenant, ingested_at, config)
                row_count = df.count()
                logger.info("Schema for %s (%s rows):", file_key, row_count)
                df.printSchema()

                valid_df, invalid_df = split_valid_invalid_rows(df, config)
                invalid_count = invalid_df.count()
                valid_count = valid_df.count()

                if invalid_count > 0:
                    try:
                        write_invalid_rows(spark, s3, invalid_df, file_key, config, ingested_at)
                    except Exception as archive_exc:
                        logger.warning("Could not archive invalid rows (proceeding with valid rows): %s", archive_exc)

                if valid_count > 0:
                    write_dataframe(valid_df, config)
                    logger.info("Wrote %s valid rows to %s", valid_count, config.iceberg_table)
                else:
                    logger.warning("No valid rows to write after filtering %d invalid rows", invalid_count)

                # Skip archival for local files (starts with /)
                if not file_key.startswith("/"):
                    ingest_date = ingested_at.split('T')[0] if 'T' in ingested_at else ingested_at[:10]
                    processed_key = archive_file(file_key, "processed", config, ingest_date)
                    move_file(s3, file_key, processed_key, config.source_bucket)
                    logger.info("Archived: %s", processed_key)
                else:
                    logger.info("Local file processed (not archived): %s", file_key)
            except Exception as exc:
                logger.error("Failed to process %s: %s", file_key, exc, exc_info=True)
                processing_failures.append((file_key, str(exc)))
                # Skip archival for local files
                if not file_key.startswith("/"):
                    try:
                        ingest_date = ingested_at.split('T')[0] if 'T' in ingested_at else ingested_at[:10]
                        failed_key = archive_file(file_key, "failed", config, ingest_date)
                        move_file(s3, file_key, failed_key, config.source_bucket)
                        logger.info("Moved to failed/: %s", failed_key)
                    except Exception as move_exc:
                        logger.error("Could not archive failed file: %s", move_exc)

        if processing_failures:
            logger.error("Processing completed with %d failure(s):", len(processing_failures))
            for file_key, error in processing_failures:
                logger.error("  - %s: %s", file_key, error)
            raise RuntimeError(f"Failed to process {len(processing_failures)} file(s). See logs for details.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
