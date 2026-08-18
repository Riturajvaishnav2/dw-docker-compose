"""
Load silver fact table from bronze traffic data.

Aggregates bronze.imsi_level_traffic data by the fact grain:
one row per IMSI, call date, client, partner, traffic direction, call type,
APN/service/event/RAT/TAC/group/destination/camel combination.

All keys are generated deterministically: md5(lower(trim(column_value)))
Fact ID is generated as: md5(concatenated dimension keys)

Measures aggregated:
- duration (sum)
- volume (sum)
- event_count (sum)
- total_charge_sdr_net (sum)
- total_charge_sdr_gross (sum)
"""

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

sys.path.insert(0, "/opt/airflow")
from jobs.common.domain_to_table_mapping import DomainTableMapping
from jobs.common.spark_catalog import register_iceberg_catalog, get_catalog_name

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def env_or_default(name: str, default: str) -> str:
    return os.getenv(name) or default


@dataclass
class Config:
    tenant: str = field(default_factory=lambda: env_or_default("TENANT", ""))
    merge_schema: bool = field(default_factory=lambda: env_or_default("MERGE_SCHEMA", "false").lower() == "true")

    def __post_init__(self) -> None:
        if self.tenant:
            normalized_tenant = self.tenant.lower()
            os.environ["ICEBERG_NAMESPACE"] = normalized_tenant
            self.tenant = normalized_tenant

        mapping = DomainTableMapping()
        self.bronze_config = mapping.get_table_config("traffic", "bronze")
        if not self.bronze_config:
            raise ValueError("Traffic bronze table config not found")

        self.iceberg_namespace = self.bronze_config.iceberg_namespace
        self.bronze_table = self.bronze_config.full_path


def generate_dimension_keys(df) -> any:
    """Generate deterministic keys for all dimension columns."""
    unknown_key = F.md5(F.lit("unknown"))

    df = (
        df.withColumn("client_pmn_key", F.coalesce(
            F.md5(F.lower(F.trim(F.col("client_pmn")))),
            unknown_key
        ))
        .withColumn("partner_pmn_key", F.coalesce(
            F.md5(F.lower(F.trim(F.col("partner_pmn")))),
            unknown_key
        ))
        .withColumn("traffic_direction_key", F.coalesce(
            F.md5(F.lower(F.trim(F.col("traffic_direction")))),
            unknown_key
        ))
        .withColumn("call_date_key", F.coalesce(
            F.md5(F.lower(F.trim(F.col("call_date")))),
            unknown_key
        ))
        .withColumn("call_month_key", F.coalesce(
            F.md5(F.lower(F.trim(F.col("call_month")))),
            unknown_key
        ))
        .withColumn("call_type_key", F.coalesce(
            F.md5(F.lower(F.trim(F.col("call_type")))),
            unknown_key
        ))
        .withColumn("call_type_level_2_key", F.when(
            F.col("call_type_level_2").isNotNull(),
            F.md5(F.lower(F.trim(F.col("call_type_level_2"))))
        ).otherwise(unknown_key))
        .withColumn("imsi_key", F.coalesce(
            F.md5(F.lower(F.trim(F.col("imsi")))),
            unknown_key
        ))
        .withColumn("apn_key", F.when(
            F.col("apn").isNotNull(),
            F.md5(F.lower(F.trim(F.col("apn"))))
        ).otherwise(unknown_key))
        .withColumn("roamer_indicator_key", F.when(
            F.col("roamer_indicator").isNotNull(),
            F.md5(F.lower(F.trim(F.col("roamer_indicator"))))
        ).otherwise(unknown_key))
        .withColumn("service_type_key", F.when(
            F.col("service_type_id").isNotNull(),
            F.md5(F.lower(F.trim(F.col("service_type_id"))))
        ).otherwise(unknown_key))
        .withColumn("event_type_key", F.when(
            F.col("event_type_id").isNotNull(),
            F.md5(F.lower(F.trim(F.col("event_type_id"))))
        ).otherwise(unknown_key))
        .withColumn("rat_type_key", F.when(
            F.col("rat_type").isNotNull(),
            F.md5(F.lower(F.trim(F.col("rat_type"))))
        ).otherwise(unknown_key))
        .withColumn("tac_key", F.when(
            F.col("tac_number").isNotNull(),
            F.md5(F.lower(F.trim(F.col("tac_number"))))
        ).otherwise(unknown_key))
        .withColumn("iot_rti_group_key", F.when(
            F.col("iot_rti_group_id").isNotNull(),
            F.md5(F.lower(F.trim(F.col("iot_rti_group_id"))))
        ).otherwise(unknown_key))
        .withColumn("destination_key", F.when(
            F.col("destination").isNotNull(),
            F.md5(F.lower(F.trim(F.col("destination"))))
        ).otherwise(unknown_key))
        .withColumn("camel_key", F.when(
            F.col("is_camel").isNotNull(),
            F.md5(F.lower(F.trim(F.col("is_camel"))))
        ).otherwise(unknown_key))
    )

    return df


def cast_measures(df) -> any:
    """Cast measure columns to proper types."""
    return (
        df.withColumn("duration", F.col("duration").cast(DecimalType(18, 6)))
        .withColumn("volume", F.col("volume").cast(DecimalType(18, 6)))
        .withColumn("event_count", F.col("event_count").cast("bigint"))
        .withColumn("total_charge_sdr_net", F.col("total_charge_sdr_net").cast(DecimalType(18, 6)))
        .withColumn("total_charge_sdr_gross", F.col("total_charge_sdr_gross").cast(DecimalType(18, 6)))
    )


def generate_fact_id(df) -> any:
    """Generate fact ID by concatenating and hashing all dimension keys."""
    fact_cols = [
        "client_pmn_key", "partner_pmn_key", "traffic_direction_key",
        "call_date_key", "call_month_key", "call_type_key", "call_type_level_2_key",
        "imsi_key", "apn_key", "roamer_indicator_key",
        "service_type_key", "event_type_key", "rat_type_key", "tac_key",
        "iot_rti_group_key", "destination_key", "camel_key"
    ]

    concat_expr = F.concat_ws("|", *[F.col(col) for col in fact_cols])
    df = df.withColumn("traffic_fact_id", F.md5(concat_expr))

    return df


def load_silver_fact(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load fact table aggregated from bronze data."""
    logger.info("Loading fact_imsi_level_traffic...")

    # Read bronze data
    bronze_df = spark.table(bronze_table)
    logger.info("Bronze table read, counting rows...")
    bronze_count = bronze_df.count()
    logger.info("Bronze table has %d rows", bronze_count)

    if bronze_count == 0:
        logger.warning("Bronze table is empty, skipping fact load")
        return

    # Generate dimension keys
    logger.info("Generating dimension keys...")
    fact_df = generate_dimension_keys(bronze_df)

    # Aggregate by grain (all dimension keys)
    dimension_cols = [
        "client_pmn_key", "partner_pmn_key", "traffic_direction_key",
        "call_date_key", "call_month_key", "call_type_key", "call_type_level_2_key",
        "imsi_key", "apn_key", "roamer_indicator_key",
        "service_type_key", "event_type_key", "rat_type_key", "tac_key",
        "iot_rti_group_key", "destination_key", "camel_key"
    ]

    # Note: Spark configs are set at submit time in run_spark_submit.sh
    # (shuffle.manager, partitions, timeouts, AQE, etc.)
    # Do not set them here as runtime config changes are not allowed

    # Aggregate measures
    logger.info("Aggregating %d rows by %d dimension columns (this may take several minutes)...", bronze_count, len(dimension_cols))
    fact_df = (
        fact_df.groupBy(*dimension_cols)
        .agg(
            F.sum(F.col("duration").cast("double")).alias("duration"),
            F.sum(F.col("volume").cast("double")).alias("volume"),
            F.sum(F.col("event_count").cast("long")).alias("event_count"),
            F.sum(F.col("total_charge_sdr_net").cast("double")).alias("total_charge_sdr_net"),
            F.sum(F.col("total_charge_sdr_gross").cast("double")).alias("total_charge_sdr_gross"),
            F.first(F.col("source_system")).alias("source_system"),
            F.first(F.col("batch_id")).alias("batch_id"),
        )
    )
    logger.info("GroupBy aggregation complete, casting measures...")

    # Cast measures to decimal
    fact_df = cast_measures(fact_df)

    # Generate fact ID
    logger.info("Generating fact IDs...")
    fact_df = generate_fact_id(fact_df)

    # Add audit columns
    logger.info("Adding audit columns...")
    fact_df = (
        fact_df.withColumn("record_hash", F.md5(F.concat_ws("|", *dimension_cols)))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    # Select final columns in order
    final_columns = [
        "traffic_fact_id",
        "client_pmn_key", "partner_pmn_key", "traffic_direction_key",
        "call_date_key", "call_month_key", "call_type_key", "call_type_level_2_key",
        "imsi_key", "apn_key", "roamer_indicator_key",
        "service_type_key", "event_type_key", "rat_type_key", "tac_key",
        "iot_rti_group_key", "destination_key", "camel_key",
        "duration", "volume", "event_count",
        "total_charge_sdr_net", "total_charge_sdr_gross",
        "source_system", "batch_id", "record_hash",
        "created_at", "updated_at"
    ]

    logger.info("Selecting final columns...")
    fact_df = fact_df.select(*final_columns)

    table_name = f"{catalog}.silver.`fact_imsi_level_traffic`"
    logger.info("Writing %d aggregated rows to %s", fact_df.count(), table_name)
    fact_df.writeTo(table_name).append()
    logger.info("Fact table loaded successfully")


def main() -> None:
    config = Config()

    spark = (
        SparkSession.builder.appName("load-silver-fact")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    catalog = register_iceberg_catalog(spark)

    try:
        logger.info("=" * 70)
        logger.info("Silver Fact Loading")
        logger.info("=" * 70)
        logger.info("Tenant:        %s", config.tenant or "all")
        logger.info("Catalog:       %s", catalog)
        logger.info("Bronze Table:  %s", config.bronze_table)
        logger.info("=" * 70)

        load_silver_fact(spark, config.bronze_table, catalog)

        logger.info("=" * 70)
        logger.info("Fact table loaded successfully")
        logger.info("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
