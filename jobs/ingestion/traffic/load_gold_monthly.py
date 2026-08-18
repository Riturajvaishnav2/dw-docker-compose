"""
Load gold.client_partner_traffic_monthly from gold.imsi_level_traffic_daily.

Aggregates daily traffic by client, partner, direction, month, call_type, service
with distinct counts of IMSI and APN.

Grain: monthly client-partner traffic summary.
"""

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/airflow")
from jobs.common.domain_to_table_mapping import DomainTableMapping
from jobs.common.spark_catalog import register_iceberg_catalog

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def env_or_default(name: str, default: str) -> str:
    return os.getenv(name) or default


@dataclass
class Config:
    tenant: str = field(default_factory=lambda: env_or_default("TENANT", ""))

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


def load_gold_monthly(spark: SparkSession, catalog: str) -> None:
    """Load gold monthly table by aggregating daily table."""
    logger.info("Loading gold.client_partner_traffic_monthly...")

    # Read gold daily table (using dynamic catalog)
    daily_df = spark.table(f"{catalog}.gold.`imsi_level_traffic_daily`")

    # Aggregate by client, partner, direction, month, call_type, service
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
            F.sum(F.col("total_duration")).cast("decimal(18,6)").alias("total_duration"),
            F.sum(F.col("total_volume")).cast("decimal(18,6)").alias("total_volume"),
            F.sum(F.col("total_event_count")).cast("bigint").alias("total_event_count"),
            F.sum(F.col("total_charge_sdr_net")).cast("decimal(18,6)").alias("total_charge_sdr_net"),
            F.sum(F.col("total_charge_sdr_gross")).cast("decimal(18,6)").alias("total_charge_sdr_gross"),
            F.countDistinct(F.col("imsi")).alias("distinct_imsi_count"),
            F.countDistinct(F.col("apn")).alias("distinct_apn_count"),
        )
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    # Select columns in order
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

    table_name = f"{catalog}.gold.`client_partner_traffic_monthly`"
    final_df.writeTo(table_name).append()
    logger.info("Loaded %d rows to %s", final_df.count(), table_name)


def main() -> None:
    config = Config()

    spark = (
        SparkSession.builder.appName("load-gold-traffic-monthly")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    catalog = register_iceberg_catalog(spark)

    try:
        logger.info("=" * 70)
        logger.info("Gold Monthly Traffic Loading")
        logger.info("=" * 70)
        logger.info("Tenant:    %s", config.tenant or "all")
        logger.info("Catalog:   %s", catalog)
        logger.info("=" * 70)

        load_gold_monthly(spark, catalog)

        logger.info("=" * 70)
        logger.info("Gold monthly table loaded successfully")
        logger.info("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
