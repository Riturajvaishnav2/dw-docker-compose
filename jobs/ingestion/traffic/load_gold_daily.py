"""
Load gold.imsi_level_traffic_daily from silver fact and dimensions.

Denormalizes silver fact table by joining with all dimension tables
to create a business-ready daily traffic table.

Grain: one row per client, partner, direction, call_date, call_type, IMSI, APN, service grouping.
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


def load_gold_daily(spark: SparkSession, catalog: str) -> None:
    """Load gold daily table by denormalizing silver fact + dimensions."""
    logger.info("Loading gold.imsi_level_traffic_daily...")

    # Read silver fact table and dimension tables (using dynamic catalog)
    fact_df = spark.table(f"{catalog}.silver.`fact_imsi_level_traffic`")

    dim_client = spark.table(f"{catalog}.silver.`dim_client`").select(
        F.col("pmn_key").alias("client_pmn_key_dim"),
        F.col("pmn").alias("client_pmn")
    )
    dim_operator = spark.table(f"{catalog}.silver.`dim_operator`").select(
        F.col("pmn_key").alias("partner_pmn_key_dim"),
        F.col("pmn").alias("partner_pmn"),
        F.col("pmn_country").alias("roaming_partner_country")
    )
    dim_traffic_direction = spark.table(f"{catalog}.silver.`dim_traffic_direction`").select(
        F.col("traffic_direction_key").alias("traffic_direction_key_dim"),
        F.col("traffic_direction")
    )
    dim_date = spark.table(f"{catalog}.silver.`dim_date`").select(
        F.col("call_date_key").alias("call_date_key_dim"),
        F.col("call_date"),
        F.col("call_month"),
        F.col("year"),
        F.col("month"),
        F.col("day")
    )
    dim_call_type = spark.table(f"{catalog}.silver.`dim_call_type`").select(
        F.col("call_type_key").alias("call_type_key_dim"),
        F.col("call_type"),
        F.col("call_type_level_2")
    )
    dim_imsi = spark.table(f"{catalog}.silver.`dim_imsi`").select(
        F.col("imsi_key").alias("imsi_key_dim"),
        F.col("imsi"),
        F.col("roamer_indicator")
    )
    dim_apn = spark.table(f"{catalog}.silver.`dim_apn`").select(
        F.col("apn_key").alias("apn_key_dim"),
        F.col("apn")
    )
    dim_service_type = spark.table(f"{catalog}.silver.`dim_service_type`").select(
        F.col("service_type_key").alias("service_type_key_dim"),
        F.col("service_type_id")
    )
    dim_event_type = spark.table(f"{catalog}.silver.`dim_event_type`").select(
        F.col("event_type_key").alias("event_type_key_dim"),
        F.col("event_type_id")
    )
    dim_rat_type = spark.table(f"{catalog}.silver.`dim_rat_type`").select(
        F.col("rat_type_key").alias("rat_type_key_dim"),
        F.col("rat_type")
    )
    dim_tac = spark.table(f"{catalog}.silver.`dim_tac`").select(
        F.col("tac_key").alias("tac_key_dim"),
        F.col("tac_number")
    )
    dim_iot_rti_group = spark.table(f"{catalog}.silver.`dim_iot_rti_group`").select(
        F.col("iot_rti_group_key").alias("iot_rti_group_key_dim"),
        F.col("iot_rti_group_id")
    )
    dim_location = spark.table(f"{catalog}.silver.`dim_location`").select(
        F.col("location_key").alias("location_key_dim"),
        F.col("location").alias("destination"),
        F.col("location_category").alias("destination_category")
    )
    dim_camel = spark.table(f"{catalog}.silver.`dim_camel`").select(
        F.col("camel_key").alias("camel_key_dim"),
        F.col("is_camel")
    )

    # Join with all dimension tables to denormalize
    daily_df = (
        fact_df
        .join(dim_client, fact_df.client_pmn_key == dim_client.client_pmn_key_dim, "left")
        .join(dim_operator, fact_df.partner_pmn_key == dim_operator.partner_pmn_key_dim, "left")
        .join(dim_traffic_direction, fact_df.traffic_direction_key == dim_traffic_direction.traffic_direction_key_dim, "left")
        .join(dim_date, fact_df.call_date_key == dim_date.call_date_key_dim, "left")
        .join(dim_call_type, fact_df.call_type_key == dim_call_type.call_type_key_dim, "left")
        .join(dim_imsi, fact_df.imsi_key == dim_imsi.imsi_key_dim, "left")
        .join(dim_apn, fact_df.apn_key == dim_apn.apn_key_dim, "left")
        .join(dim_service_type, fact_df.service_type_key == dim_service_type.service_type_key_dim, "left")
        .join(dim_event_type, fact_df.event_type_key == dim_event_type.event_type_key_dim, "left")
        .join(dim_rat_type, fact_df.rat_type_key == dim_rat_type.rat_type_key_dim, "left")
        .join(dim_tac, fact_df.tac_key == dim_tac.tac_key_dim, "left")
        .join(dim_iot_rti_group, fact_df.iot_rti_group_key == dim_iot_rti_group.iot_rti_group_key_dim, "left")
        .join(dim_location, fact_df.destination_key == dim_location.location_key_dim, "left")
        .join(dim_camel, fact_df.camel_key == dim_camel.camel_key_dim, "left")
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    # Select only the business columns (drop all _dim suffixed keys)
    final_df = daily_df.select(
        "client_pmn",
        "partner_pmn",
        "roaming_partner_country",
        "traffic_direction",
        "call_date",
        "call_month",
        "year",
        "month",
        "day",
        "call_type",
        "call_type_level_2",
        "imsi",
        "roamer_indicator",
        "apn",
        "service_type_id",
        "event_type_id",
        "rat_type",
        "tac_number",
        "iot_rti_group_id",
        "destination",
        "destination_category",
        "is_camel",
        F.col("duration").cast("decimal(18,6)").alias("total_duration"),
        F.col("volume").cast("decimal(18,6)").alias("total_volume"),
        F.col("event_count").cast("bigint").alias("total_event_count"),
        "total_charge_sdr_net",
        "total_charge_sdr_gross",
        "created_at",
        "updated_at"
    )

    table_name = f"{catalog}.gold.`imsi_level_traffic_daily`"
    final_df.writeTo(table_name).append()
    logger.info("Loaded %d rows to %s", final_df.count(), table_name)


def main() -> None:
    config = Config()

    spark = (
        SparkSession.builder.appName("load-gold-traffic-daily")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    catalog = register_iceberg_catalog(spark)

    try:
        logger.info("=" * 70)
        logger.info("Gold Daily Traffic Loading")
        logger.info("=" * 70)
        logger.info("Tenant:    %s", config.tenant or "all")
        logger.info("Catalog:   %s", catalog)
        logger.info("=" * 70)

        load_gold_daily(spark, catalog)

        logger.info("=" * 70)
        logger.info("Gold daily table loaded successfully")
        logger.info("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
