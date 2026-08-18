"""
Load silver dimension tables from bronze traffic data with upsert logic.

Extracts unique values from bronze.imsi_level_traffic and loads them into dimension tables.
Uses merge/upsert pattern: if dimension record exists (by key), skip it; if new, insert it.

Dimensions:
- dim_client, dim_operator, dim_traffic_direction, dim_date, dim_call_type
- dim_imsi, dim_apn, dim_service_type, dim_event_type, dim_rat_type
- dim_tac, dim_iot_rti_group, dim_location, dim_camel

All keys generated deterministically: md5(lower(trim(column_value)))
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


def upsert_dimension(spark: SparkSession, bronze_df, key_col: str, table_name: str, select_cols: list) -> int:
    """
    Upsert dimension: insert new records, skip existing ones (by key).
    Returns count of new records inserted.
    """
    # Get distinct values from bronze
    new_df = bronze_df.select(*select_cols).distinct()

    # Try to read existing dimension
    try:
        existing_df = spark.table(table_name).select(key_col)
        existing_keys = existing_df.rdd.map(lambda r: r[0]).collect()

        # Filter to only new records
        new_df = new_df.filter(~F.col(key_col).isin(existing_keys))
        new_count = new_df.count()

        if new_count > 0:
            new_df.writeTo(table_name).append()
            logger.info("Inserted %d new records to %s", new_count, table_name)
        else:
            logger.info("No new records for %s (all exist)", table_name)
        return new_count
    except Exception as e:
        logger.warning("Could not read existing %s, assuming fresh: %s", table_name, e)
        new_df.writeTo(table_name).append()
        return new_df.count()


def load_dimension_client(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique client PMNs with upsert."""
    logger.info("Loading dim_client...")
    bronze_df = spark.table(bronze_table).filter(F.col("client_pmn").isNotNull())

    dim_df = bronze_df.select("client_pmn").distinct()
    dim_df = (
        dim_df
        .withColumn("pmn_key", F.md5(F.lower(F.trim(F.col("client_pmn")))))
        .withColumn("pmn", F.col("client_pmn"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("pmn_key", "pmn", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_client`"
    upsert_dimension(spark, dim_df, "pmn_key", table_name, ["pmn_key", "pmn", "created_at", "updated_at"])


def load_dimension_operator(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique partner PMNs with upsert."""
    logger.info("Loading dim_operator...")
    bronze_df = spark.table(bronze_table).filter(F.col("partner_pmn").isNotNull())

    dim_df = bronze_df.select("partner_pmn", "roaming_partner_country").distinct()
    dim_df = (
        dim_df
        .withColumn("pmn_key", F.md5(F.lower(F.trim(F.col("partner_pmn")))))
        .withColumn("pmn", F.col("partner_pmn"))
        .withColumn("pmn_country", F.col("roaming_partner_country"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("pmn_key", "pmn", "pmn_country", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_operator`"
    upsert_dimension(spark, dim_df, "pmn_key", table_name, ["pmn_key", "pmn", "pmn_country", "created_at", "updated_at"])


def load_dimension_traffic_direction(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique traffic directions with upsert."""
    logger.info("Loading dim_traffic_direction...")
    bronze_df = spark.table(bronze_table).filter(F.col("traffic_direction").isNotNull())

    dim_df = bronze_df.select("traffic_direction").distinct()
    dim_df = (
        dim_df
        .withColumn("traffic_direction_key", F.md5(F.lower(F.trim(F.col("traffic_direction")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("traffic_direction_key", "traffic_direction", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_traffic_direction`"
    upsert_dimension(spark, dim_df, "traffic_direction_key", table_name,
                    ["traffic_direction_key", "traffic_direction", "created_at", "updated_at"])


def load_dimension_date(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique call dates with upsert."""
    logger.info("Loading dim_date...")
    bronze_df = spark.table(bronze_table).filter(F.col("call_date").isNotNull())

    dim_df = bronze_df.select("call_date", "call_month").distinct()
    dim_df = (
        dim_df
        .withColumn("call_date_key", F.md5(F.lower(F.trim(F.col("call_date")))))
        .withColumn("call_date", F.to_date(F.col("call_date"), "yyyy-MM-dd"))
        .withColumn("year", F.year(F.col("call_date")))
        .withColumn("month", F.month(F.col("call_date")))
        .withColumn("day", F.dayofmonth(F.col("call_date")))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("call_date_key", "call_date", "call_month", "year", "month", "day", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_date`"
    upsert_dimension(spark, dim_df, "call_date_key", table_name,
                    ["call_date_key", "call_date", "call_month", "year", "month", "day", "created_at", "updated_at"])


def load_dimension_call_type(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique call types with upsert."""
    logger.info("Loading dim_call_type...")
    bronze_df = spark.table(bronze_table).filter(F.col("call_type").isNotNull())

    dim_df = bronze_df.select("call_type", "call_type_level_2").distinct()
    dim_df = (
        dim_df
        .withColumn("call_type_key", F.md5(F.lower(F.trim(F.col("call_type")))))
        .withColumn("call_type_level_2_key", F.when(
            F.col("call_type_level_2").isNotNull(),
            F.md5(F.lower(F.trim(F.col("call_type_level_2"))))
        ).otherwise(F.lit(None)))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("call_type_key", "call_type", "call_type_level_2_key", "call_type_level_2", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_call_type`"
    upsert_dimension(spark, dim_df, "call_type_key", table_name,
                    ["call_type_key", "call_type", "call_type_level_2_key", "call_type_level_2", "created_at", "updated_at"])


def load_dimension_imsi(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique IMSIs with upsert."""
    logger.info("Loading dim_imsi...")
    bronze_df = spark.table(bronze_table).filter(F.col("imsi").isNotNull())

    dim_df = bronze_df.select("imsi", "roamer_indicator").distinct()
    dim_df = (
        dim_df
        .withColumn("imsi_key", F.md5(F.lower(F.trim(F.col("imsi")))))
        .withColumn("roamer_indicator_key", F.when(
            F.col("roamer_indicator").isNotNull(),
            F.md5(F.lower(F.trim(F.col("roamer_indicator"))))
        ).otherwise(F.lit(None)))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("imsi_key", "imsi", "roamer_indicator_key", "roamer_indicator", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_imsi`"
    upsert_dimension(spark, dim_df, "imsi_key", table_name,
                    ["imsi_key", "imsi", "roamer_indicator_key", "roamer_indicator", "created_at", "updated_at"])


def load_dimension_apn(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique APNs with upsert."""
    logger.info("Loading dim_apn...")
    bronze_df = spark.table(bronze_table).filter(F.col("apn").isNotNull())

    dim_df = bronze_df.select("apn").distinct()
    dim_df = (
        dim_df
        .withColumn("apn_key", F.md5(F.lower(F.trim(F.col("apn")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("apn_key", "apn", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_apn`"
    upsert_dimension(spark, dim_df, "apn_key", table_name, ["apn_key", "apn", "created_at", "updated_at"])


def load_dimension_service_type(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique service types with upsert."""
    logger.info("Loading dim_service_type...")
    bronze_df = spark.table(bronze_table).filter(F.col("service_type_id").isNotNull())

    dim_df = bronze_df.select("service_type_id").distinct()
    dim_df = (
        dim_df
        .withColumn("service_type_key", F.md5(F.lower(F.trim(F.col("service_type_id")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("service_type_key", "service_type_id", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_service_type`"
    upsert_dimension(spark, dim_df, "service_type_key", table_name,
                    ["service_type_key", "service_type_id", "created_at", "updated_at"])


def load_dimension_event_type(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique event types with upsert."""
    logger.info("Loading dim_event_type...")
    bronze_df = spark.table(bronze_table).filter(F.col("event_type_id").isNotNull())

    dim_df = bronze_df.select("event_type_id").distinct()
    dim_df = (
        dim_df
        .withColumn("event_type_key", F.md5(F.lower(F.trim(F.col("event_type_id")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("event_type_key", "event_type_id", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_event_type`"
    upsert_dimension(spark, dim_df, "event_type_key", table_name,
                    ["event_type_key", "event_type_id", "created_at", "updated_at"])


def load_dimension_rat_type(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique RAT types with upsert."""
    logger.info("Loading dim_rat_type...")
    bronze_df = spark.table(bronze_table).filter(F.col("rat_type").isNotNull())

    dim_df = bronze_df.select("rat_type").distinct()
    dim_df = (
        dim_df
        .withColumn("rat_type_key", F.md5(F.lower(F.trim(F.col("rat_type")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("rat_type_key", "rat_type", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_rat_type`"
    upsert_dimension(spark, dim_df, "rat_type_key", table_name, ["rat_type_key", "rat_type", "created_at", "updated_at"])


def load_dimension_tac(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique TACs with upsert."""
    logger.info("Loading dim_tac...")
    bronze_df = spark.table(bronze_table).filter(F.col("tac_number").isNotNull())

    dim_df = bronze_df.select("tac_number").distinct()
    dim_df = (
        dim_df
        .withColumn("tac_key", F.md5(F.lower(F.trim(F.col("tac_number")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("tac_key", "tac_number", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_tac`"
    upsert_dimension(spark, dim_df, "tac_key", table_name, ["tac_key", "tac_number", "created_at", "updated_at"])


def load_dimension_iot_rti_group(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique IoT RTI groups with upsert."""
    logger.info("Loading dim_iot_rti_group...")
    bronze_df = spark.table(bronze_table).filter(F.col("iot_rti_group_id").isNotNull())

    dim_df = bronze_df.select("iot_rti_group_id").distinct()
    dim_df = (
        dim_df
        .withColumn("iot_rti_group_key", F.md5(F.lower(F.trim(F.col("iot_rti_group_id")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("iot_rti_group_key", "iot_rti_group_id", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_iot_rti_group`"
    upsert_dimension(spark, dim_df, "iot_rti_group_key", table_name,
                    ["iot_rti_group_key", "iot_rti_group_id", "created_at", "updated_at"])


def load_dimension_location(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique locations with upsert."""
    logger.info("Loading dim_location...")
    bronze_df = spark.table(bronze_table).filter(F.col("destination").isNotNull())

    dim_df = bronze_df.select("destination", "destination_category").distinct()
    dim_df = (
        dim_df
        .withColumn("location_key", F.md5(F.lower(F.trim(F.col("destination")))))
        .withColumn("location", F.col("destination"))
        .withColumn("location_category", F.col("destination_category"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("location_key", "location", "location_category", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_location`"
    upsert_dimension(spark, dim_df, "location_key", table_name,
                    ["location_key", "location", "location_category", "created_at", "updated_at"])


def load_dimension_camel(spark: SparkSession, bronze_table: str, catalog: str) -> None:
    """Load unique camel indicators with upsert."""
    logger.info("Loading dim_camel...")
    bronze_df = spark.table(bronze_table).filter(F.col("is_camel").isNotNull())

    dim_df = bronze_df.select("is_camel").distinct()
    dim_df = (
        dim_df
        .withColumn("camel_key", F.md5(F.lower(F.trim(F.col("is_camel")))))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select("camel_key", "is_camel", "created_at", "updated_at")
    )

    table_name = f"{catalog}.silver.`dim_camel`"
    upsert_dimension(spark, dim_df, "camel_key", table_name, ["camel_key", "is_camel", "created_at", "updated_at"])


def main() -> None:
    config = Config()

    spark = (
        SparkSession.builder.appName("load-silver-dimensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    catalog = register_iceberg_catalog(spark)

    try:
        logger.info("=" * 70)
        logger.info("Silver Dimension Loading (Upsert Mode)")
        logger.info("=" * 70)
        logger.info("Tenant:        %s", config.tenant or "all")
        logger.info("Catalog:       %s", catalog)
        logger.info("Bronze Table:  %s", config.bronze_table)
        logger.info("=" * 70)
        load_dimension_client(spark, config.bronze_table, catalog)
        load_dimension_operator(spark, config.bronze_table, catalog)
        load_dimension_traffic_direction(spark, config.bronze_table, catalog)
        load_dimension_date(spark, config.bronze_table, catalog)
        load_dimension_call_type(spark, config.bronze_table, catalog)
        load_dimension_imsi(spark, config.bronze_table, catalog)
        load_dimension_apn(spark, config.bronze_table, catalog)
        load_dimension_service_type(spark, config.bronze_table, catalog)
        load_dimension_event_type(spark, config.bronze_table, catalog)
        load_dimension_rat_type(spark, config.bronze_table, catalog)
        load_dimension_tac(spark, config.bronze_table, catalog)
        load_dimension_iot_rti_group(spark, config.bronze_table, catalog)
        load_dimension_location(spark, config.bronze_table, catalog)
        load_dimension_camel(spark, config.bronze_table, catalog)

        logger.info("=" * 70)
        logger.info("All dimensions loaded successfully")
        logger.info("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
