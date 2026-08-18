"""Load Silver Forecast Fact Table - FIXED column count."""

import logging
import os
import sys
from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, "/opt/airflow")
from jobs.common.spark_catalog import register_iceberg_catalog

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def env_or_default(name: str, default: str) -> str:
    return os.getenv(name) or default


@dataclass
class Config:
    tenant: str = field(default_factory=lambda: env_or_default("ICEBERG_NAMESPACE", "ee").lower())


def load_fact_iot_forecast(spark: SparkSession, config: Config) -> int:
    """Load fact_iot_forecast from bronze - match silver table schema."""
    logger.info("Loading fact_iot_forecast from bronze...")

    try:
        df = spark.table(f"{config.tenant}.bronze.iot_forecast_raw")
        record_count = df.count()
        logger.info(f"Read {record_count} records from bronze")

        # Convert dates
        df = df.withColumn("period_start_date", F.to_date(F.col("period_start_date")))
        df = df.withColumn("cr_period_start_date", F.to_date(F.col("cr_period_start_date")))

        # Join with dimension tables to get IDs
        logger.info("Loading and joining with dimension tables...")

        # Partner dimension (already loaded by load_silver.py)
        dim_partner = spark.table(f"{config.tenant}.silver.dim_partner").select(
            "partner_id", "partner_name", "partner_group_name"
        ).withColumnRenamed("partner_id", "partner_master_entity_id")

        df = df.join(dim_partner, on=["partner_name", "partner_group_name"], how="left")

        # Agreement dimension (already loaded by load_silver.py)
        dim_agreement = spark.table(f"{config.tenant}.silver.dim_agreement").select(
            "iot_agreement_id", "agreement_reference"
        )

        df = df.join(dim_agreement, on="agreement_reference", how="left")

        # Client ID (generated from name hash - similar to partner)
        df = df.withColumn("client_master_entity_id",
            F.abs(F.hash(F.concat_ws("|", F.col("client_name"), F.col("client_group_name")))).cast(T.LongType()))

        # Other dimension IDs (generated from string values using hash)
        df = df.withColumn("iot_service_type_id",
            F.abs(F.hash(F.col("service_type"))).cast(T.LongType()))
        df = df.withColumn("iot_event_type_id",
            F.abs(F.hash(F.col("event_type"))).cast(T.LongType()))
        df = df.withColumn("destination_type_id",
            F.abs(F.hash(F.col("destination_type"))).cast(T.LongType()))
        df = df.withColumn("iot_rti_group_id",
            F.abs(F.hash(F.col("rti_group"))).cast(T.LongType()))

        # Add remaining placeholder ID columns (for future dimension expansions)
        df = df.withColumn("client_main_master_entity_id", F.lit(None).cast("long"))
        df = df.withColumn("partner_main_master_entity_id", F.lit(None).cast("long"))

        # Cast decimal columns to match silver schema (20,6 instead of 20,2)
        decimal_cols = ["traffic_volume", "charged_volume", "tap_charge_sdr_net",
                       "tap_charge_sdr_gross", "disc_charge_sdr_net", "disc_charge_sdr_gross"]
        for col in decimal_cols:
            df = df.withColumn(col, F.col(col).cast(T.DecimalType(20, 6)))

        # Add audit columns
        df = df.withColumn("record_hash", F.md5(F.concat_ws(":", F.col("client_name"), F.col("partner_name"))))
        df = df.withColumn("ingestion_id", F.lit("forecast_001"))
        df = df.withColumn("created_at", F.current_timestamp())

        # Cast no_of_records to long to match silver schema
        df = df.withColumn("no_of_records", F.col("no_of_records").cast(T.LongType()))

        # Select columns matching the silver table schema exactly
        final_columns = [
            "client_master_entity_id", "client_main_master_entity_id",
            "partner_master_entity_id", "partner_main_master_entity_id",
            "iot_service_type_id", "iot_event_type_id", "destination_type_id",
            "iot_agreement_id", "iot_rti_group_id",
            "traffic_direction", "period_start_date", "cr_period_start_date",
            "forecast_or_actual_ind",
            "traffic_volume", "charged_volume",
            "tap_charge_sdr_net", "tap_charge_sdr_gross",
            "disc_charge_sdr_net", "disc_charge_sdr_gross",
            "no_of_records", "record_hash", "ingestion_id", "created_at"
        ]

        logger.info("Selecting columns matching table schema...")
        df = df.select(*final_columns)

        table_name = f"{config.tenant}.silver.fact_iot_forecast"
        final_count = df.count()

        logger.info(f"Writing {final_count} rows to {table_name}...")
        df.writeTo(table_name).append()

        logger.info(f"✓ Successfully loaded {final_count} fact records")
        return final_count

    except Exception as e:
        logger.error(f"Failed to load fact table: {e}", exc_info=True)
        raise


def main() -> None:
    config = Config()
    logger.info(f"Loading fact table for tenant: {config.tenant}")

    spark = SparkSession.builder \
        .appName(f"ForecastSilverFact-{config.tenant}") \
        .getOrCreate()

    try:
        register_iceberg_catalog(spark, config.tenant)
        load_fact_iot_forecast(spark, config)
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
