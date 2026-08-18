"""Load Silver Forecast Dimension Tables from Bronze data."""

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

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


def load_dimensions(spark: SparkSession, config: Config) -> int:
    """Load all forecast dimensions from bronze data."""
    logger.info("Loading forecast dimensions from bronze data...")
    bronze_df = spark.table(f"{config.tenant}.bronze.iot_forecast_raw")
    total_count = 0

    try:
        # 1. Load dim_forecast_actual
        logger.info("Loading dim_forecast_actual...")
        forecast_actual_df = bronze_df.select(
            F.col("forecast_or_actual_ind")
        ).filter(F.col("forecast_or_actual_ind").isNotNull()).distinct()

        forecast_actual_df = forecast_actual_df \
            .withColumn("forecast_actual_key", F.abs(F.hash(F.col("forecast_or_actual_ind"))).cast(T.LongType())) \
            .withColumn("forecast_actual_desc",
                F.when(F.col("forecast_or_actual_ind") == "F", "Forecast")
                 .when(F.col("forecast_or_actual_ind") == "A", "Actual")
                 .otherwise("Unknown")
            ) \
            .select("forecast_actual_key", "forecast_or_actual_ind", "forecast_actual_desc")

        count = forecast_actual_df.count()
        forecast_actual_df.writeTo(f"{config.tenant}.silver.dim_forecast_actual").append()
        logger.info(f"  ✓ Loaded {count} forecast_actual records")
        total_count += count

        # 2. Load dim_partner
        logger.info("Loading dim_partner...")
        partner_df = bronze_df.select(
            F.col("partner_name"),
            F.col("partner_group_name")
        ).filter((F.col("partner_name").isNotNull()) & (F.col("partner_group_name").isNotNull())).distinct()

        partner_df = partner_df \
            .withColumn("partner_id", F.abs(F.hash(F.concat_ws("|", F.col("partner_name"), F.col("partner_group_name")))).cast(T.LongType())) \
            .withColumn("partner_key", F.concat_ws("_", F.col("partner_name"), F.col("partner_group_name"))) \
            .withColumn("partner_pmn", F.lit("")) \
            .withColumn("partner_country", F.lit("")) \
            .withColumn("is_active", F.lit(True)) \
            .withColumn("created_at", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp()) \
            .select("partner_id", "partner_key", "partner_name", "partner_group_name", "partner_pmn", "partner_country", "is_active", "created_at", "updated_at")

        count = partner_df.count()
        partner_df.writeTo(f"{config.tenant}.silver.dim_partner").append()
        logger.info(f"  ✓ Loaded {count} partner records")
        total_count += count

        # 3. Load dim_agreement
        logger.info("Loading dim_agreement...")
        agreement_df = bronze_df.select(
            F.col("agreement_reference")
        ).filter(F.col("agreement_reference").isNotNull()).distinct()

        agreement_df = agreement_df \
            .withColumn("agreement_id", F.abs(F.hash(F.col("agreement_reference"))).cast(T.LongType())) \
            .withColumn("agreement_key", F.col("agreement_reference")) \
            .withColumn("iot_agreement_id", F.abs(F.hash(F.col("agreement_reference"))).cast(T.LongType())) \
            .withColumn("agreement_status", F.lit("ACTIVE")) \
            .withColumn("agreement_start_date", F.lit(datetime.now(timezone.utc).date()).cast(T.DateType())) \
            .withColumn("agreement_end_date", F.lit(None).cast(T.DateType())) \
            .withColumn("currency", F.lit("SDR")) \
            .withColumn("created_at", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp()) \
            .select("agreement_id", "agreement_key", "iot_agreement_id", "agreement_reference", "agreement_status", "agreement_start_date", "agreement_end_date", "currency", "created_at", "updated_at")

        count = agreement_df.count()
        agreement_df.writeTo(f"{config.tenant}.silver.dim_agreement").append()
        logger.info(f"  ✓ Loaded {count} agreement records")
        total_count += count

        logger.info(f"✓ Successfully loaded {total_count} total dimension records")
        return total_count

    except Exception as e:
        logger.error(f"Failed to load dimensions: {e}", exc_info=True)
        raise


def main() -> None:
    config = Config()
    logger.info(f"Loading silver dimensions for tenant: {config.tenant}")

    spark = SparkSession.builder \
        .appName(f"ForecastSilverDimensions-{config.tenant}") \
        .getOrCreate()

    try:
        register_iceberg_catalog(spark, config.tenant)
        load_dimensions(spark, config)
    except Exception as e:
        logger.error(f"Failed to load silver dimensions: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
