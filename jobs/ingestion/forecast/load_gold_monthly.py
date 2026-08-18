"""
Load gold monthly summary from silver fact data.

Creates monthly aggregated view for trend analysis and budget tracking.
"""

import logging
import os
import sys
from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/airflow")
from jobs.common.spark_catalog import register_iceberg_catalog

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def env_or_default(name: str, default: str) -> str:
    return os.getenv(name) or default


@dataclass
class Config:
    tenant: str = field(default_factory=lambda: env_or_default("ICEBERG_NAMESPACE", "ee").lower())


def load_gold_monthly() -> bool:
    """Load gold monthly summary table."""
    config = Config()

    spark = SparkSession.builder.appName("ForecastGoldMonthly").getOrCreate()

    try:
        register_iceberg_catalog(spark, config.tenant)
        logger.info(f"Loading silver fact table for tenant {config.tenant}")
        fact_df = spark.table(f"{config.tenant}.silver.fact_iot_forecast")

        logger.info("Building forecast_summary_monthly...")

        # Extract year-month from period_start_date
        monthly_summary = fact_df \
            .withColumn("year_month", F.date_format(F.trunc(F.col("period_start_date"), "month"), "yyyy-MM")) \
            .groupBy("year_month", "client_master_entity_id", "forecast_or_actual_ind").agg(
                F.sum("traffic_volume").alias("total_traffic_volume"),
                F.sum("charged_volume").alias("total_charged_volume"),
                F.sum(F.col("tap_charge_sdr_net") + F.col("disc_charge_sdr_net")).alias("total_charges_net"),
                F.sum("no_of_records").alias("record_count")
            ).select(
                F.col("year_month"),
                F.col("client_master_entity_id").cast("string").alias("client_key"),
                (F.col("forecast_or_actual_ind") == "F").alias("is_forecast"),
                F.col("total_traffic_volume").cast("decimal(20,2)"),
                F.col("total_charged_volume").cast("decimal(20,2)"),
                F.col("total_charges_net").cast("decimal(20,5)").alias("total_charges_net"),
                F.col("record_count").cast("int"),
                F.current_timestamp().alias("created_at")
            )

        monthly_summary.writeTo(f"{config.tenant}.gold.forecast_summary_monthly").append()
        logger.info(f"✓ Loaded {monthly_summary.count()} records to forecast_summary_monthly")

        logger.info("Successfully loaded gold monthly summary")
        return True

    except Exception as e:
        logger.error(f"Failed to load gold monthly summary: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    load_gold_monthly()
