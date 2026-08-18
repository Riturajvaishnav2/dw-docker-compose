"""
Load gold daily aggregations from silver fact data.

Creates aggregated views for daily reporting:
- forecast_by_client_date: Total metrics grouped by client and date
- forecast_by_service_direction: Total metrics grouped by service type, direction, and date
- forecast_vs_actual_comparison: Forecast vs actual variance analysis
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


def load_gold_daily() -> bool:
    """Load gold daily aggregation tables."""
    config = Config()

    spark = SparkSession.builder.appName("ForecastGoldDaily").getOrCreate()

    try:
        register_iceberg_catalog(spark, config.tenant)
        logger.info(f"Loading silver fact table for tenant {config.tenant}")
        fact_df = spark.table(f"{config.tenant}.silver.fact_iot_forecast")
        logger.info(f"Loaded {fact_df.count()} records from silver fact table")

        # Aggregation 1: By Client and Date
        logger.info("Building forecast_by_client_date...")
        client_date_agg = fact_df.groupBy("client_master_entity_id", "period_start_date", "forecast_or_actual_ind").agg(
            F.sum("traffic_volume").alias("total_traffic_volume"),
            F.sum("charged_volume").alias("total_charged_volume"),
            F.sum("tap_charge_sdr_net").alias("total_tap_charge_net"),
            F.sum("tap_charge_sdr_gross").alias("total_tap_charge_gross"),
            F.sum("disc_charge_sdr_net").alias("total_discount_net"),
            F.sum("disc_charge_sdr_gross").alias("total_discount_gross"),
            F.sum("no_of_records").alias("record_count")
        ).select(
            F.col("client_master_entity_id").cast("string").alias("client_key"),
            F.date_format(F.col("period_start_date"), "yyyy-MM-dd").alias("date_key"),
            (F.col("forecast_or_actual_ind") == "F").alias("is_forecast"),
            F.col("total_traffic_volume").cast("decimal(20,2)"),
            F.col("total_charged_volume").cast("decimal(20,2)"),
            F.col("total_tap_charge_net").cast("decimal(20,5)").alias("total_tap_charge_net"),
            F.col("total_tap_charge_gross").cast("decimal(20,5)").alias("total_tap_charge_gross"),
            F.col("total_discount_net").cast("decimal(20,5)").alias("total_discount_net"),
            F.col("total_discount_gross").cast("decimal(20,5)").alias("total_discount_gross"),
            F.col("record_count").cast("int"),
            F.current_timestamp().alias("created_at")
        )

        client_date_agg.writeTo(f"{config.tenant}.gold.forecast_by_client_date").append()
        logger.info(f"✓ Loaded {client_date_agg.count()} records to forecast_by_client_date")

        # Aggregation 2: By Service Type and Direction
        logger.info("Building forecast_by_service_direction...")
        service_dir_agg = fact_df.groupBy("iot_service_type_id", "traffic_direction", "period_start_date", "forecast_or_actual_ind").agg(
            F.sum("traffic_volume").alias("total_traffic_volume"),
            F.sum("charged_volume").alias("total_charged_volume"),
            F.sum("tap_charge_sdr_net").alias("total_tap_charge_net"),
            F.sum("tap_charge_sdr_gross").alias("total_tap_charge_gross"),
            F.sum("disc_charge_sdr_net").alias("total_discount_net"),
            F.sum("disc_charge_sdr_gross").alias("total_discount_gross"),
            F.sum("no_of_records").alias("record_count")
        ).select(
            F.col("iot_service_type_id").cast("string").alias("service_type_key"),
            F.col("traffic_direction").alias("direction_key"),
            F.date_format(F.col("period_start_date"), "yyyy-MM-dd").alias("date_key"),
            (F.col("forecast_or_actual_ind") == "F").alias("is_forecast"),
            F.col("total_traffic_volume").cast("decimal(20,2)"),
            F.col("total_charged_volume").cast("decimal(20,2)"),
            F.col("total_tap_charge_net").cast("decimal(20,5)").alias("total_tap_charge_net"),
            F.col("total_tap_charge_gross").cast("decimal(20,5)").alias("total_tap_charge_gross"),
            F.col("total_discount_net").cast("decimal(20,5)").alias("total_discount_net"),
            F.col("total_discount_gross").cast("decimal(20,5)").alias("total_discount_gross"),
            F.col("record_count").cast("int"),
            F.current_timestamp().alias("created_at")
        )

        service_dir_agg.writeTo(f"{config.tenant}.gold.forecast_by_service_direction").append()
        logger.info(f"✓ Loaded {service_dir_agg.count()} records to forecast_by_service_direction")

        # Aggregation 3: Forecast vs Actual Comparison
        logger.info("Building forecast_vs_actual_comparison...")

        # Split forecast and actual (F=Forecast, A=Actual)
        forecast_df = fact_df.filter(F.col("forecast_or_actual_ind") == "F") \
            .groupBy("client_master_entity_id", "iot_service_type_id", "traffic_direction", "period_start_date").agg(
                F.sum("traffic_volume").alias("forecast_traffic_volume"),
                F.sum("tap_charge_sdr_net").alias("forecast_charge_net")
            )

        actual_df = fact_df.filter(F.col("forecast_or_actual_ind") == "A") \
            .groupBy("client_master_entity_id", "iot_service_type_id", "traffic_direction", "period_start_date").agg(
                F.sum("traffic_volume").alias("actual_traffic_volume"),
                F.sum("tap_charge_sdr_net").alias("actual_charge_net")
            )

        # Join and calculate variance
        comparison_df = forecast_df.join(actual_df,
            on=["client_master_entity_id", "iot_service_type_id", "traffic_direction", "period_start_date"],
            how="full_outer"
        ).fillna(0.0).select(
            F.col("client_master_entity_id").cast("string").alias("client_key"),
            F.col("iot_service_type_id").cast("string").alias("service_type_key"),
            F.col("traffic_direction").alias("direction_key"),
            F.date_format(F.col("period_start_date"), "yyyy-MM-dd").alias("date_key"),
            F.col("forecast_traffic_volume").cast("decimal(20,2)").alias("forecast_traffic_volume"),
            F.col("forecast_charge_net").cast("decimal(20,5)").alias("forecast_charge_net"),
            F.col("actual_traffic_volume").cast("decimal(20,2)").alias("actual_traffic_volume"),
            F.col("actual_charge_net").cast("decimal(20,5)").alias("actual_charge_net"),
            (F.col("forecast_traffic_volume") - F.col("actual_traffic_volume")).cast("decimal(20,2)").alias("variance_traffic"),
            (((F.col("forecast_traffic_volume") - F.col("actual_traffic_volume")) / F.col("actual_traffic_volume")) * 100)
                .cast("decimal(10,2)").alias("variance_pct"),
            (F.col("forecast_charge_net") - F.col("actual_charge_net")).cast("decimal(20,5)").alias("variance_charge"),
            F.current_timestamp().alias("created_at")
        )

        comparison_df.writeTo(f"{config.tenant}.gold.forecast_vs_actual_comparison").append()
        logger.info(f"✓ Loaded {comparison_df.count()} records to forecast_vs_actual_comparison")

        logger.info("Successfully loaded all gold daily aggregations")
        return True

    except Exception as e:
        logger.error(f"Failed to load gold daily aggregations: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    load_gold_daily()
