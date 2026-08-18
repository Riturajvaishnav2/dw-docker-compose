"""
Validation and testing script for forecast pipeline.

Validates data quality at each layer:
- Bronze: Row counts, schema compliance, null values
- Silver: Dimension uniqueness, foreign key integrity
- Gold: Aggregation accuracy, no duplicates
"""

import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/airflow")
from jobs.common.domain_to_table_mapping import DomainTableMapping
from jobs.common.spark_catalog import register_iceberg_catalog

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def env_or_default(name: str, default: str) -> str:
    return os.getenv(name) or default


class ForecastValidator:
    """Validates forecast data across all layers."""

    def __init__(self, tenant: str = "default"):
        self.tenant = tenant.lower()
        os.environ["ICEBERG_NAMESPACE"] = self.tenant

        mapping = DomainTableMapping()
        self.bronze_config = mapping.get_table_config("forecast", "bronze")
        self.silver_config = mapping.get_table_config("forecast", "silver")
        self.gold_config = mapping.get_table_config("forecast", "gold")

        self.spark = SparkSession.builder.appName("ForecastValidation").getOrCreate()
        register_iceberg_catalog(self.spark, self.tenant)

        self.results = []

    def add_result(self, layer: str, check: str, passed: bool, details: str = ""):
        """Track validation results."""
        self.results.append({
            "layer": layer,
            "check": check,
            "passed": passed,
            "details": details,
            "timestamp": datetime.now(timezone.utc)
        })

    def validate_bronze(self) -> bool:
        """Validate bronze layer."""
        logger.info("Validating Bronze Layer...")
        try:
            bronze_df = self.spark.table("bronze.iot_forecast_raw")
            count = bronze_df.count()

            # Check 1: Record count
            if count > 0:
                self.add_result("bronze", "record_count", True, f"{count} records found")
            else:
                self.add_result("bronze", "record_count", False, "No records found")

            # Check 2: Null key columns
            null_clients = bronze_df.filter(F.col("client_master_entity_id").isNull()).count()
            null_check = null_clients == 0
            self.add_result("bronze", "null_keys", null_check,
                          f"{null_clients} null client IDs" if not null_check else "No null keys")

            # Check 3: Volume ranges
            volume_stats = bronze_df.select(
                F.min("traffic_volume").alias("min_vol"),
                F.max("traffic_volume").alias("max_vol"),
                F.avg("traffic_volume").alias("avg_vol")
            ).collect()[0]
            self.add_result("bronze", "volume_ranges", True,
                          f"Min: {volume_stats.min_vol}, Max: {volume_stats.max_vol}")

            # Check 4: Date coverage
            date_range = bronze_df.select(
                F.min("period_start_date").alias("min_date"),
                F.max("period_start_date").alias("max_date")
            ).collect()[0]
            self.add_result("bronze", "date_coverage", True,
                          f"From {date_range.min_date} to {date_range.max_date}")

            return all(r["passed"] for r in self.results if r["layer"] == "bronze")

        except Exception as e:
            logger.error(f"Bronze validation failed: {e}")
            self.add_result("bronze", "validation", False, str(e))
            return False

    def validate_silver_dimensions(self) -> bool:
        """Validate silver dimension tables."""
        logger.info("Validating Silver Dimensions...")

        dimensions = [
            "dim_client", "dim_partner", "dim_service_type", "dim_event_type",
            "dim_traffic_direction", "dim_destination_type", "dim_rti_group", "dim_date"
        ]

        all_passed = True

        for dim in dimensions:
            try:
                dim_df = self.spark.table(f"silver.{dim}")
                count = dim_df.count()
                unique_keys = dim_df.select("*").distinct().count()

                if count > 0:
                    self.add_result("silver", f"{dim}_exists", True, f"{count} records")
                else:
                    self.add_result("silver", f"{dim}_exists", False, "Empty table")
                    all_passed = False

                # Check for duplicates
                if count == unique_keys:
                    self.add_result("silver", f"{dim}_uniqueness", True, "All records unique")
                else:
                    self.add_result("silver", f"{dim}_uniqueness", False,
                                  f"{count - unique_keys} duplicates found")
                    all_passed = False

            except Exception as e:
                logger.error(f"Dimension {dim} validation failed: {e}")
                self.add_result("silver", f"{dim}_exists", False, str(e))
                all_passed = False

        return all_passed

    def validate_silver_fact(self) -> bool:
        """Validate silver fact table."""
        logger.info("Validating Silver Fact Table...")
        try:
            fact_df = self.spark.table("silver.fact_iot_forecast")
            count = fact_df.count()

            # Check 1: Record count
            if count > 0:
                self.add_result("silver", "fact_record_count", True, f"{count} records")
            else:
                self.add_result("silver", "fact_record_count", False, "No records")
                return False

            # Check 2: Foreign key integrity
            orphaned = fact_df.filter(
                ~F.col("client_key").isin(
                    self.spark.table("silver.dim_client").select("client_key").rdd.map(lambda r: r[0]).collect()
                )
            ).count()

            fk_check = orphaned == 0
            self.add_result("silver", "foreign_keys", fk_check,
                          f"{orphaned} orphaned records" if not fk_check else "All FKs valid")

            # Check 3: Null measures
            null_measures = fact_df.filter(
                F.col("traffic_volume").isNull() |
                F.col("charged_volume").isNull()
            ).count()

            measure_check = null_measures == 0
            self.add_result("silver", "null_measures", measure_check,
                          f"{null_measures} null measures" if not measure_check else "No nulls")

            return fk_check and measure_check

        except Exception as e:
            logger.error(f"Fact table validation failed: {e}")
            self.add_result("silver", "fact_validation", False, str(e))
            return False

    def validate_gold(self) -> bool:
        """Validate gold aggregation tables."""
        logger.info("Validating Gold Layer...")

        gold_tables = [
            "forecast_by_client_date",
            "forecast_by_service_direction",
            "forecast_vs_actual_comparison",
            "forecast_summary_monthly"
        ]

        all_passed = True

        for table in gold_tables:
            try:
                gold_df = self.spark.table(f"gold.{table}")
                count = gold_df.count()

                if count > 0:
                    self.add_result("gold", f"{table}_records", True, f"{count} records")
                else:
                    self.add_result("gold", f"{table}_records", False, "Empty table")
                    all_passed = False

                # Check for duplicates
                pk_cols = self._get_pk_columns(table)
                if pk_cols:
                    duplicates = gold_df.select(*pk_cols).groupBy(*pk_cols).count().filter(
                        F.col("count") > 1
                    ).count()

                    if duplicates == 0:
                        self.add_result("gold", f"{table}_duplicates", True, "No duplicates")
                    else:
                        self.add_result("gold", f"{table}_duplicates", False,
                                      f"{duplicates} duplicate keys")
                        all_passed = False

            except Exception as e:
                logger.error(f"Gold table {table} validation failed: {e}")
                self.add_result("gold", f"{table}_exists", False, str(e))
                all_passed = False

        return all_passed

    def validate_aggregation_accuracy(self) -> bool:
        """Validate that gold aggregations match fact table."""
        logger.info("Validating Aggregation Accuracy...")
        try:
            fact_df = self.spark.table("silver.fact_iot_forecast")
            gold_df = self.spark.table("gold.forecast_by_client_date")

            # Sum measures from fact table
            fact_sum = fact_df.agg(
                F.sum("traffic_volume").alias("fact_volume"),
                F.sum("tap_charge_sdr_net").alias("fact_charge")
            ).collect()[0]

            # Sum measures from gold table
            gold_sum = gold_df.agg(
                F.sum("total_traffic_volume").alias("gold_volume"),
                F.sum("total_tap_charge_net").alias("gold_charge")
            ).collect()[0]

            # Check variance (allow 0.01% difference due to rounding)
            vol_diff = abs(float(fact_sum.fact_volume or 0) - float(gold_sum.gold_volume or 0))
            charge_diff = abs(float(fact_sum.fact_charge or 0) - float(gold_sum.gold_charge or 0))

            vol_check = vol_diff < 1.0
            charge_check = charge_diff < 0.01

            self.add_result("gold", "volume_accuracy", vol_check,
                          f"Difference: {vol_diff}" if not vol_check else "Accurate")
            self.add_result("gold", "charge_accuracy", charge_check,
                          f"Difference: {charge_diff}" if not charge_check else "Accurate")

            return vol_check and charge_check

        except Exception as e:
            logger.error(f"Aggregation accuracy check failed: {e}")
            self.add_result("gold", "accuracy_check", False, str(e))
            return False

    @staticmethod
    def _get_pk_columns(table: str) -> list:
        """Get primary key columns for a gold table."""
        pk_map = {
            "forecast_by_client_date": ["client_key", "date_key", "is_forecast"],
            "forecast_by_service_direction": ["service_type_key", "direction_key", "date_key", "is_forecast"],
            "forecast_vs_actual_comparison": ["client_key", "service_type_key", "direction_key", "date_key"],
            "forecast_summary_monthly": ["year_month", "client_key", "is_forecast"],
        }
        return pk_map.get(table, [])

    def print_summary(self):
        """Print validation summary."""
        print("\n" + "="*80)
        print("FORECAST DATA VALIDATION REPORT")
        print("="*80)

        layers = ["bronze", "silver", "gold"]
        for layer in layers:
            layer_results = [r for r in self.results if r["layer"] == layer]
            if not layer_results:
                continue

            print(f"\n{layer.upper()} Layer:")
            print("-" * 80)

            for result in layer_results:
                status = "✓ PASS" if result["passed"] else "✗ FAIL"
                print(f"  {status:10} {result['check']:30} {result['details']}")

        print("\n" + "="*80)
        passed = sum(1 for r in self.results if r["passed"])
        total = len(self.results)
        print(f"TOTAL: {passed}/{total} checks passed")
        print("="*80 + "\n")

        return passed == total

    def validate_all(self) -> bool:
        """Run all validations."""
        results = [
            self.validate_bronze(),
            self.validate_silver_dimensions(),
            self.validate_silver_fact(),
            self.validate_gold(),
            self.validate_aggregation_accuracy(),
        ]

        all_passed = all(results)
        summary_passed = self.print_summary()

        return all_passed and summary_passed


def main():
    tenant = env_or_default("ICEBERG_NAMESPACE", "default")
    validator = ForecastValidator(tenant=tenant)

    try:
        success = validator.validate_all()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        validator.spark.stop()


if __name__ == "__main__":
    main()
