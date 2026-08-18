"""
Create IoT Forecast gold tables (aggregated views for reporting).

Tables:
  - forecast_by_client_date (daily totals by client)
  - forecast_by_service_direction (daily totals by service/direction)
  - forecast_vs_actual_comparison (variance analysis)
  - forecast_summary_monthly (monthly trends)

Type: Aggregated reporting tables
Partitioning: By date keys
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260703_003_create_forecast_gold_tables"
down_revision = "20260703_002_create_forecast_silver_tables"

CATALOG_NAME = get_catalog_name()


def upgrade(op):
    """Create gold aggregation tables."""

    # ===== AGGREGATION TABLES =====

    # forecast_by_client_date
    op.create_table(
        namespace="gold",
        table_name="forecast_by_client_date",
        columns=[
            {"name": "client_key", "type": "string", "source_name": "client_key"},
            {"name": "date_key", "type": "string", "source_name": "date_key"},
            {"name": "is_forecast", "type": "boolean", "source_name": "is_forecast"},

            # Measures
            {"name": "total_traffic_volume", "type": "decimal(20,2)", "source_name": "total_traffic_volume"},
            {"name": "total_charged_volume", "type": "decimal(20,2)", "source_name": "total_charged_volume"},
            {"name": "total_tap_charge_net", "type": "decimal(20,5)", "source_name": "total_tap_charge_net"},
            {"name": "total_tap_charge_gross", "type": "decimal(20,5)", "source_name": "total_tap_charge_gross"},
            {"name": "total_discount_net", "type": "decimal(20,5)", "source_name": "total_discount_net"},
            {"name": "total_discount_gross", "type": "decimal(20,5)", "source_name": "total_discount_gross"},
            {"name": "record_count", "type": "int", "source_name": "record_count"},

            # Audit
            {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
        ],
        partition_by=["date_key"],
        properties={
            "table_type": "aggregation",
            "aggregation_name": "forecast_by_client_date",
            "source_table": "silver.fact_iot_forecast",
            "grain": "client,date,forecast_actual",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    # forecast_by_service_direction
    op.create_table(
        namespace="gold",
        table_name="forecast_by_service_direction",
        columns=[
            {"name": "service_type_key", "type": "string", "source_name": "service_type_key"},
            {"name": "direction_key", "type": "string", "source_name": "direction_key"},
            {"name": "date_key", "type": "string", "source_name": "date_key"},
            {"name": "is_forecast", "type": "boolean", "source_name": "is_forecast"},

            # Measures
            {"name": "total_traffic_volume", "type": "decimal(20,2)", "source_name": "total_traffic_volume"},
            {"name": "total_charged_volume", "type": "decimal(20,2)", "source_name": "total_charged_volume"},
            {"name": "total_tap_charge_net", "type": "decimal(20,5)", "source_name": "total_tap_charge_net"},
            {"name": "total_tap_charge_gross", "type": "decimal(20,5)", "source_name": "total_tap_charge_gross"},
            {"name": "total_discount_net", "type": "decimal(20,5)", "source_name": "total_discount_net"},
            {"name": "total_discount_gross", "type": "decimal(20,5)", "source_name": "total_discount_gross"},
            {"name": "record_count", "type": "int", "source_name": "record_count"},

            # Audit
            {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
        ],
        partition_by=["date_key"],
        properties={
            "table_type": "aggregation",
            "aggregation_name": "forecast_by_service_direction",
            "source_table": "silver.fact_iot_forecast",
            "grain": "service_type,direction,date,forecast_actual",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    # forecast_vs_actual_comparison
    op.create_table(
        namespace="gold",
        table_name="forecast_vs_actual_comparison",
        columns=[
            {"name": "client_key", "type": "string", "source_name": "client_key"},
            {"name": "service_type_key", "type": "string", "source_name": "service_type_key"},
            {"name": "direction_key", "type": "string", "source_name": "direction_key"},
            {"name": "date_key", "type": "string", "source_name": "date_key"},

            # Forecast Metrics
            {"name": "forecast_traffic_volume", "type": "decimal(20,2)", "source_name": "forecast_traffic_volume"},
            {"name": "forecast_charge_net", "type": "decimal(20,5)", "source_name": "forecast_charge_net"},

            # Actual Metrics
            {"name": "actual_traffic_volume", "type": "decimal(20,2)", "source_name": "actual_traffic_volume"},
            {"name": "actual_charge_net", "type": "decimal(20,5)", "source_name": "actual_charge_net"},

            # Variance Metrics
            {"name": "variance_traffic", "type": "decimal(20,2)", "source_name": "variance_traffic"},
            {"name": "variance_pct", "type": "decimal(10,2)", "source_name": "variance_pct"},
            {"name": "variance_charge", "type": "decimal(20,5)", "source_name": "variance_charge"},

            # Audit
            {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
        ],
        partition_by=["date_key"],
        properties={
            "table_type": "analysis",
            "analysis_name": "forecast_vs_actual_comparison",
            "source_table": "silver.fact_iot_forecast",
            "grain": "client,service_type,direction,date",
            "analysis_type": "variance",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    # forecast_summary_monthly
    op.create_table(
        namespace="gold",
        table_name="forecast_summary_monthly",
        columns=[
            {"name": "year_month", "type": "string", "source_name": "year_month"},  # YYYYMM
            {"name": "client_key", "type": "string", "source_name": "client_key"},
            {"name": "is_forecast", "type": "boolean", "source_name": "is_forecast"},

            # Measures
            {"name": "total_traffic_volume", "type": "decimal(20,2)", "source_name": "total_traffic_volume"},
            {"name": "total_charged_volume", "type": "decimal(20,2)", "source_name": "total_charged_volume"},
            {"name": "total_charges_net", "type": "decimal(20,5)", "source_name": "total_charges_net"},
            {"name": "record_count", "type": "int", "source_name": "record_count"},

            # Audit
            {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
        ],
        properties={
            "table_type": "summary",
            "summary_name": "forecast_summary_monthly",
            "source_table": "silver.fact_iot_forecast",
            "grain": "client,year_month,forecast_actual",
            "aggregation_period": "monthly",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    """Drop gold forecast tables."""
    tables = [
        "forecast_by_client_date",
        "forecast_by_service_direction",
        "forecast_vs_actual_comparison",
        "forecast_summary_monthly",
    ]
    for table in tables:
        op.drop_table(namespace="gold", table_name=table)
