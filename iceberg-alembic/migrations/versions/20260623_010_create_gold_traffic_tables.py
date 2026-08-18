"""
Create gold layer traffic tables for reporting and analytics.

Gold tables are denormalized/aggregated, business-ready for:
- Reporting dashboards
- Billing analysis
- Roaming analytics
- IoT traffic insights

Tables created:
- gold.imsi_level_traffic_daily - one row per client/partner/direction/date/call_type/IMSI/APN/service grain
- gold.client_partner_traffic_monthly - monthly client-partner summary aggregated
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260623_010_create_gold_traffic_tables"
down_revision = "20260623_009_create_silver_fact_imsi_level_traffic"

CATALOG_NAME = get_catalog_name()

GOLD_IMSI_DAILY_COLUMNS = [
    # Dimension attributes (denormalized from silver)
    {"name": "client_pmn", "type": "string", "source_name": "client_pmn"},
    {"name": "partner_pmn", "type": "string", "source_name": "partner_pmn"},
    {"name": "roaming_partner_country", "type": "string", "source_name": "roaming_partner_country"},
    {"name": "traffic_direction", "type": "string", "source_name": "traffic_direction"},
    {"name": "call_date", "type": "date", "source_name": "call_date"},
    {"name": "call_month", "type": "string", "source_name": "call_month"},
    {"name": "year", "type": "int", "source_name": "year"},
    {"name": "month", "type": "int", "source_name": "month"},
    {"name": "day", "type": "int", "source_name": "day"},
    {"name": "call_type", "type": "string", "source_name": "call_type"},
    {"name": "call_type_level_2", "type": "string", "source_name": "call_type_level_2"},
    {"name": "imsi", "type": "string", "source_name": "imsi"},
    {"name": "roamer_indicator", "type": "string", "source_name": "roamer_indicator"},
    {"name": "apn", "type": "string", "source_name": "apn"},
    {"name": "service_type_id", "type": "string", "source_name": "service_type_id"},
    {"name": "event_type_id", "type": "string", "source_name": "event_type_id"},
    {"name": "rat_type", "type": "string", "source_name": "rat_type"},
    {"name": "tac_number", "type": "string", "source_name": "tac_number"},
    {"name": "iot_rti_group_id", "type": "string", "source_name": "iot_rti_group_id"},
    {"name": "destination", "type": "string", "source_name": "destination"},
    {"name": "destination_category", "type": "string", "source_name": "destination_category"},
    {"name": "is_camel", "type": "string", "source_name": "is_camel"},
    # Aggregated measures
    {"name": "total_duration", "type": "decimal(18,6)", "source_name": "total_duration"},
    {"name": "total_volume", "type": "decimal(18,6)", "source_name": "total_volume"},
    {"name": "total_event_count", "type": "long", "source_name": "total_event_count"},
    {"name": "total_charge_sdr_net", "type": "decimal(18,6)", "source_name": "total_charge_sdr_net"},
    {"name": "total_charge_sdr_gross", "type": "decimal(18,6)", "source_name": "total_charge_sdr_gross"},
    # Audit columns
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]


GOLD_CLIENT_PARTNER_MONTHLY_COLUMNS = [
    # Dimension attributes (aggregated)
    {"name": "client_pmn", "type": "string", "source_name": "client_pmn"},
    {"name": "partner_pmn", "type": "string", "source_name": "partner_pmn"},
    {"name": "roaming_partner_country", "type": "string", "source_name": "roaming_partner_country"},
    {"name": "traffic_direction", "type": "string", "source_name": "traffic_direction"},
    {"name": "call_month", "type": "string", "source_name": "call_month"},
    {"name": "year", "type": "int", "source_name": "year"},
    {"name": "month", "type": "int", "source_name": "month"},
    {"name": "call_type", "type": "string", "source_name": "call_type"},
    {"name": "call_type_level_2", "type": "string", "source_name": "call_type_level_2"},
    {"name": "service_type_id", "type": "string", "source_name": "service_type_id"},
    {"name": "event_type_id", "type": "string", "source_name": "event_type_id"},
    # Aggregated measures
    {"name": "total_duration", "type": "decimal(18,6)", "source_name": "total_duration"},
    {"name": "total_volume", "type": "decimal(18,6)", "source_name": "total_volume"},
    {"name": "total_event_count", "type": "long", "source_name": "total_event_count"},
    {"name": "total_charge_sdr_net", "type": "decimal(18,6)", "source_name": "total_charge_sdr_net"},
    {"name": "total_charge_sdr_gross", "type": "decimal(18,6)", "source_name": "total_charge_sdr_gross"},
    # Distinct counts
    {"name": "distinct_imsi_count", "type": "long", "source_name": "distinct_imsi_count"},
    {"name": "distinct_apn_count", "type": "long", "source_name": "distinct_apn_count"},
    # Audit columns
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]


def upgrade(op):
    """Create gold layer tables."""
    op.create_table(
        namespace="gold",
        table_name="imsi_level_traffic_daily",
        columns=GOLD_IMSI_DAILY_COLUMNS,
        partition_by=["call_date"],
        properties={
            "table_type": "gold",
            "grain": "client_partner_direction_date_call_type_imsi_apn_service",
            "purpose": "reporting_dashboards_billing_roaming",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"gold",
        table_name="client_partner_traffic_monthly",
        columns=GOLD_CLIENT_PARTNER_MONTHLY_COLUMNS,
        partition_by=["call_month"],
        properties={
            "table_type": "gold",
            "grain": "client_partner_direction_month_call_type_service",
            "purpose": "monthly_summary_billing_analytics",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    """Drop gold layer tables."""
    op.drop_table(namespace="gold", table_name="client_partner_traffic_monthly")
    op.drop_table(namespace="gold", table_name="imsi_level_traffic_daily")
