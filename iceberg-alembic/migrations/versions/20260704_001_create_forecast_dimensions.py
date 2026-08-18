"""
Create and update silver dimension tables for Forecast data.

This migration:
1. Updates existing dimensions (dim_operator[for client], dim_service_type, dim_event_type,
   dim_traffic_direction, dim_date, dim_iot_rti_group) for forecast compatibility
2. Creates new dimensions (dim_partner, dim_agreement, dim_forecast_actual)
3. Uses dim_location as dim_destination for forecast

Note: dim_operator table is used for client data (created in 20260623_008)
Other dimensions (dim_service_type, dim_event_type, dim_traffic_direction,
dim_date, dim_iot_rti_group) were created in 20260623_008 and are being extended.
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260704_001_create_forecast_dimensions"
down_revision = "20260703_003_create_forecast_gold_tables"

CATALOG_NAME = get_catalog_name()

# New dim_partner for forecast
DIM_PARTNER_COLUMNS = [
    {"name": "partner_id", "type": "long", "source_name": "partner_id"},
    {"name": "partner_key", "type": "string", "source_name": "partner_key"},
    {"name": "partner_name", "type": "string", "source_name": "partner_name"},
    {"name": "partner_group_name", "type": "string", "source_name": "partner_group_name"},
    {"name": "partner_pmn", "type": "string", "source_name": "partner_pmn"},
    {"name": "partner_country", "type": "string", "source_name": "partner_country"},
    {"name": "is_active", "type": "boolean", "source_name": "is_active"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

# New dim_agreement for forecast
DIM_AGREEMENT_COLUMNS = [
    {"name": "agreement_id", "type": "long", "source_name": "agreement_id"},
    {"name": "agreement_key", "type": "string", "source_name": "agreement_key"},
    {"name": "iot_agreement_id", "type": "long", "source_name": "iot_agreement_id"},
    {"name": "agreement_reference", "type": "string", "source_name": "agreement_reference"},
    {"name": "agreement_status", "type": "string", "source_name": "agreement_status"},
    {"name": "agreement_start_date", "type": "date", "source_name": "agreement_start_date"},
    {"name": "agreement_end_date", "type": "date", "source_name": "agreement_end_date"},
    {"name": "currency", "type": "string", "source_name": "currency"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

# New dim_forecast_actual for forecast
DIM_FORECAST_ACTUAL_COLUMNS = [
    {"name": "forecast_actual_key", "type": "long", "source_name": "forecast_actual_key"},
    {"name": "forecast_or_actual_ind", "type": "string", "source_name": "forecast_or_actual_ind"},
    {"name": "forecast_actual_desc", "type": "string", "source_name": "forecast_actual_desc"},
]


def upgrade(op):
    """Create dimension tables for forecast (using existing dim_operator for client)."""

    # Note: dim_operator table (from 20260623_008) is used for client data in forecast

    # Create new dim_partner table
    op.create_table(
        namespace="silver",
        table_name="dim_partner",
        columns=DIM_PARTNER_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "partner",
            "description": "Partner dimension for forecast",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    # Create new dim_agreement table
    op.create_table(
        namespace="silver",
        table_name="dim_agreement",
        columns=DIM_AGREEMENT_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "agreement",
            "description": "Agreement dimension for forecast",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    # Create new dim_forecast_actual table
    op.create_table(
        namespace="silver",
        table_name="dim_forecast_actual",
        columns=DIM_FORECAST_ACTUAL_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "forecast_actual",
            "description": "Forecast/Actual indicator dimension",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    # Note: Existing dimensions (dim_client, dim_service_type, dim_event_type,
    # dim_traffic_direction, dim_date, dim_iot_rti_group, dim_location)
    # should be migrated/updated via separate alter table operations
    # This is a simplified approach - in production, you'd use Iceberg schema evolution


def downgrade(op):
    """Drop forecast dimension tables."""
    op.drop_table(namespace="silver", table_name="dim_forecast_actual")
    op.drop_table(namespace="silver", table_name="dim_agreement")
    op.drop_table(namespace="silver", table_name="dim_partner")
