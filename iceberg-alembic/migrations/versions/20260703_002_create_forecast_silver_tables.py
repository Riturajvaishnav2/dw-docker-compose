"""
Create IoT Forecast silver fact table (uses existing dimension tables).

Fact Table:
  - fact_iot_forecast: Conformed fact table with dimension foreign keys

Dimensions used (created by other migrations):
  EXISTING (from 20260623_008_create_silver_dimension_tables.py):
  - dim_operator (used for client data: pmn -> client_name, pmn_country -> client_group_name)
  - dim_service_type
  - dim_event_type
  - dim_traffic_direction
  - dim_iot_rti_group
  - dim_date
  - dim_location (used as destination)

  NEW (from 20260704_001_create_forecast_dimensions.py):
  - dim_partner
  - dim_agreement
  - dim_forecast_actual
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260703_002_create_forecast_silver_tables"
down_revision = "20260703_001_create_forecast_bronze_table"

CATALOG_NAME = get_catalog_name()


def upgrade(op):
    """Create silver fact table (dimensions are managed separately)."""

    # ===== FACT TABLE =====

    # fact_iot_forecast
    op.create_table(
        namespace="silver",
        table_name="fact_iot_forecast",
        columns=[
            # Entity Foreign Key IDs
            {"name": "client_master_entity_id", "type": "long", "source_name": "client_master_entity_id"},
            {"name": "client_main_master_entity_id", "type": "long", "source_name": "client_main_master_entity_id"},
            {"name": "partner_master_entity_id", "type": "long", "source_name": "partner_master_entity_id"},
            {"name": "partner_main_master_entity_id", "type": "long", "source_name": "partner_main_master_entity_id"},

            # Classification IDs
            {"name": "iot_service_type_id", "type": "long", "source_name": "iot_service_type_id"},
            {"name": "iot_event_type_id", "type": "long", "source_name": "iot_event_type_id"},
            {"name": "destination_type_id", "type": "long", "source_name": "destination_type_id"},
            {"name": "iot_agreement_id", "type": "long", "source_name": "iot_agreement_id"},
            {"name": "iot_rti_group_id", "type": "long", "source_name": "iot_rti_group_id"},

            # Codes and Dates
            {"name": "traffic_direction", "type": "string", "source_name": "traffic_direction"},
            {"name": "period_start_date", "type": "date", "source_name": "period_start_date"},
            {"name": "cr_period_start_date", "type": "date", "source_name": "cr_period_start_date"},
            {"name": "forecast_or_actual_ind", "type": "string", "source_name": "forecast_or_actual_ind"},

            # Measures - Volumes and Charges (DECIMAL(20,6))
            {"name": "traffic_volume", "type": "decimal(20,6)", "source_name": "traffic_volume"},
            {"name": "charged_volume", "type": "decimal(20,6)", "source_name": "charged_volume"},
            {"name": "tap_charge_sdr_net", "type": "decimal(20,6)", "source_name": "tap_charge_sdr_net"},
            {"name": "tap_charge_sdr_gross", "type": "decimal(20,6)", "source_name": "tap_charge_sdr_gross"},
            {"name": "disc_charge_sdr_net", "type": "decimal(20,6)", "source_name": "disc_charge_sdr_net"},
            {"name": "disc_charge_sdr_gross", "type": "decimal(20,6)", "source_name": "disc_charge_sdr_gross"},

            # Counters and Hashes
            {"name": "no_of_records", "type": "long", "source_name": "no_of_records"},
            {"name": "record_hash", "type": "string", "source_name": "record_hash"},
            {"name": "ingestion_id", "type": "string", "source_name": "ingestion_id"},

            # Audit
            {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
        ],
        partition_by=["period_start_date"],
        properties={
            "table_type": "fact",
            "fact_name": "iot_forecast",
            "source_model": "iot-forecast-report",
            "grain": "client,partner,service_type,event_type,traffic_direction,destination,rti_group,period_start_date",
            "unique_key": "client_master_entity_id,client_main_master_entity_id,partner_master_entity_id,partner_main_master_entity_id,iot_service_type_id,iot_event_type_id,traffic_direction,period_start_date,destination_type_id,iot_agreement_id,traffic_volume,charged_volume,tap_charge_sdr_net,tap_charge_sdr_gross,disc_charge_sdr_net,disc_charge_sdr_gross,no_of_records,iot_rti_group_id,forecast_or_actual_ind,cr_period_start_date",
            "description": "IoT Forecast fact table with dimension IDs and measures",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    """Drop silver forecast fact table (dimensions managed separately)."""
    op.drop_table(namespace="silver", table_name="fact_iot_forecast")
