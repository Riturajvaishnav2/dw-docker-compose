"""
Create the IoT Forecast bronze table (raw data ingestion layer).

Table: bronze.iot_forecast_raw
Source: VW_QLIK_IOT_FORECAST_REPORT.csv
Type: Fact table (one record per forecast/actual entry)
Partitioning: By period_start_date (daily)
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260703_001_create_forecast_bronze_table"
down_revision = "20260623_010_create_gold_traffic_tables"

CATALOG_NAME = get_catalog_name()

FORECAST_BRONZE_COLUMNS = [
    # Entity Values (values only, no IDs)
    {"name": "client_name", "type": "string", "source_name": "CLIENT_NAME"},
    {"name": "client_group_name", "type": "string", "source_name": "CLIENT_GROUP_NAME"},
    {"name": "partner_name", "type": "string", "source_name": "PARTNER_NAME"},
    {"name": "partner_group_name", "type": "string", "source_name": "PARTNER_GROUP_NAME"},

    # Classification Values
    {"name": "service_type", "type": "string", "source_name": "SERVICE_TYPE"},
    {"name": "event_type", "type": "string", "source_name": "EVENT_TYPE"},
    {"name": "traffic_direction", "type": "string", "source_name": "TRAFFIC_DIRECTION"},

    # Temporal
    {"name": "period_start_date", "type": "timestamp", "source_name": "PERIOD_START_DATE"},
    {"name": "cr_period_start_date", "type": "timestamp", "source_name": "CR_PERIOD_START_DATE"},

    # Destination and Agreement
    {"name": "destination_type", "type": "string", "source_name": "DESTINATION_TYPE"},
    {"name": "agreement_reference", "type": "string", "source_name": "AGREEMENT_REFERENCE"},

    # RTI Group
    {"name": "rti_group", "type": "string", "source_name": "RTI_GROUP"},

    # Measures - Volumes
    {"name": "traffic_volume", "type": "decimal(20,2)", "source_name": "TRAFFIC_VOLUME"},
    {"name": "charged_volume", "type": "decimal(20,2)", "source_name": "CHARGED_VOLUME"},

    # Measures - Charges (TAP)
    {"name": "tap_charge_sdr_net", "type": "decimal(20,5)", "source_name": "TAP_CHARGE_SDR_NET"},
    {"name": "tap_charge_sdr_gross", "type": "decimal(20,5)", "source_name": "TAP_CHARGE_SDR_GROSS"},

    # Measures - Charges (Discount)
    {"name": "disc_charge_sdr_net", "type": "decimal(20,5)", "source_name": "DISC_CHARGE_SDR_NET"},
    {"name": "disc_charge_sdr_gross", "type": "decimal(20,5)", "source_name": "DISC_CHARGE_SDR_GROSS"},

    # Counters
    {"name": "no_of_records", "type": "int", "source_name": "NO_OF_RECORDS"},

    # Indicators
    {"name": "forecast_or_actual_ind", "type": "string", "source_name": "FORECAST_OR_ACTUAL_IND"},

    # Audit Columns
    {"name": "_load_ts", "type": "timestamp", "source_name": "_load_ts"},
    {"name": "_load_date", "type": "date", "source_name": "_load_date"},
]


def upgrade(op):
    """Create bronze forecast table."""
    op.create_table(
        namespace="bronze",
        table_name="iot_forecast_raw",
        columns=FORECAST_BRONZE_COLUMNS,
        partition_by=["_load_date"],
        properties={
            "table_type": "iot_forecast_bronze",
            "source_model": "iot-forecast-report",
            "source_table": "VW_QLIK_IOT_FORECAST_REPORT",
            "description": "Raw IoT forecast data from Qlik reports",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    """Drop bronze forecast table."""
    op.drop_table(namespace="bronze", table_name="iot_forecast_raw")
