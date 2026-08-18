"""
Create the IMSI-level traffic bronze table.
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260604_005_create_imsi_level_traffic_bronze_table"
down_revision = None

CATALOG_NAME = get_catalog_name()

TRAFFIC_AUDIT_COLUMNS = [
    {"name": "_source_bucket", "type": "string", "source_name": "_source_bucket"},
    {"name": "_source_key", "type": "string", "source_name": "_source_key"},
    {"name": "_source_file_name", "type": "string", "source_name": "_source_file_name"},
    {"name": "_source_format", "type": "string", "source_name": "_source_format"},
    {"name": "_domain", "type": "string", "source_name": "_domain"},
    {"name": "_layer", "type": "string", "source_name": "_layer"},
    {"name": "_ingested_at", "type": "timestamp", "source_name": "_ingested_at"},
    {"name": "_ingest_date", "type": "date", "source_name": "_ingest_date"},
]


TRAFFIC_BRONZE_COLUMNS = [
    # Core IMSI-level traffic fields
    {"name": "client_pmn", "type": "string", "source_name": "client_pmn"},
    {"name": "partner_pmn", "type": "string", "source_name": "partner_pmn"},
    {"name": "traffic_direction", "type": "string", "source_name": "traffic_direction"},
    {"name": "call_date", "type": "string", "source_name": "call_date"},
    {"name": "call_type", "type": "string", "source_name": "call_type"},
    {"name": "imsi", "type": "string", "source_name": "imsi"},
    {"name": "apn", "type": "string", "source_name": "apn"},
    {"name": "duration", "type": "string", "source_name": "duration"},
    {"name": "volume", "type": "string", "source_name": "volume"},
    {"name": "event_count", "type": "string", "source_name": "event_count"},
    {"name": "roamer_indicator", "type": "string", "source_name": "roamer_indicator"},
    # IoT Traffic fields
    {"name": "total_charge_sdr_net", "type": "string", "source_name": "total_charge_sdr_net"},
    {"name": "total_charge_sdr_gross", "type": "string", "source_name": "total_charge_sdr_gross"},
    {"name": "iot_rti_group_id", "type": "string", "source_name": "iot_rti_group_id"},
    {"name": "service_type_id", "type": "string", "source_name": "service_type_id"},
    {"name": "event_type_id", "type": "string", "source_name": "event_type_id"},
    {"name": "call_type_level_2", "type": "string", "source_name": "call_type_level_2"},
    {"name": "rat_type", "type": "string", "source_name": "rat_type"},
    {"name": "tac_number", "type": "string", "source_name": "tac_number"},
    # Roaming Traffic fields
    {"name": "roaming_partner_country", "type": "string", "source_name": "roaming_partner_country"},
    {"name": "call_month", "type": "string", "source_name": "call_month"},
    {"name": "destination_category", "type": "string", "source_name": "destination_category"},
    {"name": "destination", "type": "string", "source_name": "destination"},
    {"name": "is_camel", "type": "string", "source_name": "is_camel"},
    # Extra audit fields
    {"name": "source_system", "type": "string", "source_name": "source_system"},
    {"name": "source_file_name", "type": "string", "source_name": "source_file_name"},
    {"name": "ingestion_id", "type": "string", "source_name": "ingestion_id"},
    {"name": "batch_id", "type": "string", "source_name": "batch_id"},
    {"name": "record_hash", "type": "string", "source_name": "record_hash"},
    {"name": "received_at", "type": "string", "source_name": "received_at"},
    {"name": "processing_status", "type": "string", "source_name": "processing_status"},
    {"name": "error_message", "type": "string", "source_name": "error_message"},
    *TRAFFIC_AUDIT_COLUMNS,
]


def upgrade(op):
    op.create_table(
        namespace="bronze",
        table_name="imsi_level_traffic",
        columns=TRAFFIC_BRONZE_COLUMNS,
        partition_by=["_ingest_date"],
        properties={
            "table_type": "imsi_level_traffic_bronze",
            "source_model": "imsi-level-traffic-bronze",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    op.drop_table(namespace="bronze", table_name="imsi_level_traffic")
