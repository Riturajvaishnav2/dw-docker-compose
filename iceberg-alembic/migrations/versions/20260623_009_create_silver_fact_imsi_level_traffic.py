"""
Create silver layer fact table for IMSI-level traffic.

Grain: one row per IMSI, call date, client, partner, traffic direction, call type,
APN/service/event/RAT/TAC/group/destination/camel combination.

All keys are deterministic hashes: md5(lower(trim(column_value)))
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260623_009_create_silver_fact_imsi_level_traffic"
down_revision = "20260623_008_create_silver_dimension_tables"

CATALOG_NAME = get_catalog_name()

FACT_IMSI_LEVEL_TRAFFIC_COLUMNS = [
    # Fact ID
    {"name": "traffic_fact_id", "type": "string", "source_name": "traffic_fact_id"},
    # Foreign Keys to Dimensions
    {"name": "client_pmn_key", "type": "string", "source_name": "client_pmn_key"},
    {"name": "partner_pmn_key", "type": "string", "source_name": "partner_pmn_key"},
    {"name": "traffic_direction_key", "type": "string", "source_name": "traffic_direction_key"},
    {"name": "call_date_key", "type": "string", "source_name": "call_date_key"},
    {"name": "call_month_key", "type": "string", "source_name": "call_month_key"},
    {"name": "call_type_key", "type": "string", "source_name": "call_type_key"},
    {"name": "call_type_level_2_key", "type": "string", "source_name": "call_type_level_2_key"},
    {"name": "imsi_key", "type": "string", "source_name": "imsi_key"},
    {"name": "apn_key", "type": "string", "source_name": "apn_key"},
    {"name": "roamer_indicator_key", "type": "string", "source_name": "roamer_indicator_key"},
    {"name": "service_type_key", "type": "string", "source_name": "service_type_key"},
    {"name": "event_type_key", "type": "string", "source_name": "event_type_key"},
    {"name": "rat_type_key", "type": "string", "source_name": "rat_type_key"},
    {"name": "tac_key", "type": "string", "source_name": "tac_key"},
    {"name": "iot_rti_group_key", "type": "string", "source_name": "iot_rti_group_key"},
    {"name": "destination_key", "type": "string", "source_name": "destination_key"},
    {"name": "camel_key", "type": "string", "source_name": "camel_key"},
    # Measures
    {"name": "duration", "type": "decimal(18,6)", "source_name": "duration"},
    {"name": "volume", "type": "decimal(18,6)", "source_name": "volume"},
    {"name": "event_count", "type": "long", "source_name": "event_count"},
    {"name": "total_charge_sdr_net", "type": "decimal(18,6)", "source_name": "total_charge_sdr_net"},
    {"name": "total_charge_sdr_gross", "type": "decimal(18,6)", "source_name": "total_charge_sdr_gross"},
    # Audit Columns
    {"name": "source_system", "type": "string", "source_name": "source_system"},
    {"name": "batch_id", "type": "string", "source_name": "batch_id"},
    {"name": "record_hash", "type": "string", "source_name": "record_hash"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]


def upgrade(op):
    """Create silver fact table."""
    op.create_table(
        namespace="silver",
        table_name="fact_imsi_level_traffic",
        columns=FACT_IMSI_LEVEL_TRAFFIC_COLUMNS,
        partition_by=["call_date_key"],
        properties={
            "table_type": "fact",
            "fact_name": "imsi_level_traffic",
            "grain": "imsi_call_date_client_partner_direction_type_apn_service_event_rat_tac_group_destination_camel",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    """Drop silver fact table."""
    op.drop_table(namespace="silver", table_name="fact_imsi_level_traffic")
