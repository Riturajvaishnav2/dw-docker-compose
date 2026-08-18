"""
Create silver layer dimension tables for traffic data.

Dimensions created:
- dim_client
- dim_operator
- dim_traffic_direction
- dim_date
- dim_call_type
- dim_imsi
- dim_apn
- dim_service_type
- dim_event_type
- dim_rat_type
- dim_tac
- dim_iot_rti_group
- dim_location
- dim_camel
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260623_008_create_silver_dimension_tables"
down_revision = "20260617_006_create_settlement_gold_table"

CATALOG_NAME = get_catalog_name()

DIM_CLIENT_COLUMNS = [
    {"name": "pmn_key", "type": "string", "source_name": "pmn_key"},
    {"name": "pmn", "type": "string", "source_name": "pmn"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_OPERATOR_COLUMNS = [
    {"name": "pmn_key", "type": "string", "source_name": "pmn_key"},
    {"name": "pmn", "type": "string", "source_name": "pmn"},
    {"name": "pmn_country", "type": "string", "source_name": "pmn_country"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_TRAFFIC_DIRECTION_COLUMNS = [
    {"name": "traffic_direction_key", "type": "string", "source_name": "traffic_direction_key"},
    {"name": "traffic_direction", "type": "string", "source_name": "traffic_direction"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_DATE_COLUMNS = [
    {"name": "call_date_key", "type": "string", "source_name": "call_date_key"},
    {"name": "call_date", "type": "date", "source_name": "call_date"},
    {"name": "call_month", "type": "string", "source_name": "call_month"},
    {"name": "year", "type": "int", "source_name": "year"},
    {"name": "month", "type": "int", "source_name": "month"},
    {"name": "day", "type": "int", "source_name": "day"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_CALL_TYPE_COLUMNS = [
    {"name": "call_type_key", "type": "string", "source_name": "call_type_key"},
    {"name": "call_type", "type": "string", "source_name": "call_type"},
    {"name": "call_type_level_2_key", "type": "string", "source_name": "call_type_level_2_key"},
    {"name": "call_type_level_2", "type": "string", "source_name": "call_type_level_2"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_IMSI_COLUMNS = [
    {"name": "imsi_key", "type": "string", "source_name": "imsi_key"},
    {"name": "imsi", "type": "string", "source_name": "imsi"},
    {"name": "roamer_indicator_key", "type": "string", "source_name": "roamer_indicator_key"},
    {"name": "roamer_indicator", "type": "string", "source_name": "roamer_indicator"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_APN_COLUMNS = [
    {"name": "apn_key", "type": "string", "source_name": "apn_key"},
    {"name": "apn", "type": "string", "source_name": "apn"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_SERVICE_TYPE_COLUMNS = [
    {"name": "service_type_key", "type": "string", "source_name": "service_type_key"},
    {"name": "service_type_id", "type": "string", "source_name": "service_type_id"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_EVENT_TYPE_COLUMNS = [
    {"name": "event_type_key", "type": "string", "source_name": "event_type_key"},
    {"name": "event_type_id", "type": "string", "source_name": "event_type_id"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_RAT_TYPE_COLUMNS = [
    {"name": "rat_type_key", "type": "string", "source_name": "rat_type_key"},
    {"name": "rat_type", "type": "string", "source_name": "rat_type"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_TAC_COLUMNS = [
    {"name": "tac_key", "type": "string", "source_name": "tac_key"},
    {"name": "tac_number", "type": "string", "source_name": "tac_number"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_IOT_RTI_GROUP_COLUMNS = [
    {"name": "iot_rti_group_key", "type": "string", "source_name": "iot_rti_group_key"},
    {"name": "iot_rti_group_id", "type": "string", "source_name": "iot_rti_group_id"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_LOCATION_COLUMNS = [
    {"name": "location_key", "type": "string", "source_name": "location_key"},
    {"name": "location", "type": "string", "source_name": "location"},
    {"name": "location_category", "type": "string", "source_name": "location_category"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]

DIM_CAMEL_COLUMNS = [
    {"name": "camel_key", "type": "string", "source_name": "camel_key"},
    {"name": "is_camel", "type": "string", "source_name": "is_camel"},
    {"name": "created_at", "type": "timestamp", "source_name": "created_at"},
    {"name": "updated_at", "type": "timestamp", "source_name": "updated_at"},
]


def upgrade(op):
    """Create all silver dimension tables."""
    op.create_table(
        namespace="silver",
        table_name="dim_client",
        columns=DIM_CLIENT_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "client",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_operator",
        columns=DIM_OPERATOR_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "operator",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_traffic_direction",
        columns=DIM_TRAFFIC_DIRECTION_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "traffic_direction",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_date",
        columns=DIM_DATE_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "date",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_call_type",
        columns=DIM_CALL_TYPE_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "call_type",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_imsi",
        columns=DIM_IMSI_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "imsi",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_apn",
        columns=DIM_APN_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "apn",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_service_type",
        columns=DIM_SERVICE_TYPE_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "service_type",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_event_type",
        columns=DIM_EVENT_TYPE_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "event_type",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_rat_type",
        columns=DIM_RAT_TYPE_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "rat_type",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_tac",
        columns=DIM_TAC_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "tac",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_iot_rti_group",
        columns=DIM_IOT_RTI_GROUP_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "iot_rti_group",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_location",
        columns=DIM_LOCATION_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "location",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )

    op.create_table(
        namespace=f"silver",
        table_name="dim_camel",
        columns=DIM_CAMEL_COLUMNS,
        properties={
            "table_type": "dimension",
            "dimension_name": "camel",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    """Drop all silver dimension tables."""
    op.drop_table(namespace="silver", table_name="dim_camel")
    op.drop_table(namespace="silver", table_name="dim_location")
    op.drop_table(namespace="silver", table_name="dim_iot_rti_group")
    op.drop_table(namespace="silver", table_name="dim_tac")
    op.drop_table(namespace="silver", table_name="dim_rat_type")
    op.drop_table(namespace="silver", table_name="dim_event_type")
    op.drop_table(namespace="silver", table_name="dim_service_type")
    op.drop_table(namespace="silver", table_name="dim_apn")
    op.drop_table(namespace="silver", table_name="dim_imsi")
    op.drop_table(namespace="silver", table_name="dim_call_type")
    op.drop_table(namespace="silver", table_name="dim_date")
    op.drop_table(namespace="silver", table_name="dim_traffic_direction")
    op.drop_table(namespace="silver", table_name="dim_operator")
    op.drop_table(namespace="silver", table_name="dim_client")
