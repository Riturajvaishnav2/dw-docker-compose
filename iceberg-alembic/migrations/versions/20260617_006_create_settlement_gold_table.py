"""
Create the settlement gold table for reporting and analytics.

This table consolidates settlement data from the bronze layer,
enhanced with calculated fields and business logic.
"""

from iceberg_alembic.migration_helpers import DEFAULT_TABLE_PROPERTIES, with_audit_columns
from iceberg_alembic.catalog_utils import get_catalog_name

revision = "20260617_006_create_settlement_gold_table"
down_revision = "20260604_005_create_imsi_level_traffic_bronze_table"

CATALOG_NAME = get_catalog_name()

SETTLEMENT_GOLD_COLUMNS = with_audit_columns([
    {"name": "home_pmn", "type": "string", "source_name": "home_pmn"},
    {"name": "home_operator", "type": "string", "source_name": "home_operator"},
    {"name": "traffic_direction", "type": "string", "source_name": "traffic_direction"},
    {"name": "service_type", "type": "string", "source_name": "service_type"},
    {"name": "event_type", "type": "string", "source_name": "event_type"},
    {"name": "partner_name", "type": "string", "source_name": "partner_name"},
    {"name": "partner_pmn", "type": "string", "source_name": "partner_pmn"},
    {"name": "group_name", "type": "string", "source_name": "group_name"},
    {"name": "country", "type": "string", "source_name": "country"},
    {"name": "zone_name", "type": "string", "source_name": "zone_name"},
    {"name": "traffic_period", "type": "date", "source_name": "traffic_period"},
    {"name": "agreement_reference", "type": "string", "source_name": "agreement_reference"},
    {"name": "agreement_start_date", "type": "date", "source_name": "agreement_start_date"},
    {"name": "agreement_end_date", "type": "date", "source_name": "agreement_end_date"},
    {"name": "actual_forecasted", "type": "string", "source_name": "actual_forecasted"},
    {"name": "currency", "type": "string", "source_name": "currency"},
    {"name": "conversion_rate", "type": "decimal(18,6)", "source_name": "conversion_rate"},
    {"name": "settlement_status", "type": "string", "source_name": "settlement_status"},
    {"name": "traffic_volume", "type": "decimal(38,18)", "source_name": "traffic_volume"},
    {"name": "tap_charges_excl_tax", "type": "decimal(18,6)", "source_name": "tap_charges_excl_tax"},
    {"name": "tap_charges_incl_tax", "type": "decimal(18,6)", "source_name": "tap_charges_incl_tax"},
    {"name": "tap_iot_rate_excl_tax", "type": "decimal(18,6)", "source_name": "tap_iot_rate_excl_tax"},
    {"name": "tap_iot_rate_incl_tax", "type": "decimal(18,6)", "source_name": "tap_iot_rate_incl_tax"},
    {"name": "post_discounted_charges_excl_tax", "type": "decimal(18,6)", "source_name": "post_discounted_charges_excl_tax"},
    {"name": "post_discounted_charges_incl_tax", "type": "decimal(18,6)", "source_name": "post_discounted_charges_incl_tax"},
    {"name": "sop_adjustment_excl_tax", "type": "decimal(18,6)", "source_name": "sop_adjustment_excl_tax"},
    {"name": "sop_adjustment_incl_tax", "type": "decimal(18,6)", "source_name": "sop_adjustment_incl_tax"},
    {"name": "before_sop_discounted_charge_excl_tax", "type": "decimal(18,6)", "source_name": "before_sop_discounted_charge_excl_tax"},
    {"name": "before_sop_discounted_charge_incl_tax", "type": "decimal(18,6)", "source_name": "before_sop_discounted_charge_incl_tax"},
    {"name": "post_discounted_iot_rate_excl_tax", "type": "decimal(18,6)", "source_name": "post_discounted_iot_rate_excl_tax"},
    {"name": "post_discounted_iot_rate_incl_tax", "type": "decimal(18,6)", "source_name": "post_discounted_iot_rate_incl_tax"},
    {"name": "discount_achieved_excl_tax", "type": "decimal(18,6)", "source_name": "discount_achieved_excl_tax"},
    {"name": "discount_achieved_incl_tax", "type": "decimal(18,6)", "source_name": "discount_achieved_incl_tax"},
    {"name": "agreement_status", "type": "string", "source_name": "agreement_status"},
    {"name": "negotiator", "type": "string", "source_name": "negotiator"},
    {"name": "iot_rate_source", "type": "string", "source_name": "iot_rate_source"},
    {"name": "settled_amount_excl_tax", "type": "decimal(18,6)", "source_name": "settled_amount_excl_tax"},
    {"name": "settled_amount_incl_tax", "type": "decimal(18,6)", "source_name": "settled_amount_incl_tax"},
    {"name": "call_destination", "type": "string", "source_name": "call_destination"},
    {"name": "continent", "type": "string", "source_name": "continent"},
    {"name": "iso_country_code", "type": "string", "source_name": "iso_country_code"},
    {"name": "segment", "type": "string", "source_name": "segment"},
    {"name": "traffic_direction_service_event_type", "type": "string", "source_name": "traffic_direction_service_event_type"},
    {"name": "volume_set_used_in_calculation", "type": "string", "source_name": "volume_set_used_in_calculation"},
    {"name": "document_raised", "type": "string", "source_name": "document_raised"},
    {"name": "document_settled", "type": "string", "source_name": "document_settled"},
    {"name": "dch_tap_charges_excl_tax", "type": "decimal(18,6)", "source_name": "dch_tap_charges_excl_tax"},
    {"name": "dch_tap_charges_incl_tax", "type": "decimal(18,6)", "source_name": "dch_tap_charges_incl_tax"},
    {"name": "dch_traffic_volume_actual", "type": "decimal(18,6)", "source_name": "dch_traffic_volume_actual"},
    {"name": "dch_traffic_volume_billed", "type": "decimal(18,6)", "source_name": "dch_traffic_volume_billed"},
    {"name": "discount_achieved_netting_excl_tax", "type": "decimal(18,6)", "source_name": "discount_achieved_netting_excl_tax"},
    {"name": "discount_achieved_netting_incl_tax", "type": "decimal(18,6)", "source_name": "discount_achieved_netting_incl_tax"},
    {"name": "discounted_vs_tap_rate_excl_tax", "type": "decimal(18,6)", "source_name": "discounted_vs_tap_rate_excl_tax"},
    {"name": "discounted_vs_tap_rate_incl_tax", "type": "decimal(18,6)", "source_name": "discounted_vs_tap_rate_incl_tax"},
    {"name": "document_raised_amount_excl_tax", "type": "decimal(18,6)", "source_name": "document_raised_amount_excl_tax"},
    {"name": "document_raised_amount_incl_tax", "type": "decimal(18,6)", "source_name": "document_raised_amount_incl_tax"},
    {"name": "traffic_volume_not_rounded", "type": "decimal(38,18)", "source_name": "traffic_volume_not_rounded"},
])


def upgrade(op):
    op.create_table(
        namespace="gold",
        table_name="settlement",
        columns=SETTLEMENT_GOLD_COLUMNS,
        partition_by=["_ingest_date"],
        properties={
            "table_type": "settlement_gold",
            "source_model": "settlement-reporting",
            "layer": "gold",
            **DEFAULT_TABLE_PROPERTIES,
        },
    )


def downgrade(op):
    op.drop_table(namespace="gold", table_name="settlement")
