from __future__ import annotations

from iceberg_alembic.revisions import load_revisions, order_revisions


def test_revision_chain_orders_existing_migrations() -> None:
    revisions = order_revisions(load_revisions())

    assert [revision.revision for revision in revisions] == [
        "20260529_001_create_settlement_table",
        "20260529_002_create_activation_table",
        "20260529_003_create_rating_table",
        "20260604_005_create_traffic_bronze_table",
        "20260604_006_create_traffic_silver_table",
    ]
