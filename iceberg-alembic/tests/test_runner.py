from __future__ import annotations

from pathlib import Path

from iceberg_alembic.config import CatalogConfig
from iceberg_alembic.runner import MigrationRunner


def test_upgrade_dry_run_does_not_create_state(tmp_path: Path) -> None:
    config = CatalogConfig(
        name="test",
        type="rest",
        uri="http://localhost:8181",
        warehouse="s3://${CATALOG_WAREHOUSE}/",
        namespace="orange",
        state_path=tmp_path / ".iceberg-alembic-state.json",
    )
    runner = MigrationRunner(config=config, root=Path.cwd(), dry_run=True)

    applied = runner.upgrade("head")

    assert applied == [
        "20260529_001_create_settlement_table",
        "20260529_002_create_activation_table",
        "20260529_003_create_rating_table",
        "20260604_005_create_traffic_bronze_table",
        "20260604_006_create_traffic_silver_table",
    ]
    assert not config.state_path.exists()
